/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadsAndWritesFinder.collectReadsAndWrites
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.HasALabel
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.expressions.functions.Properties
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.Input
import org.neo4j.cypher.internal.logical.plans.IntersectionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.NodeByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.UnionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTNode

/**
 * Finds all reads for a single plan.
 */
object ReadFinder {

  /**
   * Reads of a single plan.
   * The Seqs may contain duplicates. These are filtered out later in [[ConflictFinder]].
   *
   * @param readProperties          the read properties
   * @param readsUnknownProperties  `true` if the plan reads unknown properties, e.g. by calling the `properties` function.
   * @param readLabels              the read labels
   * @param readsUnknownLabels      `true` if the plan reads unknown labels, e.g. by calling the `labels` function.
   * @param nodeFilterExpressions   All node expressions that filter the rows, in a map with the dependency as key.
   *                                This also tracks if a variable is introduced by this plan.
   *                                If a variable is introduced by this plan, and no predicates are applied on that variable,
   *                                it is still present as a key in this map with an empty sequence of filter expressions.
   * @param referencedNodeVariables all referenced node variables
   */
  private[eager] case class PlanReads(
    readProperties: Seq[PropertyKeyName] = Seq.empty,
    readsUnknownProperties: Boolean = false,
    readLabels: Seq[LabelName] = Seq.empty,
    readsUnknownLabels: Boolean = false,
    nodeFilterExpressions: Map[LogicalVariable, Seq[Expression]] = Map.empty,
    referencedNodeVariables: Set[LogicalVariable] = Set.empty
  ) {

    def withPropertyRead(property: PropertyKeyName): PlanReads = {
      copy(readProperties = readProperties :+ property)
    }

    def withUnknownPropertiesRead(): PlanReads =
      copy(readsUnknownProperties = true)

    def withLabelRead(label: LabelName): PlanReads = {
      copy(readLabels = readLabels :+ label)
    }

    /**
     * Save that the plan introduces a node variable.
     * This is done by saving an empty filter expressions.
     *
     * Also save that this plan references the variable.
     */
    def withIntroducedNodeVariable(variable: LogicalVariable): PlanReads = {
      val newExpressions = nodeFilterExpressions.getOrElse(variable, Seq.empty)
      copy(
        nodeFilterExpressions = nodeFilterExpressions + (variable -> newExpressions),
        referencedNodeVariables = referencedNodeVariables + variable
      )
    }

    def withAddedNodeFilterExpression(variable: LogicalVariable, filterExpression: Expression): PlanReads = {
      val newExpressions = nodeFilterExpressions.getOrElse(variable, Seq.empty) :+ filterExpression
      copy(nodeFilterExpressions = nodeFilterExpressions + (variable -> newExpressions))
    }

    def withUnknownLabelsRead(): PlanReads =
      copy(readsUnknownLabels = true)

    def withReferencedNodeVariable(variable: LogicalVariable): PlanReads =
      copy(referencedNodeVariables = referencedNodeVariables + variable)
  }

  /**
   * Collect the reads of a single plan, not traversing into child plans.
   */
  private[eager] def collectReads(plan: LogicalPlan, semanticTable: SemanticTable): PlanReads = {
    // Match on plans
    val planReads = plan match {
      case p: LogicalLeafPlan =>
        // This extra match is not strictly necessary, but allows us to detect a missing case for new leaf plans easier because it will fail hard.
        p match {
          case AllNodesScan(varName, _) =>
            val variable = Variable(varName)(InputPosition.NONE)
            PlanReads()
              .withIntroducedNodeVariable(variable)

          case NodeByLabelScan(varName, labelName, _, _) =>
            val variable = Variable(varName)(InputPosition.NONE)
            val hasLabels = HasLabels(variable, Seq(labelName))(InputPosition.NONE)
            PlanReads()
              .withLabelRead(labelName)
              .withIntroducedNodeVariable(variable)
              .withAddedNodeFilterExpression(variable, hasLabels)

          case UnionNodeByLabelsScan(varName, labelNames, _, _) =>
            val variable = Variable(varName)(InputPosition.NONE)
            val predicates = labelNames.map { labelName =>
              HasLabels(variable, Seq(labelName))(InputPosition.NONE)
            }
            val filterExpression = Ors(predicates)(InputPosition.NONE)
            val acc = PlanReads()
              .withIntroducedNodeVariable(variable)
              .withAddedNodeFilterExpression(variable, filterExpression)
            labelNames.foldLeft(acc) { (acc, labelName) =>
              acc.withLabelRead(labelName)
            }

          case IntersectionNodeByLabelsScan(varName, labelNames, _, _) =>
            val variable = Variable(varName)(InputPosition.NONE)
            val acc = PlanReads()
              .withIntroducedNodeVariable(variable)
            labelNames.foldLeft(acc) { (acc, labelName) =>
              val variable = Variable(varName)(InputPosition.NONE)
              val hasLabels = HasLabels(variable, Seq(labelName))(InputPosition.NONE)
              acc.withLabelRead(labelName)
                .withAddedNodeFilterExpression(variable, hasLabels)
            }

          case NodeCountFromCountStore(varName, labelNames, _) =>
            // The varName is really for the count variable - we don't have a node variable.
            // But this is OK?
            val variable = Variable(varName)(InputPosition.NONE)
            val acc = PlanReads()
              .withIntroducedNodeVariable(variable)
            labelNames.flatten.foldLeft(acc) { (acc, labelName) =>
              val hasLabels = HasLabels(variable, Seq(labelName))(InputPosition.NONE)
              acc.withLabelRead(labelName)
                .withAddedNodeFilterExpression(variable, hasLabels)
            }

          case NodeIndexScan(varName, LabelToken(labelName, _), properties, _, _, _) =>
            processIndexPlan(varName, labelName, properties)

          case NodeIndexSeek(varName, LabelToken(labelName, _), properties, _, _, _, _) =>
            processIndexPlan(varName, labelName, properties)

          case NodeUniqueIndexSeek(varName, LabelToken(labelName, _), properties, _, _, _, _) =>
            processIndexPlan(varName, labelName, properties)

          case NodeIndexContainsScan(
              varName,
              LabelToken(labelName, _),
              property,
              _,
              _,
              _,
              _
            ) =>
            processIndexPlan(varName, labelName, Seq(property))

          case NodeIndexEndsWithScan(
              varName,
              LabelToken(labelName, _),
              property,
              _,
              _,
              _,
              _
            ) =>
            processIndexPlan(varName, labelName, Seq(property))

          case NodeByIdSeek(varName, _, _) =>
            // We could avoid eagerness when we have IdSeeks with a single ID.
            // As soon as we have multiple IDs, future creates could create nodes with one of those IDs.
            // Not eagerizing a single row is not worth the extra complexity, so we accept that imperfection.
            val variable = Variable(varName)(InputPosition.NONE)
            PlanReads()
              .withIntroducedNodeVariable(variable)

          case NodeByElementIdSeek(varName, _, _) =>
            // We could avoid eagerness when we have IdSeeks with a single ID.
            // As soon as we have multiple IDs, future creates could create nodes with one of those IDs.
            // Not eagerizing a single row is not worth the extra complexity, so we accept that imperfection.
            val variable = Variable(varName)(InputPosition.NONE)
            PlanReads()
              .withIntroducedNodeVariable(variable)

          case _: Argument =>
            PlanReads()

          case _: Input =>
            PlanReads()

          case x => throw new IllegalStateException(s"Leaf operator ${x.getClass.getSimpleName} not implemented yet.")
        }

      case Selection(Ands(expressions), _) =>
        expressions.foldLeft(PlanReads()) {
          case (acc, expression) => expression.dependencies
              .filter(semanticTable.isNodeNoFail)
              .foldLeft(acc)(_.withAddedNodeFilterExpression(_, expression))
        }

      case Expand(_, _, _, _, to, _, _) =>
        val variable = Variable(to)(InputPosition.NONE)
        PlanReads()
          .withIntroducedNodeVariable(variable)

      case OptionalExpand(_, _, _, _, to, _, _, _) =>
        val variable = Variable(to)(InputPosition.NONE)
        PlanReads()
          .withIntroducedNodeVariable(variable)

      case VarExpand(_, _, _, _, _, to, _, _, _, _, _) =>
        val variable = Variable(to)(InputPosition.NONE)
        PlanReads()
          .withIntroducedNodeVariable(variable)

      case ProduceResult(_, columns) if columns.exists(semanticTable.containsNode) =>
        PlanReads()
          .withUnknownPropertiesRead()
          .withUnknownLabelsRead()

      case _ => PlanReads()
    }

    // Match on expressions
    plan.folder.treeFold(planReads) {
      case otherPlan: LogicalPlan if otherPlan.id != plan.id =>
        // Do not traverse the logical plan tree! We are only looking at expressions of the given plan
        acc => SkipChildren(acc)

      case v: Variable
        // If v could be a node
        if semanticTable.types.get(v).fold(true)(_.specified contains CTNode) =>
        acc => SkipChildren(acc.withReferencedNodeVariable(v))

      case Property(expr, propertyName) if !semanticTable.isMapNoFail(expr) =>
        acc => TraverseChildren(acc.withPropertyRead(propertyName))

      case f: FunctionInvocation if f.function == Labels =>
        acc => TraverseChildren(acc.withUnknownLabelsRead())

      case f: FunctionInvocation if f.function == Properties && !semanticTable.isMapNoFail(f.args(0)) =>
        acc => TraverseChildren(acc.withUnknownPropertiesRead())

      case HasLabels(_, labels) =>
        acc => TraverseChildren(labels.foldLeft(acc)((acc, label) => acc.withLabelRead(label)))

      case HasALabel(Variable(_)) =>
        acc => TraverseChildren(acc.withUnknownLabelsRead())

      case ContainerIndex(expr, index) if !semanticTable.isIntegerNoFail(index) && !semanticTable.isMapNoFail(expr) =>
        // if we access by index, foo[0] or foo[&autoIntX] we must be accessing a list and hence we
        // are not accessing a property
        acc => SkipChildren(acc.withUnknownPropertiesRead())

      case npe: NestedPlanExpression =>
        // A nested plan expression cannot have writes
        val nestedReads = collectReadsAndWrites(npe.plan, semanticTable).reads

        // Remap all reads to the outer plan, i.e. to the PlanReads currently being built
        val readProperties = nestedReads.readProperties.plansReadingConcreteSymbol.keySet
        val readsUnknownProperties = nestedReads.readProperties.plansReadingUnknownSymbols.nonEmpty
        val readLabels = nestedReads.readLabels.plansReadingConcreteSymbol.keySet
        val readsUnknownLabels = nestedReads.readLabels.plansReadingUnknownSymbols.nonEmpty
        val nodeFilterExpressions = nestedReads.nodeFilterExpressions.view.mapValues(_.expression)
        val referencedNodeVariables = nestedReads.possibleNodeDeleteConflictPlans.keySet

        AssertMacros.checkOnlyWhenAssertionsAreEnabled(
          nestedReads.productIterator.toSeq == Seq(
            nestedReads.readProperties,
            nestedReads.readLabels,
            nestedReads.nodeFilterExpressions,
            nestedReads.possibleNodeDeleteConflictPlans
          ),
          "Make sure to edit this place when adding new fields to Reads"
        )

        acc => {
          val nextAcc = Function.chain[PlanReads](Seq(
            acc => readProperties.foldLeft(acc)(_.withPropertyRead(_)),
            acc => if (readsUnknownProperties) acc.withUnknownPropertiesRead() else acc,
            acc => readLabels.foldLeft(acc)(_.withLabelRead(_)),
            acc => if (readsUnknownLabels) acc.withUnknownLabelsRead() else acc,
            acc =>
              nodeFilterExpressions.foldLeft(acc) {
                case (acc, (variable, nfe)) => acc.withAddedNodeFilterExpression(variable, nfe)
              },
            acc => referencedNodeVariables.foldLeft(acc)(_.withReferencedNodeVariable(_))
          ))(acc)
          TraverseChildren(nextAcc)
        }
    }
  }

  private def processIndexPlan(varName: String, labelName: String, properties: Seq[IndexedProperty]): PlanReads = {
    val variable = Variable(varName)(InputPosition.NONE)
    val lN = LabelName(labelName)(InputPosition.NONE)
    val hasLabels = HasLabels(variable, Seq(lN))(InputPosition.NONE)

    val r = PlanReads()
      .withLabelRead(lN)
      .withIntroducedNodeVariable(variable)
      .withAddedNodeFilterExpression(variable, hasLabels)

    properties.foldLeft(r) {
      case (acc, IndexedProperty(PropertyKeyToken(property, _), _, _)) =>
        val propName = PropertyKeyName(property)(InputPosition.NONE)
        val propPredicate = IsNotNull(Property(variable, propName)(InputPosition.NONE))(InputPosition.NONE)

        acc
          .withPropertyRead(propName)
          .withAddedNodeFilterExpression(variable, propPredicate)
    }
  }

}
