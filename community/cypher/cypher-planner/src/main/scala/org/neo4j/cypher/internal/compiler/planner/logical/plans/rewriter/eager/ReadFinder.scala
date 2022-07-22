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

import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.expressions.functions.Properties
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InputPosition

/**
 * Finds all reads for a single plan.
 */
object ReadFinder {

  /**
   * Reads of a single plan.
   * The Seqs may contain duplicates. These are filtered out later in [[ConflictFinder]].
   *
   * @param readProperties         the read properties
   * @param readsUnknownProperties `true` if the plan reads unknown properties, e.g. by calling the `properties` function.
   * @param readLabels             the read labels
   * @param filterExpressions      All expressions that filter the rows, in a map with the dependency as key.
   * @param readsUnknownLabels     `true` if the plan reads unknown labels, e.g. by calling the `labels` function.
   * @param readsAllNodes          `true` if the plan is an [[AllNodesScan]]
   */
  private[eager] case class PlanReads(
    readProperties: Seq[PropertyKeyName] = Seq.empty,
    readsUnknownProperties: Boolean = false,
    readLabels: Seq[LabelName] = Seq.empty,
    filterExpressions: Map[LogicalVariable, Seq[Expression]] = Map.empty,
    readsUnknownLabels: Boolean = false,
    readsAllNodes: Boolean = false
  ) {

    def withPropertyRead(property: PropertyKeyName): PlanReads = {
      copy(readProperties = readProperties :+ property)
    }

    def withUnknownPropertiesRead(): PlanReads =
      copy(readsUnknownProperties = true)

    def withLabelRead(label: LabelName): PlanReads = {
      copy(readLabels = readLabels :+ label)
    }

    def withAddedFilterExpression(variable: LogicalVariable, filterExpression: Expression): PlanReads = {
      val newExpressions = filterExpressions.getOrElse(variable, Seq.empty) :+ filterExpression
      copy(filterExpressions = filterExpressions + (variable -> newExpressions))
    }

    def withUnknownLabelsRead(): PlanReads =
      copy(readsUnknownLabels = true)

    def withAllNodesRead: PlanReads =
      copy(readsAllNodes = true)

    /**
     * @return A `Seq` of all read properties.
     *         For a known `PropertyKeyName` (`Some`) and for an unknown `PropertyKeyName` (`None`).
     */
    def readPropertiesIncludingUnknown: Seq[Option[PropertyKeyName]] = {
      val concrete = readProperties.map(Some(_))
      if (readsUnknownProperties) concrete :+ None else concrete
    }
  }

  /**
   * Collect the reads of a single plan, not traversing into child plans.
   */
  private[eager] def collectReads(plan: LogicalPlan): PlanReads = plan.folder.treeFold(PlanReads()) {
    case otherPlan: LogicalPlan if otherPlan.id != plan.id =>
      acc => SkipChildren(acc) // Do not traverse the logical plan tree! We are only looking at the given plan

    // Match on plans
    case p: LogicalLeafPlan =>
      // This extra match is not strictly necessary, but allows us to detect a missing case for new leaf plans easier because it will fail hard.
      p match {
        case _: AllNodesScan =>
          acc => SkipChildren(acc.withAllNodesRead)

        case NodeByLabelScan(varName, labelName, _, _) =>
          acc =>
            val variable = Variable(varName)(InputPosition.NONE)
            val hasLabels = HasLabels(variable, Seq(labelName))(InputPosition.NONE)
            val newAcc = acc
              .withLabelRead(labelName)
              .withAddedFilterExpression(variable, hasLabels)
            SkipChildren(newAcc)

        case NodeIndexScan(varName, LabelToken(labelName, _), properties, _, _, _) =>
          acc =>
            val variable = Variable(varName)(InputPosition.NONE)
            val lN = LabelName(labelName)(InputPosition.NONE)
            val hasLabels = HasLabels(variable, Seq(lN))(InputPosition.NONE)

            val newAcc = Option(acc)
              .map(acc => acc.withLabelRead(lN))
              .map(acc => acc.withAddedFilterExpression(variable, hasLabels))
              .map(acc =>
                properties.foldLeft(acc) {
                  case (acc, IndexedProperty(PropertyKeyToken(property, _), _, _)) =>
                    acc.withPropertyRead(PropertyKeyName(property)(InputPosition.NONE))
                }
              )
              .get
            SkipChildren(newAcc)

        case _: Argument =>
          acc => SkipChildren(acc)

        case x => throw new IllegalStateException(s"Leaf operator ${x.getClass.getSimpleName} not implemented yet.")
      }

    case Selection(Ands(expressions), _) => acc =>
        val newAcc = expressions.foldLeft(acc) {
          case (acc, expression) => expression.dependencies.foldLeft(acc)(_.withAddedFilterExpression(_, expression))
        }
        TraverseChildren(newAcc)

    // Match on expressions
    case Property(_, propertyName) =>
      acc => SkipChildren(acc.withPropertyRead(propertyName))

    case f: FunctionInvocation if f.function == Labels =>
      acc => TraverseChildren(acc.withUnknownLabelsRead())

    case f: FunctionInvocation if f.function == Properties =>
      acc => TraverseChildren(acc.withUnknownPropertiesRead())

    case HasLabels(_, labels) =>
      acc => TraverseChildren(labels.foldLeft(acc)((acc, label) => acc.withLabelRead(label)))
  }

}
