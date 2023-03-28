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
import org.neo4j.cypher.internal.expressions.GetDegree
import org.neo4j.cypher.internal.expressions.HasALabel
import org.neo4j.cypher.internal.expressions.HasDegree
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThan
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasDegreeLessThan
import org.neo4j.cypher.internal.expressions.HasDegreeLessThanOrEqual
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.expressions.functions.Properties
import org.neo4j.cypher.internal.ir.CreatePattern
import org.neo4j.cypher.internal.ir.DeleteExpression
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.RemoveLabelPattern
import org.neo4j.cypher.internal.ir.SetLabelPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesPattern
import org.neo4j.cypher.internal.ir.SetNodePropertyPattern
import org.neo4j.cypher.internal.ir.SetPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetPropertiesPattern
import org.neo4j.cypher.internal.ir.SetPropertyPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertiesPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertyPattern
import org.neo4j.cypher.internal.ir.ShortestPathPattern
import org.neo4j.cypher.internal.ir.SimpleMutatingPattern
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.CommandLogicalPlan
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.DirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.DirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.Input
import org.neo4j.cypher.internal.logical.plans.IntersectionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlanExtension
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Merge
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
import org.neo4j.cypher.internal.logical.plans.PhysicalPlanningPlan
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.RelationshipCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SetLabels
import org.neo4j.cypher.internal.logical.plans.SetNodeProperties
import org.neo4j.cypher.internal.logical.plans.SetNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetNodeProperty
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperties
import org.neo4j.cypher.internal.logical.plans.SetRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperty
import org.neo4j.cypher.internal.logical.plans.UndirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.UnionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship

/**
 * Finds all reads for a single plan.
 */
object ReadFinder {

  /**
   * Reads of a single plan.
   * The Seqs may contain duplicates. These are filtered out later in [[ConflictFinder]].
   *
   * @param readNodeProperties              the read Node properties
   * @param readsUnknownNodeProperties      `true` if the plan reads unknown Node properties, e.g. by calling the `properties` function.
   * @param readLabels                      the read labels
   * @param readsUnknownLabels              `true` if the plan reads unknown labels, e.g. by calling the `labels` function.
   * @param nodeFilterExpressions           All node expressions that filter the rows, in a map with the dependency as key.
   *                                        This also tracks if a variable is introduced by this plan.
   *                                        If a variable is introduced by this plan, and no predicates are applied on that variable,
   *                                        it is still present as a key in this map with an empty sequence of filter expressions.
   * @param referencedNodeVariables         all referenced node variables
   * @param readRelProperties               the read Relationship properties
   * @param readsUnknownRelProperties       `true` if the plan reads Relationship unknown properties, e.g. by calling the `properties` function.
   * @param readTypes                       the read types
   * @param readsUnknownTypes               `true` if the plan reads unknown types, e.g. by calling the `types` function.
   * @param relationshipFilterExpressions   All type expressions that filter the rows, in a map with the dependency as key.
   *                                        This also tracks if a variable is introduced by this plan.
   *                                        If a variable is introduced by this plan, and no predicates are applied on that variable,
   *                                        it is still present as a key in this map with an empty sequence of filter expressions.
   * @param referencedRelationshipVariables all referenced Relationship variables
   */
  private[eager] case class PlanReads(
    readNodeProperties: Seq[PropertyKeyName] = Seq.empty,
    readsUnknownNodeProperties: Boolean = false,
    readLabels: Seq[LabelName] = Seq.empty,
    readsUnknownLabels: Boolean = false,
    nodeFilterExpressions: Map[LogicalVariable, Seq[Expression]] = Map.empty,
    readRelProperties: Seq[PropertyKeyName] = Seq.empty,
    readTypes: Seq[RelTypeName] = Seq.empty,
    readsUnknownTypes: Boolean = false,
    readsUnknownRelProperties: Boolean = false,
    relationshipFilterExpressions: Map[LogicalVariable, Seq[Expression]] = Map.empty,
    referencedNodeVariables: Set[LogicalVariable] = Set.empty,
    referencedRelationshipVariables: Set[LogicalVariable] = Set.empty
  ) {

    def withNodePropertyRead(property: PropertyKeyName): PlanReads = {
      copy(readNodeProperties = readNodeProperties :+ property)
    }

    def withRelPropertyRead(property: PropertyKeyName): PlanReads = {
      copy(readRelProperties = readRelProperties :+ property)
    }

    def withUnknownNodePropertiesRead(): PlanReads =
      copy(readsUnknownNodeProperties = true)

    def withUnknownRelPropertiesRead(): PlanReads =
      copy(readsUnknownRelProperties = true)

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

    def withIntroducedNodeVariable(name: String): PlanReads = {
      val variable = Variable(name)(InputPosition.NONE)
      withIntroducedNodeVariable(variable)
    }

    def withIntroducedRelationshipVariable(variable: Variable): PlanReads = {
      val newExpressions = relationshipFilterExpressions.getOrElse(variable, Seq.empty)
      copy(
        relationshipFilterExpressions = relationshipFilterExpressions + (variable -> newExpressions),
        referencedRelationshipVariables = referencedRelationshipVariables + variable
      )
    }

    def withIntroducedRelationshipVariable(name: String): PlanReads = {
      val variable = Variable(name)(InputPosition.NONE)
      val newExpressions = relationshipFilterExpressions.getOrElse(variable, Seq.empty)
      copy(
        relationshipFilterExpressions = relationshipFilterExpressions + (variable -> newExpressions),
        referencedRelationshipVariables = referencedRelationshipVariables + variable
      )
    }

    def withAddedNodeFilterExpression(variable: LogicalVariable, filterExpression: Expression): PlanReads = {
      val newExpressions = nodeFilterExpressions.getOrElse(variable, Seq.empty) :+ filterExpression
      copy(nodeFilterExpressions = nodeFilterExpressions + (variable -> newExpressions))
    }

    def withAddedRelationshipFilterExpression(variable: LogicalVariable, filterExpression: Expression): PlanReads = {
      val newExpressions = relationshipFilterExpressions.getOrElse(variable, Seq.empty) :+ filterExpression
      copy(relationshipFilterExpressions = relationshipFilterExpressions + (variable -> newExpressions))
    }

    def withUnknownLabelsRead(): PlanReads =
      copy(readsUnknownLabels = true)

    def withUnknownTypesRead(): PlanReads =
      copy(readsUnknownTypes = true)

    def withReferencedNodeVariable(variable: LogicalVariable): PlanReads =
      copy(referencedNodeVariables = referencedNodeVariables + variable)

    def withReferencedNodeVariable(name: String): PlanReads = {
      val variable = Variable(name)(InputPosition.NONE)
      withReferencedNodeVariable(variable)
    }

    def withReferencedRelationshipVariable(variable: LogicalVariable): PlanReads =
      copy(referencedRelationshipVariables = referencedRelationshipVariables + variable)

    def withReferencedRelationshipVariable(name: String): PlanReads = {
      val variable = Variable(name)(InputPosition.NONE)
      copy(referencedRelationshipVariables = referencedRelationshipVariables + variable)
    }
  }

  /**
   * Collect the reads of a single plan, not traversing into child plans.
   */
  private[eager] def collectReads(
    plan: LogicalPlan,
    semanticTable: SemanticTable,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): PlanReads = {
    // Match on plans
    val planReads = plan match {
      case p: LogicalLeafPlan =>
        // This extra match is not strictly necessary, but allows us to detect a missing case for new leaf plans easier because it fail compilation.
        p match {
          case AllNodesScan(varName, _) =>
            PlanReads()
              .withIntroducedNodeVariable(varName)

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
            val acc = PlanReads()
              .withIntroducedNodeVariable(varName)
            labelNames.foldLeft(acc) { (acc, labelName) =>
              val variable = Variable(varName)(InputPosition.NONE)
              val hasLabels = HasLabels(variable, Seq(labelName))(InputPosition.NONE)
              acc.withLabelRead(labelName)
                .withAddedNodeFilterExpression(variable, hasLabels)
            }

          case NodeCountFromCountStore(_, labelNames, _) =>
            val variable = Variable(anonymousVariableNameGenerator.nextName)(InputPosition.NONE)
            val acc = PlanReads()
              .withIntroducedNodeVariable(variable)
            labelNames.flatten.foldLeft(acc) { (acc, labelName) =>
              val hasLabels = HasLabels(variable, Seq(labelName))(InputPosition.NONE)
              acc.withLabelRead(labelName)
                .withAddedNodeFilterExpression(variable, hasLabels)
            }

          case RelationshipCountFromCountStore(_, startLabel, typeNames, endLabel, _) =>
            val relVariable = Variable(anonymousVariableNameGenerator.nextName)(InputPosition.NONE)
            val nodeVariable = Variable(anonymousVariableNameGenerator.nextName)(InputPosition.NONE)
            val acc = PlanReads()
              .withIntroducedRelationshipVariable(relVariable)
              .withIntroducedNodeVariable(nodeVariable)

            Function.chain[PlanReads](Seq(
              Seq(startLabel, endLabel).flatten.foldLeft(_) { (acc, labelName) =>
                val hasLabels = HasLabels(nodeVariable, Seq(labelName))(InputPosition.NONE)
                acc.withLabelRead(labelName)
                  .withAddedNodeFilterExpression(nodeVariable, hasLabels)
              },
              typeNames.foldLeft(_) { (acc, typeName) =>
                val hasTypes = HasTypes(relVariable, Seq(typeName))(InputPosition.NONE)
                acc.withAddedRelationshipFilterExpression(relVariable, hasTypes)
              }
            ))(acc)

          case NodeIndexScan(varName, LabelToken(labelName, _), properties, _, _, _) =>
            processNodeIndexPlan(varName, labelName, properties)

          case NodeIndexSeek(varName, LabelToken(labelName, _), properties, _, _, _, _) =>
            processNodeIndexPlan(varName, labelName, properties)

          case NodeUniqueIndexSeek(varName, LabelToken(labelName, _), properties, _, _, _, _) =>
            processNodeIndexPlan(varName, labelName, properties)

          case NodeIndexContainsScan(
              varName,
              LabelToken(labelName, _),
              property,
              _,
              _,
              _,
              _
            ) =>
            processNodeIndexPlan(varName, labelName, Seq(property))

          case NodeIndexEndsWithScan(
              varName,
              LabelToken(labelName, _),
              property,
              _,
              _,
              _,
              _
            ) =>
            processNodeIndexPlan(varName, labelName, Seq(property))

          case NodeByIdSeek(varName, _, _) =>
            // We could avoid eagerness when we have IdSeeks with a single ID.
            // As soon as we have multiple IDs, future creates could create nodes with one of those IDs.
            // Not eagerizing a single row is not worth the extra complexity, so we accept that imperfection.
            PlanReads()
              .withIntroducedNodeVariable(varName)

          case NodeByElementIdSeek(varName, _, _) =>
            // We could avoid eagerness when we have IdSeeks with a single ID.
            // As soon as we have multiple IDs, future creates could create nodes with one of those IDs.
            // Not eagerizing a single row is not worth the extra complexity, so we accept that imperfection.
            PlanReads()
              .withIntroducedNodeVariable(varName)

          case _: Argument =>
            PlanReads()

          case _: Input =>
            PlanReads()

          case UndirectedAllRelationshipsScan(idName, leftNode, rightNode, _) =>
            processRelationshipRead(idName, leftNode, rightNode)

          case DirectedAllRelationshipsScan(idName, leftNode, rightNode, _) =>
            processRelationshipRead(idName, leftNode, rightNode)

          case UndirectedRelationshipTypeScan(idName, leftNode, relType, rightNode, _, _) =>
            processRelTypeRead(idName, leftNode, relType, rightNode)

          case DirectedRelationshipTypeScan(idName, leftNode, relType, rightNode, _, _) =>
            processRelTypeRead(idName, leftNode, relType, rightNode)

          case DirectedRelationshipIndexScan(
              idName,
              leftNode,
              rightNode,
              RelationshipTypeToken(typeName, _),
              properties,
              _,
              _,
              _
            ) =>
            processRelationshipIndexPlan(idName, typeName, properties, leftNode, rightNode)

          case DirectedRelationshipIndexSeek(
              idName,
              leftNode,
              rightNode,
              RelationshipTypeToken(typeName, _),
              properties,
              _,
              _,
              _,
              _
            ) =>
            processRelationshipIndexPlan(idName, typeName, properties, leftNode, rightNode)

          case UndirectedRelationshipIndexScan(
              idName,
              leftNode,
              rightNode,
              RelationshipTypeToken(typeName, _),
              properties,
              _,
              _,
              _
            ) =>
            processRelationshipIndexPlan(idName, typeName, properties, leftNode, rightNode)

          case UndirectedRelationshipIndexSeek(
              idName,
              leftNode,
              rightNode,
              RelationshipTypeToken(typeName, _),
              properties,
              _,
              _,
              _,
              _
            ) =>
            processRelationshipIndexPlan(idName, typeName, properties, leftNode, rightNode)

          case UndirectedRelationshipUniqueIndexSeek(
              idName,
              leftNode,
              rightNode,
              RelationshipTypeToken(typeName, _),
              properties,
              _,
              _,
              _,
              _
            ) =>
            processRelationshipIndexPlan(idName, typeName, properties, leftNode, rightNode)

          case DirectedRelationshipUniqueIndexSeek(
              idName,
              leftNode,
              rightNode,
              RelationshipTypeToken(typeName, _),
              properties,
              _,
              _,
              _,
              _
            ) =>
            processRelationshipIndexPlan(idName, typeName, properties, leftNode, rightNode)

          case UndirectedRelationshipIndexContainsScan(
              idName,
              leftNode,
              rightNode,
              RelationshipTypeToken(typeName, _),
              property,
              _,
              _,
              _,
              _
            ) =>
            processRelationshipIndexPlan(idName, typeName, Seq(property), leftNode, rightNode)

          case DirectedRelationshipIndexContainsScan(
              idName,
              leftNode,
              rightNode,
              RelationshipTypeToken(typeName, _),
              property,
              _,
              _,
              _,
              _
            ) =>
            processRelationshipIndexPlan(idName, typeName, Seq(property), leftNode, rightNode)

          case UndirectedRelationshipIndexEndsWithScan(
              idName,
              leftNode,
              rightNode,
              RelationshipTypeToken(typeName, _),
              property,
              _,
              _,
              _,
              _
            ) =>
            processRelationshipIndexPlan(idName, typeName, Seq(property), leftNode, rightNode)

          case DirectedRelationshipIndexEndsWithScan(
              idName,
              leftNode,
              rightNode,
              RelationshipTypeToken(typeName, _),
              property,
              _,
              _,
              _,
              _
            ) =>
            processRelationshipIndexPlan(idName, typeName, Seq(property), leftNode, rightNode)

          case UndirectedRelationshipByIdSeek(idName, _, leftNode, rightNode, _) =>
            processRelationshipRead(idName, leftNode, rightNode)

          case DirectedRelationshipByIdSeek(idName, _, leftNode, rightNode, _) =>
            processRelationshipRead(idName, leftNode, rightNode)

          case UndirectedRelationshipByElementIdSeek(idName, _, leftNode, rightNode, _) =>
            processRelationshipRead(idName, leftNode, rightNode)

          case DirectedRelationshipByElementIdSeek(idName, _, leftNode, rightNode, _) =>
            processRelationshipRead(idName, leftNode, rightNode)

          case UndirectedUnionRelationshipTypesScan(idName, leftNode, relTypes, rightNode, _, _) =>
            processUnionRelTypeScan(idName, leftNode, relTypes, rightNode)

          case DirectedUnionRelationshipTypesScan(idName, leftNode, relTypes, rightNode, _, _) =>
            processUnionRelTypeScan(idName, leftNode, relTypes, rightNode)

          case _: PhysicalPlanningPlan | _: CommandLogicalPlan | _: LogicalLeafPlanExtension =>
            throw new IllegalStateException(s"Unsupported leaf plan in eagerness analysis: $p")
        }

      case Selection(Ands(expressions), _) =>
        expressions.foldLeft(PlanReads()) {
          case (acc, expression) =>
            val nodeExpr = expression.dependencies.filter(semanticTable.isNodeNoFail)
            val relExpr = expression.dependencies.filter(semanticTable.isRelationshipNoFail)

            Function.chain[PlanReads](Seq(
              nodeExpr.foldLeft(_)(_.withAddedNodeFilterExpression(_, expression)),
              relExpr.foldLeft(_)(_.withAddedRelationshipFilterExpression(_, expression))
            ))(acc)
        }

      case Expand(_, _, _, relTypes, to, relName, _) =>
        processExpand(relTypes, to, relName)

      case OptionalExpand(_, _, _, relTypes, to, relName, _, _) =>
        processExpand(relTypes, to, relName)

      case VarExpand(_, _, _, _, relTypes, to, relName, _, _, _, _) =>
        // relName is actually a List of relationships but we can consider it to be a Relationship Variable when doing eagerness analysis
        processExpand(relTypes, to, relName)

      case FindShortestPaths(
          _,
          ShortestPathPattern(_, PatternRelationship(name, nodes, _, types, _), _),
          _,
          _,
          _,
          _,
          _
        ) =>
        val relVariable = Variable(name)(InputPosition.NONE)
        val hasRelTypes = HasTypes(relVariable, types)(InputPosition.NONE)
        val (leftNode, rightNode) = nodes
        PlanReads()
          // relVariable is actually a List of relationships but we can consider it to be a Relationship Variable when doing eagerness analysis
          .withIntroducedRelationshipVariable(relVariable)
          .withAddedRelationshipFilterExpression(relVariable, hasRelTypes)
          .withReferencedNodeVariable(Variable(leftNode)(InputPosition.NONE))
          .withReferencedNodeVariable(Variable(rightNode)(InputPosition.NONE))

      case ProduceResult(_, columns) =>
        val hasNode = columns.exists(semanticTable.containsNode)
        val hasRel = columns.exists(semanticTable.containsRelationship)
        var result = PlanReads()
        if (hasNode) {
          result = result.withUnknownNodePropertiesRead().withUnknownLabelsRead()
        }
        if (hasRel) {
          result = result.withUnknownRelPropertiesRead().withUnknownTypesRead()
        }
        result

      case Create(_, nodes, rels) =>
        val createdNodes = nodes.map(_.idName)
        val createdRels = rels.map(_.idName)
        val referencedNodes = rels.flatMap(r => Seq(r.leftNode, r.rightNode))

        Function.chain[PlanReads](Seq(
          createdNodes.foldLeft(_)(_.withIntroducedNodeVariable(_)),
          createdRels.foldLeft(_)(_.withIntroducedRelationshipVariable(_)),
          referencedNodes.foldLeft(_)(_.withReferencedNodeVariable(_))
        ))(PlanReads())

      case Foreach(_, _, _, mutations) =>
        mutations.foldLeft(PlanReads())(processSimpleMutatingPattern)

      case Merge(_, nodes, rels, onMatch, onCreate, _) =>
        val createdNodes = nodes.map(_.idName)
        val createdRels = rels.map(_.idName)
        val referencedNodes = rels.flatMap(r => Seq(r.leftNode, r.rightNode))

        Function.chain[PlanReads](Seq(
          createdNodes.foldLeft(_)(_.withIntroducedNodeVariable(_)),
          createdRels.foldLeft(_)(_.withIntroducedRelationshipVariable(_)),
          referencedNodes.foldLeft(_)(_.withReferencedNodeVariable(_)),
          onMatch.foldLeft(_)(processSimpleMutatingPattern),
          onCreate.foldLeft(_)(processSimpleMutatingPattern)
        ))(PlanReads())

      case RemoveLabels(_, nodeName, _) =>
        PlanReads().withReferencedNodeVariable(nodeName)

      case SetLabels(_, nodeName, _) =>
        PlanReads().withReferencedNodeVariable(nodeName)

      case SetNodeProperties(_, nodeName, _) =>
        PlanReads().withReferencedNodeVariable(nodeName)

      case SetNodePropertiesFromMap(_, nodeName, _, _) =>
        PlanReads().withReferencedNodeVariable(nodeName)

      case SetNodeProperty(_, nodeName, _, _) =>
        PlanReads().withReferencedNodeVariable(nodeName)

      case SetRelationshipProperties(_, relName, _) =>
        PlanReads().withReferencedRelationshipVariable(relName)

      case SetRelationshipPropertiesFromMap(_, relName, _, _) =>
        PlanReads().withReferencedRelationshipVariable(relName)

      case SetRelationshipProperty(_, relName, _, _) =>
        PlanReads().withReferencedRelationshipVariable(relName)

      case _ => PlanReads()
    }

    def processDegreeRead(relTypeNames: Option[RelTypeName], planReads: PlanReads): PlanReads = {
      val newRelVariable = Variable(anonymousVariableNameGenerator.nextName)(InputPosition.NONE)
      val varRead = planReads.withIntroducedRelationshipVariable(newRelVariable)
      relTypeNames match {
        case Some(relTypeName) => varRead.withAddedRelationshipFilterExpression(
            newRelVariable,
            HasTypes(newRelVariable, Seq(relTypeName))(InputPosition.NONE)
          )
        case None => varRead
      }
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

      case v: Variable
        // If v could be a relationship
        if semanticTable.types.get(v).fold(true)(_.specified contains CTRelationship) =>
        acc => SkipChildren(acc.withReferencedRelationshipVariable(v))

      case Property(expr, propertyName) =>
        acc =>
          TraverseChildren(
            if (!semanticTable.isMapNoFail(expr)) {
              if (semanticTable.isRelationshipNoFail(expr))
                acc.withRelPropertyRead(propertyName)
              else if (semanticTable.isNodeNoFail(expr))
                acc.withNodePropertyRead(propertyName)
              else
                acc.withNodePropertyRead(propertyName).withRelPropertyRead(propertyName)
            } else
              acc
          )

      case GetDegree(_, relType, _) => acc =>
          TraverseChildren(processDegreeRead(relType, acc))

      case HasDegree(_, relType, _, _) => acc =>
          TraverseChildren(processDegreeRead(relType, acc))

      case HasDegreeGreaterThan(_, relType, _, _) => acc =>
          TraverseChildren(processDegreeRead(relType, acc))

      case HasDegreeGreaterThanOrEqual(_, relType, _, _) => acc =>
          TraverseChildren(processDegreeRead(relType, acc))

      case HasDegreeLessThan(_, relType, _, _) => acc =>
          TraverseChildren(processDegreeRead(relType, acc))

      case HasDegreeLessThanOrEqual(_, relType, _, _) => acc =>
          TraverseChildren(processDegreeRead(relType, acc))

      case f: FunctionInvocation if f.function == Labels =>
        acc => TraverseChildren(acc.withUnknownLabelsRead())

      case f: FunctionInvocation if f.function == Properties =>
        acc =>
          TraverseChildren(
            if (semanticTable.isRelationshipNoFail(f.args(0)))
              acc.withUnknownRelPropertiesRead()
            else if (semanticTable.isNodeNoFail(f.args(0)))
              acc.withUnknownNodePropertiesRead()
            else if (!semanticTable.isMapNoFail(f.args(0)))
              acc.withUnknownNodePropertiesRead().withUnknownRelPropertiesRead()
            else
              acc
          )

      case HasLabels(_, labels) =>
        acc => TraverseChildren(labels.foldLeft(acc)((acc, label) => acc.withLabelRead(label)))

      case HasALabel(Variable(_)) =>
        acc => TraverseChildren(acc.withUnknownLabelsRead())

      case HasLabelsOrTypes(_, labelsOrRels) =>
        acc =>
          TraverseChildren(labelsOrRels.foldLeft(acc)((acc, labelOrType) => acc.withLabelRead(labelOrType.asLabelName)))

      case ContainerIndex(expr, index) if !semanticTable.isIntegerNoFail(index) && !semanticTable.isMapNoFail(expr) =>
        // if we access by index, foo[0] or foo[&autoIntX] we must be accessing a list and hence we
        // are not accessing a property
        acc =>
          SkipChildren(acc.withUnknownNodePropertiesRead())

      case npe: NestedPlanExpression =>
        // A nested plan expression cannot have writes
        val nestedReads = collectReadsAndWrites(npe.plan, semanticTable, anonymousVariableNameGenerator).reads

        // Remap all reads to the outer plan, i.e. to the PlanReads currently being built
        val readProperties = nestedReads.readNodeProperties.plansReadingConcreteSymbol.keySet
        val readsUnknownProperties = nestedReads.readNodeProperties.plansReadingUnknownSymbols.nonEmpty
        val readLabels = nestedReads.readLabels.plansReadingConcreteSymbol.keySet
        val readsUnknownLabels = nestedReads.readLabels.plansReadingUnknownSymbols.nonEmpty
        val nodeFilterExpressions = nestedReads.nodeFilterExpressions.view.mapValues(_.expression)
        val referencedNodeVariables = nestedReads.possibleNodeDeleteConflictPlans.keySet
        val readRelProperties = nestedReads.readRelProperties.plansReadingConcreteSymbol.keySet
        val readsUnknownRelProperties = nestedReads.readRelProperties.plansReadingUnknownSymbols.nonEmpty
        val relationshipFilterExpressions = nestedReads.relationshipFilterExpressions.view.mapValues(_.expression)
        val referencedRelationshipVariables = nestedReads.possibleRelDeleteConflictPlans.keySet

        AssertMacros.checkOnlyWhenAssertionsAreEnabled(
          nestedReads.productIterator.toSeq == Seq(
            nestedReads.readNodeProperties,
            nestedReads.readLabels,
            nestedReads.nodeFilterExpressions,
            nestedReads.possibleNodeDeleteConflictPlans,
            nestedReads.readRelProperties,
            nestedReads.relationshipFilterExpressions,
            nestedReads.possibleRelDeleteConflictPlans
          ),
          "Make sure to edit this place when adding new fields to Reads"
        )

        acc => {
          val nextAcc = Function.chain[PlanReads](Seq(
            acc => readProperties.foldLeft(acc)(_.withNodePropertyRead(_)),
            acc => if (readsUnknownProperties) acc.withUnknownNodePropertiesRead() else acc,
            acc => readLabels.foldLeft(acc)(_.withLabelRead(_)),
            acc => if (readsUnknownLabels) acc.withUnknownLabelsRead() else acc,
            acc =>
              nodeFilterExpressions.foldLeft(acc) {
                case (acc, (variable, nfe)) => acc.withAddedNodeFilterExpression(variable, nfe)
              },
            acc => referencedNodeVariables.foldLeft(acc)(_.withReferencedNodeVariable(_)),
            acc => readRelProperties.foldLeft(acc)(_.withRelPropertyRead(_)),
            acc => if (readsUnknownRelProperties) acc.withUnknownRelPropertiesRead() else acc,
            acc =>
              relationshipFilterExpressions.foldLeft(acc) {
                case (acc, (variable, nfe)) => acc.withAddedRelationshipFilterExpression(variable, nfe)
              },
            acc => referencedRelationshipVariables.foldLeft(acc)(_.withReferencedRelationshipVariable(_))
          ))(acc)
          TraverseChildren(nextAcc)
        }
    }
  }

  private def processNodeIndexPlan(varName: String, labelName: String, properties: Seq[IndexedProperty]): PlanReads = {
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
          .withNodePropertyRead(propName)
          .withAddedNodeFilterExpression(variable, propPredicate)
    }
  }

  private def processSimpleMutatingPattern(
    acc: PlanReads,
    pattern: SimpleMutatingPattern
  ): PlanReads = {
    pattern match {
      case CreatePattern(nodes, rels) =>
        val createdNodes = nodes.map(_.idName)
        val createdRels = rels.map(_.idName)
        val referencedNodes = rels.flatMap(r => Seq(r.leftNode, r.rightNode))

        Function.chain[PlanReads](Seq(
          createdNodes.foldLeft(_)(_.withIntroducedNodeVariable(_)),
          createdRels.foldLeft(_)(_.withIntroducedRelationshipVariable(_)),
          referencedNodes.foldLeft(_)(_.withReferencedNodeVariable(_))
        ))(acc)

      case _: DeleteExpression =>
        acc

      case RemoveLabelPattern(nodeName, _) =>
        acc.withReferencedNodeVariable(nodeName)

      case SetLabelPattern(nodeName, _) =>
        acc.withReferencedNodeVariable(nodeName)

      case SetNodePropertiesPattern(nodeName, _) =>
        acc.withReferencedNodeVariable(nodeName)

      case SetNodePropertiesFromMapPattern(nodeName, _, _) =>
        acc.withReferencedNodeVariable(nodeName)

      case SetNodePropertyPattern(nodeName, _, _) =>
        acc.withReferencedNodeVariable(nodeName)

      case SetRelationshipPropertiesPattern(relName, _) =>
        acc.withReferencedRelationshipVariable(relName)

      case SetRelationshipPropertiesFromMapPattern(relName, _, _) =>
        acc.withReferencedRelationshipVariable(relName)

      case SetRelationshipPropertyPattern(relName, _, _) =>
        acc.withReferencedRelationshipVariable(relName)

      case _: SetPropertiesFromMapPattern => acc

      case _: SetPropertyPattern => acc

      case _: SetPropertiesPattern => acc
    }
  }

  private def processRelationshipIndexPlan(
    varName: String,
    relTypeName: String,
    properties: Seq[IndexedProperty],
    leftNode: String,
    rightNode: String
  ): PlanReads = {
    val leftVariable = Variable(leftNode)(InputPosition.NONE)
    val rightVariable = Variable(rightNode)(InputPosition.NONE)
    val variable = Variable(varName)(InputPosition.NONE)
    val tN = RelTypeName(relTypeName)(InputPosition.NONE)
    val hasType = HasTypes(variable, Seq(tN))(InputPosition.NONE)

    val r = PlanReads()
      .withIntroducedRelationshipVariable(variable)
      .withAddedRelationshipFilterExpression(variable, hasType)
      .withIntroducedNodeVariable(leftVariable)
      .withIntroducedNodeVariable(rightVariable)

    properties.foldLeft(r) {
      case (acc, IndexedProperty(PropertyKeyToken(property, _), _, _)) =>
        val propName = PropertyKeyName(property)(InputPosition.NONE)
        val propPredicate = IsNotNull(Property(variable, propName)(InputPosition.NONE))(InputPosition.NONE)

        acc
          .withRelPropertyRead(propName)
          .withAddedRelationshipFilterExpression(variable, propPredicate)
    }
  }

  private def processRelationshipRead(idName: String, leftNode: String, rightNode: String): PlanReads = {
    val leftVariable = Variable(leftNode)(InputPosition.NONE)
    val rightVariable = Variable(rightNode)(InputPosition.NONE)
    val relationshipVariable = Variable(idName)(InputPosition.NONE)
    PlanReads()
      .withIntroducedNodeVariable(leftVariable)
      .withIntroducedNodeVariable(rightVariable)
      .withIntroducedRelationshipVariable(relationshipVariable)
  }

  private def processUnionRelTypeScan(
    idName: String,
    leftNode: String,
    relTypes: Seq[RelTypeName],
    rightNode: String
  ): PlanReads = {
    val leftVariable = Variable(leftNode)(InputPosition.NONE)
    val rightVariable = Variable(rightNode)(InputPosition.NONE)
    val relationshipVariable = Variable(idName)(InputPosition.NONE)
    val predicates = relTypes.map { typeName =>
      HasTypes(relationshipVariable, Seq(typeName))(InputPosition.NONE)
    }
    val filterExpression = Ors(predicates)(InputPosition.NONE)
    PlanReads()
      .withIntroducedNodeVariable(leftVariable)
      .withIntroducedNodeVariable(rightVariable)
      .withIntroducedRelationshipVariable(relationshipVariable)
      .withAddedRelationshipFilterExpression(relationshipVariable, filterExpression)
  }

  private def processRelTypeRead(
    idName: String,
    leftNode: String,
    relType: RelTypeName,
    rightNode: String
  ): PlanReads = {
    val leftVariable = Variable(leftNode)(InputPosition.NONE)
    val rightVariable = Variable(rightNode)(InputPosition.NONE)
    val relationshipVariable = Variable(idName)(InputPosition.NONE)
    val hasTypes = HasTypes(relationshipVariable, Seq(relType))(InputPosition.NONE)
    PlanReads()
      .withIntroducedNodeVariable(leftVariable)
      .withIntroducedNodeVariable(rightVariable)
      .withIntroducedRelationshipVariable(relationshipVariable)
      .withAddedRelationshipFilterExpression(relationshipVariable, hasTypes)
  }

  private def processExpand(relTypes: Seq[RelTypeName], to: String, relName: String): PlanReads = {
    val nodeVariable = Variable(to)(InputPosition.NONE)
    val relationshipVariable = Variable(relName)(InputPosition.NONE)
    val predicates = relTypes.map { typeName =>
      HasTypes(relationshipVariable, Seq(typeName))(InputPosition.NONE)
    }
    val filterExpression = if (predicates.nonEmpty)
      Ors(predicates)(InputPosition.NONE)
    else
      HasTypes(relationshipVariable, Seq())(InputPosition.NONE)
    PlanReads()
      .withIntroducedNodeVariable(nodeVariable)
      .withIntroducedRelationshipVariable(relationshipVariable)
      .withAddedRelationshipFilterExpression(relationshipVariable, filterExpression)
  }

}
