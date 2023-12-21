/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
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
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.ShortestRelationshipPattern
import org.neo4j.cypher.internal.ir.SimpleMutatingPattern
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.AssertSameRelationship
import org.neo4j.cypher.internal.logical.plans.BFSPruningVarExpand
import org.neo4j.cypher.internal.logical.plans.BidirectionalRepeatTrail
import org.neo4j.cypher.internal.logical.plans.CacheProperties
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.CommandLogicalPlan
import org.neo4j.cypher.internal.logical.plans.ConditionalApply
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.DeleteNode
import org.neo4j.cypher.internal.logical.plans.DeletePath
import org.neo4j.cypher.internal.logical.plans.DeleteRelationship
import org.neo4j.cypher.internal.logical.plans.DetachDeleteExpression
import org.neo4j.cypher.internal.logical.plans.DetachDeleteNode
import org.neo4j.cypher.internal.logical.plans.DetachDeletePath
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
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.EmptyResult
import org.neo4j.cypher.internal.logical.plans.ErrorPlan
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLimit
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.Input
import org.neo4j.cypher.internal.logical.plans.IntersectionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LetAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSemiApply
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LoadCSV
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlanExtension
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlanExtension
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.NFA
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.NodeByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.OrderedUnion
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.PartialTop
import org.neo4j.cypher.internal.logical.plans.PartitionedAllNodesScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.PartitionedNodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.PartitionedNodeIndexScan
import org.neo4j.cypher.internal.logical.plans.PartitionedNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUnwindCollection
import org.neo4j.cypher.internal.logical.plans.PathPropagatingBFS
import org.neo4j.cypher.internal.logical.plans.PhysicalPlanningPlan
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.PruningVarExpand
import org.neo4j.cypher.internal.logical.plans.RelationshipCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.RepeatOptions
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.RunQueryAt
import org.neo4j.cypher.internal.logical.plans.SelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.SelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SemiApply
import org.neo4j.cypher.internal.logical.plans.SetLabels
import org.neo4j.cypher.internal.logical.plans.SetNodeProperties
import org.neo4j.cypher.internal.logical.plans.SetNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetNodeProperty
import org.neo4j.cypher.internal.logical.plans.SetProperties
import org.neo4j.cypher.internal.logical.plans.SetPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetProperty
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperties
import org.neo4j.cypher.internal.logical.plans.SetRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperty
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath.Mapping
import org.neo4j.cypher.internal.logical.plans.SubqueryForeach
import org.neo4j.cypher.internal.logical.plans.TestOnlyPlan
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.Top1WithTies
import org.neo4j.cypher.internal.logical.plans.Trail
import org.neo4j.cypher.internal.logical.plans.TransactionApply
import org.neo4j.cypher.internal.logical.plans.TransactionForeach
import org.neo4j.cypher.internal.logical.plans.TriadicSelection
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
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship

import scala.collection.immutable.ListSet

/**
 * Finds all reads for a single plan.
 */
object ReadFinder {

  case class AccessedProperty(property: PropertyKeyName, accessor: Option[LogicalVariable])

  case class AccessedLabel(label: LabelName, accessor: Option[LogicalVariable])

  /**
   * Reads of a single plan.
   * The Seqs may contain duplicates. These are filtered out later in [[ConflictFinder]].
   *
   * @param readNodeProperties              the read Node properties
   * @param unknownNodePropertiesAccessors  for each unknown Node properties access, e.g. by calling the `properties` function, the accessor variable, if available.
   * @param readLabels                      the read labels
   * @param unknownLabelAccessors           for each unknown Node label access, e.g. by calling the `labels` function, the accessor variable, if available.
   * @param nodeFilterExpressions           All node expressions that filter the rows, in a map with the dependency as key.
   *                                        This also tracks if a variable is introduced by this plan.
   *                                        If a variable is introduced by this plan, and no predicates are applied on that variable,
   *                                        it is still present as a key in this map with an empty sequence of filter expressions.
   * @param referencedNodeVariables         all referenced node variables
   * @param readRelProperties               the read Relationship properties
   * @param unknownRelPropertiesAccessors   for each unknown Relationship properties access, e.g. by calling the `properties` function, the accessor variable, if available.
   * @param relationshipFilterExpressions   All type expressions that filter the rows, in a map with the dependency as key.
   *                                        This also tracks if a variable is introduced by this plan.
   *                                        If a variable is introduced by this plan, and no predicates are applied on that variable,
   *                                        it is still present as a key in this map with an empty sequence of filter expressions.
   * @param referencedRelationshipVariables all referenced Relationship variables
   */
  private[eager] case class PlanReads(
    readNodeProperties: Seq[AccessedProperty] = Seq.empty,
    unknownNodePropertiesAccessors: Seq[Option[LogicalVariable]] = Seq.empty,
    readLabels: Seq[AccessedLabel] = Seq.empty,
    unknownLabelAccessors: Seq[Option[LogicalVariable]] = Seq.empty,
    nodeFilterExpressions: Map[LogicalVariable, Seq[Expression]] = Map.empty,
    readRelProperties: Seq[AccessedProperty] = Seq.empty,
    unknownRelPropertiesAccessors: Seq[Option[LogicalVariable]] = Seq.empty,
    relationshipFilterExpressions: Map[LogicalVariable, Seq[Expression]] = Map.empty,
    referencedNodeVariables: Set[LogicalVariable] = Set.empty,
    referencedRelationshipVariables: Set[LogicalVariable] = Set.empty,
    callInTx: Boolean = false
  ) {

    def withNodePropertyRead(accessedProperty: AccessedProperty): PlanReads = {
      copy(readNodeProperties = readNodeProperties :+ accessedProperty)
    }

    def withRelPropertyRead(accessedProperty: AccessedProperty): PlanReads = {
      copy(readRelProperties = readRelProperties :+ accessedProperty)
    }

    def withUnknownNodePropertiesRead(accessor: Option[LogicalVariable]): PlanReads =
      copy(unknownNodePropertiesAccessors = unknownNodePropertiesAccessors :+ accessor)

    def withUnknownRelPropertiesRead(accessor: Option[LogicalVariable]): PlanReads =
      copy(unknownRelPropertiesAccessors = unknownRelPropertiesAccessors :+ accessor)

    def withLabelRead(accessedLabel: AccessedLabel): PlanReads = {
      copy(readLabels = readLabels :+ accessedLabel)
    }

    def withCallInTx: PlanReads = {
      copy(callInTx = true)
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

    def withIntroducedRelationshipVariable(variable: LogicalVariable): PlanReads = {
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

    def withUnknownLabelsRead(variable: Option[LogicalVariable]): PlanReads =
      copy(unknownLabelAccessors = unknownLabelAccessors :+ variable)

    def withReferencedNodeVariable(variable: LogicalVariable): PlanReads =
      copy(referencedNodeVariables = referencedNodeVariables + variable)

    def withReferencedRelationshipVariable(variable: LogicalVariable): PlanReads =
      copy(referencedRelationshipVariables = referencedRelationshipVariables + variable)
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
      case AllNodesScan(varName, _) =>
        PlanReads()
          .withIntroducedNodeVariable(varName)
      case PartitionedAllNodesScan(varName, _) =>
        PlanReads()
          .withIntroducedNodeVariable(varName)

      case NodeByLabelScan(variable, labelName, _, _) =>
        val hasLabels = HasLabels(variable, Seq(labelName))(InputPosition.NONE)
        PlanReads()
          .withLabelRead(AccessedLabel(labelName, Some(variable)))
          .withIntroducedNodeVariable(variable)
          .withAddedNodeFilterExpression(variable, hasLabels)

      case PartitionedNodeByLabelScan(variable, labelName, _) =>
        val hasLabels = HasLabels(variable, Seq(labelName))(InputPosition.NONE)
        PlanReads()
          .withLabelRead(AccessedLabel(labelName, Some(variable)))
          .withIntroducedNodeVariable(variable)
          .withAddedNodeFilterExpression(variable, hasLabels)

      case UnionNodeByLabelsScan(variable, labelNames, _, _) =>
        val predicates = labelNames.map { labelName =>
          HasLabels(variable, Seq(labelName))(InputPosition.NONE)
        }
        val filterExpression = Ors(predicates)(InputPosition.NONE)
        val acc = PlanReads()
          .withIntroducedNodeVariable(variable)
          .withAddedNodeFilterExpression(variable, filterExpression)
        labelNames.foldLeft(acc) { (acc, labelName) =>
          acc.withLabelRead(AccessedLabel(labelName, Some(variable)))
        }

      case IntersectionNodeByLabelsScan(variable, labelNames, _, _) =>
        val acc = PlanReads()
          .withIntroducedNodeVariable(variable)
        labelNames.foldLeft(acc) { (acc, labelName) =>
          val hasLabels = HasLabels(variable, Seq(labelName))(InputPosition.NONE)
          acc.withLabelRead(AccessedLabel(labelName, Some(variable)))
            .withAddedNodeFilterExpression(variable, hasLabels)
        }

      case NodeCountFromCountStore(_, labelNames, _) =>
        val variable = Variable(anonymousVariableNameGenerator.nextName)(InputPosition.NONE)
        val acc = PlanReads()
          .withIntroducedNodeVariable(variable)
        labelNames.flatten.foldLeft(acc) { (acc, labelName) =>
          val hasLabels = HasLabels(variable, Seq(labelName))(InputPosition.NONE)
          acc.withLabelRead(AccessedLabel(labelName, None))
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
            acc.withLabelRead(AccessedLabel(labelName, None))
              .withAddedNodeFilterExpression(nodeVariable, hasLabels)
          },
          typeNames.foldLeft(_) { (acc, typeName) =>
            val hasTypes = HasTypes(relVariable, Seq(typeName))(InputPosition.NONE)
            acc.withAddedRelationshipFilterExpression(relVariable, hasTypes)
          }
        ))(acc)

      case NodeIndexScan(node, LabelToken(labelName, _), properties, _, _, _, _) =>
        processNodeIndexPlan(node, labelName, properties)

      case PartitionedNodeIndexScan(node, LabelToken(labelName, _), properties, _, _) =>
        processNodeIndexPlan(node, labelName, properties)

      case NodeIndexSeek(node, LabelToken(labelName, _), properties, _, _, _, _, _) =>
        processNodeIndexPlan(node, labelName, properties)

      case PartitionedNodeIndexSeek(node, LabelToken(labelName, _), properties, _, _, _) =>
        processNodeIndexPlan(node, labelName, properties)

      case NodeUniqueIndexSeek(node, LabelToken(labelName, _), properties, _, _, _, _) =>
        processNodeIndexPlan(node, labelName, properties)

      case NodeIndexContainsScan(
          node,
          LabelToken(labelName, _),
          property,
          _,
          _,
          _,
          _
        ) =>
        processNodeIndexPlan(node, labelName, Seq(property))

      case NodeIndexEndsWithScan(
          node,
          LabelToken(labelName, _),
          property,
          _,
          _,
          _,
          _
        ) =>
        processNodeIndexPlan(node, labelName, Seq(property))

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

      case Input(nodes, rels, vars, _) =>
        // An Input can introduce entities. These must be captured with
        // .withIntroducedNodeVariable / .withIntroducedRelationshipVariable
        // However, Input is always the left-most-leaf plan, and therefore stable and never part of a Conflict.
        // We only include this to be prepared for potential future refactorings
        Function.chain[PlanReads](Seq(
          nodes.foldLeft(_)(_.withIntroducedNodeVariable(_)),
          rels.foldLeft(_)(_.withIntroducedRelationshipVariable(_)),
          vars.foldLeft(_) { (acc, col) =>
            var res = acc
            if (semanticTable.typeFor(col).couldBe(CTNode)) {
              res = res.withIntroducedNodeVariable(col)
            }
            if (semanticTable.typeFor(col).couldBe(CTRelationship)) {
              res = res.withIntroducedRelationshipVariable(col)
            }
            res
          }
        ))(PlanReads())

      case UndirectedAllRelationshipsScan(relationship, leftNode, rightNode, _) =>
        processRelationshipRead(relationship, leftNode, rightNode)

      case DirectedAllRelationshipsScan(relationship, leftNode, rightNode, _) =>
        processRelationshipRead(relationship, leftNode, rightNode)

      case UndirectedRelationshipTypeScan(relationship, leftNode, relType, rightNode, _, _) =>
        processRelTypeRead(relationship, leftNode, relType, rightNode)

      case PartitionedUndirectedRelationshipTypeScan(relationship, leftNode, relType, rightNode, _) =>
        processRelTypeRead(relationship, leftNode, relType, rightNode)

      case PartitionedDirectedAllRelationshipsScan(relationship, leftNode, rightNode, _) =>
        processRelationshipRead(relationship, leftNode, rightNode)

      case PartitionedUndirectedAllRelationshipsScan(relationship, leftNode, rightNode, _) =>
        processRelationshipRead(relationship, leftNode, rightNode)

      case DirectedRelationshipTypeScan(relationship, leftNode, relType, rightNode, _, _) =>
        processRelTypeRead(relationship, leftNode, relType, rightNode)

      case PartitionedDirectedRelationshipTypeScan(relationship, leftNode, relType, rightNode, _) =>
        processRelTypeRead(relationship, leftNode, relType, rightNode)

      case DirectedRelationshipIndexScan(
          relationship,
          leftNode,
          rightNode,
          RelationshipTypeToken(typeName, _),
          properties,
          _,
          _,
          _
        ) =>
        processRelationshipIndexPlan(relationship, typeName, properties, leftNode, rightNode)

      case DirectedRelationshipIndexSeek(
          relationship,
          leftNode,
          rightNode,
          RelationshipTypeToken(typeName, _),
          properties,
          _,
          _,
          _,
          _
        ) =>
        processRelationshipIndexPlan(relationship, typeName, properties, leftNode, rightNode)

      case UndirectedRelationshipIndexScan(
          relationship,
          leftNode,
          rightNode,
          RelationshipTypeToken(typeName, _),
          properties,
          _,
          _,
          _
        ) =>
        processRelationshipIndexPlan(relationship, typeName, properties, leftNode, rightNode)

      case UndirectedRelationshipIndexSeek(
          relationship,
          leftNode,
          rightNode,
          RelationshipTypeToken(typeName, _),
          properties,
          _,
          _,
          _,
          _
        ) =>
        processRelationshipIndexPlan(relationship, typeName, properties, leftNode, rightNode)

      case UndirectedRelationshipUniqueIndexSeek(
          relationship,
          leftNode,
          rightNode,
          RelationshipTypeToken(typeName, _),
          properties,
          _,
          _,
          _,
          _
        ) =>
        processRelationshipIndexPlan(relationship, typeName, properties, leftNode, rightNode)

      case DirectedRelationshipUniqueIndexSeek(
          relationship,
          leftNode,
          rightNode,
          RelationshipTypeToken(typeName, _),
          properties,
          _,
          _,
          _,
          _
        ) =>
        processRelationshipIndexPlan(relationship, typeName, properties, leftNode, rightNode)

      case UndirectedRelationshipIndexContainsScan(
          relationship,
          leftNode,
          rightNode,
          RelationshipTypeToken(typeName, _),
          property,
          _,
          _,
          _,
          _
        ) =>
        processRelationshipIndexPlan(relationship, typeName, Seq(property), leftNode, rightNode)

      case DirectedRelationshipIndexContainsScan(
          relationship,
          leftNode,
          rightNode,
          RelationshipTypeToken(typeName, _),
          property,
          _,
          _,
          _,
          _
        ) =>
        processRelationshipIndexPlan(relationship, typeName, Seq(property), leftNode, rightNode)

      case UndirectedRelationshipIndexEndsWithScan(
          relationship,
          leftNode,
          rightNode,
          RelationshipTypeToken(typeName, _),
          property,
          _,
          _,
          _,
          _
        ) =>
        processRelationshipIndexPlan(relationship, typeName, Seq(property), leftNode, rightNode)

      case DirectedRelationshipIndexEndsWithScan(
          relationship,
          leftNode,
          rightNode,
          RelationshipTypeToken(typeName, _),
          property,
          _,
          _,
          _,
          _
        ) =>
        processRelationshipIndexPlan(relationship, typeName, Seq(property), leftNode, rightNode)

      case UndirectedRelationshipByIdSeek(relationship, _, leftNode, rightNode, _) =>
        processRelationshipRead(relationship, leftNode, rightNode)

      case DirectedRelationshipByIdSeek(relationship, _, leftNode, rightNode, _) =>
        processRelationshipRead(relationship, leftNode, rightNode)

      case UndirectedRelationshipByElementIdSeek(relationship, _, leftNode, rightNode, _) =>
        processRelationshipRead(relationship, leftNode, rightNode)

      case DirectedRelationshipByElementIdSeek(relationship, _, leftNode, rightNode, _) =>
        processRelationshipRead(relationship, leftNode, rightNode)

      case UndirectedUnionRelationshipTypesScan(relationship, leftNode, relTypes, rightNode, _, _) =>
        processUnionRelTypeScan(relationship, leftNode, relTypes, rightNode)

      case DirectedUnionRelationshipTypesScan(relationship, leftNode, relTypes, rightNode, _, _) =>
        processUnionRelTypeScan(relationship, leftNode, relTypes, rightNode)

      case Selection(predicate, _) =>
        processFilterExpression(PlanReads(), predicate, semanticTable)

      case Expand(_, _, _, relTypes, to, relName, mode) =>
        processExpand(relTypes, to, relName, mode)

      case OptionalExpand(_, _, _, relTypes, to, relName, mode, _) =>
        processExpand(relTypes, to, relName, mode)

      case VarExpand(_, _, _, _, relTypes, to, relName, _, mode, _, _) =>
        // Note: nodePredicates and relPredicates are matched further down already, since
        //  they are VariablePredicates.
        // relName is actually a List of relationships but we can consider it to be a Relationship Variable when doing eagerness analysis
        processExpand(relTypes, to, relName, mode)

      case PruningVarExpand(_, _, _, relTypes, to, _, _, _, _) =>
        // Note: nodePredicates and relPredicates are matched further down already, since
        //  they are VariablePredicates.
        // PruningVarExpand does not introduce a rel variable, but we need one to attach the predicates to.
        processExpand(
          relTypes,
          to,
          Variable(anonymousVariableNameGenerator.nextName)(InputPosition.NONE),
          Expand.ExpandAll
        )

      case BFSPruningVarExpand(_, _, _, relTypes, to, _, _, _, mode, _, _) =>
        // Note: nodePredicates and relPredicates are matched further down already, since
        //  they are VariablePredicates.
        // bfsPruningVarExpand does not introduce a rel variable, but we need one to attach the predicates to.
        processExpand(
          relTypes,
          to,
          Variable(anonymousVariableNameGenerator.nextName)(InputPosition.NONE),
          mode
        )

      case PathPropagatingBFS(_, _, _, _, _, relTypes, to, relName, _, _, _) =>
        // Note: nodePredicates and relPredicates are matched further down already, since
        //  they are VariablePredicates.
        // relName is actually a List of relationships but we can consider it to be a Relationship Variable when doing eagerness analysis
        processExpand(relTypes, to, relName, Expand.ExpandAll)

      case FindShortestPaths(
          _,
          ShortestRelationshipPattern(_, PatternRelationship(relationship, _, _, types, _), _),
          _,
          _,
          _,
          _,
          _
        ) =>
        // Note: pathPredicates is on a path - so not on an entity - therefore no need to process it.
        // Note: perStepNodePredicates and perStepRelPredicates are matched further down already, since
        //  they are VariablePredicates.
        processShortestPaths(relationship, types)

      case StatefulShortestPath(
          _,
          sourceNode,
          targetNode,
          nfa,
          mode,
          nonInlinedPreFilters,
          _,
          _,
          singletonNodeVariables,
          singletonRelationshipVariables,
          _,
          _,
          _
        ) =>
        processStatefulShortest(
          sourceNode,
          targetNode,
          nfa,
          mode,
          nonInlinedPreFilters,
          singletonNodeVariables,
          singletonRelationshipVariables,
          semanticTable
        )

      case ProduceResult(_, columns) =>
        // A ProduceResult can reference entities. These must be captured with
        // .withReferencesNodeVariable / .withReferencedRelationshipVariable
        // However, this is only used to place an Eager correctly between a read and a later Delete.
        // Since, there can be no Deletes after a ProduceResult, this information is currently not used.
        columns.foldLeft(PlanReads()) { (acc, col) =>
          var res = acc
          if (semanticTable.typeFor(col).couldBe(CTNode)) {
            res = res
              .withUnknownNodePropertiesRead(Some(col))
              .withUnknownLabelsRead(Some(col))
          }
          if (semanticTable.typeFor(col).couldBe(CTRelationship)) {
            res = res
              .withUnknownRelPropertiesRead(Some(col))
          }
          res
        }

      case ProjectEndpoints(_, rel, start, startInScope, end, endInScope, types, _, _) =>
        Function.chain[PlanReads](Seq(
          if (startInScope) identity else _.withIntroducedNodeVariable(start),
          if (endInScope) identity else _.withIntroducedNodeVariable(end),
          // rel could even be a List[Relationship]
          if (semanticTable.typeFor(rel).couldBe(CTRelationship)) _.withReferencedRelationshipVariable(rel)
          else identity,
          if (types.nonEmpty) {
            _.withAddedRelationshipFilterExpression(rel, relTypeNamesToOrs(rel, types))
          } else identity
        ))(PlanReads())

      // Projection.projectExpressions could actually contain introductions of entity variables.
      // But, these will always refer to variables that have been "found" before,
      // Thus no new entity is really introduced. Using `withIntroducedNodeVariable`
      // would lead to unnecessary Eagers. See test
      // "inserts no eager between create and stable AllNodeScan + Projection"
      // The same argument also holds for other plans:
      //  * Argument
      //  * Unwind
      //  * [Ordered]Aggregation
      //  * [Ordered]Distinct
      case Projection(_, projectExpressions) =>
        projectExpressions.foldLeft(PlanReads()) {

          // Don't introduce new variables for aliases: WITH a AS b
          case (acc, _ -> (_: Variable)) =>
            acc

          case (acc, v -> nonVariableExpression) =>
            var res = acc

            // Check both expressions to avoid introducing new variables for projections like:
            // WITH n.prop AS `n.prop`
            // If there's no entry in the type table for `n.prop`, we assume it _could_ be a node, but at the same time
            // we know that n.prop cannot be a node.
            val couldBeNode =
              semanticTable.typeFor(v).couldBe(CTNode) &&
                semanticTable.typeFor(nonVariableExpression).couldBe(CTNode)
            if (couldBeNode) {
              res = res.withIntroducedNodeVariable(v)
            }

            val couldBeRel =
              semanticTable.typeFor(v).couldBe(CTRelationship) &&
                semanticTable.typeFor(nonVariableExpression).couldBe(CTRelationship)
            if (couldBeRel) {
              res = res.withIntroducedRelationshipVariable(v)
            }

            res
        }

      case c: Create =>
        val nodes = c.nodes
        val rels = c.relationships
        val createdNodes = nodes.map(_.variable)
        val createdRels = rels.map(_.variable)
        val referencedNodes = rels.flatMap(r => Seq(r.leftNode, r.rightNode))

        Function.chain[PlanReads](Seq(
          createdNodes.foldLeft(_)(_.withIntroducedNodeVariable(_)),
          createdRels.foldLeft(_)(_.withIntroducedRelationshipVariable(_)),
          referencedNodes.foldLeft(_)(_.withReferencedNodeVariable(_))
        ))(PlanReads())

      case Foreach(_, _, _, mutations) =>
        mutations.foldLeft(PlanReads())(processSimpleMutatingPattern)

      case Merge(_, nodes, rels, onMatch, onCreate, _) =>
        val createdNodes = nodes.map(_.variable)
        val createdRels = rels.map(_.variable)
        val referencedNodes = rels.flatMap(r => Seq(r.leftNode, r.rightNode))

        Function.chain[PlanReads](Seq(
          createdNodes.foldLeft(_)(_.withIntroducedNodeVariable(_)),
          createdRels.foldLeft(_)(_.withIntroducedRelationshipVariable(_)),
          referencedNodes.foldLeft(_)(_.withReferencedNodeVariable(_)),
          onMatch.foldLeft(_)(processSimpleMutatingPattern),
          onCreate.foldLeft(_)(processSimpleMutatingPattern)
        ))(PlanReads())

      case TransactionApply(_, _, _, _, _) =>
        PlanReads().withCallInTx

      case TransactionForeach(_, _, _, _, _) =>
        PlanReads().withCallInTx

      case Trail(_, _, _, _, end, _, _, _, _, _, _, _, _) =>
        PlanReads().withIntroducedNodeVariable(end)

      case BidirectionalRepeatTrail(_, _, _, _, _, _, _, _, _, _, _, _, _) |
        RepeatOptions(_, _) =>
        throw new IllegalStateException(s"Unsupported plan in eagerness analysis: $plan")

      /*
        Be careful when adding something to this fall-through case.
        Any (new) plan that performs reads of nodes, relationships, labels, properties or types should not be in this list.
        Pay special attention to leaf plans and plans that reference nodes/relationship by name (a String) instead of through a Variable.
        When in doubt, ask the planner team.
       */
      case Aggregation(_, _, _) |
        AntiSemiApply(_, _) |
        Apply(_, _) |
        CacheProperties(_, _) |
        CartesianProduct(_, _) |
        plans.DeleteExpression(_, _) |
        DeleteNode(_, _) |
        DeletePath(_, _) |
        DeleteRelationship(_, _) |
        DetachDeleteExpression(_, _) |
        DetachDeleteNode(_, _) |
        DetachDeletePath(_, _) |
        Distinct(_, _) |
        EmptyResult(_) |
        ErrorPlan(_, _) |
        ExhaustiveLimit(_, _) |
        ForeachApply(_, _, _, _) |
        LetAntiSemiApply(_, _, _) |
        LetSelectOrAntiSemiApply(_, _, _, _) |
        LetSelectOrSemiApply(_, _, _, _) |
        LetSemiApply(_, _, _) |
        Limit(_, _) |
        LoadCSV(_, _, _, _, _, _, _) |
        OrderedAggregation(_, _, _, _) |
        OrderedDistinct(_, _, _) |
        ProcedureCall(_, _) |
        SelectOrAntiSemiApply(_, _, _) |
        SelectOrSemiApply(_, _, _) |
        SemiApply(_, _) |
        SetProperties(_, _, _) |
        SetPropertiesFromMap(_, _, _, _) |
        SetProperty(_, _, _, _) |
        Skip(_, _) |
        SubqueryForeach(_, _) |
        Union(_, _) |
        UnwindCollection(_, _, _) |
        PartitionedUnwindCollection(_, _, _) |
        ValueHashJoin(_, _, _) |
        Sort(_, _) |
        PartialSort(_, _, _, _) |
        Top(_, _, _) |
        Top1WithTies(_, _) |
        PartialTop(_, _, _, _, _) |
        OrderedUnion(_, _, _) |
        ConditionalApply(_, _, _) |
        AntiConditionalApply(_, _, _) |
        RollUpApply(_, _, _, _) |
        AssertSameNode(_, _, _) |
        AssertSameRelationship(_, _, _) |
        Optional(_, _) |
        NodeHashJoin(_, _, _) |
        LeftOuterHashJoin(_, _, _) |
        RightOuterHashJoin(_, _, _) |
        TriadicSelection(_, _, _, _, _, _) |
        RemoveLabels(_, _, _) |
        RunQueryAt(_, _, _, _, _) |
        SetLabels(_, _, _) |
        SetNodeProperties(_, _, _) |
        SetNodePropertiesFromMap(_, _, _, _) |
        SetNodeProperty(_, _, _, _) |
        SetRelationshipProperties(_, _, _) |
        SetRelationshipPropertiesFromMap(_, _, _, _) |
        SetRelationshipProperty(_, _, _, _) =>
        PlanReads()

      case _: PhysicalPlanningPlan |
        _: CommandLogicalPlan |
        _: LogicalLeafPlanExtension |
        _: LogicalPlanExtension |
        _: TestOnlyPlan |
        _: Eager =>
        throw new IllegalStateException(s"Unsupported plan in eagerness analysis: $plan")
    }

    val rewrittenPlan = plan match {
      case ssp: StatefulShortestPath =>
        // Predicates on singleton entities have been rewritten to use new variable name in the NFA,
        // because they use expressions variables. For Eagerness purposes, however, we map all variable
        // names back to the original. We would otherwise interpret this as distinct entities and that can
        // lead to unnecessary Eagers.
        val rewriteLookup = (ssp.singletonNodeVariables ++ ssp.singletonRelationshipVariables)
          .map(mapping => mapping.nfaExprVar -> mapping.rowVar)
          .toMap

        plan.endoRewrite(bottomUp(Rewriter.lift {
          case variable: LogicalVariable => rewriteLookup.getOrElse(variable, variable)
        }))
      case _ => plan
    }

    // Match on expressions
    rewrittenPlan.folder.treeFold(planReads) {
      case otherPlan: LogicalPlan if otherPlan.id != plan.id =>
        // Do not traverse the logical plan tree! We are only looking at expressions of the given plan
        acc => SkipChildren(acc)

      case v: Variable => acc =>
          var res = acc
          if (semanticTable.typeFor(v).couldBe(CTNode)) {
            res = res.withReferencedNodeVariable(v)
          }
          if (semanticTable.typeFor(v).couldBe(CTRelationship)) {
            res = res.withReferencedRelationshipVariable(v)
          }
          SkipChildren(res)

      case Property(expr, propertyName) =>
        acc =>
          var result = acc
          val typeGetter = semanticTable.typeFor(expr)
          if (typeGetter.couldBe(CTRelationship)) {
            result = result.withRelPropertyRead(AccessedProperty(propertyName, asMaybeVar(expr)))
          }
          if (typeGetter.couldBe(CTNode)) {
            result = result.withNodePropertyRead(AccessedProperty(propertyName, asMaybeVar(expr)))
          }
          TraverseChildren(result)

      case GetDegree(_, relType, _) => acc =>
          TraverseChildren(processDegreeRead(relType, acc, anonymousVariableNameGenerator))

      case HasDegree(_, relType, _, _) => acc =>
          TraverseChildren(processDegreeRead(relType, acc, anonymousVariableNameGenerator))

      case HasDegreeGreaterThan(_, relType, _, _) => acc =>
          TraverseChildren(processDegreeRead(relType, acc, anonymousVariableNameGenerator))

      case HasDegreeGreaterThanOrEqual(_, relType, _, _) => acc =>
          TraverseChildren(processDegreeRead(relType, acc, anonymousVariableNameGenerator))

      case HasDegreeLessThan(_, relType, _, _) => acc =>
          TraverseChildren(processDegreeRead(relType, acc, anonymousVariableNameGenerator))

      case HasDegreeLessThanOrEqual(_, relType, _, _) => acc =>
          TraverseChildren(processDegreeRead(relType, acc, anonymousVariableNameGenerator))

      case f: FunctionInvocation if f.function == Labels =>
        acc =>
          TraverseChildren(acc.withUnknownLabelsRead(asMaybeVar(f.args.head)))

      case f: FunctionInvocation if f.function == Properties =>
        acc =>
          var result = acc
          val typeGetter = semanticTable.typeFor(f.args(0))
          if (typeGetter.couldBe(CTRelationship)) {
            result = result.withUnknownRelPropertiesRead(asMaybeVar(f.args.head))
          }
          if (typeGetter.couldBe(CTNode)) {
            result = result.withUnknownNodePropertiesRead(asMaybeVar(f.args.head))
          }
          TraverseChildren(result)

      case HasLabels(expr, labels) =>
        acc =>
          TraverseChildren(labels.foldLeft(acc)((acc, label) =>
            acc.withLabelRead(AccessedLabel(label, asMaybeVar(expr)))
          ))

      case HasALabel(v: Variable) =>
        acc => TraverseChildren(acc.withUnknownLabelsRead(Some(v)))

      case HasLabelsOrTypes(expr, labelsOrRels) =>
        acc =>
          TraverseChildren(labelsOrRels.foldLeft(acc)((acc, labelOrType) =>
            acc.withLabelRead(AccessedLabel(labelOrType.asLabelName, asMaybeVar(expr)))
          ))

      case ContainerIndex(expr, index)
        if !semanticTable.typeFor(index).is(CTInteger) && !semanticTable.typeFor(expr).is(CTMap) =>
        // if we access by index, foo[0] or foo[&autoIntX] we must be accessing a list and hence we
        // are not accessing a property
        acc =>
          SkipChildren(acc.withUnknownNodePropertiesRead(asMaybeVar(expr)))

      case VariablePredicate(v, pred) => acc =>
          val canBeNode = semanticTable.typeFor(v).couldBe(CTNode)
          val canBeRel = semanticTable.typeFor(v).couldBe(CTRelationship)
          var nextAcc = acc
          if (canBeNode) {
            nextAcc = nextAcc.withAddedNodeFilterExpression(v, pred)
          }
          if (canBeRel) {
            nextAcc = nextAcc.withAddedRelationshipFilterExpression(v, pred)
          }
          TraverseChildren(nextAcc)

      case npe: NestedPlanExpression =>
        // A nested plan expression cannot have writes
        val nestedReads = collectReadsAndWrites(npe.plan, semanticTable, anonymousVariableNameGenerator).reads

        // Remap all reads to the outer plan, i.e. to the PlanReads currently being built
        val readNodeProperties = nestedReads.readNodeProperties.plansReadingConcreteSymbol.view
          .mapValues(_.map(_.accessor))
          .flatMap { case (p, vars) => vars.map(AccessedProperty(p, _)) }
        val unknownNodePropertiesAccessors = nestedReads.readNodeProperties.plansReadingUnknownSymbols.map(_.accessor)
        val readLabels = nestedReads.readLabels.plansReadingConcreteSymbol.view
          .mapValues(_.map(_.accessor))
          .flatMap { case (p, vars) => vars.map(AccessedLabel(p, _)) }
        val unknownLabelsAccessors = nestedReads.readLabels.plansReadingUnknownSymbols.map(_.accessor)
        val nodeFilterExpressions = nestedReads.nodeFilterExpressions.view.mapValues(_.expression)
        val referencedNodeVariables = nestedReads.possibleNodeDeleteConflictPlans.keySet
        val readRelProperties = nestedReads.readRelProperties.plansReadingConcreteSymbol.view
          .mapValues(_.map(_.accessor))
          .flatMap { case (p, vars) => vars.map(AccessedProperty(p, _)) }
        val unknownRelPropertiesAccessors = nestedReads.readRelProperties.plansReadingUnknownSymbols.map(_.accessor)
        val relationshipFilterExpressions = nestedReads.relationshipFilterExpressions.view.mapValues(_.expression)
        val referencedRelationshipVariables = nestedReads.possibleRelDeleteConflictPlans.keySet
        val callInTx = nestedReads.callInTxPlans.nonEmpty

        AssertMacros.checkOnlyWhenAssertionsAreEnabled(
          nestedReads.productIterator.toSeq == Seq(
            nestedReads.readNodeProperties,
            nestedReads.readLabels,
            nestedReads.nodeFilterExpressions,
            nestedReads.possibleNodeDeleteConflictPlans,
            nestedReads.readRelProperties,
            nestedReads.relationshipFilterExpressions,
            nestedReads.possibleRelDeleteConflictPlans,
            nestedReads.callInTxPlans
          ),
          "Make sure to edit this place when adding new fields to Reads"
        )

        acc => {
          val nextAcc = Function.chain[PlanReads](Seq(
            acc => readNodeProperties.foldLeft(acc)(_.withNodePropertyRead(_)),
            acc => unknownNodePropertiesAccessors.foldLeft(acc)(_.withUnknownNodePropertiesRead(_)),
            acc => readLabels.foldLeft(acc)(_.withLabelRead(_)),
            acc => unknownLabelsAccessors.foldLeft(acc)(_.withUnknownLabelsRead(_)),
            acc =>
              nodeFilterExpressions.foldLeft(acc) {
                case (acc, (variable, nfe)) => acc.withAddedNodeFilterExpression(variable, nfe)
              },
            acc => referencedNodeVariables.foldLeft(acc)(_.withReferencedNodeVariable(_)),
            acc => readRelProperties.foldLeft(acc)(_.withRelPropertyRead(_)),
            acc => unknownRelPropertiesAccessors.foldLeft(acc)(_.withUnknownRelPropertiesRead(_)),
            acc =>
              relationshipFilterExpressions.foldLeft(acc) {
                case (acc, (variable, nfe)) => acc.withAddedRelationshipFilterExpression(variable, nfe)
              },
            acc => referencedRelationshipVariables.foldLeft(acc)(_.withReferencedRelationshipVariable(_)),
            acc => if (callInTx) acc.withCallInTx else acc
          ))(acc)
          TraverseChildren(nextAcc)
        }
    }
  }

  private def processDegreeRead(
    relTypeNames: Option[RelTypeName],
    planReads: PlanReads,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): PlanReads = {
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

  private def processFilterExpression(acc: PlanReads, expression: Expression, semanticTable: SemanticTable): PlanReads =
    expression match {
      case Ands(expressions) => expressions.foldLeft(acc)(processFilterExpression(_, _, semanticTable))
      case _                 =>
        // Using `.is(CTNode) we can get unnecessary Eagers if we lack type information for a node
        // and adding the predicate would have allowed to disqualify a conflict.
        // But if we were to use `.couldBe(CTNode)`, we would get unnecessary Eagers for many
        // cases where type information is missing from the selection, but not from the actual introduction of a variable,
        // e.g. AssertIsNode(var)
        val nodeExpr = expression.dependencies.filter(semanticTable.typeFor(_).is(CTNode))
        val relExpr = expression.dependencies.filter(semanticTable.typeFor(_).is(CTRelationship))

        Function.chain[PlanReads](Seq(
          nodeExpr.foldLeft(_)(_.withAddedNodeFilterExpression(_, expression)),
          relExpr.foldLeft(_)(_.withAddedRelationshipFilterExpression(_, expression))
        ))(acc)
    }

  private def processStatefulShortest(
    sourceNode: LogicalVariable,
    targetNode: LogicalVariable,
    nfa: NFA,
    mode: Expand.ExpansionMode,
    nonInlinedPreFilters: Option[Expression],
    singletonNodeVariables: Set[Mapping],
    singletonRelationshipVariables: Set[Mapping],
    semanticTable: SemanticTable
  ): PlanReads = {

    val initialRead = PlanReads()
      .withReferencedNodeVariable(sourceNode)

    val alreadyBound = mode match {
      case Expand.ExpandAll  => Set(sourceNode)
      case Expand.ExpandInto => Set(sourceNode, targetNode)
    }

    Function.chain[PlanReads](Seq(
      mode match {
        case Expand.ExpandAll  => _.withIntroducedNodeVariable(targetNode)
        case Expand.ExpandInto => _.withReferencedNodeVariable(targetNode)
      },

      // We cannot find the variableGroupings since they might have been removed in RemoveUnusedGroupVariablesRewriter
      (nfa.nodes -- alreadyBound -- singletonNodeVariables.map(_.nfaExprVar)).foldLeft(_) { (acc, nodeName) =>
        acc.withIntroducedNodeVariable(nodeName)
      },
      (nfa.relationships -- singletonRelationshipVariables.map(_.nfaExprVar)).foldLeft(_) { (acc, relName) =>
        acc.withIntroducedRelationshipVariable(relName)
      },
      singletonNodeVariables.foldLeft(_) { (acc, mapping) =>
        acc.withIntroducedNodeVariable(mapping.rowVar)
      },
      singletonRelationshipVariables.foldLeft(_) { (acc, mapping) =>
        acc.withIntroducedRelationshipVariable(mapping.rowVar)
      },
      nonInlinedPreFilters.foldLeft(_)(processFilterExpression(_, _, semanticTable))
    ))(initialRead)
  }

  private def processShortestPaths(
    relVariable: LogicalVariable,
    types: Seq[RelTypeName]
  ): PlanReads = {
    val hasRelTypes = HasTypes(relVariable, types)(InputPosition.NONE)
    PlanReads()
      // relVariable is actually a List of relationships but we can consider it to be a Relationship Variable when doing eagerness analysis
      .withIntroducedRelationshipVariable(relVariable)
      .withAddedRelationshipFilterExpression(relVariable, hasRelTypes)
  }

  private def processNodeIndexPlan(
    variable: LogicalVariable,
    labelName: String,
    properties: Seq[IndexedProperty]
  ): PlanReads = {
    val lN = LabelName(labelName)(InputPosition.NONE)
    val hasLabels = HasLabels(variable, Seq(lN))(InputPosition.NONE)

    val r = PlanReads()
      .withLabelRead(AccessedLabel(lN, Some(variable)))
      .withIntroducedNodeVariable(variable)
      .withAddedNodeFilterExpression(variable, hasLabels)

    properties.foldLeft(r) {
      case (acc, IndexedProperty(PropertyKeyToken(property, _), _, _)) =>
        val propName = PropertyKeyName(property)(InputPosition.NONE)
        val propPredicate = IsNotNull(Property(variable, propName)(InputPosition.NONE))(InputPosition.NONE)

        acc
          .withNodePropertyRead(AccessedProperty(propName, Some(variable)))
          .withAddedNodeFilterExpression(variable, propPredicate)
    }
  }

  private def processSimpleMutatingPattern(
    acc: PlanReads,
    pattern: SimpleMutatingPattern
  ): PlanReads = {
    pattern match {
      case c: CreatePattern =>
        val nodes = c.nodes
        val rels = c.relationships
        val createdNodes = nodes.map(_.variable)
        val createdRels = rels.map(_.variable)
        val referencedNodes = rels.flatMap(r => Seq(r.leftNode, r.rightNode))

        Function.chain[PlanReads](Seq(
          createdNodes.foldLeft(_)(_.withIntroducedNodeVariable(_)),
          createdRels.foldLeft(_)(_.withIntroducedRelationshipVariable(_)),
          referencedNodes.foldLeft(_)(_.withReferencedNodeVariable(_))
        ))(acc)

      case _ => acc
    }
  }

  private def processRelationshipIndexPlan(
    relationship: LogicalVariable,
    relTypeName: String,
    properties: Seq[IndexedProperty],
    leftNode: LogicalVariable,
    rightNode: LogicalVariable
  ): PlanReads = {
    val tN = RelTypeName(relTypeName)(InputPosition.NONE)
    val hasType = HasTypes(relationship, Seq(tN))(InputPosition.NONE)

    val r = PlanReads()
      .withIntroducedRelationshipVariable(relationship)
      .withAddedRelationshipFilterExpression(relationship, hasType)
      .withIntroducedNodeVariable(leftNode)
      .withIntroducedNodeVariable(rightNode)

    properties.foldLeft(r) {
      case (acc, IndexedProperty(PropertyKeyToken(property, _), _, _)) =>
        val propName = PropertyKeyName(property)(InputPosition.NONE)
        val propPredicate = IsNotNull(Property(relationship, propName)(InputPosition.NONE))(InputPosition.NONE)

        acc
          .withRelPropertyRead(AccessedProperty(propName, Some(relationship)))
          .withAddedRelationshipFilterExpression(relationship, propPredicate)
    }
  }

  private def processRelationshipRead(
    relationshipVariable: LogicalVariable,
    leftVariable: LogicalVariable,
    rightVariable: LogicalVariable
  ): PlanReads = {
    PlanReads()
      .withIntroducedNodeVariable(leftVariable)
      .withIntroducedNodeVariable(rightVariable)
      .withIntroducedRelationshipVariable(relationshipVariable)
  }

  private def processUnionRelTypeScan(
    relationship: LogicalVariable,
    leftNode: LogicalVariable,
    relTypes: Seq[RelTypeName],
    rightNode: LogicalVariable
  ): PlanReads = {
    val filterExpression = relTypeNamesToOrs(relationship, relTypes)
    PlanReads()
      .withIntroducedNodeVariable(leftNode)
      .withIntroducedNodeVariable(rightNode)
      .withIntroducedRelationshipVariable(relationship)
      .withAddedRelationshipFilterExpression(relationship, filterExpression)
  }

  private def processRelTypeRead(
    relationship: LogicalVariable,
    leftNode: LogicalVariable,
    relType: RelTypeName,
    rightNode: LogicalVariable
  ): PlanReads = {
    val hasTypes = HasTypes(relationship, Seq(relType))(InputPosition.NONE)
    PlanReads()
      .withIntroducedNodeVariable(leftNode)
      .withIntroducedNodeVariable(rightNode)
      .withIntroducedRelationshipVariable(relationship)
      .withAddedRelationshipFilterExpression(relationship, hasTypes)
  }

  private def processExpand(
    relTypes: Seq[RelTypeName],
    to: LogicalVariable,
    relationshipVariable: LogicalVariable,
    mode: Expand.ExpansionMode
  ): PlanReads = {
    Function.chain[PlanReads](Seq(
      mode match {
        case Expand.ExpandAll  => _.withIntroducedNodeVariable(to)
        case Expand.ExpandInto => _.withReferencedNodeVariable(to)
      },
      _.withIntroducedRelationshipVariable(relationshipVariable),
      if (relTypes.isEmpty) {
        identity
      } else {
        val filterExpression = relTypeNamesToOrs(relationshipVariable, relTypes)
        _.withAddedRelationshipFilterExpression(relationshipVariable, filterExpression)
      }
    ))(PlanReads())
  }

  /**
   * @param relTypes must not be empty
   */
  private def relTypeNamesToOrs(relationshipVariable: LogicalVariable, relTypes: Seq[RelTypeName]): Expression = {
    val predicates = relTypes.map { typeName =>
      HasTypes(relationshipVariable, Seq(typeName))(InputPosition.NONE)
    }
    Ors.create(ListSet.from(predicates))
  }

  def asMaybeVar(expr: Expression): Option[LogicalVariable] = {
    expr match {
      case lv: LogicalVariable => Some(lv)
      case _                   => None
    }
  }
}
