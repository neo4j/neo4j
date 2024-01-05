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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.ExecutableBy
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.ElementTypeName
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.NameToken
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.UnsignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.expressions.functions.Point
import org.neo4j.cypher.internal.expressions.functions.Type
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreatePattern
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.PatternLength
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
import org.neo4j.cypher.internal.ir.ShortestRelationshipPattern
import org.neo4j.cypher.internal.ir.SimpleMutatingPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.AdministrationCommandLogicalPlan
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Anti
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.ArgumentTracker
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.AssertSameRelationship
import org.neo4j.cypher.internal.logical.plans.AssertingMultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.AssertingMultiRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.BFSPruningVarExpand
import org.neo4j.cypher.internal.logical.plans.BidirectionalRepeatTrail
import org.neo4j.cypher.internal.logical.plans.Bound
import org.neo4j.cypher.internal.logical.plans.CacheProperties
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.ColumnOrder
import org.neo4j.cypher.internal.logical.plans.CompositeQueryExpression
import org.neo4j.cypher.internal.logical.plans.ConditionalApply
import org.neo4j.cypher.internal.logical.plans.ConstraintType
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.CreateConstraint
import org.neo4j.cypher.internal.logical.plans.CreateFulltextIndex
import org.neo4j.cypher.internal.logical.plans.CreateIndex
import org.neo4j.cypher.internal.logical.plans.CreateLookupIndex
import org.neo4j.cypher.internal.logical.plans.DeleteExpression
import org.neo4j.cypher.internal.logical.plans.DeleteNode
import org.neo4j.cypher.internal.logical.plans.DeletePath
import org.neo4j.cypher.internal.logical.plans.DeleteRelationship
import org.neo4j.cypher.internal.logical.plans.Descending
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
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForConstraint
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForFulltextIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForLookupIndex
import org.neo4j.cypher.internal.logical.plans.DropConstraintOnName
import org.neo4j.cypher.internal.logical.plans.DropIndexOnName
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.EmptyResult
import org.neo4j.cypher.internal.logical.plans.ErrorPlan
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLimit
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.IndexSeekNames
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.Input
import org.neo4j.cypher.internal.logical.plans.IntersectionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LetAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSemiApply
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LoadCSV
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlans
import org.neo4j.cypher.internal.logical.plans.ManyQueryExpression
import org.neo4j.cypher.internal.logical.plans.ManySeekableArgs
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.MultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeKey
import org.neo4j.cypher.internal.logical.plans.NodePropertyExistence
import org.neo4j.cypher.internal.logical.plans.NodePropertyType
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueness
import org.neo4j.cypher.internal.logical.plans.NullifyMetadata
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.OrderedUnion
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.PartialTop
import org.neo4j.cypher.internal.logical.plans.PartitionedAllNodesScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.PartitionedIntersectionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedNodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.PartitionedNodeIndexScan
import org.neo4j.cypher.internal.logical.plans.PartitionedNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUnionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUnwindCollection
import org.neo4j.cypher.internal.logical.plans.PathPropagatingBFS
import org.neo4j.cypher.internal.logical.plans.PointBoundingBoxRange
import org.neo4j.cypher.internal.logical.plans.PointBoundingBoxSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PointDistanceRange
import org.neo4j.cypher.internal.logical.plans.PointDistanceSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PrefixSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PreserveOrder
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.PruningVarExpand
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.RangeBetween
import org.neo4j.cypher.internal.logical.plans.RangeGreaterThan
import org.neo4j.cypher.internal.logical.plans.RangeLessThan
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.logical.plans.RelationshipCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.RelationshipKey
import org.neo4j.cypher.internal.logical.plans.RelationshipPropertyExistence
import org.neo4j.cypher.internal.logical.plans.RelationshipPropertyType
import org.neo4j.cypher.internal.logical.plans.RelationshipUniqueness
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.RepeatOptions
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.SeekableArgs
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
import org.neo4j.cypher.internal.logical.plans.ShowConstraints
import org.neo4j.cypher.internal.logical.plans.ShowFunctions
import org.neo4j.cypher.internal.logical.plans.ShowIndexes
import org.neo4j.cypher.internal.logical.plans.ShowProcedures
import org.neo4j.cypher.internal.logical.plans.ShowSettings
import org.neo4j.cypher.internal.logical.plans.ShowTransactions
import org.neo4j.cypher.internal.logical.plans.SimulatedExpand
import org.neo4j.cypher.internal.logical.plans.SimulatedNodeScan
import org.neo4j.cypher.internal.logical.plans.SimulatedSelection
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.logical.plans.SingleSeekableArg
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.logical.plans.SubqueryForeach
import org.neo4j.cypher.internal.logical.plans.SystemProcedureCall
import org.neo4j.cypher.internal.logical.plans.TerminateTransactions
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.Top1WithTies
import org.neo4j.cypher.internal.logical.plans.Trail
import org.neo4j.cypher.internal.logical.plans.TransactionApply
import org.neo4j.cypher.internal.logical.plans.TransactionForeach
import org.neo4j.cypher.internal.logical.plans.TriadicBuild
import org.neo4j.cypher.internal.logical.plans.TriadicFilter
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
import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.plandescription.Arguments.Details
import org.neo4j.cypher.internal.plandescription.Arguments.EstimatedRows
import org.neo4j.cypher.internal.plandescription.Arguments.Planner
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerImpl
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerVersion
import org.neo4j.cypher.internal.plandescription.Arguments.RuntimeVersion
import org.neo4j.cypher.internal.plandescription.asPrettyString.PrettyStringInterpolator
import org.neo4j.cypher.internal.plandescription.asPrettyString.PrettyStringMaker
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound.Limited
import org.neo4j.cypher.internal.util.UpperBound.Unlimited
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.graphdb.schema.IndexType

import scala.collection.immutable.ListSet

object LogicalPlan2PlanDescription {

  def create(
    input: LogicalPlan,
    plannerName: PlannerName,
    readOnly: Boolean,
    effectiveCardinalities: EffectiveCardinalities,
    withRawCardinalities: Boolean,
    withDistinctness: Boolean,
    providedOrders: ProvidedOrders,
    runtimeOperatorMetadata: Id => Seq[Argument]
  ): InternalPlanDescription = {
    new LogicalPlan2PlanDescription(
      readOnly,
      effectiveCardinalities,
      withRawCardinalities,
      withDistinctness,
      providedOrders,
      runtimeOperatorMetadata
    )
      .create(input)
      .addArgument(RuntimeVersion.currentVersion)
      .addArgument(Planner(plannerName.toTextOutput))
      .addArgument(PlannerImpl(plannerName.name))
      .addArgument(PlannerVersion.currentVersion)
  }
}

case class LogicalPlan2PlanDescription(
  readOnly: Boolean,
  effectiveCardinalities: EffectiveCardinalities,
  withRawCardinalities: Boolean,
  withDistinctness: Boolean = false,
  providedOrders: ProvidedOrders,
  runtimeOperatorMetadata: Id => Seq[Argument]
) extends LogicalPlans.Mapper[InternalPlanDescription] {
  private val SEPARATOR = ", "

  def create(plan: LogicalPlan): InternalPlanDescription =
    LogicalPlans.map(plan, this)

  override def onLeaf(plan: LogicalPlan): InternalPlanDescription = {
    checkOnlyWhenAssertionsAreEnabled(plan.isLeaf)

    val id = plan.id
    val variables = plan.availableSymbols.map(asPrettyString(_))

    val result: InternalPlanDescription = plan match {
      case _: AdministrationCommandLogicalPlan =>
        PlanDescriptionImpl(
          id,
          "AdministrationCommand",
          NoChildren,
          Seq.empty,
          Set.empty,
          withRawCardinalities,
          withDistinctness
        )

      case AllNodesScan(idName, _) =>
        PlanDescriptionImpl(
          id,
          "AllNodesScan",
          NoChildren,
          Seq(Details(asPrettyString(idName))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartitionedAllNodesScan(idName, _) =>
        PlanDescriptionImpl(
          id,
          "PartitionedAllNodesScan",
          NoChildren,
          Seq(Details(asPrettyString(idName))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case NodeByLabelScan(idName, label, _, _) =>
        val prettyDetails = pretty"${asPrettyString(idName)}:${asPrettyString(label.name)}"
        PlanDescriptionImpl(
          id,
          "NodeByLabelScan",
          NoChildren,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartitionedNodeByLabelScan(idName, label, _) =>
        val prettyDetails = pretty"${asPrettyString(idName)}:${asPrettyString(label.name)}"
        PlanDescriptionImpl(
          id,
          "PartitionedNodeByLabelScan",
          NoChildren,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case UnionNodeByLabelsScan(idName, labels, _, _) =>
        val prettyDetails =
          pretty"${asPrettyString(idName)}:${labels.map(l => asPrettyString(l.name)).mkPrettyString("|")}"
        PlanDescriptionImpl(
          id,
          "UnionNodeByLabelsScan",
          NoChildren,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartitionedUnionNodeByLabelsScan(idName, labels, _) =>
        val prettyDetails =
          pretty"${asPrettyString(idName)}:${labels.map(l => asPrettyString(l.name)).mkPrettyString("|")}"
        PlanDescriptionImpl(
          id,
          "PartitionedUnionNodeByLabelsScan",
          NoChildren,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case IntersectionNodeByLabelsScan(idName, labels, _, _) =>
        val prettyDetails =
          pretty"${asPrettyString(idName)}:${labels.map(l => asPrettyString(l.name)).mkPrettyString("&")}"
        PlanDescriptionImpl(
          id,
          "IntersectionNodeByLabelsScan",
          NoChildren,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartitionedIntersectionNodeByLabelsScan(idName, labels, _) =>
        val prettyDetails =
          pretty"${asPrettyString(idName)}:${labels.map(l => asPrettyString(l.name)).mkPrettyString("&")}"
        PlanDescriptionImpl(
          id,
          "PartitionedIntersectionNodeByLabelsScan",
          NoChildren,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DirectedUnionRelationshipTypesScan(idName, start, types, end, _, _) =>
        val prettyTypes = types
          .map(_.name)
          .map(asPrettyString(_))
          .mkPrettyString("|")
        val prettyDetails =
          pretty"(${asPrettyString(start)})-[${asPrettyString(idName)}:$prettyTypes]->(${asPrettyString(end)})"
        PlanDescriptionImpl(
          id,
          "DirectedUnionRelationshipTypesScan",
          NoChildren,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case UndirectedUnionRelationshipTypesScan(idName, start, types, end, _, _) =>
        val prettyTypes = types
          .map(_.name)
          .map(asPrettyString(_))
          .mkPrettyString("|")
        val prettyDetails =
          pretty"(${asPrettyString(start)})-[${asPrettyString(idName)}:$prettyTypes]-(${asPrettyString(end)})"
        PlanDescriptionImpl(
          id,
          "UndirectedUnionRelationshipTypesScan",
          NoChildren,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartitionedDirectedUnionRelationshipTypesScan(idName, start, types, end, _) =>
        val prettyTypes = types
          .map(_.name)
          .map(asPrettyString(_))
          .mkPrettyString("|")
        val prettyDetails =
          pretty"(${asPrettyString(start)})-[${asPrettyString(idName)}:$prettyTypes]->(${asPrettyString(end)})"
        PlanDescriptionImpl(
          id,
          "PartitionedDirectedUnionRelationshipTypesScan",
          NoChildren,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartitionedUndirectedUnionRelationshipTypesScan(idName, start, types, end, _) =>
        val prettyTypes = types
          .map(_.name)
          .map(asPrettyString(_))
          .mkPrettyString("|")
        val prettyDetails =
          pretty"(${asPrettyString(start)})-[${asPrettyString(idName)}:$prettyTypes]-(${asPrettyString(end)})"
        PlanDescriptionImpl(
          id,
          "PartitionedUndirectedUnionRelationshipTypesScan",
          NoChildren,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case NodeByIdSeek(idName, nodeIds: SeekableArgs, _) =>
        val prettyDetails =
          pretty"${asPrettyString(idName)} WHERE id(${asPrettyString(idName)}) ${seekableArgsInfo(nodeIds)}"
        PlanDescriptionImpl(
          id,
          "NodeByIdSeek",
          NoChildren,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case NodeByElementIdSeek(idName, nodeIds: SeekableArgs, _) =>
        val prettyDetails =
          pretty"${asPrettyString(idName)} WHERE elementId(${asPrettyString(idName)}) ${seekableArgsInfo(nodeIds)}"
        PlanDescriptionImpl(
          id,
          "NodeByElementIdSeek",
          NoChildren,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case p @ NodeIndexSeek(idName, label, properties, valueExpr, _, _, indexType, _) =>
        val (indexMode, indexDesc) = getNodeIndexDescriptions(
          idName.name,
          label,
          properties.map(_.propertyKeyToken),
          indexType,
          valueExpr,
          unique = false,
          readOnly,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          indexMode,
          NoChildren,
          Seq(Details(indexDesc)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case p @ PartitionedNodeIndexSeek(idName, label, properties, valueExpr, _, indexType) =>
        val (indexMode, indexDesc) = getNodeIndexDescriptions(
          idName.name,
          label,
          properties.map(_.propertyKeyToken),
          indexType,
          valueExpr,
          unique = false,
          readOnly,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "Partitioned" + indexMode,
          NoChildren,
          Seq(Details(indexDesc)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case p @ NodeUniqueIndexSeek(idName, label, properties, valueExpr, _, _, indexType) =>
        val (indexMode, indexDesc) = getNodeIndexDescriptions(
          idName.name,
          label,
          properties.map(_.propertyKeyToken),
          indexType,
          valueExpr,
          unique = true,
          readOnly,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          indexMode,
          NoChildren,
          Seq(Details(indexDesc)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case p @ MultiNodeIndexSeek(indexLeafPlans) =>
        val (_, indexDescs) = indexLeafPlans.map(l =>
          getNodeIndexDescriptions(
            l.idName.name,
            l.label,
            l.properties.map(_.propertyKeyToken),
            l.indexType,
            l.valueExpr,
            unique = l.isInstanceOf[NodeUniqueIndexSeek],
            readOnly,
            p.cachedProperties
          )
        ).unzip
        PlanDescriptionImpl(
          id = plan.id,
          "MultiNodeIndexSeek",
          NoChildren,
          Seq(Details(indexDescs)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case p @ AssertingMultiNodeIndexSeek(_, indexLeafPlans) =>
        val (_, indexDescs) = indexLeafPlans.map(l =>
          getNodeIndexDescriptions(
            l.idName.name,
            l.label,
            l.properties.map(_.propertyKeyToken),
            l.indexType,
            l.valueExpr,
            unique = l.isInstanceOf[NodeUniqueIndexSeek],
            readOnly,
            p.cachedProperties
          )
        ).unzip
        PlanDescriptionImpl(
          id = plan.id,
          "AssertingMultiNodeIndexSeek",
          NoChildren,
          Seq(Details(indexDescs)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case p @ AssertingMultiRelationshipIndexSeek(_, _, _, _, indexLeafPlans) =>
        val (_, indexDescs) = indexLeafPlans.map(l =>
          getRelIndexDescriptions(
            l.idName.name,
            l.leftNode.name,
            l.typeToken,
            l.rightNode.name,
            l.directed,
            l.properties.map(_.propertyKeyToken),
            l.indexType,
            l.valueExpr,
            unique = l.unique,
            readOnly,
            p.cachedProperties
          )
        ).unzip
        PlanDescriptionImpl(
          id = plan.id,
          "AssertingMultiRelationshipIndexSeek",
          NoChildren,
          Seq(Details(indexDescs)),
          variables,
          withRawCardinalities,
          withDistinctness
        )
      case p @ DirectedRelationshipIndexSeek(idName, start, end, typ, properties, valueExpr, _, _, indexType, _) =>
        val (indexMode, indexDesc) = getRelIndexDescriptions(
          idName.name,
          start.name,
          typ,
          end.name,
          isDirected = true,
          properties.map(_.propertyKeyToken),
          indexType,
          valueExpr,
          unique = false,
          readOnly = readOnly,
          p.cachedProperties
        )
        PlanDescriptionImpl(id, indexMode, NoChildren, Seq(Details(indexDesc)), variables)
      case p @ UndirectedRelationshipIndexSeek(idName, start, end, typ, properties, valueExpr, _, _, indexType, _) =>
        val (indexMode, indexDesc) = getRelIndexDescriptions(
          idName.name,
          start.name,
          typ,
          end.name,
          isDirected = false,
          properties.map(_.propertyKeyToken),
          indexType,
          valueExpr,
          unique = false,
          readOnly = readOnly,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          indexMode,
          NoChildren,
          Seq(Details(indexDesc)),
          variables,
          withRawCardinalities,
          withDistinctness
        )
      case p @ PartitionedDirectedRelationshipIndexSeek(idName, start, end, typ, properties, valueExpr, _, indexType) =>
        val (indexMode, indexDesc) = getRelIndexDescriptions(
          idName.name,
          start.name,
          typ,
          end.name,
          isDirected = true,
          properties.map(_.propertyKeyToken),
          indexType,
          valueExpr,
          unique = false,
          readOnly = readOnly,
          p.cachedProperties
        )
        PlanDescriptionImpl(id, "Partitioned" + indexMode, NoChildren, Seq(Details(indexDesc)), variables)
      case p @ PartitionedUndirectedRelationshipIndexSeek(
          idName,
          start,
          end,
          typ,
          properties,
          valueExpr,
          _,
          indexType
        ) =>
        val (indexMode, indexDesc) = getRelIndexDescriptions(
          idName.name,
          start.name,
          typ,
          end.name,
          isDirected = false,
          properties.map(_.propertyKeyToken),
          indexType,
          valueExpr,
          unique = false,
          readOnly = readOnly,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "Partitioned" + indexMode,
          NoChildren,
          Seq(Details(indexDesc)),
          variables,
          withRawCardinalities,
          withDistinctness
        )
      case p @ DirectedRelationshipUniqueIndexSeek(idName, start, end, typ, properties, valueExpr, _, _, indexType) =>
        val (indexMode, indexDesc) = getRelIndexDescriptions(
          idName.name,
          start.name,
          typ,
          end.name,
          isDirected = true,
          properties.map(_.propertyKeyToken),
          indexType,
          valueExpr,
          unique = true,
          readOnly = readOnly,
          p.cachedProperties
        )
        PlanDescriptionImpl(id, indexMode, NoChildren, Seq(Details(indexDesc)), variables)
      case p @ UndirectedRelationshipUniqueIndexSeek(idName, start, end, typ, properties, valueExpr, _, _, indexType) =>
        val (indexMode, indexDesc) = getRelIndexDescriptions(
          idName.name,
          start.name,
          typ,
          end.name,
          isDirected = false,
          properties.map(_.propertyKeyToken),
          indexType,
          valueExpr,
          unique = true,
          readOnly = readOnly,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          indexMode,
          NoChildren,
          Seq(Details(indexDesc)),
          variables,
          withRawCardinalities,
          withDistinctness
        )
      case p @ DirectedRelationshipIndexScan(idName, start, end, typ, properties, _, _, indexType, _) =>
        val tokens = properties.map(_.propertyKeyToken)
        val props = tokens.map(x => asPrettyString(x.name))
        val predicates = props.map(p => pretty"$p IS NOT NULL").mkPrettyString(" AND ")
        val info = relIndexInfoString(
          idName.name,
          start.name,
          typ,
          end.name,
          isDirected = true,
          tokens,
          indexType,
          predicates,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "DirectedRelationshipIndexScan",
          NoChildren,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )
      case p @ UndirectedRelationshipIndexScan(idName, start, end, typ, properties, _, _, indexType, _) =>
        val tokens = properties.map(_.propertyKeyToken)
        val props = tokens.map(x => asPrettyString(x.name))
        val predicates = props.map(p => pretty"$p IS NOT NULL").mkPrettyString(" AND ")
        val info = relIndexInfoString(
          idName.name,
          start.name,
          typ,
          end.name,
          isDirected = false,
          tokens,
          indexType,
          predicates,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "UndirectedRelationshipIndexScan",
          NoChildren,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )
      case p @ PartitionedDirectedRelationshipIndexScan(idName, start, end, typ, properties, _, indexType) =>
        val tokens = properties.map(_.propertyKeyToken)
        val props = tokens.map(x => asPrettyString(x.name))
        val predicates = props.map(p => pretty"$p IS NOT NULL").mkPrettyString(" AND ")
        val info = relIndexInfoString(
          idName.name,
          start.name,
          typ,
          end.name,
          isDirected = true,
          tokens,
          indexType,
          predicates,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "PartitionedDirectedRelationshipIndexScan",
          NoChildren,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )
      case p @ PartitionedUndirectedRelationshipIndexScan(idName, start, end, typ, properties, _, indexType) =>
        val tokens = properties.map(_.propertyKeyToken)
        val props = tokens.map(x => asPrettyString(x.name))
        val predicates = props.map(p => pretty"$p IS NOT NULL").mkPrettyString(" AND ")
        val info = relIndexInfoString(
          idName.name,
          start.name,
          typ,
          end.name,
          isDirected = false,
          tokens,
          indexType,
          predicates,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "PartitionedUndirectedRelationshipIndexScan",
          NoChildren,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )
      case p @ DirectedRelationshipIndexContainsScan(idName, start, end, typ, property, valueExpr, _, _, indexType) =>
        val predicate = pretty"${asPrettyString(property.propertyKeyToken.name)} CONTAINS ${asPrettyString(valueExpr)}"
        val info = relIndexInfoString(
          idName.name,
          start.name,
          typ,
          end.name,
          isDirected = true,
          Seq(property.propertyKeyToken),
          indexType,
          predicate,
          p.cachedProperties
        )
        PlanDescriptionImpl(id, "DirectedRelationshipIndexContainsScan", NoChildren, Seq(Details(info)), variables)
      case p @ UndirectedRelationshipIndexContainsScan(idName, start, end, typ, property, valueExpr, _, _, indexType) =>
        val predicate = pretty"${asPrettyString(property.propertyKeyToken.name)} CONTAINS ${asPrettyString(valueExpr)}"
        val info = relIndexInfoString(
          idName.name,
          start.name,
          typ,
          end.name,
          isDirected = false,
          Seq(property.propertyKeyToken),
          indexType,
          predicate,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "UndirectedRelationshipIndexContainsScan",
          NoChildren,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )
      case p @ DirectedRelationshipIndexEndsWithScan(idName, start, end, typ, property, valueExpr, _, _, indexType) =>
        val predicate = pretty"${asPrettyString(property.propertyKeyToken.name)} ENDS WITH ${asPrettyString(valueExpr)}"
        val info = relIndexInfoString(
          idName.name,
          start.name,
          typ,
          end.name,
          isDirected = true,
          Seq(property.propertyKeyToken),
          indexType,
          predicate,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "DirectedRelationshipIndexEndsWithScan",
          NoChildren,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )
      case p @ UndirectedRelationshipIndexEndsWithScan(idName, start, end, typ, property, valueExpr, _, _, indexType) =>
        val predicate = pretty"${asPrettyString(property.propertyKeyToken.name)} ENDS WITH ${asPrettyString(valueExpr)}"
        val info = relIndexInfoString(
          idName.name,
          start.name,
          typ,
          end.name,
          isDirected = false,
          Seq(property.propertyKeyToken),
          indexType,
          predicate,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "UndirectedRelationshipIndexEndsWithScan",
          NoChildren,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case plans.Argument(argumentIds) if argumentIds.nonEmpty =>
        val details =
          if (argumentIds.nonEmpty) Seq(Details(argumentIds.map(asPrettyString(_)).mkPrettyString(SEPARATOR)))
          else Seq.empty
        PlanDescriptionImpl(id, "Argument", NoChildren, details, variables, withRawCardinalities, withDistinctness)

      case _: plans.Argument =>
        ArgumentPlanDescription(id, Seq.empty, variables)

      case DirectedRelationshipByIdSeek(idName, relIds, startNode, endNode, _) =>
        val details = Details(relationshipByIdSeekInfo(idName.name, relIds, startNode.name, endNode.name, true, "id"))
        PlanDescriptionImpl(
          id,
          "DirectedRelationshipByIdSeek",
          NoChildren,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DirectedRelationshipByElementIdSeek(idName, relIds, startNode, endNode, _) =>
        val details =
          Details(relationshipByIdSeekInfo(idName.name, relIds, startNode.name, endNode.name, true, "elementId"))
        PlanDescriptionImpl(
          id,
          "DirectedRelationshipByElementIdSeek",
          NoChildren,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case UndirectedRelationshipByIdSeek(idName, relIds, startNode, endNode, _) =>
        val details = Details(relationshipByIdSeekInfo(idName.name, relIds, startNode.name, endNode.name, false, "id"))
        PlanDescriptionImpl(
          id,
          "UndirectedRelationshipByIdSeek",
          NoChildren,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case UndirectedRelationshipByElementIdSeek(idName, relIds, startNode, endNode, _) =>
        val details =
          Details(relationshipByIdSeekInfo(idName.name, relIds, startNode.name, endNode.name, false, "elementId"))
        PlanDescriptionImpl(
          id,
          "UndirectedRelationshipByElementIdSeek",
          NoChildren,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DirectedAllRelationshipsScan(idName, start, end, _) =>
        val prettyDetails =
          pretty"(${asPrettyString(start)})-[${asPrettyString(idName)}]->(${asPrettyString(end)})"
        PlanDescriptionImpl(
          id,
          "DirectedAllRelationshipsScan",
          NoChildren,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartitionedDirectedAllRelationshipsScan(idName, start, end, _) =>
        val prettyDetails =
          pretty"(${asPrettyString(start)})-[${asPrettyString(idName)}]->(${asPrettyString(end)})"
        PlanDescriptionImpl(
          id,
          "PartitionedDirectedAllRelationshipsScan",
          NoChildren,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case UndirectedAllRelationshipsScan(idName, start, end, _) =>
        val prettyDetails =
          pretty"(${asPrettyString(start)})-[${asPrettyString(idName)}]-(${asPrettyString(end)})"
        PlanDescriptionImpl(
          id,
          "UndirectedAllRelationshipsScan",
          NoChildren,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartitionedUndirectedAllRelationshipsScan(idName, start, end, _) =>
        val prettyDetails =
          pretty"(${asPrettyString(start)})-[${asPrettyString(idName)}]-(${asPrettyString(end)})"
        PlanDescriptionImpl(
          id,
          "PartitionedUndirectedAllRelationshipsScan",
          NoChildren,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DirectedRelationshipTypeScan(idName, start, typeName, end, _, _) =>
        val prettyDetails =
          pretty"(${asPrettyString(start)})-[${asPrettyString(idName)}:${asPrettyString(typeName.name)}]->(${asPrettyString(end)})"
        PlanDescriptionImpl(
          id,
          "DirectedRelationshipTypeScan",
          NoChildren,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case UndirectedRelationshipTypeScan(idName, start, typeName, end, _, _) =>
        val prettyDetails =
          pretty"(${asPrettyString(start)})-[${asPrettyString(idName)}:${asPrettyString(typeName.name)}]-(${asPrettyString(end)})"
        PlanDescriptionImpl(
          id,
          "UndirectedRelationshipTypeScan",
          NoChildren,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartitionedDirectedRelationshipTypeScan(idName, start, typeName, end, _) =>
        val prettyDetails =
          pretty"(${asPrettyString(start)})-[${asPrettyString(idName)}:${asPrettyString(typeName.name)}]->(${asPrettyString(end)})"
        PlanDescriptionImpl(
          id,
          "PartitionedDirectedRelationshipTypeScan",
          NoChildren,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartitionedUndirectedRelationshipTypeScan(idName, start, typeName, end, _) =>
        val prettyDetails =
          pretty"(${asPrettyString(start)})-[${asPrettyString(idName)}:${asPrettyString(typeName.name)}]-(${asPrettyString(end)})"
        PlanDescriptionImpl(
          id,
          "PartitionedUndirectedRelationshipTypeScan",
          NoChildren,
          Seq(Details(prettyDetails)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Input(nodes, rels, inputVars, _) =>
        PlanDescriptionImpl(
          id,
          "Input",
          NoChildren,
          Seq(Details((nodes ++ rels ++ inputVars).map(asPrettyString(_)))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case NodeCountFromCountStore(ident, labelNames, _) =>
        val info = nodeCountFromCountStoreInfo(ident, labelNames)
        PlanDescriptionImpl(
          id,
          "NodeCountFromCountStore",
          NoChildren,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case p @ NodeIndexContainsScan(idName, label, property, valueExpr, _, _, indexType) =>
        val predicate = pretty"${asPrettyString(property.propertyKeyToken.name)} CONTAINS ${asPrettyString(valueExpr)}"
        val info = nodeIndexInfoString(
          idName.name,
          unique = false,
          label,
          Seq(property.propertyKeyToken),
          indexType,
          predicate,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "NodeIndexContainsScan",
          NoChildren,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case p @ NodeIndexEndsWithScan(idName, label, property, valueExpr, _, _, indexType) =>
        val predicate = pretty"${asPrettyString(property.propertyKeyToken.name)} ENDS WITH ${asPrettyString(valueExpr)}"
        val info = nodeIndexInfoString(
          idName.name,
          unique = false,
          label,
          Seq(property.propertyKeyToken),
          indexType,
          predicate,
          p.cachedProperties
        )
        PlanDescriptionImpl(
          id,
          "NodeIndexEndsWithScan",
          NoChildren,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case p @ NodeIndexScan(idName, label, properties, _, _, indexType, _) =>
        val tokens = properties.map(_.propertyKeyToken)
        val props = tokens.map(x => asPrettyString(x.name))
        val predicates = props.map(p => pretty"$p IS NOT NULL").mkPrettyString(" AND ")
        val info =
          nodeIndexInfoString(idName.name, unique = false, label, tokens, indexType, predicates, p.cachedProperties)
        PlanDescriptionImpl(
          id,
          "NodeIndexScan",
          NoChildren,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case p @ PartitionedNodeIndexScan(idName, label, properties, _, indexType) =>
        val tokens = properties.map(_.propertyKeyToken)
        val props = tokens.map(x => asPrettyString(x.name))
        val predicates = props.map(p => pretty"$p IS NOT NULL").mkPrettyString(" AND ")
        val info =
          nodeIndexInfoString(idName.name, unique = false, label, tokens, indexType, predicates, p.cachedProperties)
        PlanDescriptionImpl(
          id,
          "PartitionedNodeIndexScan",
          NoChildren,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SimulatedNodeScan(idName, numberOfRows) =>
        val details = Details(Seq(
          asPrettyString(idName),
          asPrettyString(UnsignedDecimalIntegerLiteral(numberOfRows.toString)(InputPosition.NONE))
        ))
        PlanDescriptionImpl(
          id,
          "SimulatedNodeScan",
          NoChildren,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case ProcedureCall(_, call) =>
        PlanDescriptionImpl(
          id,
          "ProcedureCall",
          NoChildren,
          Seq(Details(signatureInfo(call))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case RelationshipCountFromCountStore(ident, startLabel, typeNames, endLabel, _) =>
        val info = relationshipCountFromCountStoreInfo(ident, startLabel, typeNames, endLabel)
        PlanDescriptionImpl(
          id,
          "RelationshipCountFromCountStore",
          NoChildren,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DoNothingIfExistsForIndex(entityName, propertyKeyNames, indexType, nameOption, _) =>
        PlanDescriptionImpl(
          id,
          s"DoNothingIfExists(INDEX)",
          NoChildren,
          Seq(Details(indexInfo(indexType.name(), nameOption, entityName, propertyKeyNames, NoOptions))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DoNothingIfExistsForLookupIndex(entityType, nameOption, _) =>
        PlanDescriptionImpl(
          id,
          s"DoNothingIfExists(INDEX)",
          NoChildren,
          Seq(Details(lookupIndexInfo(nameOption, entityType, NoOptions))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DoNothingIfExistsForFulltextIndex(entityNames, propertyKeyNames, nameOption, _) =>
        PlanDescriptionImpl(
          id,
          s"DoNothingIfExists(INDEX)",
          NoChildren,
          Seq(Details(fulltextIndexInfo(nameOption, entityNames, propertyKeyNames, NoOptions))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case CreateIndex(
          _,
          indexType,
          entityName,
          propertyKeyNames,
          nameOption,
          options
        ) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(
          id,
          "CreateIndex",
          NoChildren,
          Seq(Details(indexInfo(indexType.name(), nameOption, entityName, propertyKeyNames, options))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case CreateLookupIndex(
          _,
          entityType,
          nameOption,
          options
        ) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(
          id,
          "CreateIndex",
          NoChildren,
          Seq(Details(lookupIndexInfo(nameOption, entityType, options))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case CreateFulltextIndex(
          _,
          entityNames,
          propertyKeyNames,
          nameOption,
          options
        ) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(
          id,
          "CreateIndex",
          NoChildren,
          Seq(Details(fulltextIndexInfo(nameOption, entityNames, propertyKeyNames, options))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DropIndexOnName(name, ifExists) =>
        val ifExistsString = if (ifExists) pretty" IF EXISTS" else pretty""
        PlanDescriptionImpl(
          id,
          "DropIndex",
          NoChildren,
          Seq(Details(pretty"INDEX ${PrettyString(Prettifier.escapeName(name))}$ifExistsString")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case s: ShowIndexes =>
        val typeDescription = asPrettyString.raw(s.indexType.description)
        val colsDescription = commandColumnInfo(s.yieldColumns, s.yieldAll)
        PlanDescriptionImpl(
          id,
          "ShowIndexes",
          NoChildren,
          Seq(Details(pretty"$typeDescription, $colsDescription")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DoNothingIfExistsForConstraint(entityName, props, assertion, name, _) =>
        val entity = props.head.map.asCanonicalStringVal
        PlanDescriptionImpl(
          id,
          s"DoNothingIfExists(CONSTRAINT)",
          NoChildren,
          Seq(Details(constraintInfo(name, entity, entityName, props, assertion))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case CreateConstraint(
          _,
          constraintType,
          label,
          properties: Seq[Property],
          nameOption,
          options
        ) => // Can be both a leaf plan and a middle plan so need to be in both places
        val entity = properties.head.map.asCanonicalStringVal
        val details = Details(constraintInfo(
          nameOption,
          entity,
          label,
          properties,
          constraintType,
          options
        ))
        PlanDescriptionImpl(
          id,
          "CreateConstraint",
          NoChildren,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DropConstraintOnName(name, ifExists) =>
        val ifExistsString = if (ifExists) pretty" IF EXISTS" else pretty""
        val constraintDetails = Details(pretty"CONSTRAINT ${PrettyString(Prettifier.escapeName(name))}$ifExistsString")
        PlanDescriptionImpl(
          id,
          "DropConstraint",
          NoChildren,
          Seq(constraintDetails),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case s: ShowConstraints =>
        val typeDescription = asPrettyString.raw(s.constraintType.description)
        val colsDescription = commandColumnInfo(s.yieldColumns, s.yieldAll)
        PlanDescriptionImpl(
          id,
          "ShowConstraints",
          NoChildren,
          Seq(Details(pretty"$typeDescription, $colsDescription")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case s: ShowProcedures =>
        val executableDescription = s.executableBy.map(e => asPrettyString.raw(e.description("procedures"))).getOrElse(
          asPrettyString.raw(ExecutableBy.defaultDescription("procedures"))
        )
        val colsDescription = commandColumnInfo(s.yieldColumns, s.yieldAll)
        PlanDescriptionImpl(
          id,
          "ShowProcedures",
          NoChildren,
          Seq(Details(pretty"$executableDescription, $colsDescription")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case s: ShowFunctions =>
        val typeDescription = asPrettyString.raw(s.functionType.description)
        val executableDescription = s.executableBy.map(e => asPrettyString.raw(e.description("functions"))).getOrElse(
          pretty"functionsForUser(all)"
        )
        val colsDescription = commandColumnInfo(s.yieldColumns, s.yieldAll)
        PlanDescriptionImpl(
          id,
          "ShowFunctions",
          NoChildren,
          Seq(Details(pretty"$typeDescription, $executableDescription, $colsDescription")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case s: ShowTransactions =>
        val idsDescription = s.ids match {
          case Left(ls) =>
            asPrettyString.raw(if (ls.isEmpty) "allTransactions" else s"transactions(${ls.mkString(", ")})")
          case Right(e) => asPrettyString.raw(s"transactions(${e.asCanonicalStringVal})")
        }
        val colsDescription = commandColumnInfo(s.yieldColumns, s.yieldAll)
        PlanDescriptionImpl(
          id,
          "ShowTransactions",
          NoChildren,
          Seq(Details(pretty"$colsDescription, $idsDescription")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case t: TerminateTransactions =>
        val idsDescription = t.ids match {
          case Left(ls) => asPrettyString.raw(ls.mkString(", "))
          case Right(e) => asPrettyString.raw(s"${e.asCanonicalStringVal}")
        }
        val colsDescription = commandColumnInfo(t.yieldColumns, t.yieldAll)
        PlanDescriptionImpl(
          id,
          "TerminateTransactions",
          NoChildren,
          Seq(Details(pretty"$colsDescription, transactions($idsDescription)")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case s: ShowSettings =>
        val namesDescription = s.names match {
          case Left(Seq()) => asPrettyString.raw("allSettings")
          case Left(names) => asPrettyString.raw(s"settings(${names.mkString(", ")})")
          case Right(e)    => asPrettyString.raw(s"settings(${e.asCanonicalStringVal})")
        }
        val colsDescription = commandColumnInfo(s.yieldColumns, s.yieldAll)
        PlanDescriptionImpl(
          id,
          "ShowSettings",
          NoChildren,
          Seq(Details(pretty"$namesDescription, $colsDescription")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SystemProcedureCall(procedureName, _, _, _, _) =>
        PlanDescriptionImpl(id, procedureName, NoChildren, Seq.empty, variables, withRawCardinalities, withDistinctness)

      case x => throw new InternalException(s"Unknown plan type: ${x.getClass.getSimpleName}. Missing a case?")
    }

    addRuntimeAttributes(addPlanningAttributes(result, plan), plan)
  }

  override def onOneChildPlan(plan: LogicalPlan, source: InternalPlanDescription): InternalPlanDescription = {
    checkOnlyWhenAssertionsAreEnabled(plan.lhs.nonEmpty)
    checkOnlyWhenAssertionsAreEnabled(plan.rhs.isEmpty)

    val id = plan.id
    val variables = plan.availableSymbols.map(asPrettyString(_))
    val children = if (source.isInstanceOf[ArgumentPlanDescription]) NoChildren else SingleChild(source)

    val result: InternalPlanDescription = plan match {
      case _: AdministrationCommandLogicalPlan =>
        PlanDescriptionImpl(
          id,
          "AdministrationCommand",
          NoChildren,
          Seq.empty,
          Set.empty,
          withRawCardinalities,
          withDistinctness
        )

      case Distinct(_, groupingExpressions) =>
        PlanDescriptionImpl(
          id,
          "Distinct",
          children,
          Seq(Details(aggregationInfo(groupingExpressions, Map.empty))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case OrderedDistinct(_, groupingExpressions, orderToLeverage) =>
        val details = aggregationInfo(groupingExpressions, Map.empty, orderToLeverage)
        PlanDescriptionImpl(
          id,
          "OrderedDistinct",
          children,
          Seq(Details(details)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Aggregation(_, groupingExpressions, aggregationExpressions) =>
        val details =
          aggregationInfo(groupingExpressions, aggregationExpressions, ordered = Seq.empty)
        PlanDescriptionImpl(
          id,
          "EagerAggregation",
          children,
          Seq(Details(details)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case OrderedAggregation(_, groupingExpressions, aggregationExpressions, orderToLeverage) =>
        val details = aggregationInfo(groupingExpressions, aggregationExpressions, orderToLeverage)
        PlanDescriptionImpl(
          id,
          "OrderedAggregation",
          children,
          Seq(Details(details)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Create(_, commands) =>
        val details = commands.map {
          case c: CreateNode => createNodeDescription(c)
          case CreateRelationship(idName, leftNode, relType, rightNode, direction, properties) =>
            expandExpressionDescription(
              leftNode,
              Some(idName),
              Seq(relType.name),
              rightNode,
              direction,
              1,
              Some(1),
              properties
            )
        }
        PlanDescriptionImpl(
          id,
          "Create",
          children,
          Seq(Details(details)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DeleteExpression(_, expression) =>
        PlanDescriptionImpl(
          id,
          "Delete",
          children,
          Seq(Details(asPrettyString(expression))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DeleteNode(_, expression) =>
        PlanDescriptionImpl(
          id,
          "Delete",
          children,
          Seq(Details(asPrettyString(expression))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DeletePath(_, expression) =>
        PlanDescriptionImpl(
          id,
          "Delete",
          children,
          Seq(Details(asPrettyString(expression))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DeleteRelationship(_, expression) =>
        PlanDescriptionImpl(
          id,
          "Delete",
          children,
          Seq(Details(asPrettyString(expression))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DetachDeleteExpression(_, expression) =>
        PlanDescriptionImpl(
          id,
          "DetachDelete",
          children,
          Seq(Details(asPrettyString(expression))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DetachDeleteNode(_, expression) =>
        PlanDescriptionImpl(
          id,
          "DetachDelete",
          children,
          Seq(Details(asPrettyString(expression))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case DetachDeletePath(_, expression) =>
        PlanDescriptionImpl(
          id,
          "DetachDelete",
          children,
          Seq(Details(asPrettyString(expression))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Eager(_, reasons) =>
        val info = eagernessReasonInfo(reasons)
        val details = if (info.nonEmpty) Seq(Details(info)) else Seq.empty
        PlanDescriptionImpl(id, "Eager", children, details, variables, withRawCardinalities, withDistinctness)

      case _: EmptyResult =>
        PlanDescriptionImpl(id, "EmptyResult", children, Seq.empty, variables, withRawCardinalities, withDistinctness)

      case NodeCountFromCountStore(idName, labelName, _) =>
        val info = nodeCountFromCountStoreInfo(idName, labelName)
        PlanDescriptionImpl(
          id,
          "NodeCountFromCountStore",
          NoChildren,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case RelationshipCountFromCountStore(idName, start, types, end, _) =>
        val info = relationshipCountFromCountStoreInfo(idName, start, types, end)
        PlanDescriptionImpl(
          id,
          "RelationshipCountFromCountStore",
          NoChildren,
          Seq(Details(info)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case _: ErrorPlan =>
        PlanDescriptionImpl(id, "Error", children, Seq.empty, variables, withRawCardinalities, withDistinctness)

      case Expand(_, fromName, dir, typeNames, toName, relName, mode) =>
        val expression = Details(expandExpressionDescription(
          fromName,
          Some(relName),
          typeNames.map(_.name),
          toName,
          dir,
          1,
          Some(1),
          None
        ))
        val modeDescr = expandModeDescription(mode)
        PlanDescriptionImpl(
          id,
          s"Expand($modeDescr)",
          children,
          Seq(expression),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SimulatedExpand(_, fromName, relName, toName, factor) =>
        val prettyFactor = asPrettyString(DecimalDoubleLiteral(factor.toString)(InputPosition.NONE))
        val details = Details(Seq(
          expandExpressionDescription(
            fromName,
            Some(relName),
            Seq.empty,
            toName,
            SemanticDirection.OUTGOING,
            1,
            Some(1),
            None
          ),
          prettyFactor
        ))
        PlanDescriptionImpl(
          id,
          "SimulatedExpand",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Limit(_, count) =>
        PlanDescriptionImpl(
          id,
          "Limit",
          children,
          Seq(Details(asPrettyString(count))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case ExhaustiveLimit(_, count) =>
        PlanDescriptionImpl(
          id,
          "ExhaustiveLimit",
          children,
          Seq(Details(asPrettyString(count))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case CacheProperties(_, properties) =>
        PlanDescriptionImpl(
          id,
          "CacheProperties",
          children,
          Seq(Details(properties.toSeq.map(asPrettyString(_)))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case OptionalExpand(_, fromName, dir, typeNames, toName, relName, mode, predicates) =>
        val predicate = predicates.map(p => pretty" WHERE ${asPrettyString(p)}").getOrElse(pretty"")
        val expandExpressionDesc =
          expandExpressionDescription(fromName, Some(relName), typeNames.map(_.name), toName, dir, 1, Some(1), None)
        val details = Details(pretty"$expandExpressionDesc$predicate")
        val modeText = mode match {
          case ExpandAll  => "OptionalExpand(All)"
          case ExpandInto => "OptionalExpand(Into)"
        }
        PlanDescriptionImpl(id, modeText, children, Seq(details), variables, withRawCardinalities, withDistinctness)

      case ProduceResult(_, columns) =>
        PlanDescriptionImpl(
          id,
          "ProduceResults",
          children,
          Seq(Details(columns.map(asPrettyString(_)))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Projection(_, expr) =>
        val expressions = Details(projectedExpressionInfo(expr))
        PlanDescriptionImpl(
          id,
          "Projection",
          children,
          Seq(expressions),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Selection(predicate, _) =>
        val details = Details(asPrettyString(predicate))
        PlanDescriptionImpl(id, "Filter", children, Seq(details), variables, withRawCardinalities, withDistinctness)

      case SimulatedSelection(_, selectivity) =>
        val details = Details(asPrettyString(DecimalDoubleLiteral(selectivity.toString)(InputPosition.NONE)))
        PlanDescriptionImpl(
          id,
          "SimulatedFilter",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Skip(_, count) =>
        PlanDescriptionImpl(
          id,
          name = "Skip",
          children,
          Seq(Details(asPrettyString(count))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case StatefulShortestPath(_, _, _, _, mode, _, _, _, _, _, _, solvedExpressionString, _) =>
        val modeDescr = expandModeDescription(mode)
        PlanDescriptionImpl(
          id = id,
          name = s"StatefulShortestPath($modeDescr)",
          children = children,
          arguments = Seq(Details(asPrettyString.solvedExpressionString(solvedExpressionString))),
          variables = variables,
          withRawCardinalities = withRawCardinalities,
          withDistinctness = withDistinctness
        )

      case FindShortestPaths(
          _,
          ShortestRelationshipPattern(
            maybePathName,
            PatternRelationship(relName, (fromName, toName), dir, relTypes, patternLength: PatternLength),
            isSingle
          ),
          nodePredicates,
          relPredicates,
          pathPredicates,
          _,
          _
        ) =>
        val patternRelationshipInfo =
          expandExpressionDescription(fromName, Some(relName), relTypes.map(_.name), toName, dir, patternLength)

        val pathName = asPrettyString(maybePathName.map(_.name).getOrElse("p"))

        val (_, nodeAndRelPredicatesDescription) =
          varExpandPredicateDescriptions(nodePredicates, relPredicates, pathName)

        val standalonePathPredicatesDescription = pathPredicates.map(asPrettyString(_)).mkPrettyString(" AND ")

        val predicatesDescription = (pathPredicates.isEmpty, (nodePredicates ++ relPredicates).isEmpty) match {
          case (true, true)  => pretty""
          case (false, true) => pretty" WHERE ${standalonePathPredicatesDescription}"
          case (true, false) => nodeAndRelPredicatesDescription
          case (false, false) =>
            Seq(nodeAndRelPredicatesDescription, standalonePathPredicatesDescription).mkPrettyString(" AND  ")
        }

        val pathPrefix =
          if ((pathPredicates ++ nodePredicates ++ relPredicates).isEmpty && maybePathName.isEmpty) {
            pretty""
          } else {
            pretty"$pathName = "
          }

        PlanDescriptionImpl(
          id,
          "ShortestPath",
          children,
          Seq(Details(pretty"$pathPrefix$patternRelationshipInfo$predicatesDescription")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case LoadCSV(_, _, variableName, _, _, _, _) =>
        PlanDescriptionImpl(
          id,
          "LoadCSV",
          children,
          Seq(Details(asPrettyString(variableName))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Merge(_, createNodes, createRelationships, onMatch, onCreate, nodesToLock) =>
        val createNodesPretty = createNodes.map(createNodeDescription)
        val createRelsPretty = createRelationships.map {
          case CreateRelationship(relationship, startNode, typ, endNode, direction, properties) =>
            expandExpressionDescription(
              startNode,
              Some(relationship),
              Seq(typ.name),
              endNode,
              direction,
              1,
              Some(1),
              properties
            )
        }
        val details: Seq[PrettyString] =
          Seq(pretty"CREATE ${(createNodesPretty ++ createRelsPretty).mkPrettyString(", ")}") ++
            (if (onMatch.nonEmpty) Seq(pretty"ON MATCH ${onMatch.map(mutatingPatternString).mkPrettyString(", ")}")
             else Seq.empty) ++
            (if (onCreate.nonEmpty) Seq(pretty"ON CREATE ${onCreate.map(mutatingPatternString).mkPrettyString(", ")}")
             else Seq.empty) ++
            (if (nodesToLock.nonEmpty) Seq(pretty"LOCK(${keyNamesInfo(nodesToLock.toSeq)})") else Seq.empty)

        val name = if (nodesToLock.isEmpty) "Merge" else "LockingMerge"
        PlanDescriptionImpl(
          id,
          name,
          children,
          Seq(Details(details)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Optional(_, protectedSymbols) =>
        PlanDescriptionImpl(
          id,
          "Optional",
          children,
          Seq(Details(keyNamesInfo(protectedSymbols.toSeq))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case _: Anti =>
        PlanDescriptionImpl(id, "Anti", children, Seq.empty, variables, withRawCardinalities, withDistinctness)

      case ProcedureCall(_, call) =>
        PlanDescriptionImpl(
          id,
          "ProcedureCall",
          children,
          Seq(Details(signatureInfo(call))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case ProjectEndpoints(_, relName, start, _, end, _, relTypes, direction, patternLength) =>
        val name = s"ProjectEndpoints"
        val relTypeNames = relTypes.map(_.name)

        val details = expandExpressionDescription(start, Some(relName), relTypeNames, end, direction, patternLength)
        PlanDescriptionImpl(
          id,
          name,
          children,
          Seq(Details(details)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PruningVarExpand(
          _,
          fromName,
          dir,
          types,
          toName,
          min,
          max,
          nodePredicates,
          relationshipPredicates
        ) =>
        val expandInfo = expandExpressionDescription(
          fromName,
          None,
          types.map(_.name),
          toName,
          dir,
          minLength = min,
          maxLength = Some(max),
          maybeProperties = None
        )
        val (expandDescriptionPrefix, predicatesDescription) =
          varExpandPredicateDescriptions(nodePredicates, relationshipPredicates)
        PlanDescriptionImpl(
          id,
          s"VarLengthExpand(Pruning)",
          children,
          Seq(Details(pretty"$expandDescriptionPrefix$expandInfo$predicatesDescription")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case BFSPruningVarExpand(
          _,
          fromName,
          dir,
          types,
          toName,
          includeStartNode,
          max,
          depthName,
          mode,
          nodePredicates,
          relationshipPredicates
        ) =>
        val expandInfo = expandExpressionDescription(
          fromName,
          None,
          types.map(_.name),
          toName,
          dir,
          minLength = if (includeStartNode) 0 else 1,
          maxLength = Some(max),
          maybeProperties = None
        )
        val modeDescr = expandModeDescription(mode)

        val (expandDescriptionPrefix, predicatesDescription) =
          varExpandPredicateDescriptions(nodePredicates, relationshipPredicates)
        val depthString = depthName.map(d => asPrettyString(s"${d.name}")).getOrElse(asPrettyString(""))
        PlanDescriptionImpl(
          id,
          s"VarLengthExpand(Pruning,BFS,$modeDescr)",
          children,
          Seq(Details(pretty"$expandDescriptionPrefix$expandInfo$predicatesDescription $depthString")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PathPropagatingBFS(
          _,
          _,
          from,
          dir,
          _,
          types,
          to,
          relName,
          length,
          nodePredicates,
          relationshipPredicates
        ) =>
        val expandDescription = expandExpressionDescription(
          from,
          Some(relName),
          types.map(_.name),
          to,
          dir,
          minLength = length.min,
          maxLength = length.max,
          maybeProperties = None
        )

        val (expandDescriptionPrefix, predicatesDescription) =
          varExpandPredicateDescriptions(nodePredicates, relationshipPredicates)

        PlanDescriptionImpl(
          id,
          "PathPropagatingBFS",
          children,
          Seq(Details(pretty"$expandDescriptionPrefix$expandDescription$predicatesDescription")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case RemoveLabels(_, idName, labelNames) =>
        val prettyId = asPrettyString(idName)
        val prettyLabels = labelNames.map(labelName => asPrettyString(labelName.name)).mkPrettyString(":", ":", "")
        val details = Details(pretty"$prettyId$prettyLabels")
        PlanDescriptionImpl(
          id,
          "RemoveLabels",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SetLabels(_, idName, labelNames) =>
        val prettyId = asPrettyString(idName)
        val prettyLabels = labelNames.map(labelName => asPrettyString(labelName.name)).mkPrettyString(":", ":", "")
        val details = Details(pretty"$prettyId$prettyLabels")
        PlanDescriptionImpl(id, "SetLabels", children, Seq(details), variables, withRawCardinalities, withDistinctness)

      case SetNodePropertiesFromMap(_, idName, expression, removeOtherProps) =>
        val details = Details(setPropertyInfo(asPrettyString(idName), expression, removeOtherProps))
        PlanDescriptionImpl(
          id,
          "SetNodePropertiesFromMap",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SetPropertiesFromMap(_, entity, expression, removeOtherProps) =>
        val details = Details(setPropertyInfo(asPrettyString(entity), expression, removeOtherProps))
        PlanDescriptionImpl(
          id,
          "SetPropertiesFromMap",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SetProperty(_, entity, propertyKey, expression) =>
        val entityString = pretty"${asPrettyString(entity)}.${asPrettyString(propertyKey.name)}"
        val details = Details(setPropertyInfo(entityString, expression, true))
        PlanDescriptionImpl(
          id,
          "SetProperty",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SetNodeProperty(_, idName, propertyKey, expression) =>
        val details = Details(setPropertyInfo(
          pretty"${asPrettyString(idName)}.${asPrettyString(propertyKey.name)}",
          expression,
          true
        ))
        PlanDescriptionImpl(
          id,
          "SetProperty",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SetRelationshipProperty(_, idName, propertyKey, expression) =>
        val details = Details(setPropertyInfo(
          pretty"${asPrettyString(idName)}.${asPrettyString(propertyKey.name)}",
          expression,
          true
        ))
        PlanDescriptionImpl(
          id,
          "SetProperty",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SetProperties(_, entity, items) =>
        val setOps = items.map {
          case (p, v) =>
            val entityString = pretty"${asPrettyString(entity)}.${asPrettyString(p.name)}"
            setPropertyInfo(entityString, v, removeOtherProps = true)
        }.mkPrettyString(", ")
        val details = Details(setOps)
        PlanDescriptionImpl(
          id,
          "SetProperties",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SetNodeProperties(_, idName, items) =>
        val setOps = items.map {
          case (p, v) =>
            val entityString = pretty"${asPrettyString(idName)}.${asPrettyString(p.name)}"
            setPropertyInfo(entityString, v, removeOtherProps = true)
        }.mkPrettyString(", ")
        val details = Details(setOps)
        PlanDescriptionImpl(
          id,
          "SetProperties",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SetRelationshipProperties(_, idName, items) =>
        val setOps = items.map {
          case (p, v) =>
            val entityString = pretty"${asPrettyString(idName)}.${asPrettyString(p.name)}"
            setPropertyInfo(entityString, v, removeOtherProps = true)
        }.mkPrettyString(", ")
        val details = Details(setOps)
        PlanDescriptionImpl(
          id,
          "SetProperties",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SetRelationshipPropertiesFromMap(_, idName, expression, removeOtherProps) =>
        val details = Details(setPropertyInfo(asPrettyString(idName), expression, removeOtherProps))
        PlanDescriptionImpl(
          id,
          "SetRelationshipPropertiesFromMap",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Sort(_, orderBy) =>
        PlanDescriptionImpl(
          id,
          "Sort",
          children,
          Seq(Details(orderInfo(orderBy))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartialSort(_, alreadySortedPrefix, stillToSortSuffix, _) =>
        PlanDescriptionImpl(
          id,
          "PartialSort",
          children,
          Seq(Details(orderInfo(alreadySortedPrefix ++ stillToSortSuffix))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Top(_, orderBy, limit) =>
        val details = pretty"${orderInfo(orderBy)} LIMIT ${asPrettyString(limit)}"
        PlanDescriptionImpl(
          id,
          "Top",
          children,
          Seq(Details(details)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Top1WithTies(_, orderBy) =>
        val details = pretty"${orderInfo(orderBy)}"
        PlanDescriptionImpl(
          id,
          "Top1WithTies",
          children,
          Seq(Details(details)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PartialTop(_, alreadySortedPrefix, stillToSortSuffix, limit, _) =>
        val details = pretty"${orderInfo(alreadySortedPrefix ++ stillToSortSuffix)} LIMIT ${asPrettyString(limit)}"
        PlanDescriptionImpl(
          id,
          "PartialTop",
          children,
          Seq(Details(details)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case UnwindCollection(_, variable, expression) =>
        val details = Details(projectedExpressionInfo(Map(variable -> expression)).mkPrettyString(SEPARATOR))
        PlanDescriptionImpl(id, "Unwind", children, Seq(details), variables, withRawCardinalities, withDistinctness)

      case PartitionedUnwindCollection(_, variable, expression) =>
        val details = Details(projectedExpressionInfo(Map(variable -> expression)).mkPrettyString(SEPARATOR))
        PlanDescriptionImpl(
          id,
          "PartitionedUnwind",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case VarExpand(
          _,
          fromName,
          dir,
          _,
          types,
          toName,
          relName,
          length,
          mode,
          nodePredicates,
          relationshipPredicates
        ) =>
        val expandDescription = expandExpressionDescription(
          fromName,
          Some(relName),
          types.map(_.name),
          toName,
          dir,
          minLength = length.min,
          maxLength = length.max,
          maybeProperties = None
        )
        val (expandDescriptionPrefix, predicatesDescription) =
          varExpandPredicateDescriptions(nodePredicates, relationshipPredicates)
        val modeDescr = expandModeDescription(mode)
        PlanDescriptionImpl(
          id,
          s"VarLengthExpand($modeDescr)",
          children,
          Seq(Details(pretty"$expandDescriptionPrefix$expandDescription$predicatesDescription")),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case CreateIndex(
          _,
          indexType,
          entityName,
          propertyKeyNames,
          nameOption,
          options
        ) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(
          id,
          "CreateIndex",
          children,
          Seq(Details(indexInfo(indexType.name(), nameOption, entityName, propertyKeyNames, options))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case CreateLookupIndex(
          _,
          isNodeIndex,
          nameOption,
          options
        ) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(
          id,
          "CreateIndex",
          children,
          Seq(Details(lookupIndexInfo(nameOption, isNodeIndex, options))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case CreateFulltextIndex(
          _,
          entityNames,
          propertyKeyNames,
          nameOption,
          options
        ) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(
          id,
          "CreateIndex",
          children,
          Seq(Details(fulltextIndexInfo(nameOption, entityNames, propertyKeyNames, options))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case CreateConstraint(
          _,
          constraintType,
          label,
          properties: Seq[Property],
          nameOption,
          options
        ) => // Can be both a leaf plan and a middle plan so need to be in both places
        val entity = properties.head.map.asCanonicalStringVal
        val details = Details(constraintInfo(
          nameOption,
          entity,
          label,
          properties,
          constraintType,
          options
        ))
        PlanDescriptionImpl(
          id,
          "CreateConstraint",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case TriadicBuild(_, sourceId, seenId, _) =>
        val details = Details(pretty"(${asPrettyString(sourceId)})--(${asPrettyString(seenId)})")
        PlanDescriptionImpl(
          id,
          "TriadicBuild",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case TriadicFilter(_, positivePredicate, sourceId, targetId, _) =>
        val positivePredicateString = if (positivePredicate) pretty"" else pretty"NOT "
        val details =
          Details(pretty"WHERE $positivePredicateString(${asPrettyString(sourceId)})--(${asPrettyString(targetId)})")
        PlanDescriptionImpl(
          id,
          "TriadicFilter",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case PreserveOrder(_) =>
        PlanDescriptionImpl(
          id,
          "PreserveOrder",
          children,
          Seq.empty[Argument],
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Foreach(_, variable, expression, mutations) =>
        val details =
          pretty"${asPrettyString(variable)} IN ${asPrettyString(expression)}" +: mutations.map(mutatingPatternString)
        PlanDescriptionImpl(
          id,
          "Foreach",
          children,
          Seq(Details(details)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case ArgumentTracker(_) =>
        PlanDescriptionImpl(id, "ArgumentTracker", children, Seq(), variables, withRawCardinalities, withDistinctness)

      case NullifyMetadata(_, _, _) =>
        PlanDescriptionImpl(
          id,
          "NullifyMetadata",
          children,
          arguments = Seq.empty,
          variables,
          withRawCardinalities,
          withDistinctness
        )
      case x => throw new InternalException(s"Unknown plan type: ${x.getClass.getSimpleName}. Missing a case?")
    }

    addRuntimeAttributes(addPlanningAttributes(result, plan), plan)
  }

  private def expandModeDescription(mode: Expand.ExpansionMode) = {
    mode match {
      case ExpandAll  => "All"
      case ExpandInto => "Into"
    }
  }

  private def callInTxsDetails(
    batchSize: Expression,
    onErrorBehaviour: InTransactionsOnErrorBehaviour,
    maybeReportAs: Option[LogicalVariable]
  ) = {
    val errorParams = onErrorBehaviour match {
      case OnErrorContinue => " ON ERROR CONTINUE"
      case OnErrorBreak    => " ON ERROR BREAK"
      case OnErrorFail     => " ON ERROR FAIL"
    }
    val reportParams = maybeReportAs.fold("")(status => s" REPORT STATUS AS ${status.name}")

    Details(
      pretty"IN TRANSACTIONS OF ${asPrettyString(batchSize)} ROWS${asPrettyString.raw(errorParams)}${asPrettyString.raw(reportParams)}"
    )
  }

  override def onTwoChildPlan(
    plan: LogicalPlan,
    lhs: InternalPlanDescription,
    rhs: InternalPlanDescription
  ): InternalPlanDescription = {
    checkOnlyWhenAssertionsAreEnabled(plan.lhs.nonEmpty)
    checkOnlyWhenAssertionsAreEnabled(plan.rhs.nonEmpty)

    val id = plan.id
    val variables = plan.availableSymbols.map(asPrettyString(_))
    val children = TwoChildren(lhs, rhs)

    val result: InternalPlanDescription = plan match {
      case _: AntiConditionalApply =>
        PlanDescriptionImpl(
          id,
          "AntiConditionalApply",
          children,
          Seq.empty,
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case _: AntiSemiApply =>
        PlanDescriptionImpl(id, "AntiSemiApply", children, Seq.empty, variables, withRawCardinalities, withDistinctness)

      case _: ConditionalApply =>
        PlanDescriptionImpl(
          id,
          "ConditionalApply",
          children,
          Seq.empty,
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case _: Apply =>
        PlanDescriptionImpl(id, "Apply", children, Seq.empty, variables, withRawCardinalities, withDistinctness)

      case AssertSameNode(node, _, _) =>
        PlanDescriptionImpl(
          id,
          "AssertSameNode",
          children,
          Seq(Details(asPrettyString(node))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case AssertSameRelationship(idName, _, _) =>
        PlanDescriptionImpl(
          id,
          "AssertSameRelationship",
          children,
          Seq(Details(asPrettyString(idName))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case CartesianProduct(_, _) =>
        PlanDescriptionImpl(
          id,
          "CartesianProduct",
          children,
          Seq.empty,
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case NodeHashJoin(nodes, _, _) =>
        PlanDescriptionImpl(
          id,
          "NodeHashJoin",
          children,
          Seq(Details(keyNamesInfo(nodes.toSeq))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case ForeachApply(_, _, variable, expression) =>
        val details = pretty"${asPrettyString(variable)} IN ${asPrettyString(expression)}"
        PlanDescriptionImpl(
          id,
          "Foreach",
          children,
          Seq(Details(details)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case LetSelectOrSemiApply(_, _, _, predicate) =>
        PlanDescriptionImpl(
          id,
          "LetSelectOrSemiApply",
          children,
          Seq(Details(asPrettyString(predicate))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case row: plans.Argument =>
        ArgumentPlanDescription(id = plan.id, Seq.empty, variables)

      case LetSelectOrAntiSemiApply(_, _, _, predicate) =>
        PlanDescriptionImpl(
          id,
          "LetSelectOrAntiSemiApply",
          children,
          Seq(Details(asPrettyString(predicate))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case LetSemiApply(_, _, idName) =>
        PlanDescriptionImpl(
          id,
          "LetSemiApply",
          children,
          Seq(Details(asPrettyString(idName))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case _: LetAntiSemiApply =>
        PlanDescriptionImpl(
          id,
          "LetAntiSemiApply",
          children,
          Seq.empty,
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case LeftOuterHashJoin(nodes, _, _) =>
        PlanDescriptionImpl(
          id,
          "NodeLeftOuterHashJoin",
          children,
          Seq(Details(keyNamesInfo(nodes.toSeq))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case RightOuterHashJoin(nodes, _, _) =>
        PlanDescriptionImpl(
          id,
          "NodeRightOuterHashJoin",
          children,
          Seq(Details(keyNamesInfo(nodes.toSeq))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case RollUpApply(_, _, collectionName, variableToCollect) =>
        val detailsList = Seq(collectionName, variableToCollect).map(e => keyNamesInfo(Seq(e)))
        PlanDescriptionImpl(
          id,
          "RollUpApply",
          children,
          Seq(Details(detailsList)),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SelectOrAntiSemiApply(_, _, predicate) =>
        PlanDescriptionImpl(
          id,
          "SelectOrAntiSemiApply",
          children,
          Seq(Details(asPrettyString(predicate))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case SelectOrSemiApply(_, _, predicate) =>
        PlanDescriptionImpl(
          id,
          "SelectOrSemiApply",
          children,
          Seq(Details(asPrettyString(predicate))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case _: SemiApply =>
        PlanDescriptionImpl(id, "SemiApply", children, Seq.empty, variables, withRawCardinalities, withDistinctness)

      case TransactionForeach(_, _, batchSize, onErrorBehaviour, maybeReportAs) =>
        val details = callInTxsDetails(batchSize, onErrorBehaviour, maybeReportAs)
        PlanDescriptionImpl(
          id,
          "TransactionForeach",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case TransactionApply(_, _, batchSize, onErrorBehaviour, maybeReportAs) =>
        val details = callInTxsDetails(batchSize, onErrorBehaviour, maybeReportAs)
        PlanDescriptionImpl(
          id,
          "TransactionApply",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case TriadicSelection(_, _, positivePredicate, source, seen, target) =>
        val positivePredicateString = if (positivePredicate) pretty"" else pretty"NOT "
        val details =
          Details(pretty"WHERE $positivePredicateString(${asPrettyString(source)})--(${asPrettyString(target)})")
        PlanDescriptionImpl(
          id,
          "TriadicSelection",
          children,
          Seq(details),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case _: Union =>
        PlanDescriptionImpl(id, "Union", children, Seq.empty, variables, withRawCardinalities, withDistinctness)

      case _: OrderedUnion =>
        PlanDescriptionImpl(id, "OrderedUnion", children, Seq.empty, variables)

      case ValueHashJoin(_, _, predicate) =>
        PlanDescriptionImpl(
          id = id,
          name = "ValueHashJoin",
          children = children,
          arguments = Seq(Details(asPrettyString(predicate))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case _: MultiNodeIndexSeek | _: AssertingMultiNodeIndexSeek | _: SubqueryForeach =>
        PlanDescriptionImpl(
          id = plan.id,
          plan.productPrefix,
          children,
          Seq.empty,
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case Trail(_, _, repetition, start, end, _, _, _, _, _, _, _, _) =>
        PlanDescriptionImpl(
          id = plan.id,
          "Repeat(Trail)",
          children,
          Seq(Details(repeatDetails(repetition, start, end))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case BidirectionalRepeatTrail(_, _, repetition, start, end, _, _, _, _, _, _, _, _) =>
        PlanDescriptionImpl(
          id = plan.id,
          "BidirectionalRepeat(Trail)",
          children,
          Seq(Details(repeatDetails(repetition, start, end))),
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case RepeatOptions(_, _) =>
        PlanDescriptionImpl(
          id = plan.id,
          "RepeatOptions",
          children,
          Seq.empty,
          variables,
          withRawCardinalities,
          withDistinctness
        )

      case x => throw new InternalException(s"Unknown plan type: ${x.getClass.getSimpleName}. Missing a case?")
    }

    addRuntimeAttributes(addPlanningAttributes(result, plan), plan)
  }

  private def repeatDetails(repetition: Repetition, start: LogicalVariable, end: LogicalVariable): PrettyString = {
    val repString = repetition match {
      case Repetition(min, Limited(n)) =>
        pretty"{${asPrettyString.raw(min.toString)}, ${asPrettyString.raw(n.toString)}}"
      case Repetition(min, Unlimited) =>
        pretty"{${asPrettyString.raw(min.toString)}, *}"
    }
    pretty"(${asPrettyString(start.name)}) (...)$repString (${asPrettyString(end.name)})"
  }

  private def addPlanningAttributes(
    description: InternalPlanDescription,
    plan: LogicalPlan
  ): InternalPlanDescription = {
    Function.chain[InternalPlanDescription](Seq(
      if (effectiveCardinalities.isDefinedAt(plan.id)) {
        val effectiveCardinality = effectiveCardinalities.get(plan.id)
        _.addArgument(EstimatedRows(
          effectiveCardinality.amount,
          effectiveCardinality.originalCardinality.map(_.amount)
        ))
      } else {
        identity
      },
      if (providedOrders.isDefinedAt(plan.id) && !providedOrders(plan.id).isEmpty) {
        _.addArgument(asPrettyString.order(providedOrders(plan.id)))
      } else {
        identity
      },
      if (withDistinctness) {
        _.addArgument(asPrettyString.distinctness(plan.distinctness))
      } else {
        identity
      }
    ))(description)
  }

  private def addRuntimeAttributes(description: InternalPlanDescription, plan: LogicalPlan): InternalPlanDescription = {
    runtimeOperatorMetadata(plan.id).foldLeft(description)((acc, x) => acc.addArgument(x))
  }

  /**
   * @return (a prefix to put before the expand description, a description of the predicates to put after the expand description)
   */
  private def varExpandPredicateDescriptions(
    nodePredicates: Seq[VariablePredicate],
    relationshipPredicates: Seq[VariablePredicate],
    pathName: PrettyString = pretty"p"
  ): (PrettyString, PrettyString) = {
    val predicateStrings = nodePredicates.map(buildNodePredicatesDescription(_, pathName)) ++
      relationshipPredicates.map(buildRelationshipPredicatesDescription(_, pathName))

    val (expandDescriptionPrefix, predicatesDescription) =
      if (predicateStrings.isEmpty) {
        (pretty"", pretty"")
      } else {
        (pretty"$pathName = ", predicateStrings.mkPrettyString(" WHERE ", " AND ", ""))
      }
    (expandDescriptionPrefix, predicatesDescription)
  }

  private def buildNodePredicatesDescription(nodePredicate: VariablePredicate, pathName: PrettyString): PrettyString = {
    val nodePredicateInfo = asPrettyString(nodePredicate.predicate)
    val predVar = asPrettyString(nodePredicate.variable)
    pretty"all($predVar IN nodes($pathName) WHERE $nodePredicateInfo)"
  }

  private def buildRelationshipPredicatesDescription(
    relPredicate: VariablePredicate,
    pathName: PrettyString
  ): PrettyString = {
    val relPredicateInfo = asPrettyString(relPredicate.predicate)
    val predVar = asPrettyString(relPredicate.variable)
    pretty"all($predVar IN relationships($pathName) WHERE $relPredicateInfo)"
  }

  private def getNodeIndexDescriptions(
    idName: String,
    label: LabelToken,
    propertyKeys: Seq[PropertyKeyToken],
    indexType: IndexType,
    valueExpr: QueryExpression[expressions.Expression],
    unique: Boolean,
    readOnly: Boolean,
    caches: Seq[expressions.Expression]
  ): (String, PrettyString) = {

    val name = nodeIndexOperatorName(valueExpr, unique, readOnly)
    val predicate = indexPredicateString(propertyKeys, valueExpr)
    val info = nodeIndexInfoString(idName, unique, label, propertyKeys, indexType, predicate, caches)

    (name, info)
  }

  private def getRelIndexDescriptions(
    idName: String,
    start: String,
    typeToken: RelationshipTypeToken,
    end: String,
    isDirected: Boolean,
    propertyKeys: Seq[PropertyKeyToken],
    indexType: IndexType,
    valueExpr: QueryExpression[expressions.Expression],
    unique: Boolean,
    readOnly: Boolean,
    caches: Seq[expressions.Expression]
  ): (String, PrettyString) = {

    val name = relationshipIndexOperatorName(valueExpr, unique, readOnly, isDirected)
    val predicate = indexPredicateString(propertyKeys, valueExpr)
    val info = relIndexInfoString(idName, start, typeToken, end, isDirected, propertyKeys, indexType, predicate, caches)

    (name, info)
  }

  private def nodeIndexOperatorName(
    valueExpr: QueryExpression[expressions.Expression],
    unique: Boolean,
    readOnly: Boolean
  ): String = {
    indexOperatorName(NodeIndexSeek, valueExpr, unique, readOnly)
  }

  private def relationshipIndexOperatorName(
    valueExpr: QueryExpression[expressions.Expression],
    unique: Boolean,
    readOnly: Boolean,
    directed: Boolean
  ): String = {
    val indexSeekNames = if (directed) DirectedRelationshipIndexSeek else UndirectedRelationshipIndexSeek
    indexOperatorName(indexSeekNames, valueExpr, unique, readOnly)
  }

  private def indexOperatorName(
    indexSeekNames: IndexSeekNames,
    valueExpr: QueryExpression[expressions.Expression],
    unique: Boolean,
    readOnly: Boolean
  ): String = {
    def findName(exactOnly: Boolean = true) =
      if (unique && !readOnly && exactOnly) {
        indexSeekNames.PLAN_DESCRIPTION_UNIQUE_LOCKING_INDEX_SEEK_NAME
      } else if (unique) {
        indexSeekNames.PLAN_DESCRIPTION_UNIQUE_INDEX_SEEK_NAME
      } else {
        indexSeekNames.PLAN_DESCRIPTION_INDEX_SEEK_NAME
      }

    valueExpr match {
      case _: ExistenceQueryExpression[expressions.Expression] =>
        indexSeekNames.PLAN_DESCRIPTION_INDEX_SCAN_NAME
      case _: RangeQueryExpression[expressions.Expression] =>
        if (unique) indexSeekNames.PLAN_DESCRIPTION_UNIQUE_INDEX_SEEK_RANGE_NAME
        else indexSeekNames.PLAN_DESCRIPTION_INDEX_SEEK_RANGE_NAME
      case e: CompositeQueryExpression[expressions.Expression] =>
        findName(e.exactOnly)
      case _ =>
        findName()
    }
  }

  private def indexPredicateString(
    propertyKeys: Seq[PropertyKeyToken],
    valueExpr: QueryExpression[expressions.Expression]
  ): PrettyString = valueExpr match {
    case _: ExistenceQueryExpression[expressions.Expression] =>
      pretty"${asPrettyString(propertyKeys.head.name)} IS NOT NULL"

    case e: RangeQueryExpression[expressions.Expression] =>
      checkOnlyWhenAssertionsAreEnabled(propertyKeys.size == 1)
      e.expression match {
        case PrefixSeekRangeWrapper(range) =>
          val propertyKeyName = asPrettyString(propertyKeys.head.name)
          pretty"$propertyKeyName STARTS WITH ${asPrettyString(range.prefix)}"

        case InequalitySeekRangeWrapper(RangeLessThan(bounds)) =>
          bounds.map(rangeBoundString(propertyKeys.head, _, '<')).toIndexedSeq.mkPrettyString(" AND ")

        case InequalitySeekRangeWrapper(RangeGreaterThan(bounds)) =>
          bounds.map(rangeBoundString(propertyKeys.head, _, '>')).toIndexedSeq.mkPrettyString(" AND ")

        case InequalitySeekRangeWrapper(RangeBetween(greaterThanBounds, lessThanBounds)) =>
          val gtBoundString = greaterThanBounds.bounds.map(rangeBoundString(propertyKeys.head, _, '>'))
          val ltBoundStrings = lessThanBounds.bounds.map(rangeBoundString(propertyKeys.head, _, '<'))
          (gtBoundString ++ ltBoundStrings).toIndexedSeq.mkPrettyString(" AND ")

        case PointDistanceSeekRangeWrapper(PointDistanceRange(point, distance, inclusive)) =>
          val poi = prettyPoint(point)
          val propertyKeyName = asPrettyString(propertyKeys.head.name)
          val distanceStr = asPrettyString(distance)
          pretty"point.distance($propertyKeyName, $poi) <${if (inclusive) pretty"=" else pretty""} $distanceStr"

        case PointBoundingBoxSeekRangeWrapper(PointBoundingBoxRange(ll, ur)) =>
          val pll = prettyPoint(ll)
          val pur = prettyPoint(ur)
          val propertyKeyName = asPrettyString(propertyKeys.head.name)
          pretty"point.withinBBox($propertyKeyName, $pll, $pur)"
        case _ =>
          throw new IllegalStateException("The expression did not confomr to the expected type RangeQueryExpression")
      }

    case e: SingleQueryExpression[expressions.Expression] =>
      val propertyKeyName = asPrettyString(propertyKeys.head.name)
      pretty"$propertyKeyName = ${asPrettyString(e.expression)}"

    case e: ManyQueryExpression[expressions.Expression] =>
      val (eqOp, innerExp) = e.expression match {
        case ll @ ListLiteral(es) =>
          if (es.size == 1) (pretty"=", es.head) else (pretty"IN", ll)
        // This case is used for example when the expression in a parameter
        case x => (pretty"IN", x)
      }
      val propertyKeyName = asPrettyString(propertyKeys.head.name)
      pretty"$propertyKeyName $eqOp ${asPrettyString(innerExp)}"

    case e: CompositeQueryExpression[expressions.Expression] =>
      val predicates = e.inner.zipWithIndex.map {
        case (exp, i) => indexPredicateString(Seq(propertyKeys(i)), exp)
      }
      predicates.mkPrettyString(" AND ")
  }

  private def rangeBoundString(
    propertyKey: PropertyKeyToken,
    bound: Bound[expressions.Expression],
    sign: Char
  ): PrettyString = {
    pretty"${asPrettyString(propertyKey.name)} ${asPrettyString.raw(s"$sign${bound.inequalitySignSuffix}")} ${asPrettyString(bound.endPoint)}"
  }

  private def prettyPoint(point: Expression) = {
    val funcName = Point.name
    point match {
      case FunctionInvocation(Namespace(List()), FunctionName(`funcName`), _, Seq(MapExpression(args)), _) =>
        pretty"point(${args.map(_._2).map(asPrettyString(_)).mkPrettyString(", ")})"
      case _ => asPrettyString(point)
    }
  }

  private def nodeCountFromCountStoreInfo(ident: LogicalVariable, labelNames: List[Option[LabelName]]): PrettyString = {
    val nodes = labelNames.map {
      case Some(label) => pretty"(:${asPrettyString(label.name)})"
      case None        => pretty"()"
    }.mkPrettyString(", ")
    pretty"count( $nodes ) AS ${asPrettyString(ident.name)}"
  }

  private def relationshipCountFromCountStoreInfo(
    ident: LogicalVariable,
    startLabel: Option[LabelName],
    typeNames: Seq[RelTypeName],
    endLabel: Option[LabelName]
  ): PrettyString = {
    val start = startLabel
      .map(_.name)
      .map(asPrettyString(_))
      .map(l => pretty":$l")
      .getOrElse(pretty"")
    val end = endLabel
      .map(_.name)
      .map(asPrettyString(_))
      .map(l => pretty":$l")
      .getOrElse(pretty"")
    val types =
      if (typeNames.nonEmpty) {
        typeNames
          .map(_.name)
          .map(asPrettyString(_))
          .mkPrettyString(":", "|", "")
      } else {
        pretty""
      }

    pretty"count( ($start)-[$types]->($end) ) AS ${asPrettyString(ident.name)}"
  }

  private def relationshipByIdSeekInfo(
    idName: String,
    relIds: SeekableArgs,
    startNode: String,
    endNode: String,
    isDirectional: Boolean,
    functionName: String
  ): PrettyString = {
    val predicate = seekableArgsInfo(relIds)
    val directionString = if (isDirectional) pretty">" else pretty""
    val prettyStartNode = asPrettyString(startNode)
    val prettyIdName = asPrettyString(idName)
    val prettyEndNode = asPrettyString(endNode)
    pretty"(${prettyStartNode})-[$prettyIdName]-$directionString($prettyEndNode) WHERE ${asPrettyString(functionName)}($prettyIdName) $predicate"
  }

  private def seekableArgsInfo(seekableArgs: SeekableArgs): PrettyString = seekableArgs match {
    case ManySeekableArgs(ListLiteral(exprs)) if exprs.size == 1 =>
      pretty"= ${asPrettyString(exprs.head)}"
    case ManySeekableArgs(expr) =>
      pretty"IN ${asPrettyString(expr)}"
    case SingleSeekableArg(expr) =>
      pretty"= ${asPrettyString(expr)}"
  }

  private def signatureInfo(call: ResolvedCall): PrettyString = {
    val argString = call.callArguments.map(asPrettyString(_)).mkPrettyString(SEPARATOR)
    val resultString = call.callResultTypes
      .map { case (name, typ) =>
        pretty"${asPrettyString(name)} :: ${asPrettyString.raw(typ.normalizedCypherTypeString())}"
      }
      .mkPrettyString(SEPARATOR)
    pretty"${asPrettyString.raw(call.qualifiedName.toString)}($argString) :: ($resultString)"
  }

  private def orderInfo(orderBy: Seq[ColumnOrder]): PrettyString = {
    orderBy.map {
      case Ascending(id)  => pretty"${asPrettyString(id)} ASC"
      case Descending(id) => pretty"${asPrettyString(id)} DESC"
    }.mkPrettyString(SEPARATOR)
  }

  private def eagernessReasonInfo(reasons: ListSet[EagernessReason]): Seq[PrettyString] = {
    reasons.toSeq.flatMap(eagernessReasonDetails).sorted
  }

  private def eagernessReasonDetails(reason: EagernessReason): Seq[PrettyString] = reason match {
    case r: EagernessReason.NonUnique =>
      Seq(formatEagernessReason(nonUniqueEagernessReasonDetails(r)))
    case r: EagernessReason.ReasonWithConflict =>
      Seq(formatEagernessReason(nonUniqueEagernessReasonDetails(r.reason), Some(r.conflict)))
    case EagernessReason.UpdateStrategyEager =>
      Seq(formatEagernessReason(pretty"updateStrategy=eager"))
    case EagernessReason.WriteAfterCallInTransactions =>
      Seq(formatEagernessReason(pretty"write after CALL { ... } IN TRANSACTIONS"))
    case EagernessReason.Summarized(summary) =>
      summary.map { case (reason, EagernessReason.SummaryEntry(conflict, count)) =>
        formatEagernessReason(
          nonUniqueEagernessReasonDetails(reason),
          Some(conflict),
          Option.when(count > 1)(count - 1)
        )
      }.toSeq
    case EagernessReason.ProcedureCallEager =>
      Seq(formatEagernessReason(pretty"Eager ProcedureCall"))
    case EagernessReason.Unknown =>
      Seq.empty
  }

  private def formatEagernessReason(
    detail: PrettyString,
    conflict: Option[EagernessReason.Conflict] = None,
    extraCount: Option[Int] = None
  ): PrettyString = {
    val extraCountStr = extraCount.fold("")(c => s", and $c more conflicting operators")
    val conflictStr = conflict.fold("")(c => s" (${conflictInfo(c)}$extraCountStr)")
    pretty"$detail${asPrettyString.raw(conflictStr)}"
  }

  private def nonUniqueEagernessReasonDetails(reason: EagernessReason.NonUnique): PrettyString = {
    reason match {
      case EagernessReason.LabelReadSetConflict(label) =>
        pretty"read/set conflict for label: ${asPrettyString(label)}"
      case EagernessReason.TypeReadSetConflict(label) =>
        (pretty"read/set conflict for relationship type: ${asPrettyString(label)}")
      case EagernessReason.LabelReadRemoveConflict(label) =>
        (pretty"read/remove conflict for label: ${asPrettyString(label)}")
      case EagernessReason.ReadDeleteConflict(identifier) =>
        (pretty"read/delete conflict for variable: ${asPrettyString(identifier)}")
      case EagernessReason.ReadCreateConflict =>
        (pretty"read/create conflict")
      case EagernessReason.PropertyReadSetConflict(property) =>
        (pretty"read/set conflict for property: ${asPrettyString(property)}")
      case EagernessReason.UnknownPropertyReadSetConflict =>
        (pretty"read/set conflict for some property")
    }
  }

  private def conflictInfo(conflict: EagernessReason.Conflict): String =
    s"Operator: ${conflict.first.x} vs ${conflict.second.x}"

  private def expandExpressionDescription(
    from: LogicalVariable,
    maybeRelName: Option[LogicalVariable],
    relTypes: Seq[String],
    to: LogicalVariable,
    direction: SemanticDirection,
    patternLength: PatternLength
  ): PrettyString = {
    val (min, maybeMax) = patternLength match {
      case SimplePatternLength             => (1, Some(1))
      case VarPatternLength(min, maybeMax) => (min, maybeMax)
    }

    expandExpressionDescription(from, maybeRelName, relTypes, to, direction, min, maybeMax, None)
  }

  private def createNodeDescription(cn: CreateNode) = {
    val CreateNode(node, labels, properties) = cn
    val separator = if (labels.isEmpty) pretty": " else pretty" "
    val labelsString =
      if (labels.nonEmpty) labels.map(x => asPrettyString(x.name)).mkPrettyString(":", ":", "") else pretty""
    val propsString = properties.map(p => pretty"$separator${asPrettyString(p)}").getOrElse(pretty"")
    pretty"(${asPrettyString(node)}$labelsString$propsString)"
  }

  private def expandExpressionDescription(
    from: LogicalVariable,
    maybeRelName: Option[LogicalVariable],
    relTypes: Seq[String],
    to: LogicalVariable,
    direction: SemanticDirection,
    minLength: Int,
    maxLength: Option[Int],
    maybeProperties: Option[Expression]
  ): PrettyString = {
    val left = if (direction == SemanticDirection.INCOMING) pretty"<-" else pretty"-"
    val right = if (direction == SemanticDirection.OUTGOING) pretty"->" else pretty"-"
    val types = if (relTypes.isEmpty) pretty"" else relTypes.map(asPrettyString(_)).mkPrettyString(":", "|", "")
    val separator = if (relTypes.isEmpty) pretty": " else pretty" "
    val propsString = maybeProperties.map(p => pretty"$separator${asPrettyString(p)}").getOrElse(pretty"")
    val lengthDescr: PrettyString = (minLength, maxLength) match {
      case (1, Some(1)) => pretty""
      case (1, None)    => pretty"*"
      case (1, Some(m)) => pretty"*..${asPrettyString.raw(m.toString)}"
      case (`minLength`, Some(`minLength`)) =>
        pretty"*${asPrettyString.raw(minLength.toString)}"
      case _ =>
        pretty"*${asPrettyString.raw(minLength.toString)}..${asPrettyString.raw(maxLength.map(_.toString).getOrElse(""))}"
    }
    val relName = asPrettyString(maybeRelName.map(_.name).getOrElse(""))
    val relInfo =
      if (lengthDescr == pretty"" && relTypes.isEmpty && relName.prettifiedString.isEmpty) pretty""
      else pretty"[$relName$types$lengthDescr$propsString]"
    pretty"(${asPrettyString(from)})$left$relInfo$right(${asPrettyString(to)})"
  }

  private def nodeIndexInfoString(
    idName: String,
    unique: Boolean,
    label: NameToken[_],
    propertyKeys: Seq[PropertyKeyToken],
    indexType: IndexType,
    predicate: PrettyString,
    caches: Seq[expressions.Expression]
  ): PrettyString = {
    val indexStr = if (unique) pretty"UNIQUE " else pretty"${asPrettyString(indexType.name())} INDEX "
    val propertyKeyString = propertyKeys.map(x => asPrettyString(x.name)).mkPrettyString(SEPARATOR)
    pretty"$indexStr${asPrettyString(idName)}:${asPrettyString(label.name)}($propertyKeyString) WHERE $predicate${cachesSuffix(caches)}"
  }

  private def relIndexInfoString(
    idName: String,
    start: String,
    relType: NameToken[_],
    end: String,
    isDirected: Boolean,
    propertyKeys: Seq[PropertyKeyToken],
    indexType: IndexType,
    predicate: PrettyString,
    caches: Seq[expressions.Expression]
  ): PrettyString = {
    val propertyKeyString = propertyKeys.map(x => asPrettyString(x.name)).mkPrettyString(SEPARATOR)
    val left = pretty"-"
    val right = if (isDirected) pretty"->" else pretty"-"
    val relInfo = pretty"[${asPrettyString(idName)}:${asPrettyString(relType.name)}($propertyKeyString)]"
    val pattern = pretty"(${asPrettyString(start)})$left$relInfo$right(${asPrettyString(end)})"
    pretty"${asPrettyString(indexType.name())} INDEX $pattern WHERE $predicate${cachesSuffix(caches)}"
  }

  private def aggregationInfo(
    groupingExpressions: Map[LogicalVariable, Expression],
    aggregationExpressions: Map[LogicalVariable, Expression],
    ordered: Seq[Expression] = Seq.empty
  ): PrettyString = {
    val sanitizedOrdered = ordered.map(asPrettyString(_)).toIndexedSeq
    val groupingInfo = projectedExpressionInfo(groupingExpressions, sanitizedOrdered)
    val aggregatingInfo = projectedExpressionInfo(aggregationExpressions)
    (groupingInfo ++ aggregatingInfo).mkPrettyString(SEPARATOR)
  }

  private def projectedExpressionInfo(
    expressions: Map[LogicalVariable, Expression],
    ordered: IndexedSeq[PrettyString] = IndexedSeq.empty
  ): Seq[PrettyString] = {
    expressions.toList.map { case (k, v) =>
      val key = asPrettyString(k)
      val value = asPrettyString(v)
      (key, value)
    }.sortBy {
      case (key, _) if ordered.contains(key)     => ordered.indexOf(key)
      case (_, value) if ordered.contains(value) => ordered.indexOf(value)
      case _                                     => Int.MaxValue
    }.map { case (key, value) => if (key == value) key else pretty"$value AS $key" }
  }

  private def keyNamesInfo(keys: Iterable[LogicalVariable]): PrettyString = {
    keys
      .map(asPrettyString(_))
      .mkPrettyString(SEPARATOR)
  }

  private def cachesSuffix(caches: Seq[expressions.Expression]): PrettyString = {
    if (caches.isEmpty) pretty"" else caches.map(asPrettyString(_)).mkPrettyString(", ", ", ", "")
  }

  private def indexInfo(
    indexType: String,
    nameOption: Option[Either[String, Parameter]],
    entityName: ElementTypeName,
    properties: Seq[PropertyKeyName],
    options: Options
  ): PrettyString = {
    val name = nameOption.map(n => pretty" ${PrettyString(Prettifier.escapeName(n))}").getOrElse(pretty"")
    val propertyString = properties.map(asPrettyString(_)).mkPrettyString("(", SEPARATOR, ")")
    val pattern = entityName match {
      case label: LabelName =>
        val prettyLabel = asPrettyString(label.name)
        pretty"(:$prettyLabel)"
      case relType: RelTypeName =>
        val prettyType = asPrettyString(relType.name)
        pretty"()-[:$prettyType]-()"
    }
    pretty"${asPrettyString.raw(indexType)} INDEX$name FOR $pattern ON $propertyString${prettyOptions(options)}"
  }

  private def fulltextIndexInfo(
    nameOption: Option[Either[String, Parameter]],
    entityNames: Either[List[LabelName], List[RelTypeName]],
    properties: Seq[PropertyKeyName],
    options: Options
  ): PrettyString = {
    val name = nameOption.map(n => pretty" ${PrettyString(Prettifier.escapeName(n))}").getOrElse(pretty"")
    val propertyString = properties.map(asPrettyString(_)).mkPrettyString("[", SEPARATOR, "]")
    val pattern = entityNames match {
      case Left(labels) =>
        val innerPattern = labels.map(l => asPrettyString(l.name)).mkPrettyString(":", "|", "")
        pretty"($innerPattern)"
      case Right(relTypes) =>
        val innerPattern = relTypes.map(r => asPrettyString(r.name)).mkPrettyString(":", "|", "")
        pretty"()-[$innerPattern]-()"
    }
    pretty"FULLTEXT INDEX$name FOR $pattern ON EACH $propertyString${prettyOptions(options)}"
  }

  private def lookupIndexInfo(
    nameOption: Option[Either[String, Parameter]],
    entityType: EntityType,
    options: Options
  ): PrettyString = {
    val name = nameOption.map(n => pretty" ${PrettyString(Prettifier.escapeName(n))}").getOrElse(pretty"")
    val (pattern, function) = entityType match {
      case EntityType.NODE         => (pretty"(n)", pretty"${asPrettyString.raw(Labels.name)}(n)")
      case EntityType.RELATIONSHIP => (pretty"()-[r]-()", pretty"${asPrettyString.raw(Type.name)}(r)")
    }
    pretty"LOOKUP INDEX$name FOR $pattern ON EACH $function${prettyOptions(options)}"
  }

  private def constraintInfo(
    nameOption: Option[Either[String, Parameter]],
    entity: String,
    entityName: ElementTypeName,
    properties: Seq[Property],
    constraintType: ConstraintType,
    options: Options = NoOptions,
    useForAndRequire: Boolean = true
  ): PrettyString = {
    val name = nameOption.map(n => pretty" ${PrettyString(Prettifier.escapeName(n))}").getOrElse(pretty"")
    val assertion = constraintType match {
      case NodePropertyExistence | RelationshipPropertyExistence => "IS NOT NULL"
      case NodeKey                                               => "IS NODE KEY"
      case RelationshipKey                                       => "IS RELATIONSHIP KEY"
      case NodeUniqueness | RelationshipUniqueness               => "IS UNIQUE"
      case NodePropertyType(t)                                   => s"IS :: ${t.description}"
      case RelationshipPropertyType(t)                           => s"IS :: ${t.description}"
    }
    val prettyAssertion = asPrettyString.raw(assertion)

    val propertyString = properties.map(asPrettyString(_)).mkPrettyString("(", SEPARATOR, ")")
    val prettyEntity = asPrettyString(entity)

    val entityInfo = entityName match {
      case label: LabelName     => pretty"($prettyEntity:${asPrettyString(label)})"
      case relType: RelTypeName => pretty"()-[$prettyEntity:${asPrettyString(relType)}]-()"
    }
    val onOrFor = if (useForAndRequire) pretty"FOR" else pretty"ON"
    val assertOrRequire = if (useForAndRequire) pretty"REQUIRE" else pretty"ASSERT"

    pretty"CONSTRAINT$name $onOrFor $entityInfo $assertOrRequire $propertyString $prettyAssertion${prettyOptions(options)}"
  }

  private def prettyOptions(options: Options): PrettyString = options match {
    case NoOptions               => pretty""
    case OptionsParam(parameter) => pretty" OPTIONS ${asPrettyString(parameter)}"
    case OptionsMap(options) =>
      pretty" OPTIONS ${
          options.map({
            case (s, e) => pretty"${asPrettyString(s)}: ${asPrettyString(e)}"
          }).mkPrettyString("{", SEPARATOR, "}")
        }"
  }

  private def setPropertyInfo(idName: PrettyString, expression: Expression, removeOtherProps: Boolean): PrettyString = {
    val setString = if (removeOtherProps) pretty"=" else pretty"+="

    pretty"$idName $setString ${asPrettyString(expression)}"
  }

  def mutatingPatternString(setOp: SimpleMutatingPattern): PrettyString = setOp match {
    case CreatePattern(commands) =>
      val details = commands.map {
        case c: CreateNode => createNodeDescription(c)
        case CreateRelationship(relationship, startNode, typ, endNode, direction, properties) =>
          expandExpressionDescription(
            startNode,
            Some(relationship),
            Seq(typ.name),
            endNode,
            direction,
            1,
            Some(1),
            properties
          )
      }
      pretty"CREATE ${details.mkPrettyString(", ")}"
    case org.neo4j.cypher.internal.ir.DeleteExpression(toDelete, forced) =>
      if (forced) pretty"DETACH DELETE ${asPrettyString(toDelete)}" else pretty"DELETE ${asPrettyString(toDelete)}"
    case SetLabelPattern(node, labelNames) =>
      val prettyId = asPrettyString(node)
      val prettyLabels = labelNames.map(labelName => asPrettyString(labelName.name)).mkPrettyString(":", ":", "")
      pretty"SET $prettyId$prettyLabels"
    case RemoveLabelPattern(node, labelNames) =>
      val prettyId = asPrettyString(node)
      val prettyLabels = labelNames.map(labelName => asPrettyString(labelName.name)).mkPrettyString(":", ":", "")
      pretty"REMOVE $prettyId$prettyLabels"
    case SetNodePropertyPattern(node, propertyKey, value) =>
      pretty"SET ${setPropertyInfo(pretty"${asPrettyString(node)}.${asPrettyString(propertyKey.name)}", value, removeOtherProps = true)}"
    case SetNodePropertiesFromMapPattern(node, value, removeOtherProps) =>
      pretty"SET ${setPropertyInfo(asPrettyString(node), value, removeOtherProps)}"
    case SetRelationshipPropertyPattern(relationship, propertyKey, value) =>
      pretty"SET ${setPropertyInfo(pretty"${asPrettyString(relationship)}.${asPrettyString(propertyKey.name)}", value, removeOtherProps = true)}"
    case SetRelationshipPropertiesFromMapPattern(relationship, value, removeOtherProps) =>
      pretty"SET ${setPropertyInfo(asPrettyString(relationship), value, removeOtherProps)}"
    case SetPropertyPattern(entity, propertyKey, expression) =>
      val entityString = pretty"${asPrettyString(entity)}.${asPrettyString(propertyKey.name)}"
      pretty"SET ${setPropertyInfo(entityString, expression, true)}"
    case SetPropertiesFromMapPattern(entity, expression, removeOtherProps) =>
      pretty"SET ${setPropertyInfo(asPrettyString(entity), expression, removeOtherProps)}"
    case SetPropertiesPattern(entity, items) =>
      val setOps = items.map {
        case (p, e) =>
          val entityString = pretty"${asPrettyString(entity)}.${asPrettyString(p.name)}"
          setPropertyInfo(entityString, e, removeOtherProps = true)
      }.mkPrettyString(", ")
      pretty"SET $setOps"
    case SetNodePropertiesPattern(entity, items) =>
      val setOps = items.map {
        case (p, e) =>
          val entityString = pretty"${asPrettyString(entity)}.${asPrettyString(p.name)}"
          setPropertyInfo(entityString, e, removeOtherProps = true)
      }.mkPrettyString(", ")
      pretty"SET $setOps"
    case SetRelationshipPropertiesPattern(entity, items) =>
      val setOps = items.map {
        case (p, e) =>
          val entityString = pretty"${asPrettyString(entity)}.${asPrettyString(p.name)}"
          setPropertyInfo(entityString, e, removeOtherProps = true)
      }.mkPrettyString(", ")
      pretty"SET $setOps"
  }

  private def commandColumnInfo(yieldColumns: List[CommandResultItem], yieldAll: Boolean): PrettyString =
    if (yieldColumns.nonEmpty)
      asPrettyString.raw(yieldColumns.map(y => {
        val variableName = y.originalName
        val aliasName = y.aliasedVariable.name

        if (!variableName.equals(aliasName)) s"$variableName AS $aliasName"
        else variableName
      }).mkString("columns(", ", ", ")"))
    else if (yieldAll) pretty"allColumns"
    else pretty"defaultColumns"
}
