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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.CommandClause
import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.ExpandHintAll
import org.neo4j.cypher.internal.ast.ExpandHintInto
import org.neo4j.cypher.internal.ast.ExpandHintMode
import org.neo4j.cypher.internal.ast.GraphReference
import org.neo4j.cypher.internal.ast.IrHint
import org.neo4j.cypher.internal.ast.ShowColumn
import org.neo4j.cypher.internal.ast.ShowConstraintsClause
import org.neo4j.cypher.internal.ast.ShowCurrentGraphTypeClause
import org.neo4j.cypher.internal.ast.ShowDatabasesClause
import org.neo4j.cypher.internal.ast.ShowFunctionsClause
import org.neo4j.cypher.internal.ast.ShowIndexesClause
import org.neo4j.cypher.internal.ast.ShowProceduresClause
import org.neo4j.cypher.internal.ast.ShowSettingsClause
import org.neo4j.cypher.internal.ast.ShowTransactionsClause
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsDisjointByMode
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsDisjointByParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsErrorParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsReportParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsRetryParameters
import org.neo4j.cypher.internal.ast.TerminateTransactionsClause
import org.neo4j.cypher.internal.ast.Union.UnionMapping
import org.neo4j.cypher.internal.ast.UsingExpandStepHint
import org.neo4j.cypher.internal.ast.UsingExpandStepId
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.ast.UsingScanHint
import org.neo4j.cypher.internal.ast.UsingStatefulShortestPathHint
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper
import org.neo4j.cypher.internal.compiler.planner.ProcedureCallProjection
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.RemoteBatchingResult
import org.neo4j.cypher.internal.compiler.planner.logical.RemoteBatchingSubQueryResult
import org.neo4j.cypher.internal.compiler.planner.logical.irExpressionRewriter
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.LogicalPlanProducer.solvedForTailApply
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.ContainsSearchMode
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EndsWithSearchMode
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.StringSearchMode
import org.neo4j.cypher.internal.compiler.planner.logical.steps.projection.MaybeReportedProjections
import org.neo4j.cypher.internal.compiler.planner.logical.steps.skipAndLimit.planLimitOnTopOf
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.AllReduceAccumulator
import org.neo4j.cypher.internal.expressions.CachedHasProperty
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.HasAnyDynamicLabel
import org.neo4j.cypher.internal.expressions.HasAnyDynamicType
import org.neo4j.cypher.internal.expressions.HasDynamicLabels
import org.neo4j.cypher.internal.expressions.HasDynamicType
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.ImpliedLabel
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapProjection
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.PartialPredicate
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.UnPositionedVariable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.VariableGrouping
import org.neo4j.cypher.internal.expressions.functions.Collect
import org.neo4j.cypher.internal.expressions.functions.UnresolvedFunction
import org.neo4j.cypher.internal.frontend.phases.Namespacer
import org.neo4j.cypher.internal.frontend.phases.ResolvedNonLocalCall
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.CSVFormat
import org.neo4j.cypher.internal.ir.CallSubqueryHorizon
import org.neo4j.cypher.internal.ir.CommandProjection
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreatePattern
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.ir.DeleteExpression
import org.neo4j.cypher.internal.ir.DistinctQueryProjection
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.ForeachPattern
import org.neo4j.cypher.internal.ir.LoadCSVProjection
import org.neo4j.cypher.internal.ir.MergeNodePattern
import org.neo4j.cypher.internal.ir.MergeRelationshipPattern
import org.neo4j.cypher.internal.ir.MutatingPattern
import org.neo4j.cypher.internal.ir.NodeBinding
import org.neo4j.cypher.internal.ir.PassthroughAllHorizon
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryProjection
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.RemoveLabelPattern
import org.neo4j.cypher.internal.ir.RunQueryAtProjection
import org.neo4j.cypher.internal.ir.SelectivePathPattern
import org.neo4j.cypher.internal.ir.SetDynamicPropertyPattern
import org.neo4j.cypher.internal.ir.SetLabelPattern
import org.neo4j.cypher.internal.ir.SetMutatingPattern
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
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.UnionQuery
import org.neo4j.cypher.internal.ir.UnwindProjection
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.ir.VectorSearchClause
import org.neo4j.cypher.internal.ir.ast.IRExpression
import org.neo4j.cypher.internal.ir.ordering
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.AssertSameRelationship
import org.neo4j.cypher.internal.logical.plans.AtMostOneRow
import org.neo4j.cypher.internal.logical.plans.CachedProperties
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.ColumnOrder
import org.neo4j.cypher.internal.logical.plans.CommandDefaultColumn
import org.neo4j.cypher.internal.logical.plans.CommandYieldColumn
import org.neo4j.cypher.internal.logical.plans.ConditionalApply
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
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipVectorIndexSearch
import org.neo4j.cypher.internal.logical.plans.DirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.DistinctColumns
import org.neo4j.cypher.internal.logical.plans.Distinctness
import org.neo4j.cypher.internal.logical.plans.DynamicDirectedRelationshipTypeLookup
import org.neo4j.cypher.internal.logical.plans.DynamicElement
import org.neo4j.cypher.internal.logical.plans.DynamicLabelNodeLookup
import org.neo4j.cypher.internal.logical.plans.DynamicUndirectedRelationshipTypeLookup
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.EmptyResult
import org.neo4j.cypher.internal.logical.plans.ErrorPlan
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLimit
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Expand.ExpansionMode
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths.DisallowSameNode
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths.SkipSameNode
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
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
import org.neo4j.cypher.internal.logical.plans.LogicalBinaryPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString
import org.neo4j.cypher.internal.logical.plans.MatchAllQueryExpression
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.NFA
import org.neo4j.cypher.internal.logical.plans.NFA.PathLength
import org.neo4j.cypher.internal.logical.plans.NodeByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeLogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeVectorIndexSearch
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.OrderedUnion
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.RelationshipCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.RelationshipLogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.RemoteBatchProperties
import org.neo4j.cypher.internal.logical.plans.RemoteBatchPropertiesWithFilter
import org.neo4j.cypher.internal.logical.plans.RemoteBatchPropertiesWithPushdownOperators
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.RepeatAcyclic
import org.neo4j.cypher.internal.logical.plans.RepeatTrail
import org.neo4j.cypher.internal.logical.plans.RepeatWalk
import org.neo4j.cypher.internal.logical.plans.RewrittenSubQueryPredicates
import org.neo4j.cypher.internal.logical.plans.RewrittenSubQueryPredicates.RewrittenSubQueryPredicatesMap
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.RunQueryAt
import org.neo4j.cypher.internal.logical.plans.SeekableArgs
import org.neo4j.cypher.internal.logical.plans.SelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.SelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.Selection.LabelAndRelTypeInfo
import org.neo4j.cypher.internal.logical.plans.SemiApply
import org.neo4j.cypher.internal.logical.plans.SetDynamicProperty
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
import org.neo4j.cypher.internal.logical.plans.ShowCurrentGraphType
import org.neo4j.cypher.internal.logical.plans.ShowDatabases
import org.neo4j.cypher.internal.logical.plans.ShowFunctions
import org.neo4j.cypher.internal.logical.plans.ShowIndexes
import org.neo4j.cypher.internal.logical.plans.ShowProcedures
import org.neo4j.cypher.internal.logical.plans.ShowSettings
import org.neo4j.cypher.internal.logical.plans.ShowTransactions
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath.LengthBounds
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath.Mapping
import org.neo4j.cypher.internal.logical.plans.SubqueryForeach
import org.neo4j.cypher.internal.logical.plans.SubtractionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.TerminateTransactions
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.Top1WithTies
import org.neo4j.cypher.internal.logical.plans.TransactionApply
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency
import org.neo4j.cypher.internal.logical.plans.TransactionForeach
import org.neo4j.cypher.internal.logical.plans.TraversalPathMode
import org.neo4j.cypher.internal.logical.plans.TraversalPathMode.Trail
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
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipVectorIndexSearch
import org.neo4j.cypher.internal.logical.plans.UndirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.UpdatingPlan
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.ValueMergeJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.logical.plans.ordering.DefaultProvidedOrderFactory
import org.neo4j.cypher.internal.logical.plans.ordering.ParallelExecutionProvidedOrderFactory
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrderFactory
import org.neo4j.cypher.internal.macros.AssertMacros3
import org.neo4j.cypher.internal.macros.AssertMacros3.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.IndexType
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.util.AssertionRunner
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.PredicateHelper.coercePredicatesWithAnds
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.exceptions.ExhaustiveShortestPathForbiddenException
import org.neo4j.exceptions.InternalException

import scala.util.chaining.scalaUtilChainingOps

/*
 * The responsibility of this class is to produce the correct solved PlannerQuery when creating logical plans.
 * No other functionality or logic should live here - this is supposed to be a very simple class that does not need
 * much testing
 */
case class LogicalPlanProducer(
  cardinalityModel: CardinalityModel,
  planningAttributes: PlanningAttributes,
  idGen: IdGen
) {

  implicit val implicitIdGen: IdGen = idGen
  private val solveds = planningAttributes.solveds
  private val cardinalities = planningAttributes.cardinalities
  private val providedOrders = planningAttributes.providedOrders
  private val leveragedOrders = planningAttributes.leveragedOrders
  private val labelAndRelTypeInfos = planningAttributes.labelAndRelTypeInfos
  private val cachedPropertiesPerPlan = planningAttributes.cachedPropertiesPerPlan

  private val attributesWithoutSolveds =
    planningAttributes.asAttributes(idGen).without(solveds, planningAttributes.effectiveCardinalities)

  /**
   * This object is simply to group methods that are used by the [[SubqueryExpressionSolver]], and thus do not need to update `solveds`
   */
  object ForSubqueryExpressionSolver {

    def planArgument(argumentIds: Set[LogicalVariable], context: LogicalPlanningContext): LogicalPlan = {
      val previouslyCachedProperties =
        context.plannerState.previouslyCachedProperties
      annotate(
        Argument(argumentIds),
        SinglePlannerQuery.empty.updateQueryProjection(
          _.withImportedExposedSymbols(context.plannerState.importedSubqueryVariables)
        ),
        ProvidedOrder.empty,
        previouslyCachedProperties,
        context
      )
    }

    def planApply(left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
      val plan = Apply(left, right)
      val providedOrder =
        providedOrderOfApply(left, right, plan, context.settings.executionModel, context.providedOrderFactory)
      // The RHS is the leaf plan we are wrapping under an apply in order to solve the pattern expression.
      // It has the correct solved
      val solved = solveds.get(right.id)
      annotate(
        plan,
        solved,
        providedOrder,
        cachedPropertiesPerPlan.get(right.id),
        context
      )
    }

    def planRollup(
      lhs: LogicalPlan,
      rhs: LogicalPlan,
      collectionName: LogicalVariable,
      variableToCollect: LogicalVariable,
      context: LogicalPlanningContext
    ): LogicalPlan = {
      // The LHS is either the plan we're building on top of, with the correct solved or it is the result of [[planArgument]].
      // The RHS is the sub-query
      val solved = solveds.get(lhs.id)
      annotate(
        RollUpApply(lhs, rhs, collectionName, variableToCollect),
        solved,
        ProvidedOrder.Left,
        cachedPropertiesPerPlan.get(rhs.id),
        context
      )
    }

    def planCountExpressionApply(
      lhs: LogicalPlan,
      rhs: LogicalPlan,
      context: LogicalPlanningContext
    ): LogicalPlan = {
      val solved = solveds.get(lhs.id)
      val plan = Apply(lhs, rhs)
      annotate(
        plan,
        solved,
        providedOrderOfApply(lhs, rhs, plan, context.settings.executionModel, context.providedOrderFactory),
        cachedPropertiesPerPlan.get(rhs.id),
        context
      )
    }
  }

  def solvePredicate(plan: LogicalPlan, solvedExpression: Expression): LogicalPlan = {
    solvePredicates(plan, Set(solvedExpression))
  }

  def solvePredicates(plan: LogicalPlan, solvedExpressions: Set[Expression]): LogicalPlan = {
    // Keep other attributes but change solved
    val keptAttributes =
      Attributes(idGen, cardinalities, providedOrders, leveragedOrders, labelAndRelTypeInfos, cachedPropertiesPerPlan)
    val newPlan = plan.copyPlanWithIdGen(keptAttributes.copy(plan.id))
    val solvedPlannerQuery =
      solveds.get(plan.id).asSinglePlannerQuery.amendQueryGraph(_.addPredicates(solvedExpressions))
    solveds.set(newPlan.id, solvedPlannerQuery)
    newPlan
  }

  def markAsSolved(plan: LogicalPlan, solved: SinglePlannerQuery): LogicalPlan = {
    // Keep other attributes but change solved
    val keptAttributes =
      Attributes(idGen, cardinalities, providedOrders, leveragedOrders, labelAndRelTypeInfos, cachedPropertiesPerPlan)
    val newPlan = plan.copyPlanWithIdGen(keptAttributes.copy(plan.id))
    solveds.set(newPlan.id, solved)
    newPlan
  }

  def solvePredicateInHorizon(plan: LogicalPlan, solvedExpression: Expression): LogicalPlan = {
    // Keep other attributes but change solved
    val keptAttributes =
      Attributes(idGen, cardinalities, providedOrders, leveragedOrders, labelAndRelTypeInfos, cachedPropertiesPerPlan)
    val newPlan = plan.copyPlanWithIdGen(keptAttributes.copy(plan.id))
    val solvedPlannerQuery = solveds.get(plan.id).asSinglePlannerQuery.updateTailOrSelf(_.updateHorizon {
      case horizon: QueryProjection => horizon.addPredicates(solvedExpression)
      case horizon                  => horizon
    })
    solveds.set(newPlan.id, solvedPlannerQuery)
    newPlan
  }

  def planAllNodesScan(
    variable: LogicalVariable,
    argumentIds: Set[LogicalVariable],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved =
      RegularSinglePlannerQuery(
        queryGraph = QueryGraph(argumentIds = argumentIds, patternNodes = Set(variable)),
        horizon = RegularQueryProjection(
          importedExposedSymbols = context.plannerState.importedSubqueryVariables
        )
      )

    annotate(
      AllNodesScan(variable, argumentIds),
      solved,
      ProvidedOrder.empty,
      context.plannerState.previouslyCachedProperties,
      context
    )
  }

  /**
   * @param variable           the name of the relationship variable
   * @param patternForLeafPlan the pattern to use for the leaf plan
   * @param originalPattern    the original pattern, as it appears in the query graph
   * @param hiddenSelections   selections that make the leaf plan solve the originalPattern instead.
   *                           Must not contain any pattern expressions or pattern comprehensions.
   */
  def planAllRelationshipsScan(
    variable: LogicalVariable,
    patternForLeafPlan: PatternRelationship,
    originalPattern: PatternRelationship,
    hiddenSelections: Seq[Expression],
    argumentIds: Set[LogicalVariable],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    require(patternForLeafPlan.types.isEmpty)

    def planLeaf: LogicalPlan = {
      val (firstNode, secondNode) = patternForLeafPlan.inOrder
      val solved =
        RegularSinglePlannerQuery(
          queryGraph =
            QueryGraph(
              argumentIds = argumentIds,
              patternNodes = Set(firstNode, secondNode),
              patternRelationships = Set(patternForLeafPlan)
            ),
          horizon = RegularQueryProjection(
            importedExposedSymbols = context.plannerState.importedSubqueryVariables
          )
        )

      val leafPlan =
        if (patternForLeafPlan.dir == BOTH) {
          UndirectedAllRelationshipsScan(variable, firstNode, secondNode, argumentIds)
        } else {
          DirectedAllRelationshipsScan(variable, firstNode, secondNode, argumentIds)
        }
      annotate(leafPlan, solved, ProvidedOrder.empty, context.plannerState.previouslyCachedProperties, context)
    }

    planHiddenSelectionIfNeeded(planLeaf, hiddenSelections, context, originalPattern)
  }

  /**
   * @param variable           the name of the relationship variable
   * @param relType            the relType to scan
   * @param patternForLeafPlan the pattern to use for the leaf plan
   * @param originalPattern    the original pattern, as it appears in the query graph
   * @param hiddenSelections   selections that make the leaf plan solve the originalPattern instead.
   *                           Must not contain any pattern expressions or pattern comprehensions.
   */
  def planRelationshipByTypeScan(
    variable: LogicalVariable,
    relType: RelTypeName,
    patternForLeafPlan: PatternRelationship,
    originalPattern: PatternRelationship,
    hiddenSelections: Seq[Expression],
    solvedHint: Option[UsingScanHint],
    argumentIds: Set[LogicalVariable],
    providedOrder: ProvidedOrder,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    def planLeaf: LogicalPlan = {
      val (firstNode, secondNode) = patternForLeafPlan.inOrder
      val leafPlan: RelationshipLogicalLeafPlan =
        if (patternForLeafPlan.dir == BOTH) {
          UndirectedRelationshipTypeScan(
            variable,
            firstNode,
            relType,
            secondNode,
            argumentIds,
            toIndexOrder(providedOrder)
          )
        } else {
          DirectedRelationshipTypeScan(
            variable,
            firstNode,
            relType,
            secondNode,
            argumentIds,
            toIndexOrder(providedOrder)
          )
        }

      annotateRelationshipLeafPlan(
        leafPlan,
        patternForLeafPlan,
        Seq.empty,
        solvedHint,
        argumentIds,
        providedOrder,
        context,
        context.plannerState.previouslyCachedProperties
      )
    }

    planHiddenSelectionIfNeeded(planLeaf, hiddenSelections, context, originalPattern)
  }

  /**
   * @param variable           the name of the relationship variable
   * @param relTypes           the relTypes to scan
   * @param patternForLeafPlan the pattern to use for the leaf plan
   * @param originalPattern    the original pattern, as it appears in the query graph
   * @param hiddenSelections   selections that make the leaf plan solve the originalPattern instead.
   *                           Must not contain any pattern expressions or pattern comprehensions.
   */
  def planUnionRelationshipByTypeScan(
    variable: LogicalVariable,
    relTypes: Seq[RelTypeName],
    patternForLeafPlan: PatternRelationship,
    originalPattern: PatternRelationship,
    hiddenSelections: Seq[Expression],
    solvedHints: Seq[UsingScanHint],
    argumentIds: Set[LogicalVariable],
    providedOrder: ProvidedOrder,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    def planLeaf: LogicalPlan = {
      val (firstNode, secondNode) = patternForLeafPlan.inOrder
      val leafPlan: RelationshipLogicalLeafPlan =
        if (patternForLeafPlan.dir == BOTH) {
          UndirectedUnionRelationshipTypesScan(
            variable,
            firstNode,
            relTypes,
            secondNode,
            argumentIds,
            toIndexOrder(providedOrder)
          )
        } else {
          DirectedUnionRelationshipTypesScan(
            variable,
            firstNode,
            relTypes,
            secondNode,
            argumentIds,
            toIndexOrder(providedOrder)
          )
        }

      annotateRelationshipLeafPlan(
        leafPlan,
        patternForLeafPlan,
        Seq.empty,
        solvedHints,
        argumentIds,
        providedOrder,
        context,
        context.plannerState.previouslyCachedProperties
      )
    }

    planHiddenSelectionIfNeeded(planLeaf, hiddenSelections, context, originalPattern)
  }

  def planDynamicRelationshipByTypeLookup(
    variable: LogicalVariable,
    relationshipTypes: Expression,
    operator: DynamicElement.SetOperator,
    patternForLeafPlan: PatternRelationship,
    originalPattern: PatternRelationship,
    hiddenSelections: Seq[Expression],
    argumentIds: Set[LogicalVariable],
    providedOrder: ProvidedOrder,
    context: LogicalPlanningContext,
    solvedPropertyPredicates: Set[Expression],
    propertyPredicates: Map[PropertyKeyToken, Expression]
  ): LogicalPlan = {
    val predicate =
      operator match {
        case DynamicElement.All => HasDynamicType(variable, Seq(relationshipTypes))(InputPosition.NONE)
        case DynamicElement.Any => HasAnyDynamicType(variable, Seq(relationshipTypes))(InputPosition.NONE)
      }

    val solver = SubqueryExpressionSolver.solverForLeafPlan(argumentIds, context)
    val rewrittenRelationshipTypes = solver.solve(relationshipTypes)
    val newArguments = solver.newArguments

    val element = DynamicElement.Simple(rewrittenRelationshipTypes, operator)
    val allArgumentIds = argumentIds.union(newArguments)
    val indexOrder = toIndexOrder(providedOrder)

    val leafPlan = patternForLeafPlan.dir match {
      case SemanticDirection.OUTGOING =>
        DynamicDirectedRelationshipTypeLookup(
          idName = Some(variable),
          startNode = Some(patternForLeafPlan.left),
          relType = element,
          endNode = Some(patternForLeafPlan.right),
          argumentIds = allArgumentIds,
          indexOrder = indexOrder,
          propertyPredicates = propertyPredicates
        )
      case SemanticDirection.INCOMING =>
        DynamicDirectedRelationshipTypeLookup(
          idName = Some(variable),
          startNode = Some(patternForLeafPlan.right),
          relType = element,
          endNode = Some(patternForLeafPlan.left),
          argumentIds = allArgumentIds,
          indexOrder = indexOrder,
          propertyPredicates = propertyPredicates
        )
      case SemanticDirection.BOTH =>
        DynamicUndirectedRelationshipTypeLookup(
          idName = Some(variable),
          leftNode = Some(patternForLeafPlan.left),
          relType = element,
          rightNode = Some(patternForLeafPlan.right),
          argumentIds = allArgumentIds,
          indexOrder = indexOrder,
          propertyPredicates = propertyPredicates
        )
    }

    val annotatedLeafPlan =
      annotateRelationshipLeafPlan(
        leafPlan = leafPlan,
        patternForLeafPlan = patternForLeafPlan,
        solvedPredicates = List(predicate) ++ solvedPropertyPredicates,
        solvedHint = Nil,
        argumentIds = argumentIds,
        providedOrder = providedOrder,
        context = context,
        cachedProperties = context.plannerState.previouslyCachedProperties
      )

    val rewrittenPlan = solver.rewriteLeafPlan(annotatedLeafPlan)

    planHiddenSelectionIfNeeded(rewrittenPlan, hiddenSelections, context, originalPattern)
  }

  def planRelationshipIndexScan(
    variable: LogicalVariable,
    relationshipType: RelationshipTypeToken,
    patternForLeafPlan: PatternRelationship,
    originalPattern: PatternRelationship,
    properties: Seq[IndexedProperty],
    solvedPredicates: Seq[Expression] = Seq.empty,
    solvedHint: Option[UsingIndexHint] = None,
    hiddenSelections: Seq[Expression],
    argumentIds: Set[LogicalVariable],
    providedOrder: ProvidedOrder,
    indexOrder: IndexOrder,
    context: LogicalPlanningContext,
    indexType: IndexType,
    supportPartitionedScan: Boolean
  ): LogicalPlan = {
    def planLeaf: LogicalPlan = {
      val leafPlan =
        if (patternForLeafPlan.dir == BOTH) {
          UndirectedRelationshipIndexScan(
            variable,
            patternForLeafPlan.inOrder._1,
            patternForLeafPlan.inOrder._2,
            relationshipType,
            properties,
            argumentIds,
            indexOrder,
            indexType.toPublicApi,
            supportPartitionedScan
          )
        } else {
          DirectedRelationshipIndexScan(
            variable,
            patternForLeafPlan.inOrder._1,
            patternForLeafPlan.inOrder._2,
            relationshipType,
            properties,
            argumentIds,
            indexOrder,
            indexType.toPublicApi,
            supportPartitionedScan: Boolean
          )
        }

      annotateRelationshipLeafPlan(
        leafPlan,
        patternForLeafPlan,
        solvedPredicates,
        solvedHint,
        argumentIds,
        providedOrder,
        context,
        cachedPropertiesForIndexedProperties(context, variable, properties)
      )
    }

    planHiddenSelectionIfNeeded(planLeaf, hiddenSelections, context, originalPattern)
  }

  def planRelationshipIndexStringSearchScan(
    variable: LogicalVariable,
    relationshipType: RelationshipTypeToken,
    patternForLeafPlan: PatternRelationship,
    originalPattern: PatternRelationship,
    properties: Seq[IndexedProperty],
    stringSearchMode: StringSearchMode,
    solvedPredicates: Seq[Expression] = Seq.empty,
    solvedHint: Option[UsingIndexHint] = None,
    hiddenSelections: Seq[Expression],
    valueExpr: Expression,
    argumentIds: Set[LogicalVariable],
    providedOrder: ProvidedOrder,
    indexOrder: IndexOrder,
    context: LogicalPlanningContext,
    indexType: IndexType
  ): LogicalPlan = {
    def planLeaf = {
      val solver = SubqueryExpressionSolver.solverForLeafPlan(argumentIds, context)
      val rewrittenValueExpr = solver.solve(valueExpr)
      val newArguments = solver.newArguments

      val leafPlan = (patternForLeafPlan.dir, stringSearchMode) match {
        case (SemanticDirection.BOTH, ContainsSearchMode) =>
          UndirectedRelationshipIndexContainsScan(
            Some(variable),
            Some(patternForLeafPlan.inOrder._1),
            Some(patternForLeafPlan.inOrder._2),
            relationshipType,
            properties.head,
            rewrittenValueExpr,
            argumentIds ++ newArguments,
            indexOrder,
            indexType.toPublicApi
          )
        case (SemanticDirection.BOTH, EndsWithSearchMode) =>
          UndirectedRelationshipIndexEndsWithScan(
            Some(variable),
            Some(patternForLeafPlan.inOrder._1),
            Some(patternForLeafPlan.inOrder._2),
            relationshipType,
            properties.head,
            rewrittenValueExpr,
            argumentIds ++ newArguments,
            indexOrder,
            indexType.toPublicApi
          )
        case (SemanticDirection.INCOMING | SemanticDirection.OUTGOING, ContainsSearchMode) =>
          DirectedRelationshipIndexContainsScan(
            Some(variable),
            Some(patternForLeafPlan.inOrder._1),
            Some(patternForLeafPlan.inOrder._2),
            relationshipType,
            properties.head,
            rewrittenValueExpr,
            argumentIds ++ newArguments,
            indexOrder,
            indexType.toPublicApi
          )
        case (SemanticDirection.INCOMING | SemanticDirection.OUTGOING, EndsWithSearchMode) =>
          DirectedRelationshipIndexEndsWithScan(
            Some(variable),
            Some(patternForLeafPlan.inOrder._1),
            Some(patternForLeafPlan.inOrder._2),
            relationshipType,
            properties.head,
            rewrittenValueExpr,
            argumentIds ++ newArguments,
            indexOrder,
            indexType.toPublicApi
          )
      }

      solver.rewriteLeafPlan {
        annotateRelationshipLeafPlan(
          leafPlan,
          patternForLeafPlan,
          solvedPredicates,
          solvedHint,
          argumentIds,
          providedOrder,
          context,
          cachedPropertiesForIndexedProperties(context, variable, properties)
        )
      }
    }

    planHiddenSelectionIfNeeded(planLeaf, hiddenSelections, context, originalPattern)
  }

  def planRelationshipIndexSeek(
    variable: LogicalVariable,
    typeToken: RelationshipTypeToken,
    properties: Seq[IndexedProperty],
    valueExpr: QueryExpression[Expression],
    argumentIds: Set[LogicalVariable],
    indexOrder: IndexOrder,
    patternForLeafPlan: PatternRelationship,
    originalPattern: PatternRelationship,
    solvedPredicates: Seq[Expression],
    solvedHint: Option[UsingIndexHint],
    hiddenSelections: Seq[Expression],
    providedOrder: ProvidedOrder,
    context: LogicalPlanningContext,
    indexType: IndexType,
    unique: Boolean,
    supportPartitionedScan: Boolean
  ): LogicalPlan = {

    def planLeaf = {
      val solver = SubqueryExpressionSolver.solverForLeafPlan(argumentIds, context)
      val rewrittenValueExpr = valueExpr.map(solver.solve(_))
      val newArguments = solver.newArguments

      val leafPlan =
        if (patternForLeafPlan.dir == SemanticDirection.BOTH) {
          def makeUndirected() =
            if (unique)
              UndirectedRelationshipUniqueIndexSeek(
                variable,
                patternForLeafPlan.left,
                patternForLeafPlan.right,
                typeToken,
                properties,
                rewrittenValueExpr,
                argumentIds ++ newArguments,
                indexOrder,
                indexType.toPublicApi
              )
            else
              UndirectedRelationshipIndexSeek(
                variable,
                patternForLeafPlan.left,
                patternForLeafPlan.right,
                typeToken,
                properties,
                rewrittenValueExpr,
                argumentIds ++ newArguments,
                indexOrder,
                indexType.toPublicApi,
                supportPartitionedScan
              )

          makeUndirected()
        } else {
          def makeDirected() =
            if (unique)
              DirectedRelationshipUniqueIndexSeek(
                variable,
                patternForLeafPlan.inOrder._1,
                patternForLeafPlan.inOrder._2,
                typeToken,
                properties,
                rewrittenValueExpr,
                argumentIds ++ newArguments,
                indexOrder,
                indexType.toPublicApi
              )
            else
              DirectedRelationshipIndexSeek(
                variable,
                patternForLeafPlan.inOrder._1,
                patternForLeafPlan.inOrder._2,
                typeToken,
                properties,
                rewrittenValueExpr,
                argumentIds ++ newArguments,
                indexOrder,
                indexType.toPublicApi,
                supportPartitionedScan
              )

          makeDirected()
        }

      solver.rewriteLeafPlan {
        annotateRelationshipLeafPlan(
          leafPlan,
          patternForLeafPlan,
          solvedPredicates,
          solvedHint,
          argumentIds,
          providedOrder,
          context,
          cachedPropertiesForIndexedProperties(context, variable, properties)
        )
      }
    }

    planHiddenSelectionIfNeeded(planLeaf, hiddenSelections, context, originalPattern)
  }

  /**
   * @param variable           the name of the relationship variable
   * @param patternForLeafPlan the pattern to use for the leaf plan
   * @param originalPattern    the original pattern, as it appears in the query graph
   * @param hiddenSelections   selections that make the leaf plan solve the originalPattern instead.
   *                           Must not contain any pattern expressions or pattern comprehensions.
   */
  def planRelationshipByIdSeek(
    variable: LogicalVariable,
    relIds: SeekableArgs,
    patternForLeafPlan: PatternRelationship,
    originalPattern: PatternRelationship,
    hiddenSelections: Seq[Expression],
    argumentIds: Set[LogicalVariable],
    solvedPredicates: Seq[Expression] = Seq.empty,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    doPlanRelationshipByIdSeek(
      UndirectedRelationshipByIdSeek.apply,
      DirectedRelationshipByIdSeek.apply,
      variable,
      relIds,
      patternForLeafPlan,
      originalPattern,
      hiddenSelections,
      argumentIds,
      solvedPredicates,
      context
    )
  }

  def planRelationshipByElementIdSeek(
    variable: LogicalVariable,
    relIds: SeekableArgs,
    patternForLeafPlan: PatternRelationship,
    originalPattern: PatternRelationship,
    hiddenSelections: Seq[Expression],
    argumentIds: Set[LogicalVariable],
    solvedPredicates: Seq[Expression] = Seq.empty,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    doPlanRelationshipByIdSeek(
      UndirectedRelationshipByElementIdSeek.apply,
      DirectedRelationshipByElementIdSeek.apply,
      variable,
      relIds,
      patternForLeafPlan,
      originalPattern,
      hiddenSelections,
      argumentIds,
      solvedPredicates,
      context
    )
  }

  private def doPlanRelationshipByIdSeek(
    makeUndirected: (
      LogicalVariable,
      SeekableArgs,
      LogicalVariable,
      LogicalVariable,
      Set[LogicalVariable]
    ) => RelationshipLogicalLeafPlan,
    makeDirected: (
      LogicalVariable,
      SeekableArgs,
      LogicalVariable,
      LogicalVariable,
      Set[LogicalVariable]
    ) => RelationshipLogicalLeafPlan,
    variable: LogicalVariable,
    relIds: SeekableArgs,
    patternForLeafPlan: PatternRelationship,
    originalPattern: PatternRelationship,
    hiddenSelections: Seq[Expression],
    argumentIds: Set[LogicalVariable],
    solvedPredicates: Seq[Expression],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    def planLeaf: LogicalPlan = {
      val (firstNode, secondNode) = patternForLeafPlan.inOrder
      val solver = SubqueryExpressionSolver.solverForLeafPlan(argumentIds, context)
      val rewrittenRelIds = relIds.mapValues(solver.solve(_))
      val newArguments = solver.newArguments

      val leafPlan =
        if (patternForLeafPlan.dir == BOTH) {
          makeUndirected(
            variable,
            rewrittenRelIds,
            firstNode,
            secondNode,
            argumentIds ++ newArguments
          )
        } else {
          makeDirected(
            variable,
            rewrittenRelIds,
            firstNode,
            secondNode,
            argumentIds ++ newArguments
          )
        }
      solver.rewriteLeafPlan {
        annotateRelationshipLeafPlan(
          leafPlan,
          patternForLeafPlan,
          solvedPredicates,
          None,
          argumentIds,
          ProvidedOrder.empty,
          context,
          context.plannerState.previouslyCachedProperties
        )
      }
    }

    planHiddenSelectionIfNeeded(planLeaf, hiddenSelections, context, originalPattern)
  }

  private def annotateRelationshipLeafPlan(
    leafPlan: RelationshipLogicalLeafPlan,
    patternForLeafPlan: PatternRelationship,
    solvedPredicates: Seq[Expression],
    solvedHint: IterableOnce[IrHint],
    argumentIds: Set[LogicalVariable],
    providedOrder: ProvidedOrder,
    context: LogicalPlanningContext,
    cachedProperties: CachedProperties
  ): RelationshipLogicalLeafPlan = {
    val solved = RegularSinglePlannerQuery(
      queryGraph =
        QueryGraph.empty
          .addPatternRelationship(patternForLeafPlan)
          .addPredicates(solvedPredicates: _*)
          .addHints(solvedHint)
          .addArgumentIds(argumentIds.toIndexedSeq),
      horizon = RegularQueryProjection(
        importedExposedSymbols = context.plannerState.importedSubqueryVariables
      )
    )

    annotate(leafPlan, solved, providedOrder, cachedProperties, context)
  }

  private def computeBatchSize(maybeBatchSize: Option[Expression]): Expression = {
    maybeBatchSize match {
      case Some(batchSize) => batchSize
      case None => SignedDecimalIntegerLiteral(TransactionForeach.defaultBatchSize.toString)(InputPosition.NONE)
    }
  }

  private def computeConcurrency(maybeConcurrency: Option[Option[Expression]]): TransactionConcurrency = {
    maybeConcurrency match {
      case Some(Some(concurrency)) => TransactionConcurrency.Concurrent(Some(concurrency))
      case Some(None)              => TransactionConcurrency.Concurrent(None)
      case None                    => TransactionConcurrency.Serial
    }
  }

  private def computeErrorBehaviour(maybeErrorParams: Option[InTransactionsErrorParameters])
    : (InTransactionsOnErrorBehaviour, Option[InTransactionsRetryParameters]) = {
    maybeErrorParams match {
      case Some(InTransactionsErrorParameters(
          behaviour @ (OnErrorRetryThenContinue | OnErrorRetryThenBreak | OnErrorRetryThenFail),
          retryParams
        )) =>
        (behaviour, retryParams)
      case Some(InTransactionsErrorParameters(behaviour, None)) =>
        (behaviour, None)
      case None =>
        (TransactionForeach.defaultOnErrorBehaviour, None)
      case _ =>
        throw new IllegalArgumentException("Invalid combination of error parameters and retry parameters")
    }
  }

  private def computeMaybeReportAs(maybeReportParams: Option[InTransactionsReportParameters])
    : Option[LogicalVariable] = {
    maybeReportParams.map(_.reportAs)
  }

  private def computeEffectiveDisjointBy(maybeDisjointByParams: Option[InTransactionsDisjointByParameters])
    : Seq[Expression] = {
    maybeDisjointByParams.map(_.mode) match {
      case Some(InTransactionsDisjointByMode.DisjointByExpressions(expressions)) => expressions
      case _                                                                     => Seq.empty
    }
  }

  /**
   * Plan a selection on `hiddenSelections` but, in the solveds, pretend to solve only the predicates of the leaf plan and `originalPattern` instead of the leaf plan's pattern.
   *
   * @param source           the source leaf plan
   * @param hiddenSelections the selections to test in this operator
   * @param context          planning context
   * @param solvedPattern    the pattern we will claim to have solved
   * @return hidden selection on top of source plan
   */
  private def planHiddenSelectionIfNeeded(
    source: LogicalPlan,
    hiddenSelections: Seq[Expression],
    context: LogicalPlanningContext,
    solvedPattern: PatternRelationship
  ): LogicalPlan = {
    if (hiddenSelections.isEmpty) {
      source
    } else {
      val solved =
        solveds.get(source.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.withPattern(solvedPattern)))
      planSelectionWithGivenSolved(source, hiddenSelections, solved, context)
    }
  }

  def planApply(left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    planApplyWithCachedProperties(left, right, context, cachedPropertiesPerPlan.get(right.id))
  }

  def planApplyWithCachedProperties(
    left: LogicalPlan,
    right: LogicalPlan,
    context: LogicalPlanningContext,
    cachedProperties: CachedProperties
  ): LogicalPlan = {
    // We don't want to keep the arguments that this Apply is inserting on the RHS, so we remove them here.
    val rhsSolved =
      solveds.get(right.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.withArgumentIds(Set.empty)))
    val solved = solveds.get(left.id).asSinglePlannerQuery ++ rhsSolved
    val plan = Apply(left, right)
    val providedOrder =
      providedOrderOfApply(left, right, plan, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, cachedProperties, context)
  }

  def planMergeApply(left: LogicalPlan, right: Merge, context: LogicalPlanningContext): LogicalPlan = {
    val lhsSolved = solveds.get(left.id).asSinglePlannerQuery
    val rhsSolved = solveds.get(right.id).asSinglePlannerQuery
    val solved =
      lhsSolved.updateTailOrSelf(
        _.amendQueryGraph(_.addMutatingPatterns(rhsSolved.queryGraph.mutatingPatterns))
          .resetQueryProjection()
      )

    val plan = Apply(left, right)
    val providedOrder =
      providedOrderOfApply(left, right, plan, context.settings.executionModel, context.providedOrderFactory)
    annotate(
      plan,
      solved,
      providedOrder,
      cachedPropertiesPerPlan.get(right.id),
      context
    )
  }

  def planSubquery(
    left: LogicalPlan,
    right: LogicalPlan,
    context: LogicalPlanningContext,
    correlated: Boolean,
    yielding: Boolean,
    inTransactionsParameters: Option[InTransactionsParameters],
    optional: Boolean,
    importedVariables: Set[LogicalVariable],
    importedSymbolsFromLastCallSubquery: Set[LogicalVariable]
  ): LogicalPlan = {
    val solvedLeft = solveds.get(left.id)
    val solvedRight = solveds.get(right.id)
    val solved = solvedLeft.asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(CallSubqueryHorizon(
      solvedRight,
      correlated,
      yielding,
      inTransactionsParameters,
      optional,
      importedVariables,
      importedSymbolsFromLastCallSubquery
    )))

    val plan =
      if (yielding) {
        inTransactionsParameters match {
          case Some(InTransactionsParameters(
              batchParams,
              concurrencyParams,
              errorParams,
              reportParams,
              disjointByParams
            )) =>
            val (errorBehaviour, retryParams) = computeErrorBehaviour(errorParams)
            TransactionApply(
              left,
              right,
              computeBatchSize(batchParams.map(_.batchSize)),
              computeConcurrency(concurrencyParams.map(_.concurrency)),
              errorBehaviour,
              computeMaybeReportAs(reportParams),
              retryParams,
              disjointByParams,
              computeEffectiveDisjointBy(disjointByParams)
            )
          case None =>
            if (!correlated && solvedRight.readOnly) {
              CartesianProduct(left, right)
            } else {
              Apply(left, right)
            }
        }
      } else {
        inTransactionsParameters match {
          case Some(InTransactionsParameters(
              batchParams,
              concurrencyParams,
              errorParams,
              reportParams,
              disjointByParams
            )) =>
            val (errorBehaviour, retryParams) = computeErrorBehaviour(errorParams)
            TransactionForeach(
              left,
              right,
              computeBatchSize(batchParams.map(_.batchSize)),
              computeConcurrency(concurrencyParams.map(_.concurrency)),
              errorBehaviour,
              computeMaybeReportAs(reportParams),
              retryParams,
              disjointByParams,
              computeEffectiveDisjointBy(disjointByParams)
            )
          case None => SubqueryForeach(left, right)
        }
      }

    val providedOrder =
      providedOrderOfApply(left, right, plan, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, cachedPropertiesPerPlan.get(right.id), context)
  }

  def planTailApply(left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solvedForTailApply(left, right, solveds)
    val plan = Apply(left, right)
    val providedOrder =
      providedOrderOfApply(left, right, plan, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, cachedPropertiesPerPlan.get(right.id), context)
  }

  def planInputApply(
    left: LogicalPlan,
    right: LogicalPlan,
    symbols: Seq[Variable],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(right.id).asSinglePlannerQuery.withInput(symbols)
    val plan = Apply(left, right)
    val providedOrder =
      providedOrderOfApply(left, right, plan, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, CachedProperties.empty, context)
  }

  def planCartesianProduct(left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val solved: SinglePlannerQuery =
      solveds.get(left.id).asSinglePlannerQuery ++ solveds.get(right.id).asSinglePlannerQuery
    val plan = CartesianProduct(left, right)
    val providedOrder =
      providedOrderOfApply(left, right, plan, context.settings.executionModel, context.providedOrderFactory)
    annotate(
      plan,
      solved,
      providedOrder,
      cachedPropertiesPerPlan.get(left.id).intersectProperties(cachedPropertiesPerPlan.get(right.id)),
      context
    )
  }

  def planSimpleExpand(
    left: LogicalPlan,
    from: LogicalVariable,
    to: LogicalVariable,
    pattern: PatternRelationship,
    mode: ExpansionMode,
    context: LogicalPlanningContext,
    hints: Iterable[IrHint]
  ): LogicalPlan = {
    val dir = pattern.directionRelativeTo(from)
    val solved =
      solveds.get(left.id).asSinglePlannerQuery
        .amendQueryGraph { qg =>
          val alreadySolvedExpandStepIds = qg.hints.collect {
            case h: UsingExpandStepHint => h.stepId
          }
          qg.addPatternRelationship(pattern)
            .addHints(hints.collect {
              case h: UsingExpandStepHint
                if LogicalPlanProducer.expandHintClaims(
                  h,
                  planFrom = from,
                  planTo = to,
                  planRelIds = Set(pattern.variable),
                  planMode = mode,
                  claimedStepIds = alreadySolvedExpandStepIds
                ) => h
            })
        }
    annotate(
      Expand(left, from, dir, pattern.types, to, pattern.variable, mode),
      solved,
      ProvidedOrder.Left,
      cachedPropertiesPerPlan.get(left.id),
      context
    )
  }

  def planVarExpand(
    source: LogicalPlan,
    from: LogicalVariable,
    to: LogicalVariable,
    patternRelationship: PatternRelationship,
    relationshipPredicates: ListSet[VariablePredicate],
    nodePredicates: ListSet[VariablePredicate],
    solvedPredicates: ListSet[Expression],
    expansionMode: ExpansionMode,
    pathMode: TraversalPathMode,
    context: LogicalPlanningContext,
    hints: Iterable[IrHint]
  ): LogicalPlan = {

    val dir = patternRelationship.directionRelativeTo(from)

    patternRelationship.length match {
      case l: VarPatternLength =>
        val projectedDir = projectedDirection(patternRelationship, from, dir)

        val solved =
          solveds.get(source.id).asSinglePlannerQuery
            .amendQueryGraph { qg =>
              val alreadySolvedExpandStepIds: Set[UsingExpandStepId] = qg.hints.collect {
                case h: UsingExpandStepHint => h.stepId
              }
              qg
                .addPatternRelationship(patternRelationship)
                .addPredicates(solvedPredicates)
                .addHints(hints.collect {
                  case h: UsingExpandStepHint
                    if LogicalPlanProducer.expandHintClaims(
                      h,
                      planFrom = from,
                      planTo = to,
                      planRelIds = Set(patternRelationship.variable),
                      planMode = expansionMode,
                      claimedStepIds = alreadySolvedExpandStepIds
                    ) => h
                })
            }

        val (rewrittenRelationshipPredicates, rewrittenNodePredicates, _, rewrittenSource) =
          solveSubqueryExpressionsForExtractedPredicates(
            source,
            nodePredicates,
            relationshipPredicates,
            Set.empty,
            context
          )

        val cachedProperties = cachedPropertiesPerPlan.get(source.id)

        annotate(
          VarExpand(
            source = rewrittenSource,
            from = from,
            dir = dir,
            projectedDir = projectedDir,
            types = patternRelationship.types,
            maybeTo = Some(to),
            maybeRelName = Some(patternRelationship.variable),
            length = l,
            expansionMode = expansionMode,
            nodePredicates = rewrittenNodePredicates.toSeq,
            relationshipPredicates = rewrittenRelationshipPredicates.toSeq,
            pathMode = pathMode
          ),
          solved,
          ProvidedOrder.Left,
          cachedProperties,
          context
        )

      case _ =>
        throw InternalException.internalError(
          this.getClass.getSimpleName,
          "Expected a varlength path to be here"
        )
    }
  }

  /**
   * `extractPredicates` extracts the Predicates ouf of the FilterScopes they are inside. The ListSubqueryExpressionSolver needs
   * to know if things are inside a different scope to work correctly. Otherwise it will plan RollupApply when not allowed,
   * or plan the wrong `NestedPlanExpression`. Since extracting the scope instead of the inner predicate is not straightforward,
   * the easiest solution is this one: we wrap each predicate in a FilterScope, give it to the ListSubqueryExpressionSolver,
   * and then extract it from the FilterScope again.
   *
   * @return rewritten predicates and source (Relationship, Node, Path, Source)
   */
  private def solveSubqueryExpressionsForExtractedPredicates(
    source: LogicalPlan,
    nodePredicates: Set[VariablePredicate],
    relationshipPredicates: Set[VariablePredicate],
    pathPredicates: Set[Expression],
    context: LogicalPlanningContext
  ): (Set[VariablePredicate], Set[VariablePredicate], Set[Expression], LogicalPlan) = {
    val solver = SubqueryExpressionSolver.solverFor(source, context)

    def solveVariablePredicate(variablePredicate: VariablePredicate): VariablePredicate = {
      val filterScope = FilterScope(variablePredicate.variable, Some(variablePredicate.predicate))(
        variablePredicate.predicate.position
      )
      val rewrittenFilterScope = solver.solve(filterScope).asInstanceOf[FilterScope]
      VariablePredicate(rewrittenFilterScope.variable, rewrittenFilterScope.innerPredicate.get)
    }

    val rewrittenRelationshipPredicates = relationshipPredicates.map(solveVariablePredicate)
    val rewrittenNodePredicates = nodePredicates.map(solveVariablePredicate)
    val rewrittenPathPredicates = pathPredicates.map(solver.solve(_))
    val rewrittenSource = solver.rewrittenPlan()
    (rewrittenRelationshipPredicates, rewrittenNodePredicates, rewrittenPathPredicates, rewrittenSource)
  }

  def fixupTrailRhsPlan(
    originalPlan: LogicalPlan,
    argumentsToRemove: Set[LogicalVariable],
    predicatesToRemove: Set[Expression]
  ): LogicalPlan = {
    val fixedSolved = solveds.get(originalPlan.id).asSinglePlannerQuery.amendQueryGraph {
      qg =>
        // We added these in QPPInnerPlanner, so for solved we have to remove them again.
        qg.removeArgumentIds(argumentsToRemove)
          .withSelections(qg.selections.filter(p => !predicatesToRemove.contains(p.expr)))
    }

    val newPlan = originalPlan.copyPlanWithIdGen(attributesWithoutSolveds.copy(originalPlan.id))
    solveds.set(newPlan.id, fixedSolved)
    newPlan
  }

  def planRepeat(
    source: LogicalPlan,
    pattern: QuantifiedPathPattern,
    startBinding: NodeBinding,
    endBinding: NodeBinding,
    context: LogicalPlanningContext,
    innerPlan: LogicalPlan,
    predicates: Seq[Expression],
    previouslyBoundRelationships: Set[LogicalVariable],
    previouslyBoundRelationshipGroups: Set[LogicalVariable],
    previouslyBoundNodes: Set[LogicalVariable],
    previouslyBoundNodeGroups: Set[LogicalVariable],
    reverseGroupVariableProjections: Boolean,
    expansionMode: ExpansionMode,
    pathMode: TraversalPathMode,
    allReduceAccumulators: Set[AllReduceAccumulator],
    hints: Iterable[IrHint]
  ): LogicalPlan = {
    // Ensure that innerPlan does conform with the pattern contained inside the quantified path pattern before we mark it as solved
    try {
      VerifyBestPlan(
        plan = innerPlan,
        expected = SinglePlannerQuery.empty
          .withHorizon(RegularQueryProjection(importedExposedSymbols = context.plannerState.importedSubqueryVariables))
          .withQueryGraph(pattern.asQueryGraph),
        context = context
      )
    } catch {
      // As the planner query is generated by us, we would never expect `VerifyBestPlan` to fail on it.
      case planVerificationException: InternalException => throw InternalException.internalError(
          this.getClass.getSimpleName,
          "The provided inner plan doesn't conform with the quantified path pattern being planned",
          planVerificationException
        )
    }

    val solved = solveds.get(source.id).asSinglePlannerQuery.amendQueryGraph { qg =>
      val alreadySolvedExpandStepIds = qg.hints.collect {
        case h: UsingExpandStepHint => h.stepId
      }
      val relationshipGroupVariables = pattern.relationshipVariableGroupings.map(_.group)
      qg.addQuantifiedPathPattern(pattern)
        .addPredicates(predicates: _*)
        .addHints(hints.collect {
          case h: UsingExpandStepHint
            if LogicalPlanProducer.expandHintClaims(
              h,
              planFrom = startBinding.outer,
              planTo = endBinding.outer,
              planRelIds = relationshipGroupVariables,
              planMode = expansionMode,
              claimedStepIds = alreadySolvedExpandStepIds
            ) => h
        })
    }

    val (rewrittenSourcePlan, rewrittenAllReduceAccumulators) =
      allReduceAccumulators.toVector.sortBy(_.position).foldLeft((source, Set.empty[AllReduceAccumulator])) {
        case ((plan, accumulators), allReduceAcc) =>
          val (rewrittenInit, rewrittenPlan) =
            SubqueryExpressionSolver.ForSingle.solve(plan, allReduceAcc.initial, context)
          (rewrittenPlan, accumulators + allReduceAcc.copy(initial = rewrittenInit)(allReduceAcc.position))
      }

    val providedOrderRule = ProvidedOrder.Left
    val repeatPlan = pathMode match {
      case TraversalPathMode.Trail =>
        RepeatTrail(
          left = rewrittenSourcePlan,
          right = innerPlan,
          repetition = pattern.repetition,
          start = startBinding.outer,
          end = endBinding.outer,
          innerStart = startBinding.inner,
          innerEnd = endBinding.inner,
          nodeVariableGroupings = pattern.nodeVariableGroupings,
          relationshipVariableGroupings = pattern.relationshipVariableGroupings,
          innerRelationships = pattern.patternRelationships.map(p => p.variable).toSet,
          previouslyBoundRelationships = previouslyBoundRelationships,
          previouslyBoundRelationshipGroups = previouslyBoundRelationshipGroups,
          reverseGroupVariableProjections = reverseGroupVariableProjections,
          expansionMode = expansionMode,
          accumulatorMappings = rewrittenAllReduceAccumulators
        )
      case TraversalPathMode.Walk =>
        RepeatWalk(
          left = rewrittenSourcePlan,
          right = innerPlan,
          repetition = pattern.repetition,
          start = startBinding.outer,
          end = endBinding.outer,
          innerStart = startBinding.inner,
          innerEnd = endBinding.inner,
          nodeVariableGroupings = pattern.nodeVariableGroupings,
          relationshipVariableGroupings = pattern.relationshipVariableGroupings,
          reverseGroupVariableProjections = reverseGroupVariableProjections,
          innerRelationships = pattern.patternRelationships.map(p => p.variable).toSet,
          expansionMode = expansionMode,
          accumulatorMappings = rewrittenAllReduceAccumulators
        )
      case TraversalPathMode.Acyclic =>
        RepeatAcyclic(
          left = rewrittenSourcePlan,
          right = innerPlan,
          repetition = pattern.repetition,
          start = startBinding.outer,
          end = endBinding.outer,
          innerStart = startBinding.inner,
          innerEnd = endBinding.inner,
          nodeVariableGroupings = pattern.nodeVariableGroupings,
          innerNodes = pattern.patternNodes,
          previouslyBoundNodes = previouslyBoundNodes,
          previouslyBoundNodeGroups = previouslyBoundNodeGroups,
          relationshipVariableGroupings = pattern.relationshipVariableGroupings,
          innerRelationships = pattern.patternRelationships.map(p => p.variable).toSet,
          previouslyBoundRelationships = previouslyBoundRelationships,
          previouslyBoundRelationshipGroups = previouslyBoundRelationshipGroups,
          reverseGroupVariableProjections = reverseGroupVariableProjections,
          expansionMode = expansionMode,
          accumulatorMappings = rewrittenAllReduceAccumulators
        )
    }
    annotate(
      repeatPlan,
      solved,
      providedOrderRule,
      cachedPropertiesPerPlan.get(innerPlan.id),
      context
    )
  }

  def planNodeByIdSeek(
    variable: LogicalVariable,
    nodeIds: SeekableArgs,
    solvedPredicates: Seq[Expression] = Seq.empty,
    argumentIds: Set[LogicalVariable],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    doPlanNodeByIdSeek(NodeByIdSeek.apply, variable, nodeIds, solvedPredicates, argumentIds, context)
  }

  def planNodeByElementIdSeek(
    variable: LogicalVariable,
    nodeIds: SeekableArgs,
    solvedPredicates: Seq[Expression] = Seq.empty,
    argumentIds: Set[LogicalVariable],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    doPlanNodeByIdSeek(NodeByElementIdSeek.apply, variable, nodeIds, solvedPredicates, argumentIds, context)
  }

  private def doPlanNodeByIdSeek(
    makePlan: (LogicalVariable, SeekableArgs, Set[LogicalVariable]) => NodeLogicalLeafPlan,
    variable: LogicalVariable,
    nodeIds: SeekableArgs,
    solvedPredicates: Seq[Expression],
    argumentIds: Set[LogicalVariable],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(
      queryGraph =
        QueryGraph.empty
          .addPatternNodes(variable)
          .addPredicates(solvedPredicates: _*)
          .addArgumentIds(argumentIds.toIndexedSeq),
      horizon = RegularQueryProjection(
        importedExposedSymbols = context.plannerState.importedSubqueryVariables
      )
    )
    val solver = SubqueryExpressionSolver.solverForLeafPlan(argumentIds, context)
    val rewrittenNodeIds = nodeIds.mapValues(solver.solve(_))
    val newArguments = solver.newArguments
    val leafPlan = annotate(
      makePlan(variable, rewrittenNodeIds, argumentIds ++ newArguments),
      solved,
      ProvidedOrder.empty,
      context.plannerState.previouslyCachedProperties,
      context
    )
    solver.rewriteLeafPlan(leafPlan)
  }

  def planNodeByLabelScan(
    variable: LogicalVariable,
    label: LabelName,
    solvedPredicates: Seq[Expression],
    solvedHint: Option[UsingScanHint] = None,
    argumentIds: Set[LogicalVariable],
    providedOrder: ProvidedOrder,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(
      queryGraph =
        QueryGraph.empty
          .addPatternNodes(variable)
          .addPredicates(solvedPredicates: _*)
          .addHints(solvedHint)
          .addArgumentIds(argumentIds.toIndexedSeq),
      horizon = RegularQueryProjection(
        importedExposedSymbols = context.plannerState.importedSubqueryVariables
      )
    )
    annotate(
      NodeByLabelScan(variable, label, argumentIds, toIndexOrder(providedOrder)),
      solved,
      providedOrder,
      context.plannerState.previouslyCachedProperties,
      context
    )
  }

  def planUnionNodeByLabelsScan(
    variable: Variable,
    labels: Seq[LabelName],
    solvedPredicates: Seq[Expression],
    solvedHints: Seq[UsingScanHint] = Seq.empty,
    argumentIds: Set[LogicalVariable],
    providedOrder: ProvidedOrder,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(
      queryGraph =
        QueryGraph.empty
          .addPatternNodes(variable)
          .addPredicates(solvedPredicates: _*)
          .addHints(solvedHints)
          .addArgumentIds(argumentIds.toIndexedSeq),
      horizon = RegularQueryProjection(
        importedExposedSymbols = context.plannerState.importedSubqueryVariables
      )
    )
    annotate(
      UnionNodeByLabelsScan(variable, labels, argumentIds, toIndexOrder(providedOrder)),
      solved,
      providedOrder,
      context.plannerState.previouslyCachedProperties,
      context
    )
  }

  def planIntersectNodeByLabelsScan(
    variable: Variable,
    labels: Seq[LabelName],
    solvedPredicates: Seq[Expression],
    solvedHints: Seq[UsingScanHint] = Seq.empty,
    argumentIds: Set[LogicalVariable],
    providedOrder: ProvidedOrder,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(
      queryGraph =
        QueryGraph.empty
          .addPatternNodes(variable)
          .addPredicates(solvedPredicates: _*)
          .addHints(solvedHints)
          .addArgumentIds(argumentIds.toIndexedSeq),
      horizon = RegularQueryProjection(
        importedExposedSymbols = context.plannerState.importedSubqueryVariables
      )
    )
    annotate(
      IntersectionNodeByLabelsScan(variable, labels, argumentIds, toIndexOrder(providedOrder)),
      solved,
      providedOrder,
      context.plannerState.previouslyCachedProperties,
      context
    )
  }

  def planSubtractionNodeByLabelsScan(
    variable: Variable,
    positiveLabels: Seq[LabelName],
    negativeLabels: Seq[LabelName],
    solvedPredicates: Seq[Expression],
    solvedHints: Seq[UsingScanHint] = Seq.empty,
    argumentIds: Set[LogicalVariable],
    providedOrder: ProvidedOrder,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(
      queryGraph =
        QueryGraph.empty
          .addPatternNodes(variable)
          .addPredicates(solvedPredicates: _*)
          .addHints(solvedHints)
          .addArgumentIds(argumentIds.toIndexedSeq),
      horizon = RegularQueryProjection(
        importedExposedSymbols = context.plannerState.importedSubqueryVariables
      )
    )
    annotate(
      SubtractionNodeByLabelsScan(variable, positiveLabels, negativeLabels, argumentIds, toIndexOrder(providedOrder)),
      solved,
      providedOrder,
      context.plannerState.previouslyCachedProperties,
      context
    )
  }

  def planDynamicLabelNodeLookup(
    variable: LogicalVariable,
    labels: Expression,
    operator: DynamicElement.SetOperator,
    argumentIds: Set[LogicalVariable],
    context: LogicalPlanningContext,
    solvedPropertyPredicates: Set[Expression],
    propertyPredicates: Map[PropertyKeyToken, Expression]
  ): LogicalPlan = {
    val predicate =
      operator match {
        case DynamicElement.All => HasDynamicLabels(variable, Seq(labels))(InputPosition.NONE)
        case DynamicElement.Any => HasAnyDynamicLabel(variable, Seq(labels))(InputPosition.NONE)
      }

    val solved = RegularSinglePlannerQuery(
      queryGraph =
        QueryGraph.empty
          .addPatternNodes(variable)
          .addPredicates(predicate)
          .addPredicates(solvedPropertyPredicates)
          .addArgumentIds(argumentIds.toIndexedSeq),
      horizon = RegularQueryProjection(
        importedExposedSymbols = context.plannerState.importedSubqueryVariables
      )
    )

    val solver = SubqueryExpressionSolver.solverForLeafPlan(argumentIds, context)
    val rewrittenLabels = solver.solve(labels)
    val newArguments = solver.newArguments

    val plan = DynamicLabelNodeLookup(
      idName = variable,
      labelExpr = DynamicElement.Simple(rewrittenLabels, operator),
      argumentIds = argumentIds.union(newArguments),
      propertyPredicates = propertyPredicates
    )

    val annotatedPlan =
      annotate(plan, solved, ProvidedOrder.empty, context.plannerState.previouslyCachedProperties, context)

    solver.rewriteLeafPlan(annotatedPlan)
  }

  def planNodeIndexSeek(
    variable: LogicalVariable,
    label: LabelToken,
    properties: Seq[IndexedProperty],
    valueExpr: QueryExpression[Expression],
    solvedPredicates: Seq[Expression] = Seq.empty,
    solvedHint: Option[UsingIndexHint] = None,
    argumentIds: Set[LogicalVariable],
    providedOrder: ProvidedOrder,
    indexOrder: IndexOrder,
    context: LogicalPlanningContext,
    indexType: IndexType,
    supportPartitionedScan: Boolean
  ): LogicalPlan = {
    val queryGraph = QueryGraph.empty
      .addPatternNodes(variable)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)

    val solved = RegularSinglePlannerQuery(
      queryGraph = queryGraph,
      horizon = RegularQueryProjection(
        importedExposedSymbols = context.plannerState.importedSubqueryVariables
      )
    )

    val solver = SubqueryExpressionSolver.solverForLeafPlan(argumentIds, context)
    val rewrittenValueExpr = valueExpr.map(solver.solve(_))
    val newArguments = solver.newArguments

    val plan = NodeIndexSeek(
      variable,
      label,
      properties,
      rewrittenValueExpr,
      argumentIds ++ newArguments,
      indexOrder,
      indexType.toPublicApi,
      supportPartitionedScan
    )

    val annotatedPlan =
      annotate(
        plan,
        solved,
        providedOrder,
        cachedPropertiesForIndexedProperties(context, variable, properties),
        context
      )

    solver.rewriteLeafPlan(annotatedPlan)
  }

  def planNodeIndexScan(
    variable: LogicalVariable,
    label: LabelToken,
    properties: Seq[IndexedProperty],
    solvedPredicates: Seq[Expression] = Seq.empty,
    solvedHint: Option[UsingIndexHint] = None,
    argumentIds: Set[LogicalVariable],
    providedOrder: ProvidedOrder,
    indexOrder: IndexOrder,
    context: LogicalPlanningContext,
    indexType: IndexType,
    supportPartitionedScan: Boolean
  ): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(
      queryGraph =
        QueryGraph.empty
          .addPatternNodes(variable)
          .addPredicates(solvedPredicates: _*)
          .addHints(solvedHint)
          .addArgumentIds(argumentIds.toIndexedSeq),
      horizon = RegularQueryProjection(
        importedExposedSymbols = context.plannerState.importedSubqueryVariables
      )
    )
    annotate(
      NodeIndexScan(
        variable,
        label,
        properties,
        argumentIds,
        indexOrder,
        indexType.toPublicApi,
        supportPartitionedScan
      ),
      solved,
      providedOrder,
      cachedPropertiesForIndexedProperties(context, variable, properties),
      context
    )
  }

  def planNodeIndexStringSearchScan(
    variable: LogicalVariable,
    label: LabelToken,
    properties: Seq[IndexedProperty],
    stringSearchMode: StringSearchMode,
    solvedPredicates: Seq[Expression],
    solvedHint: Option[UsingIndexHint],
    valueExpr: Expression,
    argumentIds: Set[LogicalVariable],
    providedOrder: ProvidedOrder,
    indexOrder: IndexOrder,
    context: LogicalPlanningContext,
    indexType: IndexType
  ): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(
      queryGraph =
        QueryGraph.empty
          .addPatternNodes(variable)
          .addPredicates(solvedPredicates: _*)
          .addHints(solvedHint)
          .addArgumentIds(argumentIds.toIndexedSeq),
      horizon = RegularQueryProjection(
        importedExposedSymbols = context.plannerState.importedSubqueryVariables
      )
    )
    val solver = SubqueryExpressionSolver.solverForLeafPlan(argumentIds, context)
    val rewrittenValueExpr = solver.solve(valueExpr)
    val newArguments = solver.newArguments

    val planTemplate = stringSearchMode match {
      case ContainsSearchMode => NodeIndexContainsScan(_, _, _, _, _, _, _)
      case EndsWithSearchMode => NodeIndexEndsWithScan(_, _, _, _, _, _, _)
    }

    val plan = planTemplate(
      variable,
      label,
      properties.head,
      rewrittenValueExpr,
      argumentIds ++ newArguments,
      indexOrder,
      indexType.toPublicApi
    )
    val annotatedPlan = annotate(
      plan,
      solved,
      providedOrder,
      cachedPropertiesForIndexedProperties(context, variable, properties),
      context
    )

    solver.rewriteLeafPlan(annotatedPlan)
  }

  def planNodeHashJoin(
    nodes: Set[LogicalVariable],
    left: LogicalPlan,
    right: LogicalPlan,
    hints: Set[UsingJoinHint],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val plannerQuery = solveds.get(left.id).asSinglePlannerQuery ++ solveds.get(right.id).asSinglePlannerQuery
    val solved = plannerQuery.amendQueryGraph(_.addHints(hints))
    annotate(
      NodeHashJoin(nodes, left, right),
      solved,
      ProvidedOrder.Right,
      cachedPropertiesPerPlan.get(left.id).intersect(cachedPropertiesPerPlan.get(right.id)),
      context
    )
  }

  def planValueHashJoin(
    left: LogicalPlan,
    right: LogicalPlan,
    join: Equals,
    originalPredicate: Expression,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val plannerQuery = solveds.get(left.id).asSinglePlannerQuery ++ solveds.get(right.id).asSinglePlannerQuery
    val solved = plannerQuery.amendQueryGraph(_.addPredicates(originalPredicate))

    val (rewrittenLhsExpr, rewrittenLhs) = SubqueryExpressionSolver.ForSingle.solve(left, join.lhs, context)
    val (rewrittenRhsExpr, rewrittenRhs) = SubqueryExpressionSolver.ForSingle.solve(right, join.rhs, context)
    val rewrittenJoin = join.copy(lhs = rewrittenLhsExpr, rhs = rewrittenRhsExpr)(join.position)

    annotate(
      ValueHashJoin(rewrittenLhs, rewrittenRhs, rewrittenJoin),
      solved,
      ProvidedOrder.Right,
      cachedPropertiesPerPlan.get(rewrittenLhs.id).intersectProperties(cachedPropertiesPerPlan.get(rewrittenRhs.id)),
      context
    )
  }

  def planMergeJoin(
    left: LogicalPlan,
    right: LogicalPlan,
    join: Equals,
    originalPredicate: Expression,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val plannerQuery = solveds.get(left.id).asSinglePlannerQuery ++ solveds.get(right.id).asSinglePlannerQuery
    val solved = plannerQuery.amendQueryGraph(_.addPredicates(originalPredicate))

    val (rewrittenLhsExpr, rewrittenLhs) = SubqueryExpressionSolver.ForSingle.solve(left, join.lhs, context)
    val (rewrittenRhsExpr, rewrittenRhs) = SubqueryExpressionSolver.ForSingle.solve(right, join.rhs, context)
    val rewrittenJoin = join.copy(lhs = rewrittenLhsExpr, rhs = rewrittenRhsExpr)(join.position)

    val mergeJoinPlan = ValueMergeJoin(rewrittenLhs, rewrittenRhs, rewrittenJoin)

    val providedOrder =
      providedOrders.get(left.id)
        .fromBoth(context.providedOrderFactory, Some(mergeJoinPlan))

    annotate(
      mergeJoinPlan,
      solved,
      providedOrder,
      cachedPropertiesPerPlan.get(rewrittenLhs.id).intersectProperties(cachedPropertiesPerPlan.get(rewrittenRhs.id)),
      context
    )
    markOrderAsLeveragedBackwardsUntilOrigin(mergeJoinPlan, context.providedOrderFactory)
    mergeJoinPlan
  }

  def planNodeUniqueIndexSeek(
    variable: LogicalVariable,
    label: LabelToken,
    properties: Seq[IndexedProperty],
    valueExpr: QueryExpression[Expression],
    solvedPredicates: Seq[Expression] = Seq.empty,
    solvedHint: Option[UsingIndexHint] = None,
    argumentIds: Set[LogicalVariable],
    providedOrder: ProvidedOrder,
    indexOrder: IndexOrder,
    context: LogicalPlanningContext,
    indexType: IndexType,
    supportPartitionedScan: Boolean
  ): LogicalPlan = {
    val queryGraph = QueryGraph.empty
      .addPatternNodes(variable)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)

    val solved = RegularSinglePlannerQuery(
      queryGraph = queryGraph,
      horizon = RegularQueryProjection(
        importedExposedSymbols = context.plannerState.importedSubqueryVariables
      )
    )

    val solver = SubqueryExpressionSolver.solverForLeafPlan(argumentIds, context)
    val rewrittenValueExpr = valueExpr.map(solver.solve(_))
    val newArguments = solver.newArguments

    val plan = NodeUniqueIndexSeek(
      variable,
      label,
      properties,
      rewrittenValueExpr,
      argumentIds ++ newArguments,
      indexOrder,
      indexType.toPublicApi,
      supportPartitionedScan
    )

    val annotatedPlan =
      annotate(
        plan,
        solved,
        providedOrder,
        cachedPropertiesForIndexedProperties(context, variable, properties),
        context
      )

    solver.rewriteLeafPlan(annotatedPlan)

  }

  def planNodeVectorIndexSearch(
    context: LogicalPlanningContext,
    resultVariable: LogicalVariable,
    labels: Seq[LabelToken],
    indexedProperties: Seq[IndexedProperty],
    indexName: String,
    embedding: Expression,
    where: Option[Where],
    maybeFilter: Option[QueryExpression[Expression]],
    limit: Expression,
    scoreVariable: Option[LogicalVariable],
    argumentIds: Set[LogicalVariable],
    implicitlySolvedPredicates: Set[Expression]
  ): LogicalPlan = {

    val solved = RegularSinglePlannerQuery(
      queryGraph =
        QueryGraph.empty
          .addPatternNodes(resultVariable)
          .addSearchClause(Some(VectorSearchClause(
            resultVariable,
            indexName,
            embedding,
            where,
            limit,
            scoreVariable
          )))
          .addPredicates(implicitlySolvedPredicates)
          .addArgumentIds(argumentIds),
      horizon = RegularQueryProjection(
        importedExposedSymbols = context.plannerState.importedSubqueryVariables
      )
    )

    val solver = SubqueryExpressionSolver.solverForLeafPlan(argumentIds, context)

    val rewrittenEmbedding = solver.solve(embedding)
    // While we cannot have subqueries in limit expressions today, we apply the solver here as a precautionary measure,
    // should we change that restriction in the future.
    val rewrittenLimit = solver.solve(limit)
    val newArguments = solver.newArguments
    val allArgumentIds = argumentIds.union(newArguments)

    def createNodeVectorIndexSearchPlan(variable: LogicalVariable) = {
      val nodeVectorIndexSearch = NodeVectorIndexSearch(
        idName = variable,
        labels = labels,
        properties = indexedProperties,
        score = scoreVariable,
        indexName = indexName,
        vector = rewrittenEmbedding,
        limit = rewrittenLimit,
        // TODO: we only produce match all for now
        entityFilter = MatchAllQueryExpression,
        maybePropertyFilter = maybeFilter,
        argumentIds = allArgumentIds
      )(idGen)

      val annotatedVectorSearchPlan =
        annotate(
          nodeVectorIndexSearch,
          solved,
          ProvidedOrder.empty,
          cachedPropertiesForIndexedProperties(context, variable, indexedProperties),
          context
        )

      solver.rewriteLeafPlan(annotatedVectorSearchPlan)
    }

    // If the variable has already been previously solved, we need to add a selection to join
    // on the vector search results
    if (argumentIds.contains(resultVariable)) {
      val renamedVariable =
        UnPositionedVariable.varFor(Namespacer.genName(
          context.staticComponents.anonymousVariableNameGenerator,
          resultVariable.name
        ))

      val rewrittenAnnotatedPlan = createNodeVectorIndexSearchPlan(renamedVariable)

      val finalPlan =
        Selection(Seq(Equals(renamedVariable, resultVariable)(InputPosition.NONE)), rewrittenAnnotatedPlan)(idGen)

      annotate(
        finalPlan,
        solved,
        ProvidedOrder.empty,
        CachedProperties.empty,
        context
      )
    } else {
      createNodeVectorIndexSearchPlan(resultVariable)
    }
  }

  def planRelationshipVectorIndexSearch(
    context: LogicalPlanningContext,
    patternRelationship: PatternRelationship,
    indexedTypes: Seq[RelationshipTypeToken],
    indexedProperties: Seq[IndexedProperty],
    indexName: String,
    embedding: Expression,
    where: Option[Where],
    maybeFilter: Option[QueryExpression[Expression]],
    limit: Expression,
    scoreVariable: Option[LogicalVariable],
    argumentIds: Set[LogicalVariable],
    implicitlySolvedPredicates: Set[Expression] = Set.empty
  ): LogicalPlan = {
    val selectionsFromUnsolvedTypes: Seq[Expression] = {
      // The relationship vector index determines the relationship type that is actually solved.
      // The index could cover multiple relationship types, so we need to check which types are actually solved by the index.
      // If the pattern relationship has a type that is either not included in the index or is a specific subset of it, then we should identify that and solve it separately as a hidden selection.
      val solvedTypes = indexedTypes.map(_.name).toSet
      val typesToSolve = patternRelationship.types.map(_.name).toSet
      if (typesToSolve.nonEmpty && !solvedTypes.subsetOf(typesToSolve)) {
        // Assume the types supported by the index are ACTS_IN and KNOWS, but the pattern relationship only allows ACTS_IN
        // Then we need to solve the ACTS_IN type separately as a hidden selection.
        // Furthermore, if the index only supports KNOWS, but the pattern relationship allows only ACTS_IN, we need a hidden selection that filters out all relationships (since a relationship only has one type).
        // However, if it was the converse, i.e., the pattern relationship allows both ACTS_IN and KNOWS, but the index only supports ACTS_IN,
        // then all relationships returned by the index would be valid since ACTS_IN is a valid subset of (ACTS_IN, KNOWS)
        // Also, if the pattern relationship does not have any types specified, then we assume all types returned by the index are valid
        val relTypeQueries = patternRelationship.types.map(relType =>
          HasTypes(patternRelationship.variable, Seq(relType))(InputPosition.NONE)
        )
        Seq(Ors.create(ListSet.from(relTypeQueries)))
      } else Seq.empty
    }

    val solvedQueryGraphWithPredicate =
      QueryGraph.empty
        .addSearchClause(Some(VectorSearchClause(
          patternRelationship.variable,
          indexName,
          embedding,
          where,
          limit,
          scoreVariable
        )))
        .addPredicates(implicitlySolvedPredicates)
        .addArgumentIds(argumentIds)
        .pipe { qg =>
          if (selectionsFromUnsolvedTypes.isEmpty) {
            // We have solved all types, lets add the pattern relationship to the query graph
            qg.addPatternRelationship(patternRelationship)
          } else {
            // Let the hidden selection handle the solved pattern relationship to the query graph
            qg
          }
        }

    val solved = RegularSinglePlannerQuery(
      queryGraph = solvedQueryGraphWithPredicate,
      horizon = RegularQueryProjection(
        importedExposedSymbols = context.plannerState.importedSubqueryVariables
      )
    )

    val solver = SubqueryExpressionSolver.solverForLeafPlan(argumentIds, context)

    val rewrittenEmbedding = solver.solve(embedding)
    val rewrittenLimit = solver.solve(limit)
    val newArguments = solver.newArguments
    val allArgumentIds = argumentIds.union(newArguments)

    val (startNode, endNode) = patternRelationship.inOrder

    val relVectorIndexSearch = patternRelationship.dir match {
      case SemanticDirection.BOTH => UndirectedRelationshipVectorIndexSearch(
          idName = Some(patternRelationship.variable),
          startNode = Some(startNode),
          endNode = Some(endNode),
          typeTokens = indexedTypes,
          properties = indexedProperties,
          score = scoreVariable,
          indexName = indexName,
          vector = rewrittenEmbedding,
          limit = rewrittenLimit,
          // TODO: we only produce match all for now
          entityFilter = MatchAllQueryExpression,
          maybePropertyFilter = maybeFilter,
          argumentIds = allArgumentIds
        )(idGen)
      case _ => DirectedRelationshipVectorIndexSearch(
          idName = Some(patternRelationship.variable),
          startNode = Some(startNode),
          endNode = Some(endNode),
          typeTokens = indexedTypes,
          properties = indexedProperties,
          score = scoreVariable,
          indexName = indexName,
          vector = rewrittenEmbedding,
          limit = rewrittenLimit,
          // TODO: we only produce match all for now
          entityFilter = MatchAllQueryExpression,
          maybePropertyFilter = maybeFilter,
          argumentIds = allArgumentIds
        )(idGen)
    }

    val annotatedPlan =
      annotate(
        relVectorIndexSearch,
        solved,
        ProvidedOrder.empty,
        cachedPropertiesForIndexedProperties(context, patternRelationship.variable, indexedProperties),
        context
      )
    val rewritten = solver.rewriteLeafPlan(annotatedPlan)

    planHiddenSelectionIfNeeded(rewritten, selectionsFromUnsolvedTypes, context, patternRelationship)
  }

  private def cachedPropertiesForIndexedProperties(
    context: LogicalPlanningContext,
    variable: LogicalVariable,
    properties: Seq[IndexedProperty]
  ): CachedProperties =
    context.plannerState.previouslyCachedProperties.addAll(properties.view.collect {
      case property if property.getValueFromIndex == GetValue =>
        CachedProperty(
          variable,
          variable,
          PropertyKeyName(property.propertyKeyToken.name)(InputPosition.NONE),
          property.entityType
        )(InputPosition.NONE)
    }.toSet)

  def planAssertSameNode(
    node: LogicalVariable,
    left: LogicalPlan,
    right: LogicalPlan,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved: SinglePlannerQuery =
      solveds.get(left.id).asSinglePlannerQuery ++ solveds.get(right.id).asSinglePlannerQuery
    annotate(
      AssertSameNode(node, left, right),
      solved,
      ProvidedOrder.Left,
      cachedPropertiesPerPlan.get(left.id).intersect(cachedPropertiesPerPlan.get(right.id)),
      context
    )
  }

  def planAssertSameRelationship(
    relationship: PatternRelationship,
    left: LogicalPlan,
    right: LogicalPlan,
    context: LogicalPlanningContext
  ): LogicalPlan =
    annotate(
      AssertSameRelationship(relationship.variable, left, right),
      solveds.get(left.id).asSinglePlannerQuery ++ solveds.get(right.id).asSinglePlannerQuery,
      ProvidedOrder.Left,
      cachedPropertiesPerPlan.get(left.id).intersect(cachedPropertiesPerPlan.get(right.id)),
      context
    )

  def planOptional(
    inputPlan: LogicalPlan,
    ids: Set[LogicalVariable],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    annotate(
      Optional(inputPlan, ids),
      solveds.get(inputPlan.id),
      ProvidedOrder.Left,
      cachedPropertiesPerPlan.get(inputPlan.id),
      context
    )
  }

  def planOptionalMatch(
    inputPlan: LogicalPlan,
    ids: Set[LogicalVariable],
    context: LogicalPlanningContext,
    optionalQG: QueryGraph
  ): LogicalPlan = {
    val patternNodes =
      optionalQG
        .patternNodes
        .intersect(ids)
        .toSeq

    val patternRelationships =
      optionalQG
        .patternRelationships
        .filter(rel => ids(rel.variable))

    val optionalMatchQG =
      solveds
        .get(inputPlan.id)
        .asSinglePlannerQuery
        .queryGraph
        .addPatternNodes(patternNodes: _*)
        .addPatternRelationships(patternRelationships)

    val solved = RegularSinglePlannerQuery(
      queryGraph =
        QueryGraph.empty
          .addOptionalMatch(optionalMatchQG)
          .withArgumentIds(ids),
      horizon = RegularQueryProjection(
        importedExposedSymbols = context.plannerState.importedSubqueryVariables
      )
    )

    annotate(
      Optional(inputPlan, ids),
      solved,
      ProvidedOrder.Left,
      cachedPropertiesPerPlan.get(inputPlan.id),
      context
    )
  }

  def planLeftOuterHashJoin(
    nodes: Set[LogicalVariable],
    left: LogicalPlan,
    right: LogicalPlan,
    hints: Set[UsingJoinHint],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(left.id).asSinglePlannerQuery.amendQueryGraph(
      _.addOptionalMatch(solveds.get(right.id).asSinglePlannerQuery.queryGraph.addHints(hints))
    )
    val inputOrder = providedOrders.get(right.id)

    val plan = LeftOuterHashJoin(nodes, left, right)
    val providedOrder =
      if (inputOrder.columns.exists(!_.isAscending)) {
        // Join nodes that are not matched from the RHS will result in rows with null in the Sort column.
        // These nulls will always be at the end. That is the correct order for ASC.
        // If there is at least a DESC column, we cannot provide any order.
        ProvidedOrder.empty
      } else {
        // If the order is on a join column (or derived from a join column), we cannot continue guaranteeing that order.
        // The join nodes that are not matched from the RHS will appear out-of-order after all join nodes which were matched.
        inputOrder
          .upToExcluding(nodes)(context.providedOrderFactory, Some(plan))
          .fromRight(context.providedOrderFactory, Some(plan))
      }
    annotate(
      plan,
      solved,
      providedOrder,
      cachedPropertiesPerPlan.get(left.id).intersect(cachedPropertiesPerPlan.get(right.id)),
      context
    )
  }

  def planRightOuterHashJoin(
    nodes: Set[LogicalVariable],
    left: LogicalPlan,
    right: LogicalPlan,
    hints: Set[UsingJoinHint],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(right.id).asSinglePlannerQuery.amendQueryGraph(
      _.addOptionalMatch(solveds.get(left.id).asSinglePlannerQuery.queryGraph.addHints(hints))
    )
    annotate(
      RightOuterHashJoin(nodes, left, right),
      solved,
      ProvidedOrder.Right,
      cachedPropertiesPerPlan.get(left.id).intersect(cachedPropertiesPerPlan.get(right.id)),
      context
    )
  }

  def planSelection(source: LogicalPlan, predicates: Seq[Expression], context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(source.id).asSinglePlannerQuery
    val (rewrittenPredicates, rewrittenSource) =
      SubqueryExpressionSolver.ForMulti.solve(source, predicates, context)

    val RemoteBatchingSubQueryResult(
      rewrittenExpressionsWithCachedProperties,
      planWithProperties
    ) =
      context.settings.remoteBatchPropertiesStrategy.planBatchPropertiesForSelections(
        solved.queryGraph,
        rewrittenSource,
        context,
        rewrittenPredicates
      )

    // planBatchPropertiesForSelections can solve some property predicates using RemoteBatchPropertiesWithFilter, which
    // will be evaluated on the shards. We need to consider those predicates as being solved by this selection.
    val solvedWithFetchedProperties = solveds.get(planWithProperties.id).asSinglePlannerQuery

    // The rewrittenExpressionsWithCachedProperties will be solved too by this selection
    val expressionsToReport = rewrittenExpressionsWithCachedProperties.originalExpressions.toSeq
    val updatedSourcePlan =
      solvedWithFetchedProperties.updateTailOrSelf(_.amendQueryGraph(_.addPredicates(expressionsToReport: _*)))

    coercePredicatesWithAnds(
      rewrittenExpressionsWithCachedProperties.allRewrittenExpressions
    ).fold(planWithProperties) {
      coercedRewrittenPredicates =>
        annotateSelection(
          Selection(coercedRewrittenPredicates, planWithProperties),
          updatedSourcePlan,
          ProvidedOrder.Left,
          cachedPropertiesPerPlan.get(planWithProperties.id),
          context
        )
    }
  }

  def planSelectionWithSolvedPredicates(
    source: LogicalPlan,
    previouslyRewrittenPredicates: RewrittenSubQueryPredicatesMap,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved =
      solveds.get(source.id).asSinglePlannerQuery.updateTailOrSelf(
        _.amendQueryGraph(_.addPredicates(previouslyRewrittenPredicates.originalExpressions))
      )
    val (rewrittenPredicates, rewrittenSource) =
      SubqueryExpressionSolver.ForMulti.solve(
        source,
        previouslyRewrittenPredicates.allRewrittenExpressions.toSeq,
        context
      )
    val cachedProperties = cachedPropertiesPerPlan.get(source.id)

    coercePredicatesWithAnds(rewrittenPredicates.allRewrittenExpressions).fold(source) { coercedRewrittenPredicates =>
      annotateSelection(
        Selection(coercedRewrittenPredicates, rewrittenSource),
        solved,
        ProvidedOrder.Left,
        cachedProperties,
        context
      )
    }
  }

  def planHorizonSelection(
    source: LogicalPlan,
    previouslyRewrittenPredicates: RewrittenSubQueryPredicatesMap,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(source.id).asSinglePlannerQuery.updateTailOrSelf(_.updateHorizon {
      case p: QueryProjection => p.addPredicates(previouslyRewrittenPredicates.originalExpressions)
      case _ => throw new IllegalArgumentException("You can only plan HorizonSelection after a projection")
    })

    val (rewrittenPredicates, rewrittenSource) =
      if (
        context.settings.executionModel.providedOrderPreserving || interestingOrderConfig.orderToSolve.requiredOrderCandidate.isEmpty
      ) {
        // solve existential subquery predicates
        val (solvedPredicates, existsPlan) =
          SubqueryExpressionSolver.ForExistentialSubquery.solve(
            source,
            previouslyRewrittenPredicates.originalExpressions, // lets use the original expressions here since the solver will also rewrite them.
            interestingOrderConfig,
            context
          )
        val unsolvedPredicates =
          previouslyRewrittenPredicates.backingStore.filterNot {
            case (rewritten, original) => solvedPredicates.contains(rewritten) || solvedPredicates.contains(original)
          }.keys.toSeq
        // solve remaining predicates
        val (solvedExpressions, solvedPlan) =
          SubqueryExpressionSolver.ForMulti.solve(existsPlan, unsolvedPredicates, context)
        (solvedExpressions.allRewrittenExpressions, solvedPlan)
      } else {
        // If the execution model does not preserve order and there is an ORDER BY, we are not allowed to use
        // NestedPlanExpressions here.
        val rewriter = irExpressionRewriter(source, context)
        val rewrittenPredicates = previouslyRewrittenPredicates.allRewrittenExpressions.endoRewrite(rewriter)
        (rewrittenPredicates, source)
      }

    coercePredicatesWithAnds(rewrittenPredicates).fold(rewrittenSource) { coercedRewrittenPredicates =>
      annotateSelection(
        Selection(coercedRewrittenPredicates, rewrittenSource),
        solved,
        ProvidedOrder.Left,
        cachedPropertiesPerPlan.get(source.id),
        context
      )
    }
  }

  /**
   * Plan a selection with `solved` already given.
   * The predicates are not run through the [[SubqueryExpressionSolver]], so they must not contain any IR expressions.
   */
  private def planSelectionWithGivenSolved(
    source: LogicalPlan,
    predicates: Seq[Expression],
    solved: PlannerQuery,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val RemoteBatchingSubQueryResult(
      rewrittenExpressionsWithCachedProperties,
      planWithProperties
    ) =
      context.settings.remoteBatchPropertiesStrategy.planBatchPropertiesForSelections(
        solved.asSinglePlannerQuery.queryGraph,
        source,
        context,
        RewrittenSubQueryPredicates.withNoRewrittenExprs(predicates)
      )
    coercePredicatesWithAnds(rewrittenExpressionsWithCachedProperties.allRewrittenExpressions).fold(source) {
      coercedPredicates =>
        annotateSelection(
          Selection(coercedPredicates, planWithProperties),
          solved,
          ProvidedOrder.Left,
          cachedPropertiesPerPlan.get(planWithProperties.id),
          context
        )
    }
  }

  // Using the solver for `expr` in all SemiApply-like plans is kinda stupid.
  // The idea is that `expr` is cheap to evaluate while the subquery (`inner`) is costly.
  // If `expr` is _also_ an IRExpression, that is not true any longer,
  // and it could be cheaper to execute the one subquery  (`inner`) instead of the other (`expr`).

  def planSelectOrAntiSemiApply(
    outer: LogicalPlan,
    inner: LogicalPlan,
    expr: Expression,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val (rewrittenExpr, rewrittenOuter) = SubqueryExpressionSolver.ForSingle.solve(outer, expr, context)
    annotate(
      SelectOrAntiSemiApply(rewrittenOuter, inner, rewrittenExpr),
      solveds.get(outer.id),
      ProvidedOrder.Left,
      cachedPropertiesPerPlan.get(rewrittenOuter.id),
      context
    )
  }

  def planLetSelectOrAntiSemiApply(
    outer: LogicalPlan,
    inner: LogicalPlan,
    id: LogicalVariable,
    expr: Expression,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val (rewrittenExpr, rewrittenOuter) = SubqueryExpressionSolver.ForSingle.solve(outer, expr, context)
    annotate(
      LetSelectOrAntiSemiApply(rewrittenOuter, inner, id, rewrittenExpr),
      solveds.get(outer.id),
      ProvidedOrder.Left,
      cachedPropertiesPerPlan.get(rewrittenOuter.id),
      context
    )
  }

  def planSelectOrSemiApply(
    outer: LogicalPlan,
    inner: LogicalPlan,
    expr: Expression,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val (rewrittenExpr, rewrittenOuter) = SubqueryExpressionSolver.ForSingle.solve(outer, expr, context)
    annotate(
      SelectOrSemiApply(rewrittenOuter, inner, rewrittenExpr),
      solveds.get(outer.id),
      ProvidedOrder.Left,
      cachedPropertiesPerPlan.get(rewrittenOuter.id),
      context
    )
  }

  def planLetSelectOrSemiApply(
    outer: LogicalPlan,
    inner: LogicalPlan,
    id: LogicalVariable,
    expr: Expression,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val (rewrittenExpr, rewrittenOuter) = SubqueryExpressionSolver.ForSingle.solve(outer, expr, context)
    annotate(
      LetSelectOrSemiApply(rewrittenOuter, inner, id, rewrittenExpr),
      solveds.get(outer.id),
      ProvidedOrder.Left,
      cachedPropertiesPerPlan.get(rewrittenOuter.id),
      context
    )
  }

  def planLetAntiSemiApply(
    left: LogicalPlan,
    right: LogicalPlan,
    id: LogicalVariable,
    context: LogicalPlanningContext
  ): LogicalPlan =
    annotate(
      LetAntiSemiApply(left, right, id),
      solveds.get(left.id),
      ProvidedOrder.Left,
      cachedPropertiesPerPlan.get(right.id),
      context
    )

  def planLetSemiApply(
    left: LogicalPlan,
    right: LogicalPlan,
    id: LogicalVariable,
    context: LogicalPlanningContext
  ): LogicalPlan =
    annotate(
      LetSemiApply(left, right, id),
      solveds.get(left.id),
      ProvidedOrder.Left,
      cachedPropertiesPerPlan.get(right.id),
      context
    )

  def planAntiSemiApply(
    left: LogicalPlan,
    right: LogicalPlan,
    expr: Expression,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addPredicates(expr)))
    annotate(AntiSemiApply(left, right), solved, ProvidedOrder.Left, cachedPropertiesPerPlan.get(right.id), context)
  }

  def planSemiApply(
    left: LogicalPlan,
    right: LogicalPlan,
    expr: Expression,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addPredicates(expr)))
    annotate(SemiApply(left, right), solved, ProvidedOrder.Left, cachedPropertiesPerPlan.get(right.id), context)
  }

  def planSemiApplyInHorizon(
    left: LogicalPlan,
    right: LogicalPlan,
    expr: Expression,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.updateHorizon {
      case horizon: QueryProjection => horizon.addPredicates(expr)
      case horizon                  => horizon
    })
    annotate(SemiApply(left, right), solved, ProvidedOrder.Left, cachedPropertiesPerPlan.get(right.id), context)
  }

  def planAntiSemiApplyInHorizon(
    left: LogicalPlan,
    right: LogicalPlan,
    expr: Expression,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.updateHorizon {
      case horizon: QueryProjection => horizon.addPredicates(expr)
      case horizon                  => horizon
    })
    annotate(AntiSemiApply(left, right), solved, ProvidedOrder.Left, cachedPropertiesPerPlan.get(right.id), context)
  }

  def planQueryArgument(queryGraph: QueryGraph, context: LogicalPlanningContext): LogicalPlan = {
    val patternNodes = queryGraph.argumentIds intersect queryGraph.patternNodes
    val patternRels =
      queryGraph.patternRelationships
        .filter(rel => queryGraph.argumentIds.contains(rel.variable))
        .map(_.variable)
    val otherIds = queryGraph.argumentIds -- patternNodes
    planArgument(patternNodes, patternRels, otherIds, context, context.plannerState.previouslyCachedProperties)
  }

  def planArgument(
    patternNodes: Set[LogicalVariable],
    patternRels: Set[LogicalVariable] = Set.empty,
    other: Set[LogicalVariable] = Set.empty,
    context: LogicalPlanningContext,
    previouslyCachedProperties: CachedProperties
  ): LogicalPlan = {
    val coveredIds = patternNodes ++ patternRels ++ other

    val solved = RegularSinglePlannerQuery(
      queryGraph =
        QueryGraph(
          argumentIds = coveredIds,
          patternNodes = patternNodes,
          patternRelationships = Set.empty
        ),
      horizon = RegularQueryProjection(
        importedExposedSymbols = context.plannerState.importedSubqueryVariables
      )
    )

    annotate(Argument(coveredIds), solved, ProvidedOrder.empty, previouslyCachedProperties, context)
  }

  def planArgument(context: LogicalPlanningContext): LogicalPlan =
    annotate(
      Argument(Set.empty),
      RegularSinglePlannerQuery(horizon =
        RegularQueryProjection(importedExposedSymbols = context.plannerState.importedSubqueryVariables)
      ),
      ProvidedOrder.empty,
      context.plannerState.previouslyCachedProperties,
      context
    )

  def planEmptyProjection(inner: LogicalPlan, context: LogicalPlanningContext): LogicalPlan =
    annotate(
      EmptyResult(inner),
      solveds.get(inner.id),
      ProvidedOrder.empty,
      cachedPropertiesPerPlan.get(inner.id),
      context
    )

  def planStarProjection(
    inner: LogicalPlan,
    reported: MaybeReportedProjections
  ): LogicalPlan = {
    reported.maybeProjections.fold(inner) { reported =>
      val newSolved: SinglePlannerQuery = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(
        _.updateQueryProjection(_.withAddedProjections(reported))
      )

      markAsSolved(inner, newSolved)
    }
  }

  /**
   * @param expressions must be solved by the ListSubqueryExpressionSolver. This is not done here since that can influence the projection list,
   *                    thus this logic is put into [[projection]] instead.
   */
  def planRegularProjection(
    inner: LogicalPlan,
    expressions: Map[LogicalVariable, Expression],
    reported: MaybeReportedProjections,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val innerSolved: SinglePlannerQuery = solveds.get(inner.id).asSinglePlannerQuery
    val solved = reported.maybeProjections.fold(innerSolved) { reportedProjections =>
      innerSolved.updateTailOrSelf(_.updateQueryProjection(_.withAddedProjections(reportedProjections)))
    }

    val newProjections = expressions.view.filterKeys(projectedVar =>
      !inner.availableSymbols.contains(projectedVar)
    ).toMap
    if (newProjections.isEmpty) {
      markAsSolved(inner, solved)
    } else
      planRegularProjectionHelper(inner, newProjections, context, solved, cachedPropertiesPerPlan.get(inner.id))
  }

  /**
   * @param grouping                 must be solved by the ListSubqueryExpressionSolver. This is not done here since that can influence if we plan aggregation or projection, etc,
   *                                 thus this logic is put into [[aggregation]] instead.
   * @param aggregation              must be solved by the ListSubqueryExpressionSolver.
   * @param previousInterestingOrder The previous interesting order, if it exists, and only if the plannerQuery has an empty query graph.
   */
  def planAggregation(
    left: LogicalPlan,
    grouping: Map[LogicalVariable, Expression],
    aggregation: Map[LogicalVariable, Expression],
    reportedGrouping: Map[LogicalVariable, Expression],
    reportedAggregation: Map[LogicalVariable, Expression],
    previousInterestingOrder: Option[InterestingOrder],
    optionalPreprocessingToPlan: AggregatingQueryProjection.OptionalPreprocessing,
    optionalPreprocessingToReport: AggregatingQueryProjection.OptionalPreprocessing,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(
      AggregatingQueryProjection(
        groupingExpressions = reportedGrouping,
        aggregationExpressions = reportedAggregation,
        importedExposedSymbols = context.plannerState.importedSubqueryVariables,
        optionalPreprocessing = optionalPreprocessingToReport
      )
    ))

    val sourcePlan = planOptionalPreprocessingForAggregation(left, optionalPreprocessingToPlan, context)

    // NOTE: aggregation order is not used here as it is lost after aggregation
    val trimmedAndRenamed = trimAndRenameProvidedOrder(providedOrders.get(sourcePlan.id), grouping)

    val agg = Aggregation(sourcePlan, grouping, aggregation)
    val plan = annotate(
      agg,
      solved,
      context.providedOrderFactory.providedOrder(trimmedAndRenamed, ProvidedOrder.Left, Some(agg)),
      cachedPropertiesPerPlan.get(sourcePlan.id).retain(accessedPropertiesInGroupingKeys(grouping)),
      context
    )

    def hasCollectOrUDF = aggregation.values.exists {
      case fi: FunctionInvocation => fi.function == Collect || fi.function == UnresolvedFunction
      case _                      => false
    }
    def hasOrderedAggregation = aggregation.values.exists {
      case fi: FunctionInvocation => fi.isOrdered
      case _                      => false
    }
    // Aggregation functions may leverage the order of a preceding ORDER BY, if no other clause is inbetween.
    // Collect and potentially user defined aggregations need this.
    // Also ordered aggregation functions (e.g. count(DISTINCT x) ASC) will need
    // a leveragedOrder hint to ensure rows are sent through in argument order.
    if (
      (previousInterestingOrder.exists(_.requiredOrderCandidate.nonEmpty) && hasCollectOrUDF) || hasOrderedAggregation
    ) {
      markOrderAsLeveragedBackwardsUntilOrigin(plan, context.providedOrderFactory)
    }

    plan
  }

  private def planOptionalPreprocessingForAggregation(
    source: LogicalPlan,
    optionalPreprocessing: AggregatingQueryProjection.OptionalPreprocessing,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    optionalPreprocessing match {
      case AggregatingQueryProjection.OptionalPreprocessing.Passthrough =>
        source

      case AggregatingQueryProjection.OptionalPreprocessing.FilterAndLimit(filterExpr, limitExpr) =>
        val solved = solveds.get(source.id)

        val filtered = filterExpr.fold(source) { filterExpr =>
          planSelectionWithGivenSolved(source, Seq(filterExpr), solved, context)
        }

        val limit = planLimitOnTopOf(filtered, limitExpr)
        annotate(limit, solved, ProvidedOrder.Left, cachedPropertiesPerPlan.get(filtered.id), context)
    }
  }

  def planOrderedAggregation(
    left: LogicalPlan,
    grouping: Map[LogicalVariable, Expression],
    aggregation: Map[LogicalVariable, Expression],
    orderToLeverage: Seq[Expression],
    reportedGrouping: Map[LogicalVariable, Expression],
    reportedAggregation: Map[LogicalVariable, Expression],
    optionalPreprocessing: AggregatingQueryProjection.OptionalPreprocessing,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(
      AggregatingQueryProjection(
        groupingExpressions = reportedGrouping,
        aggregationExpressions = reportedAggregation,
        importedExposedSymbols = context.plannerState.importedSubqueryVariables,
        optionalPreprocessing = optionalPreprocessing
      )
    ))

    // NOTE: aggregation order is not used here as it is lost after aggregation
    val trimmedAndRenamed = trimAndRenameProvidedOrder(providedOrders.get(left.id), grouping)

    val agg = OrderedAggregation(left, grouping, aggregation, orderToLeverage)
    val plan = annotate(
      agg,
      solved,
      context.providedOrderFactory.providedOrder(trimmedAndRenamed, ProvidedOrder.Left, Some(agg)),
      cachedPropertiesPerPlan.get(left.id).retain(accessedPropertiesInGroupingKeys(grouping)),
      context
    )
    markOrderAsLeveragedBackwardsUntilOrigin(plan, context.providedOrderFactory)
    plan
  }

  /**
   * The only purpose of this method is to set the solved correctly for something that is already sorted.
   */
  def updateSolvedForSortedItems(
    inner: LogicalPlan,
    interestingOrder: InterestingOrder,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    // Keep some attributes, but change solved
    val keptAttributes =
      Attributes(idGen, cardinalities, leveragedOrders, labelAndRelTypeInfos, cachedPropertiesPerPlan)
    val newPlan = inner.copyPlanWithIdGen(keptAttributes.copy(inner.id))
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.withInterestingOrder(interestingOrder))
    val providedOrder = providedOrders.get(inner.id)
    annotate(newPlan, solved, providedOrder, cachedPropertiesPerPlan.get(inner.id), context)
  }

  def planCountStoreNodeAggregation(
    query: SinglePlannerQuery,
    projectedColumn: LogicalVariable,
    labels: List[Option[LabelName]],
    argumentIds: Set[LogicalVariable],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(query.queryGraph, query.interestingOrder, query.horizon)
    val plan = NodeCountFromCountStore(projectedColumn, labels, argumentIds)
    annotate(
      plan,
      solved,
      context.providedOrderFactory.providedOrder(
        query.interestingOrder.requiredOrderCandidate.order,
        ProvidedOrder.Self,
        Some(plan)
      ),
      context.plannerState.previouslyCachedProperties,
      context
    )
  }

  def planCountStoreRelationshipAggregation(
    query: SinglePlannerQuery,
    variable: LogicalVariable,
    startLabel: Option[LabelName],
    typeNames: Seq[RelTypeName],
    endLabel: Option[LabelName],
    argumentIds: Set[LogicalVariable],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved: SinglePlannerQuery = RegularSinglePlannerQuery(query.queryGraph, query.interestingOrder, query.horizon)
    val plan = RelationshipCountFromCountStore(variable, startLabel, typeNames, endLabel, argumentIds)
    annotate(
      plan,
      solved,
      context.providedOrderFactory.providedOrder(
        query.interestingOrder.requiredOrderCandidate.order,
        ProvidedOrder.Self,
        Some(plan)
      ),
      context.plannerState.previouslyCachedProperties,
      context
    )
  }

  def planSkip(
    inner: LogicalPlan,
    count: Expression,
    interestingOrder: InterestingOrder,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    // `count` is not allowed to be an IRExpression
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(
      _.updateQueryProjection(_.updatePagination(_.withSkipExpression(count)))
    )
    val plan = annotate(Skip(inner, count), solved, ProvidedOrder.Left, cachedPropertiesPerPlan.get(inner.id), context)
    if (interestingOrder.requiredOrderCandidate.nonEmpty) {
      markOrderAsLeveragedBackwardsUntilOrigin(plan, context.providedOrderFactory)
    }
    plan
  }

  def planLoadCSV(
    inner: LogicalPlan,
    variable: LogicalVariable,
    url: Expression,
    format: CSVFormat,
    fieldTerminator: Option[StringLiteral],
    context: LogicalPlanningContext,
    importedSymbolsFromLastCallSubquery: Set[LogicalVariable]
  ): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(LoadCSVProjection(
      variable,
      url,
      format,
      fieldTerminator,
      importedSymbolsFromLastCallSubquery
    )))
    val (rewrittenUrl, rewrittenInner) = SubqueryExpressionSolver.ForSingle.solve(inner, url, context)
    annotate(
      LoadCSV(
        rewrittenInner,
        rewrittenUrl,
        variable,
        format,
        fieldTerminator.map(_.value),
        context.settings.legacyCsvQuoteEscaping,
        context.settings.csvBufferSize
      ),
      solved,
      ProvidedOrder.Left,
      CachedProperties.empty,
      context
    )
  }

  def planInput(symbols: Seq[Variable], context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(
      queryInput = Some(symbols),
      horizon = RegularQueryProjection(
        importedExposedSymbols = context.plannerState.importedSubqueryVariables
      )
    )
    annotate(Input(symbols.map(_.name)), solved, ProvidedOrder.empty, CachedProperties.empty, context)
  }

  def planUnwind(
    inner: LogicalPlan,
    variable: LogicalVariable,
    expression: Expression,
    context: LogicalPlanningContext,
    importedSymbolsFromLastCallSubquery: Set[LogicalVariable]
  ): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery
        .updateTailOrSelf(_.withHorizon(UnwindProjection(variable, expression, importedSymbolsFromLastCallSubquery)))
    val (rewrittenExpression, rewrittenInner) = SubqueryExpressionSolver.ForSingle.solve(inner, expression, context)
    val RemoteBatchingResult(
      rewrittenExpressionsWithCachedProperties,
      planWithAllProperties
    ) = context.settings.remoteBatchPropertiesStrategy.planRemoteBatchProperties(
      rewrittenInner,
      context,
      Iterable(rewrittenExpression)
    )

    annotate(
      UnwindCollection(
        planWithAllProperties,
        variable,
        rewrittenExpressionsWithCachedProperties.rewrittenExpressionOrSelf(rewrittenExpression)
      ),
      solved,
      ProvidedOrder.Left,
      cachedPropertiesPerPlan.get(planWithAllProperties.id),
      context
    )
  }

  def planProcedureCall(
    inner: LogicalPlan,
    call: ResolvedNonLocalCall,
    context: LogicalPlanningContext,
    importedSymbolsFromLastCallSubquery: Set[LogicalVariable]
  ): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery
        .updateTailOrSelf(_.withHorizon(ProcedureCallProjection(call, importedSymbolsFromLastCallSubquery)))
    val solver = SubqueryExpressionSolver.solverFor(inner, context)
    val rewrittenCall = call.mapCallArguments(solver.solve(_))
    val rewrittenInner = solver.rewrittenPlan()

    val _call = if (call.containsNoUpdates)
      annotate(
        ProcedureCall(rewrittenInner, rewrittenCall),
        solved,
        ProvidedOrder.Left,
        cachedPropertiesPerPlan.get(inner.id),
        context
      )
    else
      annotate(
        ProcedureCall(rewrittenInner, rewrittenCall),
        solved,
        ProvidedOrder.empty,
        CachedProperties.empty,
        context
      )

    if (call.optional) planOptional(_call, inner.availableSymbols, context) else _call
  }

  def planCommand(
    inner: LogicalPlan,
    clause: CommandClause,
    context: LogicalPlanningContext,
    importedSymbolsFromLastCallSubquery: Set[LogicalVariable]
  ): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(
      CommandProjection(clause, importedSymbolsFromLastCallSubquery)
    ))

    def removeUnneededVariables(columns: List[ShowColumn], yieldItems: List[CommandResultItem]) = {
      val relevantVariables =
        if (yieldItems.nonEmpty) yieldItems.map(_.aliasedVariable).toSet
        else columns.map(_.variable).toSet

      val showColumns = columns.map(sc => CommandDefaultColumn(sc.name, sc.cypherType))
      val yieldColumns = yieldItems.map(yc => CommandYieldColumn(yc.originalName, yc.aliasedVariable.name))

      (relevantVariables, showColumns, yieldColumns)
    }

    val plan = clause match {
      case s: ShowIndexesClause =>
        val (relevantVariables, showColumns, yieldColumns) =
          removeUnneededVariables(s.unfilteredColumns.columns, s.yieldItems)
        ShowIndexes(
          s.indexType,
          showColumns,
          yieldColumns,
          s.yieldAll,
          relevantVariables,
          inner.availableSymbols
        )
      case s: ShowConstraintsClause =>
        val (relevantVariables, showColumns, yieldColumns) =
          removeUnneededVariables(s.unfilteredColumns.columns, s.yieldItems)
        ShowConstraints(
          s.constraintType,
          showColumns,
          yieldColumns,
          s.yieldAll,
          relevantVariables,
          inner.availableSymbols
        )
      case s: ShowCurrentGraphTypeClause =>
        val (relevantVariables, showColumns, yieldColumns) =
          removeUnneededVariables(s.unfilteredColumns.columns, s.yieldItems)
        ShowCurrentGraphType(
          s.asGraph,
          showColumns,
          yieldColumns,
          s.yieldAll,
          relevantVariables,
          inner.availableSymbols
        )
      case s: ShowProceduresClause =>
        val (relevantVariables, showColumns, yieldColumns) =
          removeUnneededVariables(s.unfilteredColumns.columns, s.yieldItems)
        ShowProcedures(
          s.executable,
          showColumns,
          yieldColumns,
          s.yieldAll,
          relevantVariables,
          inner.availableSymbols
        )
      case s: ShowFunctionsClause =>
        val (relevantVariables, showColumns, yieldColumns) =
          removeUnneededVariables(s.unfilteredColumns.columns, s.yieldItems)
        ShowFunctions(
          s.functionType,
          s.executable,
          showColumns,
          yieldColumns,
          s.yieldAll,
          relevantVariables,
          inner.availableSymbols
        )
      case s: ShowTransactionsClause =>
        val (relevantVariables, showColumns, yieldColumns) =
          removeUnneededVariables(s.unfilteredColumns.columns, s.yieldItems)
        ShowTransactions(
          s.names,
          showColumns,
          yieldColumns,
          s.yieldAll,
          relevantVariables,
          inner.availableSymbols
        )
      case s: TerminateTransactionsClause =>
        val (relevantVariables, showColumns, yieldColumns) =
          removeUnneededVariables(s.unfilteredColumns.columns, s.yieldItems)
        TerminateTransactions(s.names, showColumns, yieldColumns, s.yieldAll, relevantVariables, inner.availableSymbols)
      case s: ShowSettingsClause =>
        val (relevantVariables, showColumns, yieldColumns) =
          removeUnneededVariables(s.unfilteredColumns.columns, s.yieldItems)
        ShowSettings(s.names, showColumns, yieldColumns, s.yieldAll, relevantVariables, inner.availableSymbols)
      // System database only commands
      case s: ShowDatabasesClause =>
        val (relevantVariables, showColumns, yieldColumns) =
          removeUnneededVariables(s.unfilteredColumns.columns, s.yieldItems)
        ShowDatabases(s.dbScope, showColumns, yieldColumns, s.yieldAll, relevantVariables, inner.availableSymbols)

    }
    val annotatedPlan = annotate(plan, solved, ProvidedOrder.empty, CachedProperties.empty, context)

    val apply = Apply(inner, annotatedPlan)
    annotate(apply, solved, ProvidedOrder.empty, cachedPropertiesPerPlan.get(annotatedPlan.id), context)
  }

  def planPassAll(
    inner: LogicalPlan,
    context: LogicalPlanningContext,
    importedSymbolsFromLastCallSubquery: Set[LogicalVariable]
  ): LogicalPlan = {
    val solved = solveds.get(inner.id)
      .asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(PassthroughAllHorizon(importedSymbolsFromLastCallSubquery)))
    // Keep some attributes, but change solved
    val keptAttributes =
      Attributes(idGen, cardinalities, leveragedOrders, providedOrders, labelAndRelTypeInfos, cachedPropertiesPerPlan)
    val newPlan = inner.copyPlanWithIdGen(keptAttributes.copy(inner.id))
    annotate(newPlan, solved, providedOrders.get(inner.id), cachedPropertiesPerPlan.get(inner.id), context)
  }

  def planLimit(
    inner: LogicalPlan,
    effectiveCount: Expression,
    reportedCount: Expression,
    interestingOrder: InterestingOrder,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    // `effectiveCount` is not allowed to be an IRExpression
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(
      _.updateQueryProjection(_.updatePagination(_.withLimitExpression(reportedCount)))
    )
    val plan =
      annotate(Limit(inner, effectiveCount), solved, ProvidedOrder.Left, cachedPropertiesPerPlan.get(inner.id), context)
    if (interestingOrder.requiredOrderCandidate.nonEmpty) {
      markOrderAsLeveragedBackwardsUntilOrigin(plan, context.providedOrderFactory)
    }
    plan
  }

  def planExhaustiveLimit(
    inner: LogicalPlan,
    effectiveCount: Expression,
    reportedCount: Expression,
    interestingOrder: InterestingOrder,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    // `effectiveCount` is not allowed to be an IRExpression
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(
      _.updateQueryProjection(_.updatePagination(_.withLimitExpression(reportedCount)))
    )
    val plan = annotate(
      ExhaustiveLimit(inner, effectiveCount),
      solved,
      ProvidedOrder.Left,
      cachedPropertiesPerPlan.get(inner.id),
      context
    )
    if (interestingOrder.requiredOrderCandidate.nonEmpty) {
      markOrderAsLeveragedBackwardsUntilOrigin(plan, context.providedOrderFactory)
    }
    plan
  }

  // In case we have SKIP n LIMIT m, we want to limit by (n + m), since we plan the Limit before the Skip.
  def planSkipAndLimit(
    inner: LogicalPlan,
    skipExpr: Expression,
    limitExpr: Expression,
    interestingOrder: InterestingOrder,
    context: LogicalPlanningContext,
    useExhaustiveLimit: Boolean
  ): LogicalPlan = {
    val solvedSkip = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(
      _.updateQueryProjection(_.updatePagination(_.withSkipExpression(skipExpr)))
    )
    val solvedSkipAndLimit =
      solvedSkip.updateTailOrSelf(_.updateQueryProjection(_.updatePagination(_.withLimitExpression(limitExpr))))

    val skipCardinality = cardinalityModel(
      solvedSkip,
      context.plannerState.input.labelInfo,
      context.plannerState.input.relTypeInfo,
      context.semanticTable,
      context.plannerState.indexCompatiblePredicatesProviderContext,
      context.staticComponents.graphSchemaOptimizations
    )
    val limitCardinality = cardinalityModel(
      solvedSkipAndLimit,
      context.plannerState.input.labelInfo,
      context.plannerState.input.relTypeInfo,
      context.semanticTable,
      context.plannerState.indexCompatiblePredicatesProviderContext,
      context.staticComponents.graphSchemaOptimizations
    )
    val innerCardinality = cardinalities.get(inner.id)
    val skippedRows = innerCardinality - skipCardinality

    val effectiveLimitExpr = Add(limitExpr, skipExpr)(limitExpr.position)
    val limitPlan =
      if (useExhaustiveLimit) {
        planExhaustiveLimit(inner, effectiveLimitExpr, limitExpr, interestingOrder, context)
      } else {
        planLimit(inner, effectiveLimitExpr, limitExpr, interestingOrder, context)
      }
    cardinalities.set(limitPlan.id, skippedRows + limitCardinality)

    planSkip(limitPlan, skipExpr, interestingOrder, context)
  }

  def planLimitForAggregation(
    inner: LogicalPlan,
    reportedGrouping: Map[LogicalVariable, Expression],
    reportedAggregation: Map[LogicalVariable, Expression],
    interestingOrder: InterestingOrder,
    optionalPreprocessing: AggregatingQueryProjection.OptionalPreprocessing,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(
      AggregatingQueryProjection(
        groupingExpressions = reportedGrouping,
        aggregationExpressions = reportedAggregation,
        importedExposedSymbols = context.plannerState.importedSubqueryVariables,
        optionalPreprocessing = optionalPreprocessing
      )
    ).withInterestingOrder(interestingOrder))
    val providedOrderRule = ProvidedOrder.Left
    val limitPlan = planLimitOnTopOf(inner, SignedDecimalIntegerLiteral("1")(InputPosition.NONE))
    val annotatedLimitPlan =
      annotate(limitPlan, solved, providedOrderRule, cachedPropertiesPerPlan.get(inner.id), context)

    // The limit leverages the order, not the following optional
    markOrderAsLeveragedBackwardsUntilOrigin(annotatedLimitPlan, context.providedOrderFactory)

    val plan = Optional(annotatedLimitPlan)
    annotate(plan, solved, providedOrderRule, cachedPropertiesPerPlan.get(annotatedLimitPlan.id), context)
  }

  def planSort(
    inner: LogicalPlan,
    sortColumns: Seq[ColumnOrder],
    orderColumns: Seq[ordering.ColumnOrder],
    interestingOrder: InterestingOrder,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.withInterestingOrder(interestingOrder))
    val plan = Sort(inner, sortColumns)
    annotate(
      plan,
      solved,
      context.providedOrderFactory.providedOrder(orderColumns, ProvidedOrder.Self, Some(plan)),
      cachedPropertiesPerPlan.get(inner.id),
      context
    )
  }

  def planTop(
    inner: LogicalPlan,
    limit: Expression,
    sortColumns: Seq[ColumnOrder],
    orderColumns: Seq[ordering.ColumnOrder],
    interestingOrder: InterestingOrder,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.withInterestingOrder(interestingOrder)
      .updateQueryProjection(_.updatePagination(_.withLimitExpression(limit))))
    val plan = Top(inner, sortColumns, limit)
    val top = annotate(
      plan,
      solved,
      context.providedOrderFactory.providedOrder(orderColumns, ProvidedOrder.Self, Some(plan)),
      cachedPropertiesPerPlan.get(inner.id),
      context
    )
    if (interestingOrder.requiredOrderCandidate.nonEmpty) {
      markOrderAsLeveragedBackwardsUntilOrigin(top, context.providedOrderFactory)
    }
    top
  }

  def planTop1WithTies(
    inner: LogicalPlan,
    sortColumns: Seq[ColumnOrder],
    orderColumns: Seq[ordering.ColumnOrder],
    interestingOrder: InterestingOrder,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.withInterestingOrder(interestingOrder)
      .updateQueryProjection(
        _.updatePagination(_.withLimitExpression(SignedDecimalIntegerLiteral("1")(InputPosition.NONE)))
      ))
    val plan = Top1WithTies(inner, sortColumns)
    val top = annotate(
      plan,
      solved,
      context.providedOrderFactory.providedOrder(orderColumns, ProvidedOrder.Self, Some(plan)),
      cachedPropertiesPerPlan.get(inner.id),
      context
    )
    if (interestingOrder.requiredOrderCandidate.nonEmpty) {
      markOrderAsLeveragedBackwardsUntilOrigin(top, context.providedOrderFactory)
    }
    top
  }

  def planPartialSort(
    inner: LogicalPlan,
    alreadySortedPrefix: Seq[ColumnOrder],
    stillToSortSuffix: Seq[ColumnOrder],
    orderColumns: Seq[ordering.ColumnOrder],
    interestingOrder: InterestingOrder,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.withInterestingOrder(interestingOrder))
    val sort = PartialSort(inner, alreadySortedPrefix, stillToSortSuffix, None)
    val plan = annotate(
      sort,
      solved,
      context.providedOrderFactory.providedOrder(orderColumns, ProvidedOrder.Left, Some(sort)),
      cachedPropertiesPerPlan.get(inner.id),
      context
    )
    markOrderAsLeveragedBackwardsUntilOrigin(plan, context.providedOrderFactory)
    plan
  }

  def planShortestRelationship(
    inner: LogicalPlan,
    shortestRelationship: ShortestRelationshipPattern,
    nodePredicates: Set[VariablePredicate],
    relPredicates: Set[VariablePredicate],
    pathPredicates: Set[Expression],
    solvedPredicates: Set[Expression],
    withFallBack: Boolean,
    disallowSameNode: Boolean = true,
    context: LogicalPlanningContext
  ): LogicalPlan = {

    val (predicateWithIrExpressionReferencingPath, otherPathPredicates) = {
      val variables = shortestRelationship.pathAndRelationshipVariables
      pathPredicates.partition(_.folder.treeExists {
        case ire: IRExpression => ire.dependencies.intersect(variables).nonEmpty
      })
    }

    val rewrittenPredicatesWithIrExpressionReferencingPath =
      predicateWithIrExpressionReferencingPath.endoRewrite(irExpressionRewriter(inner, context))

    val solved = solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(
      _.addShortestRelationship(shortestRelationship).addPredicates(solvedPredicates)
    )

    val (rewrittenRelationshipPredicates, rewrittenNodePredicates, rewrittenOtherPathPredicates, rewrittenSource) =
      solveSubqueryExpressionsForExtractedPredicates(
        inner,
        nodePredicates,
        relPredicates,
        otherPathPredicates,
        context
      )
    val rewrittenPathPredicates = rewrittenOtherPathPredicates ++ rewrittenPredicatesWithIrExpressionReferencingPath

    annotate(
      FindShortestPaths(
        rewrittenSource,
        shortestRelationship,
        rewrittenNodePredicates.toSeq,
        rewrittenRelationshipPredicates.toSeq,
        rewrittenPathPredicates.toSeq,
        withFallBack,
        if (disallowSameNode) DisallowSameNode else SkipSameNode,
        Trail
      ),
      solved,
      ProvidedOrder.Left,
      cachedPropertiesPerPlan.get(inner.id),
      context
    )
  }

  def planStatefulShortest(
    inner: LogicalPlan,
    startNode: LogicalVariable,
    endNode: LogicalVariable,
    nfa: NFA,
    mode: ExpansionMode,
    nonInlinedPreFilters: Option[Expression],
    nodeVariableGroupings: Set[VariableGrouping],
    relationshipVariableGroupings: Set[VariableGrouping],
    singletonNodeVariables: Set[Mapping],
    singletonRelationshipVariables: Set[Mapping],
    selector: StatefulShortestPath.Selector,
    solvedExpressionAsString: String,
    solvedSpp: SelectivePathPattern,
    solvedPredicates: Seq[Expression],
    reverseGroupVariableProjections: Boolean,
    hints: Set[UsingStatefulShortestPathHint],
    context: LogicalPlanningContext,
    pathLength: PathLength,
    pathMode: TraversalPathMode
  ): StatefulShortestPath = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(
      _.addSelectivePathPattern(solvedSpp)
        .addPredicates(solvedPredicates: _*)
        .addHints(hints)
    )
    val (rewrittenNFA, rewrittenNonInlinablePreFilters) = {
      // We do not use the SubqueryExpressionSolver, since all expressions for StatefulShortest
      // must be planned with NestedPlanExpressions.
      val rewriter = irExpressionRewriter(inner, context)
      val rewrittenNFA = nfa.endoRewrite(rewriter)
      val rewrittenNonInlinablePreFilters = nonInlinedPreFilters.endoRewrite(rewriter)
      (rewrittenNFA, rewrittenNonInlinablePreFilters)
    }

    val plan = StatefulShortestPath(
      inner,
      startNode,
      endNode,
      rewrittenNFA,
      mode,
      rewrittenNonInlinablePreFilters,
      nodeVariableGroupings,
      relationshipVariableGroupings,
      singletonNodeVariables,
      singletonRelationshipVariables,
      selector,
      solvedExpressionAsString,
      reverseGroupVariableProjections,
      LengthBounds(pathLength.min, pathLength.maybeMax),
      pathMode
    )
    annotate(plan, solved, ProvidedOrder.Left, cachedPropertiesPerPlan.get(inner.id), context)
  }

  def planProjectEndpoints(
    inner: LogicalPlan,
    start: LogicalVariable,
    startInScope: Boolean,
    end: LogicalVariable,
    endInScope: Boolean,
    patternRel: PatternRelationship,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(_.addPatternRelationship(patternRel))
    annotate(
      ProjectEndpoints(
        inner,
        patternRel.variable,
        start,
        startInScope,
        end,
        endInScope,
        patternRel.types,
        patternRel.dir,
        patternRel.length
      ),
      solved,
      ProvidedOrder.Left,
      cachedPropertiesPerPlan.get(inner.id),
      context
    )
  }

  def planProjectionForUnionMapping(
    inner: LogicalPlan,
    unionMapping: Map[LogicalVariable, Expression],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val previouslyCachedProperties = cachedPropertiesPerPlan.get(inner.id)
    val cachedPropertiesForUnionMapping = previouslyCachedProperties.rename(renamedVariables(unionMapping))
    annotate(
      Projection(inner, unionMapping),
      solveds.get(inner.id),
      ProvidedOrder.Left,
      cachedPropertiesForUnionMapping,
      context
    )
  }

  def planUnion(
    left: LogicalPlan,
    right: LogicalPlan,
    unionMappings: List[UnionMapping],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solvedLeft = solveds.get(left.id)
    val solvedRight = solveds.get(right.id).asSinglePlannerQuery
    val solved = UnionQuery(solvedLeft, solvedRight, distinct = false, unionMappings)

    val plan = Union(left, right)
    annotate(
      plan,
      solved,
      ProvidedOrder.empty,
      cachedPropertiesPerPlan.get(left.id).intersect(cachedPropertiesPerPlan.get(right.id)),
      context
    )
  }

  def planOrderedUnion(
    left: LogicalPlan,
    right: LogicalPlan,
    unionMappings: List[UnionMapping],
    sortedColumns: Seq[ColumnOrder],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solvedLeft = solveds.get(left.id)
    val solvedRight = solveds.get(right.id).asSinglePlannerQuery
    val solved = UnionQuery(solvedLeft, solvedRight, distinct = false, unionMappings)

    val union = OrderedUnion(left, right, sortedColumns)
    val providedOrder = providedOrders.get(left.id)
      .commonPrefixWith(providedOrders.get(right.id))(context.providedOrderFactory, Some(union))
      .fromBoth(context.providedOrderFactory, Some(union))

    val plan = annotate(
      union,
      solved,
      providedOrder,
      cachedPropertiesPerPlan.get(left.id).intersect(cachedPropertiesPerPlan.get(right.id)),
      context
    )
    markOrderAsLeveragedBackwardsUntilOrigin(plan, context.providedOrderFactory)
    plan
  }

  def planDistinctForUnion(left: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val returnAll = left.localAvailableSymbols.map { s => s -> s }

    val solved = solveds.get(left.id) match {
      case u: UnionQuery => markDistinctInUnion(u)
      case _ => throw new IllegalStateException("Planning a distinct for union, but no union was planned before.")
    }
    if (returnAll.isEmpty) {
      annotate(
        left.copyPlanWithIdGen(idGen),
        solved,
        ProvidedOrder.Left,
        cachedPropertiesPerPlan.get(left.id),
        context
      )
    } else {
      annotate(
        Distinct(left, returnAll.toMap),
        solved,
        ProvidedOrder.Left,
        cachedPropertiesPerPlan.get(left.id),
        context
      )
    }
  }

  def planOrderedDistinctForUnion(
    left: LogicalPlan,
    orderToLeverage: Seq[Expression],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val returnAll = left.localAvailableSymbols.map { s => s -> s }

    val solved = solveds.get(left.id) match {
      case u: UnionQuery => markDistinctInUnion(u)
      case _ => throw new IllegalStateException("Planning a distinct for or union, but no union was planned before.")
    }
    if (returnAll.isEmpty) {
      annotate(left.copyPlanWithIdGen(idGen), solved, ProvidedOrder.Left, cachedPropertiesPerPlan.get(left.id), context)
    } else {
      val RemoteBatchingResult(rewrittenExpressions, rewrittenPlan) =
        context.settings.remoteBatchPropertiesStrategy.planRemoteBatchProperties(left, context, orderToLeverage)

      val plan = annotate(
        OrderedDistinct(
          rewrittenPlan,
          returnAll.toMap,
          rewrittenExpressions.allRewrittenExpressions.toSeq
        ),
        solveds.get(rewrittenPlan.id),
        ProvidedOrder.Left,
        cachedPropertiesPerPlan.get(rewrittenPlan.id),
        context
      )
      markOrderAsLeveragedBackwardsUntilOrigin(plan, context.providedOrderFactory)
      plan

    }
  }

  private def markDistinctInUnion(query: PlannerQuery): PlannerQuery = {
    query match {
      case u @ UnionQuery(lhs, _, _, _) => u.copy(lhs = markDistinctInUnion(lhs), distinct = true)
      case s                            => s
    }
  }

  /**
   * @param expressions must be solved by the ListSubqueryExpressionSolver. This is not done here since that can influence how we plan distinct,
   *                    thus this logic is put into [[distinct]] instead.
   */
  def planDistinct(
    left: LogicalPlan,
    expressions: Map[LogicalVariable, Expression],
    reported: Map[LogicalVariable, Expression],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved: SinglePlannerQuery =
      solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.updateQueryProjection(_ =>
        DistinctQueryProjection(reported, importedExposedSymbols = context.plannerState.importedSubqueryVariables)
      ))
    val columnsWithRenames = renameProvidedOrderColumns(providedOrders.get(left.id).columns, expressions)
    val plan = Distinct(left, expressions)
    val providedOrder = context.providedOrderFactory.providedOrder(columnsWithRenames, ProvidedOrder.Left, Some(plan))
    annotate(
      plan,
      solved,
      providedOrder,
      cachedPropertiesPerPlan.get(left.id).retain(accessedPropertiesInGroupingKeys(expressions)),
      context
    )
  }

  /**
   * Keep the left plan, but mark DISTINCT as solved.
   * Used when DISTINCT is used, but we can determine it is not really necessary.
   */
  def planEmptyDistinct(
    left: LogicalPlan,
    reported: Map[LogicalVariable, Expression],
    context: LogicalPlanningContext
  ): LogicalPlan = {

    val solved: SinglePlannerQuery =
      solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.updateQueryProjection(_ =>
        DistinctQueryProjection(reported, importedExposedSymbols = context.plannerState.importedSubqueryVariables)
      ))

    // Change solved
    val keptAttributes =
      Attributes(idGen, providedOrders, leveragedOrders, labelAndRelTypeInfos, cachedPropertiesPerPlan)
    val newPlan = left.copyPlanWithIdGen(keptAttributes.copy(left.id))
    solveds.set(newPlan.id, solved)
    cardinalities.set(newPlan.id, cardinalities.get(left.id))
    newPlan
  }

  /**
   * Plan a Projection, but mark DISTINCT as solved.
   * Used when DISTINCT is used but we can determine it is not really needed.
   *
   * @param expressions must be solved by the ListSubqueryExpressionSolver. This is not done here since that can influence how we plan distinct,
   *                    thus this logic is put into [[distinct]] instead.
   */
  def planProjectionForDistinct(
    left: LogicalPlan,
    expressions: Map[LogicalVariable, Expression],
    reported: Map[LogicalVariable, Expression],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved: SinglePlannerQuery =
      solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.updateQueryProjection(_ =>
        DistinctQueryProjection(reported, importedExposedSymbols = context.plannerState.importedSubqueryVariables)
      ))

    planRegularProjectionHelper(
      left,
      expressions,
      context,
      solved,
      cachedPropertiesPerPlan.get(left.id).retain(accessedPropertiesInGroupingKeys(expressions))
    )
  }

  private def accessedPropertiesInGroupingKeys(expressions: Map[LogicalVariable, Expression])
    : Map[LogicalVariable, Set[PropertyKeyName]] =
    PropertyAccessHelper.findPropertyAccesses(expressions.values).groupMap(_.variable)(propertyAccess =>
      PropertyKeyName(propertyAccess.propertyName)(InputPosition.NONE)
    )

  /**
   *
   * @param expressions must be solved by the ListSubqueryExpressionSolver. This is not done here since that can influence how we plan distinct,
   *                    thus this logic is put into [[distinct]] instead.
   */
  def planOrderedDistinct(
    left: LogicalPlan,
    expressions: Map[LogicalVariable, Expression],
    orderToLeverage: Seq[Expression],
    reported: Map[LogicalVariable, Expression],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved: SinglePlannerQuery =
      solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.updateQueryProjection(_ =>
        DistinctQueryProjection(reported, importedExposedSymbols = context.plannerState.importedSubqueryVariables)
      ))
    val columnsWithRenames = renameProvidedOrderColumns(providedOrders.get(left.id).columns, expressions)
    val distinct = OrderedDistinct(left, expressions, orderToLeverage)
    val providedOrder =
      context.providedOrderFactory.providedOrder(columnsWithRenames, ProvidedOrder.Left, Some(distinct))
    val plan = annotate(
      distinct,
      solved,
      providedOrder,
      cachedPropertiesPerPlan.get(left.id).retain(accessedPropertiesInGroupingKeys(expressions)),
      context
    )
    markOrderAsLeveragedBackwardsUntilOrigin(plan, context.providedOrderFactory)
    plan
  }

  def updateSolvedForOr(
    orPlan: LogicalPlan,
    solvedQueryGraph: QueryGraph,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(orPlan.id) match {
      case UnionQuery(lhs, rhs, _, _) => rhs.updateTailOrSelf { that =>
          val newHints = lhs.allHints ++ rhs.allHints
          that.withQueryGraph(solvedQueryGraph.withHints(newHints))
        }
      case q => q.asSinglePlannerQuery.withQueryGraph(solvedQueryGraph.withHints(q.allHints))
    }
    val cardinality = cardinalityModel(
      solved,
      context.plannerState.input.labelInfo,
      context.plannerState.input.relTypeInfo,
      context.semanticTable,
      context.plannerState.indexCompatiblePredicatesProviderContext,
      context.staticComponents.graphSchemaOptimizations
    )
    // Change solved and cardinality
    val keptAttributes =
      Attributes(idGen, providedOrders, leveragedOrders, labelAndRelTypeInfos, cachedPropertiesPerPlan)
    val newPlan = orPlan.copyPlanWithIdGen(keptAttributes.copy(orPlan.id))
    solveds.set(newPlan.id, solved)
    cardinalities.set(newPlan.id, cardinality)
    newPlan
  }

  def planTriadicSelection(
    positivePredicate: Boolean,
    left: LogicalPlan,
    sourceId: LogicalVariable,
    seenId: LogicalVariable,
    targetId: LogicalVariable,
    right: LogicalPlan,
    predicate: Expression,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = {
      val leftSolved = solveds.get(left.id).asSinglePlannerQuery
      val rightSolved = solveds.get(right.id).asSinglePlannerQuery.amendQueryGraph(_.removeArguments())
      (leftSolved ++ rightSolved).updateTailOrSelf(_.amendQueryGraph(_.addPredicates(predicate)))
    }
    annotate(
      TriadicSelection(left, right, positivePredicate, sourceId, seenId, targetId),
      solved,
      ProvidedOrder.Left,
      cachedPropertiesPerPlan.get(right.id),
      context
    )
  }

  private def cachedPropertiesAfterMutatingPattern(
    mutatingPattern: MutatingPattern,
    planWithMutatingPattern: LogicalPlan
  ): CachedProperties = {
    if (mutatingPattern.invalidatesCachedProperties)
      CachedProperties.empty
    else
      cachedPropertiesPerPlan.get(planWithMutatingPattern.id)
  }

  def planCreate(inner: LogicalPlan, pattern: CreatePattern, context: LogicalPlanningContext): LogicalPlan = {
    // Plan remoteBatchProperties when property references are used in the CREATE
    val (innerRewrittenRBPs, patternRewrittenCachedProps) =
      context.settings.remoteBatchPropertiesStrategy
        .planRemoteBatchPropertiesForMutatingPattern(inner, context, pattern)

    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(
        _.amendQueryGraph(_.addMutatingPatterns(pattern))
          .resetQueryProjection()
      )
    val (rewrittenPattern: CreatePattern, rewrittenInner) =
      SubqueryExpressionSolver.ForMappable().solve(innerRewrittenRBPs, patternRewrittenCachedProps, context)
    val plan = plans.Create(rewrittenInner, rewrittenPattern.commands)
    val providedOrder =
      providedOrderOfUpdate(plan, rewrittenInner, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, cachedPropertiesAfterMutatingPattern(pattern, innerRewrittenRBPs), context)
  }

  def planMerge(
    inner: LogicalPlan,
    createNodePatterns: Seq[CreateNode],
    createRelationshipPatterns: Seq[CreateRelationship],
    onMatchPatterns: Seq[SetMutatingPattern],
    onCreatePatterns: Seq[SetMutatingPattern],
    nodesToLock: Set[LogicalVariable],
    context: LogicalPlanningContext
  ): Merge = {
    // MERGE has row-by-row visibility.
    // To maintain the visibility, even with subqueries, we must use NestedPlanExpressions.
    // This only applies to the "write part" of the MERGE.
    // The read, which is the `inner` plan is free to use RollUpApply, etc.
    val rewriter = irExpressionRewriter(inner, context)

    val (mergePattern, innerRewrittenRBPs, patternRewrittenCachedProps) =
      if (createRelationshipPatterns.isEmpty) {
        val mergeNodePattern = MergeNodePattern(
          createNodePatterns.head,
          solveds(inner.id).asSinglePlannerQuery.queryGraph,
          onCreatePatterns,
          onMatchPatterns
        )
        // Plan remoteBatchProperties when property references are used in the MERGE
        val (innerRewrittenRBPs, patternRewrittenCachedProps) =
          context.settings.remoteBatchPropertiesStrategy
            .planRemoteBatchPropertiesForMutatingPattern(inner, context, mergeNodePattern)

        (mergeNodePattern, innerRewrittenRBPs, patternRewrittenCachedProps)

      } else {
        val mergeRelPattern = MergeRelationshipPattern(
          createNodePatterns,
          createRelationshipPatterns,
          solveds(inner.id).asSinglePlannerQuery.queryGraph,
          onCreatePatterns,
          onMatchPatterns
        )
        // Plan remoteBatchProperties when property references are used in the MERGE
        val (innerRewrittenRBPs, patternRewrittenCachedProps) =
          context.settings.remoteBatchPropertiesStrategy
            .planRemoteBatchPropertiesForMutatingPattern(inner, context, mergeRelPattern)

        (mergeRelPattern, innerRewrittenRBPs, patternRewrittenCachedProps)
      }

    val rewrittenNodePatterns = patternRewrittenCachedProps.createNodePatterns.endoRewrite(rewriter)
    val rewrittenRelPatterns = patternRewrittenCachedProps.createRelationshipPatterns.endoRewrite(rewriter)

    val solved =
      RegularSinglePlannerQuery()
        .amendQueryGraph(_.addMutatingPatterns(mergePattern))
        .resetQueryProjection()
    val merge =
      Merge(
        innerRewrittenRBPs,
        rewrittenNodePatterns,
        rewrittenRelPatterns,
        patternRewrittenCachedProps.onMatchPatterns,
        patternRewrittenCachedProps.onCreatePatterns,
        nodesToLock
      )
    val providedOrder =
      providedOrderOfUpdate(merge, innerRewrittenRBPs, context.settings.executionModel, context.providedOrderFactory)
    annotate(
      merge,
      solved,
      providedOrder,
      cachedPropertiesAfterMutatingPattern(mergePattern, innerRewrittenRBPs),
      context
    )
  }

  def planConditionalApply(
    lhs: LogicalPlan,
    rhs: LogicalPlan,
    idNames: Seq[LogicalVariable],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(lhs.id).asSinglePlannerQuery ++ solveds.get(rhs.id).asSinglePlannerQuery
    val plan = ConditionalApply(lhs, rhs, idNames)
    val providedOrder =
      providedOrderOfApply(lhs, rhs, plan, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, cachedPropertiesPerPlan.get(rhs.id), context)
  }

  def planAntiConditionalApply(
    lhs: LogicalPlan,
    rhs: LogicalPlan,
    idNames: Seq[LogicalVariable],
    context: LogicalPlanningContext,
    maybeSolved: Option[SinglePlannerQuery] = None
  ): LogicalPlan = {
    val solved =
      maybeSolved.getOrElse(solveds.get(lhs.id).asSinglePlannerQuery ++ solveds.get(rhs.id).asSinglePlannerQuery)
    val plan = AntiConditionalApply(lhs, rhs, idNames)
    val providedOrder =
      providedOrderOfApply(lhs, rhs, plan, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, cachedPropertiesPerPlan.get(rhs.id), context)
  }

  def planDeleteNode(inner: LogicalPlan, delete: DeleteExpression, context: LogicalPlanningContext): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(
        _.amendQueryGraph(_.addMutatingPatterns(delete))
          .resetQueryProjection()
      )
    val (rewrittenDelete, rewrittenInner) = SubqueryExpressionSolver.ForMappable().solve(inner, delete, context)
    val plan =
      if (delete.detachDelete) {
        DetachDeleteNode(rewrittenInner, rewrittenDelete.expression)
      } else {
        DeleteNode(rewrittenInner, rewrittenDelete.expression)
      }
    val providedOrder =
      providedOrderOfUpdate(plan, inner, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, cachedPropertiesAfterMutatingPattern(delete, inner), context)
  }

  def planDeleteRelationship(
    inner: LogicalPlan,
    delete: DeleteExpression,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(
        _.amendQueryGraph(_.addMutatingPatterns(delete))
          .resetQueryProjection()
      )
    val (rewrittenDelete, rewrittenInner) = SubqueryExpressionSolver.ForMappable().solve(inner, delete, context)
    val plan = DeleteRelationship(rewrittenInner, rewrittenDelete.expression)
    val providedOrder =
      providedOrderOfUpdate(plan, inner, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, cachedPropertiesAfterMutatingPattern(delete, inner), context)
  }

  def planDeletePath(inner: LogicalPlan, delete: DeleteExpression, context: LogicalPlanningContext): LogicalPlan = {
    // `delete.expression` can only be a PathExpression, ListSubqueryExpressionSolver not needed
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(
        _.amendQueryGraph(_.addMutatingPatterns(delete))
          .resetQueryProjection()
      )

    val plan =
      if (delete.detachDelete) {
        DetachDeletePath(inner, delete.expression)
      } else {
        DeletePath(inner, delete.expression)
      }
    val providedOrder =
      providedOrderOfUpdate(plan, inner, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, cachedPropertiesAfterMutatingPattern(delete, inner), context)
  }

  def planDeleteExpression(
    inner: LogicalPlan,
    delete: DeleteExpression,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(
        _.amendQueryGraph(_.addMutatingPatterns(delete))
          .resetQueryProjection()
      )
    val (rewrittenDelete, rewrittenInner) = SubqueryExpressionSolver.ForMappable().solve(inner, delete, context)
    val plan =
      if (delete.detachDelete) {
        DetachDeleteExpression(rewrittenInner, rewrittenDelete.expression)
      } else {
        plans.DeleteExpression(rewrittenInner, rewrittenDelete.expression)
      }
    val providedOrder =
      providedOrderOfUpdate(plan, inner, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, cachedPropertiesAfterMutatingPattern(delete, inner), context)
  }

  def planSetLabel(inner: LogicalPlan, pattern: SetLabelPattern, context: LogicalPlanningContext): LogicalPlan = {
    // Plan remoteBatchProperties when property references are used to set the node label
    val (innerRewrittenRBPs, patternRewrittenCachedProps) =
      context.settings.remoteBatchPropertiesStrategy
        .planRemoteBatchPropertiesForMutatingPattern(inner, context, pattern)

    val solved =
      solveds.get(innerRewrittenRBPs.id)
        .asSinglePlannerQuery.updateTailOrSelf(
          _.amendQueryGraph(_.addMutatingPatterns(pattern))
            .resetQueryProjection()
        )
    val rewrittenDynamicLabels =
      patternRewrittenCachedProps.dynamicLabels.toSet.endoRewrite(irExpressionRewriter(innerRewrittenRBPs, context))
    val plan = SetLabels(
      innerRewrittenRBPs,
      patternRewrittenCachedProps.variable,
      patternRewrittenCachedProps.labels.toSet,
      rewrittenDynamicLabels
    )
    val providedOrder =
      providedOrderOfUpdate(plan, innerRewrittenRBPs, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, cachedPropertiesAfterMutatingPattern(pattern, innerRewrittenRBPs), context)
  }

  def planSetNodeProperty(
    inner: LogicalPlan,
    pattern: SetNodePropertyPattern,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    // Plan remoteBatchProperties when property references are used to set the node property
    val (innerRewrittenRBPs, patternRewrittenCachedProps) =
      context.settings.remoteBatchPropertiesStrategy
        .planRemoteBatchPropertiesForMutatingPattern(inner, context, pattern)

    val solved =
      solveds.get(innerRewrittenRBPs.id)
        .asSinglePlannerQuery.updateTailOrSelf(
          _.amendQueryGraph(_.addMutatingPatterns(pattern))
            .resetQueryProjection()
        )

    // SET has currently row-by-row visibility. This could change in a major release.
    // To maintain the visibility, even with subqueries, we must use NestedPlanExpressions.
    val rewriter = irExpressionRewriter(innerRewrittenRBPs, context)
    val rewrittenPattern = patternRewrittenCachedProps.endoRewrite(rewriter)

    val plan = SetNodeProperty(
      innerRewrittenRBPs,
      rewrittenPattern.variable,
      rewrittenPattern.propertyKey,
      rewrittenPattern.expression
    )
    val providedOrder =
      providedOrderOfUpdate(plan, innerRewrittenRBPs, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, cachedPropertiesAfterMutatingPattern(pattern, innerRewrittenRBPs), context)
  }

  def planSetNodeProperties(
    inner: LogicalPlan,
    pattern: SetNodePropertiesPattern,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    // Plan remoteBatchProperties when property references are used to set the node properties
    val (innerRewrittenRBPs, patternRewrittenCachedProps) =
      context.settings.remoteBatchPropertiesStrategy
        .planRemoteBatchPropertiesForMutatingPattern(inner, context, pattern)

    val solved =
      solveds.get(innerRewrittenRBPs.id)
        .asSinglePlannerQuery.updateTailOrSelf(
          _.amendQueryGraph(_.addMutatingPatterns(pattern))
            .resetQueryProjection()
        )

    // SET has currently row-by-row visibility. This could change in a major release.
    // To maintain the visibility, even with subqueries, we must use NestedPlanExpressions.
    val rewriter = irExpressionRewriter(innerRewrittenRBPs, context)
    val rewrittenPattern = patternRewrittenCachedProps.endoRewrite(rewriter)

    val plan = SetNodeProperties(innerRewrittenRBPs, rewrittenPattern.variable, rewrittenPattern.items)
    val providedOrder =
      providedOrderOfUpdate(plan, innerRewrittenRBPs, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, cachedPropertiesAfterMutatingPattern(pattern, innerRewrittenRBPs), context)
  }

  def planSetNodePropertiesFromMap(
    inner: LogicalPlan,
    pattern: SetNodePropertiesFromMapPattern,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    // Plan remoteBatchProperties when property references are used to set the node properties
    val (innerRewrittenRBPs, patternRewrittenCachedProps) =
      context.settings.remoteBatchPropertiesStrategy
        .planRemoteBatchPropertiesForMutatingPattern(inner, context, pattern)

    val solved =
      solveds.get(innerRewrittenRBPs.id)
        .asSinglePlannerQuery.updateTailOrSelf(
          _.amendQueryGraph(_.addMutatingPatterns(pattern))
            .resetQueryProjection()
        )

    // SET has currently row-by-row visibility. This could change in a major release.
    // To maintain the visibility, even with subqueries, we must use NestedPlanExpressions.
    val rewriter = irExpressionRewriter(innerRewrittenRBPs, context)
    val rewrittenPattern = patternRewrittenCachedProps.endoRewrite(rewriter)

    val plan = SetNodePropertiesFromMap(
      innerRewrittenRBPs,
      rewrittenPattern.variable,
      rewrittenPattern.expression,
      rewrittenPattern.removeOtherProps
    )
    val providedOrder =
      providedOrderOfUpdate(plan, innerRewrittenRBPs, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, cachedPropertiesAfterMutatingPattern(pattern, innerRewrittenRBPs), context)
  }

  def planSetRelationshipProperty(
    inner: LogicalPlan,
    pattern: SetRelationshipPropertyPattern,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    // Plan remoteBatchProperties when property references are used to set the relationship property
    val (innerRewrittenRBPs, patternRewrittenCachedProps) =
      context.settings.remoteBatchPropertiesStrategy
        .planRemoteBatchPropertiesForMutatingPattern(inner, context, pattern)

    val solved =
      solveds.get(innerRewrittenRBPs.id)
        .asSinglePlannerQuery.updateTailOrSelf(
          _.amendQueryGraph(_.addMutatingPatterns(pattern))
            .resetQueryProjection()
        )

    // SET has currently row-by-row visibility. This could change in a major release.
    // To maintain the visibility, even with subqueries, we must use NestedPlanExpressions.
    val rewriter = irExpressionRewriter(innerRewrittenRBPs, context)
    val rewrittenPattern = patternRewrittenCachedProps.endoRewrite(rewriter)

    val plan = SetRelationshipProperty(
      innerRewrittenRBPs,
      rewrittenPattern.variable,
      rewrittenPattern.propertyKey,
      rewrittenPattern.expression
    )
    val providedOrder =
      providedOrderOfUpdate(plan, innerRewrittenRBPs, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, cachedPropertiesAfterMutatingPattern(pattern, innerRewrittenRBPs), context)
  }

  def planSetRelationshipProperties(
    inner: LogicalPlan,
    pattern: SetRelationshipPropertiesPattern,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    // Plan remoteBatchProperties when property references are used to set the relationship properties
    val (innerRewrittenRBPs, patternRewrittenCachedProps) =
      context.settings.remoteBatchPropertiesStrategy
        .planRemoteBatchPropertiesForMutatingPattern(inner, context, pattern)

    val solved =
      solveds.get(innerRewrittenRBPs.id)
        .asSinglePlannerQuery.updateTailOrSelf(
          _.amendQueryGraph(_.addMutatingPatterns(pattern))
            .resetQueryProjection()
        )

    // SET has currently row-by-row visibility. This could change in a major release.
    // To maintain the visibility, even with subqueries, we must use NestedPlanExpressions.
    val rewriter = irExpressionRewriter(innerRewrittenRBPs, context)
    val rewrittenPattern = patternRewrittenCachedProps.endoRewrite(rewriter)

    val plan = SetRelationshipProperties(innerRewrittenRBPs, rewrittenPattern.variable, rewrittenPattern.items)
    val providedOrder =
      providedOrderOfUpdate(plan, innerRewrittenRBPs, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, cachedPropertiesAfterMutatingPattern(pattern, innerRewrittenRBPs), context)
  }

  def planSetRelationshipPropertiesFromMap(
    inner: LogicalPlan,
    pattern: SetRelationshipPropertiesFromMapPattern,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    // Plan remoteBatchProperties when property references are used to set the relationship properties
    val (innerRewrittenRBPs, patternRewrittenCachedProps) =
      context.settings.remoteBatchPropertiesStrategy
        .planRemoteBatchPropertiesForMutatingPattern(inner, context, pattern)

    val solved =
      solveds.get(innerRewrittenRBPs.id)
        .asSinglePlannerQuery.updateTailOrSelf(
          _.amendQueryGraph(_.addMutatingPatterns(pattern))
            .resetQueryProjection()
        )

    // SET has currently row-by-row visibility. This could change in a major release.
    // To maintain the visibility, even with subqueries, we must use NestedPlanExpressions.
    val rewriter = irExpressionRewriter(innerRewrittenRBPs, context)
    val rewrittenPattern = patternRewrittenCachedProps.endoRewrite(rewriter)

    val plan = SetRelationshipPropertiesFromMap(
      innerRewrittenRBPs,
      rewrittenPattern.variable,
      rewrittenPattern.expression,
      rewrittenPattern.removeOtherProps
    )
    val providedOrder =
      providedOrderOfUpdate(plan, innerRewrittenRBPs, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, cachedPropertiesAfterMutatingPattern(pattern, innerRewrittenRBPs), context)
  }

  def planSetPropertiesFromMap(
    inner: LogicalPlan,
    pattern: SetPropertiesFromMapPattern,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    // Plan remoteBatchProperties when property references are used to set the properties
    val (innerRewrittenRBPs, patternRewrittenCachedProps) =
      context.settings.remoteBatchPropertiesStrategy
        .planRemoteBatchPropertiesForMutatingPattern(inner, context, pattern)

    val solved =
      solveds.get(innerRewrittenRBPs.id).asSinglePlannerQuery
        .updateTailOrSelf(
          _.amendQueryGraph(_.addMutatingPatterns(pattern))
            .resetQueryProjection()
        )

    // SET has currently row-by-row visibility. This could change in a major release.
    // To maintain the visibility, even with subqueries, we must use NestedPlanExpressions.
    val rewriter = irExpressionRewriter(innerRewrittenRBPs, context)
    val rewrittenPattern = patternRewrittenCachedProps.endoRewrite(rewriter)

    val plan = SetPropertiesFromMap(
      innerRewrittenRBPs,
      rewrittenPattern.entityExpression,
      rewrittenPattern.expression,
      rewrittenPattern.removeOtherProps
    )
    val providedOrder =
      providedOrderOfUpdate(plan, innerRewrittenRBPs, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, cachedPropertiesAfterMutatingPattern(pattern, innerRewrittenRBPs), context)
  }

  def planSetProperty(inner: LogicalPlan, pattern: SetPropertyPattern, context: LogicalPlanningContext): LogicalPlan = {
    // Plan remoteBatchProperties when property references are used to set the property
    val (innerRewrittenRBPs, patternRewrittenCachedProps) =
      context.settings.remoteBatchPropertiesStrategy
        .planRemoteBatchPropertiesForMutatingPattern(inner, context, pattern)

    val solved =
      solveds.get(innerRewrittenRBPs.id).asSinglePlannerQuery
        .updateTailOrSelf(
          _.amendQueryGraph(_.addMutatingPatterns(pattern))
            .resetQueryProjection()
        )

    // SET has currently row-by-row visibility. This could change in a major release.
    // To maintain the visibility, even with subqueries, we must use NestedPlanExpressions.
    val rewriter = irExpressionRewriter(innerRewrittenRBPs, context)
    val rewrittenPattern = patternRewrittenCachedProps.endoRewrite(rewriter)

    val plan = SetProperty(
      innerRewrittenRBPs,
      rewrittenPattern.entityExpression,
      rewrittenPattern.propertyKeyName,
      rewrittenPattern.expression
    )
    val providedOrder =
      providedOrderOfUpdate(plan, innerRewrittenRBPs, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, cachedPropertiesAfterMutatingPattern(pattern, innerRewrittenRBPs), context)
  }

  def planSetProperties(
    inner: LogicalPlan,
    pattern: SetPropertiesPattern,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    // Plan remoteBatchProperties when property references are used to set the properties
    val (innerRewrittenRBPs, patternRewrittenCachedProps) =
      context.settings.remoteBatchPropertiesStrategy
        .planRemoteBatchPropertiesForMutatingPattern(inner, context, pattern)

    val solved =
      solveds.get(innerRewrittenRBPs.id).asSinglePlannerQuery
        .updateTailOrSelf(
          _.amendQueryGraph(_.addMutatingPatterns(pattern))
            .resetQueryProjection()
        )

    // SET has currently row-by-row visibility. This could change in a major release.
    // To maintain the visibility, even with subqueries, we must use NestedPlanExpressions.
    val rewriter = irExpressionRewriter(innerRewrittenRBPs, context)
    val rewrittenPattern = patternRewrittenCachedProps.endoRewrite(rewriter)

    val plan = SetProperties(innerRewrittenRBPs, rewrittenPattern.entityExpression, rewrittenPattern.items)
    val providedOrder =
      providedOrderOfUpdate(plan, innerRewrittenRBPs, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, cachedPropertiesAfterMutatingPattern(pattern, innerRewrittenRBPs), context)
  }

  def planSetDynamicProperty(
    inner: LogicalPlan,
    pattern: SetDynamicPropertyPattern,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    // Plan remoteBatchProperties when property references are used to set the label
    val (innerRewrittenRBPs, patternRewrittenCachedProps) =
      context.settings.remoteBatchPropertiesStrategy
        .planRemoteBatchPropertiesForMutatingPattern(inner, context, pattern)

    val solved =
      solveds.get(innerRewrittenRBPs.id)
        .asSinglePlannerQuery.updateTailOrSelf(
          _.amendQueryGraph(_.addMutatingPatterns(pattern))
            .resetQueryProjection()
        )

    // SET has currently row-by-row visibility. This could change in a major release.
    // To maintain the visibility, even with subqueries, we must use NestedPlanExpressions.
    val rewriter = irExpressionRewriter(innerRewrittenRBPs, context)
    val rewrittenPattern = patternRewrittenCachedProps.endoRewrite(rewriter)

    val plan = SetDynamicProperty(
      innerRewrittenRBPs,
      rewrittenPattern.entity,
      rewrittenPattern.property,
      rewrittenPattern.expression
    )
    val providedOrder =
      providedOrderOfUpdate(plan, innerRewrittenRBPs, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, cachedPropertiesAfterMutatingPattern(pattern, innerRewrittenRBPs), context)
  }

  def planRemoveLabel(inner: LogicalPlan, pattern: RemoveLabelPattern, context: LogicalPlanningContext): LogicalPlan = {
    // Plan remoteBatchProperties when property references are used to define the label that needs to be removed
    val (innerRewrittenRBPs, patternRewrittenCachedProps) =
      context.settings.remoteBatchPropertiesStrategy
        .planRemoteBatchPropertiesForMutatingPattern(inner, context, pattern)

    val solved =
      solveds.get(innerRewrittenRBPs.id)
        .asSinglePlannerQuery.updateTailOrSelf(
          _.amendQueryGraph(_.addMutatingPatterns(pattern))
            .resetQueryProjection()
        )
    val rewrittenDynamicLabels =
      patternRewrittenCachedProps.dynamicLabels.toSet.endoRewrite(irExpressionRewriter(innerRewrittenRBPs, context))
    val plan = RemoveLabels(
      innerRewrittenRBPs,
      patternRewrittenCachedProps.variable,
      patternRewrittenCachedProps.labels.toSet,
      rewrittenDynamicLabels
    )
    val providedOrder =
      providedOrderOfUpdate(plan, innerRewrittenRBPs, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, cachedPropertiesAfterMutatingPattern(pattern, innerRewrittenRBPs), context)
  }

  def planForeachApply(
    left: LogicalPlan,
    innerUpdates: LogicalPlan,
    pattern: ForeachPattern,
    context: LogicalPlanningContext,
    expression: Expression
  ): LogicalPlan = {
    val solved =
      solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(
        _.amendQueryGraph(_.addMutatingPatterns(pattern))
          .resetQueryProjection()
      )
    val (rewrittenExpression, rewrittenLeft) = SubqueryExpressionSolver.ForSingle.solve(left, expression, context)
    val plan = ForeachApply(rewrittenLeft, innerUpdates, pattern.variable, rewrittenExpression)
    val providedOrder = providedOrderOfApply(
      rewrittenLeft,
      innerUpdates,
      plan,
      context.settings.executionModel,
      context.providedOrderFactory
    )
    annotate(plan, solved, providedOrder, cachedPropertiesAfterMutatingPattern(pattern, left), context)
  }

  def planForeach(
    inner: LogicalPlan,
    pattern: ForeachPattern,
    context: LogicalPlanningContext,
    expression: Expression
  ): LogicalPlan = {
    // Plan remoteBatchProperties when property references are used in the mutating patterns
    val (innerRewrittenRBPs, patternRewrittenCachedProps) =
      context.settings.remoteBatchPropertiesStrategy
        .planRemoteBatchPropertiesForMutatingPattern(inner, context, pattern)

    val solved =
      solveds.get(innerRewrittenRBPs.id).asSinglePlannerQuery
        .updateTailOrSelf(
          _.amendQueryGraph(_.addMutatingPatterns(pattern))
            .resetQueryProjection()
        )
    val (rewrittenExpression, rewrittenLeft) = SubqueryExpressionSolver
      .ForSingle.solve(innerRewrittenRBPs, expression, context)
    val plan = Foreach(
      rewrittenLeft,
      pattern.variable,
      rewrittenExpression,
      patternRewrittenCachedProps.getSimpleMutatingPatterns
    )
    val providedOrder =
      providedOrderOfUpdate(plan, innerRewrittenRBPs, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, cachedPropertiesAfterMutatingPattern(pattern, innerRewrittenRBPs), context)
  }

  def planEager(
    inner: LogicalPlan,
    context: LogicalPlanningContext,
    reasons: ListSet[EagernessReason]
  ): LogicalPlan =
    annotate(
      Eager(inner, reasons),
      solveds.get(inner.id),
      ProvidedOrder.Left,
      cachedPropertiesPerPlan.get(inner.id),
      context
    )

  def planError(
    inner: LogicalPlan,
    exception: ExhaustiveShortestPathForbiddenException,
    context: LogicalPlanningContext
  ): LogicalPlan =
    annotate(ErrorPlan(inner, exception), solveds.get(inner.id), ProvidedOrder.Left, CachedProperties.empty, context)

  /**
   * @param lastInterestingOrders the interesting order of the last part of the whole query, or `None` for UNION queries.
   */
  def planProduceResult(
    inner: LogicalPlan,
    columns: Seq[LogicalVariable],
    lastInterestingOrders: Option[InterestingOrder],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val produceResult = ProduceResult.withNoCachedProperties(inner, columns)
    if (columns.nonEmpty) {
      def markTailAsFinal(query: SinglePlannerQuery): SinglePlannerQuery =
        query.updateTailOrSelf(
          _.updateQueryProjection(_.markAsFinal)
        )

      def markTailsAsFinal(oldSolved: PlannerQuery): PlannerQuery = oldSolved match {
        case query: SinglePlannerQuery => markTailAsFinal(query)
        case uq @ UnionQuery(lhs, rhs, _, _) =>
          uq.copy(
            lhs = markTailsAsFinal(lhs),
            rhs = markTailAsFinal(rhs)
          )
      }

      val newSolved = markTailsAsFinal(solveds.get(inner.id))
      solveds.set(produceResult.id, newSolved)
    } else {
      solveds.copy(inner.id, produceResult.id)
    }
    // Do not calculate cardinality for ProduceResult. Since the passed context does not have accurate label information
    // It will get a wrong value with some projections. Use the cardinality of inner instead
    cardinalities.copy(inner.id, produceResult.id)
    providedOrders.set(
      produceResult.id,
      providedOrders.get(inner.id).fromLeft(context.providedOrderFactory, Some(produceResult))
    )

    if (lastInterestingOrders.exists(_.requiredOrderCandidate.nonEmpty)) {
      markOrderAsLeveragedBackwardsUntilOrigin(produceResult, context.providedOrderFactory)
    }

    produceResult
  }

  def planRunQueryAt(
    inner: LogicalPlan,
    graphReference: GraphReference,
    queryString: String,
    parameters: Set[Parameter],
    importsAsParameters: Map[Parameter, LogicalVariable],
    columns: Set[LogicalVariable],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val horizon = RunQueryAtProjection(
      graphReference,
      queryString,
      parameters,
      importsAsParameters,
      columns,
      importedExposedSymbols = context.plannerState.importedSubqueryVariables
    )
    val solved =
      solveds
        .get(inner.id)
        .asSinglePlannerQuery
        .updateTailOrSelf(_.withHorizon(horizon))
    val runQueryAt = RunQueryAt(inner, queryString, graphReference, parameters, importsAsParameters, columns)
    annotate(runQueryAt, solved, ProvidedOrder.empty, CachedProperties.empty, context)
  }

  def planRemoteBatchProperties(
    inner: LogicalPlan,
    properties: Set[CachedProperty],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(inner.id)
    val cachedProperties = cachedPropertiesPerPlan.get(inner.id).addAll(properties)
    val plan = inner match {
      case RemoteBatchProperties(nestedInner, nestedProperties) =>
        RemoteBatchProperties(nestedInner, nestedProperties ++ properties)
      // remote batch properties with filter is restricted to a single variable, so only merge if all the next properties to cache match that variable.
      case RemoteBatchPropertiesWithFilter(nestedInner, predicates, nestedProperties)
        if nestedProperties.headOption.exists(_.dependencies == properties.flatMap(_.dependencies)) =>
        RemoteBatchPropertiesWithFilter(nestedInner, predicates, nestedProperties ++ properties)
      case _ => RemoteBatchProperties(inner, properties.map(identity))
    }
    annotate(
      plan,
      solved,
      ProvidedOrder.Left,
      cachedProperties,
      context
    )
  }

  def changeSourceOnRemoteBatchProperties(
    newInner: LogicalPlan,
    currentRemoteBatchProperties: RemoteBatchProperties,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val newRemoteBatchProperties = RemoteBatchProperties(newInner, currentRemoteBatchProperties.properties)

    val solved =
      solveds.get(newInner.id).asSinglePlannerQuery
        .updateTailOrSelf(_.withInterestingOrder(
          solveds.get(currentRemoteBatchProperties.id).asSinglePlannerQuery.interestingOrder
        ))

    annotate(
      newRemoteBatchProperties,
      solved,
      ProvidedOrder.Left,
      cachedPropertiesPerPlan.get(currentRemoteBatchProperties.id),
      context
    )
  }

  def planRemoteBatchPropertiesForHorizonFilters(
    inner: LogicalPlan,
    properties: Set[CachedProperty],
    context: LogicalPlanningContext,
    inlinablePredicates: RewrittenSubQueryPredicatesMap
  ): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.updateHorizon {
      case p: QueryProjection => p.addPredicates(inlinablePredicates.originalExpressions)
      case horizon            => horizon
    })

    val cachedProperties = cachedPropertiesPerPlan.get(inner.id).addAll(properties)
    val plan =
      mergeAndPlanRemoteBatchPropertiesWithFilter(properties, inlinablePredicates.allRewrittenExpressions, inner)
    annotate(plan, solved, ProvidedOrder.Left, cachedProperties, context)
  }

  def planRemoteBatchPropertiesWithFilter(
    inner: LogicalPlan,
    properties: Set[CachedProperty],
    context: LogicalPlanningContext,
    inlinablePredicatesToExecute: Seq[Expression],
    inlinablePredicatesToReport: Seq[Expression]
  ): LogicalPlan = {
    val cachedProperties = cachedPropertiesPerPlan.get(inner.id).addAll(properties)
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(
      _.amendQueryGraph(_.addPredicates(inlinablePredicatesToReport: _*))
    )
    val plan = mergeAndPlanRemoteBatchPropertiesWithFilter(properties, inlinablePredicatesToExecute, inner)
    annotate(plan, solved, ProvidedOrder.Left, cachedProperties, context)
  }

  def planShardSelections(
    selection: RemoteBatchPropertiesWithPushdownOperators,
    rewrittenSubQueryPredicates: RewrittenSubQueryPredicatesMap,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val cachedProperties = cachedPropertiesPerPlan.get(selection.source.id).add(
      selection.variable,
      selection.entityType,
      selection.properties
    )
    val solved = solveds.get(selection.source.id).asSinglePlannerQuery.updateTailOrSelf(
      _.amendQueryGraph(_.addPredicates(rewrittenSubQueryPredicates.originalExpressions))
    )

    val plan = mergePushdownShardOperator(selection)
    annotate(plan, solved, ProvidedOrder.Left, cachedProperties, context)
  }

  def planProjectionsOnShards(
    projectionOperation: RemoteBatchPropertiesWithPushdownOperators,
    rewrittenSubQueryPredicates: RewrittenSubQueryPredicatesMap,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val cachedProperties = cachedPropertiesPerPlan.get(projectionOperation.source.id).add(
      projectionOperation.variable,
      projectionOperation.entityType,
      projectionOperation.properties
    )

    val plan = mergePushdownShardOperator(
      projectionOperation
    )
    val solved = solveds.get(projectionOperation.source.id).asSinglePlannerQuery.updateTailOrSelf(_.updateHorizon {
      case p: QueryProjection => p.addPredicates(rewrittenSubQueryPredicates.originalExpressions)
      case horizon            => horizon
    })

    annotate(plan, solved, ProvidedOrder.Left, cachedProperties, context)
  }

  private def mergePushdownShardOperator(
    remoteBatchPropertiesWithPushdownOperators: RemoteBatchPropertiesWithPushdownOperators
  ): LogicalPlan = remoteBatchPropertiesWithPushdownOperators.source match {
    case RemoteBatchProperties(nestedInner, nestedProperties)
      if nestedProperties.forall(_.dependencies == Set(remoteBatchPropertiesWithPushdownOperators.variable)) =>
      remoteBatchPropertiesWithPushdownOperators.copy(
        source = nestedInner,
        properties = remoteBatchPropertiesWithPushdownOperators.properties ++ nestedProperties.map(_.propertyKey)
      )(idGen)
    case Apply(RemoteBatchProperties(nestedInner, nestedProperties), _: Argument)
      if nestedProperties.forall(_.dependencies == Set(remoteBatchPropertiesWithPushdownOperators.variable)) =>
      remoteBatchPropertiesWithPushdownOperators.copy(
        source = nestedInner,
        properties = remoteBatchPropertiesWithPushdownOperators.properties ++ nestedProperties.map(_.propertyKey)
      )(idGen)
    case innerRemoteBatchPropertiesWithPushdown: RemoteBatchPropertiesWithPushdownOperators
      if innerRemoteBatchPropertiesWithPushdown.variable == remoteBatchPropertiesWithPushdownOperators.variable =>
      mergeRemoteBatchPropertiesWithPushdownOperators(
        remoteBatchPropertiesWithPushdownOperators,
        innerRemoteBatchPropertiesWithPushdown
      )
    case Apply(innerRemoteBatchPropertiesWithPushdown: RemoteBatchPropertiesWithPushdownOperators, _: Argument)
      if innerRemoteBatchPropertiesWithPushdown.variable == remoteBatchPropertiesWithPushdownOperators.variable =>
      mergeRemoteBatchPropertiesWithPushdownOperators(
        remoteBatchPropertiesWithPushdownOperators,
        innerRemoteBatchPropertiesWithPushdown
      )
    case _ =>
      remoteBatchPropertiesWithPushdownOperators
  }

  private def mergeRemoteBatchPropertiesWithPushdownOperators(
    remoteBatchPropertiesWithPushdownOperators: RemoteBatchPropertiesWithPushdownOperators,
    innerRemoteBatchPropertiesWithPushdown: RemoteBatchPropertiesWithPushdownOperators
  ) = {
    RemoteBatchPropertiesWithPushdownOperators(
      source = innerRemoteBatchPropertiesWithPushdown.source,
      variable = innerRemoteBatchPropertiesWithPushdown.variable,
      entityType = innerRemoteBatchPropertiesWithPushdown.entityType,
      properties =
        innerRemoteBatchPropertiesWithPushdown.properties ++ remoteBatchPropertiesWithPushdownOperators.properties,
      predicates =
        innerRemoteBatchPropertiesWithPushdown.predicates ++ remoteBatchPropertiesWithPushdownOperators.predicates,
      distinctBy = innerRemoteBatchPropertiesWithPushdown.distinctBy.orElse(
        remoteBatchPropertiesWithPushdownOperators.distinctBy
      ),
      orderBy = innerRemoteBatchPropertiesWithPushdown.orderBy ++ remoteBatchPropertiesWithPushdownOperators.orderBy,
      limit =
        remoteBatchPropertiesWithPushdownOperators.limit.orElse(innerRemoteBatchPropertiesWithPushdown.limit),
      importedConstantValues =
        innerRemoteBatchPropertiesWithPushdown.importedConstantValues ++ remoteBatchPropertiesWithPushdownOperators.importedConstantValues,
      importedPerRowValues =
        innerRemoteBatchPropertiesWithPushdown.importedPerRowValues ++ remoteBatchPropertiesWithPushdownOperators.importedPerRowValues
    )(idGen)
  }

  /**
   * RemoteBatchPropertiesWithFilter will only fetch properties for a SINGLE variable.
   * To maintain the correctness of this assumption, we can merge the current set of properties with the previous operator only if
   * 1. the previous operator is RemoteBatchProperties where all the properties being fetched are for the same variable as the current remoteBatchProperties
   * 2. the previous operator is a RemoteBatchPropertiesWithFilter  where all the properties being fetched are for the same variable.
   */
  private def mergeAndPlanRemoteBatchPropertiesWithFilter(
    properties: Set[CachedProperty],
    inlinablePredicates: Iterable[Expression],
    inner: LogicalPlan
  ): RemoteBatchPropertiesWithFilter = {
    val logicalVariablesOfProperties = properties.map(_.entityVariable)
    inner match {
      case RemoteBatchProperties(nestedInner, nestedProperties)
        if nestedProperties.forall(_.dependencies == logicalVariablesOfProperties) =>
        RemoteBatchPropertiesWithFilter(nestedInner, inlinablePredicates.toSet, nestedProperties ++ properties)
      case RemoteBatchPropertiesWithFilter(nestedInner, nestedPredicates, nestedProperties)
        if nestedProperties.headOption.exists(_.dependencies == logicalVariablesOfProperties) =>
        RemoteBatchPropertiesWithFilter(
          nestedInner,
          nestedPredicates ++ inlinablePredicates,
          nestedProperties ++ properties
        )
      case _ => RemoteBatchPropertiesWithFilter(inner, inlinablePredicates.toSet, properties.map(identity))
    }
  }

  def addMissingStandaloneArgumentPatternNodes(
    plan: LogicalPlan,
    query: SinglePlannerQuery,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(plan.id).asSinglePlannerQuery
    val missingNodes = query.queryGraph.standaloneArgumentPatternNodes diff solved.queryGraph.patternNodes
    if (missingNodes.isEmpty) {
      plan
    } else {
      val newSolved = solved.amendQueryGraph(_.addPatternNodes(missingNodes.toSeq: _*))
      val providedOrder = providedOrders.get(plan.id)
      val cachedProperties = cachedPropertiesPerPlan.get(plan.id)
      annotate(plan.copyPlanWithIdGen(idGen), newSolved, providedOrder, cachedProperties, context)
    }
  }

  /**
   * Updates may make the current provided order invalid since the order may depend on something that gets mutated.
   * If this is the case, this method returns an empty provided order, otherwise it forwards the provided order from the left.
   */
  private def providedOrderOfUpdate(
    updatePlan: UpdatingPlan,
    sourcePlan: LogicalPlan,
    executionModel: ExecutionModel,
    providedOrderFactory: ProvidedOrderFactory
  ): ProvidedOrder =
    if (invalidatesProvidedOrder(updatePlan, executionModel)) {
      ProvidedOrder.empty
    } else {
      providedOrders.get(sourcePlan.id).fromLeft(providedOrderFactory, Some(updatePlan))
    }

  private def providedOrderOfApply(
    left: LogicalPlan,
    right: LogicalPlan,
    plan: LogicalPlan,
    executionModel: ExecutionModel,
    providedOrderFactory: ProvidedOrderFactory
  ): ProvidedOrder = {
    // Plans with a rhs may invalidate the provided order coming from the lhs. If this is the case, this method returns an empty provided order.
    if (invalidatesProvidedOrderRecursive(right, executionModel)) {
      ProvidedOrder.empty
    } else {
      val leftProvidedOrder = providedOrders.get(left.id)
      val rightProvidedOrder = providedOrders.get(right.id)
      val leftDistinctness = left.distinctness

      LogicalPlanProducer.providedOrderOfApply(
        leftProvidedOrder,
        rightProvidedOrder,
        leftDistinctness,
        plan,
        providedOrderFactory
      )
    }
  }

  private def assertRhsDoesNotInvalidateLhsOrder(
    plan: LogicalPlan,
    providedOrder: ProvidedOrder,
    executionModel: ExecutionModel
  ): Unit = {
    if (AssertionRunner.ASSERTIONS_ENABLED) {
      (plan, providedOrder.orderOrigin) match {
        case (rollUpApply: RollUpApply, _) if rollUpApply.right.readOnly =>
          // special case for RollUpApply as it is assumed to not invalidate LHS order regardless of RHS plans
          ()
        case (plan: LogicalBinaryPlan, Some(ProvidedOrder.Left))
          if invalidatesProvidedOrderRecursive(plan.right, executionModel) =>
          val msg =
            s"""LHS claims to provide an order, but RHS contains clauses that invalidates this order.
               |Provided order: $providedOrder
               |Plan:
               |$plan""".stripMargin
          throw new AssertionError(msg)
        case _ =>
      }
    }
  }

  /**
   * Currently we consider all updates, except MERGE, as invalidating provided order
   */
  private def invalidatesProvidedOrder(plan: LogicalPlan, executionModel: ExecutionModel): Boolean = {
    (plan match {
      // MERGE will either be ordered by its inner plan or create a single row which by
      // definition is ordered. However, if you do ON MATCH SET ... that might invalidate the
      // inner ordering.
      case m: Merge => m.onMatch.nonEmpty
      case _        => plan.isUpdatingPlan
    }) || executionModel.invalidatesProvidedOrder(plan)
  }

  private def invalidatesProvidedOrderRecursive(plan: LogicalPlan, executionModel: ExecutionModel): Boolean =
    plan.folder.treeExists {
      case logicalPlan: LogicalPlan if invalidatesProvidedOrder(logicalPlan, executionModel) => true
    }

  /**
   * Compute cardinality for a plan. Set this cardinality in the Cardinalities attribute.
   * Set the other attributes with the provided arguments (solved and providedOrder).
   *
   * @return the same plan
   */
  private def annotate[T <: LogicalPlan](
    plan: T,
    solved: PlannerQuery,
    providedOrder: ProvidedOrder,
    cachedProperties: CachedProperties,
    context: LogicalPlanningContext
  ): T = {
    assertNoBadExpressionsExists(plan)
    assertRhsDoesNotInvalidateLhsOrder(plan, providedOrder, context.settings.executionModel)
    val cardinality =
      cardinalityModel(
        solved,
        context.plannerState.input.labelInfo,
        context.plannerState.input.relTypeInfo,
        context.semanticTable,
        context.plannerState.indexCompatiblePredicatesProviderContext,
        context.staticComponents.graphSchemaOptimizations
      )
    solveds.set(plan.id, solved)
    cardinalities.set(plan.id, cardinality)
    AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
      providedOrder.isEmpty || Set(plan.lhs, plan.lhs).flatten.forall(p => providedOrders.get(p.id) ne providedOrder),
      s"A plan must not use the same provided order instance as one of its children. Make sure to use the ProvidedOrderFactory."
    )
    providedOrders.set(plan.id, providedOrder)
    cachedPropertiesPerPlan.set(plan.id, cachedProperties)
    plan
  }

  /**
   * Compute cardinality for a plan. Set this cardinality in the Cardinalities attribute.
   * Forward the ProvidedOrder from the LHS or RHS depending on the given `providedOrderPropagationRule`.
   *
   * @return the same plan
   */
  private def annotate[T <: LogicalPlan](
    plan: T,
    solved: PlannerQuery,
    providedOrderPropagationRule: ProvidedOrder.OrderOrigin,
    cachedProperties: CachedProperties,
    context: LogicalPlanningContext
  ): T = {
    val providedOrder = providedOrderPropagationRule match {
      case ProvidedOrder.Left =>
        providedOrders.get(plan.lhs.get.id).fromLeft(context.providedOrderFactory, Some(plan))
      case ProvidedOrder.Right =>
        providedOrders.get(plan.rhs.get.id).fromRight(context.providedOrderFactory, Some(plan))
      case ProvidedOrder.Both => throw new IllegalAccessException("Not allowed to pass ProvidedOrder.Both to annotate.")
      case ProvidedOrder.Self => throw new IllegalAccessException("Not allowed to pass ProvidedOrder.Self to annotate.")
    }
    annotate(plan, solved, providedOrder, cachedProperties, context)
  }

  /**
   * Same as [[annotate()]], but in addition also set the labelAndRelTypeInfos attribute.
   *
   * @return the same plan
   */
  private def annotateSelection(
    selection: Selection,
    solved: PlannerQuery,
    providedOrderPropagationRule: ProvidedOrder.OrderOrigin,
    cachedProperties: CachedProperties,
    context: LogicalPlanningContext
  ): Selection = {
    labelAndRelTypeInfos.set(
      selection.id,
      Some(LabelAndRelTypeInfo(context.plannerState.input.labelInfo, context.plannerState.input.relTypeInfo))
    )

    annotate(selection, solved, providedOrderPropagationRule, cachedProperties, context)
  }

  /**
   * There probably exists some type level way of achieving this with type safety instead of manually searching through the expression tree like this
   */
  private def assertNoBadExpressionsExists(root: Any): Unit = {
    checkOnlyWhenAssertionsAreEnabled(!root.folder.treeExists {
      case _: PatternComprehension | _: PatternExpression | _: IRExpression | _: MapProjection | _: PartialPredicate[_]
        | _: ImpliedLabel =>
        throw InternalException.internalError(
          this.getClass.getSimpleName,
          s"This expression should not be added to a logical plan:\n$root"
        )
      case _ =>
        false
    })
  }

  private def projectedDirection(
    pattern: PatternRelationship,
    from: LogicalVariable,
    dir: SemanticDirection
  ): SemanticDirection = {
    if (dir == SemanticDirection.BOTH) {
      if (from == pattern.left) {
        SemanticDirection.OUTGOING
      } else {
        SemanticDirection.INCOMING
      }
    } else {
      pattern.dir
    }
  }

  private def planRegularProjectionHelper(
    inner: LogicalPlan,
    expressions: Map[LogicalVariable, Expression],
    context: LogicalPlanningContext,
    solved: SinglePlannerQuery,
    cachedPropertiesToReport: CachedProperties
  ): Projection = {
    val columnsWithRenames = renameProvidedOrderColumns(providedOrders.get(inner.id).columns, expressions)
    val plan = Projection(inner, expressions)
    val providedOrder = context.providedOrderFactory.providedOrder(columnsWithRenames, ProvidedOrder.Left, Some(plan))
    annotate(
      plan,
      solved,
      providedOrder,
      cachedPropertiesToReport.rename(renamedVariables(expressions)),
      context
    )
  }

  private def renamedVariables(expressions: Map[LogicalVariable, Expression]): Map[LogicalVariable, LogicalVariable] =
    expressions.collect {
      case (newNamedVariable: LogicalVariable, oldNamedVariable: LogicalVariable)
        if newNamedVariable.name != oldNamedVariable.name => (oldNamedVariable, newNamedVariable)
    }

  /**
   * The provided order is used to describe the current ordering of the LogicalPlan within a complete plan tree. For
   * index leaf operators this can be planned as an IndexOrder for the index to provide. In that case it only works
   * if all columns are sorted in the same direction, so we need to narrow the scope for these index operations.
   */
  private def toIndexOrder(providedOrder: ProvidedOrder): IndexOrder = providedOrder match {
    case ProvidedOrder.empty                                           => IndexOrderNone
    case ProvidedOrder(columns) if columns.forall(c => c.isAscending)  => IndexOrderAscending
    case ProvidedOrder(columns) if columns.forall(c => !c.isAscending) => IndexOrderDescending
    case _ => throw new IllegalStateException("Cannot mix ascending and descending columns when using index order")
  }

  /**
   * Rename sort columns if they are renamed in a projection.
   */
  private def renameProvidedOrderColumns(
    columns: Seq[ordering.ColumnOrder],
    projectExpressions: Map[LogicalVariable, Expression]
  ): Seq[ordering.ColumnOrder] = {
    columns.map {
      case columnOrder @ ordering.ColumnOrder(e @ Property(v: Variable, p @ PropertyKeyName(propName))) =>
        projectExpressions.collectFirst {
          case (
              newVar,
              Property(`v`, PropertyKeyName(`propName`)) | CachedProperty(
                _,
                `v`,
                PropertyKeyName(`propName`),
                _,
                _,
                _
              )
            ) =>
            ordering.ColumnOrder(newVar, columnOrder.isAscending)
          case (newVar, `v`) =>
            ordering.ColumnOrder(
              Property(newVar, PropertyKeyName(propName)(p.position))(e.position),
              columnOrder.isAscending
            )
        }.getOrElse(columnOrder)
      case columnOrder @ ordering.ColumnOrder(expression) =>
        projectExpressions.collectFirst {
          case (newVar, `expression`) =>
            ordering.ColumnOrder(newVar, columnOrder.isAscending)
        }.getOrElse(columnOrder)
    }
  }

  private def trimAndRenameProvidedOrder(
    providedOrder: ProvidedOrder,
    grouping: Map[LogicalVariable, Expression]
  ): Seq[ordering.ColumnOrder] = {
    // Trim provided order for each sort column, if it is a non-grouping column
    val trimmed = providedOrder.columns.takeWhile {
      case ordering.ColumnOrder(Property(v: Variable, PropertyKeyName(propName))) =>
        grouping.values.exists {
          case CachedProperty(`v`, _, PropertyKeyName(`propName`), _, _, _) => true
          case CachedHasProperty(`v`, _, PropertyKeyName(`propName`), _, _) => true
          case Property(`v`, PropertyKeyName(`propName`))                   => true
          case _                                                            => false
        }
      case ordering.ColumnOrder(expression) =>
        grouping.values.exists {
          case `expression` => true
          case _            => false
        }
    }
    renameProvidedOrderColumns(trimmed, grouping)
  }

  /**
   * Starting from `lp`, traverse the logical plan backwards until finding the origin(s) of the current provided order.
   * For each plan on the way, set `leveragedOrder` to `true`.
   *
   * @param lp the plan that leverages a provided order. Must be an already annotated plan.
   */
  private def markOrderAsLeveragedBackwardsUntilOrigin(
    lp: LogicalPlan,
    providedOrderFactory: ProvidedOrderFactory
  ): Unit = {
    def setIfUndefined(plan: LogicalPlan, leveragedOrders: LeveragedOrders, bool: Boolean): Unit = {
      if (!leveragedOrders.isDefinedAt(plan.id)) leveragedOrders.set(plan.id, bool)
    }

    setIfUndefined(lp, leveragedOrders, bool = true)

    def loop(current: LogicalPlan): Unit = {
      setIfUndefined(current, leveragedOrders, bool = true)
      val origin = providedOrders.get(current.id).orderOrigin
      origin match {
        case Some(ProvidedOrder.Left)  => loop(current.lhs.get)
        case Some(ProvidedOrder.Right) => loop(current.rhs.get)
        case Some(ProvidedOrder.Both)  => loop(current.lhs.get); loop(current.rhs.get)
        case Some(ProvidedOrder.Self)  => // done
        case None =>
          val bug = "While marking leveraged order we encountered a plan with no provided order. This is a bug."
          providedOrderFactory match {
            case DefaultProvidedOrderFactory =>
              throw new IllegalStateException(
                s"$bug\n${LogicalPlanToPlanBuilderString(current)}"
              )
            case ParallelExecutionProvidedOrderFactory =>
              throw new IllegalStateException(
                s"$bug In the meantime, try running without `runtime=parallel`.\n${LogicalPlanToPlanBuilderString(current)}"
              )
          }

      }
    }

    providedOrders.get(lp.id).orderOrigin match {
      case Some(ProvidedOrder.Left)  => lp.lhs.foreach(loop)
      case Some(ProvidedOrder.Right) => lp.rhs.foreach(loop)
      case Some(ProvidedOrder.Both)  => lp.lhs.foreach(loop); lp.rhs.foreach(loop)
      case Some(
          ProvidedOrder.Self
        ) => // If the plan both introduces and leverages the order, we do not want to traverse into the children
      case None =>
        // The plan itself leverages the order, but does not maintain it.
        // Currently, in that case we assume it is a one-child plan,
        // since at the time of writing there is no two child plan that leverages and destroys ordering
        lp.lhs.foreach(loop)
        AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
          lp.rhs.isEmpty,
          "We assume that there is no two-child plan leveraging but destroying ordering."
        )
    }
  }
}

object LogicalPlanProducer {

  /**
   * @return whether `hintMode` accepts `planMode`
   */
  private def expandModeMatches(hintMode: Option[ExpandHintMode], planMode: ExpansionMode): Boolean =
    hintMode match {
      case Some(ExpandHintAll)  => planMode == ExpandAll
      case Some(ExpandHintInto) => planMode == ExpandInto
      case None                 => true
    }

  /**
   * True iff `hint` matches the node connection from `planFrom` to `planTo` via `planRelIds` with expansion mode
   * `planMode`, given that `claimedStepIds` are already solved.
   */
  private def expandHintClaims(
    hint: UsingExpandStepHint,
    planFrom: LogicalVariable,
    planTo: LogicalVariable,
    planRelIds: Set[LogicalVariable],
    planMode: ExpansionMode,
    claimedStepIds: Set[UsingExpandStepId]
  ): Boolean = {
    val UsingExpandStepHint(hintFrom, hintTo, hintVia, hintMode, _, mustFollow) = hint

    val endpointsMatch =
      hintFrom.forall(_ == planFrom) &&
        hintTo.forall(_ == planTo)

    val viaMatches = hintVia.forall(planRelIds.contains)

    val mustFollowSolved = mustFollow.subsetOf(claimedStepIds)

    endpointsMatch &&
    viaMatches &&
    expandModeMatches(hintMode, planMode) &&
    mustFollowSolved
  }

  /**
   * This method assumes that no invalidation of provided order happens on the RHS.
   * It combines the leftProvidedOrder and rightProvidedOrder taking into account
   * leftDistinctness, describing if and how the LHS rows are distinct.
   */
  private[steps] def providedOrderOfApply(
    leftProvidedOrder: ProvidedOrder,
    rightProvidedOrder: ProvidedOrder,
    leftDistinctness: Distinctness,
    plan: LogicalPlan,
    providedOrderFactory: ProvidedOrderFactory
  ): ProvidedOrder = {
    // To combine two orders, we concatenate their columns, if both orders are non-empty.
    def combinedOrder: ProvidedOrder = {
      if (leftProvidedOrder.isEmpty) {
        rightProvidedOrder.fromRight(providedOrderFactory, Some(plan))
      } else if (rightProvidedOrder.isEmpty) {
        leftProvidedOrder.fromLeft(providedOrderFactory, Some(plan))
      } else {
        leftProvidedOrder
          .followedBy(rightProvidedOrder)(providedOrderFactory, Some(plan))
          .fromBoth(providedOrderFactory, Some(plan))
      }
    }

    def leftProvidedOrderPrefixes: Iterator[Set[Expression]] =
      for (l <- (1 to leftProvidedOrder.columns.length).iterator) yield {
        leftProvidedOrder.columns.take(l).map(_.expression).toSet
      }

    leftDistinctness match {
      case AtMostOneRow =>
        combinedOrder
      case DistinctColumns(columns) if leftProvidedOrderPrefixes.contains(columns) =>
        // We can use the combined order if a prefix of the leftProvidedOrder is distinct
        combinedOrder
      case _ =>
        // If the LHS has duplicate values, we cannot guarantee any added order from the RHS
        leftProvidedOrder.fromLeft(providedOrderFactory, Some(plan))
    }
  }

  def solvedForTailApply(
    left: LogicalPlan,
    right: LogicalPlan,
    solveds: PlanningAttributes.Solveds
  ): SinglePlannerQuery = {
    solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.withTail(solveds.get(right.id).asSinglePlannerQuery))
  }

}
