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
import org.neo4j.cypher.internal.ast.GraphReference
import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.ShowConstraintsClause
import org.neo4j.cypher.internal.ast.ShowFunctionsClause
import org.neo4j.cypher.internal.ast.ShowIndexesClause
import org.neo4j.cypher.internal.ast.ShowProceduresClause
import org.neo4j.cypher.internal.ast.ShowSettingsClause
import org.neo4j.cypher.internal.ast.ShowTransactionsClause
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsErrorParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsReportParameters
import org.neo4j.cypher.internal.ast.TerminateTransactionsClause
import org.neo4j.cypher.internal.ast.Union.UnionMapping
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.ast.UsingScanHint
import org.neo4j.cypher.internal.ast.UsingStatefulShortestPathHint
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.helpers.PredicateHelper.coercePredicatesWithAnds
import org.neo4j.cypher.internal.compiler.planner.ProcedureCallProjection
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.irExpressionRewriter
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.ContainsSearchMode
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EndsWithSearchMode
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.StringSearchMode
import org.neo4j.cypher.internal.compiler.planner.logical.steps.skipAndLimit.planLimitOnTopOf
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.CachedHasProperty
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapProjection
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.VariableGrouping
import org.neo4j.cypher.internal.expressions.functions.Collect
import org.neo4j.cypher.internal.expressions.functions.UnresolvedFunction
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
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
import org.neo4j.cypher.internal.ir.NodeBinding
import org.neo4j.cypher.internal.ir.PassthroughAllHorizon
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryProjection
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
import org.neo4j.cypher.internal.ir.SimpleMutatingPattern
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.UnionQuery
import org.neo4j.cypher.internal.ir.UnwindProjection
import org.neo4j.cypher.internal.ir.VarPatternLength
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
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.ColumnOrder
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
import org.neo4j.cypher.internal.logical.plans.DirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.DistinctColumns
import org.neo4j.cypher.internal.logical.plans.Distinctness
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.EmptyResult
import org.neo4j.cypher.internal.logical.plans.ErrorPlan
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLimit
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpansionMode
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths.DisallowSameNode
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths.SkipSameNode
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.ForeachApply
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
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString
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
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
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
import org.neo4j.cypher.internal.logical.plans.TerminateTransactions
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.Top1WithTies
import org.neo4j.cypher.internal.logical.plans.Trail
import org.neo4j.cypher.internal.logical.plans.TransactionApply
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency
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
import org.neo4j.cypher.internal.logical.plans.UpdatingPlan
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.logical.plans.ordering.DefaultProvidedOrderFactory
import org.neo4j.cypher.internal.logical.plans.ordering.ParallelExecutionProvidedOrderFactory
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrderFactory
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.IndexType
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.util.AssertionRunner
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.exceptions.ExhaustiveShortestPathForbiddenException
import org.neo4j.exceptions.InternalException

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

  private val attributesWithoutSolveds =
    planningAttributes.asAttributes(idGen).without(solveds, planningAttributes.effectiveCardinalities)

  /**
   * This object is simply to group methods that are used by the [[SubqueryExpressionSolver]], and thus do not need to update `solveds`
   */
  object ForSubqueryExpressionSolver {

    def planArgument(argumentIds: Set[LogicalVariable], context: LogicalPlanningContext): LogicalPlan = {
      annotate(Argument(argumentIds), SinglePlannerQuery.empty, ProvidedOrder.empty, context)
    }

    def planApply(left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
      val plan = Apply(left, right)
      val providedOrder =
        providedOrderOfApply(left, right, plan, context.settings.executionModel, context.providedOrderFactory)
      // The RHS is the leaf plan we are wrapping under an apply in order to solve the pattern expression.
      // It has the correct solved
      val solved = solveds.get(right.id)
      annotate(plan, solved, providedOrder, context)
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
        context
      )
    }
  }

  def solvePredicate(plan: LogicalPlan, solvedExpression: Expression): LogicalPlan = {
    solvePredicates(plan, Set(solvedExpression))
  }

  def solvePredicates(plan: LogicalPlan, solvedExpressions: Set[Expression]): LogicalPlan = {
    // Keep other attributes but change solved
    val keptAttributes = Attributes(idGen, cardinalities, providedOrders, leveragedOrders, labelAndRelTypeInfos)
    val newPlan = plan.copyPlanWithIdGen(keptAttributes.copy(plan.id))
    val solvedPlannerQuery =
      solveds.get(plan.id).asSinglePlannerQuery.amendQueryGraph(_.addPredicates(solvedExpressions.toSeq: _*))
    solveds.set(newPlan.id, solvedPlannerQuery)
    newPlan
  }

  def solvePredicateInHorizon(plan: LogicalPlan, solvedExpression: Expression): LogicalPlan = {
    // Keep other attributes but change solved
    val keptAttributes = Attributes(idGen, cardinalities, providedOrders, leveragedOrders, labelAndRelTypeInfos)
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
      RegularSinglePlannerQuery(queryGraph = QueryGraph(argumentIds = argumentIds, patternNodes = Set(variable)))

    annotate(AllNodesScan(variable, argumentIds), solved, ProvidedOrder.empty, context)
  }

  /**
   * @param variable             the name of the relationship variable
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
        RegularSinglePlannerQuery(queryGraph =
          QueryGraph(
            argumentIds = argumentIds,
            patternNodes = Set(firstNode, secondNode),
            patternRelationships = Set(patternForLeafPlan)
          )
        )

      val leafPlan =
        if (patternForLeafPlan.dir == BOTH) {
          UndirectedAllRelationshipsScan(variable, firstNode, secondNode, argumentIds)
        } else {
          DirectedAllRelationshipsScan(variable, firstNode, secondNode, argumentIds)
        }
      annotate(leafPlan, solved, ProvidedOrder.empty, context)
    }

    planHiddenSelectionIfNeeded(planLeaf, hiddenSelections, context, originalPattern)
  }

  /**
   * @param variable             the name of the relationship variable
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
        context
      )
    }

    planHiddenSelectionIfNeeded(planLeaf, hiddenSelections, context, originalPattern)
  }

  /**
   * @param variable             the name of the relationship variable
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
        context
      )
    }

    planHiddenSelectionIfNeeded(planLeaf, hiddenSelections, context, originalPattern)
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
        context
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

      val planTemplate = (patternForLeafPlan.dir, stringSearchMode) match {
        case (SemanticDirection.BOTH, ContainsSearchMode) =>
          UndirectedRelationshipIndexContainsScan(_, _, _, _, _, _, _, _, _)
        case (SemanticDirection.BOTH, EndsWithSearchMode) =>
          UndirectedRelationshipIndexEndsWithScan(_, _, _, _, _, _, _, _, _)
        case (SemanticDirection.INCOMING | SemanticDirection.OUTGOING, ContainsSearchMode) =>
          DirectedRelationshipIndexContainsScan(_, _, _, _, _, _, _, _, _)
        case (SemanticDirection.INCOMING | SemanticDirection.OUTGOING, EndsWithSearchMode) =>
          DirectedRelationshipIndexEndsWithScan(_, _, _, _, _, _, _, _, _)
      }

      val leafPlan = planTemplate(
        variable,
        patternForLeafPlan.inOrder._1,
        patternForLeafPlan.inOrder._2,
        relationshipType,
        properties.head,
        rewrittenValueExpr,
        argumentIds ++ newArguments,
        indexOrder,
        indexType.toPublicApi
      )

      solver.rewriteLeafPlan {
        annotateRelationshipLeafPlan(
          leafPlan,
          patternForLeafPlan,
          solvedPredicates,
          solvedHint,
          argumentIds,
          providedOrder,
          context
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
          context
        )
      }
    }

    planHiddenSelectionIfNeeded(planLeaf, hiddenSelections, context, originalPattern)
  }

  /**
   * @param variable             the name of the relationship variable
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
          context
        )
      }
    }

    planHiddenSelectionIfNeeded(planLeaf, hiddenSelections, context, originalPattern)
  }

  private def annotateRelationshipLeafPlan(
    leafPlan: RelationshipLogicalLeafPlan,
    patternForLeafPlan: PatternRelationship,
    solvedPredicates: Seq[Expression],
    solvedHint: IterableOnce[Hint],
    argumentIds: Set[LogicalVariable],
    providedOrder: ProvidedOrder,
    context: LogicalPlanningContext
  ) = {
    val solved = RegularSinglePlannerQuery(queryGraph =
      QueryGraph.empty
        .addPatternRelationship(patternForLeafPlan)
        .addPredicates(solvedPredicates: _*)
        .addHints(solvedHint)
        .addArgumentIds(argumentIds.toIndexedSeq)
    )

    annotate(leafPlan, solved, providedOrder, context)
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
    : InTransactionsOnErrorBehaviour = {
    maybeErrorParams.map(_.behaviour).getOrElse(TransactionForeach.defaultOnErrorBehaviour)
  }

  private def computeMaybeReportAs(maybeReportParams: Option[InTransactionsReportParameters])
    : Option[LogicalVariable] = {
    maybeReportParams.map(_.reportAs)
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
    // We don't want to keep the arguments that this Apply is inserting on the RHS, so we remove them here.
    val rhsSolved =
      solveds.get(right.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.withArgumentIds(Set.empty)))
    val solved = solveds.get(left.id).asSinglePlannerQuery ++ rhsSolved
    val plan = Apply(left, right)
    val providedOrder =
      providedOrderOfApply(left, right, plan, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, context)
  }

  def planMergeApply(left: LogicalPlan, right: Merge, context: LogicalPlanningContext): LogicalPlan = {
    val lhsSolved = solveds.get(left.id).asSinglePlannerQuery
    val rhsSolved = solveds.get(right.id).asSinglePlannerQuery
    val solved =
      lhsSolved.updateTailOrSelf(_.amendQueryGraph(_.addMutatingPatterns(rhsSolved.queryGraph.mutatingPatterns)))

    val plan = Apply(left, right)
    val providedOrder =
      providedOrderOfApply(left, right, plan, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, context)
  }

  def planSubquery(
    left: LogicalPlan,
    right: LogicalPlan,
    context: LogicalPlanningContext,
    correlated: Boolean,
    yielding: Boolean,
    inTransactionsParameters: Option[InTransactionsParameters]
  ): LogicalPlan = {
    val solvedLeft = solveds.get(left.id)
    val solvedRight = solveds.get(right.id)
    val solved = solvedLeft.asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(CallSubqueryHorizon(
      solvedRight,
      correlated,
      yielding,
      inTransactionsParameters
    )))

    val plan =
      if (yielding) {
        inTransactionsParameters match {
          case Some(InTransactionsParameters(batchParams, concurrencyParams, errorParams, reportParams)) =>
            TransactionApply(
              left,
              right,
              computeBatchSize(batchParams.map(_.batchSize)),
              computeConcurrency(concurrencyParams.map(_.concurrency)),
              computeErrorBehaviour(errorParams),
              computeMaybeReportAs(reportParams)
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
          case Some(InTransactionsParameters(batchParams, concurrencyParams, errorParams, reportParams)) =>
            TransactionForeach(
              left,
              right,
              computeBatchSize(batchParams.map(_.batchSize)),
              computeConcurrency(concurrencyParams.map(_.concurrency)),
              computeErrorBehaviour(errorParams),
              computeMaybeReportAs(reportParams)
            )
          case None => SubqueryForeach(left, right)
        }
      }

    val providedOrder =
      providedOrderOfApply(left, right, plan, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, context)
  }

  def planTailApply(left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val solved =
      solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.withTail(solveds.get(right.id).asSinglePlannerQuery))
    val plan = Apply(left, right)
    val providedOrder =
      providedOrderOfApply(left, right, plan, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, context)
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
    annotate(plan, solved, providedOrder, context)
  }

  def planCartesianProduct(left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val solved: SinglePlannerQuery =
      solveds.get(left.id).asSinglePlannerQuery ++ solveds.get(right.id).asSinglePlannerQuery
    val plan = CartesianProduct(left, right)
    val providedOrder =
      providedOrderOfApply(left, right, plan, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, context)
  }

  def planSimpleExpand(
    left: LogicalPlan,
    from: LogicalVariable,
    to: LogicalVariable,
    pattern: PatternRelationship,
    mode: ExpansionMode,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val dir = pattern.directionRelativeTo(from)
    val solved = solveds.get(left.id).asSinglePlannerQuery.amendQueryGraph(_.addPatternRelationship(pattern))
    annotate(
      Expand(left, from, dir, pattern.types, to, pattern.variable, mode),
      solved,
      ProvidedOrder.Left,
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
    mode: ExpansionMode,
    context: LogicalPlanningContext
  ): LogicalPlan = {

    val dir = patternRelationship.directionRelativeTo(from)

    patternRelationship.length match {
      case l: VarPatternLength =>
        val projectedDir = projectedDirection(patternRelationship, from, dir)

        val solved = solveds.get(source.id).asSinglePlannerQuery.amendQueryGraph(_
          .addPatternRelationship(patternRelationship)
          .addPredicates(solvedPredicates.toSeq: _*))

        val (rewrittenRelationshipPredicates, rewrittenNodePredicates, _, rewrittenSource) =
          solveSubqueryExpressionsForExtractedPredicates(
            source,
            nodePredicates,
            relationshipPredicates,
            Set.empty,
            context
          )
        annotate(
          VarExpand(
            source = rewrittenSource,
            from = from,
            dir = dir,
            projectedDir = projectedDir,
            types = patternRelationship.types,
            to = to,
            relName = patternRelationship.variable,
            length = l,
            mode = mode,
            nodePredicates = rewrittenNodePredicates.toSeq,
            relationshipPredicates = rewrittenRelationshipPredicates.toSeq
          ),
          solved,
          ProvidedOrder.Left,
          context
        )

      case _ => throw new InternalException("Expected a varlength path to be here")
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

  def planTrail(
    source: LogicalPlan,
    pattern: QuantifiedPathPattern,
    startBinding: NodeBinding,
    endBinding: NodeBinding,
    maybeHiddenFilter: Option[Expression],
    context: LogicalPlanningContext,
    innerPlan: LogicalPlan,
    predicates: Seq[Expression],
    previouslyBoundRelationships: Set[LogicalVariable],
    previouslyBoundRelationshipGroups: Set[LogicalVariable],
    reverseGroupVariableProjections: Boolean
  ): LogicalPlan = {
    // Ensure that innerPlan does conform with the pattern contained inside of the quantified path pattern before we mark it as solved
    try {
      VerifyBestPlan(innerPlan, SinglePlannerQuery.empty.withQueryGraph(pattern.asQueryGraph), context)
    } catch {
      case planVerificationException: InternalException => throw new InternalException(
          "The provided inner plan doesn't conform with the quantified path pattern being planned",
          planVerificationException
        )
    }

    val solved = solveds.get(source.id).asSinglePlannerQuery.amendQueryGraph(_
      .addQuantifiedPathPattern(pattern)
      .addPredicates(predicates: _*))

    val providedOrderRule = ProvidedOrder.Left
    val trailPlan = annotate(
      Trail(
        left = source,
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
        reverseGroupVariableProjections = reverseGroupVariableProjections
      ),
      solved,
      providedOrderRule,
      context
    )

    maybeHiddenFilter match {
      case Some(hiddenFilter) =>
        annotateSelection(
          Selection(Seq(hiddenFilter), trailPlan),
          solved,
          providedOrderRule,
          context
        )
      case None => trailPlan
    }
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
    val solved = RegularSinglePlannerQuery(queryGraph =
      QueryGraph.empty
        .addPatternNodes(variable)
        .addPredicates(solvedPredicates: _*)
        .addArgumentIds(argumentIds.toIndexedSeq)
    )
    val solver = SubqueryExpressionSolver.solverForLeafPlan(argumentIds, context)
    val rewrittenNodeIds = nodeIds.mapValues(solver.solve(_))
    val newArguments = solver.newArguments
    val leafPlan = annotate(
      makePlan(variable, rewrittenNodeIds, argumentIds ++ newArguments),
      solved,
      ProvidedOrder.empty,
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
    val solved = RegularSinglePlannerQuery(queryGraph =
      QueryGraph.empty
        .addPatternNodes(variable)
        .addPredicates(solvedPredicates: _*)
        .addHints(solvedHint)
        .addArgumentIds(argumentIds.toIndexedSeq)
    )
    annotate(
      NodeByLabelScan(variable, label, argumentIds, toIndexOrder(providedOrder)),
      solved,
      providedOrder,
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
    val solved = RegularSinglePlannerQuery(queryGraph =
      QueryGraph.empty
        .addPatternNodes(variable)
        .addPredicates(solvedPredicates: _*)
        .addHints(solvedHints)
        .addArgumentIds(argumentIds.toIndexedSeq)
    )
    annotate(
      UnionNodeByLabelsScan(variable, labels, argumentIds, toIndexOrder(providedOrder)),
      solved,
      providedOrder,
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
    val solved = RegularSinglePlannerQuery(queryGraph =
      QueryGraph.empty
        .addPatternNodes(variable)
        .addPredicates(solvedPredicates: _*)
        .addHints(solvedHints)
        .addArgumentIds(argumentIds.toIndexedSeq)
    )
    annotate(
      IntersectionNodeByLabelsScan(variable, labels, argumentIds, toIndexOrder(providedOrder)),
      solved,
      providedOrder,
      context
    )
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

    val solved = RegularSinglePlannerQuery(queryGraph = queryGraph)

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
    val annotatedPlan = annotate(plan, solved, providedOrder, context)

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
    val solved = RegularSinglePlannerQuery(queryGraph =
      QueryGraph.empty
        .addPatternNodes(variable)
        .addPredicates(solvedPredicates: _*)
        .addHints(solvedHint)
        .addArgumentIds(argumentIds.toIndexedSeq)
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
    val solved = RegularSinglePlannerQuery(queryGraph =
      QueryGraph.empty
        .addPatternNodes(variable)
        .addPredicates(solvedPredicates: _*)
        .addHints(solvedHint)
        .addArgumentIds(argumentIds.toIndexedSeq)
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
    val annotatedPlan = annotate(plan, solved, providedOrder, context)

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
    annotate(NodeHashJoin(nodes, left, right), solved, ProvidedOrder.Right, context)
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
      context
    )
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

    val solved = RegularSinglePlannerQuery(queryGraph = queryGraph)

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
    val annotatedPlan = annotate(plan, solved, providedOrder, context)

    solver.rewriteLeafPlan(annotatedPlan)

  }

  def planAssertSameNode(
    node: LogicalVariable,
    left: LogicalPlan,
    right: LogicalPlan,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved: SinglePlannerQuery =
      solveds.get(left.id).asSinglePlannerQuery ++ solveds.get(right.id).asSinglePlannerQuery
    annotate(AssertSameNode(node, left, right), solved, ProvidedOrder.Left, context)
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
      context
    )

  def planOptional(
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

    val solved = RegularSinglePlannerQuery(queryGraph =
      QueryGraph.empty
        .addOptionalMatch(optionalMatchQG)
        .withArgumentIds(ids)
    )

    annotate(Optional(inputPlan, ids), solved, ProvidedOrder.Left, context)
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
    annotate(plan, solved, providedOrder, context)
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
      context
    )
  }

  def planSelection(source: LogicalPlan, predicates: Seq[Expression], context: LogicalPlanningContext): LogicalPlan = {
    val solved =
      solveds.get(source.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addPredicates(predicates: _*)))
    val (rewrittenPredicates, rewrittenSource) =
      SubqueryExpressionSolver.ForMulti.solve(source, predicates, context)

    coercePredicatesWithAnds(rewrittenPredicates).fold(source) { coercedRewrittenPredicates =>
      annotateSelection(
        Selection(coercedRewrittenPredicates, rewrittenSource),
        solved,
        ProvidedOrder.Left,
        context
      )
    }
  }

  def planHorizonSelection(
    source: LogicalPlan,
    predicates: Seq[Expression],
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(source.id).asSinglePlannerQuery.updateTailOrSelf(_.updateHorizon {
      case p: QueryProjection => p.addPredicates(predicates: _*)
      case _ => throw new IllegalArgumentException("You can only plan HorizonSelection after a projection")
    })

    val (rewrittenPredicates, rewrittenSource) =
      if (
        context.settings.executionModel.providedOrderPreserving || interestingOrderConfig.orderToSolve.requiredOrderCandidate.isEmpty
      ) {
        // solve existential subquery predicates
        val (solvedPredicates, existsPlan) =
          SubqueryExpressionSolver.ForExistentialSubquery.solve(source, predicates, interestingOrderConfig, context)
        val unsolvedPredicates = predicates.filterNot(solvedPredicates.contains(_))

        // solve remaining predicates
        SubqueryExpressionSolver.ForMulti.solve(existsPlan, unsolvedPredicates, context)
      } else {
        // If the execution model does not preserve order and there is an ORDER BY, we are not allowed to use
        // NestedPlanExpressions here.
        val rewriter = irExpressionRewriter(source, context)
        val rewrittenPredicates = predicates.endoRewrite(rewriter)
        (rewrittenPredicates, source)
      }

    coercePredicatesWithAnds(rewrittenPredicates).fold(rewrittenSource) { coercedRewrittenPredicates =>
      annotateSelection(
        Selection(coercedRewrittenPredicates, rewrittenSource),
        solved,
        ProvidedOrder.Left,
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
    coercePredicatesWithAnds(predicates).fold(source) { coercedPredicates =>
      annotateSelection(
        Selection(coercedPredicates, source),
        solved,
        ProvidedOrder.Left,
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
      context
    )

  def planLetSemiApply(
    left: LogicalPlan,
    right: LogicalPlan,
    id: LogicalVariable,
    context: LogicalPlanningContext
  ): LogicalPlan =
    annotate(LetSemiApply(left, right, id), solveds.get(left.id), ProvidedOrder.Left, context)

  def planAntiSemiApply(
    left: LogicalPlan,
    right: LogicalPlan,
    expr: Expression,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addPredicates(expr)))
    annotate(AntiSemiApply(left, right), solved, ProvidedOrder.Left, context)
  }

  def planSemiApply(
    left: LogicalPlan,
    right: LogicalPlan,
    expr: Expression,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addPredicates(expr)))
    annotate(SemiApply(left, right), solved, ProvidedOrder.Left, context)
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
    annotate(SemiApply(left, right), solved, ProvidedOrder.Left, context)
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
    annotate(AntiSemiApply(left, right), solved, ProvidedOrder.Left, context)
  }

  def planQueryArgument(queryGraph: QueryGraph, context: LogicalPlanningContext): LogicalPlan = {
    val patternNodes = queryGraph.argumentIds intersect queryGraph.patternNodes
    val patternRels =
      queryGraph.patternRelationships
        .filter(rel => queryGraph.argumentIds.contains(rel.variable))
        .map(_.variable)
    val otherIds = queryGraph.argumentIds -- patternNodes
    planArgument(patternNodes, patternRels, otherIds, context)
  }

  def planArgument(
    patternNodes: Set[LogicalVariable],
    patternRels: Set[LogicalVariable] = Set.empty,
    other: Set[LogicalVariable] = Set.empty,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val coveredIds = patternNodes ++ patternRels ++ other

    val solved = RegularSinglePlannerQuery(queryGraph =
      QueryGraph(
        argumentIds = coveredIds,
        patternNodes = patternNodes,
        patternRelationships = Set.empty
      )
    )

    annotate(Argument(coveredIds), solved, ProvidedOrder.empty, context)
  }

  def planArgument(context: LogicalPlanningContext): LogicalPlan =
    annotate(Argument(Set.empty), SinglePlannerQuery.empty, ProvidedOrder.empty, context)

  def planEmptyProjection(inner: LogicalPlan, context: LogicalPlanningContext): LogicalPlan =
    annotate(EmptyResult(inner), solveds.get(inner.id), ProvidedOrder.empty, context)

  def planStarProjection(inner: LogicalPlan, reported: Option[Map[LogicalVariable, Expression]]): LogicalPlan = {
    reported.fold(inner) { reported =>
      val newSolved: SinglePlannerQuery = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(
        _.updateQueryProjection(_.withAddedProjections(reported))
      )
      // Keep some attributes, but change solved
      val keptAttributes = Attributes(idGen, cardinalities, providedOrders, leveragedOrders, labelAndRelTypeInfos)
      val newPlan = inner.copyPlanWithIdGen(keptAttributes.copy(inner.id))
      solveds.set(newPlan.id, newSolved)
      newPlan
    }
  }

  /**
   * @param expressions must be solved by the ListSubqueryExpressionSolver. This is not done here since that can influence the projection list,
   *                    thus this logic is put into [[projection]] instead.
   */
  def planRegularProjection(
    inner: LogicalPlan,
    expressions: Map[LogicalVariable, Expression],
    reported: Option[Map[LogicalVariable, Expression]],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val innerSolved: SinglePlannerQuery = solveds.get(inner.id).asSinglePlannerQuery
    val solved = reported.fold(innerSolved) { reported =>
      innerSolved.updateTailOrSelf(_.updateQueryProjection(_.withAddedProjections(reported)))
    }

    planRegularProjectionHelper(inner, expressions, context, solved)
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
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(
      AggregatingQueryProjection(groupingExpressions = reportedGrouping, aggregationExpressions = reportedAggregation)
    ))

    // NOTE: aggregation order is not used here as it is lost after aggregation
    val trimmedAndRenamed = trimAndRenameProvidedOrder(providedOrders.get(left.id), grouping)

    val agg = Aggregation(left, grouping, aggregation)
    val plan = annotate(
      agg,
      solved,
      context.providedOrderFactory.providedOrder(trimmedAndRenamed, ProvidedOrder.Left, Some(agg)),
      context
    )

    def hasCollectOrUDF = aggregation.values.exists {
      case fi: FunctionInvocation => fi.function == Collect || fi.function == UnresolvedFunction
      case _                      => false
    }
    // Aggregation functions may leverage the order of a preceding ORDER BY, if no other clause is inbetween.
    // In practice, this is only collect and potentially user defined aggregations
    if (previousInterestingOrder.exists(_.requiredOrderCandidate.nonEmpty) && hasCollectOrUDF) {
      markOrderAsLeveragedBackwardsUntilOrigin(plan, context.providedOrderFactory)
    }

    plan
  }

  def planOrderedAggregation(
    left: LogicalPlan,
    grouping: Map[LogicalVariable, Expression],
    aggregation: Map[LogicalVariable, Expression],
    orderToLeverage: Seq[Expression],
    reportedGrouping: Map[LogicalVariable, Expression],
    reportedAggregation: Map[LogicalVariable, Expression],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(
      AggregatingQueryProjection(groupingExpressions = reportedGrouping, aggregationExpressions = reportedAggregation)
    ))

    // NOTE: aggregation order is not used here as it is lost after aggregation
    val trimmedAndRenamed = trimAndRenameProvidedOrder(providedOrders.get(left.id), grouping)

    val agg = OrderedAggregation(left, grouping, aggregation, orderToLeverage)
    val plan = annotate(
      agg,
      solved,
      context.providedOrderFactory.providedOrder(trimmedAndRenamed, ProvidedOrder.Left, Some(agg)),
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
    val keptAttributes = Attributes(idGen, cardinalities, leveragedOrders, labelAndRelTypeInfos)
    val newPlan = inner.copyPlanWithIdGen(keptAttributes.copy(inner.id))
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.withInterestingOrder(interestingOrder))
    val providedOrder = providedOrders.get(inner.id)
    annotate(newPlan, solved, providedOrder, context)
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
    val plan = annotate(Skip(inner, count), solved, ProvidedOrder.Left, context)
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
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(LoadCSVProjection(
      variable,
      url,
      format,
      fieldTerminator
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
      context
    )
  }

  def planInput(symbols: Seq[Variable], context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(queryInput = Some(symbols))
    annotate(Input(symbols.map(_.name)), solved, ProvidedOrder.empty, context)
  }

  def planUnwind(
    inner: LogicalPlan,
    variable: LogicalVariable,
    expression: Expression,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(UnwindProjection(variable, expression)))
    val (rewrittenExpression, rewrittenInner) = SubqueryExpressionSolver.ForSingle.solve(inner, expression, context)
    annotate(
      UnwindCollection(rewrittenInner, variable, rewrittenExpression),
      solved,
      ProvidedOrder.Left,
      context
    )
  }

  def planProcedureCall(inner: LogicalPlan, call: ResolvedCall, context: LogicalPlanningContext): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(ProcedureCallProjection(call)))
    val solver = SubqueryExpressionSolver.solverFor(inner, context)
    val rewrittenCall = call.mapCallArguments(solver.solve(_))
    val rewrittenInner = solver.rewrittenPlan()

    if (call.containsNoUpdates)
      annotate(ProcedureCall(rewrittenInner, rewrittenCall), solved, ProvidedOrder.Left, context)
    else
      annotate(ProcedureCall(rewrittenInner, rewrittenCall), solved, ProvidedOrder.empty, context)
  }

  def planCommand(inner: LogicalPlan, clause: CommandClause, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(CommandProjection(clause)))

    val plan = clause match {
      case s: ShowIndexesClause =>
        ShowIndexes(
          s.indexType,
          s.unfilteredColumns.columns,
          s.yieldItems,
          s.yieldAll
        )
      case s: ShowConstraintsClause =>
        ShowConstraints(
          s.constraintType,
          s.unfilteredColumns.columns,
          s.yieldItems,
          s.yieldAll
        )
      case s: ShowProceduresClause =>
        ShowProcedures(
          s.executable,
          s.unfilteredColumns.columns,
          s.yieldItems,
          s.yieldAll
        )
      case s: ShowFunctionsClause =>
        ShowFunctions(
          s.functionType,
          s.executable,
          s.unfilteredColumns.columns,
          s.yieldItems,
          s.yieldAll
        )
      case s: ShowTransactionsClause =>
        ShowTransactions(
          s.names,
          s.unfilteredColumns.columns,
          s.yieldItems,
          s.yieldAll
        )
      case s: TerminateTransactionsClause =>
        TerminateTransactions(s.names, s.unfilteredColumns.columns, s.yieldItems, s.yieldAll)
      case s: ShowSettingsClause =>
        ShowSettings(s.names, s.unfilteredColumns.columns, s.yieldItems, s.yieldAll)
    }
    val annotatedPlan = annotate(plan, solved, ProvidedOrder.empty, context)

    val apply = Apply(inner, annotatedPlan)
    annotate(apply, solved, ProvidedOrder.empty, context)
  }

  def planPassAll(inner: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(PassthroughAllHorizon()))
    // Keep some attributes, but change solved
    val keptAttributes = Attributes(idGen, cardinalities, leveragedOrders, providedOrders, labelAndRelTypeInfos)
    val newPlan = inner.copyPlanWithIdGen(keptAttributes.copy(inner.id))
    annotate(newPlan, solved, providedOrders.get(inner.id), context)
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
    val plan = annotate(Limit(inner, effectiveCount), solved, ProvidedOrder.Left, context)
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
    val plan = annotate(ExhaustiveLimit(inner, effectiveCount), solved, ProvidedOrder.Left, context)
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
      context.plannerState.indexCompatiblePredicatesProviderContext
    )
    val limitCardinality = cardinalityModel(
      solvedSkipAndLimit,
      context.plannerState.input.labelInfo,
      context.plannerState.input.relTypeInfo,
      context.semanticTable,
      context.plannerState.indexCompatiblePredicatesProviderContext
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
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(
      AggregatingQueryProjection(groupingExpressions = reportedGrouping, aggregationExpressions = reportedAggregation)
    ).withInterestingOrder(interestingOrder))
    val providedOrderRule = ProvidedOrder.Left
    val limitPlan = planLimitOnTopOf(inner, SignedDecimalIntegerLiteral("1")(InputPosition.NONE))
    val annotatedLimitPlan = annotate(limitPlan, solved, providedOrderRule, context)

    // The limit leverages the order, not the following optional
    markOrderAsLeveragedBackwardsUntilOrigin(annotatedLimitPlan, context.providedOrderFactory)

    val plan = Optional(annotatedLimitPlan)
    annotate(plan, solved, providedOrderRule, context)
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

    val (predicateWithIrExpressionReferencingPath, otherPathPredicates) = shortestRelationship.maybePathVar match {
      case Some(pathVariable) => pathPredicates.partition(_.folder.treeExists {
          case ire: IRExpression => ire.dependencies.contains(pathVariable)
        })
      case None => (Set.empty[Expression], pathPredicates)
    }

    val rewrittenPredicatesWithIrExpressionReferencingPath =
      predicateWithIrExpressionReferencingPath.endoRewrite(irExpressionRewriter(inner, context))

    val solved = solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(
      _.addShortestRelationship(shortestRelationship).addPredicates(solvedPredicates.toSeq: _*)
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
        if (disallowSameNode) DisallowSameNode else SkipSameNode
      ),
      solved,
      ProvidedOrder.Left,
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
    pathLength: PathLength
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
      LengthBounds(pathLength.min, pathLength.maybeMax)
    )
    annotate(plan, solved, ProvidedOrder.Left, context)
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
      context
    )
  }

  def planProjectionForUnionMapping(
    inner: LogicalPlan,
    expressions: Map[LogicalVariable, Expression],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    annotate(
      Projection(inner, expressions),
      solveds.get(inner.id),
      ProvidedOrder.Left,
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
    annotate(plan, solved, ProvidedOrder.empty, context)
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

    val plan = annotate(union, solved, providedOrder, context)
    markOrderAsLeveragedBackwardsUntilOrigin(plan, context.providedOrderFactory)
    plan
  }

  def planDistinctForUnion(left: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val returnAll = left.availableSymbols.map { s => s -> s }

    val solved = solveds.get(left.id) match {
      case u: UnionQuery => markDistinctInUnion(u)
      case _ => throw new IllegalStateException("Planning a distinct for union, but no union was planned before.")
    }
    if (returnAll.isEmpty) {
      annotate(left.copyPlanWithIdGen(idGen), solved, ProvidedOrder.Left, context)
    } else {
      annotate(Distinct(left, returnAll.toMap), solved, ProvidedOrder.Left, context)
    }
  }

  def planOrderedDistinctForUnion(
    left: LogicalPlan,
    orderToLeverage: Seq[Expression],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val returnAll = left.availableSymbols.map { s => s -> s }

    val solved = solveds.get(left.id) match {
      case u: UnionQuery => markDistinctInUnion(u)
      case _ => throw new IllegalStateException("Planning a distinct for or union, but no union was planned before.")
    }
    if (returnAll.isEmpty) {
      annotate(left.copyPlanWithIdGen(idGen), solved, ProvidedOrder.Left, context)
    } else {
      val plan = annotate(
        OrderedDistinct(left, returnAll.toMap, orderToLeverage),
        solved,
        ProvidedOrder.Left,
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
        DistinctQueryProjection(reported)
      ))
    val columnsWithRenames = renameProvidedOrderColumns(providedOrders.get(left.id).columns, expressions)
    val plan = Distinct(left, expressions)
    val providedOrder = context.providedOrderFactory.providedOrder(columnsWithRenames, ProvidedOrder.Left, Some(plan))
    annotate(
      plan,
      solved,
      providedOrder,
      context
    )
  }

  /**
   * Keep the left plan, but mark DISTINCT as solved.
   * Used when DISTINCT is used but we can determine it is not really needed.
   */
  def planEmptyDistinct(
    left: LogicalPlan,
    reported: Map[LogicalVariable, Expression],
    context: LogicalPlanningContext
  ): LogicalPlan = {

    val solved: SinglePlannerQuery =
      solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.updateQueryProjection(_ =>
        DistinctQueryProjection(reported)
      ))

    val cardinality = cardinalityModel(
      solved,
      context.plannerState.input.labelInfo,
      context.plannerState.input.relTypeInfo,
      context.semanticTable,
      context.plannerState.indexCompatiblePredicatesProviderContext
    )
    // Change solved and cardinality
    val keptAttributes = Attributes(idGen, providedOrders, leveragedOrders, labelAndRelTypeInfos)
    val newPlan = left.copyPlanWithIdGen(keptAttributes.copy(left.id))
    solveds.set(newPlan.id, solved)
    cardinalities.set(newPlan.id, cardinality)
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
        DistinctQueryProjection(reported)
      ))

    planRegularProjectionHelper(left, expressions, context, solved)
  }

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
        DistinctQueryProjection(reported)
      ))
    val columnsWithRenames = renameProvidedOrderColumns(providedOrders.get(left.id).columns, expressions)
    val distinct = OrderedDistinct(left, expressions, orderToLeverage)
    val providedOrder =
      context.providedOrderFactory.providedOrder(columnsWithRenames, ProvidedOrder.Left, Some(distinct))
    val plan = annotate(
      distinct,
      solved,
      providedOrder,
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
      case q => throw new IllegalStateException(s"Expected orPlan to solve a UnionQuery, got: $q")
    }
    val cardinality = cardinalityModel(
      solved,
      context.plannerState.input.labelInfo,
      context.plannerState.input.relTypeInfo,
      context.semanticTable,
      context.plannerState.indexCompatiblePredicatesProviderContext
    )
    // Change solved and cardinality
    val keptAttributes = Attributes(idGen, providedOrders, leveragedOrders, labelAndRelTypeInfos)
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
      context
    )
  }

  def planCreate(inner: LogicalPlan, pattern: CreatePattern, context: LogicalPlanningContext): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addMutatingPatterns(pattern)))
    val (rewrittenPattern: CreatePattern, rewrittenInner) =
      SubqueryExpressionSolver.ForMappable().solve(inner, pattern, context)
    val plan = plans.Create(rewrittenInner, rewrittenPattern.commands)
    val providedOrder =
      providedOrderOfUpdate(plan, rewrittenInner, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, context)
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

    val patterns =
      if (createRelationshipPatterns.isEmpty) {
        MergeNodePattern(
          createNodePatterns.head,
          solveds(inner.id).asSinglePlannerQuery.queryGraph,
          onCreatePatterns,
          onMatchPatterns
        )
      } else {
        MergeRelationshipPattern(
          createNodePatterns,
          createRelationshipPatterns,
          solveds(inner.id).asSinglePlannerQuery.queryGraph,
          onCreatePatterns,
          onMatchPatterns
        )
      }
    val rewrittenNodePatterns = createNodePatterns.endoRewrite(rewriter)
    val rewrittenRelPatterns = createRelationshipPatterns.endoRewrite(rewriter)

    val solved = RegularSinglePlannerQuery().amendQueryGraph(_.addMutatingPatterns(patterns))
    val merge =
      Merge(
        inner,
        rewrittenNodePatterns,
        rewrittenRelPatterns,
        onMatchPatterns,
        onCreatePatterns,
        nodesToLock
      )
    val providedOrder =
      providedOrderOfUpdate(merge, inner, context.settings.executionModel, context.providedOrderFactory)
    annotate(merge, solved, providedOrder, context)
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
    annotate(plan, solved, providedOrder, context)
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
    annotate(plan, solved, providedOrder, context)
  }

  def planDeleteNode(inner: LogicalPlan, delete: DeleteExpression, context: LogicalPlanningContext): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addMutatingPatterns(delete)))
    val (rewrittenDelete, rewrittenInner) = SubqueryExpressionSolver.ForMappable().solve(inner, delete, context)
    val plan =
      if (delete.detachDelete) {
        DetachDeleteNode(rewrittenInner, rewrittenDelete.expression)
      } else {
        DeleteNode(rewrittenInner, rewrittenDelete.expression)
      }
    val providedOrder =
      providedOrderOfUpdate(plan, inner, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, context)
  }

  def planDeleteRelationship(
    inner: LogicalPlan,
    delete: DeleteExpression,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addMutatingPatterns(delete)))
    val (rewrittenDelete, rewrittenInner) = SubqueryExpressionSolver.ForMappable().solve(inner, delete, context)
    val plan = DeleteRelationship(rewrittenInner, rewrittenDelete.expression)
    val providedOrder =
      providedOrderOfUpdate(plan, inner, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, context)
  }

  def planDeletePath(inner: LogicalPlan, delete: DeleteExpression, context: LogicalPlanningContext): LogicalPlan = {
    // `delete.expression` can only be a PathExpression, ListSubqueryExpressionSolver not needed
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addMutatingPatterns(delete)))

    val plan =
      if (delete.detachDelete) {
        DetachDeletePath(inner, delete.expression)
      } else {
        DeletePath(inner, delete.expression)
      }
    val providedOrder =
      providedOrderOfUpdate(plan, inner, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, context)
  }

  def planDeleteExpression(
    inner: LogicalPlan,
    delete: DeleteExpression,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addMutatingPatterns(delete)))
    val (rewrittenDelete, rewrittenInner) = SubqueryExpressionSolver.ForMappable().solve(inner, delete, context)
    val plan =
      if (delete.detachDelete) {
        DetachDeleteExpression(rewrittenInner, rewrittenDelete.expression)
      } else {
        plans.DeleteExpression(rewrittenInner, rewrittenDelete.expression)
      }
    val providedOrder =
      providedOrderOfUpdate(plan, inner, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, context)
  }

  def planSetLabel(inner: LogicalPlan, pattern: SetLabelPattern, context: LogicalPlanningContext): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addMutatingPatterns(pattern)))
    val plan = SetLabels(inner, pattern.variable, pattern.labels.toSet)
    val providedOrder =
      providedOrderOfUpdate(plan, inner, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, context)
  }

  def planSetNodeProperty(
    inner: LogicalPlan,
    pattern: SetNodePropertyPattern,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addMutatingPatterns(pattern)))

    // SET has currently row-by-row visibility. This could change in a major release.
    // To maintain the visibility, even with subqueries, we must use NestedPlanExpressions.
    val rewriter = irExpressionRewriter(inner, context)
    val rewrittenPattern = pattern.endoRewrite(rewriter)

    val plan = SetNodeProperty(
      inner,
      rewrittenPattern.variable,
      rewrittenPattern.propertyKey,
      rewrittenPattern.expression
    )
    val providedOrder =
      providedOrderOfUpdate(plan, inner, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, context)
  }

  def planSetNodeProperties(
    inner: LogicalPlan,
    pattern: SetNodePropertiesPattern,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addMutatingPatterns(pattern)))

    // SET has currently row-by-row visibility. This could change in a major release.
    // To maintain the visibility, even with subqueries, we must use NestedPlanExpressions.
    val rewriter = irExpressionRewriter(inner, context)
    val rewrittenPattern = pattern.endoRewrite(rewriter)

    val plan = SetNodeProperties(inner, rewrittenPattern.variable, rewrittenPattern.items)
    val providedOrder =
      providedOrderOfUpdate(plan, inner, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, context)
  }

  def planSetNodePropertiesFromMap(
    inner: LogicalPlan,
    pattern: SetNodePropertiesFromMapPattern,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addMutatingPatterns(pattern)))

    // SET has currently row-by-row visibility. This could change in a major release.
    // To maintain the visibility, even with subqueries, we must use NestedPlanExpressions.
    val rewriter = irExpressionRewriter(inner, context)
    val rewrittenPattern = pattern.endoRewrite(rewriter)

    val plan = SetNodePropertiesFromMap(
      inner,
      rewrittenPattern.variable,
      rewrittenPattern.expression,
      rewrittenPattern.removeOtherProps
    )
    val providedOrder =
      providedOrderOfUpdate(plan, inner, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, context)
  }

  def planSetRelationshipProperty(
    inner: LogicalPlan,
    pattern: SetRelationshipPropertyPattern,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addMutatingPatterns(pattern)))

    // SET has currently row-by-row visibility. This could change in a major release.
    // To maintain the visibility, even with subqueries, we must use NestedPlanExpressions.
    val rewriter = irExpressionRewriter(inner, context)
    val rewrittenPattern = pattern.endoRewrite(rewriter)

    val plan = SetRelationshipProperty(
      inner,
      rewrittenPattern.variable,
      rewrittenPattern.propertyKey,
      rewrittenPattern.expression
    )
    val providedOrder =
      providedOrderOfUpdate(plan, inner, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, context)
  }

  def planSetRelationshipProperties(
    inner: LogicalPlan,
    pattern: SetRelationshipPropertiesPattern,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addMutatingPatterns(pattern)))

    // SET has currently row-by-row visibility. This could change in a major release.
    // To maintain the visibility, even with subqueries, we must use NestedPlanExpressions.
    val rewriter = irExpressionRewriter(inner, context)
    val rewrittenPattern = pattern.endoRewrite(rewriter)

    val plan = SetRelationshipProperties(inner, rewrittenPattern.variable, rewrittenPattern.items)
    val providedOrder =
      providedOrderOfUpdate(plan, inner, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, context)
  }

  def planSetRelationshipPropertiesFromMap(
    inner: LogicalPlan,
    pattern: SetRelationshipPropertiesFromMapPattern,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addMutatingPatterns(pattern)))

    // SET has currently row-by-row visibility. This could change in a major release.
    // To maintain the visibility, even with subqueries, we must use NestedPlanExpressions.
    val rewriter = irExpressionRewriter(inner, context)
    val rewrittenPattern = pattern.endoRewrite(rewriter)

    val plan = SetRelationshipPropertiesFromMap(
      inner,
      rewrittenPattern.variable,
      rewrittenPattern.expression,
      rewrittenPattern.removeOtherProps
    )
    val providedOrder =
      providedOrderOfUpdate(plan, inner, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, context)
  }

  def planSetPropertiesFromMap(
    inner: LogicalPlan,
    pattern: SetPropertiesFromMapPattern,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addMutatingPatterns(pattern)))

    // SET has currently row-by-row visibility. This could change in a major release.
    // To maintain the visibility, even with subqueries, we must use NestedPlanExpressions.
    val rewriter = irExpressionRewriter(inner, context)
    val rewrittenPattern = pattern.endoRewrite(rewriter)

    val plan = SetPropertiesFromMap(
      inner,
      rewrittenPattern.entityExpression,
      rewrittenPattern.expression,
      rewrittenPattern.removeOtherProps
    )
    val providedOrder =
      providedOrderOfUpdate(plan, inner, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, context)
  }

  def planSetProperty(inner: LogicalPlan, pattern: SetPropertyPattern, context: LogicalPlanningContext): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addMutatingPatterns(pattern)))

    // SET has currently row-by-row visibility. This could change in a major release.
    // To maintain the visibility, even with subqueries, we must use NestedPlanExpressions.
    val rewriter = irExpressionRewriter(inner, context)
    val rewrittenPattern = pattern.endoRewrite(rewriter)

    val plan = SetProperty(
      inner,
      rewrittenPattern.entityExpression,
      rewrittenPattern.propertyKeyName,
      rewrittenPattern.expression
    )
    val providedOrder =
      providedOrderOfUpdate(plan, inner, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, context)
  }

  def planSetProperties(
    inner: LogicalPlan,
    pattern: SetPropertiesPattern,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addMutatingPatterns(pattern)))

    // SET has currently row-by-row visibility. This could change in a major release.
    // To maintain the visibility, even with subqueries, we must use NestedPlanExpressions.
    val rewriter = irExpressionRewriter(inner, context)
    val rewrittenPattern = pattern.endoRewrite(rewriter)

    val plan = SetProperties(inner, rewrittenPattern.entityExpression, rewrittenPattern.items)
    val providedOrder =
      providedOrderOfUpdate(plan, inner, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, context)
  }

  def planSetDynamicProperty(
    inner: LogicalPlan,
    pattern: SetDynamicPropertyPattern,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addMutatingPatterns(pattern)))

    // SET has currently row-by-row visibility. This could change in a major release.
    // To maintain the visibility, even with subqueries, we must use NestedPlanExpressions.
    val rewriter = irExpressionRewriter(inner, context)
    val rewrittenPattern = pattern.endoRewrite(rewriter)

    val plan = SetDynamicProperty(
      inner,
      rewrittenPattern.entity,
      rewrittenPattern.property,
      rewrittenPattern.expression
    )
    val providedOrder =
      providedOrderOfUpdate(plan, inner, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, context)
  }

  def planRemoveLabel(inner: LogicalPlan, pattern: RemoveLabelPattern, context: LogicalPlanningContext): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addMutatingPatterns(pattern)))
    val plan = RemoveLabels(inner, pattern.variable, pattern.labels.toSet)
    val providedOrder =
      providedOrderOfUpdate(plan, inner, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, context)
  }

  def planForeachApply(
    left: LogicalPlan,
    innerUpdates: LogicalPlan,
    pattern: ForeachPattern,
    context: LogicalPlanningContext,
    expression: Expression
  ): LogicalPlan = {
    val solved =
      solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addMutatingPatterns(pattern)))
    val (rewrittenExpression, rewrittenLeft) = SubqueryExpressionSolver.ForSingle.solve(left, expression, context)
    val plan = ForeachApply(rewrittenLeft, innerUpdates, pattern.variable, rewrittenExpression)
    val providedOrder = providedOrderOfApply(
      rewrittenLeft,
      innerUpdates,
      plan,
      context.settings.executionModel,
      context.providedOrderFactory
    )
    annotate(plan, solved, providedOrder, context)
  }

  def planForeach(
    inner: LogicalPlan,
    pattern: ForeachPattern,
    context: LogicalPlanningContext,
    expression: Expression,
    mutations: collection.Seq[SimpleMutatingPattern]
  ): LogicalPlan = {
    val solved =
      solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addMutatingPatterns(pattern)))
    val (rewrittenExpression, rewrittenLeft) = SubqueryExpressionSolver.ForSingle.solve(inner, expression, context)
    val plan = Foreach(
      rewrittenLeft,
      pattern.variable,
      rewrittenExpression,
      mutations
    )
    val providedOrder =
      providedOrderOfUpdate(plan, inner, context.settings.executionModel, context.providedOrderFactory)
    annotate(plan, solved, providedOrder, context)
  }

  def planEager(
    inner: LogicalPlan,
    context: LogicalPlanningContext,
    reasons: ListSet[EagernessReason]
  ): LogicalPlan =
    annotate(Eager(inner, reasons), solveds.get(inner.id), ProvidedOrder.Left, context)

  def planError(
    inner: LogicalPlan,
    exception: ExhaustiveShortestPathForbiddenException,
    context: LogicalPlanningContext
  ): LogicalPlan =
    annotate(ErrorPlan(inner, exception), solveds.get(inner.id), ProvidedOrder.Left, context)

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
    val horizon = RunQueryAtProjection(graphReference, queryString, parameters, importsAsParameters, columns)
    val solved =
      solveds
        .get(inner.id)
        .asSinglePlannerQuery
        .updateTailOrSelf(_.withHorizon(horizon))
    val runQueryAt = RunQueryAt(inner, queryString, graphReference, parameters, importsAsParameters, columns)
    annotate(runQueryAt, solved, ProvidedOrder.empty, context)
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
      annotate(plan.copyPlanWithIdGen(idGen), newSolved, providedOrder, context)
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
      (plan.lhs, plan.rhs, providedOrder.orderOrigin) match {
        case (Some(left), Some(right), Some(ProvidedOrder.Left))
          if invalidatesProvidedOrderRecursive(right, executionModel) =>
          val msg =
            s"LHS claims to provide an order, but RHS contains clauses that invalidates this order.\nProvided order: $providedOrder\nLHS: $left\nRHS: $right"
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
      // definition is ordered. However if you do ON MATCH SET ... that might invalidate the
      // inner ordering.
      case m: Merge => m.onMatch.nonEmpty
      case _        => plan.isUpdatingPlan
    }) || executionModel.invalidatesProvidedOrder(plan)
  }

  private def invalidatesProvidedOrderRecursive(plan: LogicalPlan, executionModel: ExecutionModel): Boolean =
    plan.folder.treeExists { case plan: LogicalPlan if invalidatesProvidedOrder(plan, executionModel) => true }

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
        context.plannerState.indexCompatiblePredicatesProviderContext
      )
    solveds.set(plan.id, solved)
    cardinalities.set(plan.id, cardinality)
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      providedOrder.isEmpty || Set(plan.lhs, plan.lhs).flatten.forall(p => providedOrders.get(p.id) ne providedOrder),
      s"A plan must not use the same provided order instance as one of its children. Make sure to use the ProvidedOrderFactory."
    )
    providedOrders.set(plan.id, providedOrder)
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
    annotate(plan, solved, providedOrder, context)
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
    context: LogicalPlanningContext
  ): Selection = {
    labelAndRelTypeInfos.set(
      selection.id,
      Some(LabelAndRelTypeInfo(context.plannerState.input.labelInfo, context.plannerState.input.relTypeInfo))
    )

    annotate(selection, solved, providedOrderPropagationRule, context)
  }

  /**
   * There probably exists some type level way of achieving this with type safety instead of manually searching through the expression tree like this
   */
  private def assertNoBadExpressionsExists(root: Any): Unit = {
    checkOnlyWhenAssertionsAreEnabled(!root.folder.treeExists {
      case _: PatternComprehension | _: PatternExpression | _: IRExpression | _: MapProjection =>
        throw new InternalException(s"This expression should not be added to a logical plan:\n$root")
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
    solved: SinglePlannerQuery
  ): Projection = {
    val columnsWithRenames = renameProvidedOrderColumns(providedOrders.get(inner.id).columns, expressions)
    val plan = Projection(inner, expressions)
    val providedOrder = context.providedOrderFactory.providedOrder(columnsWithRenames, ProvidedOrder.Left, Some(plan))
    annotate(
      plan,
      solved,
      providedOrder,
      context
    )
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
                `v`,
                _,
                PropertyKeyName(`propName`),
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
          case CachedProperty(`v`, _, PropertyKeyName(`propName`), _, _)    => true
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
        AssertMacros.checkOnlyWhenAssertionsAreEnabled(
          lp.rhs.isEmpty,
          "We assume that there is no two-child plan leveraging but destroying ordering."
        )
    }
  }
}

object LogicalPlanProducer {

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
}
