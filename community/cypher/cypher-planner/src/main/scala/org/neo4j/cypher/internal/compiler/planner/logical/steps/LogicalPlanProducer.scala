/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.Union.UnionMapping
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.ast.UsingScanHint
import org.neo4j.cypher.internal.compiler.helpers.ListSupport
import org.neo4j.cypher.internal.compiler.helpers.PredicateHelper.coercePredicatesWithAnds
import org.neo4j.cypher.internal.compiler.planner.ProcedureCallProjection
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.ExistsSubClause
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.MapProjection
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Collect
import org.neo4j.cypher.internal.expressions.functions.UnresolvedFunction
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.CSVFormat
import org.neo4j.cypher.internal.ir.CallSubqueryHorizon
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreatePattern
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.ir.DeleteExpression
import org.neo4j.cypher.internal.ir.DistinctQueryProjection
import org.neo4j.cypher.internal.ir.ForeachPattern
import org.neo4j.cypher.internal.ir.LoadCSVProjection
import org.neo4j.cypher.internal.ir.PassthroughAllHorizon
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.PlannerQueryPart
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.RemoveLabelPattern
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SetLabelPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetNodePropertyPattern
import org.neo4j.cypher.internal.ir.SetPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetPropertyPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertyPattern
import org.neo4j.cypher.internal.ir.ShortestPathPattern
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.UnionQuery
import org.neo4j.cypher.internal.ir.UnwindProjection
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.ColumnOrder
import org.neo4j.cypher.internal.logical.plans.ConditionalApply
import org.neo4j.cypher.internal.logical.plans.DeleteNode
import org.neo4j.cypher.internal.logical.plans.DeletePath
import org.neo4j.cypher.internal.logical.plans.DeleteRelationship
import org.neo4j.cypher.internal.logical.plans.DetachDeleteExpression
import org.neo4j.cypher.internal.logical.plans.DetachDeleteNode
import org.neo4j.cypher.internal.logical.plans.DetachDeletePath
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.DoNotIncludeTies
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.EmptyResult
import org.neo4j.cypher.internal.logical.plans.ErrorPlan
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ExpansionMode
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.Input
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LetAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSemiApply
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LoadCSV
import org.neo4j.cypher.internal.logical.plans.LockNodes
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.MergeCreateNode
import org.neo4j.cypher.internal.logical.plans.MergeCreateRelationship
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
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.RelationshipCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.SeekableArgs
import org.neo4j.cypher.internal.logical.plans.SelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.SelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SemiApply
import org.neo4j.cypher.internal.logical.plans.SetLabels
import org.neo4j.cypher.internal.logical.plans.SetNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetNodeProperty
import org.neo4j.cypher.internal.logical.plans.SetPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetProperty
import org.neo4j.cypher.internal.logical.plans.SetRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperty
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.Ties
import org.neo4j.cypher.internal.logical.plans.TriadicSelection
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.logical.plans.VariablePredicate
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.exceptions.ExhaustiveShortestPathForbiddenException
import org.neo4j.exceptions.InternalException

import scala.annotation.tailrec

/*
 * The responsibility of this class is to produce the correct solved PlannerQuery when creating logical plans.
 * No other functionality or logic should live here - this is supposed to be a very simple class that does not need
 * much testing
 */
case class LogicalPlanProducer(cardinalityModel: CardinalityModel, planningAttributes: PlanningAttributes, idGen : IdGen) extends ListSupport {

  implicit val implicitIdGen: IdGen = idGen
  private val solveds = planningAttributes.solveds
  private val cardinalities = planningAttributes.cardinalities
  private val providedOrders = planningAttributes.providedOrders
  private val leveragedOrders = planningAttributes.leveragedOrders

  /**
   * This object is simply to group methods that are used by the pattern expression solver, and thus do not need to update `solveds`
   */
  object ForPatternExpressionSolver {

    def planArgument(argumentIds: Set[String],
                     context: LogicalPlanningContext): LogicalPlan = {
      annotate(Argument(argumentIds), SinglePlannerQuery.empty, ProvidedOrder.empty, context)
    }

    def planApply(left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
      // If the LHS has duplicate values, we cannot guarantee any added order from the RHS
      val providedOrder = providedOrders.get(left.id).fromLeft
      // The RHS is the leaf plan we are wrapping under an apply in order to solve the pattern expression.
      // It has the correct solved
      val solved = solveds.get(right.id)
      annotate(Apply(left, right), solved, providedOrder, context)
    }

    def planRollup(lhs: LogicalPlan,
                   rhs: LogicalPlan,
                   collectionName: String,
                   variableToCollect: String,
                   nullable: Set[String],
                   context: LogicalPlanningContext): LogicalPlan = {
      // The LHS is either the plan we're building on top of, with the correct solved or it is the result of [[planArgument]].
      // The RHS is the sub-query
      val solved = solveds.get(lhs.id)
      annotate(RollUpApply(lhs, rhs, collectionName, variableToCollect, nullable), solved, providedOrders.get(lhs.id).fromLeft, context)
    }
  }

  def planLock(plan: LogicalPlan, nodesToLock: Set[String], context: LogicalPlanningContext): LogicalPlan =
    annotate(LockNodes(plan, nodesToLock), solveds.get(plan.id), providedOrders.get(plan.id).fromLeft, context)

  def solvePredicate(plan: LogicalPlan, solvedExpression: Expression, context: LogicalPlanningContext): LogicalPlan = {
    // Keep other attributes but change solved
    val keptAttributes = Attributes(idGen, cardinalities, providedOrders, leveragedOrders)
    val newPlan = plan.copyPlanWithIdGen(keptAttributes.copy(plan.id))
    val solvedPlannerQuery = solveds.get(plan.id).asSinglePlannerQuery.amendQueryGraph(_.addPredicates(solvedExpression))
    solveds.set(newPlan.id, solvedPlannerQuery)
    newPlan
  }

  def planAllNodesScan(idName: String, argumentIds: Set[String], context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(queryGraph = QueryGraph(argumentIds = argumentIds, patternNodes = Set(idName)))
    annotate(AllNodesScan(idName, argumentIds), solved, ProvidedOrder.empty, context)
  }

  def planApply(left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    // We don't want to keep the arguments that this Apply is inserting on the RHS, so we remove them here.
    val rhsSolved = solveds.get(right.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.withArgumentIds(Set.empty)))
    val solved = solveds.get(left.id).asSinglePlannerQuery ++ rhsSolved
    // If the LHS has duplicate values, we cannot guarantee any added order from the RHS
    val providedOrder = providedOrders.get(left.id).fromLeft
    annotate(Apply(left, right), solved, providedOrder, context)
  }

  def planSubquery(left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext, correlated: Boolean): LogicalPlan = {
    val solvedLeft = solveds.get(left.id)
    val solvedRight = solveds.get(right.id)
    val solved = solvedLeft.asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(CallSubqueryHorizon(solvedRight, correlated)))

    // If the LHS has duplicate values, we cannot guarantee any added order from the RHS
    val providedOrder = providedOrders.get(left.id).fromLeft
    val plan: LogicalPlan =
      if (!correlated)
        CartesianProduct(left, right)
      else
        Apply(left, right)

    annotate(plan, solved, providedOrder, context)
  }

  def planTailApply(left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.withTail(solveds.get(right.id).asSinglePlannerQuery))
    // If the LHS has duplicate values, we cannot guarantee any added order from the RHS
    val providedOrder = providedOrders.get(left.id).fromLeft
    annotate(Apply(left, right), solved, providedOrder, context)
  }

  def planInputApply(left: LogicalPlan, right: LogicalPlan, symbols: Seq[String], context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(right.id).asSinglePlannerQuery.withInput(symbols)
    // If the LHS has duplicate values, we cannot guarantee any added order from the RHS
    val providedOrder = providedOrders.get(left.id).fromLeft
    annotate(Apply(left, right), solved, providedOrder, context)
  }

  def planCartesianProduct(left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val solved: SinglePlannerQuery = solveds.get(left.id).asSinglePlannerQuery ++ solveds.get(right.id).asSinglePlannerQuery
    // If the LHS has duplicate values, we cannot guarantee any added order from the RHS
    val providedOrder = providedOrders.get(left.id).fromLeft
    annotate(CartesianProduct(left, right), solved, providedOrder, context)
  }

  def planDirectedRelationshipByIdSeek(idName: String,
                                       relIds: SeekableArgs,
                                       startNode: String,
                                       endNode: String,
                                       pattern: PatternRelationship,
                                       argumentIds: Set[String],
                                       solvedPredicates: Seq[Expression] = Seq.empty,
                                       context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(queryGraph = QueryGraph.empty
      .addPatternRelationship(pattern)
      .addPredicates(solvedPredicates: _*)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    val solver = PatternExpressionSolver.solverForLeafPlan(argumentIds, context)
    val rewrittenRelIds = relIds.mapValues(solver.solve(_))
    val newArguments = solver.newArguments
    // Is this ordered by relationship id?
    val leafPlan = annotate(DirectedRelationshipByIdSeek(idName, rewrittenRelIds, startNode, endNode, argumentIds ++ newArguments), solved, ProvidedOrder.empty, context)
    solver.rewriteLeafPlan(leafPlan)
  }

  def planUndirectedRelationshipByIdSeek(idName: String,
                                         relIds: SeekableArgs,
                                         leftNode: String,
                                         rightNode: String,
                                         pattern: PatternRelationship,
                                         argumentIds: Set[String],
                                         solvedPredicates: Seq[Expression] = Seq.empty,
                                         context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(queryGraph = QueryGraph.empty
      .addPatternRelationship(pattern)
      .addPredicates(solvedPredicates: _*)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    val solver = PatternExpressionSolver.solverForLeafPlan(argumentIds, context)
    val rewrittenRelIds = relIds.mapValues(solver.solve(_))
    val newArguments = solver.newArguments
    // Is this ordered by relationship id?
    val leafPlan = annotate(UndirectedRelationshipByIdSeek(idName, rewrittenRelIds, leftNode, rightNode, argumentIds ++ newArguments), solved, ProvidedOrder.empty, context)
    solver.rewriteLeafPlan(leafPlan)
  }

  def planSimpleExpand(left: LogicalPlan,
                       from: String,
                       dir: SemanticDirection,
                       to: String,
                       pattern: PatternRelationship,
                       mode: ExpansionMode,
                       context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(left.id).asSinglePlannerQuery.amendQueryGraph(_.addPatternRelationship(pattern))
    val providedOrder = providedOrders.get(left.id).fromLeft
    annotate(Expand(left, from, dir, pattern.types, to, pattern.name, mode), solved, providedOrder, context)
  }

  def planVarExpand(source: LogicalPlan,
                    from: String,
                    dir: SemanticDirection,
                    to: String,
                    pattern: PatternRelationship,
                    relationshipPredicate: Option[VariablePredicate],
                    nodePredicate: Option[VariablePredicate],
                    solvedPredicates: Seq[Expression],
                    mode: ExpansionMode,
                    context: LogicalPlanningContext): LogicalPlan = pattern.length match {
    case l: VarPatternLength =>
      val projectedDir = projectedDirection(pattern, from, dir)

      val solved = solveds.get(source.id).asSinglePlannerQuery.amendQueryGraph(_
        .addPatternRelationship(pattern)
        .addPredicates(solvedPredicates: _*)
      )

      val solver = PatternExpressionSolver.solverFor(source, context)

      /**
       * There are multiple considerations about this strange method.
       * a) If we solve PatternExpressions before `extractPredicates` in `expandStepSolver`, then the `replaceVariable` rewriter
       *    does not properly recurse into NestedPlanExpressions. It is thus easier to first rewrite the predicates and then
       *    let the PatternExpressionSolver solve any PatternExpressions in them.
       * b) `extractPredicates` extracts the Predicates ouf of the FilterScopes they are inside. The PatternExpressionSolver needs
       *    to know if things are inside a different scope to work correctly. Otherwise it will plan RollupApply when not allowed,
       *    or plan the wrong `NestedPlanExpression`. Since extracting the scope instead of the inner predicate is not straightforward
       *    either, the easiest solution is this one: we wrap each predicate in a FilterScope, give it the the PatternExpressionSolver,
       *    and then extract it from the FilterScope again.
       */
      def solveVariablePredicate(variablePredicate: VariablePredicate): VariablePredicate = {
        val filterScope = FilterScope(variablePredicate.variable, Some(variablePredicate.predicate))(variablePredicate.predicate.position)
        val rewrittenFilterScope = solver.solve(filterScope).asInstanceOf[FilterScope]
        VariablePredicate(rewrittenFilterScope.variable, rewrittenFilterScope.innerPredicate.get)
      }

      val rewrittenRelationshipPredicate = relationshipPredicate.map(solveVariablePredicate)
      val rewrittenNodePredicate = nodePredicate.map(solveVariablePredicate)
      val rewrittenSource = solver.rewrittenPlan()
      annotate(VarExpand(
        source = rewrittenSource,
        from = from,
        dir = dir,
        projectedDir = projectedDir,
        types = pattern.types,
        to = to,
        relName = pattern.name,
        length = l,
        mode = mode,
        nodePredicate = rewrittenNodePredicate,
        relationshipPredicate = rewrittenRelationshipPredicate), solved, providedOrders.get(source.id).fromLeft, context)

    case _ => throw new InternalException("Expected a varlength path to be here")
  }

  def planHiddenSelection(predicates: Seq[Expression], left: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    annotate(Selection(coercePredicatesWithAnds(predicates), left), solveds.get(left.id), providedOrders.get(left.id).fromLeft, context)
  }

  def planNodeByIdSeek(variable: Variable,
                       nodeIds: SeekableArgs,
                       solvedPredicates: Seq[Expression] = Seq.empty,
                       argumentIds: Set[String],
                       context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(variable.name)
      .addPredicates(solvedPredicates: _*)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    val solver = PatternExpressionSolver.solverForLeafPlan(argumentIds, context)
    val rewrittenNodeIds = nodeIds.mapValues(solver.solve(_))
    val newArguments = solver.newArguments
    val leafPlan = annotate(NodeByIdSeek(variable.name, rewrittenNodeIds, argumentIds ++ newArguments), solved, ProvidedOrder.empty, context)
    solver.rewriteLeafPlan(leafPlan)
  }

  def planNodeByLabelScan(variable: Variable,
                          label: LabelName,
                          solvedPredicates: Seq[Expression],
                          solvedHint: Option[UsingScanHint] = None,
                          argumentIds: Set[String],
                          providedOrder: ProvidedOrder,
                          context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(variable.name)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    annotate(NodeByLabelScan(variable.name, label, argumentIds, toIndexOrder(providedOrder)), solved, providedOrder, context)
  }

  def planNodeIndexSeek(idName: String,
                        label: LabelToken,
                        properties: Seq[IndexedProperty],
                        valueExpr: QueryExpression[Expression],
                        solvedPredicates: Seq[Expression] = Seq.empty,
                        solvedPredicatesForCardinalityEstimation: Seq[Expression] = Seq.empty,
                        solvedHint: Option[UsingIndexHint] = None,
                        argumentIds: Set[String],
                        providedOrder: ProvidedOrder,
                        context: LogicalPlanningContext): LogicalPlan = {
    val queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    // We know solvedPredicates is a subset of solvedPredicatesForCardinalityEstimation
    val solved = RegularSinglePlannerQuery(queryGraph = queryGraph)
    val solvedForCardinalityEstimation = RegularSinglePlannerQuery(queryGraph.addPredicates(solvedPredicatesForCardinalityEstimation: _*))

    val solver = PatternExpressionSolver.solverForLeafPlan(argumentIds, context)
    val rewrittenValueExpr = valueExpr.map(solver.solve(_))
    val newArguments = solver.newArguments
    val plan = NodeIndexSeek(idName, label, properties, rewrittenValueExpr, argumentIds ++ newArguments, toIndexOrder(providedOrder))
    val cardinality = cardinalityModel(solvedForCardinalityEstimation, context.input, context.semanticTable)
    solveds.set(plan.id, solved)
    cardinalities.set(plan.id, cardinality)
    providedOrders.set(plan.id, providedOrder)
    solver.rewriteLeafPlan(plan)
  }

  def planNodeIndexScan(idName: String,
                        label: LabelToken,
                        properties: Seq[IndexedProperty],
                        solvedPredicates: Seq[Expression] = Seq.empty,
                        solvedHint: Option[UsingIndexHint] = None,
                        argumentIds: Set[String],
                        providedOrder: ProvidedOrder,
                        context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    annotate(NodeIndexScan(idName, label, properties, argumentIds, toIndexOrder(providedOrder)), solved, providedOrder, context)
  }

  def planNodeIndexContainsScan(idName: String,
                                label: LabelToken,
                                properties: Seq[IndexedProperty],
                                solvedPredicates: Seq[Expression],
                                solvedHint: Option[UsingIndexHint],
                                valueExpr: Expression,
                                argumentIds: Set[String],
                                providedOrder: ProvidedOrder,
                                context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    val solver = PatternExpressionSolver.solverForLeafPlan(argumentIds, context)
    val rewrittenValueExpr = solver.solve(valueExpr)
    val newArguments = solver.newArguments
    val plan = annotate(NodeIndexContainsScan(idName, label, properties.head, rewrittenValueExpr, argumentIds ++ newArguments, toIndexOrder(providedOrder)), solved, providedOrder, context)
    solver.rewriteLeafPlan(plan)
  }

  def planNodeIndexEndsWithScan(idName: String,
                                label: LabelToken,
                                properties: Seq[IndexedProperty],
                                solvedPredicates: Seq[Expression],
                                solvedHint: Option[UsingIndexHint],
                                valueExpr: Expression,
                                argumentIds: Set[String],
                                providedOrder: ProvidedOrder,
                                context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    val solver = PatternExpressionSolver.solverForLeafPlan(argumentIds, context)
    val rewrittenValueExpr = solver.solve(valueExpr)
    val newArguments = solver.newArguments
    val plan = annotate(NodeIndexEndsWithScan(idName, label, properties.head, rewrittenValueExpr, argumentIds ++ newArguments, toIndexOrder(providedOrder)), solved, providedOrder, context)
    solver.rewriteLeafPlan(plan)
  }

  def planNodeHashJoin(nodes: Set[String], left: LogicalPlan, right: LogicalPlan, hints: Set[UsingJoinHint], context: LogicalPlanningContext): LogicalPlan = {
    val plannerQuery = solveds.get(left.id).asSinglePlannerQuery ++ solveds.get(right.id).asSinglePlannerQuery
    val solved = plannerQuery.amendQueryGraph(_.addHints(hints))
    annotate(NodeHashJoin(nodes, left, right), solved, providedOrders.get(right.id).fromRight, context)
  }

  def planValueHashJoin(left: LogicalPlan, right: LogicalPlan, join: Equals, originalPredicate: Equals, context: LogicalPlanningContext): LogicalPlan = {
    val plannerQuery = solveds.get(left.id).asSinglePlannerQuery ++ solveds.get(right.id).asSinglePlannerQuery
    val solved = plannerQuery.amendQueryGraph(_.addPredicates(originalPredicate))
    // `join` is an Expression that could go through the PatternExpressionSolver, but a value hash join
    // is only planned for Expressions such as `lhs.prop = rhs.prop`
    annotate(ValueHashJoin(left, right, join), solved, providedOrders.get(right.id).fromRight, context)
  }

  def planNodeUniqueIndexSeek(idName: String,
                              label: LabelToken,
                              properties: Seq[IndexedProperty],
                              valueExpr: QueryExpression[Expression],
                              solvedPredicates: Seq[Expression] = Seq.empty,
                              solvedPredicatesForCardinalityEstimation: Seq[Expression] = Seq.empty,
                              solvedHint: Option[UsingIndexHint] = None,
                              argumentIds: Set[String],
                              providedOrder: ProvidedOrder,
                              context: LogicalPlanningContext): LogicalPlan = {
    val queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    // We know solvedPredicates is a subset of solvedPredicatesForCardinalityEstimation
    val solved = RegularSinglePlannerQuery(queryGraph = queryGraph)
    val solvedForCardinalityEstimation = RegularSinglePlannerQuery(queryGraph.addPredicates(solvedPredicatesForCardinalityEstimation: _*))

    val solver = PatternExpressionSolver.solverForLeafPlan(argumentIds, context)
    val rewrittenValueExpr = valueExpr.map(solver.solve(_))
    val newArguments = solver.newArguments
    val plan = NodeUniqueIndexSeek(idName, label, properties, rewrittenValueExpr, argumentIds ++ newArguments, toIndexOrder(providedOrder))
    val cardinality = cardinalityModel(solvedForCardinalityEstimation, context.input, context.semanticTable)
    solveds.set(plan.id, solved)
    cardinalities.set(plan.id, cardinality)
    providedOrders.set(plan.id, providedOrder)
    solver.rewriteLeafPlan(plan)
  }

  def planAssertSameNode(node: String, left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val solved: SinglePlannerQuery = solveds.get(left.id).asSinglePlannerQuery ++ solveds.get(right.id).asSinglePlannerQuery
    annotate(AssertSameNode(node, left, right), solved, providedOrders.get(left.id).fromLeft, context)
  }

  def planOptional(inputPlan: LogicalPlan, ids: Set[String], context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(queryGraph = QueryGraph.empty
      .withAddedOptionalMatch(solveds.get(inputPlan.id).asSinglePlannerQuery.queryGraph)
      .withArgumentIds(ids)
    )
    annotate(Optional(inputPlan, ids), solved, providedOrders.get(inputPlan.id).fromLeft, context)
  }

  def planLeftOuterHashJoin(nodes: Set[String], left: LogicalPlan, right: LogicalPlan, hints: Set[UsingJoinHint], context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(left.id).asSinglePlannerQuery.amendQueryGraph(_.withAddedOptionalMatch(solveds.get(right.id).asSinglePlannerQuery.queryGraph.addHints(hints)))
    val providedOrder = providedOrders.get(right.id).upToExcluding(nodes).fromRight
    annotate(LeftOuterHashJoin(nodes, left, right), solved, providedOrder, context)
  }

  def planRightOuterHashJoin(nodes: Set[String], left: LogicalPlan, right: LogicalPlan, hints: Set[UsingJoinHint], context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(right.id).asSinglePlannerQuery.amendQueryGraph(_.withAddedOptionalMatch(solveds.get(left.id).asSinglePlannerQuery.queryGraph.addHints(hints)))
    annotate(RightOuterHashJoin(nodes, left, right), solved, providedOrders.get(right.id).fromRight, context)
  }

  def planSelection(source: LogicalPlan, predicates: Seq[Expression], context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(source.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addPredicates(predicates: _*)))
    val (rewrittenPredicates, rewrittenSource) = PatternExpressionSolver.ForMulti.solve(source, predicates, context)
    annotate(Selection(coercePredicatesWithAnds(rewrittenPredicates), rewrittenSource), solved, providedOrders.get(source.id).fromLeft, context)
  }

  def planHorizonSelection(source: LogicalPlan, predicates: Seq[Expression], interestingOrder: InterestingOrder, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(source.id).asSinglePlannerQuery.updateTailOrSelf(_.updateHorizon {
      case p: QueryProjection => p.addPredicates(predicates: _*)
      case _ => throw new IllegalArgumentException("You can only plan HorizonSelection after a projection")
    })

    // solve existential subquery predicates
    val (solvedPredicates, existsPlan) = PatternExpressionSolver.ForExistentialSubquery.solve(source, predicates, interestingOrder, context)
    if (solvedPredicates.nonEmpty) {
      annotate(Selection(coercePredicatesWithAnds(solvedPredicates), existsPlan), solved, providedOrders.get(source.id).fromLeft, context)
    }
    val filteredPredicates = predicates.filterNot(solvedPredicates.contains(_))

    // solve remaining predicates
    val newPlan = if (filteredPredicates.nonEmpty) {
      val (rewrittenPredicates, rewrittenSource) = PatternExpressionSolver.ForMulti.solve(existsPlan, filteredPredicates, context)
      annotate(Selection(coercePredicatesWithAnds(rewrittenPredicates), rewrittenSource), solved, providedOrders.get(existsPlan.id).fromLeft, context)
    } else {
      existsPlan
    }

    newPlan
  }

  // Using the solver for `expr` in all SemiApply-like plans is kinda stupid.
  // The idea is that `expr` is cheap to evaluate while the subquery (`inner`) is costly.
  // If `expr` is _also_ a PatternExpression or PatternComprehension, that is not true any longer,
  // and it could be cheaper to execute the one subquery  (`inner`) instead of the other (`expr`).

  def planSelectOrAntiSemiApply(outer: LogicalPlan, inner: LogicalPlan, expr: Expression, context: LogicalPlanningContext): LogicalPlan = {
    val (rewrittenExpr, rewrittenOuter) = PatternExpressionSolver.ForSingle.solve(outer, expr, context)
    annotate(SelectOrAntiSemiApply(rewrittenOuter, inner, rewrittenExpr), solveds.get(outer.id), providedOrders.get(outer.id).fromLeft, context)
  }

  def planLetSelectOrAntiSemiApply(outer: LogicalPlan, inner: LogicalPlan, id: String, expr: Expression, context: LogicalPlanningContext): LogicalPlan = {
    val (rewrittenExpr, rewrittenOuter) = PatternExpressionSolver.ForSingle.solve(outer, expr, context)
    annotate(LetSelectOrAntiSemiApply(rewrittenOuter, inner, id, rewrittenExpr), solveds.get(outer.id), providedOrders.get(outer.id).fromLeft, context)
  }

  def planSelectOrSemiApply(outer: LogicalPlan, inner: LogicalPlan, expr: Expression, context: LogicalPlanningContext): LogicalPlan = {
    val (rewrittenExpr, rewrittenOuter) = PatternExpressionSolver.ForSingle.solve(outer, expr, context)
    annotate(SelectOrSemiApply(rewrittenOuter, inner, rewrittenExpr), solveds.get(outer.id), providedOrders.get(outer.id).fromLeft, context)
  }

  def planLetSelectOrSemiApply(outer: LogicalPlan, inner: LogicalPlan, id: String, expr: Expression, context: LogicalPlanningContext): LogicalPlan = {
    val (rewrittenExpr, rewrittenOuter) = PatternExpressionSolver.ForSingle.solve(outer, expr, context)
    annotate(LetSelectOrSemiApply(rewrittenOuter, inner, id, rewrittenExpr), solveds.get(outer.id), providedOrders.get(outer.id).fromLeft, context)
  }

  def planLetAntiSemiApply(left: LogicalPlan, right: LogicalPlan, id: String, context: LogicalPlanningContext): LogicalPlan =
    annotate(LetAntiSemiApply(left, right, id), solveds.get(left.id), providedOrders.get(left.id).fromLeft, context)

  def planLetSemiApply(left: LogicalPlan, right: LogicalPlan, id: String, context: LogicalPlanningContext): LogicalPlan =
    annotate(LetSemiApply(left, right, id), solveds.get(left.id), providedOrders.get(left.id).fromLeft, context)

  def planAntiSemiApply(left: LogicalPlan, right: LogicalPlan, expr: Expression, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addPredicates(expr)))
    annotate(AntiSemiApply(left, right), solved, providedOrders.get(left.id).fromLeft, context)
  }

  def planSemiApply(left: LogicalPlan, right: LogicalPlan, expr: Expression, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.amendQueryGraph(_.addPredicates(expr)))
    annotate(SemiApply(left, right), solved, providedOrders.get(left.id).fromLeft, context)
  }

  def planSemiApplyInHorizon(left: LogicalPlan, right: LogicalPlan, expr: ExistsSubClause, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.updateHorizon {
      case horizon: QueryProjection => horizon.addPredicates(expr)
      case horizon => horizon
    })
    annotate(SemiApply(left, right), solved, providedOrders.get(left.id).fromLeft, context)
  }

  def planQueryArgument(queryGraph: QueryGraph, context: LogicalPlanningContext): LogicalPlan = {
    val patternNodes = queryGraph.argumentIds intersect queryGraph.patternNodes
    val patternRels = queryGraph.patternRelationships.filter(rel => queryGraph.argumentIds.contains(rel.name))
    val otherIds = queryGraph.argumentIds -- patternNodes
    planArgument(patternNodes, patternRels, otherIds, context)
  }

  def planArgumentFrom(plan: LogicalPlan, context: LogicalPlanningContext): LogicalPlan =
    annotate(Argument(plan.availableSymbols), solveds.get(plan.id), ProvidedOrder.empty, context)

  def planArgument(patternNodes: Set[String],
                   patternRels: Set[PatternRelationship] = Set.empty,
                   other: Set[String] = Set.empty,
                   context: LogicalPlanningContext): LogicalPlan = {
    val relIds = patternRels.map(_.name)
    val coveredIds = patternNodes ++ relIds ++ other

    val solved = RegularSinglePlannerQuery(queryGraph =
      QueryGraph(
        argumentIds = coveredIds,
        patternNodes = patternNodes,
        patternRelationships = Set.empty
      ))

    annotate(Argument(coveredIds), solved, ProvidedOrder.empty, context)
  }

  def planArgument(context: LogicalPlanningContext): LogicalPlan =
    annotate(Argument(Set.empty), SinglePlannerQuery.empty, ProvidedOrder.empty, context)

  def planEmptyProjection(inner: LogicalPlan, context: LogicalPlanningContext): LogicalPlan =
    annotate(EmptyResult(inner), solveds.get(inner.id), ProvidedOrder.empty, context)

  def planStarProjection(inner: LogicalPlan, reported: Map[String, Expression], context: LogicalPlanningContext): LogicalPlan = {
    val newSolved: SinglePlannerQuery = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.updateQueryProjection(_.withAddedProjections(reported)))
    // Keep some attributes, but change solved
    val keptAttributes = Attributes(idGen, cardinalities, providedOrders, leveragedOrders)
    val newPlan = inner.copyPlanWithIdGen(keptAttributes.copy(inner.id))
    solveds.set(newPlan.id, newSolved)
    newPlan
  }

  /**
   * @param expressions must be solved by the PatternExpressionSolver. This is not done here since that can influence the projection list,
   *                    thus this logic is put into [[projection]] instead.
   */
  def planRegularProjection(inner: LogicalPlan, expressions: Map[String, Expression], reported: Map[String, Expression], context: LogicalPlanningContext): LogicalPlan = {
    val solved: SinglePlannerQuery = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.updateQueryProjection(_.withAddedProjections(reported)))
    planRegularProjectionHelper(inner, expressions, context, solved)
  }

  /**
   * @param grouping                 must be solved by the PatternExpressionSolver. This is not done here since that can influence if we plan aggregation or projection, etc,
   *                                 thus this logic is put into [[aggregation]] instead.
   * @param aggregation              must be solved by the PatternExpressionSolver.
   * @param previousInterestingOrder the interesting order of the previous query part, if there was a previous part
   */
  def planAggregation(left: LogicalPlan,
                      grouping: Map[String, Expression],
                      aggregation: Map[String, Expression],
                      reportedGrouping: Map[String, Expression],
                      reportedAggregation: Map[String, Expression],
                      previousInterestingOrder: Option[InterestingOrder],
                      context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(
      AggregatingQueryProjection(groupingExpressions = reportedGrouping, aggregationExpressions = reportedAggregation)
    ))

    val trimmedAndRenamed = trimAndRenameProvidedOrder(providedOrders.get(left.id), grouping)

    val plan = annotate(Aggregation(left, grouping, aggregation), solved, ProvidedOrder(trimmedAndRenamed, ProvidedOrder.Left), context)

    def hasCollectOrUDF = aggregation.values.exists {
      case fi:FunctionInvocation => fi.function == Collect || fi.function == UnresolvedFunction
      case _ => false
    }
    // Aggregation functions may leverage the order of a preceding ORDER BY.
    // In practice, this is only collect and potentially user defined aggregations
    if (previousInterestingOrder.exists(_.requiredOrderCandidate.nonEmpty) && hasCollectOrUDF) {
      markOrderAsLeveragedBackwardsUntilOrigin(plan)
    }

    plan
  }

  def planOrderedAggregation(left: LogicalPlan,
                             grouping: Map[String, Expression],
                             aggregation: Map[String, Expression],
                             orderToLeverage: Seq[Expression],
                             reportedGrouping: Map[String, Expression],
                             reportedAggregation: Map[String, Expression],
                             context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(
      AggregatingQueryProjection(groupingExpressions = reportedGrouping, aggregationExpressions = reportedAggregation)
    ))

    val trimmedAndRenamed = trimAndRenameProvidedOrder(providedOrders.get(left.id), grouping)

    val plan = annotate(OrderedAggregation(left, grouping, aggregation, orderToLeverage), solved, ProvidedOrder(trimmedAndRenamed, ProvidedOrder.Left), context)
    markOrderAsLeveragedBackwardsUntilOrigin(plan)
    plan
  }

  /**
   * The only purpose of this method is to set the solved correctly for something that is already sorted.
   */
  def updateSolvedForSortedItems(inner: LogicalPlan,
                                 interestingOrder: InterestingOrder,
                                 context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.withInterestingOrder(interestingOrder))
    val providedOrder = providedOrders.get(inner.id)
    annotate(inner.copyPlanWithIdGen(idGen), solved, providedOrder, context)
  }

  def planCountStoreNodeAggregation(query: SinglePlannerQuery, projectedColumn: String, labels: List[Option[LabelName]], argumentIds: Set[String], context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(query.queryGraph, query.interestingOrder, query.horizon)
    annotate(NodeCountFromCountStore(projectedColumn, labels, argumentIds), solved, query.interestingOrder.requiredOrderCandidate.asProvidedOrder, context)
  }

  def planCountStoreRelationshipAggregation(query: SinglePlannerQuery, idName: String, startLabel: Option[LabelName],
                                            typeNames: Seq[RelTypeName], endLabel: Option[LabelName], argumentIds: Set[String], context: LogicalPlanningContext): LogicalPlan = {
    val solved: SinglePlannerQuery = RegularSinglePlannerQuery(query.queryGraph, query.interestingOrder, query.horizon)
    annotate(RelationshipCountFromCountStore(idName, startLabel, typeNames, endLabel, argumentIds), solved, query.interestingOrder.requiredOrderCandidate.asProvidedOrder, context)
  }

  def planSkip(inner: LogicalPlan, count: Expression, interestingOrder: InterestingOrder, context: LogicalPlanningContext): LogicalPlan = {
    // `count` is not allowed to be a PatternComprehension or PatternExpression
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.updateQueryProjection(_.updatePagination(_.withSkipExpression(count))))
    val plan = annotate(Skip(inner, count), solved, providedOrders.get(inner.id).fromLeft, context)
    if (interestingOrder.requiredOrderCandidate.nonEmpty) {
      markOrderAsLeveragedBackwardsUntilOrigin(plan)
    }
    plan
  }

  def planLoadCSV(inner: LogicalPlan, variableName: String, url: Expression, format: CSVFormat, fieldTerminator: Option[StringLiteral], context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(LoadCSVProjection(variableName, url, format, fieldTerminator)))
    val (rewrittenUrl, rewrittenInner) = PatternExpressionSolver.ForSingle.solve(inner, url, context)
    annotate(LoadCSV(rewrittenInner, rewrittenUrl, variableName, format, fieldTerminator.map(_.value), context.legacyCsvQuoteEscaping,
      context.csvBufferSize), solved, providedOrders.get(rewrittenInner.id).fromLeft, context)
  }

  def planInput(symbols: Seq[String], context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularSinglePlannerQuery(queryInput = Some(symbols))
    annotate(Input(symbols), solved, ProvidedOrder.empty, context)
  }

  def planUnwind(inner: LogicalPlan, name: String, expression: Expression, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(UnwindProjection(name, expression)))
    val (rewrittenExpression, rewrittenInner) = PatternExpressionSolver.ForSingle.solve(inner, expression, context)
    annotate(UnwindCollection(rewrittenInner, name, rewrittenExpression), solved, providedOrders.get(rewrittenInner.id).fromLeft, context)
  }

  def planCallProcedure(inner: LogicalPlan, call: ResolvedCall, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(ProcedureCallProjection(call)))
    val solver = PatternExpressionSolver.solverFor(inner, context)
    val rewrittenCall = call.mapCallArguments(solver.solve(_))
    val rewrittenInner = solver.rewrittenPlan()
    annotate(ProcedureCall(rewrittenInner, rewrittenCall), solved, providedOrders.get(rewrittenInner.id).fromLeft, context)
  }

  def planPassAll(inner: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(PassthroughAllHorizon()))
    // Keep some attributes, but change solved
    val keptAttributes = Attributes(idGen, cardinalities, leveragedOrders)
    val newPlan = inner.copyPlanWithIdGen(keptAttributes.copy(inner.id))
    annotate(newPlan, solved, providedOrders.get(inner.id).fromLeft, context)
  }

  def planLimit(inner: LogicalPlan,
                effectiveCount: Expression,
                reportedCount: Expression,
                interestingOrder: InterestingOrder,
                ties: Ties = DoNotIncludeTies,
                context: LogicalPlanningContext): LogicalPlan = {
    // `effectiveCount` is not allowed to be a PatternComprehension or PatternExpression
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.updateQueryProjection(_.updatePagination(_.withLimitExpression(reportedCount))))
    val plan = annotate(Limit(inner, effectiveCount, ties), solved, providedOrders.get(inner.id).fromLeft, context)
    if (interestingOrder.requiredOrderCandidate.nonEmpty) {
      markOrderAsLeveragedBackwardsUntilOrigin(plan)
    }
    plan
  }

  def planLimitForAggregation(inner: LogicalPlan,
                              ties: Ties = DoNotIncludeTies,
                              reportedGrouping: Map[String, Expression],
                              reportedAggregation: Map[String, Expression],
                              interestingOrder: InterestingOrder,
                              context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.withHorizon(
      AggregatingQueryProjection(groupingExpressions = reportedGrouping, aggregationExpressions = reportedAggregation)
    ).withInterestingOrder(interestingOrder))
    val providedOrder = providedOrders.get(inner.id).fromLeft
    val limitPlan = Limit(inner, SignedDecimalIntegerLiteral("1")(InputPosition.NONE), ties)
    val annotatedLimitPlan = annotate(limitPlan, solved, providedOrder, context)

    // The limit leverages the order, not the following optional
    markOrderAsLeveragedBackwardsUntilOrigin(annotatedLimitPlan)

    val plan = Optional(annotatedLimitPlan)
    annotate(plan, solved, providedOrder, context)
  }

  def planSort(inner: LogicalPlan,
               sortColumns: Seq[ColumnOrder],
               orderColumns: Seq[ProvidedOrder.Column],
               interestingOrder: InterestingOrder,
               context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.withInterestingOrder(interestingOrder))
    annotate(Sort(inner, sortColumns), solved, ProvidedOrder(orderColumns, ProvidedOrder.Self), context)
  }

  def planPartialSort(inner: LogicalPlan,
                      alreadySortedPrefix: Seq[ColumnOrder],
                      stillToSortSuffix: Seq[ColumnOrder],
                      orderColumns: Seq[ProvidedOrder.Column],
                      interestingOrder: InterestingOrder,
                      context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.updateTailOrSelf(_.withInterestingOrder(interestingOrder))
    val plan = annotate(PartialSort(inner, alreadySortedPrefix, stillToSortSuffix), solved, ProvidedOrder(orderColumns, ProvidedOrder.Left), context)
    markOrderAsLeveragedBackwardsUntilOrigin(plan)
    plan
  }

  def planShortestPath(inner: LogicalPlan,
                       shortestPaths: ShortestPathPattern,
                       predicates: Seq[Expression],
                       withFallBack: Boolean,
                       disallowSameNode: Boolean = true,
                       context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(_.addShortestPath(shortestPaths).addPredicates(predicates: _*))
    val (rewrittenPredicates, rewrittenInner) = PatternExpressionSolver.ForMulti.solve(inner, predicates, context)
    annotate(FindShortestPaths(rewrittenInner, shortestPaths, rewrittenPredicates, withFallBack, disallowSameNode), solved, providedOrders.get(rewrittenInner.id).fromLeft, context)
  }

  def planProjectEndpoints(inner: LogicalPlan, start: String, startInScope: Boolean, end: String, endInScope: Boolean, patternRel: PatternRelationship, context: LogicalPlanningContext): LogicalPlan = {
    val relTypes = patternRel.types.asNonEmptyOption
    val directed = patternRel.dir != SemanticDirection.BOTH
    val solved = solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(_.addPatternRelationship(patternRel))
    annotate(ProjectEndpoints(inner, patternRel.name,
      start, startInScope,
      end, endInScope,
      relTypes, directed, patternRel.length), solved, providedOrders.get(inner.id).fromLeft, context)
  }

  def planUnionForOrLeaves(left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val plan = Union(left, right)

    /* TODO: This is not correct in any way: solveds.get(left.id)
     LogicalPlan.solved contains a PlannerQuery, but to represent a Union, we'd need a UnionQuery instead
     Not very important at the moment, but dirty.
     */
    val solved = solveds.get(left.id)
    val solvedWithAllHints = solved.asSinglePlannerQuery.amendQueryGraph(qg => qg.copy(hints = qg.hints ++ solveds.get(right.id).allHints))
    solveds.set(plan.id, solvedWithAllHints)

    // Even if solveds is broken, it's nice to get the cardinality correct
    val lhsCardinality = cardinalities(left.id)
    val rhsCardinality = cardinalities(right.id)
    cardinalities.set(plan.id, lhsCardinality + rhsCardinality)
    providedOrders.set(plan.id, ProvidedOrder.empty)
    plan
  }

  def planDistinctForOrLeaves(left: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val returnAll = left.availableSymbols.map { s => s -> Variable(s)(InputPosition.NONE) }
    annotate(Distinct(left, returnAll.toMap), solveds.get(left.id), providedOrders.get(left.id).fromLeft, context)
  }

  def planProjectionForUnionMapping(inner: LogicalPlan, expressions: Map[String, Expression], context: LogicalPlanningContext): LogicalPlan = {
    annotate(Projection(inner, expressions), solveds.get(inner.id), providedOrders.get(inner.id).fromLeft, context)
  }

  def planUnion(left: LogicalPlan, right: LogicalPlan, unionMappings: List[UnionMapping], context: LogicalPlanningContext): LogicalPlan = {
    val solvedLeft = solveds.get(left.id)
    val solvedRight = solveds.get(right.id).asSinglePlannerQuery
    val solved = UnionQuery(solvedLeft, solvedRight, distinct = false, unionMappings)

    annotate(Union(left, right), solved, ProvidedOrder.empty, context)
  }

  def planDistinctForUnion(left: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val returnAll = left.availableSymbols.map { s => s -> Variable(s)(InputPosition.NONE) }

    val solved = solveds.get(left.id) match {
      case u: UnionQuery => markDistinctInUnion(u)
      case _ => throw new IllegalStateException("Planning a distinct for union, but no union was planned before.")
    }
    if (returnAll.isEmpty) {
      annotate(left.copyPlanWithIdGen(idGen), solved, providedOrders.get(left.id).fromLeft, context)
    } else {
      annotate(Distinct(left, returnAll.toMap), solved, providedOrders.get(left.id).fromLeft, context)
    }
  }

  private def markDistinctInUnion(query: PlannerQueryPart): PlannerQueryPart = {
    query match {
      case u@UnionQuery(part, _, _, _) => u.copy(part = markDistinctInUnion(part), distinct = true)
      case s => s
    }
  }

  /**
   *
   * @param expressions must be solved by the PatternExpressionSolver. This is not done here since that can influence how we plan distinct,
   *                    thus this logic is put into [[distinct]] instead.
   */
  def planDistinct(left: LogicalPlan, expressions: Map[String, Expression], reported: Map[String, Expression], context: LogicalPlanningContext): LogicalPlan = {
    val solved: SinglePlannerQuery = solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.updateQueryProjection(_ => DistinctQueryProjection(reported)))
    val columnsWithRenames = renameProvidedOrderColumns(providedOrders.get(left.id).columns, expressions)
    val providedOrder =  ProvidedOrder(columnsWithRenames, ProvidedOrder.Left)
    annotate(Distinct(left, expressions), solved, providedOrder, context)
  }

  /**
   *
   * @param expressions must be solved by the PatternExpressionSolver. This is not done here since that can influence how we plan distinct,
   *                    thus this logic is put into [[distinct]] instead.
   */
  def planOrderedDistinct(left: LogicalPlan,
                          expressions: Map[String, Expression],
                          orderToLeverage: Seq[Expression],
                          reported: Map[String, Expression],
                          context: LogicalPlanningContext): LogicalPlan = {
    val solved: SinglePlannerQuery = solveds.get(left.id).asSinglePlannerQuery.updateTailOrSelf(_.updateQueryProjection(_ => DistinctQueryProjection(reported)))
    val columnsWithRenames = renameProvidedOrderColumns(providedOrders.get(left.id).columns, expressions)
    val providedOrder =  ProvidedOrder(columnsWithRenames, ProvidedOrder.Left)
    annotate(OrderedDistinct(left, expressions, orderToLeverage), solved, providedOrder, context)
  }

  def updateSolvedForOr(orPlan: LogicalPlan, orPredicate: Ors, predicates: Set[Expression], context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(orPlan.id).asSinglePlannerQuery.updateTailOrSelf { that =>

      /*
        * We want to report all solved predicates, so we have kept track of what each subplan reports to solve.
        * There is no need to report the predicates that are inside the OR (exprs),
        * since we will add the OR itself instead.
        */
      val newSelections = Selections.from((predicates -- orPredicate.exprs + orPredicate).toSeq)
      that.amendQueryGraph(qg => qg.withSelections(newSelections))
    }
    val cardinality = context.cardinality.apply(solved, context.input, context.semanticTable)
    // Change solved and cardinality
    val keptAttributes = Attributes(idGen, providedOrders, leveragedOrders)
    val newPlan = orPlan.copyPlanWithIdGen(keptAttributes.copy(orPlan.id))
    solveds.set(newPlan.id, solved)
    cardinalities.set(newPlan.id, cardinality)
    newPlan
  }

  def planTriadicSelection(positivePredicate: Boolean,
                           left: LogicalPlan,
                           sourceId: String,
                           seenId: String,
                           targetId: String,
                           right: LogicalPlan,
                           predicate: Expression,
                           context: LogicalPlanningContext): LogicalPlan = {
    val solved = (solveds.get(left.id).asSinglePlannerQuery ++ solveds.get(right.id).asSinglePlannerQuery).updateTailOrSelf(_.amendQueryGraph(_.addPredicates(predicate)))
    annotate(TriadicSelection(left, right, positivePredicate, sourceId, seenId, targetId), solved, providedOrders.get(left.id).fromLeft, context)
  }

  def planCreate(inner: LogicalPlan, pattern: CreatePattern, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(_.addMutatingPatterns(pattern))
    val (rewrittenPattern, rewrittenInner) = PatternExpressionSolver.ForMappable().solve(inner, pattern, context)
    annotate(plans.Create(rewrittenInner, rewrittenPattern.nodes, rewrittenPattern.relationships), solved, providedOrders.get(rewrittenInner.id).fromLeft, context)
  }

  def planMergeCreateNode(inner: LogicalPlan, pattern: CreateNode, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(_.addMutatingPatterns(CreatePattern(List(pattern), Nil)))
    val (rewrittenPattern, rewrittenInner) = PatternExpressionSolver.ForMappable().solve(inner, pattern, context)
    annotate(MergeCreateNode(rewrittenInner, rewrittenPattern.idName, rewrittenPattern.labels, rewrittenPattern.properties), solved, providedOrders.get(rewrittenInner.id).fromLeft, context)
  }

  def planMergeCreateRelationship(inner: LogicalPlan, pattern: CreateRelationship, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(_.addMutatingPatterns(CreatePattern(Nil, List(pattern))))
    val (rewrittenPattern, rewrittenInner) = PatternExpressionSolver.ForMappable().solve(inner, pattern, context)
    annotate(MergeCreateRelationship(rewrittenInner, rewrittenPattern.idName, rewrittenPattern.startNode, rewrittenPattern.relType, rewrittenPattern.endNode, rewrittenPattern.properties), solved, providedOrders.get(rewrittenInner.id).fromLeft, context)
  }

  def planConditionalApply(lhs: LogicalPlan, rhs: LogicalPlan, idNames: Seq[String], context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(lhs.id).asSinglePlannerQuery ++ solveds.get(rhs.id).asSinglePlannerQuery
    // If the LHS has duplicate values, we cannot guarantee any added order from the RHS
    val providedOrder = providedOrders.get(lhs.id).fromLeft
    annotate(ConditionalApply(lhs, rhs, idNames), solved, providedOrder, context)
  }


  def planAntiConditionalApply(inner: LogicalPlan, outer: LogicalPlan, idNames: Seq[String], context: LogicalPlanningContext, maybeSolved: Option[SinglePlannerQuery] = None): LogicalPlan = {
    val solved = maybeSolved.getOrElse(solveds.get(inner.id).asSinglePlannerQuery ++ solveds.get(outer.id).asSinglePlannerQuery)
    // If the LHS has duplicate values, we cannot guarantee any added order from the RHS
    val providedOrder = providedOrders.get(inner.id).fromLeft
    annotate(AntiConditionalApply(inner, outer, idNames), solved, providedOrder, context)
  }

  def planDeleteNode(inner: LogicalPlan, delete: DeleteExpression, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(_.addMutatingPatterns(delete))
    val (rewrittenDelete, rewrittenInner) = PatternExpressionSolver.ForMappable().solve(inner, delete, context)
    if (delete.forced) {
      annotate(DetachDeleteNode(rewrittenInner, rewrittenDelete.expression), solved, providedOrders.get(rewrittenInner.id).fromLeft, context)
    } else {
      annotate(DeleteNode(rewrittenInner, rewrittenDelete.expression), solved, providedOrders.get(rewrittenInner.id).fromLeft, context)
    }
  }

  def planDeleteRelationship(inner: LogicalPlan, delete: DeleteExpression, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(_.addMutatingPatterns(delete))
    val (rewrittenDelete, rewrittenInner) = PatternExpressionSolver.ForMappable().solve(inner, delete, context)
    annotate(DeleteRelationship(rewrittenInner, rewrittenDelete.expression), solved, providedOrders.get(rewrittenInner.id).fromLeft, context)
  }

  def planDeletePath(inner: LogicalPlan, delete: DeleteExpression, context: LogicalPlanningContext): LogicalPlan = {
    // `delete.expression` can only be a PathExpression, PatternExpressionSolver not needed
    val solved = solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(_.addMutatingPatterns(delete))

    if (delete.forced) {
      annotate(DetachDeletePath(inner, delete.expression), solved, providedOrders.get(inner.id).fromLeft, context)
    } else {
      annotate(DeletePath(inner, delete.expression), solved, providedOrders.get(inner.id).fromLeft, context)
    }
  }

  def planDeleteExpression(inner: LogicalPlan, delete: DeleteExpression, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(_.addMutatingPatterns(delete))
    val (rewrittenDelete, rewrittenInner) = PatternExpressionSolver.ForMappable().solve(inner, delete, context)
    if (delete.forced) {
      annotate(DetachDeleteExpression(rewrittenInner, rewrittenDelete.expression), solved, providedOrders.get(rewrittenInner.id).fromLeft, context)
    } else {
      annotate(plans.DeleteExpression(rewrittenInner, rewrittenDelete.expression), solved, providedOrders.get(rewrittenInner.id).fromLeft, context)
    }
  }

  def planSetLabel(inner: LogicalPlan, pattern: SetLabelPattern, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(_.addMutatingPatterns(pattern))

    annotate(SetLabels(inner, pattern.idName, pattern.labels), solved, providedOrders.get(inner.id).fromLeft, context)
  }

  def planSetNodeProperty(inner: LogicalPlan, pattern: SetNodePropertyPattern, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(_.addMutatingPatterns(pattern))
    val (rewrittenPattern, rewrittenInner) = PatternExpressionSolver.ForMappable().solve(inner, pattern, context)
    annotate(SetNodeProperty(rewrittenInner, rewrittenPattern.idName, rewrittenPattern.propertyKey, rewrittenPattern.expression), solved, providedOrders.get(rewrittenInner.id).fromLeft, context)
  }

  def planSetNodePropertiesFromMap(inner: LogicalPlan, pattern: SetNodePropertiesFromMapPattern, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(_.addMutatingPatterns(pattern))
    val (rewrittenPattern, rewrittenInner) = PatternExpressionSolver.ForMappable().solve(inner, pattern, context)
    annotate(SetNodePropertiesFromMap(rewrittenInner, rewrittenPattern.idName, rewrittenPattern.expression, rewrittenPattern.removeOtherProps), solved, providedOrders.get(rewrittenInner.id).fromLeft, context)
  }

  def planSetRelationshipProperty(inner: LogicalPlan, pattern: SetRelationshipPropertyPattern, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(_.addMutatingPatterns(pattern))
    val (rewrittenPattern, rewrittenInner) = PatternExpressionSolver.ForMappable().solve(inner, pattern, context)
    annotate(SetRelationshipProperty(rewrittenInner, rewrittenPattern.idName, rewrittenPattern.propertyKey, rewrittenPattern.expression), solved, providedOrders.get(rewrittenInner.id).fromLeft, context)
  }

  def planSetRelationshipPropertiesFromMap(inner: LogicalPlan, pattern: SetRelationshipPropertiesFromMapPattern, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(_.addMutatingPatterns(pattern))
    val (rewrittenPattern, rewrittenInner) = PatternExpressionSolver.ForMappable().solve(inner, pattern, context)
    annotate(SetRelationshipPropertiesFromMap(rewrittenInner, rewrittenPattern.idName, rewrittenPattern.expression, rewrittenPattern.removeOtherProps), solved, providedOrders.get(rewrittenInner.id).fromLeft, context)
  }

  def planSetPropertiesFromMap(inner: LogicalPlan, pattern: SetPropertiesFromMapPattern, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(_.addMutatingPatterns(pattern))
    val (rewrittenPattern, rewrittenInner) = PatternExpressionSolver.ForMappable().solve(inner, pattern, context)
    annotate(SetPropertiesFromMap(rewrittenInner, rewrittenPattern.entityExpression, rewrittenPattern.expression, rewrittenPattern.removeOtherProps), solved, providedOrders.get(inner.id).fromLeft, context)
  }

  def planSetProperty(inner: LogicalPlan, pattern: SetPropertyPattern, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(_.addMutatingPatterns(pattern))
    val (rewrittenPattern, rewrittenInner) = PatternExpressionSolver.ForMappable().solve(inner, pattern, context)
    annotate(SetProperty(rewrittenInner, rewrittenPattern.entityExpression, rewrittenPattern.propertyKeyName, rewrittenPattern.expression), solved, providedOrders.get(rewrittenInner.id).fromLeft, context)
  }

  def planRemoveLabel(inner: LogicalPlan, pattern: RemoveLabelPattern, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).asSinglePlannerQuery.amendQueryGraph(_.addMutatingPatterns(pattern))

    annotate(RemoveLabels(inner, pattern.idName, pattern.labels), solved, providedOrders.get(inner.id).fromLeft, context)
  }

  def planForeachApply(left: LogicalPlan,
                       innerUpdates: LogicalPlan,
                       pattern: ForeachPattern,
                       context: LogicalPlanningContext,
                       expression: Expression): LogicalPlan = {
    val solved = solveds.get(left.id).asSinglePlannerQuery.amendQueryGraph(_.addMutatingPatterns(pattern))
    val (rewrittenExpression, rewrittenLeft) = PatternExpressionSolver.ForSingle.solve(left, expression, context)
    annotate(ForeachApply(rewrittenLeft, innerUpdates, pattern.variable, rewrittenExpression), solved, providedOrders.get(rewrittenLeft.id).fromLeft, context)
  }

  def planEager(inner: LogicalPlan, context: LogicalPlanningContext): LogicalPlan =
    annotate(Eager(inner), solveds.get(inner.id), providedOrders.get(inner.id).fromLeft, context)

  def planError(inner: LogicalPlan, exception: ExhaustiveShortestPathForbiddenException, context: LogicalPlanningContext): LogicalPlan =
    annotate(ErrorPlan(inner, exception), solveds.get(inner.id), providedOrders.get(inner.id).fromLeft, context)

  /**
   * @param lastInterestingOrders the interesting order of the last part of the whole query, or `None` for UNION queries.
   */
  def planProduceResult(inner: LogicalPlan, columns: Seq[String], lastInterestingOrders: Option[InterestingOrder], context: LogicalPlanningContext): LogicalPlan = {
    val produceResult = ProduceResult(inner, columns)
    solveds.copy(inner.id, produceResult.id)
    // Do not calculate cardinality for ProduceResult. Since the passed context does not have accurate label information
    // It will get a wrong value with some projections. Use the cardinality of inner instead
    cardinalities.copy(inner.id, produceResult.id)
    providedOrders.set(produceResult.id, providedOrders.get(inner.id).fromLeft)

    if (lastInterestingOrders.exists(_.requiredOrderCandidate.nonEmpty)) {
      markOrderAsLeveragedBackwardsUntilOrigin(produceResult)
    }

    produceResult
  }

  /**
   * Compute cardinality for a plan. Set this cardinality in the Cardinalities attribute.
   * Set the other attributes with the provided arguments (solved and providedOrder).
   *
   * @return the same plan
   */
  private def annotate(plan: LogicalPlan, solved: PlannerQueryPart, providedOrder: ProvidedOrder, context: LogicalPlanningContext): LogicalPlan = {
    assertNoBadExpressionsExists(plan)
    val cardinality = cardinalityModel(solved, context.input, context.semanticTable)
    solveds.set(plan.id, solved)
    cardinalities.set(plan.id, cardinality)
    providedOrders.set(plan.id, providedOrder)
    plan
  }

  /**
   * There probably exists some type level way of achieving this with type safety instead of manually searching through the expression tree like this
   */
  private def assertNoBadExpressionsExists(root: Any): Unit = {
    checkOnlyWhenAssertionsAreEnabled(!new FoldableAny(root).treeExists {
      case _: PatternComprehension | _: PatternExpression | _: MapProjection =>
        throw new InternalException(s"This expression should not be added to a logical plan:\n$root")
      case _ =>
        false
    })
  }

  private def projectedDirection(pattern: PatternRelationship, from: String, dir: SemanticDirection): SemanticDirection = {
    if (dir == SemanticDirection.BOTH) {
      if (from == pattern.left) {
        SemanticDirection.OUTGOING
      } else {
        SemanticDirection.INCOMING
      }
    }
    else {
      pattern.dir
    }
  }

  private def planRegularProjectionHelper(inner: LogicalPlan, expressions: Map[String, Expression], context: LogicalPlanningContext, solved: SinglePlannerQuery) = {
    val columnsWithRenames = renameProvidedOrderColumns(providedOrders.get(inner.id).columns, expressions)
    val providedOrder = ProvidedOrder(columnsWithRenames, ProvidedOrder.Left)
    annotate(Projection(inner, expressions), solved, providedOrder, context)
  }

  /**
   * The provided order is used to describe the current ordering of the LogicalPlan within a complete plan tree. For
   * index leaf operators this can be planned as an IndexOrder for the index to provide. In that case it only works
   * if all columns are sorted in the same direction, so we need to narrow the scope for these index operations.
   */
  private def toIndexOrder(providedOrder: ProvidedOrder): IndexOrder = providedOrder match {
    case ProvidedOrder.empty => IndexOrderNone
    case ProvidedOrder(columns) if columns.forall(c => c.isAscending) => IndexOrderAscending
    case ProvidedOrder(columns) if columns.forall(c => !c.isAscending) => IndexOrderDescending
    case _ => throw new IllegalStateException("Cannot mix ascending and descending columns when using index order")
  }

  /**
   * Rename sort columns if they are renamed in a projection.
   */
  private def renameProvidedOrderColumns(columns: Seq[ProvidedOrder.Column], projectExpressions: Map[String, Expression]): Seq[ProvidedOrder.Column] = {
    columns.map {
      case columnOrder@ProvidedOrder.Column(e@Property(v@Variable(varName), p@PropertyKeyName(propName))) =>
        projectExpressions.collectFirst {
          case (newName, Property(Variable(`varName`), PropertyKeyName(`propName`)) | CachedProperty(`varName`, _, PropertyKeyName(`propName`), _)) =>
            ProvidedOrder.Column(Variable(newName)(v.position), columnOrder.isAscending)
          case (newName, Variable(`varName`)) =>
            ProvidedOrder.Column(Property(Variable(newName)(v.position), PropertyKeyName(propName)(p.position))(e.position), columnOrder.isAscending)
        }.getOrElse(columnOrder)
      case columnOrder@ProvidedOrder.Column(expression) =>
        projectExpressions.collectFirst {
          case (newName, `expression`) => ProvidedOrder.Column(Variable(newName)(expression.position), columnOrder.isAscending)
        }.getOrElse(columnOrder)
    }
  }

  private def trimAndRenameProvidedOrder(providedOrder: ProvidedOrder, grouping: Map[String, Expression]): Seq[ProvidedOrder.Column] = {
    // Trim provided order for each sort column, if it is a non-grouping column
    val trimmed = providedOrder.columns.takeWhile {
      case ProvidedOrder.Column(Property(Variable(varName), PropertyKeyName(propName))) =>
        grouping.values.exists {
          case CachedProperty(`varName`, _, PropertyKeyName(`propName`), _) => true
          case Property(Variable(`varName`), PropertyKeyName(`propName`)) => true
          case _ => false
        }
      case ProvidedOrder.Column(expression) =>
        grouping.values.exists {
          case `expression` => true
          case _ => false
        }
    }
    renameProvidedOrderColumns(trimmed, grouping)
  }

  /**
   * Starting from `lp`, traverse the logical plan backwards until finding the origin of the current provided order.
   * For each plan on the way, set `leveragedOrder` to `true`.
   *
   * @param lp the plan that leverages a provided order. Must be an already annotated plan.
   */
  private def markOrderAsLeveragedBackwardsUntilOrigin(lp: LogicalPlan): Unit = {
    leveragedOrders.set(lp.id, true)

    @tailrec
    def loop(current: LogicalPlan): Unit = {
      leveragedOrders.set(current.id, true)
      val origin = providedOrders.get(current.id).orderOrigin
      origin match {
        case Some(ProvidedOrder.Left) => loop(current.lhs.get)
        case Some(ProvidedOrder.Right) => loop(current.rhs.get)
        case Some(ProvidedOrder.Self) => // done
        case None =>
          AssertMacros.checkOnlyWhenAssertionsAreEnabled(false,
            s"While marking leveraged order we encountered a plan with no provided order: $current")
      }
    }

    providedOrders.get(lp.id).orderOrigin match {
      case Some(ProvidedOrder.Left) => lp.lhs.foreach(loop)
      case Some(ProvidedOrder.Right) => lp.rhs.foreach(loop)
      case Some(ProvidedOrder.Self) => // If the plan both introduces and leverages the order, we do not want to traverse into the children
      case None =>
        // The plan itself leverages the order, but does not maintain it.
        // Currently, in that case we assume it is a one-child plan,
        // since at the time of writing there is no two child plan that leverages and destroys ordering
        lp.lhs.foreach(loop)
        AssertMacros.checkOnlyWhenAssertionsAreEnabled(lp.rhs.isEmpty, "We assume that there is no two-child plan leveraging but destroying ordering.")
    }
  }

}
