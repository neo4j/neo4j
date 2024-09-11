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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.logical.idp.BestResults
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.aggregation
import org.neo4j.cypher.internal.compiler.planner.logical.steps.distinct
import org.neo4j.cypher.internal.compiler.planner.logical.steps.projection
import org.neo4j.cypher.internal.compiler.planner.logical.steps.skipAndLimit
import org.neo4j.cypher.internal.ir.AbstractProcedureCallProjection
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.CallSubqueryHorizon
import org.neo4j.cypher.internal.ir.CommandProjection
import org.neo4j.cypher.internal.ir.DistinctQueryProjection
import org.neo4j.cypher.internal.ir.LoadCSVProjection
import org.neo4j.cypher.internal.ir.PassthroughAllHorizon
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RunQueryAtProjection
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.UnwindProjection
import org.neo4j.cypher.internal.ir.ast.IRExpression
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

/*
Planning event horizons means planning the WITH clauses between query patterns. Some of these clauses are inlined
away when going from a string query to a QueryGraph. The remaining WITHs are the ones containing ORDER BY/LIMIT,
aggregation and UNWIND.
 */
case object PlanEventHorizon extends EventHorizonPlanner {

  override protected def doPlanHorizon(
    plannerQuery: SinglePlannerQuery,
    incomingPlans: BestResults[LogicalPlan],
    prevInterestingOrder: Option[InterestingOrder],
    context: LogicalPlanningContext
  ): BestResults[LogicalPlan] = {
    val pickBest: CandidateSelector = context.plannerState.config.pickBestCandidate(context)
    // This config will only plan Sort if there is a required order in this plannerQuery
    val sortIfSelfRequiredConfig = InterestingOrderConfig(plannerQuery.interestingOrder)
    // This config will even plan Sort if there is a required order in a tail plannerQuery
    val sortIfTailOrSelfRequiredConfig = InterestingOrderConfig.interestingOrderForPart(
      query = plannerQuery,
      isRhs = false,
      isHorizon = true
    )

    // Plans horizon on top of the current best-overall plan, ensuring ordering only if required by the current query part.
    def planSortIfSelfRequired = planHorizonForPlan(
      plannerQuery,
      incomingPlans.bestResult,
      prevInterestingOrder,
      context,
      sortIfSelfRequiredConfig
    )
    // Plans horizon on top of the current best-overall plan, ensuring ordering if required by the current OR later query part.
    def planSortIfTailOrSelfRequired = planHorizonForPlan(
      plannerQuery,
      incomingPlans.bestResult,
      prevInterestingOrder,
      context,
      sortIfTailOrSelfRequiredConfig
    )
    // Plans horizon on top of the current best-sorted plan
    def maintainSort = incomingPlans.bestResultFulfillingReq.map(planHorizonForPlan(
      plannerQuery,
      _,
      prevInterestingOrder,
      context,
      sortIfTailOrSelfRequiredConfig
    ))

    val currentPartHasRequiredOrder = plannerQuery.interestingOrder.requiredOrderCandidate.nonEmpty
    val tailHasRequiredOrder = sortIfSelfRequiredConfig != sortIfTailOrSelfRequiredConfig

    if (currentPartHasRequiredOrder) {
      // Both best-overall and best-sorted plans must fulfill the required order, so at this point we can pick one of them
      val bestOverall = pickBest(
        Seq(planSortIfSelfRequired) ++ maintainSort,
        "best overall plan with horizon"
      ).getOrElse(throw new IllegalStateException("Planner returned no best overall plan"))
      BestResults(bestOverall, None)
    } else if (tailHasRequiredOrder) {
      // For best-overall keep the current best-overall plan
      val bestOverall = planSortIfSelfRequired
      // For best-sorted we can choose between the current best-sorted and the current best-overall with sorting planned on top
      val bestSorted =
        pickBest(Seq(planSortIfTailOrSelfRequired) ++ maintainSort, "best sorted plan with horizon")
      BestResults(bestOverall, bestSorted)
    } else {
      // No ordering requirements, only keep the best-overall plan
      val bestOverall = planSortIfSelfRequired
      BestResults(bestOverall, None)
    }
  }

  private[logical] def planHorizonForPlan(
    query: SinglePlannerQuery,
    plan: LogicalPlan,
    previousInterestingOrder: Option[InterestingOrder],
    context: LogicalPlanningContext,
    interestingOrderConfig: InterestingOrderConfig
  ): LogicalPlan = {
    val selectedPlan =
      context.plannerState.config.applySelections(plan, query.queryGraph, interestingOrderConfig, context)
    // We only want to mark a planned Sort (or a projection for a Sort) as solved if the ORDER BY comes from the current horizon.
    val updateSolvedOrdering = query.interestingOrder.requiredOrderCandidate.nonEmpty

    val planSort: LogicalPlan => LogicalPlan =
      SortPlanner.ensureSortedPlanWithSolved(_, interestingOrderConfig, context, updateSolvedOrdering)

    val planSkipAndLimit: LogicalPlan => LogicalPlan = skipAndLimit(_, query, context)

    def planWhere(selections: Selections): LogicalPlan => LogicalPlan = (p: LogicalPlan) =>
      if (selections.isEmpty) {
        p
      } else {
        val predicates = selections.flatPredicates
        context.staticComponents.logicalPlanProducer.planHorizonSelection(
          p,
          predicates,
          interestingOrderConfig,
          context
        )
      }

    val projectedPlan = query.horizon match {
      case aggregatingProjection: AggregatingQueryProjection =>
        val planAggregation: LogicalPlan => LogicalPlan = aggregation(
          _,
          aggregatingProjection,
          interestingOrderConfig.orderToReport,
          previousInterestingOrder,
          context
        )

        // for aggregation, sort happens after the projection. The provided order of the aggregation plan will include
        // renames of the projection, thus we need to rename this as well for the required order before considering planning a sort.
        Function.chain(Seq(
          planAggregation,
          planSort,
          planSkipAndLimit,
          planWhere(aggregatingProjection.selections)
        ))(selectedPlan)

      case regularProjection: RegularQueryProjection =>
        val projectSubqueryExpressions: LogicalPlan => LogicalPlan = (p: LogicalPlan) => {
          val subqueryExpressionProjections =
            regularProjection.projections.filter(_._2.folder.treeFindByClass[IRExpression].nonEmpty)
          projection(
            p,
            subqueryExpressionProjections,
            Some(subqueryExpressionProjections),
            context
          )
        }

        val planProjection: LogicalPlan => LogicalPlan = (p: LogicalPlan) =>
          if (regularProjection.projections.isEmpty && query.tail.isEmpty) {
            if (context.plannerState.isInSubquery) {
              p
            } else {
              context.staticComponents.logicalPlanProducer.planEmptyProjection(p, context)
            }
          } else {
            projection(
              p,
              regularProjection.projections,
              Some(regularProjection.projections),
              context
            )
          }

        def sortFirst = Function.chain(Seq(
          planSort,
          planSkipAndLimit,
          planProjection,
          planWhere(regularProjection.selections)
        ))
        def projectSubqueryExpressionsFirst = Function.chain(Seq(
          projectSubqueryExpressions,
          sortFirst
        ))

        def sortFirstWithFallback(initialPlan: LogicalPlan): LogicalPlan = {
          val sortedPlan = sortFirst(initialPlan)
          SortPlanner.orderSatisfaction(interestingOrderConfig, context, sortedPlan) match {
            case InterestingOrder.FullSatisfaction() => sortedPlan
            case _                                   =>
              // Some subquery expression invalidated the ordering, start over.
              projectSubqueryExpressionsFirst(initialPlan)
          }
        }

        // Normally, we will first sort and then apply projections. This is cheaper in the the case of a LIMIT,
        // where the projection only needs to be applied to fewer rows.
        // If the runtime is not order preserving, we should do subquery expression projections (which can break an incoming order)
        // before sorting.
        if (context.settings.executionModel.providedOrderPreserving) sortFirstWithFallback(selectedPlan)
        else projectSubqueryExpressionsFirst(selectedPlan)

      case distinctProjection: DistinctQueryProjection =>
        def planDistinct: LogicalPlan => LogicalPlan = distinct(_, distinctProjection, context)

        // for distinct, sort happens after the projection. The provided order of the distinct plan will include
        // renames of the projection, thus we need to rename this as well for the required order before considering planning a sort.
        Function.chain(Seq(
          planDistinct,
          planSort,
          planSkipAndLimit,
          planWhere(distinctProjection.selections)
        ))(selectedPlan)

      case UnwindProjection(variable, expression) =>
        val projected =
          context.staticComponents.logicalPlanProducer.planUnwind(selectedPlan, variable, expression, context)
        SortPlanner.ensureSortedPlanWithSolved(projected, interestingOrderConfig, context, updateSolvedOrdering)

      case projection: AbstractProcedureCallProjection =>
        val projected = context.staticComponents.logicalPlanProducer.planProcedureCall(plan, projection.call, context)
        SortPlanner.ensureSortedPlanWithSolved(projected, interestingOrderConfig, context, updateSolvedOrdering)

      case LoadCSVProjection(variableName, url, format, fieldTerminator) =>
        val projected =
          context.staticComponents.logicalPlanProducer.planLoadCSV(
            plan,
            variableName,
            url,
            format,
            fieldTerminator,
            context
          )
        SortPlanner.ensureSortedPlanWithSolved(projected, interestingOrderConfig, context, updateSolvedOrdering)

      case PassthroughAllHorizon() =>
        val projected = context.staticComponents.logicalPlanProducer.planPassAll(plan, context)
        SortPlanner.ensureSortedPlanWithSolved(projected, interestingOrderConfig, context, updateSolvedOrdering)

      case CallSubqueryHorizon(callSubquery, correlated, yielding, inTransactionsParameters, optional) =>
        val subqueryContext =
          if (correlated)
            context.withModifiedPlannerState(_
              .forSubquery()
              .withUpdatedLabelInfo(plan, context.staticComponents.planningAttributes.solveds))
          else
            context.withModifiedPlannerState(_.forSubquery())

        val subPlan = plannerQueryPlanner.plan(callSubquery, subqueryContext)

        val finalSubPlan = if (optional)
          context.staticComponents.logicalPlanProducer.planOptional(
            subPlan,
            plan.availableSymbols,
            subqueryContext
          )
        else subPlan

        val projected = context.staticComponents.logicalPlanProducer.planSubquery(
          plan,
          finalSubPlan,
          context,
          correlated,
          yielding,
          inTransactionsParameters,
          optional
        )
        SortPlanner.ensureSortedPlanWithSolved(projected, interestingOrderConfig, context, updateSolvedOrdering)

      case CommandProjection(clause) =>
        val commandPlan = context.staticComponents.logicalPlanProducer.planCommand(plan, clause, context)
        SortPlanner.ensureSortedPlanWithSolved(commandPlan, interestingOrderConfig, context, updateSolvedOrdering)

      case RunQueryAtProjection(graphReference, queryString, parameters, importsAsParameters, columns) =>
        val runQueryAt =
          context
            .staticComponents
            .logicalPlanProducer
            .planRunQueryAt(plan, graphReference, queryString, parameters, importsAsParameters, columns, context)
        SortPlanner.ensureSortedPlanWithSolved(runQueryAt, interestingOrderConfig, context, updateSolvedOrdering)
    }

    // We need to check if reads introduced in the horizon conflicts with future writes
    val eagerAnalyzer = EagerAnalyzer(context)
    eagerAnalyzer.horizonEagerize(projectedPlan, query)
  }
}
