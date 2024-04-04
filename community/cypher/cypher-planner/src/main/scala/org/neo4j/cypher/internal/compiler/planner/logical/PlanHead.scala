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

import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper
import org.neo4j.cypher.internal.compiler.planner.logical.idp.BestResults
import org.neo4j.cypher.internal.compiler.planner.logical.steps.BestPlans
import org.neo4j.cypher.internal.compiler.planner.logical.steps.countStorePlanner
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

/**
 * Solves the first query graph and its horizon of a SinglePlannerQuery.
 */
case class PlanHead(
  matchPlanner: MatchPlanner = planMatch,
  eventHorizonPlanner: EventHorizonPlanner = PlanEventHorizon,
  planUpdates: UpdatesPlanner = PlanUpdates
) extends HeadPlanner {

  override def plan(
    headQuery: SinglePlannerQuery,
    context: LogicalPlanningContext
  ): (BestPlans, LogicalPlanningContext) = {
    val aggregationPropertyAccesses = PropertyAccessHelper.findAggregationPropertyAccesses(headQuery)
    val localPropertyAccesses = PropertyAccessHelper.findLocalPropertyAccesses(headQuery)
    val updatedContext = context.withModifiedPlannerState(_
      .withAggregationProperties(aggregationPropertyAccesses)
      .withAccessedProperties(localPropertyAccesses ++ aggregationPropertyAccesses))

    val plans = countStorePlanner(headQuery, updatedContext) match {
      case Some(plan) =>
        BestResults(plan, None)
      case None =>
        val matchPlans = matchPlanner.plan(headQuery, updatedContext)
        // We take all plans solving the MATCH part. This could be two, if we have a required order.
        val plansWithInput: BestResults[LogicalPlan] = matchPlans.map(planUpdatesAndInput(_, headQuery, updatedContext))

        val plansWithHorizon = eventHorizonPlanner.planHorizon(headQuery, plansWithInput, None, updatedContext)
        plansWithHorizon.map(context.staticComponents.logicalPlanProducer.addMissingStandaloneArgumentPatternNodes(
          _,
          headQuery,
          updatedContext
        ))
    }

    (plans, updatedContext)
  }

  /**
   * Plan updates, query input, and horizon for all of them.
   * Horizon planning will ensure that any ORDER BY clause is solved, so in the end we have up to two plans that are comparable.
   */
  private def planUpdatesAndInput(
    matchPlan: LogicalPlan,
    headQuery: SinglePlannerQuery,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val planWithUpdates = planUpdates.plan(headQuery, matchPlan, firstPlannerQuery = true, context)

    headQuery.queryInput match {
      case Some(variables) =>
        val inputPlan = context.staticComponents.logicalPlanProducer.planInput(variables, context)
        context.staticComponents.logicalPlanProducer.planInputApply(inputPlan, planWithUpdates, variables, context)
      case None => planWithUpdates
    }
  }
}
