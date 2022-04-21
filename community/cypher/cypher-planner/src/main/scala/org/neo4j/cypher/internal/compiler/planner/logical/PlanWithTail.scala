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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper
import org.neo4j.cypher.internal.compiler.planner.logical.steps.BestPlans
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

/*
This class ties together disparate query graphs through their event horizons. It does so by using Apply,
which in most cases is then rewritten away by LogicalPlan rewriting.
 */
case class PlanWithTail(
  eventHorizonPlanner: EventHorizonPlanner = PlanEventHorizon,
  matchPlanner: MatchPlanner = planMatch,
  updatesPlanner: UpdatesPlanner = PlanUpdates
) extends TailPlanner {

  override def plan(
    lhsPlans: BestPlans,
    tailQuery: SinglePlannerQuery,
    previousInterestingOrder: InterestingOrder,
    context: LogicalPlanningContext
  ): (BestPlans, LogicalPlanningContext) = {
    val updatedContext = context
      .withAccessedProperties(PropertyAccessHelper.findLocalPropertyAccesses(tailQuery))

    val rhsPlan = planRhs(tailQuery, updatedContext.withOuterPlan(lhsPlans.bestResult))
    planApply(lhsPlans, rhsPlan, previousInterestingOrder, tailQuery, updatedContext)
  }

  private def planRhs(tailQuery: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    val rhsPlan =
      matchPlanner.plan(tailQuery, context, rhsPart = true).result // always expecting a single plan currently
    val rhsPlanWithUpdates = updatesPlanner.plan(tailQuery, rhsPlan, firstPlannerQuery = false, context)
    context.logicalPlanProducer.addMissingStandaloneArgumentPatternNodes(rhsPlanWithUpdates, tailQuery, context)
  }

  private def planApply(
    lhsPlans: BestPlans,
    rhsPlan: LogicalPlan,
    previousInterestingOrder: InterestingOrder,
    tailQuery: SinglePlannerQuery,
    lhsContext: LogicalPlanningContext
  ): (BestPlans, LogicalPlanningContext) = {
    val applyPlans = lhsPlans.map(lhsContext.logicalPlanProducer.planTailApply(_, rhsPlan, lhsContext))
    val applyContext = lhsContext.withUpdatedLabelInfo(applyPlans.bestResult)
    val horizonPlans =
      eventHorizonPlanner.planHorizon(tailQuery, applyPlans, Some(previousInterestingOrder), applyContext)
    val contextForTail =
      applyContext.withUpdatedLabelInfo(
        horizonPlans.bestResult
      ) // cardinality should be the same for all plans, let's use the first one
    (horizonPlans, contextForTail)
  }
}
