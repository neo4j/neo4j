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

import org.neo4j.cypher.internal.compiler.planner.logical.steps.BestPlans
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.attribution.Attributes

import scala.annotation.tailrec

/*
This class ties together disparate query graphs through their event horizons. It does so by using Apply,
which in most cases is then rewritten away by LogicalPlan rewriting.
*/
case class PlanWithTail(planEventHorizon: EventHorizonPlanner = PlanEventHorizon,
                        planPart: MatchPlanner = planMatch,
                        planUpdates: UpdatesPlanner = PlanUpdates)
  extends TailPlanner {

  override def apply(lhsPlans: BestPlans,
                     in: SinglePlannerQuery,
                     context: LogicalPlanningContext,
                     limitSelectivities: List[Selectivity]): (LogicalPlan, LogicalPlanningContext) = {

    val (bestPlans, updatedContext) = doPlan(lhsPlans, in, context, limitSelectivities.tail)
    val pickBest = updatedContext.config.pickBestCandidate(updatedContext)
    val plan = pickBest(bestPlans.allResults.toIterable, s"best finalized plan for ${in.queryGraph}").get
    (plan, updatedContext)
  }

  @tailrec
  private def doPlan(lhs: BestPlans, in: SinglePlannerQuery, context: LogicalPlanningContext, limitSelectivities: List[Selectivity]): (BestPlans, LogicalPlanningContext) = {
    (in.tail, limitSelectivities.headOption.fold(context)(context.withLimitSelectivity)) match {
      case (Some(plannerQuery), context) =>
        val rhsContext = context.withOuterPlan(lhs.bestResult)
        val partPlan = planPart(plannerQuery, rhsContext, rhsPart = true).result // always expecting a single plan currently
        val planWithUpdates = planUpdates(plannerQuery, partPlan, firstPlannerQuery = false, context)

        val applyPlans = lhs.map(context.logicalPlanProducer.planTailApply(_, planWithUpdates, context))
        val applyContext = context.withUpdatedLabelInfo(applyPlans.bestResult)
        val horizonPlans = planEventHorizon.planHorizon(plannerQuery, applyPlans, Some(in.interestingOrder), applyContext)
        val contextForTail = applyContext.withUpdatedLabelInfo(horizonPlans.bestResult) // cardinality should be the same for all plans, let's use the first one

        doPlan(horizonPlans, plannerQuery, contextForTail, limitSelectivities.tail)

      case (None, context) =>
        val unnest = Eagerness.unnestEager(
          context.planningAttributes.solveds,
          context.planningAttributes.cardinalities,
          context.planningAttributes.providedOrders,
          Attributes(context.idGen)
        )

        val plans = lhs.map(_.endoRewrite(unnest))
        (plans, context)
    }
  }
}
