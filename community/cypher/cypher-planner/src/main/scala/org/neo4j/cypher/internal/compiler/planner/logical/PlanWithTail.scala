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

import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
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

  override def apply(lhsPlans: Seq[LogicalPlan], in: SinglePlannerQuery, context: LogicalPlanningContext): (LogicalPlan, LogicalPlanningContext) = {
    val plansAndContexts: Seq[(LogicalPlan, LogicalPlanningContext)] = lhsPlans.map(doPlan(_, in, context))
    val updatedContext = plansAndContexts.head._2 // should be the same for all plans, let's use the first one
    val pickBest = updatedContext.config.pickBestCandidate(updatedContext)
    pickBest[(LogicalPlan, LogicalPlanningContext)](_._1, plansAndContexts, s"best finalized plan for ${in.queryGraph}").get
  }

  @tailrec
  private def doPlan(lhs: LogicalPlan, in: SinglePlannerQuery, context: LogicalPlanningContext): (LogicalPlan, LogicalPlanningContext) = {
    in.tail match {
      case Some(plannerQuery) =>
        val partPlan = planPart(plannerQuery, context, rhsPart = true).result // always expecting a single plan currently

        val planWithUpdates = planUpdates(plannerQuery, partPlan, firstPlannerQuery = false, context)

        val applyPlan = context.logicalPlanProducer.planTailApply(lhs, planWithUpdates, context)

        val applyContext = context.withUpdatedLabelInfo(applyPlan)
        val projectedPlan = planEventHorizon(plannerQuery, applyPlan, Some(in.interestingOrder), applyContext)
        val projectedContext = applyContext.withUpdatedLabelInfo(projectedPlan)

        doPlan(projectedPlan, plannerQuery, projectedContext)

      case None =>
        val attributes = Attributes(context.idGen, context.planningAttributes.cardinalities, context.planningAttributes.providedOrders)
        (lhs.endoRewrite(Eagerness.unnestEager(context.planningAttributes.solveds, attributes)), context)
    }
  }
}
