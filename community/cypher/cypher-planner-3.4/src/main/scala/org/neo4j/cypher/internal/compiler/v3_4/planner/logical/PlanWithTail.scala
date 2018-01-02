/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical

import org.neo4j.cypher.internal.ir.v3_4.PlannerQuery
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan

/*
This class ties together disparate query graphs through their event horizons. It does so by using Apply,
which in most cases is then rewritten away by LogicalPlan rewriting.
*/
case class PlanWithTail(planEventHorizon: ((PlannerQuery, LogicalPlan, LogicalPlanningContext) => LogicalPlan) = PlanEventHorizon,
                        planPart: (PlannerQuery, LogicalPlanningContext) => LogicalPlan = planPart,
                        planUpdates: ((PlannerQuery, LogicalPlan, Boolean, LogicalPlanningContext) => (LogicalPlan, LogicalPlanningContext)) = PlanUpdates)
  extends ((LogicalPlan, Option[PlannerQuery], LogicalPlanningContext) => LogicalPlan) {

  override def apply(lhs: LogicalPlan, remaining: Option[PlannerQuery], context: LogicalPlanningContext): LogicalPlan = {
    remaining match {
      case Some(plannerQuery) =>
        val lhsContext = context.withUpdatedCardinalityInformation(lhs)
        val partPlan = planPart(plannerQuery, lhsContext)
        val firstPlannerQuery = false
        val (planWithUpdates, newContext) = planUpdates(plannerQuery, partPlan, firstPlannerQuery, lhsContext)

        val applyPlan = newContext.logicalPlanProducer.planTailApply(lhs, planWithUpdates, context)

        val applyContext = newContext.withUpdatedCardinalityInformation(applyPlan)
        val projectedPlan = planEventHorizon(plannerQuery, applyPlan, applyContext)
        val projectedContext = applyContext.withUpdatedCardinalityInformation(projectedPlan)

        this.apply(projectedPlan, plannerQuery.tail, projectedContext)

      case None =>
        lhs.endoRewrite(Eagerness.unnestEager)
    }
  }
}
