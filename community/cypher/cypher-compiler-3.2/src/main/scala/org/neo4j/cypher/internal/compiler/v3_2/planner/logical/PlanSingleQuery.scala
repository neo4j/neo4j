/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical

import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.steps.{countStorePlanner, verifyBestPlan}
import org.neo4j.cypher.internal.ir.v3_2.PlannerQuery

/*
This coordinates PlannerQuery planning and delegates work to the classes that do the actual planning of
QueryGraphs and EventHorizons
 */
case class PlanSingleQuery(planPart: (PlannerQuery, LogicalPlanningContext) => LogicalPlan = planPart,
                           planEventHorizon: LogicalPlanningFunction2[PlannerQuery, LogicalPlan, LogicalPlan] = PlanEventHorizon,
                           planWithTail: LogicalPlanningFunction2[LogicalPlan, Option[PlannerQuery], LogicalPlan] = PlanWithTail(),
                           planUpdates: LogicalPlanningFunction3[PlannerQuery, LogicalPlan, Boolean, LogicalPlan] = PlanUpdates) extends LogicalPlanningFunction1[PlannerQuery, LogicalPlan] {

  override def apply(in: PlannerQuery)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val (completePlan, ctx) = countStorePlanner(in) match {
      case Some(plan) =>
        (plan, context.recurse(plan))
      case None =>
        val partPlan = planPart(in, context)
        val planWithUpdates = planUpdates(in, partPlan, true /*first QG*/)(context)
        val projectedPlan = planEventHorizon(in, planWithUpdates)
        val projectedContext = context.recurse(projectedPlan)
        (projectedPlan, projectedContext)
    }

    val finalPlan = planWithTail(completePlan, in.tail)(ctx)
    verifyBestPlan(finalPlan, in)
  }
}
