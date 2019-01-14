/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical

import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps.alignGetValueFromIndexBehavior
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps.countStorePlanner
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps.verifyBestPlan
import org.neo4j.cypher.internal.ir.v3_5.PlannerQuery
import org.neo4j.cypher.internal.v3_5.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v3_5.util.attribution.IdGen

/*
This coordinates PlannerQuery planning and delegates work to the classes that do the actual planning of
QueryGraphs and EventHorizons
 */
case class PlanSingleQuery(planPart: PartPlanner = planPart,
                           planEventHorizon: EventHorizonPlanner = PlanEventHorizon,
                           planWithTail: TailPlanner = PlanWithTail(),
                           planUpdates:UpdatesPlanner = PlanUpdates)
  extends SingleQueryPlanner {

  override def apply(in: PlannerQuery, context: LogicalPlanningContext, idGen: IdGen): (LogicalPlan, LogicalPlanningContext) = {
    val (completePlan, ctx) =
      countStorePlanner(in, context) match {
        case Some(plan) =>
          (plan, context.withUpdatedCardinalityInformation(plan))
        case None =>
          val attributes = context.planningAttributes.asAttributes(idGen)

          val partPlan = planPart(in, context)
          val (planWithUpdates, contextAfterUpdates) = planUpdates(in, partPlan, firstPlannerQuery = true, context)
          val projectedPlan = planEventHorizon(in, planWithUpdates, contextAfterUpdates)
          val projectedContext = contextAfterUpdates.withUpdatedCardinalityInformation(projectedPlan)

          // Mark properties from indexes to be fetched, if the properties are used later in the query
          val alignedPlan = alignGetValueFromIndexBehavior(in, projectedPlan, context.logicalPlanProducer, context.planningAttributes.solveds, attributes)
          (alignedPlan, projectedContext)
      }

    val (finalPlan, finalContext) = planWithTail(completePlan, in.tail, ctx, idGen)
    (verifyBestPlan(finalPlan, in, finalContext), finalContext)
  }
}

trait PartPlanner {
  def apply(query: PlannerQuery, context: LogicalPlanningContext): LogicalPlan
}

trait EventHorizonPlanner {
  def apply(query: PlannerQuery, plan: LogicalPlan, context: LogicalPlanningContext): LogicalPlan
}

trait TailPlanner {
  def apply(lhs: LogicalPlan, remaining: Option[PlannerQuery], context: LogicalPlanningContext, idGen: IdGen): (LogicalPlan, LogicalPlanningContext)
}

trait UpdatesPlanner {
  def apply(query: PlannerQuery, in: LogicalPlan, firstPlannerQuery: Boolean, context: LogicalPlanningContext): (LogicalPlan, LogicalPlanningContext)
}
