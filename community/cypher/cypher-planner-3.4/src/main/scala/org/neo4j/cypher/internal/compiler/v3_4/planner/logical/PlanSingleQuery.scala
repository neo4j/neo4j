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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical

import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps.{countStorePlanner, verifyBestPlan}
import org.neo4j.cypher.internal.ir.v3_4.PlannerQuery
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.util.v3_4.attribution.{Attributes, IdGen}
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan

/*
This coordinates PlannerQuery planning and delegates work to the classes that do the actual planning of
QueryGraphs and EventHorizons
 */
case class PlanSingleQuery(planPart: (PlannerQuery, LogicalPlanningContext, Solveds, Cardinalities) => LogicalPlan = planPart,
                           planEventHorizon: ((PlannerQuery, LogicalPlan, LogicalPlanningContext, Solveds, Cardinalities) => LogicalPlan) = PlanEventHorizon,
                           planWithTail: ((LogicalPlan, Option[PlannerQuery], LogicalPlanningContext, Solveds, Cardinalities, Attributes) => LogicalPlan) = PlanWithTail(),
                           planUpdates: ((PlannerQuery, LogicalPlan, Boolean, LogicalPlanningContext, Solveds, Cardinalities) => (LogicalPlan, LogicalPlanningContext)) = PlanUpdates)
  extends ((PlannerQuery, LogicalPlanningContext, Solveds, Cardinalities, IdGen) => LogicalPlan) {

  override def apply(in: PlannerQuery, context: LogicalPlanningContext,
                     solveds: Solveds, cardinalities: Cardinalities, idGen: IdGen): LogicalPlan = {
    val (completePlan, ctx) = countStorePlanner(in, context, solveds, cardinalities) match {
      case Some(plan) =>
        (plan, context.withUpdatedCardinalityInformation(plan, solveds, cardinalities))
      case None =>
        val partPlan = planPart(in, context, solveds, cardinalities)
        val (planWithUpdates, newContext) = planUpdates(in, partPlan, true /*first QG*/, context, solveds, cardinalities)
        val projectedPlan = planEventHorizon(in, planWithUpdates, newContext, solveds, cardinalities)
        val projectedContext = newContext.withUpdatedCardinalityInformation(projectedPlan, solveds, cardinalities)
        (projectedPlan, projectedContext)
    }

    val finalPlan = planWithTail(completePlan, in.tail, ctx, solveds, cardinalities, Attributes(idGen))
    verifyBestPlan(finalPlan, in, context, solveds, cardinalities)
  }
}
