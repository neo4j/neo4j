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

import org.neo4j.cypher.internal.ir.v3_4.PlannerQuery
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.util.v3_4.attribution.Attributes
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan

/*
This class ties together disparate query graphs through their event horizons. It does so by using Apply,
which in most cases is then rewritten away by LogicalPlan rewriting.
*/
case class PlanWithTail(planEventHorizon: ((PlannerQuery, LogicalPlan, LogicalPlanningContext, Solveds, Cardinalities) => LogicalPlan) = PlanEventHorizon,
                        planPart: (PlannerQuery, LogicalPlanningContext, Solveds, Cardinalities) => LogicalPlan = planPart,
                        planUpdates: ((PlannerQuery, LogicalPlan, Boolean, LogicalPlanningContext, Solveds, Cardinalities) => (LogicalPlan, LogicalPlanningContext)) = PlanUpdates)
  extends ((LogicalPlan, Option[PlannerQuery], LogicalPlanningContext, Solveds, Cardinalities, Attributes) => LogicalPlan) {

  override def apply(lhs: LogicalPlan, remaining: Option[PlannerQuery], context: LogicalPlanningContext,
                     solveds: Solveds, cardinalities: Cardinalities, otherAttributes: Attributes): LogicalPlan = {
    remaining match {
      case Some(plannerQuery) =>
        val lhsContext = context.withUpdatedCardinalityInformation(lhs, solveds, cardinalities)
        val partPlan = planPart(plannerQuery, lhsContext, solveds, cardinalities)
        val firstPlannerQuery = false
        val (planWithUpdates, newContext) = planUpdates(plannerQuery, partPlan, firstPlannerQuery, lhsContext, solveds, cardinalities)

        val applyPlan = newContext.logicalPlanProducer.planTailApply(lhs, planWithUpdates, context)

        val applyContext = newContext.withUpdatedCardinalityInformation(applyPlan, solveds, cardinalities)
        val projectedPlan = planEventHorizon(plannerQuery, applyPlan, applyContext, solveds, cardinalities)
        val projectedContext = applyContext.withUpdatedCardinalityInformation(projectedPlan, solveds, cardinalities)

        this.apply(projectedPlan, plannerQuery.tail, projectedContext, solveds, cardinalities, otherAttributes)

      case None =>
        lhs.endoRewrite(Eagerness.unnestEager(solveds, otherAttributes.withAlso(cardinalities)))
    }
  }
}
