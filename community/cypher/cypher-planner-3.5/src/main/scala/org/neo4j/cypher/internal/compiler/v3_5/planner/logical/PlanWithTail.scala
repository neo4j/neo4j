/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.cypher.internal.ir.v3_5.PlannerQuery
import org.neo4j.cypher.internal.v3_5.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v3_5.util.attribution.Attributes
import org.neo4j.cypher.internal.v3_5.util.attribution.IdGen

/*
This class ties together disparate query graphs through their event horizons. It does so by using Apply,
which in most cases is then rewritten away by LogicalPlan rewriting.
*/
case class PlanWithTail(planEventHorizon: EventHorizonPlanner = PlanEventHorizon,
                        planPart: PartPlanner = planPart,
                        planUpdates: UpdatesPlanner = PlanUpdates)
  extends TailPlanner {

  override def apply(lhs: LogicalPlan, remaining: Option[PlannerQuery], context: LogicalPlanningContext, idGen: IdGen): (LogicalPlan, LogicalPlanningContext) = {
    remaining match {
      case Some(plannerQuery) =>
        val attributes = context.planningAttributes.asAttributes(idGen)
        val lhsContext = context.withUpdatedCardinalityInformation(lhs)

        val partPlan = planPart(plannerQuery, lhsContext)
        val (planWithUpdates, newContext) = planUpdates(plannerQuery, partPlan, firstPlannerQuery = false, lhsContext)

        // Mark properties from indexes to be fetched, if the properties are used later in the query
        val alignedPlan = alignGetValueFromIndexBehavior(plannerQuery, planWithUpdates, context.logicalPlanProducer, context.planningAttributes.solveds, attributes)
        val applyPlan = newContext.logicalPlanProducer.planTailApply(lhs, alignedPlan, context)

        val applyContext = newContext.withUpdatedCardinalityInformation(applyPlan)
        val projectedPlan = planEventHorizon(plannerQuery, applyPlan, applyContext)
        val projectedContext = applyContext.withUpdatedCardinalityInformation(projectedPlan)

        this.apply(projectedPlan, plannerQuery.tail, projectedContext, idGen)

      case None =>
        val attributes = Attributes(idGen, context.planningAttributes.cardinalities, context.planningAttributes.providedOrders)
        (lhs.endoRewrite(Eagerness.unnestEager(context.planningAttributes.solveds, attributes)), context)
    }
  }
}
