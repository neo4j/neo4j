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
package org.neo4j.cypher.internal.compiler.v4_0.planner.logical

import org.neo4j.cypher.internal.compiler.v4_0.helpers.AggregationHelper
import org.neo4j.cypher.internal.compiler.v4_0.planner.logical.steps.{alignGetValueFromIndexBehavior, countStorePlanner, verifyBestPlan}
import org.neo4j.cypher.internal.ir.v4_0.{PlannerQuery, _}
import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.cypher.internal.v4_0.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v4_0.util.attribution.IdGen

import scala.collection.mutable

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
    val updatedContext = addAggregatedPropertiesToContext(in, context)

    val (completePlan, ctx) =
      countStorePlanner(in, updatedContext) match {
        case Some(plan) =>
          (plan, updatedContext.withUpdatedCardinalityInformation(plan))
        case None =>
          val attributes = updatedContext.planningAttributes.asAttributes(idGen)

          val partPlan = planPart(in, updatedContext)
          val (planWithUpdates, contextAfterUpdates) = planUpdates(in, partPlan, firstPlannerQuery = true, updatedContext)
          val projectedPlan = planEventHorizon(in, planWithUpdates, contextAfterUpdates)
          val projectedContext = contextAfterUpdates.withUpdatedCardinalityInformation(projectedPlan)

          // Mark properties from indexes to be fetched, if the properties are used later in the query
          val alignedPlan = alignGetValueFromIndexBehavior(in, projectedPlan, updatedContext.logicalPlanProducer, updatedContext.planningAttributes.solveds, attributes)
          (alignedPlan, projectedContext)
      }

    val (finalPlan, finalContext) = planWithTail(completePlan, in, ctx, idGen)
    (verifyBestPlan(finalPlan, in, finalContext), finalContext)
  }

  val renamings: mutable.Map[String, Expression] = mutable.Map.empty

  /*
   * Extract all properties over which aggregation is performed, where we potentially could use a NodeIndexScan.
   * The renamings map is used to keep track of any projections changing the name of the property,
   * as in MATCH (n:Label) WITH n.prop1 AS prop RETURN count(prop)
   */
  def addAggregatedPropertiesToContext(currentQuery: PlannerQuery, context: LogicalPlanningContext): LogicalPlanningContext = {

    // If the graph is mutated between the MATCH and the aggregation, an index scan might lead to the wrong number of mutations
    if (currentQuery.queryGraph.mutatingPatterns.nonEmpty) return context

    currentQuery.horizon match {
      case aggr: AggregatingQueryProjection =>
        if (aggr.groupingExpressions.isEmpty) // needed here to not enter next case
          AggregationHelper.extractProperties(aggr.aggregationExpressions, renamings) match {
            case properties: Set[(String, String)] if properties.nonEmpty => context.withAggregationProperties(properties)
            case _ => context
          }
        else context
      case proj: QueryProjection =>
        currentQuery.tail match {
          case Some(tail) =>
            renamings ++= proj.projections
            addAggregatedPropertiesToContext(tail, context)
          case _ => context
        }
      case _ =>
        currentQuery.tail match {
          case Some(tail) => addAggregatedPropertiesToContext(tail, context)
          case _ => context
        }
    }
  }
}

trait PartPlanner {
  def apply(query: PlannerQuery, context: LogicalPlanningContext, rhsPart: Boolean = false): LogicalPlan
}

trait EventHorizonPlanner {
  def apply(query: PlannerQuery, plan: LogicalPlan, context: LogicalPlanningContext): LogicalPlan
}

trait TailPlanner {
  def apply(lhs: LogicalPlan, in: PlannerQuery, context: LogicalPlanningContext, idGen: IdGen): (LogicalPlan, LogicalPlanningContext)
}

trait UpdatesPlanner {
  def apply(query: PlannerQuery, in: LogicalPlan, firstPlannerQuery: Boolean, context: LogicalPlanningContext): (LogicalPlan, LogicalPlanningContext)
}
