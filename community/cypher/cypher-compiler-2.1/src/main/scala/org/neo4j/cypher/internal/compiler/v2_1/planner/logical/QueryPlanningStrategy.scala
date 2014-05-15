/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps._
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.QueryPlan

class QueryPlanningStrategy(config: PlanningStrategyConfiguration = PlanningStrategyConfiguration.default) extends PlanningStrategy {

  import QueryPlanProducer._

  def plan(implicit context: LogicalPlanningContext, leafPlan: Option[QueryPlan] = None): QueryPlan = {
    val query = context.query
    val firstPart = planPart(query, leafPlan)
    val projectedFirstPart = planEventHorizon(query.projection, firstPart)
    val finalPlan = plan(projectedFirstPart, query.tail, context)
    verifyBestPlan(finalPlan)
  }

  private def plan(pred: QueryPlan, remaining: Option[PlannerQuery], context: LogicalPlanningContext): QueryPlan = remaining match {
    case Some(query) =>
      val innerContext = context.copy(query = query)
      val lhs = pred
      val rhs = planPart(query, Some(planQueryArgumentRow(query.graph)))(innerContext)
      val applyPlan = planTailApply(lhs, rhs)
      val projectedPlan = planEventHorizon(query.projection, applyPlan)(innerContext)
      plan( projectedPlan, query.tail, innerContext )
    case None =>
      pred
  }

  private def planPart(query: PlannerQuery, leafPlan: Option[QueryPlan] = None)(implicit context: LogicalPlanningContext): QueryPlan = {
    val graphSolvingContext = context.asQueryGraphSolvingContext(query.graph)
    val afterSolvingPattern = context.strategy.plan(graphSolvingContext, leafPlan)

    afterSolvingPattern
  }

  private def planEventHorizon(queryProjection: QueryProjection, plan: QueryPlan)(implicit context: LogicalPlanningContext) = {
    queryProjection match {
      case aggr:AggregationProjection =>
        val aggregationPlan = aggregation(plan, aggr)
        sortSkipAndLimit(aggregationPlan)

      case _ =>
        val sortedAndLimited = sortSkipAndLimit(plan)
        projection(sortedAndLimited, queryProjection.projections)
    }
  }
}
