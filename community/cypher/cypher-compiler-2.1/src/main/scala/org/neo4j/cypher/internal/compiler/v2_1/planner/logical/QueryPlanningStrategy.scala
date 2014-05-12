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
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.QueryPlan
import org.neo4j.cypher.internal.compiler.v2_1.planner.{CantHandleQueryException, AggregationProjection, PlannerQuery}

class QueryPlanningStrategy(config: PlanningStrategyConfiguration = PlanningStrategyConfiguration.default) extends PlanningStrategy {
  def plan(implicit context: LogicalPlanningContext, leafPlan: Option[QueryPlan] = None): QueryPlan = {
    val query = context.query
    if(query.projection.isInstanceOf[AggregationProjection])
      throw new CantHandleQueryException

    val graphSolvingContext = context.asQueryGraphSolvingContext(query.graph)

    val afterSolvingPattern = context.strategy.plan(graphSolvingContext, leafPlan)
    val afterProjection = projection(afterSolvingPattern)

    val finalPlan: QueryPlan = query.tail match {
      case Some(tail) =>
        val finalPlan = plan(context.copy(query = tail), Some(QueryPlan(afterProjection.plan, PlannerQuery.empty)))
        finalPlan.copy(solved = afterProjection.solved.withTail(tail))

      case _ => afterProjection
    }

    verifyBestPlan(finalPlan)
  }
}
