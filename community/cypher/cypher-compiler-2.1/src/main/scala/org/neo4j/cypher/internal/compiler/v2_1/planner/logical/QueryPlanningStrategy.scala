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
import org.neo4j.cypher.internal.compiler.v2_1.ast.PatternExpression

class QueryPlanningStrategy(config: PlanningStrategyConfiguration = PlanningStrategyConfiguration.default) extends PlanningStrategy {

  import QueryPlanProducer._

  def plan(queries: UnionQuery)(implicit context: LogicalPlanningContext, subQueryLookupTable: Map[PatternExpression, QueryGraph], leafPlan: Option[QueryPlan] = None): QueryPlan = queries match {
    case UnionQuery(Seq(query), false) =>
      val firstPart = planPart(query, leafPlan)
      val projectedFirstPart = planEventHorizon(query, firstPart)
      val finalPlan = plan(projectedFirstPart, query.tail)
      verifyBestPlan(finalPlan, query)

    case _ => throw new CantHandleQueryException
  }

  private def plan(pred: QueryPlan, remaining: Option[PlannerQuery])(implicit context: LogicalPlanningContext, subQueryLookupTable: Map[PatternExpression, QueryGraph]): QueryPlan = remaining match {
    case Some(query) =>
      val lhs = pred
      val rhs = planPart(query, Some(planQueryArgumentRow(query.graph)))
      val applyPlan = planTailApply(lhs, rhs)
      val projectedPlan = planEventHorizon(query, applyPlan)(context, subQueryLookupTable)
      plan(projectedPlan, query.tail)
    case None =>
      pred
  }

  private def planPart(query: PlannerQuery, leafPlan: Option[QueryPlan] = None)(implicit context: LogicalPlanningContext, subQueryLookupTable: Map[PatternExpression, QueryGraph]): QueryPlan = {
    context.strategy.plan(query.graph)(context, subQueryLookupTable, leafPlan)
  }

  private def planEventHorizon(query: PlannerQuery, plan: QueryPlan)(implicit context: LogicalPlanningContext, subQueryLookupTable: Map[PatternExpression, QueryGraph]) = {
    val selectedPlan = config.applySelections(plan, query.graph)
    val queryProjection = query.projection
    val projectedPlan = queryProjection match {
      case aggr: AggregationProjection =>
        val aggregationPlan = aggregation(selectedPlan, aggr)
        sortSkipAndLimit(aggregationPlan, query)

      case _ =>
        val sortedAndLimited = sortSkipAndLimit(selectedPlan, query)
        projection(sortedAndLimited, queryProjection.projections)
    }

    projectedPlan
  }
}
