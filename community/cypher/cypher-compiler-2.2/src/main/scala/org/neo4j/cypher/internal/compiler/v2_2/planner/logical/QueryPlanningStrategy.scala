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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2.ast.{AliasedReturnItem, Identifier}
import org.neo4j.cypher.internal.compiler.v2_2.planner._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps._

class QueryPlanningStrategy(config: PlanningStrategyConfiguration = PlanningStrategyConfiguration.default) extends PlanningStrategy {

  import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.QueryPlanProducer._

  def plan(unionQuery: UnionQuery)(implicit context: LogicalPlanningContext, leafPlan: Option[QueryPlan] = None): LogicalPlan = unionQuery match {
    case UnionQuery(queries, distinct) =>
      val logicalPlans: Seq[LogicalPlan] = queries.map(p => planSingleQuery(p).plan)
      val unionPlan = logicalPlans.reduce[LogicalPlan] {
        case (p1, p2) => Union(p1, p2)
      }

      if (distinct)
        distinctify(unionPlan)
      else
        unionPlan

    case _ => throw new CantHandleQueryException
  }

  protected def planSingleQuery(query: PlannerQuery)(implicit context: LogicalPlanningContext, leafPlan: Option[QueryPlan] = None): QueryPlan = {
    val firstPart = planPart(query, leafPlan)
    val projectedFirstPart = planEventHorizon(query, firstPart)
    val finalPlan = planWithTail(projectedFirstPart, query.tail)
    verifyBestPlan(finalPlan, query)
  }

  private def distinctify(p: LogicalPlan): LogicalPlan = {
    val returnAll = QueryProjection.forIds(p.availableSymbols) map {
      case AliasedReturnItem(e, Identifier(key)) => key -> e // This smells awful.
    }

    Aggregation(p, returnAll.toMap, Map.empty)
  }

  private def planWithTail(pred: QueryPlan, remaining: Option[PlannerQuery])(implicit context: LogicalPlanningContext): QueryPlan = remaining match {
    case Some(query) =>
      val lhs = pred
      val rhs = planPart(query, Some(planQueryArgumentRow(query.graph)))
      val applyPlan = planTailApply(lhs, rhs)
      val projectedPlan = planEventHorizon(query, applyPlan)(context)
      planWithTail(projectedPlan, query.tail)
    case None =>
      pred
  }

  private def planPart(query: PlannerQuery, leafPlan: Option[QueryPlan])(implicit context: LogicalPlanningContext): QueryPlan = {
    context.strategy.plan(query.graph)(context, leafPlan)
  }

  private def planEventHorizon(query: PlannerQuery, plan: QueryPlan)(implicit context: LogicalPlanningContext): QueryPlan = {
    val selectedPlan = config.applySelections(plan, query.graph)
    val projectedPlan = query.horizon match {
      case aggregatingProjection: AggregatingQueryProjection =>
        val aggregationPlan = aggregation(selectedPlan, aggregatingProjection)
        val sortedAndLimited = sortSkipAndLimit(aggregationPlan, query)
        projection(sortedAndLimited, aggregatingProjection.projections.keys.map {
          x => x -> Identifier(x)(null)
        }.toMap)

      case queryProjection: RegularQueryProjection =>
        val projectedPlan = projection(selectedPlan, queryProjection.projections)
        val sortedAndLimited = sortSkipAndLimit(projectedPlan, query)
        projection(sortedAndLimited, queryProjection.projections.keys.map {
          x => x -> Identifier(x)(null)
        }.toMap)

      case UnwindProjection(identifier, expression) =>
        planUnwind(plan, identifier, expression)

      case _ =>
        throw new CantHandleQueryException
    }

    projectedPlan
  }
}
