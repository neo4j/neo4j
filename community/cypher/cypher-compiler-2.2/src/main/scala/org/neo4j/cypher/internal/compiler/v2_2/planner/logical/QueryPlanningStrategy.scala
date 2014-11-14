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

import org.neo4j.cypher.internal.compiler.v2_2.Rewriter
import org.neo4j.cypher.internal.compiler.v2_2.planner._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.rewriter.LogicalPlanRewriter
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps._

class QueryPlanningStrategy(config: PlanningStrategyConfiguration = PlanningStrategyConfiguration.default,
                            rewriter: Rewriter = LogicalPlanRewriter)
  extends PlanningStrategy {

  def plan(unionQuery: UnionQuery)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan] = None): LogicalPlan = unionQuery match {
    case UnionQuery(queries, distinct) =>
      val plan = planQuery(queries, distinct)
      plan.endoRewrite(rewriter)

    case _ => throw new CantHandleQueryException
  }

  private def planQuery(queries: Seq[PlannerQuery], distinct: Boolean)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan] = None) = {
    val logicalPlans: Seq[LogicalPlan] = queries.map(p => planSingleQuery(p))
    val unionPlan = logicalPlans.reduce[LogicalPlan] {
      case (p1, p2) => planUnion(p1, p2)
    }

    if (distinct)
      planDistinct(unionPlan)
    else
      unionPlan
  }

  protected def planSingleQuery(query: PlannerQuery)(implicit context: LogicalPlanningContext, leafPlan: Option[LogicalPlan] = None): LogicalPlan = {
    val firstPart = planPart(query, leafPlan)
    val projectedFirstPart = planEventHorizon(query, firstPart)
    val finalPlan = planWithTail(projectedFirstPart, query.tail)
    verifyBestPlan(finalPlan, query)
  }

  private def planWithTail(pred: LogicalPlan, remaining: Option[PlannerQuery])(implicit context: LogicalPlanningContext): LogicalPlan = remaining match {
    case Some(query) =>
      val lhs = pred
      val newContext = context.recurse(lhs)
      val rhs = planPart(query, Some(planQueryArgumentRow(query.graph)))(newContext)
      val applyPlan = planTailApply(lhs, rhs)
      val projectedPlan = planEventHorizon(query, applyPlan)(context)
      val completePlan = planExpressions(projectedPlan)

      planWithTail(completePlan, query.tail)(newContext)
    case None =>
      pred
  }

  private def planPart(query: PlannerQuery, leafPlan: Option[LogicalPlan])(implicit context: LogicalPlanningContext): LogicalPlan = {
    context.strategy.plan(query.graph)(context, leafPlan)
  }

  private def planEventHorizon(query: PlannerQuery, plan: LogicalPlan)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val selectedPlan = config.applySelections(plan, query.graph)
    val projectedPlan = query.horizon match {
      case aggregatingProjection: AggregatingQueryProjection =>
        val aggregationPlan = aggregation(selectedPlan, aggregatingProjection)
        sortSkipAndLimit(aggregationPlan, query)

      case queryProjection: RegularQueryProjection =>
        val sortedAndLimited = sortSkipAndLimit(selectedPlan, query)
        projection(sortedAndLimited, queryProjection.projections, intermediate = query.tail.isDefined)

      case UnwindProjection(identifier, expression) =>
        planUnwind(plan, identifier, expression)

      case _ =>
        throw new CantHandleQueryException
    }

    projectedPlan
  }

  private def planExpressions(plan: LogicalPlan)(implicit context: LogicalPlanningContext): LogicalPlan = {
    // bottom up rewrite of logical plans
    // for each logical plan
    // plan.mapExpression(f)
    //
    // f: rewrite all expressions, searching for pattern expressions
    // for each pattern epxressions, plan, and replace with NestedPlanExpression
    plan
  }
}
