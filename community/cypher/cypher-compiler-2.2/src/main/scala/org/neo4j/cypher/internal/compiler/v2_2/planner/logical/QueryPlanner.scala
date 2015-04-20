/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.planner._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.rewriter.LogicalPlanRewriter
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.{aggregation, projection, sortSkipAndLimit, verifyBestPlan}
import org.neo4j.cypher.internal.compiler.v2_2.tracing.rewriters.RewriterStepSequencer

import scala.annotation.tailrec

trait QueryPlanner {
  def plan(plannerQuery: UnionQuery)(implicit context: LogicalPlanningContext): LogicalPlan
}

class DefaultQueryPlanner(planRewriter: Rewriter,
                          config: QueryPlannerConfiguration = QueryPlannerConfiguration.default,
                          expressionRewriterFactory: (LogicalPlanningContext => Rewriter) = ExpressionRewriterFactory)
  extends QueryPlanner {

  def plan(unionQuery: UnionQuery)(implicit context: LogicalPlanningContext): LogicalPlan = unionQuery match {
    case UnionQuery(queries, distinct) =>
      val plan = planQuery(queries, distinct)
      plan.endoRewrite(planRewriter)

    case _ =>
      throw new CantHandleQueryException
  }

  private def planQuery(queries: Seq[PlannerQuery], distinct: Boolean)(implicit context: LogicalPlanningContext) = {
    val logicalPlans: Seq[LogicalPlan] = queries.map(p => planSingleQuery(p))
    val unionPlan = logicalPlans.reduce[LogicalPlan] {
      case (p1, p2) => context.logicalPlanProducer.planUnion(p1, p2)
    }

    if (distinct)
      context.logicalPlanProducer.planDistinct(unionPlan)
    else
      unionPlan
  }

  protected def planSingleQuery(query: PlannerQuery)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val partPlan = planPart(query, context, None)

    val projectedPlan = planEventHorizon(query, partPlan)
    val projectedContext = context.recurse(projectedPlan)
    val expressionRewriter = expressionRewriterFactory(projectedContext)
    val completePlan = projectedPlan.endoRewrite(expressionRewriter)

    val finalPlan = planWithTail(completePlan, query.tail)(projectedContext)
    verifyBestPlan(finalPlan, query)
  }

  @tailrec
  private def planWithTail(pred: LogicalPlan, remaining: Option[PlannerQuery])(implicit context: LogicalPlanningContext): LogicalPlan =
    remaining match {
      case Some(query) =>
        val lhs = pred
        val lhsContext = context.recurse(lhs)
        val rhs = planPart(query, lhsContext, Some(context.logicalPlanProducer.planQueryArgumentRow(query.graph)))
        val applyPlan = context.logicalPlanProducer.planTailApply(lhs, rhs)

        val applyContext = lhsContext.recurse(applyPlan)
        val projectedPlan = planEventHorizon(query, applyPlan)(applyContext)

        val projectedContext = applyContext.recurse(projectedPlan)
        val expressionRewriter = expressionRewriterFactory(projectedContext)
        val completePlan = projectedPlan.endoRewrite(expressionRewriter)

        // planning nested expressions doesn't change outer cardinality
        planWithTail(completePlan, query.tail)(projectedContext)

      case None =>
        pred
    }

  private def planPart(query: PlannerQuery, context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]): LogicalPlan = {
    val ctx = query.preferredStrictness match {
      case Some(mode) if !context.input.strictness.exists(mode == _) => context.withStrictness(mode)
      case _ => context
    }
    ctx.strategy.plan(query.graph)(ctx, leafPlan)
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
        context.logicalPlanProducer.planUnwind(plan, identifier, expression)

      case _ =>
        throw new CantHandleQueryException
    }

    projectedPlan
  }
}

