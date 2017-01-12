/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical

import org.neo4j.cypher.internal.compiler.v3_0.planner._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.{LogicalPlan, ProduceResult}
import org.neo4j.cypher.internal.frontend.v3_0.Rewriter

trait QueryPlanner {
  def plan(plannerQuery: UnionQuery)(implicit context: LogicalPlanningContext): (Option[PeriodicCommit], LogicalPlan)
}

case class DefaultQueryPlanner(planRewriter: Rewriter,
                               planSingleQuery: LogicalPlanningFunction1[PlannerQuery, LogicalPlan] = PlanSingleQuery())
  extends QueryPlanner {

  def plan(unionQuery: UnionQuery)(implicit context: LogicalPlanningContext): (Option[PeriodicCommit], LogicalPlan) = unionQuery match {
    case UnionQuery(queries, distinct, returns, periodicCommitHint) =>
      val plan = planQueries(queries, distinct)
      val rewrittenPlan = plan.endoRewrite(planRewriter)
      (periodicCommitHint, createProduceResultOperator(rewrittenPlan, unionQuery))

    case _ =>
      throw new CantHandleQueryException
  }

  private def createProduceResultOperator(in: LogicalPlan, unionQuery: UnionQuery)
                                         (implicit context: LogicalPlanningContext): LogicalPlan = {
    val columns = unionQuery.returns.map(_.name)
    ProduceResult(columns, in)
  }

  private def planQueries(queries: Seq[PlannerQuery], distinct: Boolean)(implicit context: LogicalPlanningContext) = {
    val logicalPlans: Seq[LogicalPlan] = queries.map(p => planSingleQuery(p))
    val unionPlan = logicalPlans.reduce[LogicalPlan] {
      case (p1, p2) => context.logicalPlanProducer.planUnion(p1, p2)
    }

    if (distinct)
      context.logicalPlanProducer.planDistinct(unionPlan)
    else
      unionPlan
  }
}

case object planPart extends ((PlannerQuery, LogicalPlanningContext) => LogicalPlan) {

  def apply(query: PlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    val ctx = query.preferredStrictness match {
      case Some(mode) if !context.input.strictness.contains(mode) => context.withStrictness(mode)
      case _ => context
    }
    ctx.strategy.plan(query.queryGraph)(ctx)
  }
}
