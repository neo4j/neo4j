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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical

import org.neo4j.cypher.internal.compiler.v3_2.phases._
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.{LogicalPlan, ProduceResult}
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.frontend.v3_2.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.frontend.v3_2.phases.Phase
import org.neo4j.cypher.internal.ir.v3_2.exception.CantHandleQueryException
import org.neo4j.cypher.internal.ir.v3_2.{PeriodicCommit, PlannerQuery, UnionQuery}

case class QueryPlanner(planSingleQuery: LogicalPlanningFunction1[PlannerQuery, LogicalPlan] = PlanSingleQuery()) extends Phase[CompilerContext, CompilationState, CompilationState] {

  override def phase = LOGICAL_PLANNING

  override def description = "using cost estimates, plan the query to a logical plan"

  override def postConditions = Set(CompilationContains[LogicalPlan])

  override def process(from: CompilationState, context: CompilerContext): CompilationState = {
    val logicalPlanProducer = LogicalPlanProducer(context.metrics.cardinality)
    val logicalPlanningContext = LogicalPlanningContext(
      planContext = context.planContext,
      logicalPlanProducer = logicalPlanProducer,
      metrics = context.metrics,
      semanticTable = from.semanticTable,
      strategy = context.queryGraphSolver,
      notificationLogger = context.notificationLogger,
      useErrorsOverWarnings = context.config.useErrorsOverWarnings,
      errorIfShortestPathFallbackUsedAtRuntime = context.config.errorIfShortestPathFallbackUsedAtRuntime,
      errorIfShortestPathHasCommonNodesAtRuntime = context.config.errorIfShortestPathHasCommonNodesAtRuntime,
      config = QueryPlannerConfiguration.default.withUpdateStrategy(context.updateStrategy),
      legacyCsvQuoteEscaping = context.config.legacyCsvQuoteEscaping
    )

    val (perCommit, logicalPlan) = plan(from.unionQuery)(logicalPlanningContext)

    from.copy(maybePeriodicCommit = Some(perCommit), maybeLogicalPlan = Some(logicalPlan))
  }

  def plan(unionQuery: UnionQuery)(implicit context: LogicalPlanningContext): (Option[PeriodicCommit], LogicalPlan) =
    unionQuery match {
      case UnionQuery(queries, distinct, _, periodicCommitHint) =>
        val plan = planQueries(queries, distinct)
        (periodicCommitHint, createProduceResultOperator(plan, unionQuery))

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
