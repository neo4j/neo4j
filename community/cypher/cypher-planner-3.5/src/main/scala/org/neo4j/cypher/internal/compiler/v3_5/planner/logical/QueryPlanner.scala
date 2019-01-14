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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical

import org.neo4j.cypher.internal.compiler.v3_5.phases._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.Metrics.{CostModel, QueryGraphSolverInput}
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps.{LogicalPlanProducer, SystemOutCostLogger, devNullListener}
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.cypher.internal.v3_5.frontend.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.v3_5.frontend.phases.Phase
import org.neo4j.cypher.internal.v3_5.util.Cost
import org.neo4j.cypher.internal.v3_5.util.attribution.IdGen

case class QueryPlanner(planSingleQuery: SingleQueryPlanner = PlanSingleQuery())
  extends Phase[PlannerContext, LogicalPlanState, LogicalPlanState] {


  override def phase = LOGICAL_PLANNING

  override def description = "using cost estimates, plan the query to a logical plan"

  override def postConditions = Set(CompilationContains[LogicalPlan])

  override def process(from: LogicalPlanState, context: PlannerContext): LogicalPlanState = {
    val debugCosts = context.debugOptions.contains("dumpcosts")

    val costComparisonListener = if (debugCosts)
      new ReportCostComparisonsAsRows
    else if (java.lang.Boolean.getBoolean("pickBestPlan.VERBOSE"))
      SystemOutCostLogger
    else
      devNullListener

    val planningAttributes = from.planningAttributes
    val logicalPlanProducer = LogicalPlanProducer(context.metrics.cardinality, planningAttributes, context.logicalPlanIdGen)
    val logicalPlanningContext = LogicalPlanningContext(
      planContext = context.planContext,
      logicalPlanProducer = logicalPlanProducer,
      metrics = getMetricsFrom(context),
      semanticTable = from.semanticTable(),
      strategy = context.queryGraphSolver,
      notificationLogger = context.notificationLogger,
      useErrorsOverWarnings = context.config.useErrorsOverWarnings,
      errorIfShortestPathFallbackUsedAtRuntime = context.config.errorIfShortestPathFallbackUsedAtRuntime,
      errorIfShortestPathHasCommonNodesAtRuntime = context.config.errorIfShortestPathHasCommonNodesAtRuntime,
      config = QueryPlannerConfiguration.default.withUpdateStrategy(context.updateStrategy),
      legacyCsvQuoteEscaping = context.config.legacyCsvQuoteEscaping,
      csvBufferSize = context.config.csvBufferSize,
      costComparisonListener = costComparisonListener,
      planningAttributes = planningAttributes
    )

    val (perCommit, logicalPlan, newLogicalPlanningContext) = plan(from.unionQuery, logicalPlanningContext, planningAttributes.solveds, planningAttributes.cardinalities, context.logicalPlanIdGen)

    costComparisonListener match {
      case debug: ReportCostComparisonsAsRows => debug.addPlan(from)
      case _ => from.copy(
        maybePeriodicCommit = Some(perCommit),
        maybeLogicalPlan = Some(logicalPlan),
        maybeSemanticTable = Some(newLogicalPlanningContext.semanticTable))
    }
  }

  private def getMetricsFrom(context: PlannerContext) = if (context.debugOptions.contains("inverse_cost")) {
    context.metrics.copy(cost = new CostModel {
      override def apply(v1: LogicalPlan, v2: QueryGraphSolverInput, v3: Cardinalities): Cost = -context.metrics.cost(v1, v2, v3)
    })
  } else {
    context.metrics
  }

  def plan(unionQuery: UnionQuery, context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities, idGen: IdGen): (Option[PeriodicCommit], LogicalPlan, LogicalPlanningContext) =
    unionQuery match {
      case UnionQuery(queries, distinct, _, periodicCommitHint) =>
        val (plan, newContext) = planQueries(queries, distinct, context, idGen)
        (periodicCommitHint, createProduceResultOperator(plan, unionQuery, newContext), newContext)
    }

  private def createProduceResultOperator(in: LogicalPlan,
                                          unionQuery: UnionQuery,
                                          context: LogicalPlanningContext): LogicalPlan =
    context.logicalPlanProducer.planProduceResult(in, unionQuery.returns, context)

  private def planQueries(queries: Seq[PlannerQuery], distinct: Boolean, context: LogicalPlanningContext, idGen: IdGen) = {
    val (logicalPlans, finalContext) = queries.foldLeft((Seq.empty[LogicalPlan], context)) {
      case ((plans, currentContext), currentQuery) =>
        val (singlePlan, newContext) = planSingleQuery(currentQuery, context, idGen)
        (plans :+ singlePlan, newContext)
    }
    val unionPlan = logicalPlans.reduce[LogicalPlan] {
      case (p1, p2) => finalContext.logicalPlanProducer.planUnion(p1, p2, finalContext)
    }

    if (distinct)
      (finalContext.logicalPlanProducer.planDistinctStar(unionPlan, finalContext), finalContext)
    else
      (unionPlan, finalContext)
  }
}

case object planPart extends PartPlanner {

  def apply(query: PlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    val ctx = query.preferredStrictness match {
      case Some(mode) if !context.input.strictness.contains(mode) => context.withStrictness(mode)
      case _ => context
    }
    ctx.strategy.plan(query.queryGraph, query.interestingOrder, ctx)
  }
}

trait SingleQueryPlanner {
  def apply(in: PlannerQuery, context: LogicalPlanningContext, idGen: IdGen): (LogicalPlan, LogicalPlanningContext)
}
