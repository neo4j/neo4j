/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.phases._
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.{CostModel, QueryGraphSolverInput}
import org.neo4j.cypher.internal.compiler.planner.logical.steps.{LogicalPlanProducer, SystemOutCostLogger, devNullListener, verifyBestPlan}
import org.neo4j.cypher.internal.ir._
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.v4_0.frontend.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.v4_0.frontend.phases.Phase
import org.neo4j.cypher.internal.v4_0.util.Cost

case object QueryPlanner
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
      planningAttributes = planningAttributes,
      innerVariableNamer = context.innerVariableNamer,
      idGen = context.logicalPlanIdGen
    )

    // Not using from.returnColumns, since they are the original ones given by the user,
    // whereas the one in the statement might have been rewritten and contain the variables
    // that will actually be available to ProduceResults
    val produceResultColumns = from.statement().returnColumns.map(_.name)
    val (perCommit, logicalPlan, newLogicalPlanningContext) = plan(from.query, logicalPlanningContext, produceResultColumns)

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

  def plan(query: PlannerQuery, context: LogicalPlanningContext, produceResultColumns: Seq[String]): (Option[PeriodicCommit], LogicalPlan, LogicalPlanningContext) = {
    val (plan, newContext) = plannerQueryPartPlanner.plan(query.query, context)
    val planWithProduceResults = createProduceResultOperator(plan, produceResultColumns, newContext)
    verifyBestPlan(planWithProduceResults, query.query,newContext)
    (query.periodicCommit, planWithProduceResults, newContext)
  }

  private def createProduceResultOperator(in: LogicalPlan,
                                          produceResultColumns: Seq[String],
                                          context: LogicalPlanningContext): LogicalPlan =
    context.logicalPlanProducer.planProduceResult(in, produceResultColumns, context)

}

/**
  * Combines multiple PlannerQuery plans together with Union
  */
case object plannerQueryPartPlanner {

  /**
   * Plan a part of a query
   * @param plannerQueryPart the part to plan
   * @param context the context
   * @param distinctifyUnions if `true`, a distinct will be inserted for distinct UNIONs.
   * @return the plan
   */
  def plan(plannerQueryPart: PlannerQueryPart, context: LogicalPlanningContext, distinctifyUnions: Boolean = true): (LogicalPlan, LogicalPlanningContext) =
    plannerQueryPart match {
      case pq:SinglePlannerQuery =>
        PlanSingleQuery()(pq, context)
      case UnionQuery(part, query, distinct, unionMappings) =>
        val projectionsForPart = unionMappings.map(um => um.unionVariable.name -> um.variableInPart).toMap
        val projectionsForQuery = unionMappings.map(um => um.unionVariable.name -> um.variableInQuery).toMap

        val (partPlan, partContext) = plan(part, context, distinctifyUnions = false) // Only one distinct at the top level
        val partPlanWithProjection = partContext.logicalPlanProducer.planProjectionForUnionMapping(partPlan, projectionsForPart, partContext)

        val (queryPlan, finalContext) = PlanSingleQuery()(query, partContext)
        val queryPlanWithProjection = finalContext.logicalPlanProducer.planRegularProjection(queryPlan, projectionsForQuery, Map.empty, finalContext)

        val unionPlan = finalContext.logicalPlanProducer.planUnion(partPlanWithProjection, queryPlanWithProjection, unionMappings, finalContext)
        if (distinct && distinctifyUnions)
          (finalContext.logicalPlanProducer.planDistinctForUnion(unionPlan, finalContext), finalContext)
        else
          (unionPlan, finalContext)
    }
}

case object planPart extends PartPlanner {

  def apply(query: SinglePlannerQuery, context: LogicalPlanningContext, rhsPart: Boolean = false): LogicalPlan = {
    val ctx = query.preferredStrictness match {
      case Some(mode) if !context.input.strictness.contains(mode) => context.withStrictness(mode)
      case _ => context
    }
    ctx.strategy.plan(query.queryGraph, interestingOrderForPart(query, rhsPart), ctx)
  }

  // Extract the interesting InterestingOrder for this part of the query
  // If the required order has dependency on argument, then it should not solve the ordering here
  // If we have a mutating pattern that depends on the sorting variables, we cannot solve ordering here
  private def interestingOrderForPart(query: SinglePlannerQuery, isRhs: Boolean) = {
    val interestingOrder = query.interestingOrder
    if (isRhs)
      interestingOrder.asInteresting
    else
      query.horizon match {
        case _: AggregatingQueryProjection | _: DistinctQueryProjection =>
          interestingOrder.asInteresting

        case _ =>
          val orderCandidate = interestingOrder.requiredOrderCandidate.order
          val orderingDependencies = orderCandidate.flatMap(_.projections).flatMap(_._2.dependencies) ++ orderCandidate.flatMap(_.expression.dependencies)
          val mutatingDependencies = query.queryGraph.mutatingPatterns.flatMap(_.dependencies)
          if (orderingDependencies.exists(dep => query.queryGraph.argumentIds.contains(dep.name) || mutatingDependencies.contains(dep.name)))
            interestingOrder.asInteresting
          else
            interestingOrder
      }
  }
}

trait SingleQueryPlanner {
  def apply(in: SinglePlannerQuery, context: LogicalPlanningContext): (LogicalPlan, LogicalPlanningContext)
}
