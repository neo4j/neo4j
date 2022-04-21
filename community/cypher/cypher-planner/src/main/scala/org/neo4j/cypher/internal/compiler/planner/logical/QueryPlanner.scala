/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.phases.AttributeFullyAssigned
import org.neo4j.cypher.internal.compiler.phases.CompilationContains
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.planner.logical.steps.SystemOutCostLogger
import org.neo4j.cypher.internal.compiler.planner.logical.steps.VerifyBestPlan
import org.neo4j.cypher.internal.compiler.planner.logical.steps.devNullListener
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.TokensResolved
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.PlannerQueryPart
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.UnionQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.util.StepSequencer

/**
 * Using cost estimates, plan the query to a logical plan.
 */
case object QueryPlanner
    extends Phase[PlannerContext, LogicalPlanState, LogicalPlanState] with StepSequencer.Step
    with PlanPipelineTransformerFactory {

  override def phase = LOGICAL_PLANNING

  override def process(from: LogicalPlanState, context: PlannerContext): LogicalPlanState = {
    val logicalPlanningContext = getLogicalPlanningContext(from, context)

    // Not using from.returnColumns, since they are the original ones given by the user,
    // whereas the one in the statement might have been rewritten and contain the variables
    // that will actually be available to ProduceResults
    val produceResultColumns = from.statement().returnColumns.map(_.name)
    val logicalPlan = plan(from.query, logicalPlanningContext, produceResultColumns)

    from.copy(
      maybeLogicalPlan = Some(logicalPlan),
      maybeSemanticTable = Some(logicalPlanningContext.semanticTable)
    )
  }

  def getLogicalPlanningContext(from: LogicalPlanState, context: PlannerContext): LogicalPlanningContext = {
    val printCostComparisons =
      context.debugOptions.printCostComparisonsEnabled || java.lang.Boolean.getBoolean("pickBestPlan.VERBOSE")

    val costComparisonListener =
      if (printCostComparisons)
        SystemOutCostLogger
      else
        devNullListener

    val planningAttributes = from.planningAttributes
    val logicalPlanProducer =
      LogicalPlanProducer(context.metrics.cardinality, planningAttributes, context.logicalPlanIdGen)
    LogicalPlanningContext(
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
      idGen = context.logicalPlanIdGen,
      executionModel = context.executionModel,
      debugOptions = context.debugOptions,
      anonymousVariableNameGenerator = from.anonymousVariableNameGenerator,
      cancellationChecker = context.cancellationChecker,
      planningTextIndexesEnabled = context.config.planningTextIndexesEnabled,
      planningRangeIndexesEnabled = context.config.planningRangeIndexesEnabled,
      planningPointIndexesEnabled = context.config.planningPointIndexesEnabled
    )
  }

  private def getMetricsFrom(context: PlannerContext) =
    if (context.debugOptions.inverseCostEnabled) {
      context.metrics.copy(cost =
        (
          v1: LogicalPlan,
          v2: QueryGraphSolverInput,
          v3: SemanticTable,
          v4: Cardinalities,
          v5: ProvidedOrders,
          monitor
        ) => -context.metrics.cost.costFor(v1, v2, v3, v4, v5, monitor)
      )
    } else {
      context.metrics
    }

  def plan(query: PlannerQuery, context: LogicalPlanningContext, produceResultColumns: Seq[String]): LogicalPlan = {
    val plan = plannerQueryPartPlanner.plan(query.query, context)

    val lastInterestingOrder = query.query match {
      case spq: SinglePlannerQuery => Some(spq.last.interestingOrder)
      case _                       => None
    }

    val planWithProduceResults =
      context.logicalPlanProducer.planProduceResult(plan, produceResultColumns, lastInterestingOrder, context)
    VerifyBestPlan(plan = planWithProduceResults, expected = query.query, context = context)
    planWithProduceResults
  }

  override def preConditions: Set[StepSequencer.Condition] = Set(
    // This works on the IR
    CompilationContains[UnionQuery],
    UnnecessaryOptionalMatchesRemoved,
    TokensResolved
  )

  override def postConditions = Set(
    CompilationContains[LogicalPlan],
    AttributeFullyAssigned[Solveds],
    AttributeFullyAssigned[Cardinalities],
    AttributeFullyAssigned[ProvidedOrders]
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): Transformer[PlannerContext, LogicalPlanState, LogicalPlanState] = this

}

/**
 * Combines multiple PlannerQuery plans together with Union
 */
case object plannerQueryPartPlanner {

  private val planSingleQuery = PlanSingleQuery()

  /**
   * Plan a part of a query
   * @param plannerQueryPart the part to plan
   * @param context the context
   * @param distinctifyUnions if `true`, a distinct will be inserted for distinct UNIONs.
   * @return the plan
   */
  def plan(
    plannerQueryPart: PlannerQueryPart,
    context: LogicalPlanningContext,
    distinctifyUnions: Boolean = true
  ): LogicalPlan =
    plannerQueryPart match {
      case pq: SinglePlannerQuery =>
        planSingleQuery.plan(pq, context)
      case UnionQuery(part, query, distinct, unionMappings) =>
        val projectionsForPart = unionMappings.map(um => um.unionVariable.name -> um.variableInPart).toMap
        val projectionsForQuery = unionMappings.map(um => um.unionVariable.name -> um.variableInQuery).toMap

        val partPlan = plan(part, context, distinctifyUnions = false) // Only one distinct at the top level
        val partPlanWithProjection =
          context.logicalPlanProducer.planProjectionForUnionMapping(partPlan, projectionsForPart, context)

        val queryPlan = planSingleQuery.plan(query, context)
        val queryPlanWithProjection =
          context.logicalPlanProducer.planRegularProjection(queryPlan, projectionsForQuery, None, context)

        val unionPlan =
          context.logicalPlanProducer.planUnion(partPlanWithProjection, queryPlanWithProjection, unionMappings, context)
        if (distinct && distinctifyUnions)
          context.logicalPlanProducer.planDistinctForUnion(unionPlan, context)
        else
          unionPlan
    }
}

trait SingleQueryPlanner {
  def plan(in: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan
}
