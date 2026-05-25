/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.PropertyAccess
import org.neo4j.cypher.internal.compiler.phases.AttributeFullyAssigned
import org.neo4j.cypher.internal.compiler.phases.CompilationContains
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.ResolveTokens
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext.Settings
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext.StaticComponents
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.LabelInferenceStrategy
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPLogger
import org.neo4j.cypher.internal.compiler.planner.logical.schema.GraphSchemaOptimizations
import org.neo4j.cypher.internal.compiler.planner.logical.steps.CostComparisonListener
import org.neo4j.cypher.internal.compiler.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.planner.logical.steps.VerifyBestPlan
import org.neo4j.cypher.internal.compiler.planner.logical.steps.projection.MaybeReportedProjections
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.frontend.phases.CopyQuantifiedPathPatternPredicatesToJuxtaposedNodes
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.SchemaInferenceUsageMetricKey
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerConfig
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.UnionQuery
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.ir.ast.IRExpression
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
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

    // Update the schema inference usage metrics
    val maybeLabelInferenceMetricKey = context.labelInferenceStrategy match {
      case _: LabelInferenceStrategy.NoInference.type        => Some(SchemaInferenceUsageMetricKey.OFF)
      case _: LabelInferenceStrategy.InferOnlyIfNoOtherLabel => Some(SchemaInferenceUsageMetricKey.MOST_SELECTIVE_LABEL)
      case _                                                 => None
    }
    maybeLabelInferenceMetricKey.foreach(context.internalUsageStats.incrementSchemaInferenceUsageCount)

    // Not using from.returnColumns, since they are the original ones given by the user,
    // whereas the one in the statement might have been rewritten and contain the variables
    // that will actually be available to ProduceResults
    val produceResultColumns = from.statement().returnColumns

    val logicalPlan =
      try {
        plan(from.query, logicalPlanningContext, produceResultColumns)
      } finally {
        if (context.debugOptions.printIDPLog) {
          logicalPlanningContext.staticComponents.idpLogger.result().foreach(println)
        }
      }

    from.copy(maybeSemanticTable = Some(logicalPlanningContext.semanticTable), maybeLogicalPlan = Some(logicalPlan))
  }

  def getLogicalPlanningContext(from: LogicalPlanState, context: PlannerContext): LogicalPlanningContext = {
    val planningAttributes = from.planningAttributes
    val logicalPlanProducer =
      LogicalPlanProducer(context.metrics.cardinality, planningAttributes, context.logicalPlanIdGen)

    // Graph schema optimizations (GSO) are not enabled
    // - for write queries (see test "Should not use schema optimizations in write queries")
    // - when the transaction state has changed (see test "Should not use schema optimizations when transaction state has changed")
    // Constraints can be violated temporarily within a query, which could lead to wrong results when GSO are
    // enabled for write queries or when the transaction state is not empty
    val enableGraphSchemaOptimizations =
      context.config.planningGraphSchemaOptimizationsEnabled() &&
        from.query.readOnly &&
        !context.planContext.txStateHasChanges()

    val staticComponents = StaticComponents(
      planContext = context.planContext,
      notificationLogger = context.notificationLogger,
      planningAttributes = planningAttributes,
      logicalPlanProducer = logicalPlanProducer,
      queryGraphSolver = context.queryGraphSolver,
      metrics = getMetricsFrom(context),
      idGen = context.logicalPlanIdGen,
      anonymousVariableNameGenerator = from.anonymousVariableNameGenerator,
      cancellationChecker = context.cancellationChecker,
      semanticTable = from.semanticTable(),
      costComparisonListener = CostComparisonListener.givenDebugOptions(context.debugOptions, context.log),
      readOnly = from.query.readOnly,
      labelInferenceStrategy = context.labelInferenceStrategy,
      idpLogger = IDPLogger.givenDebugOptions(context.debugOptions),
      graphSchemaOptimizations =
        GraphSchemaOptimizations.fromConfig(enableGraphSchemaOptimizations, context.planContext),
      planningStepsLogger = PlanningStepsLogger.givenDebugOptions(context.debugOptions)
    )

    val settings = Settings(
      executionModel = context.executionModel,
      updateStrategy = context.updateStrategy,
      debugOptions = context.debugOptions,
      predicatesAsUnionMaxSize = context.config.predicatesAsUnionMaxSize(),
      useErrorsOverWarnings = context.config.useErrorsOverWarnings(),
      errorIfShortestPathFallbackUsedAtRuntime = context.config.errorIfShortestPathFallbackUsedAtRuntime(),
      errorIfShortestPathHasCommonNodesAtRuntime = context.config.errorIfShortestPathHasCommonNodesAtRuntime(),
      legacyCsvQuoteEscaping = context.config.legacyCsvQuoteEscaping(),
      csvBufferSize = context.config.csvBufferSize(),
      planningIntersectionScansEnabled = context.config.planningIntersectionScansEnabled(),
      statefulShortestPlanningRewriteQuantifiersAbove =
        context.config.statefulShortestPlanningRewriteQuantifiersAbove(),
      planVarExpandInto = context.planVarExpandInto,
      remoteBatchPropertiesStrategy = RemoteBatchingStrategy.fromConfig(context),
      shardOperatorPushdownStrategy =
        ShardOperatorPushdownStrategy.fromConfig(from.query, context),
      dynamicLabelScansEnabled = context.config.dynamicLabelScansEnabled(),
      dynamicLabelIndexUseEnabled = context.config.dynamicLabelIndexUseEnabled(),
      existsWithImplicitLimitEnabled = context.config.existsWithImplicitLimitEnabled(),
      selectorCandidatesMaximum = context.config.selectorCandidatesMaximum(),
      planningMergeJoinEnabled = context.config.planningMergeJoinEnabled()
    )

    LogicalPlanningContext(staticComponents, settings)
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
          v6: Set[PropertyAccess],
          v7: GraphStatistics,
          monitor
        ) => -context.metrics.cost.costFor(v1, v2, v3, v4, v5, v6, v7, monitor)
      )
    } else {
      context.metrics
    }

  def plan(
    query: PlannerQuery,
    context: LogicalPlanningContext,
    produceResultColumns: Seq[LogicalVariable]
  ): LogicalPlan = {
    val plan = plannerQueryPlanner.plan(query, context)

    val lastInterestingOrder = query match {
      case spq: SinglePlannerQuery => Some(spq.last.interestingOrder)
      case _                       => None
    }

    val planWithProduceResults =
      context.staticComponents.logicalPlanProducer.planProduceResult(
        plan,
        produceResultColumns,
        lastInterestingOrder,
        context
      )
    VerifyBestPlan(plan = planWithProduceResults, expected = query, context = context)
    planWithProduceResults
  }

  override def preConditions: Set[StepSequencer.Condition] = Set(
    // This works on the IR
    CompilationContains[PlannerQuery](),
    OptionalMatchRemover.completed,
    GetDegreeRewriterStep.completed,
    UnfulfillableQueryRewriter.completed,
    EmptyRelationshipListEndpointProjection.completed,
    VarLengthQuantifierMerger.completed,
    ResolveTokens.completed,
    CopyQuantifiedPathPatternPredicatesToJuxtaposedNodes.completed,
    InlineRelationshipTypePredicates.completed,
    StatefulShortestPlanningHintsInserter.completed,
    LimitBeforeCountRewriter.completed
  )

  override def postConditions: Set[StepSequencer.Condition] = Set(
    CompilationContains[LogicalPlan](),
    AttributeFullyAssigned[Solveds](),
    AttributeFullyAssigned[Cardinalities](),
    AttributeFullyAssigned[ProvidedOrders]()
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(planPipelineConfig: PlanPipelineTransformerConfig)
    : Transformer[PlannerContext, LogicalPlanState, LogicalPlanState] = this

}

/**
 * Combines multiple PlannerQuery plans together with Union
 */
case object plannerQueryPlanner {

  private val planSingleQuery = PlanSingleQuery()

  /**
   * Plan a query
   * @param plannerQuery the query to plan
   * @param context the context
   * @param distinctifyUnions if `true`, a distinct will be inserted for distinct UNIONs.
   * @return the plan
   */
  def plan(
    plannerQuery: PlannerQuery,
    context: LogicalPlanningContext,
    distinctifyUnions: Boolean = true
  ): LogicalPlan =
    plannerQuery match {
      case pq: SinglePlannerQuery =>
        planSingleQuery.plan(pq, context)
      case UnionQuery(lhs, rhs, distinct, unionMappings) =>
        val projectionsForLhs = unionMappings.map(um => um.unionVariable -> um.variableInLhs).toMap
        val projectionsForRhs = unionMappings.map(um => um.unionVariable -> um.variableInRhs).toMap

        val lhsPlan = plan(lhs, context, distinctifyUnions = false) // Only one distinct at the top level
        val lhsPlanWithProjection =
          context.staticComponents.logicalPlanProducer.planProjectionForUnionMapping(
            lhsPlan,
            projectionsForLhs,
            context
          )

        val rhsPlan = planSingleQuery.plan(rhs, context)
        val rhsPlanWithProjection =
          context.staticComponents.logicalPlanProducer.planRegularProjection(
            rhsPlan,
            projectionsForRhs,
            MaybeReportedProjections.empty,
            context
          )

        val unionPlan =
          context.staticComponents.logicalPlanProducer.planUnion(
            lhsPlanWithProjection,
            rhsPlanWithProjection,
            unionMappings,
            context
          )
        if (distinct && distinctifyUnions)
          context.staticComponents.logicalPlanProducer.planDistinctForUnion(unionPlan, context)
        else
          unionPlan
    }

  /**
   * Plan a subquery from an IRExpression, but change the context to include the latest labelInfo.
   */
  def planSubqueryWithLabelInfo(
    outerPlan: LogicalPlan,
    subqueryExpression: IRExpression,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    planSubquery(
      subqueryExpression,
      context.withModifiedPlannerState(_
        .withUpdatedLabelInfo(outerPlan, context.staticComponents.planningAttributes.solveds))
    )
  }

  /**
   * Plan a subquery from an IRExpression with the given context.
   */
  def planSubquery(subqueryExpression: IRExpression, context: LogicalPlanningContext): LogicalPlan = {
    context.staticComponents.idpLogger.markScope("planSubquery") {
      plan(
        subqueryExpression.query,
        context.withModifiedPlannerState(_.forSubquery(
          subqueryExpression.computedScopeDependencies.getOrElse(Set.empty),
          isExistsSubquery = subqueryExpression.isInstanceOf[ExistsIRExpression]
        ))
      )
    }
  }
}
