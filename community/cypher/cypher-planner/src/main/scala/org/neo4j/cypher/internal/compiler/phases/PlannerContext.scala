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
package org.neo4j.cypher.internal.compiler.phases

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.SyntaxExceptionCreator
import org.neo4j.cypher.internal.compiler.UpdateStrategy
import org.neo4j.cypher.internal.compiler.planner.Optimisation
import org.neo4j.cypher.internal.compiler.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics
import org.neo4j.cypher.internal.compiler.planner.logical.MetricsFactory
import org.neo4j.cypher.internal.compiler.planner.logical.QueryGraphSolver
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.LabelInferenceStrategy
import org.neo4j.cypher.internal.frontend.notification.InternalNotificationStats
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.InternalUsageStats
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.notification.InternalNotificationLogger
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.options.CypherInferSchemaPartsOption
import org.neo4j.cypher.internal.options.CypherParallelRepeatHeuristicOption
import org.neo4j.cypher.internal.options.CypherPlanVarExpandInto
import org.neo4j.cypher.internal.options.CypherStatefulShortestPlanningModeOption
import org.neo4j.cypher.internal.options.CypherTransactionBatchStrategyOption
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.messages.MessageUtilProvider
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.logging.Log
import org.neo4j.values.virtual.MapValue

import java.time.Clock

trait PlannerContext extends BaseContext {
  def planContext: PlanContext
  def metrics: Metrics
  def config: CypherPlannerConfiguration
  def queryGraphSolver: QueryGraphSolver
  def updateStrategy: UpdateStrategy
  def debugOptions: CypherDebugOptions
  def clock: Clock
  def logicalPlanIdGen: IdGen
  def params: MapValue
  def executionModel: ExecutionModel
  def materializedEntitiesMode: Boolean
  def statefulShortestPlanningMode: CypherStatefulShortestPlanningModeOption
  def planVarExpandInto: CypherPlanVarExpandInto
  def optimisations: Set[Optimisation]
  def parallelRepeatHeuristic: CypherParallelRepeatHeuristicOption
  def databaseReferenceRepository: DatabaseReferenceRepository
  def databaseId: NamedDatabaseId
  def log: Log
  def securityLog: AbstractSecurityLog
  def internalNotificationStats: InternalNotificationStats
  def labelInferenceStrategy: LabelInferenceStrategy

  /** Resolved batch strategy for `CALL ... IN CONCURRENT TRANSACTIONS` (setting + preparser-option override). */
  def transactionBatchStrategy: CypherTransactionBatchStrategyOption
  def withNotificationLogger(notificationLogger: InternalNotificationLogger): PlannerContext
}

class BaseContextImpl(
  final override val cypherVersion: CypherVersion,
  final override val cypherExceptionFactory: CypherExceptionFactory,
  final override val tracer: CompilationPhaseTracer,
  final override val notificationLogger: InternalNotificationLogger,
  final override val monitors: Monitors,
  final override val cancellationChecker: CancellationChecker,
  final override val internalUsageStats: InternalUsageStats,
  final override val sessionDatabase: DatabaseReference,
  final override val semanticFeatures: Seq[SemanticFeature],
  final override val isScopeQuery: Boolean,
  final override val shadowedFunctions: Set[String],
  final override val isDebugSession: Boolean
) extends BaseContext {

  final override val errorHandler: Seq[SemanticErrorDef] => Unit =
    SyntaxExceptionCreator.throwOnError(cypherExceptionFactory)

  final override def errorMessageProvider: ErrorMessageProvider = MessageUtilProvider
}

object BaseContextImpl {

  def apply(
    cypherVersion: CypherVersion,
    tracer: CompilationPhaseTracer,
    notificationLogger: InternalNotificationLogger,
    queryText: String,
    offset: Option[InputPosition],
    monitors: Monitors,
    cancellationChecker: CancellationChecker,
    internalSyntaxUsageStats: InternalUsageStats,
    sessionDatabase: DatabaseReference,
    semanticFeatures: Seq[SemanticFeature],
    isScopeQuery: Boolean,
    shadowedFunctions: Set[String],
    isDebugSession: Boolean
  ): BaseContextImpl = {
    val exceptionFactory = Neo4jCypherExceptionFactory(queryText, offset)
    new BaseContextImpl(
      cypherVersion,
      exceptionFactory,
      tracer,
      notificationLogger,
      monitors,
      cancellationChecker,
      internalSyntaxUsageStats,
      sessionDatabase,
      semanticFeatures,
      isScopeQuery,
      shadowedFunctions,
      isDebugSession
    )
  }
}

final class PlannerContextImpl(
  override val cypherVersion: CypherVersion,
  override val cypherExceptionFactory: CypherExceptionFactory,
  override val tracer: CompilationPhaseTracer,
  override val notificationLogger: InternalNotificationLogger,
  override val planContext: PlanContext,
  override val monitors: Monitors,
  override val metrics: Metrics,
  override val config: CypherPlannerConfiguration,
  override val queryGraphSolver: QueryGraphSolver,
  override val updateStrategy: UpdateStrategy,
  override val debugOptions: CypherDebugOptions,
  override val clock: Clock,
  override val logicalPlanIdGen: IdGen,
  override val params: MapValue,
  override val executionModel: ExecutionModel,
  override val cancellationChecker: CancellationChecker,
  override val materializedEntitiesMode: Boolean,
  override val statefulShortestPlanningMode: CypherStatefulShortestPlanningModeOption,
  override val planVarExpandInto: CypherPlanVarExpandInto,
  override val optimisations: Set[Optimisation],
  override val parallelRepeatHeuristic: CypherParallelRepeatHeuristicOption,
  override val databaseReferenceRepository: DatabaseReferenceRepository,
  override val databaseId: NamedDatabaseId,
  override val log: Log,
  override val securityLog: AbstractSecurityLog,
  override val internalNotificationStats: InternalNotificationStats,
  override val internalUsageStats: InternalUsageStats,
  override val labelInferenceStrategy: LabelInferenceStrategy,
  override val sessionDatabase: DatabaseReference,
  override val semanticFeatures: Seq[SemanticFeature],
  override val shadowedFunctions: Set[String],
  override val transactionBatchStrategy: CypherTransactionBatchStrategyOption
) extends PlannerContext {

  override val errorHandler: Seq[SemanticErrorDef] => Unit =
    SyntaxExceptionCreator.throwOnError(cypherExceptionFactory)

  override def errorMessageProvider: ErrorMessageProvider = MessageUtilProvider

  def withNotificationLogger(notificationLogger: InternalNotificationLogger): PlannerContext = new PlannerContextImpl(
    cypherVersion = cypherVersion,
    cypherExceptionFactory = cypherExceptionFactory,
    tracer = tracer,
    notificationLogger = notificationLogger,
    planContext = planContext.withNotificationLogger(notificationLogger),
    monitors = monitors,
    metrics = metrics,
    config = config,
    queryGraphSolver = queryGraphSolver,
    updateStrategy = updateStrategy,
    debugOptions = debugOptions,
    clock = clock,
    logicalPlanIdGen = logicalPlanIdGen,
    params = params,
    executionModel = executionModel,
    cancellationChecker = cancellationChecker,
    materializedEntitiesMode = materializedEntitiesMode,
    statefulShortestPlanningMode = statefulShortestPlanningMode,
    planVarExpandInto = planVarExpandInto,
    optimisations = optimisations,
    parallelRepeatHeuristic = parallelRepeatHeuristic,
    databaseReferenceRepository = databaseReferenceRepository,
    databaseId = databaseId,
    log = log,
    securityLog = securityLog,
    internalNotificationStats = internalNotificationStats,
    internalUsageStats = internalUsageStats,
    labelInferenceStrategy = labelInferenceStrategy,
    sessionDatabase = sessionDatabase,
    semanticFeatures = semanticFeatures,
    shadowedFunctions = shadowedFunctions,
    transactionBatchStrategy = transactionBatchStrategy
  )

  override def isScopeQuery: Boolean = false
  override def isDebugSession: Boolean = false
}

object PlannerContext {

  def apply(
    cypherVersion: CypherVersion,
    tracer: CompilationPhaseTracer,
    notificationLogger: InternalNotificationLogger,
    planContext: PlanContext,
    queryText: String,
    debugOptions: CypherDebugOptions,
    executionModel: ExecutionModel,
    offset: Option[InputPosition],
    monitors: Monitors,
    metricsFactory: MetricsFactory,
    queryGraphSolver: QueryGraphSolver,
    config: CypherPlannerConfiguration,
    updateStrategy: UpdateStrategy,
    clock: Clock,
    logicalPlanIdGen: IdGen,
    evaluator: ExpressionEvaluator,
    params: MapValue,
    cancellationChecker: CancellationChecker,
    materializedEntitiesMode: Boolean,
    labelInference: CypherInferSchemaPartsOption,
    statefulShortestPlanningMode: CypherStatefulShortestPlanningModeOption,
    planVarExpandInto: CypherPlanVarExpandInto,
    optimisations: Set[Optimisation],
    parallelRepeatHeuristic: CypherParallelRepeatHeuristicOption,
    databaseReferenceRepository: DatabaseReferenceRepository,
    databaseId: NamedDatabaseId,
    log: Log,
    securityLog: AbstractSecurityLog,
    internalNotificationStats: InternalNotificationStats,
    internalUsageStats: InternalUsageStats,
    sessionDatabase: DatabaseReference,
    semanticFeatures: Seq[SemanticFeature],
    shadowedFunctions: Set[String],
    transactionBatchStrategy: CypherTransactionBatchStrategyOption
  ): PlannerContextImpl = {
    val exceptionFactory = Neo4jCypherExceptionFactory(queryText, offset)
    val labelInferenceStrategy = LabelInferenceStrategy.fromConfig(planContext, labelInference, optimisations)

    val metrics = metricsFactory.newMetrics(
      planContext,
      evaluator,
      executionModel,
      cancellationChecker,
      labelInferenceStrategy
    )

    new PlannerContextImpl(
      cypherVersion,
      exceptionFactory,
      tracer,
      notificationLogger,
      planContext,
      monitors,
      metrics,
      config,
      queryGraphSolver,
      updateStrategy,
      debugOptions,
      clock,
      logicalPlanIdGen,
      params,
      executionModel,
      cancellationChecker,
      materializedEntitiesMode,
      statefulShortestPlanningMode,
      planVarExpandInto,
      optimisations,
      parallelRepeatHeuristic,
      databaseReferenceRepository,
      databaseId,
      log,
      securityLog,
      internalNotificationStats,
      internalUsageStats,
      labelInferenceStrategy,
      sessionDatabase,
      semanticFeatures = semanticFeatures,
      shadowedFunctions = shadowedFunctions,
      transactionBatchStrategy = transactionBatchStrategy
    )
  }
}
