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
import org.neo4j.cypher.internal.options.CypherPlanVarExpandInto
import org.neo4j.cypher.internal.options.CypherStatefulShortestPlanningModeOption
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.messages.MessageUtilProvider
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.logging.Log
import org.neo4j.values.virtual.MapValue

import java.time.Clock

class BaseContextImpl(
  override val cypherVersion: CypherVersion,
  override val cypherExceptionFactory: CypherExceptionFactory,
  override val tracer: CompilationPhaseTracer,
  override val notificationLogger: InternalNotificationLogger,
  override val monitors: Monitors,
  override val cancellationChecker: CancellationChecker,
  override val internalUsageStats: InternalUsageStats,
  val sessionDatabase: DatabaseReference,
  override val semanticFeatures: Seq[SemanticFeature],
  override val isScopeQuery: Boolean
) extends BaseContext {

  override val errorHandler: Seq[SemanticErrorDef] => Unit =
    SyntaxExceptionCreator.throwOnError(cypherExceptionFactory)

  override val errorMessageProvider: ErrorMessageProvider = MessageUtilProvider
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
    isScopeQuery: Boolean
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
      isScopeQuery
    )
  }
}

class PlannerContext(
  cypherVersion: CypherVersion,
  cypherExceptionFactory: CypherExceptionFactory,
  tracer: CompilationPhaseTracer,
  notificationLogger: InternalNotificationLogger,
  val planContext: PlanContext,
  monitors: Monitors,
  val metrics: Metrics,
  val config: CypherPlannerConfiguration,
  val queryGraphSolver: QueryGraphSolver,
  val updateStrategy: UpdateStrategy,
  val debugOptions: CypherDebugOptions,
  val clock: Clock,
  val logicalPlanIdGen: IdGen,
  val params: MapValue,
  val executionModel: ExecutionModel,
  cancellationChecker: CancellationChecker,
  val materializedEntitiesMode: Boolean,
  val statefulShortestPlanningMode: CypherStatefulShortestPlanningModeOption,
  val planVarExpandInto: CypherPlanVarExpandInto,
  val databaseReferenceRepository: DatabaseReferenceRepository,
  val databaseId: NamedDatabaseId,
  val log: Log,
  val internalNotificationStats: InternalNotificationStats,
  internalSyntaxUsageStats: InternalUsageStats,
  val labelInferenceStrategy: LabelInferenceStrategy,
  override val sessionDatabase: DatabaseReference,
  override val semanticFeatures: Seq[SemanticFeature] = Seq()
) extends BaseContextImpl(
      cypherVersion,
      cypherExceptionFactory,
      tracer,
      notificationLogger,
      monitors,
      cancellationChecker,
      internalSyntaxUsageStats,
      sessionDatabase,
      semanticFeatures,
      false
    ) {

  /**
   * Return a copy with the given notificationLogger
   */
  def withNotificationLogger(notificationLogger: InternalNotificationLogger): PlannerContext = {
    val newPlanContext = planContext.withNotificationLogger(notificationLogger)
    new PlannerContext(
      cypherVersion,
      cypherExceptionFactory,
      tracer,
      notificationLogger,
      newPlanContext,
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
      databaseReferenceRepository,
      databaseId,
      log,
      internalNotificationStats,
      internalSyntaxUsageStats,
      labelInferenceStrategy,
      sessionDatabase
    )
  }
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
    databaseReferenceRepository: DatabaseReferenceRepository,
    databaseId: NamedDatabaseId,
    log: Log,
    internalNotificationStats: InternalNotificationStats,
    internalUsageStats: InternalUsageStats,
    sessionDatabase: DatabaseReference
  ): PlannerContext = {
    val exceptionFactory = Neo4jCypherExceptionFactory(queryText, offset)

    val labelInferenceStrategy = LabelInferenceStrategy.fromConfig(planContext, labelInference)

    val metrics = metricsFactory.newMetrics(
      planContext,
      evaluator,
      executionModel,
      cancellationChecker,
      labelInferenceStrategy
    )

    new PlannerContext(
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
      databaseReferenceRepository,
      databaseId,
      log,
      internalNotificationStats,
      internalUsageStats,
      labelInferenceStrategy,
      sessionDatabase
    )
  }
}
