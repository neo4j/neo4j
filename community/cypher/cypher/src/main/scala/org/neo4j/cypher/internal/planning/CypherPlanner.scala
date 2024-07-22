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
package org.neo4j.cypher.internal.planning

import org.neo4j.cypher.internal.AdministrationCommandRuntime
import org.neo4j.cypher.internal.CompilerWithExpressionCodeGenOption
import org.neo4j.cypher.internal.CypherQueryObfuscator
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.FineToReuse
import org.neo4j.cypher.internal.FullyParsedQuery
import org.neo4j.cypher.internal.MaybeReusable
import org.neo4j.cypher.internal.PlanFingerprint
import org.neo4j.cypher.internal.PlanFingerprintReference
import org.neo4j.cypher.internal.PreParsedQuery
import org.neo4j.cypher.internal.QueryOptions
import org.neo4j.cypher.internal.ReusabilityState
import org.neo4j.cypher.internal.SchemaCommandRuntime
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.cache.CypherQueryCaches
import org.neo4j.cypher.internal.cache.CypherQueryCaches.AstCache
import org.neo4j.cypher.internal.cache.CypherQueryCaches.AstCache.AstCacheValue
import org.neo4j.cypher.internal.cache.CypherQueryCaches.LogicalPlanCache
import org.neo4j.cypher.internal.cache.CypherQueryCaches.LogicalPlanCache.CacheableLogicalPlan
import org.neo4j.cypher.internal.compiler
import org.neo4j.cypher.internal.compiler.CypherParsingConfig
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.ExecutionModel.BatchedParallel
import org.neo4j.cypher.internal.compiler.ExecutionModel.BatchedSingleThreaded
import org.neo4j.cypher.internal.compiler.ExecutionModel.Volcano
import org.neo4j.cypher.internal.compiler.UpdateStrategy
import org.neo4j.cypher.internal.compiler.defaultUpdateStrategy
import org.neo4j.cypher.internal.compiler.eagerUpdateStrategy
import org.neo4j.cypher.internal.compiler.phases.CachableLogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.logical.CachedSimpleMetricsFactory
import org.neo4j.cypher.internal.compiler.planner.logical.idp.ComponentConnectorPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.idp.ConfigurableIDPSolverConfig
import org.neo4j.cypher.internal.compiler.planner.logical.idp.DPSolverConfig
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPQueryGraphSolver
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPQueryGraphSolverMonitor
import org.neo4j.cypher.internal.compiler.planner.logical.idp.SingleComponentPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.idp.cartesianProductsOrValueJoins
import org.neo4j.cypher.internal.compiler.planner.logical.simpleExpressionEvaluator
import org.neo4j.cypher.internal.compiler.planner.logical.steps.ExistsSubqueryPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.ExistsSubqueryPlannerWithCaching
import org.neo4j.cypher.internal.evaluator.SimpleInternalExpressionEvaluator
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.SensitiveLiteral
import org.neo4j.cypher.internal.expressions.SensitiveParameter
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.InternalSyntaxUsageStats
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
import org.neo4j.cypher.internal.logical.plans.AdministrationCommandLogicalPlan
import org.neo4j.cypher.internal.logical.plans.LoadCSV
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.SystemProcedureCall
import org.neo4j.cypher.internal.options.CypherConnectComponentsPlannerOption
import org.neo4j.cypher.internal.options.CypherPlannerOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.options.CypherUpdateStrategy
import org.neo4j.cypher.internal.planner.spi.CostBasedPlannerName
import org.neo4j.cypher.internal.planner.spi.DPPlannerName
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planning.CypherPlanner.createQueryGraphSolver
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.cypher.internal.spi.ExceptionTranslatingPlanContext
import org.neo4j.cypher.internal.spi.TransactionBoundPlanContext
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.ComposedNotificationLogger
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.cypher.internal.util.InternalNotificationStats
import org.neo4j.cypher.internal.util.RecordingNotificationLogger
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.devNullLogger
import org.neo4j.exceptions.Neo4jException
import org.neo4j.exceptions.NotSystemDatabaseException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.kernel.api.query.QueryObfuscator
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.kernel.impl.api.SchemaStateKey
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.logging.InternalLog
import org.neo4j.monitoring
import org.neo4j.notifications.MissingParametersNotification
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder

import java.time.Clock

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object CypherPlanner {

  /**
   * This back-door is intended for quick handling of bugs and support cases
   * where we need to inject some specific indexes and statistics.
   */
  var customPlanContextCreator
    : Option[(TransactionalContextWrapper, InternalNotificationLogger, InternalLog, CypherVersion) => PlanContext] =
    None

  /**
   * Create a Query Graph solver that matches the configurations and pre-parser options.
   */
  private[planning] def createQueryGraphSolver(
    config: CypherPlannerConfiguration,
    plannerOption: CypherPlannerOption,
    connectComponentsPlannerOption: CypherConnectComponentsPlannerOption,
    disableExistsSubqueryCaching: Boolean,
    monitors: Monitors
  ): IDPQueryGraphSolver = {
    val plannerName: CostBasedPlannerName =
      plannerOption match {
        case CypherPlannerOption.default                        => CostBasedPlannerName.default
        case CypherPlannerOption.cost | CypherPlannerOption.idp => IDPPlannerName
        case CypherPlannerOption.dp                             => DPPlannerName
        case _ => throw new IllegalArgumentException(s"unknown cost based planner: ${plannerOption.name}")
      }

    // Let's only create a monitor when we have a valid plannerName
    val (monitor, solverConfig) = plannerName match {
      case IDPPlannerName =>
        val monitor = monitors.newMonitor[IDPQueryGraphSolverMonitor]()
        val solverConfig = new ConfigurableIDPSolverConfig(
          maxTableSize = config.idpMaxTableSize(),
          iterationDurationLimit = config.idpIterationDuration()
        )
        (monitor, solverConfig)
      case DPPlannerName =>
        val monitor = monitors.newMonitor[IDPQueryGraphSolverMonitor]()
        (monitor, DPSolverConfig)
    }

    val singleComponentPlanner = SingleComponentPlanner(solverConfig)(monitor)
    val componentConnectorPlanner = connectComponentsPlannerOption match {
      case CypherConnectComponentsPlannerOption.idp |
        CypherConnectComponentsPlannerOption.default =>
        ComponentConnectorPlanner(singleComponentPlanner, solverConfig)(monitor)
      case CypherConnectComponentsPlannerOption.greedy => cartesianProductsOrValueJoins
    }

    val existsSubqueryPlanner =
      if (disableExistsSubqueryCaching) ExistsSubqueryPlanner
      else ExistsSubqueryPlannerWithCaching()

    IDPQueryGraphSolver(singleComponentPlanner, componentConnectorPlanner, existsSubqueryPlanner)(monitor)
  }

}

/**
 * Cypher planner, which either parses and plans a [[PreParsedQuery]] into a [[LogicalPlanResult]] or just plans [[FullyParsedQuery]].
 */
case class CypherPlanner(
  parsingConfig: CypherParsingConfig,
  plannerConfig: CypherPlannerConfiguration,
  clock: Clock,
  kernelMonitors: monitoring.Monitors,
  log: InternalLog,
  queryCaches: CypherQueryCaches,
  plannerOption: CypherPlannerOption,
  databaseReferenceRepository: DatabaseReferenceRepository,
  internalNotificationStats: InternalNotificationStats,
  internalSyntaxUsageStats: InternalSyntaxUsageStats
) {

  private val caches = new queryCaches.CypherPlannerCaches()

  private val monitors: Monitors = WrappedMonitors(kernelMonitors)

  private val planner: compiler.CypherPlanner[PlannerContext] =
    compiler.CypherPlanner(monitors, parsingConfig, plannerConfig, clock, internalSyntaxUsageStats)

  private val schemaStateKey: SchemaStateKey = SchemaStateKey.newKey()

  /**
   * Clear the caches of this caching compiler.
   *
   * @return the number of entries that were cleared
   */
  def clearCaches(): Long = {
    Math.max(caches.astCache.clear(), caches.logicalPlanCache.clear())
  }

  /**
   * Get the parsed query from cache, or parses and caches it.
   */
  @throws(classOf[SyntaxException])
  private def getOrParse(
    preParsedQuery: PreParsedQuery,
    params: MapValue,
    notificationLogger: InternalNotificationLogger,
    offset: InputPosition,
    tracer: CompilationPhaseTracer,
    cancellationChecker: CancellationChecker,
    sessionDatabase: DatabaseReference
  ): BaseState = {
    val key = AstCache.key(preParsedQuery, params, parsingConfig.useParameterSizeHint)
    val maybeValue = caches.astCache.get(key)
    val value = maybeValue.getOrElse {
      val parsedQuery = planner.parseQuery(
        preParsedQuery.statement,
        preParsedQuery.rawStatement,
        preParsedQuery.options.queryOptions.cypherVersion,
        notificationLogger,
        preParsedQuery.options.queryOptions.planner.name,
        Some(offset),
        tracer,
        params,
        cancellationChecker,
        sessionDatabase
      )
      val value = AstCache.AstCacheValue(parsedQuery, notificationLogger.notifications)
      if (!plannerConfig.planSystemCommands) caches.astCache.put(key, value)
      value
    }
    value.notifications.foreach(notificationLogger.log)
    value.parsedQuery
  }

  def insertIntoCache(
    preParsedQuery: PreParsedQuery,
    params: MapValue,
    parsedQuery: BaseState,
    parsingNotifications: Set[InternalNotification]
  ): Unit = {
    if (plannerConfig.planSystemCommands) {
      return
    }
    val key = AstCache.key(preParsedQuery, params, parsingConfig.useParameterSizeHint)
    caches.astCache.put(key, AstCacheValue(parsedQuery, parsingNotifications))
  }

  /**
   * Compile pre-parsed query into a logical plan.
   *
   * @param preParsedQuery       pre-parsed query to convert
   * @param tracer               tracer to which events of the parsing and planning are reported
   * @param transactionalContext transactional context to use during parsing and planning
   * @throws Neo4jException public cypher exceptions on compilation problems
   * @return a logical plan result
   */
  def parseAndPlan(
    preParsedQuery: PreParsedQuery,
    tracer: CompilationPhaseTracer,
    transactionalContext: TransactionalContext,
    params: MapValue,
    runtime: CypherRuntime[_],
    notificationLogger: InternalNotificationLogger,
    sessionDatabase: DatabaseReference
  ): LogicalPlanResult = {
    val transactionalContextWrapper = TransactionalContextWrapper(transactionalContext)
    val syntacticQuery = getOrParse(
      preParsedQuery,
      params,
      notificationLogger,
      preParsedQuery.options.offset,
      tracer,
      transactionalContextWrapper.cancellationChecker,
      sessionDatabase = sessionDatabase
    )

    // The parser populates the notificationLogger as a side-effect of its work, therefore
    // in the case of a cached query the notificationLogger will not be properly filled
    syntacticQuery.maybeSemantics.map(_.notifications).getOrElse(Set.empty).foreach(notificationLogger.log)

    doPlan(
      syntacticQuery,
      preParsedQuery.options,
      tracer,
      transactionalContextWrapper,
      params,
      runtime,
      notificationLogger,
      preParsedQuery.rawStatement
    )
  }

  /**
   * Plan fully-parsed query into a logical plan.
   *
   * @param fullyParsedQuery     a fully-parsed query to plan
   * @param tracer               tracer to which events of the parsing and planning are reported
   * @param transactionalContext transactional context to use during parsing and planning
   * @throws Neo4jException public cypher exceptions on compilation problems
   * @return a logical plan result
   */
  @throws[Neo4jException]
  def plan(
    fullyParsedQuery: FullyParsedQuery,
    tracer: CompilationPhaseTracer,
    transactionalContext: TransactionalContext,
    params: MapValue,
    runtime: CypherRuntime[_],
    notificationLogger: InternalNotificationLogger
  ): LogicalPlanResult = {
    val transactionalContextWrapper = TransactionalContextWrapper(transactionalContext)
    doPlan(
      fullyParsedQuery.state,
      fullyParsedQuery.options,
      tracer,
      transactionalContextWrapper,
      params,
      runtime,
      notificationLogger,
      fullyParsedQuery.state.queryText
    )
  }

  private def doPlan(
    syntacticQuery: BaseState,
    options: QueryOptions,
    tracer: CompilationPhaseTracer,
    transactionalContextWrapper: TransactionalContextWrapper,
    params: MapValue,
    runtime: CypherRuntime[_],
    notificationLogger: InternalNotificationLogger,
    rawQueryText: String
  ): LogicalPlanResult = {
    // Context used for db communication during planning
    val createPlanContext = CypherPlanner.customPlanContextCreator.getOrElse(TransactionBoundPlanContext.apply _)
    val planContext =
      new ExceptionTranslatingPlanContext(createPlanContext(
        transactionalContextWrapper,
        notificationLogger,
        log,
        options.queryOptions.cypherVersion.actualVersion
      ))

    val inferredRuntime: CypherRuntimeOption = options.queryOptions.runtime match {
      case CypherRuntimeOption.default => runtime.correspondingRuntimeOption.getOrElse(CypherRuntimeOption.default)
      case x                           => x
    }
    val containsUpdates: Boolean = syntacticQuery.statement().containsUpdates
    val executionModel = inferredRuntime match {
      case CypherRuntimeOption.pipelined =>
        BatchedSingleThreaded(plannerConfig.pipelinedBatchSizeSmall(), plannerConfig.pipelinedBatchSizeBig())
      case CypherRuntimeOption.parallel if !containsUpdates =>
        BatchedParallel(plannerConfig.pipelinedBatchSizeSmall(), plannerConfig.pipelinedBatchSizeBig())
      case _ => Volcano
    }
    val maybeUpdateStrategy: Option[UpdateStrategy] = options.queryOptions.updateStrategy match {
      case CypherUpdateStrategy.eager => Some(eagerUpdateStrategy)
      case _                          => None
    }

    // Context used to create logical plans
    val plannerContext = PlannerContext(
      tracer,
      notificationLogger,
      planContext,
      rawQueryText,
      options.queryOptions.debugOptions,
      executionModel,
      Some(options.offset),
      monitors,
      CachedSimpleMetricsFactory,
      createQueryGraphSolver(
        plannerConfig,
        plannerOption,
        options.queryOptions.connectComponentsPlanner,
        options.queryOptions.debugOptions.disableExistsSubqueryCaching,
        monitors
      ),
      plannerConfig,
      maybeUpdateStrategy.getOrElse(defaultUpdateStrategy),
      clock,
      new SequentialIdGen(),
      simpleExpressionEvaluator,
      params,
      transactionalContextWrapper.cancellationChecker,
      options.materializedEntitiesMode,
      options.queryOptions.eagerAnalyzer,
      options.queryOptions.inferSchemaParts,
      options.queryOptions.statefulShortestPlanningModeOption,
      options.queryOptions.planVarExpandInto,
      databaseReferenceRepository,
      transactionalContextWrapper.databaseId,
      log,
      internalNotificationStats,
      internalSyntaxUsageStats,
      null
    )

    // Prepare query for caching
    val preparedQuery = planner.normalizeQuery(syntacticQuery, plannerContext)

    val (queryParamNames, autoExtractParams) =
      parameterNamesAndValues(preparedQuery.statement(), preparedQuery.maybeExtractedParams) match {
        case (qpn: ArrayBuffer[String], aep: MapValue) => (qpn.toSeq, aep)
      }

    // Get obfuscator out ASAP to make query text available for `dbms.listQueries`, etc
    val obfuscator = CypherQueryObfuscator(preparedQuery.obfuscationMetadata())
    transactionalContextWrapper.kernelTransactionalContext.executingQuery.onObfuscatorReady(
      obfuscator,
      options.offset.offset
    )

    checkForSchemaChanges(transactionalContextWrapper)

    // If the query is not cached we want to do the full planning
    def createPlan(shouldBeCached: Boolean, missingParameterNames: Seq[String] = Seq.empty) =
      doCreatePlan(
        preparedQuery,
        plannerContext,
        notificationLogger,
        runtime,
        shouldBeCached,
        missingParameterNames
      )

    // Filter the parameters to retain only those that are actually used in the query (or a subset of them, if not enough
    // parameters where given in the first place)
    val filteredParams: MapValue =
      params.updatedWith(autoExtractParams).filter((name, _) => queryParamNames.contains(name))

    val enoughParametersSupplied =
      queryParamNames.size == filteredParams.size // this is relevant if the query has parameters

    val compilerWithExpressionCodeGenOption = new CompilerWithExpressionCodeGenOption[CacheableLogicalPlan] {
      override def compile(): CacheableLogicalPlan = createPlan(shouldBeCached = true)
      override def compileWithExpressionCodeGen(): CacheableLogicalPlan = compile()
      override def maybeCompileWithExpressionCodeGen(
        hitCount: Int,
        shouldRecompile: () => Boolean
      ): Option[CacheableLogicalPlan] = None
    }

    val cacheableLogicalPlan =
      // We don't want to cache any query without enough given parameters (although EXPLAIN queries will succeed)
      if (options.queryOptions.debugOptions.isEmpty && (queryParamNames.isEmpty || enoughParametersSupplied)) {
        val cacheKey = LogicalPlanCache.key(
          syntacticQuery.statement(),
          options,
          filteredParams,
          parsingConfig.useParameterSizeHint,
          transactionalContextWrapper.kernelTransaction.dataRead().transactionStateHasChanges()
        )

        caches.logicalPlanCache.computeIfAbsentOrStale(
          cacheKey,
          transactionalContextWrapper.kernelTransactionalContext,
          compilerWithExpressionCodeGenOption,
          options.queryOptions.replan,
          transactionalContextWrapper.kernelExecutingQuery.id()
        )
      } else if (!enoughParametersSupplied) {
        createPlan(
          shouldBeCached = false,
          missingParameterNames = queryParamNames.filterNot(filteredParams.containsKey)
        )
      } else {
        createPlan(shouldBeCached = false)
      }
    LogicalPlanResult(
      cacheableLogicalPlan.logicalPlanState,
      queryParamNames,
      autoExtractParams,
      cacheableLogicalPlan.reusability,
      plannerContext,
      (notificationLogger.notifications ++ cacheableLogicalPlan.notifications).toIndexedSeq,
      cacheableLogicalPlan.shouldBeCached,
      obfuscator
    )
  }

  private def doCreatePlan(
    preparedQuery: BaseState,
    outerContext: PlannerContext,
    outerNotificationLogger: InternalNotificationLogger,
    runtime: CypherRuntime[_],
    shouldBeCached: Boolean,
    missingParameterNames: Seq[String]
  ): CacheableLogicalPlan = {
    // Only collects the notifications from planning.
    val (planningNotificationsLogger, notificationLogger) = outerNotificationLogger match {
      case `devNullLogger` =>
        (devNullLogger, devNullLogger)
      case _ =>
        val pL = new RecordingNotificationLogger()
        val nL = new ComposedNotificationLogger(outerNotificationLogger, pL)
        (pL, nL)
    }
    val context = outerContext.withNotificationLogger(notificationLogger)

    val (logicalPlanState, reusabilityState, shouldCache) =
      doCreatePlanWithLocalNotificationLogger(
        preparedQuery,
        runtime,
        shouldBeCached,
        missingParameterNames,
        notificationLogger,
        context
      )

    CacheableLogicalPlan(
      logicalPlanState.asCachableLogicalPlanState(),
      reusabilityState,
      // Only cache planning-related notifications here
      planningNotificationsLogger.notifications.toIndexedSeq,
      shouldCache
    )
  }

  private def doCreatePlanWithLocalNotificationLogger(
    preparedQuery: BaseState,
    runtime: CypherRuntime[_],
    shouldBeCached: Boolean,
    missingParameterNames: Seq[String],
    notificationLogger: InternalNotificationLogger,
    context: PlannerContext
  ): (LogicalPlanState, ReusabilityState, Boolean) = {
    val planContext = context.planContext
    val logicalPlanStateOld = planner.planPreparedQuery(preparedQuery, context)
    val hasLoadCsv = logicalPlanStateOld.logicalPlan.folder.treeFind[LogicalPlan] {
      case _: LoadCSV => true
    }.nonEmpty
    val logicalPlanState = logicalPlanStateOld.copy(hasLoadCSV = hasLoadCsv)
    notification.LogicalPlanNotifications
      .checkForNotifications(logicalPlanState.maybeLogicalPlan.get, planContext, plannerConfig)
      .foreach(notificationLogger.log)
    if (missingParameterNames.nonEmpty) {
      notificationLogger.log(MissingParametersNotification(missingParameterNames))
    }
    val (reusabilityState, shouldCache) = runtime match {
      case m: AdministrationCommandRuntime =>
        if (m.isApplicableAdministrationCommand(logicalPlanState.logicalPlan)) {
          val allowQueryCaching = logicalPlanState.maybeLogicalPlan match {
            case Some(_: SystemProcedureCall)    => false
            case Some(ContainsSensitiveFields()) => false
            case _                               => true
          }
          (FineToReuse, allowQueryCaching)
        } else {
          logicalPlanState.maybeLogicalPlan match {
            case Some(ProcedureCall(_, ResolvedCall(signature, _, _, _, _, _))) if signature.systemProcedure =>
              (FineToReuse, false)
            case Some(_: ProcedureCall) =>
              throw new NotSystemDatabaseException("Attempting invalid procedure call in administration runtime")
            case Some(plan: AdministrationCommandLogicalPlan) =>
              throw plan.invalid("Unsupported administration command: " + logicalPlanState.queryText)
            case _ => throw new NotSystemDatabaseException(
                "Attempting invalid administration command in administration runtime"
              )
          }
        }
      case _ if SchemaCommandRuntime.isApplicable(logicalPlanState) => (FineToReuse, shouldBeCached)
      case _ =>
        val fingerprint = PlanFingerprint.take(
          clock,
          planContext.lastCommittedTxIdProvider,
          planContext.statistics,
          logicalPlanState.maybeProcedureSignatureVersion
        )
        val fingerprintReference = new PlanFingerprintReference(fingerprint)
        (MaybeReusable(fingerprintReference), shouldBeCached)
    }

    val notifications = notificationLogger.notifications

    // Record stats for finalized notifications, used for notification counter metrics
    notifications.foreach { context.internalNotificationStats.incrementNotificationCount }

    (logicalPlanState, reusabilityState, shouldCache)
  }

  private def checkForSchemaChanges(tcw: TransactionalContextWrapper): Unit =
    tcw.getOrCreateFromSchemaState(schemaStateKey, caches.logicalPlanCache.clear())

  private def parameterNamesAndValues(
    statement: Statement,
    extracted: Option[Map[AutoExtractedParameter, Expression]]
  ): (ArrayBuffer[String], MapValue) = {
    val evaluator = new SimpleInternalExpressionEvaluator
    val names = mutable.ArrayBuffer.empty[String]
    val mapBuilder = new MapValueBuilder()
    statement.folder.findAllByClass[Parameter].foreach {
      case p: AutoExtractedParameter =>
        val value =
          extracted.map(_(p)).getOrElse(throw new IllegalStateException(s"Parameter $p hasn't been extracted"))
        names += p.name
        mapBuilder.add(p.name, evaluator.evaluate(value))
      case ExplicitParameter(name, _, _) =>
        names += name
    }
    (names.distinct, mapBuilder.build())
  }
}

object ContainsSensitiveFields {

  def unapply(plan: LogicalPlan): Boolean = {
    plan.folder.treeExists {
      case _: SensitiveLiteral   => true
      case _: SensitiveParameter => true
    }
  }
}

case class LogicalPlanResult(
  logicalPlanState: CachableLogicalPlanState,
  paramNames: Seq[String],
  extractedParams: MapValue,
  reusability: ReusabilityState,
  plannerContext: PlannerContext,
  notifications: IndexedSeq[InternalNotification],
  shouldBeCached: Boolean,
  queryObfuscator: QueryObfuscator
)
