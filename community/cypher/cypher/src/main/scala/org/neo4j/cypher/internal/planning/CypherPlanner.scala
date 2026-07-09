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
import org.neo4j.cypher.internal.MaybeReusable
import org.neo4j.cypher.internal.ObfuscationPolicy
import org.neo4j.cypher.internal.PlanFingerprint
import org.neo4j.cypher.internal.PlanFingerprintReference
import org.neo4j.cypher.internal.QueryCache
import org.neo4j.cypher.internal.QueryCache.CompileReason
import org.neo4j.cypher.internal.QueryCache.QueryCacheResult
import org.neo4j.cypher.internal.ReusabilityState
import org.neo4j.cypher.internal.SchemaCommandRuntime
import org.neo4j.cypher.internal.ast.AdministrationCommand
import org.neo4j.cypher.internal.ast.SchemaCommand
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.cache.CypherQueryCaches
import org.neo4j.cypher.internal.cache.CypherQueryCaches.AstCache
import org.neo4j.cypher.internal.cache.CypherQueryCaches.AstCache.AstCacheValue
import org.neo4j.cypher.internal.cache.CypherQueryCaches.CacheStrategy
import org.neo4j.cypher.internal.cache.CypherQueryCaches.LogicalPlanCache
import org.neo4j.cypher.internal.cache.CypherQueryCaches.LogicalPlanCache.CacheableLogicalPlan
import org.neo4j.cypher.internal.compiler.CypherParsing
import org.neo4j.cypher.internal.compiler.CypherParsingConfig
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.ExecutionModel.BatchedParallel
import org.neo4j.cypher.internal.compiler.ExecutionModel.BatchedSingleThreaded
import org.neo4j.cypher.internal.compiler.ExecutionModel.Volcano
import org.neo4j.cypher.internal.compiler.UpdateStrategy
import org.neo4j.cypher.internal.compiler.defaultUpdateStrategy
import org.neo4j.cypher.internal.compiler.eagerUpdateStrategy
import org.neo4j.cypher.internal.compiler.helpers.HistogramsFromConfigHelper
import org.neo4j.cypher.internal.compiler.helpers.HistogramsFromConfigHelper.graphStatisticsDecoratorWithHistogramsFromConfig
import org.neo4j.cypher.internal.compiler.phases.CachableLogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.planPipeLine
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.prepareForCaching
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.systemPipeLine
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.CypherPlannerVersionWithOptimisations
import org.neo4j.cypher.internal.compiler.planner.logical.CachedSimpleMetricsFactory
import org.neo4j.cypher.internal.compiler.planner.logical.debug.DebugPrinter
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
import org.neo4j.cypher.internal.frontend.notification.InternalNotificationStats
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.InternalUsageStats
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.frontend.phases.QueryLanguage
import org.neo4j.cypher.internal.frontend.phases.ResolvedNonLocalCall
import org.neo4j.cypher.internal.frontend.phases.ScopedProcedureSignatureResolver
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.logical.plans.AdministrationCommandLogicalPlan
import org.neo4j.cypher.internal.logical.plans.AllowedNonAdministrationCommands
import org.neo4j.cypher.internal.logical.plans.LoadCSV
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.SchemaLogicalPlan
import org.neo4j.cypher.internal.logical.plans.SystemProcedureCall
import org.neo4j.cypher.internal.notification.ComposedNotificationLogger
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.notification.InternalNotificationLogger
import org.neo4j.cypher.internal.notification.MissingParametersNotification
import org.neo4j.cypher.internal.notification.RecordingNotificationLogger
import org.neo4j.cypher.internal.notification.devNullLogger
import org.neo4j.cypher.internal.options.CypherConnectComponentsPlannerOption
import org.neo4j.cypher.internal.options.CypherParallelRuntimeConfigOption
import org.neo4j.cypher.internal.options.CypherPipelinedBatchSize
import org.neo4j.cypher.internal.options.CypherPipelinedBatchSizePresetOption
import org.neo4j.cypher.internal.options.CypherPlannerOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.options.CypherUpdateStrategy
import org.neo4j.cypher.internal.planner.spi.CostBasedPlannerName
import org.neo4j.cypher.internal.planner.spi.DPPlannerName
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.IndexComparatorFactory
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planning.TransformingPlanner.DefaultTransformers
import org.neo4j.cypher.internal.planning.TransformingPlanner.preventCaching
import org.neo4j.cypher.internal.preparser.FullyParsedQuery
import org.neo4j.cypher.internal.preparser.PreParsedQuery
import org.neo4j.cypher.internal.preparser.QueryOptions
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundReadTokenContext
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.cypher.internal.spi.ExceptionTranslatingPlanContext
import org.neo4j.cypher.internal.spi.ExceptionTranslatingResolver
import org.neo4j.cypher.internal.spi.TransactionBoundIndexComparatorFactory
import org.neo4j.cypher.internal.spi.TransactionBoundPlanContext
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.DisallowedOnSystemException
import org.neo4j.exceptions.Neo4jException
import org.neo4j.exceptions.SecurityAdministrationException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.kernel.api.query.QueryObfuscator
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.kernel.impl.api.SchemaStateKey
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.kernel.impl.query.TransactionalContext.DatabaseMode.SHARDED
import org.neo4j.logging.InternalLog
import org.neo4j.monitoring
import org.neo4j.util.VisibleForTesting
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder

import java.time.Clock
import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.IterableHasAsScala

/**
 * Cypher planner, which either parses and plans a [[PreParsedQuery]] into a [[LogicalPlanResult]] or just plans [[FullyParsedQuery]].
 */
sealed trait CypherPlanner {

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
    notificationLogger: InternalNotificationLogger,
    cacheStrategy: CacheStrategy
  ): LogicalPlanResult

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
    sessionDatabase: DatabaseReference,
    cacheStrategy: CacheStrategy
  ): LogicalPlanResult

  /**
   * Clear the caches of this caching compiler.
   *
   * @return the number of entries that were cleared
   */
  def clearCaches(): Long

  def insertIntoCache(
    preParsedQuery: PreParsedQuery,
    params: MapValue,
    parsedQuery: BaseState,
    parsingNotifications: Set[InternalNotification]
  ): Unit

  @VisibleForTesting
  def astCacheSize: Int

  @VisibleForTesting
  def getFromAstCache(astKey: AstCache.Key): Option[CypherQueryCaches.AstCache.Value]
}

object DefaultCypherPlanner {

  /**
   * Creates an instance of the default planner implementation.
   * Note, this instance can not be re-used with a different [[CypherPlannerConfiguration]]!
   */
  def apply(
    parsingConfig: CypherParsingConfig,
    plannerConfig: CypherPlannerConfiguration,
    clock: Clock,
    kernelMonitors: monitoring.Monitors,
    log: InternalLog,
    securityLog: AbstractSecurityLog,
    queryCaches: CypherQueryCaches,
    plannerOption: CypherPlannerOption,
    databaseReferenceRepository: DatabaseReferenceRepository,
    schemaCommandRuntime: SchemaCommandRuntime,
    internalNotificationStats: InternalNotificationStats,
    internalUsageStats: InternalUsageStats
  ): TransformingPlanner = new TransformingPlanner(
    transformers =
      if (plannerConfig.planSystemCommands) TransformingPlanner.SystemCommandTransformers
      else TransformingPlanner.DefaultTransformers,
    parsingConfig = parsingConfig,
    plannerConfig = plannerConfig,
    clock = clock,
    kernelMonitors = kernelMonitors,
    log = log,
    securityLog = securityLog,
    queryCaches = queryCaches,
    plannerOption = plannerOption,
    databaseReferenceRepository = databaseReferenceRepository,
    schemaCommandRuntime = schemaCommandRuntime,
    internalNotificationStats = internalNotificationStats,
    internalUsageStats = internalUsageStats,
    supportsFingerprint = true
  )

  def withTransformers(
    transformers: TransformingPlanner.Transformers[PlannerContext],
    parsingConfig: CypherParsingConfig,
    plannerConfig: CypherPlannerConfiguration,
    clock: Clock,
    kernelMonitors: monitoring.Monitors,
    log: InternalLog,
    securityLog: AbstractSecurityLog,
    queryCaches: CypherQueryCaches,
    plannerOption: CypherPlannerOption,
    databaseReferenceRepository: DatabaseReferenceRepository,
    schemaCommandRuntime: SchemaCommandRuntime,
    internalNotificationStats: InternalNotificationStats,
    internalUsageStats: InternalUsageStats,
    supportsFingerprint: Boolean
  ): TransformingPlanner = new TransformingPlanner(
    transformers = transformers,
    parsingConfig = parsingConfig,
    plannerConfig = plannerConfig,
    clock = clock,
    kernelMonitors = kernelMonitors,
    log = log,
    securityLog = securityLog,
    queryCaches = queryCaches,
    plannerOption = plannerOption,
    databaseReferenceRepository = databaseReferenceRepository,
    schemaCommandRuntime = schemaCommandRuntime,
    internalNotificationStats = internalNotificationStats,
    internalUsageStats = internalUsageStats,
    supportsFingerprint = supportsFingerprint
  )
}

object TransformingPlanner {

  /**
   * This back-door is intended for quick handling of bugs and support cases
   * where we need to inject some specific indexes and statistics.
   */
  var customPlanContextCreator: Option[(
    TransactionalContextWrapper,
    InternalNotificationLogger,
    InternalLog,
    CypherVersion,
    GraphStatistics => GraphStatistics
  ) => PlanContext] =
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

  @VisibleForTesting
  def selectExecutionModel(
    runtimeOption: CypherRuntimeOption,
    containsUpdates: Boolean,
    getParallelRuntimeConfigOption: () => CypherParallelRuntimeConfigOption,
    getBatchSize: () => CypherPipelinedBatchSize
  ): ExecutionModel = runtimeOption match {
    case CypherRuntimeOption.pipelined =>
      val batchSize = getBatchSize()
      BatchedSingleThreaded(batchSize.small, batchSize.big)
    case CypherRuntimeOption.parallel if !containsUpdates =>
      val batchSize = getBatchSize()
      val inferredRuntimeConfig = getParallelRuntimeConfigOption()
      BatchedParallel(batchSize.small, batchSize.big, inferredRuntimeConfig.leverageOrder)
    case _ => Volcano
  }

  private def preventCaching(state: BaseState): Boolean =
    state.maybeResolvedParams.exists(_.nonEmpty)

  trait Transformers[Context <: BaseContext] {

    /** Normalize a parsed statement, usually to prepare for caching. */
    def normalizeQuery: Transformer[Context, BaseState, BaseState]

    /** Query planning transformer. */
    // TODO The context should not be needed to instantiate the pipeline
    def plan(context: Context): Transformer[Context, BaseState, LogicalPlanState]
  }

  object DefaultTransformers extends Transformers[PlannerContext] {
    override def normalizeQuery: Transformer[PlannerContext, BaseState, BaseState] = prepareForCaching

    override def plan(context: PlannerContext): Transformer[PlannerContext, BaseState, LogicalPlanState] = {
      val planning = planPipeLine(
        allowSubqueryDuplicationInCnf = context.config.allowDuplicatingSubqueryExpressionsInCnfNormalizer()
      )
      if (context.debugOptions.toStringEnabled) {
        planning andThen DebugPrinter
      } else {
        planning
      }
    }
  }

  object SystemCommandTransformers extends Transformers[PlannerContext] {
    override def normalizeQuery: Transformer[PlannerContext, BaseState, BaseState] = prepareForCaching

    override def plan(context: PlannerContext): Transformer[PlannerContext, BaseState, LogicalPlanState] =
      systemPipeLine
  }
}

/** [[CypherPlanner]] implementation based on [[Transformer]]s. */
final class TransformingPlanner private[planning] (
  transformers: TransformingPlanner.Transformers[PlannerContext],
  parsingConfig: CypherParsingConfig,
  plannerConfig: CypherPlannerConfiguration,
  clock: Clock,
  kernelMonitors: monitoring.Monitors,
  log: InternalLog,
  securityLog: AbstractSecurityLog,
  queryCaches: CypherQueryCaches,
  plannerOption: CypherPlannerOption,
  databaseReferenceRepository: DatabaseReferenceRepository,
  schemaCommandRuntime: SchemaCommandRuntime,
  internalNotificationStats: InternalNotificationStats,
  internalUsageStats: InternalUsageStats,
  supportsFingerprint: Boolean
) extends CypherPlanner {
  private val caches = new queryCaches.CypherPlannerCaches()
  private val monitors: Monitors = WrappedMonitors(kernelMonitors)
  private val parsing = new CypherParsing(monitors, parsingConfig, internalUsageStats)
  private val schemaStateKey: SchemaStateKey = SchemaStateKey.newKey()

  // Obfuscator flags come from injected configuration, not from the transactional context: callers
  // (and test harnesses) may provide a context without a graph, and resolving Config through the
  // context costs a dependency lookup per compilation.
  private val obfuscateLiterals: () => Boolean = parsingConfig.obfuscateLiterals
  private val exposeFullyObfuscatedQueryView: Boolean = parsingConfig.exposeFullyObfuscatedQueryView

  override def clearCaches(): Long = {
    parsing.clearDFACaches()
    Math.max(caches.astCache.clear(), caches.logicalPlanCache.clear())
  }

  /**
   * Get the parsed query from cache, or parses and caches it.
   *
   * Parsing is split at the obfuscation-metadata boundary. On a cache miss we run the pre-half,
   * wire the obfuscator onto the [[ExecutingQuery]] via `onObfuscatorReady`, then run the post-half.
   * This enables semantic analysis errors to propagate with the query text already attached to the
   * executing-query, so the logs can carry the `query` field.
   * Failures in the pre-half remain un-attached (best-effort: no obfuscator yet).
   *
   * On a cache hit we return the cached fully-parsed [[BaseState]]. The obfuscator is wired up
   * later by the caller's existing `onObfuscatorReady` call at the planning stage.
   */
  @throws(classOf[SyntaxException])
  private def getOrParse(
    preParsedQuery: PreParsedQuery,
    params: MapValue,
    notificationLogger: InternalNotificationLogger,
    offset: InputPosition,
    tracer: CompilationPhaseTracer,
    cancellationChecker: CancellationChecker,
    resolver: ScopedProcedureSignatureResolver,
    sessionDatabase: DatabaseReference,
    shadowedFunctions: Set[String],
    cacheStrategy: CacheStrategy,
    transactionalContextWrapper: TransactionalContextWrapper
  ): BaseState = {
    def parseQuery(): BaseState = {
      val (preState, context, parsingConfig) = parsing.parseQueryPreObfuscator(
        queryText = preParsedQuery.statement,
        rawQueryText = preParsedQuery.rawStatement,
        cypherVersion = preParsedQuery.resolvedLanguage,
        notificationLogger = notificationLogger,
        plannerNameText = preParsedQuery.options.queryOptions.planner.name,
        offset = Some(offset),
        tracer = tracer,
        params = params,
        cancellationChecker = cancellationChecker,
        resolver = resolver,
        sessionDatabase = sessionDatabase,
        isScopeQuery = preParsedQuery.options.queryOptions.planMode.isScope,
        shadowedFunctions = shadowedFunctions
      )

      val postState = parsing.parseQueryPostObfuscator(preState, context, parsingConfig, params)

      val obfuscator = CypherQueryObfuscator(
        postState.maybeObfuscationMetadata,
        ObfuscationPolicy.fromConfig(obfuscateLiterals(), exposeFullyObfuscatedQueryView)
      )
      transactionalContextWrapper.kernelTransactionalContext.executingQuery
        .onObfuscatorReady(obfuscator, offset.offset)

      postState
    }

    if (!cacheStrategy.astShouldBeCached) {
      val parsedQuery = parseQuery()
      notificationLogger.notifications.foreach(notificationLogger.log)
      parsedQuery
    } else {
      val key = AstCache.key(preParsedQuery, params, parsingConfig.useParameterSizeHint)
      val maybeValue = caches.astCache.get(key).filter { v =>
        // Reject cached entry if procedure signatures changed since it was parsed.
        // None.forall is true, so queries without procedures always pass (never stale).
        v.parsedQuery.maybeProcedureSignatureVersion.forall(_ == resolver.procedureSignatureVersion)
      }
      val value = maybeValue.getOrElse {
        val parsedQuery = parseQuery()
        val value = AstCache.AstCacheValue(parsedQuery, notificationLogger.notifications)
        if (!plannerConfig.planSystemCommands && !preventCaching(parsedQuery)) {
          caches.astCache.put(key, value)
        }
        value
      }
      value.notifications.foreach(notificationLogger.log)
      value.parsedQuery
    }
  }

  override def insertIntoCache(
    preParsedQuery: PreParsedQuery,
    params: MapValue,
    parsedQuery: BaseState,
    parsingNotifications: Set[InternalNotification]
  ): Unit = {
    // We don't want to cache any query when a parameter has been solved
    if (plannerConfig.planSystemCommands || preventCaching(parsedQuery)) {
      return
    }
    val key = AstCache.key(preParsedQuery, params, parsingConfig.useParameterSizeHint)
    caches.astCache.put(key, AstCacheValue(parsedQuery, parsingNotifications))
  }

  override def parseAndPlan(
    preParsedQuery: PreParsedQuery,
    tracer: CompilationPhaseTracer,
    transactionalContext: TransactionalContext,
    params: MapValue,
    runtime: CypherRuntime[_],
    notificationLogger: InternalNotificationLogger,
    sessionDatabase: DatabaseReference,
    cacheStrategy: CacheStrategy
  ): LogicalPlanResult = {
    val transactionalContextWrapper = if (transactionalContext.databaseMode() == SHARDED)
      TransactionalContextWrapper.cachedSchemaWrapper(transactionalContext)
    else TransactionalContextWrapper(transactionalContext)

    val shadowedFunctions = transactionalContextWrapper.procedures.shadowedNamespaces(
      QueryLanguage.toKernelScope(preParsedQuery.resolvedLanguage)
    ).asScala.toSet
    val resolver = new ExceptionTranslatingResolver(
      TransactionBoundPlanContext.resolver(transactionalContextWrapper, preParsedQuery.resolvedLanguage)
    )
    val syntacticQuery = getOrParse(
      preParsedQuery,
      params,
      notificationLogger,
      preParsedQuery.options.offset,
      tracer,
      transactionalContextWrapper.cancellationChecker,
      resolver,
      sessionDatabase = sessionDatabase,
      shadowedFunctions,
      cacheStrategy,
      transactionalContextWrapper
    )
    val cacheStrategyAfterParsing = cacheStrategy.updateFromAst(syntacticQuery.statement())

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
      preParsedQuery.rawStatement,
      cacheStrategyAfterParsing
    )
  }

  @throws[Neo4jException]
  override def plan(
    fullyParsedQuery: FullyParsedQuery,
    tracer: CompilationPhaseTracer,
    transactionalContext: TransactionalContext,
    params: MapValue,
    runtime: CypherRuntime[_],
    notificationLogger: InternalNotificationLogger,
    cacheStrategy: CacheStrategy
  ): LogicalPlanResult = {
    val transactionalContextWrapper = TransactionalContextWrapper(transactionalContext)
    val cacheStrategyAfterParsing = cacheStrategy.updateFromAst(fullyParsedQuery.state.statement())
    doPlan(
      fullyParsedQuery.state,
      fullyParsedQuery.options,
      tracer,
      transactionalContextWrapper,
      params,
      runtime,
      notificationLogger,
      fullyParsedQuery.state.queryText,
      cacheStrategyAfterParsing
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
    rawQueryText: String,
    cacheStrategy: CacheStrategy
  ): LogicalPlanResult = {
    val planningStartNanos = System.nanoTime()
    def getBatchSize: CypherPipelinedBatchSize = {
      CypherPipelinedBatchSizePresetOption.batchSizeConfigFrom(
        options.queryOptions.pipelinedBatchSizePresetOption,
        plannerConfig.pipelinedBatchSizeSmall(),
        plannerConfig.pipelinedBatchSizeBig()
      )
    }

    // Context used for db communication during planning
    val createPlanContext = TransformingPlanner.customPlanContextCreator.getOrElse(TransactionBoundPlanContext.apply _)

    // Group the histograms from the config by the histogram key: entity type, labelId or relTypeId, PropertyKeyId
    // To get the histogram key, the label or type string and property key string will be resolved to ids
    val tokenContext = new TransactionBoundReadTokenContext(transactionalContextWrapper) {}
    val histogramsFromConfigWithIdsGrouped =
      plannerConfig.histograms
        .flatMap { histogram =>
          HistogramsFromConfigHelper
            .getHistogramKey(histogram, tokenContext)
            .map(_ -> histogram)
        }
        .groupBy(_._1)
        .view.mapValues(_.map(_._2))
        .toMap

    val graphStatisticsDecorator: GraphStatistics => GraphStatistics =
      graphStatisticsDecoratorWithHistogramsFromConfig(histogramsFromConfigWithIdsGrouped)

    val planContext =
      new ExceptionTranslatingPlanContext(createPlanContext(
        transactionalContextWrapper,
        notificationLogger,
        log,
        options.resolvedLanguage,
        graphStatisticsDecorator
      ))

    val inferredRuntime: CypherRuntimeOption = options.queryOptions.runtime match {
      case CypherRuntimeOption.default => runtime.correspondingRuntimeOption.getOrElse(CypherRuntimeOption.default)
      case x                           => x
    }
    val containsUpdates: Boolean = syntacticQuery.statement().containsUpdates
    val inferredRuntimeConfig = () => options.queryOptions.parallelRuntimeConfigOption
    val executionModel =
      TransformingPlanner.selectExecutionModel(
        inferredRuntime,
        containsUpdates,
        inferredRuntimeConfig,
        () => getBatchSize
      )
    val maybeUpdateStrategy: Option[UpdateStrategy] = options.queryOptions.updateStrategy match {
      case CypherUpdateStrategy.eager => Some(eagerUpdateStrategy)
      case _                          => None
    }

    // Context used to create logical plans
    val plannerContext = PlannerContext(
      options.resolvedLanguage,
      tracer,
      notificationLogger,
      planContext,
      rawQueryText,
      options.queryOptions.debugOptions,
      executionModel,
      Some(options.offset),
      monitors,
      CachedSimpleMetricsFactory,
      TransformingPlanner.createQueryGraphSolver(
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
      options.queryOptions.inferSchemaParts,
      options.queryOptions.statefulShortestPlanningModeOption,
      options.queryOptions.planVarExpandInto,
      CypherPlannerVersionWithOptimisations.allSupportedOptimisations(
        options.queryOptions.plannerVersionOption
      ),
      options.queryOptions.parallelRepeatHeuristic,
      databaseReferenceRepository,
      transactionalContextWrapper.databaseId,
      log,
      securityLog,
      internalNotificationStats,
      internalUsageStats,
      null,
      semanticFeatures = CypherParsingConfig.getEnabledFeatures(
        parsingConfig.semanticFeatures,
        plannerConfig.targetsComposite,
        parsingConfig.queryRouterForCompositeEnabled
      ),
      shadowedFunctions = transactionalContextWrapper.procedures.shadowedNamespaces(
        QueryLanguage.toKernelScope(options.resolvedLanguage)
      ).asScala.toSet,
      transactionBatchStrategy = options.queryOptions.transactionBatchStrategy
    )

    // Prepare query for caching
    val preparedQuery = transformers.normalizeQuery.transform(syntacticQuery, plannerContext)

    val (queryParamNames, autoExtractParams) =
      parameterNamesAndValues(preparedQuery.statement(), preparedQuery.maybeExtractedParams) match {
        case (qpn: ArrayBuffer[String], aep: MapValue) => (qpn.toSeq, aep)
      }

    // Get obfuscator out ASAP to make query text available for `dbms.listQueries`, etc
    val obfuscator = CypherQueryObfuscator(
      preparedQuery.maybeObfuscationMetadata,
      ObfuscationPolicy.fromConfig(obfuscateLiterals(), exposeFullyObfuscatedQueryView)
    )
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
      override def compile(): CacheableLogicalPlan =
        createPlan(shouldBeCached = !preventCaching(preparedQuery))
      override def compileWithExpressionCodeGen(): CacheableLogicalPlan = compile()
      override def maybeCompileWithExpressionCodeGen(
        hitCount: Int,
        shouldRecompile: () => Boolean
      ): Option[CacheableLogicalPlan] = None
    }

    val canBeCached = options.queryOptions.debugOptions.isEmpty && (queryParamNames.isEmpty || enoughParametersSupplied)
    val queryCacheResult =
      // We don't want to cache any query without enough given parameters (although EXPLAIN queries will succeed)
      if (cacheStrategy.logicalPlanShouldBeCached && canBeCached) {
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
          transactionalContextWrapper.kernelExecutingQuery.id(),
          cacheStrategy
        )
      } else if (!enoughParametersSupplied) {
        QueryCacheResult(
          createPlan(
            shouldBeCached = canBeCached,
            missingParameterNames = queryParamNames.filterNot(filteredParams.containsKey)
          ),
          Some(CompileReason.SkipCache),
          waitTimeMillis = 0L
        )
      } else {
        QueryCacheResult(createPlan(shouldBeCached = canBeCached), Some(CompileReason.SkipCache), waitTimeMillis = 0L)
      }

    val cacheableLogicalPlan = queryCacheResult.executableQuery
    val planningReason = queryCacheResult.compileReason

    val updatedCachableLogicalPlan = {
      cacheableLogicalPlan.logicalPlanState.logicalPlan match {
        case a: AllowedNonAdministrationCommands
          if a.maybePlan.isEmpty &&
            plannerContext.cypherVersion.isEqualOrAfter(CypherVersion.Cypher25) &&
            (options.queryOptions.executionMode.isExplain || options.queryOptions.executionMode.isProfile) =>
          val fromState = LogicalPlanState(preparedQuery).withStatement(a.statement)
          val maybePlan = DefaultTransformers.plan(plannerContext).transform(fromState, plannerContext).maybeLogicalPlan

          maybePlan match {
            case Some(plan) => cacheableLogicalPlan.copy(
                logicalPlanState = cacheableLogicalPlan.logicalPlanState.copy(logicalPlan = a.addPlan(plan))
              )
            case _ => cacheableLogicalPlan
          }
        case _ => cacheableLogicalPlan
      }
    }

    val cacheStrategyAfterPlanning = cacheStrategy.updateFromLogicalPlan(updatedCachableLogicalPlan)
    val planningTimeMillis = NANOSECONDS.toMillis(System.nanoTime() - planningStartNanos)
    LogicalPlanResult(
      updatedCachableLogicalPlan.logicalPlanState,
      queryParamNames,
      autoExtractParams,
      updatedCachableLogicalPlan.reusability,
      plannerContext,
      (notificationLogger.notifications ++ updatedCachableLogicalPlan.notifications).toIndexedSeq,
      cacheStrategyAfterPlanning,
      obfuscator,
      TransactionBoundIndexComparatorFactory,
      planningTimeMillis,
      planningReason
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
    val logicalPlanStateOld = transformers.plan(context).transform(preparedQuery, context)
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
            case Some(ProcedureCall(_, ResolvedNonLocalCall(signature, _, _, _, _, _, _)))
              if signature.systemProcedure =>
              (FineToReuse, false)
            case Some(_: ProcedureCall) =>
              throw DisallowedOnSystemException.disallowedOnSystemException(
                "Attempting invalid procedure call in administration runtime",
                logicalPlanState.queryText
              )
            case Some(_: AdministrationCommandLogicalPlan) =>
              val name = logicalPlanState.statement() match {
                case s: AdministrationCommand => s.commandDescription
                case s: Statement             => s.getClass.getSimpleName
              }
              throw SecurityAdministrationException.unsupportedInCommunity(logicalPlanState.queryText, name)
            case _ => throw DisallowedOnSystemException.disallowedOnSystemException(
                "Attempting invalid administration command in administration runtime",
                logicalPlanState.queryText
              )
          }
        }
      case _ if logicalPlanState.logicalPlan.isInstanceOf[SchemaLogicalPlan] =>
        // _ is a FallbackRuntime mostly, which may or may not contain an instance of SchemaCommandRuntime, so we have to
        // pass the right one in.
        if (schemaCommandRuntime.isApplicable(logicalPlanState.logicalPlan)) {
          (FineToReuse, shouldBeCached)
        } else {
          val name = logicalPlanState.statement() match {
            case s: SchemaCommand => s.commandDescription
            case s: Statement     => s.getClass.getSimpleName
          }
          throw CantCompileQueryException.commandUnsupportedInCommunityEdition(name)
        }
      case _ if !supportsFingerprint =>
        (FineToReuse, shouldBeCached)
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

  @VisibleForTesting
  def astCacheSize: Int = {
    caches.astCache.asMap().size()
  }

  @VisibleForTesting
  def getFromAstCache(astKey: AstCache.Key): Option[CypherQueryCaches.AstCache.Value] = {
    caches.astCache.get(astKey)
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
  cacheStrategy: CacheStrategy,
  queryObfuscator: QueryObfuscator,
  indexSelector: IndexComparatorFactory,
  planningTimeMillis: Long,
  compileReason: Option[QueryCache.CompileReason]
)
