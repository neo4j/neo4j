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
package org.neo4j.cypher.internal.cache

import com.github.benmanes.caffeine.cache
import com.github.benmanes.caffeine.cache.RemovalListener
import org.neo4j.cypher.ASTCacheMetricsMonitor
import org.neo4j.cypher.CacheMetricsMonitor
import org.neo4j.cypher.ExecutableQueryCacheMetricsMonitor
import org.neo4j.cypher.ExecutionPlanCacheMetricsMonitor
import org.neo4j.cypher.LogicalPlanCacheMetricsMonitor
import org.neo4j.cypher.PreParserCacheMetricsMonitor
import org.neo4j.cypher.internal.CacheabilityInfo
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.DefaultPlanStalenessCaller
import org.neo4j.cypher.internal.ExecutableQuery
import org.neo4j.cypher.internal.ExecutingQueryTracer
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.PlanStalenessCaller
import org.neo4j.cypher.internal.QueryCache
import org.neo4j.cypher.internal.QueryCache.CacheKey
import org.neo4j.cypher.internal.QueryCache.ParameterTypeMap
import org.neo4j.cypher.internal.ReusabilityState
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.cache.CypherQueryCaches.AstCache
import org.neo4j.cypher.internal.cache.CypherQueryCaches.CacheCommon
import org.neo4j.cypher.internal.cache.CypherQueryCaches.CacheStrategy.updateDefaultValue
import org.neo4j.cypher.internal.cache.CypherQueryCaches.Config.ExecutionPlanCacheSize
import org.neo4j.cypher.internal.cache.CypherQueryCaches.Config.ExecutionPlanCacheSize.Default
import org.neo4j.cypher.internal.cache.CypherQueryCaches.Config.ExecutionPlanCacheSize.Disabled
import org.neo4j.cypher.internal.cache.CypherQueryCaches.Config.ExecutionPlanCacheSize.Sized
import org.neo4j.cypher.internal.cache.CypherQueryCaches.Config.SoftCacheSize
import org.neo4j.cypher.internal.cache.CypherQueryCaches.ExecutableQueryCache
import org.neo4j.cypher.internal.cache.CypherQueryCaches.ExecutionPlanCache
import org.neo4j.cypher.internal.cache.CypherQueryCaches.LogicalPlanCache
import org.neo4j.cypher.internal.cache.CypherQueryCaches.LogicalPlanCache.CacheableLogicalPlan
import org.neo4j.cypher.internal.cache.CypherQueryCaches.PreParserCache
import org.neo4j.cypher.internal.cache.CypherQueryCaches.PredefinedCacheTracers
import org.neo4j.cypher.internal.cache.CypherQueryCaches.QueryCacheLogger
import org.neo4j.cypher.internal.cache.CypherQueryCaches.withDebugMonitor
import org.neo4j.cypher.internal.compiler.StatsDivergenceCalculator
import org.neo4j.cypher.internal.compiler.phases.CachableLogicalPlanState
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.config.StatsDivergenceCalculatorConfig
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.options.CypherCacheOption
import org.neo4j.cypher.internal.options.CypherQueryOptions
import org.neo4j.cypher.internal.planner.spi.ImmutablePlanningAttributes
import org.neo4j.cypher.internal.planner.spi.PlanningAttributesCacheKey
import org.neo4j.cypher.internal.preparser.InputQuery
import org.neo4j.cypher.internal.preparser.PreParsedQuery
import org.neo4j.cypher.internal.preparser.QueryOptions
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.function.Observable
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.impl.query.CacheMetrics
import org.neo4j.kernel.impl.query.QueryCacheStatistics
import org.neo4j.logging.InternalLogProvider
import org.neo4j.memory.HeapEstimator
import org.neo4j.monitoring.Monitors
import org.neo4j.util.VisibleForTesting
import org.neo4j.values.virtual.MapValue

import java.io.Closeable
import java.time.Clock
import java.util.concurrent.CopyOnWriteArrayList

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava

/**
 * Defines types for all query caches
 */
object CypherQueryCaches {

  // --- Config -----------------------------------------------------

  /**
   * Collects configuration for cypher query caches
   *
   * @param cacheSize                       Maximum size of each separate cache
   * @param executionPlanCacheSize          Configures the execution plan cache
   * @param divergenceConfig                Configures the statistics divergence calculator used to compute logical plan staleness
   * @param enableExecutionPlanCacheTracing Enable tracing in the execution plan cache
   */
  case class Config(
    cacheSize: CacheSize,
    executionPlanCacheSize: ExecutionPlanCacheSize,
    divergenceConfig: StatsDivergenceCalculatorConfig,
    enableExecutionPlanCacheTracing: Boolean,
    enableDebugMonitors: Boolean,
    softCacheSize: SoftCacheSize
  ) {

    // Java helper
    def this(cypherConfig: CypherConfiguration, cacheSize: CacheSize) = this(
      cacheSize,
      ExecutionPlanCacheSize.fromInt(cypherConfig.executionPlanCacheSize),
      cypherConfig.statsDivergenceCalculator,
      cypherConfig.enableMonitors,
      cypherConfig.enableQueryCacheMonitors,
      if (cypherConfig.softQueryCacheEnabled) {
        SoftCacheSize.Sized(
          CacheSize.Dynamic(cypherConfig.queryCacheStrongSize),
          CacheSize.Dynamic(cypherConfig.queryCacheSoftSize)
        )
      } else {
        SoftCacheSize.Disabled
      }
    )
  }

  object Config {

    def fromCypherConfiguration(cypherConfig: CypherConfiguration) =
      new Config(cypherConfig, CacheSize.Dynamic(cypherConfig.queryCacheSize))

    def fromCypherConfiguration(cypherConfig: CypherConfiguration, cacheSize: Observable[Integer]) =
      new Config(cypherConfig, CacheSize.Dynamic(cacheSize))

    sealed trait ExecutionPlanCacheSize

    object ExecutionPlanCacheSize {
      case object Disabled extends ExecutionPlanCacheSize
      case object Default extends ExecutionPlanCacheSize

      case class Sized(cacheSize: Int) extends ExecutionPlanCacheSize {
        require(cacheSize > 0, s"Cache size cannot be negative. Got $cacheSize.")
      }

      /**
       * See [[org.neo4j.configuration.GraphDatabaseInternalSettings.query_execution_plan_cache_size]]
       */
      def fromInt(executionPlanCacheSize: Int): ExecutionPlanCacheSize = executionPlanCacheSize match {
        case -1 => Default
        case 0  => Disabled
        case n  => Sized(n)
      }
    }

    sealed trait SoftCacheSize

    object SoftCacheSize {
      case object Disabled extends SoftCacheSize

      case class Sized(strongSize: CacheSize, softSize: CacheSize) extends SoftCacheSize

    }
  }

  // --- Helpers ----------------------------------------------------

  abstract class CacheCompanion(val kind: String) {
    type Key
    type Value

    val monitorTag: String = s"cypher.cache.$kind"
  }

  trait CacheMonitorHelpers {
    this: CacheCompanion =>

    type Tracer = CacheTracer[Key]

    /**
     * Create a new monitor (publisher), tagged to this cache type
     */
    def newMonitor(monitors: Monitors): Tracer =
      monitors.newMonitor(classOf[Tracer], monitorTag)

    /**
     * Add a listener (subscriber), tagged to this cache type
     */
    def addMonitorListener[T <: Tracer](monitors: Monitors, tracer: T): T = {
      monitors.addMonitorListener(tracer, monitorTag)
      tracer
    }
  }

  trait CacheCommon extends Closeable {
    def kind: String = companion.kind

    def companion: CacheCompanion

    def estimatedSize(): Long

    def clear(): Long
  }

  // --- Cache types ------------------------------------------------

  object PreParserCache extends CacheCompanion("preparser") with CacheMonitorHelpers {
    type Key = PreParsedQuery.CacheKey
    type Value = PreParsedQuery

    class Cache(
      cacheFactory: CacheFactory,
      size: CacheSize,
      tracer: CacheTracer[Key]
    ) extends LFUCache[Key, Value](cacheFactory.resolveCacheKind(kind), size, tracer) with CacheCommon {
      override def companion: CacheCompanion = PreParserCache
    }
  }

  case class CacheKeyWithParameterType(key: InputQuery.CacheKey, parameterTypes: ParameterTypeMap)

  def astKey(preParsedQuery: PreParsedQuery, params: MapValue, useParameterSizeHint: Boolean): AstCache.Key =
    CacheKeyWithParameterType(preParsedQuery.cacheKey, QueryCache.extractParameterTypeMap(params, useParameterSizeHint))

  def astKeyRawQuery(preParsedQuery: PreParsedQuery, params: MapValue, useParameterSizeHint: Boolean): AstCache.Key =
    CacheKeyWithParameterType(
      preParsedQuery.cacheKeyWithRawStatement,
      QueryCache.extractParameterTypeMap(params, useParameterSizeHint)
    )

  object AstCache extends CacheCompanion("ast") with CacheMonitorHelpers {
    type Key = CacheKeyWithParameterType
    type Value = AstCacheValue

    case class AstCacheValue(parsedQuery: BaseState, notifications: Set[InternalNotification])

    def key(preParsedQuery: PreParsedQuery, params: MapValue, useParameterSizeHint: Boolean): AstCache.Key =
      astKey(preParsedQuery, params, useParameterSizeHint)

    class Cache(
      cacheFactory: CacheFactory,
      size: CacheSize,
      tracer: CacheTracer[Key]
    ) extends LFUCache[Key, Value](cacheFactory.resolveCacheKind(kind), size, tracer) with CacheCommon {

      override def companion: CacheCompanion = AstCache
    }
  }

  object LogicalPlanCache extends CacheCompanion("logical_plan") with CacheMonitorHelpers {
    case class KeyParams(statement: Statement, queryOptions: String)

    def key(
      statement: Statement,
      queryOptions: QueryOptions,
      params: MapValue,
      useParameterSizeHint: Boolean,
      txStateHasChanges: Boolean
    ): LogicalPlanCache.Key =
      CacheKey(
        KeyParams(statement, queryOptions.logicalPlanCacheKey),
        QueryCache.extractParameterTypeMap(params, useParameterSizeHint),
        txStateHasChanges,
        queryOptions.resolvedLanguage
      )

    type Key = CacheKey[LogicalPlanCache.KeyParams]
    type Value = CacheableLogicalPlan

    case class CacheableLogicalPlan(
      logicalPlanState: CachableLogicalPlanState,
      reusability: ReusabilityState,
      notifications: IndexedSeq[InternalNotification],
      override val shouldBeCached: Boolean
    ) extends CacheabilityInfo

    object LogicalPlanCacheQueryTracer extends ExecutingQueryTracer {
      override def cacheHit(executingQuery: ExecutingQuery): Unit = executingQuery.logicalPlanCacheHit()
      override def cacheMiss(executingQuery: ExecutingQuery): Unit = executingQuery.logicalPlanCacheMiss()
    }

    class Cache(
      cacheFactory: CacheFactory,
      maximumSize: CacheSize,
      stalenessCaller: PlanStalenessCaller[Value],
      tracer: CacheTracer[Key]
    ) extends QueryCache[Key, Value](
          cacheFactory.resolveCacheKind(kind),
          maximumSize,
          stalenessCaller,
          tracer,
          LogicalPlanCacheQueryTracer
        ) with CacheCommon {
      def companion: CacheCompanion = LogicalPlanCache

      override protected def shouldBeCached(cacheStrategy: CacheStrategy): Boolean = {
        cacheStrategy.logicalPlanShouldBeCached
      }
    }

    class SoftCache(
      cacheFactory: CacheFactory,
      strongSize: CacheSize,
      softSize: CacheSize,
      stalenessCaller: PlanStalenessCaller[Value],
      tracer: CacheTracer[Key]
    ) extends Cache(cacheFactory, maximumSize = strongSize, stalenessCaller = stalenessCaller, tracer = tracer) {

      override protected def createInner(
        innerFactory: CaffeineCacheFactory,
        size: CacheSize,
        listener: RemovalListener[CacheKey[LogicalPlanCache.KeyParams], CacheEntry]
      ): cache.Cache[Key, CacheEntry] = innerFactory.createWithSoftBackingCache(size, softSize, listener)
    }
  }

  case class ExecutionPlanCacheKey(
    runtimeKey: String,
    logicalPlan: LogicalPlan,
    planningAttributesCacheKey: PlanningAttributesCacheKey,
    resolvedLanguage: CypherVersion
  )

  case class CachedExecutionPlan(
    executionPlan: ExecutionPlan,
    effectiveCardinalities: ImmutablePlanningAttributes.EffectiveCardinalities,
    providedOrders: ImmutablePlanningAttributes.ProvidedOrders
  )

  object ExecutionPlanCache extends CacheCompanion("execution_plan") with CacheMonitorHelpers {
    type Key = ExecutionPlanCacheKey
    type Value = CachedExecutionPlan

    /**
     * Result of a cache lookup, indicating whether the value was freshly computed.
     */
    case class CacheResult(value: Value, isNewEntry: Boolean)

    abstract class Cache extends CacheCommon {
      def computeIfAbsent(cacheWhen: => Boolean, key: => Key, compute: => Value): CacheResult

      override def companion: CacheCompanion = ExecutionPlanCache
    }

  }

  object ExecutableQueryCache extends CacheCompanion("executable_query") with CacheMonitorHelpers {
    type Key = CacheKey[InputQuery.CacheKey]
    type Value = ExecutableQuery

    object ExecutableQueryCacheQueryTracer extends ExecutingQueryTracer {
      override def cacheHit(executingQuery: ExecutingQuery): Unit = executingQuery.executableQueryCacheHit()
      override def cacheMiss(executingQuery: ExecutingQuery): Unit = executingQuery.executableQueryCacheMiss()
    }

    class Cache(
      cacheFactory: CacheFactory,
      maximumSize: CacheSize,
      stalenessCaller: PlanStalenessCaller[Value],
      tracer: CacheTracer[Key]
    ) extends QueryCache[Key, Value](
          cacheFactory.resolveCacheKind(kind),
          maximumSize,
          stalenessCaller,
          tracer,
          ExecutableQueryCacheQueryTracer
        ) with CacheCommon {
      def companion: CacheCompanion = ExecutableQueryCache

      override protected def shouldBeCached(cacheStrategy: CacheStrategy): Boolean = {
        cacheStrategy.executableQueryShouldBeCached
      }
    }

    class SoftCache(
      cacheFactory: CacheFactory,
      strongSize: CacheSize,
      softSize: CacheSize,
      stalenessCaller: PlanStalenessCaller[Value],
      tracer: CacheTracer[Key]
    ) extends Cache(cacheFactory, strongSize, stalenessCaller, tracer) {

      override protected def createInner(
        innerFactory: CaffeineCacheFactory,
        size: CacheSize,
        listener: RemovalListener[CacheKey[InputQuery.CacheKey], CacheEntry]
      ): cache.Cache[CacheKey[InputQuery.CacheKey], CacheEntry] =
        innerFactory.createWithSoftBackingCache(size, softSize, listener)
    }
  }

  // --- Logging ----------------------------------------------------

  trait QueryCacheLogger[Key] extends CacheTracer[Key] {

    protected val itemType: String
    protected val doLog: String => Unit

    override def cacheStale(key: Key, secondsSinceReplan: Int, queryId: String, maybeReason: Option[String]): Unit = {
      super.cacheStale(key, secondsSinceReplan, queryId, maybeReason)
      logText(
        (Seq(s"Discarded stale $itemType from the $itemType cache after $secondsSinceReplan seconds") ++
          maybeReason.map(r => s". Reason: $r").toSeq).mkString(""),
        queryId
      )
    }

    override def logText(text: String, queryId: String): Unit = {
      doLog(s"$text. Query id: $queryId.")
    }
  }

  def withDebugMonitor[T](
    config: Config,
    cacheTracer: CacheTracer[T],
    monitorTracer: => CacheTracer[T]
  ): CacheTracer[T] = {
    if (config.enableDebugMonitors)
      new CombinedCacheTracer[T](cacheTracer, monitorTracer)
    else
      cacheTracer
  }

  trait PredefinedCacheTracers {
    val preParser: PreParserCacheMetricsMonitor = new PreParserCacheMetricsMonitor
    val ast: ASTCacheMetricsMonitor = new ASTCacheMetricsMonitor
    val executionPlan: ExecutionPlanCacheMetricsMonitor = new ExecutionPlanCacheMetricsMonitor
    val logicalPlan: LogicalPlanCacheMetricsMonitor = new LogicalPlanCacheMetricsMonitor
    val executablePlan: ExecutableQueryCacheMetricsMonitor = new ExecutableQueryCacheMetricsMonitor

    def perCacheKind: Map[String, CacheMetricsMonitor[_]] = {
      Seq(
        preParser,
        ast,
        executionPlan,
        logicalPlan,
        executablePlan
      ).map(tracer => tracer.cacheKind -> tracer).toMap
    }
  }

  case class CacheStrategy(
    executableQueryCache: CypherCacheOption,
    preParserCache: CypherCacheOption,
    astCache: CypherCacheOption,
    logicalPlanCache: CypherCacheOption,
    executionPlanCache: CypherCacheOption
  )(config: CypherConfiguration = null) {

    def executableQueryShouldBeCached: Boolean = CacheStrategy.shouldBeCached(executableQueryCache)
    def preParserShouldBeCached: Boolean = CacheStrategy.shouldBeCached(preParserCache)
    def astShouldBeCached: Boolean = CacheStrategy.shouldBeCached(astCache)
    def logicalPlanShouldBeCached: Boolean = CacheStrategy.shouldBeCached(logicalPlanCache)
    def executionPlanShouldBeCached: Boolean = CacheStrategy.shouldBeCached(executionPlanCache)

    // Used for testing and benchmarking
    def unknownKindShouldBeCached: Boolean = CacheStrategy.shouldBeCached(executableQueryCache)

    def withConfig(config: CypherConfiguration): CacheStrategy = this.copy()(config)

    def updateFromQueryOptions(options: CypherQueryOptions): CacheStrategy = {
      options.cache match {
        case CypherCacheOption.force => CacheStrategy.forceAll
        case CypherCacheOption.skip  => CacheStrategy.skipAll
        case _                       => this
      }
    }

    def updateFromQueryText(queryText: String): CacheStrategy = {
      if (
        (executableQueryCache eq CypherCacheOption.default) ||
        (preParserCache eq CypherCacheOption.default) ||
        (astCache eq CypherCacheOption.default)
      ) {
        // If any of these three are set to default, we need to check the size to see if it is below the threshold
        val maxQueryTextSize = config.queryCacheMaxQueryTextSize
        val shouldBeCached = maxQueryTextSize > 0 && {
          val queryTextSize = HeapEstimator.sizeOf(queryText)
          val isBelowTheLimit = queryTextSize <= maxQueryTextSize
          isBelowTheLimit
        }
        this.copy(
          executableQueryCache = updateDefaultValue(executableQueryCache, shouldBeCached),
          preParserCache = updateDefaultValue(preParserCache, shouldBeCached),
          astCache = updateDefaultValue(astCache, shouldBeCached)
        )(config)
      } else {
        this.copy()(config)
      }
    }

    def updateFromAst(ast: Statement): CacheStrategy = {
      logicalPlanCache match {
        case CypherCacheOption.default =>
          val maxAstSize = config.queryCacheMaxAstSize
          // NOTE: Negative value means unlimited
          val shouldBeCached = maxAstSize < 0 || maxAstSize > 0 && {
            val astSize = estimateAstSize(ast)
            val isBelowTheLimit = astSize <= maxAstSize
            isBelowTheLimit
          }
          this.copy(logicalPlanCache = updateDefaultValue(logicalPlanCache, shouldBeCached))(config)
        case _ =>
          this
      }
    }

    def updateFromLogicalPlan(cacheableLogicalPlan: CacheableLogicalPlan): CacheStrategy = {
      executionPlanCache match {
        case CypherCacheOption.force | CypherCacheOption.default if !cacheableLogicalPlan.shouldBeCached =>
          // Some conditions determined by the planner prevents caching and even overrides force
          this.copy(executionPlanCache = CypherCacheOption.skip)(config)
        case CypherCacheOption.default =>
          val maxLogicalPlanSize = config.queryCacheMaxLogicalPlanSize
          // NOTE: Negative value means unlimited
          val shouldBeCached = maxLogicalPlanSize < 0 || maxLogicalPlanSize > 0 && {
            val logicalPlanSize = estimateLogicalPlanSize(cacheableLogicalPlan.logicalPlanState.logicalPlan)
            val isBelowTheLimit = logicalPlanSize <= maxLogicalPlanSize
            isBelowTheLimit
          }
          this.copy(executionPlanCache = updateDefaultValue(executionPlanCache, shouldBeCached))(config)
        case _ =>
          this
      }
    }

  }

  object CacheStrategy {

    private def shouldBeCached(option: CypherCacheOption): Boolean = option != CypherCacheOption.skip

    private def updateDefaultValue(current: CypherCacheOption, shouldBeCached: Boolean): CypherCacheOption = {
      current match {
        case CypherCacheOption.default if shouldBeCached  => CypherCacheOption.force
        case CypherCacheOption.default if !shouldBeCached => CypherCacheOption.skip
        case _                                            => current
      }
    }

    val default: CacheStrategy = CacheStrategy(
      CypherCacheOption.default,
      CypherCacheOption.default,
      CypherCacheOption.default,
      CypherCacheOption.default,
      CypherCacheOption.default
    )()

    val skipAll: CacheStrategy = CacheStrategy(
      CypherCacheOption.skip,
      CypherCacheOption.skip,
      CypherCacheOption.skip,
      CypherCacheOption.skip,
      CypherCacheOption.skip
    )()

    val forceAll: CacheStrategy = CacheStrategy(
      CypherCacheOption.force,
      CypherCacheOption.force,
      CypherCacheOption.force,
      CypherCacheOption.force,
      CypherCacheOption.force
    )()

    @VisibleForTesting
    val defaultDefault: CacheStrategy =
      default.withConfig(CypherConfiguration.fromConfig(org.neo4j.configuration.Config.defaults()))
  }

  def estimateAstSize(ast: Statement): Long = {
    ast.folder.treeCount {
      case _: ASTNode => ()
    }
  }

  def estimateLogicalPlanSize(logicalPlan: LogicalPlan): Long = {
    logicalPlan.folder.treeCount {
      case _: LogicalPlan => ()
      case _: Expression  => ()
    }
  }
}

/**
 * Container for all caches associated with a single cypher execution stack (i.e. a single database)
 *
 * @param config                    Configuration for all caches
 * @param lastCommittedTxIdProvider Reports the id of the latest committed transaction. Used to compute logical plan staleness
 * @param cacheFactory              Factory used to create the backing caffeine caches
 * @param clock                     Clock used to compute logical plan staleness
 * @param kernelMonitors            Monitors to publish events to
 * @param logProvider               Provides logs for logging eviction events etc.
 */
class CypherQueryCaches(
  config: CypherQueryCaches.Config,
  lastCommittedTxIdProvider: () => Long,
  cacheFactory: CacheFactory,
  clock: Clock,
  kernelMonitors: Monitors,
  logProvider: InternalLogProvider
) extends Closeable {

  private val log = logProvider.getLog(getClass)

  private val allCaches = new CopyOnWriteArrayList[CacheCommon]()

  private object cacheTracers extends PredefinedCacheTracers {

    override val logicalPlan: LogicalPlanCacheMetricsMonitor =
      new LogicalPlanCacheMetricsMonitor with QueryCacheLogger[CypherQueryCaches.LogicalPlanCache.Key] {
        override protected val itemType: String = "plan"
        override protected val doLog: String => Unit = log.debug
      }

    override val executablePlan: ExecutableQueryCacheMetricsMonitor =
      new ExecutableQueryCacheMetricsMonitor with QueryCacheLogger[ExecutableQueryCache.Key] {
        override protected val itemType: String = "query"
        override protected val doLog: String => Unit = log.info
      }
  }

  /**
   * Caches pre-parsing
   */
  val preParserCache: PreParserCache.Cache =
    registerCache(new PreParserCache.Cache(
      cacheFactory,
      config.cacheSize,
      withDebugMonitor(config, cacheTracers.preParser, PreParserCache.newMonitor(kernelMonitors))
    ))

  /**
   * Container for caches used by a single planner instance
   */
  class CypherPlannerCaches() {

    /**
     * Caches parsing
     */
    val astCache: AstCache.Cache = registerCache(new AstCache.Cache(
      cacheFactory,
      config.cacheSize,
      withDebugMonitor(config, cacheTracers.ast, AstCache.newMonitor(kernelMonitors))
    ))

    /**
     * Caches logical planning
     */
    val logicalPlanCache: LogicalPlanCache.Cache = {
      val stalenessCaller = new DefaultPlanStalenessCaller[LogicalPlanCache.Value](
        clock,
        divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(config.divergenceConfig),
        lastCommittedTxIdProvider,
        (state, _) => state.reusability,
        log
      )
      registerCache(
        config.softCacheSize match {
          case SoftCacheSize.Disabled => new LogicalPlanCache.Cache(
              cacheFactory = cacheFactory,
              maximumSize = config.cacheSize,
              stalenessCaller = stalenessCaller,
              tracer = withDebugMonitor(config, cacheTracers.logicalPlan, LogicalPlanCache.newMonitor(kernelMonitors))
            )
          case SoftCacheSize.Sized(strongSize, softSize) => new LogicalPlanCache.SoftCache(
              cacheFactory = cacheFactory,
              strongSize = strongSize,
              softSize = softSize,
              stalenessCaller = stalenessCaller,
              tracer = withDebugMonitor(config, cacheTracers.logicalPlan, LogicalPlanCache.newMonitor(kernelMonitors))
            )
        }
      )
    }
  }

  /**
     * Caches physical planning
     */
  val executionPlanCache: ExecutionPlanCache.Cache = registerCache(new ExecutionPlanCache.Cache {

    private type InnerCache = LFUCache[ExecutionPlanCache.Key, ExecutionPlanCache.Value]

    private val tracer: CacheTracer[ExecutionPlanCache.Key] = {
      if (config.enableExecutionPlanCacheTracing) {
        withDebugMonitor(config, cacheTracers.executionPlan, ExecutionPlanCache.newMonitor(kernelMonitors))
      } else {
        new CacheTracer[ExecutionPlanCache.Key] {}
      }
    }

    private val maybeCache: Option[InnerCache] = config.executionPlanCacheSize match {
      case Disabled => None
      case Default => Some(new InnerCache(
          cacheFactory.resolveCacheKind(kind),
          config.cacheSize,
          tracer
        ))
      case Sized(cacheSize) =>
        Some(new InnerCache(cacheFactory.resolveCacheKind(kind), CacheSize.Static(cacheSize), tracer))
    }

    def close(): Unit = maybeCache match {
      case Some(closable: java.io.Closeable) => closable.close()
      case _                                 => ()
    }

    def computeIfAbsent(
      cacheWhen: => Boolean,
      key: => ExecutionPlanCache.Key,
      compute: => ExecutionPlanCache.Value
    ): ExecutionPlanCache.CacheResult =
      maybeCache match {
        case Some(cache) if cacheWhen =>
          var isNew = false
          val value = cache.computeIfAbsent(key, { isNew = true; compute })
          ExecutionPlanCache.CacheResult(value, isNew)

        case _ =>
          ExecutionPlanCache.CacheResult(compute, isNewEntry = false)
      }

    def clear(): Long = maybeCache match {
      case Some(cache) => cache.clear()
      case None        => 0
    }

    override def estimatedSize(): Long = maybeCache.fold(0L)(_.estimatedSize())
  })

  /**
     * Caches complete query processing
     */
  val executableQueryCache: ExecutableQueryCache.Cache = {
    val stalenessCaller = new DefaultPlanStalenessCaller[ExecutableQuery](
      clock = clock,
      divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(config.divergenceConfig),
      lastCommittedTxIdProvider = lastCommittedTxIdProvider,
      reusabilityInfo = (eq, ctx) => eq.reusabilityState(lastCommittedTxIdProvider, ctx),
      log = log
    )
    registerCache(
      config.softCacheSize match {
        case SoftCacheSize.Disabled => new ExecutableQueryCache.Cache(
            cacheFactory = cacheFactory,
            maximumSize = config.cacheSize,
            stalenessCaller = stalenessCaller,
            tracer =
              withDebugMonitor(config, cacheTracers.executablePlan, ExecutableQueryCache.newMonitor(kernelMonitors))
          )
        case SoftCacheSize.Sized(strongSize, softSize) => new ExecutableQueryCache.SoftCache(
            cacheFactory = cacheFactory,
            strongSize = strongSize,
            softSize = softSize,
            stalenessCaller = stalenessCaller,
            tracer =
              withDebugMonitor(config, cacheTracers.executablePlan, ExecutableQueryCache.newMonitor(kernelMonitors))
          )
      }
    )
  }

  private def registerCache[T <: CacheCommon](cache: T): T = {
    allCaches.add(cache)
    cache
  }

  def close(): Unit =
    allCaches.forEach(_.close())

  private object stats extends QueryCacheStatistics {

    override def preParserCacheEntries(): Long =
      preParserCache.estimatedSize()

    override def astCacheEntries(): Long =
      allCaches.asScala
        .collect { case c: AstCache.Cache => c.estimatedSize() }
        .sum

    override def logicalPlanCacheEntries(): Long =
      allCaches.asScala
        .collect { case c: LogicalPlanCache.Cache => c.estimatedSize() }
        .sum

    override def executionPlanCacheEntries(): Long = executionPlanCache.estimatedSize()

    override def executableQueryCacheEntries(): Long = executableQueryCache.estimatedSize()

    // Warning! This is O(n).
    override def executableQueryCacheCodeGenSize(): Long =
      executableQueryCache.values.map(_.value.codeGenByteCodeSize).sum

    override def numberOfReplans(): Long = cacheTracers.executablePlan.numberOfReplans

    override def replanWaitTime(): Long = cacheTracers.executablePlan.replanWaitTime

    override def metricsPerCacheKind(): java.util.Map[String, CacheMetrics] = {
      (cacheTracers.perCacheKind: Map[String, CacheMetrics]).asJava
    }
  }

  def statistics(): QueryCacheStatistics = stats

  def clearAll(): Unit =
    allCaches.forEach(c => c.clear())
}
