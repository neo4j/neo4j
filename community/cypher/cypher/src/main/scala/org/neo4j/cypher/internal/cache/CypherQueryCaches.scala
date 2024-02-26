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
import org.neo4j.cypher.internal.DefaultPlanStalenessCaller
import org.neo4j.cypher.internal.ExecutableQuery
import org.neo4j.cypher.internal.ExecutingQueryTracer
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.InputQuery
import org.neo4j.cypher.internal.PlanStalenessCaller
import org.neo4j.cypher.internal.PreParsedQuery
import org.neo4j.cypher.internal.QueryCache
import org.neo4j.cypher.internal.QueryCache.CacheKey
import org.neo4j.cypher.internal.QueryCache.ParameterTypeMap
import org.neo4j.cypher.internal.QueryOptions
import org.neo4j.cypher.internal.ReusabilityState
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.cache.CypherQueryCaches.AstCache
import org.neo4j.cypher.internal.cache.CypherQueryCaches.CacheCommon
import org.neo4j.cypher.internal.cache.CypherQueryCaches.Config.ExecutionPlanCacheSize
import org.neo4j.cypher.internal.cache.CypherQueryCaches.Config.ExecutionPlanCacheSize.Default
import org.neo4j.cypher.internal.cache.CypherQueryCaches.Config.ExecutionPlanCacheSize.Disabled
import org.neo4j.cypher.internal.cache.CypherQueryCaches.Config.ExecutionPlanCacheSize.Sized
import org.neo4j.cypher.internal.cache.CypherQueryCaches.Config.SoftCacheSize
import org.neo4j.cypher.internal.cache.CypherQueryCaches.ExecutableQueryCache
import org.neo4j.cypher.internal.cache.CypherQueryCaches.ExecutionPlanCache
import org.neo4j.cypher.internal.cache.CypherQueryCaches.LogicalPlanCache
import org.neo4j.cypher.internal.cache.CypherQueryCaches.PreParserCache
import org.neo4j.cypher.internal.cache.CypherQueryCaches.PredefinedCacheTracers
import org.neo4j.cypher.internal.cache.CypherQueryCaches.QueryCacheStaleLogger
import org.neo4j.cypher.internal.cache.CypherQueryCaches.withDebugMonitor
import org.neo4j.cypher.internal.compiler.StatsDivergenceCalculator
import org.neo4j.cypher.internal.compiler.phases.CachableLogicalPlanState
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.config.StatsDivergenceCalculatorConfig
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributesCacheKey
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.function.Observable
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.impl.query.CacheMetrics
import org.neo4j.kernel.impl.query.QueryCacheStatistics
import org.neo4j.logging.InternalLogProvider
import org.neo4j.monitoring.Monitors
import org.neo4j.values.virtual.MapValue

import java.lang
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

  trait CacheCommon {
    def kind: String = companion.kind

    def companion: CacheCompanion

    def estimatedSize(): Long

    def clear(): Long
  }

  // --- Cache types ------------------------------------------------

  object PreParserCache extends CacheCompanion("preparser") with CacheMonitorHelpers {
    type Key = String
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
        txStateHasChanges
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
        listener: RemovalListener[CacheKey[LogicalPlanCache.KeyParams], CachedValue]
      ): cache.Cache[Key, CachedValue] = innerFactory.createWithSoftBackingCache(size, softSize, listener)
    }
  }

  case class ExecutionPlanCacheKey(
    runtimeKey: String,
    logicalPlan: LogicalPlan,
    planningAttributesCacheKey: PlanningAttributesCacheKey
  )

  case class CachedExecutionPlan(
    executionPlan: ExecutionPlan,
    effectiveCardinalities: EffectiveCardinalities,
    providedOrders: ProvidedOrders
  )

  object ExecutionPlanCache extends CacheCompanion("execution_plan") with CacheMonitorHelpers {
    type Key = ExecutionPlanCacheKey
    type Value = CachedExecutionPlan

    abstract class Cache extends CacheCommon {
      def computeIfAbsent(cacheWhen: => Boolean, key: => Key, compute: => Value): Value

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
        listener: RemovalListener[CacheKey[InputQuery.CacheKey], CachedValue]
      ): cache.Cache[CacheKey[InputQuery.CacheKey], CachedValue] =
        innerFactory.createWithSoftBackingCache(size, softSize, listener)
    }
  }

  // --- Logging ----------------------------------------------------

  trait QueryCacheStaleLogger[Key] extends CacheTracer[Key] {

    protected val itemType: String
    protected val doLog: String => Unit

    override def cacheStale(key: Key, secondsSinceReplan: Int, queryId: String, maybeReason: Option[String]): Unit = {
      super.cacheStale(key, secondsSinceReplan, queryId, maybeReason)
      doLog(
        (Seq(s"Discarded stale $itemType from the $itemType cache after $secondsSinceReplan seconds.") ++
          maybeReason.map(r => s"Reason: $r.").toSeq ++
          Seq(s"Query id: $queryId.")).mkString(" ")
      )
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
) {

  private val log = logProvider.getLog(getClass)

  private val allCaches = new CopyOnWriteArrayList[CacheCommon]()

  private object cacheTracers extends PredefinedCacheTracers {

    override val logicalPlan: LogicalPlanCacheMetricsMonitor =
      new LogicalPlanCacheMetricsMonitor with QueryCacheStaleLogger[CypherQueryCaches.LogicalPlanCache.Key] {
        override protected val itemType: String = "plan"
        override protected val doLog: String => Unit = log.debug
      }

    override val executablePlan: ExecutableQueryCacheMetricsMonitor =
      new ExecutableQueryCacheMetricsMonitor with QueryCacheStaleLogger[ExecutableQueryCache.Key] {
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

    def computeIfAbsent(
      cacheWhen: => Boolean,
      key: => ExecutionPlanCache.Key,
      compute: => ExecutionPlanCache.Value
    ): ExecutionPlanCache.Value =
      maybeCache match {
        case Some(cache) if cacheWhen =>
          cache.computeIfAbsent(key, compute)

        case _ =>
          compute
      }

    def clear(): Long = maybeCache match {
      case Some(cache) => cache.clear()
      case None        => 0
    }

    override def estimatedSize(): Long =
      maybeCache.fold(0L)(_.estimatedSize())
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

  private object stats extends QueryCacheStatistics {

    override def preParserCacheEntries(): lang.Long =
      preParserCache.estimatedSize()

    override def astCacheEntries(): lang.Long =
      allCaches.asScala
        .collect { case c: AstCache.Cache => c.estimatedSize() }
        .sum

    override def logicalPlanCacheEntries(): lang.Long =
      allCaches.asScala
        .collect { case c: LogicalPlanCache.Cache => c.estimatedSize() }
        .sum

    override def executionPlanCacheEntries(): lang.Long =
      executionPlanCache.estimatedSize()

    override def executableQueryCacheEntries(): lang.Long =
      executableQueryCache.estimatedSize()

    override def numberOfReplans(): lang.Long =
      cacheTracers.executablePlan.numberOfReplans

    override def replanWaitTime(): lang.Long =
      cacheTracers.executablePlan.replanWaitTime

    override def metricsPerCacheKind(): java.util.Map[String, CacheMetrics] = {
      (cacheTracers.perCacheKind: Map[String, CacheMetrics]).asJava
    }
  }

  def statistics(): QueryCacheStatistics = stats

  def clearAll(): Unit =
    allCaches.forEach(c => c.clear())
}
