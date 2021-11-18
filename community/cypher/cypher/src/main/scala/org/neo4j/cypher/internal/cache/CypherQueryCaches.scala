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
package org.neo4j.cypher.internal.cache

import org.neo4j.cypher.internal.CacheTracer
import org.neo4j.cypher.internal.CacheabilityInfo
import org.neo4j.cypher.internal.DefaultPlanStalenessCaller
import org.neo4j.cypher.internal.ExecutableQuery
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.PreParsedQuery
import org.neo4j.cypher.internal.QueryCache
import org.neo4j.cypher.internal.QueryCache.CacheKey
import org.neo4j.cypher.internal.QueryCache.ParameterTypeMap
import org.neo4j.cypher.internal.ReusabilityState
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.cache.CypherQueryCaches.AstCache
import org.neo4j.cypher.internal.cache.CypherQueryCaches.Config.ExecutionPlanCacheSize
import org.neo4j.cypher.internal.cache.CypherQueryCaches.Config.ExecutionPlanCacheSize.Default
import org.neo4j.cypher.internal.cache.CypherQueryCaches.Config.ExecutionPlanCacheSize.Disabled
import org.neo4j.cypher.internal.cache.CypherQueryCaches.Config.ExecutionPlanCacheSize.Sized
import org.neo4j.cypher.internal.cache.CypherQueryCaches.ExecutableQueryCache
import org.neo4j.cypher.internal.cache.CypherQueryCaches.ExecutionPlanCache
import org.neo4j.cypher.internal.cache.CypherQueryCaches.LogicalPlanCache
import org.neo4j.cypher.internal.cache.CypherQueryCaches.QueryCacheStaleLogger
import org.neo4j.cypher.internal.compiler.StatsDivergenceCalculator
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.config.StatsDivergenceCalculatorConfig
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.planner.spi.PlanningAttributesCacheKey
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.logging.LogProvider
import org.neo4j.monitoring.Monitors
import org.neo4j.values.virtual.MapValue

import java.time.Clock

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
    cacheSize: Int,
    executionPlanCacheSize: ExecutionPlanCacheSize,
    divergenceConfig: StatsDivergenceCalculatorConfig,
    enableExecutionPlanCacheTracing: Boolean
  ) {
    // Java helper
    def this(cypherConfig: CypherConfiguration) = this(
      cypherConfig.queryCacheSize,
      ExecutionPlanCacheSize.fromInt(cypherConfig.executionPlanCacheSize),
      cypherConfig.statsDivergenceCalculator,
      cypherConfig.enableMonitors,
    )
  }

  object Config {
    def fromCypherConfiguration(cypherConfig: CypherConfiguration) =
      new Config(cypherConfig)

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
  }

  // --- Helpers ----------------------------------------------------

  trait CacheCompanion {
    type Key
    type Value

    def monitorTag: String =
      getClass.getSimpleName.replace("$", "")
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

  // --- Cache types ------------------------------------------------

  object PreParserCache extends CacheCompanion {
    type Key = String
    type Value = PreParsedQuery
  }

  object AstCache extends CacheCompanion {
    type Key = AstCacheKey
    type Value = BaseState

    case class AstCacheKey(
      key: String,
      parameterTypes: ParameterTypeMap
    )
  }

  object LogicalPlanCache extends CacheCompanion with CacheMonitorHelpers {
    type Key = CacheKey[Statement]
    type Value = CacheableLogicalPlan

    case class CacheableLogicalPlan(
      logicalPlanState: LogicalPlanState,
      reusability: ReusabilityState,
      notifications: IndexedSeq[InternalNotification],
      override val shouldBeCached: Boolean
    ) extends CacheabilityInfo
  }

  object ExecutionPlanCache extends CacheCompanion with CacheMonitorHelpers {
    type Key = ExecutionPlanCacheKey
    type Value = (ExecutionPlan, PlanningAttributes)

    case class ExecutionPlanCacheKey(
      runtimeKey: String,
      logicalPlan: LogicalPlan,
      planningAttributesCacheKey: PlanningAttributesCacheKey
    )
  }

  object ExecutableQueryCache extends CacheCompanion with CacheMonitorHelpers {
    type Key = CacheKey[String]
    type Value = ExecutableQuery
  }

  // --- Logging ----------------------------------------------------

  class QueryCacheStaleLogger[Key](itemType: String, doLog: String => Unit) extends CacheTracer[Key] {
    override def queryCacheStale(key: Key, secondsSinceReplan: Int, queryId: String, maybeReason: Option[String]): Unit =
      doLog(
        (Seq(s"Discarded stale $itemType from the $itemType cache after $secondsSinceReplan seconds.") ++
         maybeReason.map(r => s"Reason: $r.").toSeq ++
         Seq(s"Query id: $queryId.")
          ).mkString(" ")
      )
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
  cacheFactory: CaffeineCacheFactory,
  clock: Clock,
  kernelMonitors: Monitors,
  logProvider: LogProvider,
) {

  private val log = logProvider.getLog(getClass)

  /**
   * Caches pre-parsing
   */
  object preParserCache extends LFUCache[String, PreParsedQuery](cacheFactory, config.cacheSize)

  /**
   * Container for caches used by a single planner instance
   */
  class CypherPlannerCaches() {

    /**
     * Caches parsing
     */
    object astCache extends LFUCache[AstCache.Key, AstCache.Value](cacheFactory, config.cacheSize) {

      def key(preParsedQuery: PreParsedQuery, params: MapValue): AstCache.Key =
        AstCache.AstCacheKey(preParsedQuery.cacheKey, QueryCache.extractParameterTypeMap(params))
    }

    /**
     * Caches logical planning
     */
    object logicalPlanCache extends QueryCache[LogicalPlanCache.Key, LogicalPlanCache.Value](
      cacheFactory = cacheFactory,
      maximumSize = config.cacheSize,
      stalenessCaller = new DefaultPlanStalenessCaller[LogicalPlanCache.Value](
        clock,
        divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(config.divergenceConfig),
        lastCommittedTxIdProvider,
        (state, _) => state.reusability,
        log),
      tracer = LogicalPlanCache.newMonitor(kernelMonitors)
    ) {
      LogicalPlanCache.addMonitorListener(kernelMonitors, new QueryCacheStaleLogger[LogicalPlanCache.Key]("plan", log.debug))
    }
  }

  /**
   * Caches physical planning
   */
  object executionPlanCache {

    private type Cache = LFUCache[ExecutionPlanCache.Key, ExecutionPlanCache.Value]

    private val maybeCache: Option[Cache] = config.executionPlanCacheSize match {
      case Disabled         => None
      case Default          => Some(new Cache(cacheFactory, config.cacheSize))
      case Sized(cacheSize) => Some(new Cache(cacheFactory, cacheSize))
    }

    private val tracer: CacheTracer[ExecutionPlanCache.Key] =
      if (config.enableExecutionPlanCacheTracing) {
        ExecutionPlanCache.newMonitor(kernelMonitors)
      } else {
        new CacheTracer[ExecutionPlanCache.Key] {}
      }

    def computeIfAbsent(
      cacheWhen: => Boolean,
      key: => ExecutionPlanCache.Key,
      compute: => ExecutionPlanCache.Value
    ): ExecutionPlanCache.Value =
      maybeCache match {
        case Some(cache) if cacheWhen =>
          var hit = true
          val keyVal = key
          val result = cache.computeIfAbsent(keyVal, {
            hit = false
            compute
          })
          if (hit) {
            tracer.queryCacheHit(keyVal, "")
          } else {
            tracer.queryCacheMiss(keyVal, "")
          }
          result

        case _ =>
          compute
      }

    def clear(): Long = maybeCache match {
      case Some(cache) => cache.clear()
      case None        => 0
    }
  }

  /**
   * Caches complete query processing
   */
  object executableQueryCache extends QueryCache[ExecutableQueryCache.Key, ExecutableQueryCache.Value](
    cacheFactory = cacheFactory,
    maximumSize = config.cacheSize,
    stalenessCaller = new DefaultPlanStalenessCaller[ExecutableQuery](
      clock = clock,
      divergenceCalculator = StatsDivergenceCalculator.divergenceCalculatorFor(config.divergenceConfig),
      lastCommittedTxIdProvider = lastCommittedTxIdProvider,
      reusabilityInfo = (eq, ctx) => eq.reusabilityState(lastCommittedTxIdProvider, ctx),
      log = log),
    tracer = ExecutableQueryCache.newMonitor(kernelMonitors),
  ) {
    ExecutableQueryCache.addMonitorListener(kernelMonitors, new QueryCacheStaleLogger[ExecutableQueryCache.Key]("query", log.info))
  }

}

