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
package org.neo4j.cypher.internal.planning

import org.neo4j.cypher.internal.CacheTracer
import org.neo4j.cypher.internal.CacheabilityInfo
import org.neo4j.cypher.internal.DefaultPlanStalenessCaller
import org.neo4j.cypher.internal.PlanStalenessCaller
import org.neo4j.cypher.internal.QueryCache
import org.neo4j.cypher.internal.QueryCache.CacheKey
import org.neo4j.cypher.internal.ReusabilityState
import org.neo4j.cypher.internal.cache.CaffeineCacheFactory
import org.neo4j.cypher.internal.compiler.StatsDivergenceCalculator
import org.neo4j.cypher.internal.compiler.phases.CachableLogicalPlanState
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.logging.Log

import java.time.Clock

/**
 * Cache which stores logical plans indexed by an AST statement.
 *
 * @param maximumSize               Maximum size of this cache
 * @param tracer                    Traces cache activity
 * @param clock                     Clock used to compute logical plan staleness
 * @param divergence                Statistics divergence calculator used to compute logical plan staleness
 * @param lastCommittedTxIdProvider Last committed transaction id provider used to compute logical plan staleness
 * @tparam STATEMENT Type of AST statement used as key
 */
class AstLogicalPlanCache[STATEMENT <: AnyRef](override val cacheFactory: CaffeineCacheFactory,
                                               override val maximumSize: Int,
                                               override val tracer: CacheTracer[CacheKey[STATEMENT]],
                                               clock: Clock,
                                               divergence: StatsDivergenceCalculator,
                                               lastCommittedTxIdProvider: () => Long,
                                               log: Log)
  extends QueryCache[CacheKey[STATEMENT], CacheableLogicalPlan](
    cacheFactory,
    maximumSize,
    AstLogicalPlanCache.stalenessCaller(clock,
      divergence,
      lastCommittedTxIdProvider,
      log),
    tracer) {

  def logStalePlanRemovalMonitor(log: Log): CacheTracer[STATEMENT] =
    new CacheTracer[STATEMENT] {
      override def queryCacheStale(key: STATEMENT, secondsSinceReplan: Int, metaData: String, maybeReason: Option[String]): Unit = {
        log.debug(s"Discarded stale plan from the plan cache after $secondsSinceReplan " +
          s"seconds${maybeReason.fold("")(r => s". Reason: $r")}. Metadata: $metaData")
      }

      override def queryCacheHit(queryKey: STATEMENT, metaData: String): Unit = {}

      override def queryCacheMiss(queryKey: STATEMENT, metaData: String): Unit = {}

      override def queryCacheFlush(sizeOfCacheBeforeFlush: Long): Unit = {}

      override def queryCompile(queryKey: STATEMENT, metaData: String): Unit = {}

      override def queryCompileWithExpressionCodeGen(queryKey: STATEMENT, metaData: String): Unit = {}
    }
}

object AstLogicalPlanCache {
  /**
   * @param clock                     Clock for measuring elapsed time.
   * @param divergence                Computes is the plan i stale depending on changes in the underlying
   *                                  statistics, and how much time has passed.
   * @param lastCommittedTxIdProvider Reports the id of the latest committed transaction.
   * @return a [[PlanStalenessCaller]]
   */
  def stalenessCaller(clock: Clock,
                      divergence: StatsDivergenceCalculator,
                      lastCommittedTxIdProvider: () => Long,
                      log: Log): PlanStalenessCaller[CacheableLogicalPlan] = {
    new DefaultPlanStalenessCaller[CacheableLogicalPlan](clock, divergence, lastCommittedTxIdProvider, (state, _) => state.reusability, log)
  }
}

case class CacheableLogicalPlan(logicalPlanState: CachableLogicalPlanState,
                                reusability: ReusabilityState, notifications: IndexedSeq[InternalNotification],
                                override val shouldBeCached: Boolean) extends CacheabilityInfo
