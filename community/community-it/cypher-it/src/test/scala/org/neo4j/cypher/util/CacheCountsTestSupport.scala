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
package org.neo4j.cypher.util

import org.neo4j.cypher.GraphDatabaseTestSupport
import org.neo4j.cypher.internal.cache.CypherQueryCaches.CacheCompanion
import org.neo4j.cypher.util.CacheCountsTestSupport.CacheCounts
import org.neo4j.kernel.impl.query.CacheMetrics
import org.neo4j.kernel.impl.query.QueryCacheStatistics

import scala.jdk.CollectionConverters.IterableHasAsScala

trait CacheCountsTestSupport {
  self: GraphDatabaseTestSupport =>

  def cacheCountsFor(cache: CacheCompanion): CacheCounts = {
    cacheCountsFor(cache, graph.getDependencyResolver.resolveDependency(classOf[QueryCacheStatistics]))
  }

  def cacheCountsFor(cache: CacheCompanion, statistics: QueryCacheStatistics): CacheCounts = {
    statistics.metricsPerCacheKind().values().asScala.collectFirst {
      case metrics: CacheMetrics if metrics.cacheKind() == cache.kind =>
        CacheCounts.fromMetrics(metrics)
    }.get
  }

  def cacheCountsDiffFor(cache: CacheCompanion)(testBody: (() => CacheCounts) => Unit): Unit = {
    val initialCounts = cacheCountsFor(cache)
    val diffCountsF = () => cacheCountsFor(cache) - initialCounts;
    testBody(diffCountsF)
  }
}

object CacheCountsTestSupport {

  case class CacheCounts(
    hits: Long = 0,
    misses: Long = 0,
    flushes: Long = 0,
    evicted: Long = 0,
    discards: Long = 0,
    compilations: Long = 0,
    compilationsWithExpressionCodeGen: Long = 0,
    awaits: Long = 0
  ) {

    def -(that: CacheCounts): CacheCounts = {
      CacheCounts(
        hits = hits - that.hits,
        misses = misses - that.misses,
        flushes = flushes - that.flushes,
        evicted = evicted - that.evicted,
        discards = discards - that.discards,
        compilations = compilations - that.compilations,
        compilationsWithExpressionCodeGen = compilationsWithExpressionCodeGen - that.compilationsWithExpressionCodeGen,
        awaits = awaits - that.awaits
      )
    }

    override def toString =
      s"hits = $hits, misses = $misses, flushes = $flushes, evicted = $evicted, discards = $discards, compilations = $compilations, compilationsWithExpressionCodeGen = $compilationsWithExpressionCodeGen, awaits = $awaits"
  }

  object CacheCounts {

    def fromMetrics(metrics: CacheMetrics): CacheCounts = {
      CacheCounts(
        hits = metrics.getHits,
        misses = metrics.getMisses,
        flushes = metrics.getCacheFlushes,
        evicted = metrics.getStaleEntries,
        discards = metrics.getDiscards,
        compilations =
          metrics.getCompiled - metrics.getCompiledWithExpressionCodeGen, // avoid double-counting getCompiledWithExpressionCodeGen
        compilationsWithExpressionCodeGen = metrics.getCompiledWithExpressionCodeGen,
        awaits = metrics.getAwaits
      )
    }
  }
}
