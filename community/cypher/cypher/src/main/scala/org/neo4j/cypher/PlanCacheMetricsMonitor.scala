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
package org.neo4j.cypher

import org.neo4j.cypher.internal.InputQuery
import org.neo4j.cypher.internal.QueryCache.CacheKey
import org.neo4j.cypher.internal.cache.CacheTracer
import org.neo4j.cypher.internal.cache.CypherQueryCaches
import org.neo4j.kernel.impl.query.CacheMetrics

import java.util.concurrent.atomic.LongAdder

abstract class CacheMetricsMonitor[KEY] extends CacheTracer[KEY] with CacheMetrics {
  val monitorTag: String

  private val hits = new LongAdder
  private val misses = new LongAdder
  private val compiled = new LongAdder
  private val compiledWithExpressionCodeGen = new LongAdder
  private val discards = new LongAdder
  private val staleEntries = new LongAdder
  private val cacheFlushes = new LongAdder
  private val awaits = new LongAdder

  override def cacheHit(key: KEY, metaData: String): Unit = hits.increment()

  override def cacheMiss(key: KEY, metaData: String): Unit = misses.increment()

  override def compute(key: KEY, metaData: String): Unit = compiled.increment()

  override def discard(key: KEY, metaData: String): Unit = discards.increment()

  override def awaitOngoingComputation(key: KEY, metaData: String): Unit = awaits.increment()

  override def computeWithExpressionCodeGen(key: KEY, metaData: String): Unit = {
    compiled.increment()
    compiledWithExpressionCodeGen.increment()
  }

  override def cacheStale(key: KEY, secondsSinceCompute: Int, metaData: String, maybeReason: Option[String]): Unit =
    staleEntries.increment()

  override def cacheFlush(sizeOfCacheBeforeFlush: Long): Unit = {
    discards.add(sizeOfCacheBeforeFlush)
    cacheFlushes.increment()
  }

  override def getHits: Long = hits.sum()
  override def getMisses: Long = misses.sum()
  override def getCompiled: Long = compiled.sum()
  override def getCompiledWithExpressionCodeGen(): Long = compiledWithExpressionCodeGen.sum()
  override def getDiscards: Long = discards.sum()
  override def getStaleEntries: Long = staleEntries.sum()
  override def getCacheFlushes: Long = cacheFlushes.sum()
  override def getAwaits: Long = awaits.sum()
}

class PreParserCacheMetricsMonitor() extends CacheMetricsMonitor[CypherQueryCaches.PreParserCache.Key] {
  override val monitorTag: String = CypherQueryCaches.PreParserCache.monitorTag
  override val cacheKind: String = CypherQueryCaches.PreParserCache.kind
}

class ASTCacheMetricsMonitor() extends CacheMetricsMonitor[CypherQueryCaches.AstCache.Key] {
  override val monitorTag: String = CypherQueryCaches.AstCache.monitorTag
  override val cacheKind: String = CypherQueryCaches.AstCache.kind
}

class LogicalPlanCacheMetricsMonitor() extends CacheMetricsMonitor[CypherQueryCaches.LogicalPlanCache.Key] {
  override val monitorTag: String = CypherQueryCaches.LogicalPlanCache.monitorTag
  override val cacheKind: String = CypherQueryCaches.LogicalPlanCache.kind
}

class ExecutionPlanCacheMetricsMonitor() extends CacheMetricsMonitor[CypherQueryCaches.ExecutionPlanCache.Key] {
  override val monitorTag: String = CypherQueryCaches.ExecutionPlanCache.monitorTag
  override val cacheKind: String = CypherQueryCaches.ExecutionPlanCache.kind
}

class ExecutableQueryCacheMetricsMonitor() extends CacheMetricsMonitor[CypherQueryCaches.ExecutableQueryCache.Key] {
  override val monitorTag: String = CypherQueryCaches.ExecutableQueryCache.monitorTag
  override val cacheKind: String = CypherQueryCaches.ExecutableQueryCache.kind

  private val counter = new LongAdder
  private val waitTime = new LongAdder

  override def cacheStale(
    queryKey: CacheKey[InputQuery.CacheKey],
    secondsSincePlan: Int,
    metaData: String,
    maybeReason: Option[String]
  ): Unit = {
    super.cacheStale(queryKey, secondsSincePlan, metaData, maybeReason)
    counter.increment()
    waitTime.add(secondsSincePlan)
  }

  def numberOfReplans: Long = counter.sum()

  def replanWaitTime: Long = waitTime.sum()
}
