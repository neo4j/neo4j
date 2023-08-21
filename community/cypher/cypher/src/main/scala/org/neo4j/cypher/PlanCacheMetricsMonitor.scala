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

import java.util.concurrent.atomic.AtomicLong

abstract class CacheMetricsMonitor[KEY] extends CacheTracer[KEY] {
  val monitorTag: String
  val cacheKind: String

  private val hits = new AtomicLong
  private val misses = new AtomicLong
  private val compiled = new AtomicLong
  private val discards = new AtomicLong
  private val staleEntries = new AtomicLong
  private val cacheFlushes = new AtomicLong
  override def cacheHit(key: KEY, metaData: String): Unit = hits.incrementAndGet()

  override def cacheMiss(key: KEY, metaData: String): Unit = misses.incrementAndGet()

  override def compute(key: KEY, metaData: String): Unit = compiled.incrementAndGet()

  override def discard(key: KEY, metaData: String): Unit = discards.incrementAndGet()

  override def computeWithExpressionCodeGen(key: KEY, metaData: String): Unit = compiled.incrementAndGet()

  override def cacheStale(key: KEY, secondsSinceCompute: Int, metaData: String, maybeReason: Option[String]): Unit =
    staleEntries.incrementAndGet()

  override def cacheFlush(sizeOfCacheBeforeFlush: Long): Unit = {
    discards.addAndGet(sizeOfCacheBeforeFlush)
    cacheFlushes.incrementAndGet()
  }

  def getHits: Long = hits.get()
  def getMisses: Long = misses.get()
  def getCompiled: Long = compiled.get()
  def getDiscards: Long = discards.get()
  def getStaleEntries: Long = staleEntries.get()
  def getCacheFlushes: Long = cacheFlushes.get()
}

class PreParserCacheMetricsMonitor(extraTag: String) extends CacheMetricsMonitor[CypherQueryCaches.PreParserCache.Key] {
  override val monitorTag: String = CypherQueryCaches.PreParserCache.monitorTag + extraTag
  override val cacheKind: String = CypherQueryCaches.PreParserCache.kind
}

class ASTCacheMetricsMonitor(extraTag: String) extends CacheMetricsMonitor[CypherQueryCaches.AstCache.Key] {
  override val monitorTag: String = CypherQueryCaches.AstCache.monitorTag + extraTag
  override val cacheKind: String = CypherQueryCaches.AstCache.kind
}

class LogicalPlanCacheMetricsMonitor(extraTag: String)
    extends CacheMetricsMonitor[CypherQueryCaches.LogicalPlanCache.Key] {
  override val monitorTag: String = CypherQueryCaches.LogicalPlanCache.monitorTag + extraTag
  override val cacheKind: String = CypherQueryCaches.LogicalPlanCache.kind
}

class ExecutionPlanCacheMetricsMonitor(extraTag: String)
    extends CacheMetricsMonitor[CypherQueryCaches.ExecutionPlanCache.Key] {
  override val monitorTag: String = CypherQueryCaches.ExecutionPlanCache.monitorTag + extraTag
  override val cacheKind: String = CypherQueryCaches.ExecutionPlanCache.kind
}

class ExecutableQueryCacheMetricsMonitor(extraTag: String)
    extends CacheMetricsMonitor[CypherQueryCaches.ExecutableQueryCache.Key] {
  override val monitorTag: String = CypherQueryCaches.ExecutableQueryCache.monitorTag + extraTag
  override val cacheKind: String = CypherQueryCaches.ExecutableQueryCache.kind

  private val counter = new AtomicLong()
  private val waitTime = new AtomicLong()

  override def cacheStale(
    queryKey: CacheKey[InputQuery.CacheKey],
    secondsSincePlan: Int,
    metaData: String,
    maybeReason: Option[String]
  ): Unit = {
    super.cacheStale(queryKey, secondsSincePlan, metaData, maybeReason)
    counter.incrementAndGet()
    waitTime.addAndGet(secondsSincePlan)
  }

  def numberOfReplans: Long = counter.get()

  def replanWaitTime: Long = waitTime.get()
}
