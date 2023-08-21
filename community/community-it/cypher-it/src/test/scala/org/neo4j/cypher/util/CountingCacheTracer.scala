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

import org.neo4j.cypher.internal.cache.CacheTracer
import org.neo4j.cypher.util.CountingCacheTracer.CacheCounts

import java.util.concurrent.atomic.AtomicLong

class CountingCacheTracer[Key] extends CacheTracer[Key] {
  protected val hits = new AtomicLong
  protected val misses = new AtomicLong
  protected val flushes = new AtomicLong
  protected val evicted = new AtomicLong
  protected val discards = new AtomicLong
  protected val compilations = new AtomicLong
  protected val compilationsWithExpressionCodeGen = new AtomicLong

  def counts: CacheCounts = CacheCounts(
    hits.get,
    misses.get,
    flushes.get,
    evicted.get,
    discards.get,
    compilations.get,
    compilationsWithExpressionCodeGen.get
  )

  override def cacheHit(queryKey: Key, metaData: String): Unit =
    hits.incrementAndGet()

  override def cacheMiss(queryKey: Key, metaData: String): Unit =
    misses.incrementAndGet()

  override def compute(queryKey: Key, metaData: String): Unit =
    compilations.incrementAndGet()

  override def discard(key: Key, metaData: String): Unit =
    discards.incrementAndGet()

  override def computeWithExpressionCodeGen(queryKey: Key, metaData: String): Unit =
    compilationsWithExpressionCodeGen.incrementAndGet()

  override def cacheStale(
    queryKey: Key,
    secondsSincePlan: Int,
    metaData: String,
    maybeReason: Option[String]
  ): Unit =
    evicted.incrementAndGet()

  override def cacheFlush(sizeOfCacheBeforeFlush: Long): Unit =
    flushes.incrementAndGet()
}

object CountingCacheTracer {

  case class CacheCounts(
    hits: Long = 0,
    misses: Long = 0,
    flushes: Long = 0,
    evicted: Long = 0,
    discards: Long = 0,
    compilations: Long = 0,
    compilationsWithExpressionCodeGen: Long = 0
  ) {

    override def toString =
      s"hits = $hits, misses = $misses, flushes = $flushes, evicted = $evicted, discards = $discards, compilations = $compilations, compilationsWithExpressionCodeGen = $compilationsWithExpressionCodeGen"
  }
}
