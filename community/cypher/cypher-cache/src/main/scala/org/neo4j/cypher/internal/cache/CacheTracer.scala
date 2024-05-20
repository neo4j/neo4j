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

/**
 * Tracer for cache activity.
 * Default implementations do nothing.
 */
trait CacheTracer[KEY] {

  /**
   * The item was found in the cache and was not stale.
   */
  def cacheHit(key: KEY, metaData: String): Unit = ()

  /**
   * The item was not found in the cache.
   */
  def cacheMiss(key: KEY, metaData: String): Unit = ()

  /**
   * A value was computed. This is either a new value or replacing a value for a stale item.
   */
  def compute(key: KEY, metaData: String): Unit = ()

  /**
   * A value was removed from the cache.
   */
  def discard(key: KEY, metaData: String): Unit = ()

  /**
   * Some compute was invoked to compute a key to a value, requesting expression code generation.
   */
  def computeWithExpressionCodeGen(key: KEY, metaData: String): Unit = ()

  /**
   * The item was found in the cache but has become stale.
   * @param secondsSinceCompute how long since the last compute.
   * @param maybeReason maybe a reason clarifying why the item was stale.
   */
  def cacheStale(key: KEY, secondsSinceCompute: Int, metaData: String, maybeReason: Option[String]): Unit =
    ()

  /**
   * The query cache was flushed.
   */
  def cacheFlush(sizeOfCacheBeforeFlush: Long): Unit = ()

  /**
   * When testing for the presence of a key, some other Thread was currently computing the value.
   * This Thread was therefore paused until that computation was done.
   */
  def awaitOngoingComputation(key: KEY, metaData: String): Unit = ()
}
