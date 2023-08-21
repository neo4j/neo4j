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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.runtime.MemoizingMeasurable.INVALID_CACHED_SIZE
import org.neo4j.memory.Measurable

/**
 * This can be used to memoize the estimatedHeapUsage of a Measurable.
 * Users have to take care themselves to clear the cached value when it becomes invalid.
 *
 * Does not influence the behavior of `estimatedHeapUsage()`.
 */
trait MemoizingMeasurable extends Measurable {
  private var cachedEstimatedHeapUsage = INVALID_CACHED_SIZE

  /**
   * Return the estimatedHeapUsage. If this value is cached, read it from the cache.
   * Otherwise, compute it using `estimatedHeapUsage()` and write it to the cache.
   */
  def estimatedHeapUsageWithCache: Long = {
    if (cachedEstimatedHeapUsage == INVALID_CACHED_SIZE) {
      cachedEstimatedHeapUsage = estimatedHeapUsage()
    }
    cachedEstimatedHeapUsage
  }

  def copyCachedEstimatedHeapUsageFrom(other: MemoizingMeasurable): Unit = {
    cachedEstimatedHeapUsage = other.cachedEstimatedHeapUsage
  }

  /**
   * Clear a previously cached estimatedHeapUsage
   */
  def clearCachedEstimatedHeapUsage(): Unit = {
    cachedEstimatedHeapUsage = INVALID_CACHED_SIZE
  }
}

object MemoizingMeasurable {
  private val INVALID_CACHED_SIZE = -1L
}
