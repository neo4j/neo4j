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
package org.neo4j.cypher.internal.profiling

import org.neo4j.cypher.result.OperatorProfile

/**
 * Expose various query execution kernel statistics
 */
trait KernelStatisticProvider {
  /**
   * @return observed page cache hits that was caused by particular query execution
   */
  def getPageCacheHits: Long

  /**
   * @return observer page cache misses that was caused by particular query execution
   */
  def getPageCacheMisses: Long
}

object NoKernelStatisticProvider extends KernelStatisticProvider {
  override def getPageCacheHits: Long = OperatorProfile.NO_DATA

  override def getPageCacheMisses: Long = OperatorProfile.NO_DATA
}
