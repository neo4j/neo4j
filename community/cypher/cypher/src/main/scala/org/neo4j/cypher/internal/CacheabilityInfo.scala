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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.util.InternalNotification

/**
 * This trait is just there to give the ability to hold caching information
 */
trait CacheabilityInfo {

  /**
   * This field indicates if a query should be cached in the query caches or not
   * (e.g. "EXPLAIN query with not enough given parameters" will be executable but useless in the cache ).
   * The reason for this is that we don't want to pollute the caches with entries that will never be used
   */
  def shouldBeCached: Boolean

  def notifications: IndexedSeq[InternalNotification]
}
