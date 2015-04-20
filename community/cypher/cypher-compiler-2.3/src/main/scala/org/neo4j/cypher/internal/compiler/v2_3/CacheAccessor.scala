/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.internal.LRUCache

trait CacheAccessor[K, T] {
  def getOrElseUpdate(cache: LRUCache[K, T])(key: K, f: => T): T
  def remove(cache: LRUCache[K, T])(key: K)
}

class MonitoringCacheAccessor[K, T](monitor: CypherCacheHitMonitor[K]) extends CacheAccessor[K, T] {

  def getOrElseUpdate(cache: LRUCache[K, T])(key: K, f: => T): T = {
    var updated = false
    val value = cache(key, {
      updated = true
      f
    })

    if (updated)
      monitor.cacheMiss(key)
    else
      monitor.cacheHit(key)

    value
  }

  def remove(cache: LRUCache[K, T])(key: K): Unit = {
    cache.remove(key)
    monitor.cacheDiscard(key)
  }
}
