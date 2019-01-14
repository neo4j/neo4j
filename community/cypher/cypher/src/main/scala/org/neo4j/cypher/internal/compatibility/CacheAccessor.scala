/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility

import org.neo4j.cypher.internal.compiler.v3_4.{CacheCheckResult, FineToReuse, NeedsReplan}

trait CacheAccessor[K <: AnyRef, T <: AnyRef] {
  def getOrElseUpdate(cache: LFUCache[K, T])(key: K, f: => T): T
  def put(cache: LFUCache[K, T])(key: K, value: T, userKey: String, secondsSinceReplan: Int): T
}

trait PlanProducer[T] {
  def produceWithExistingTX: T
}

class QueryCache[K <: AnyRef, T <: AnyRef](cacheAccessor: CacheAccessor[K, T], cache: LFUCache[K, T]) {
  def getOrElseUpdate(key: K, userKey: String, checkPlanStillValid: T => CacheCheckResult, produce: PlanProducer[T]): (T, Boolean) = {
    if (cache.size == 0)
      (produce.produceWithExistingTX, false)
    else {
      var planned = false
      val plan: T = cacheAccessor.getOrElseUpdate(cache)(key, {
        planned = true
        produce.produceWithExistingTX
      })

      if (planned)
        (plan, true)
      else {
        // We found a matching plan in the cache. let's make sure it's OK to use again.
        checkPlanStillValid(plan) match {
          case NeedsReplan(secondsSinceReplan) =>
            val newPlan = produce.produceWithExistingTX
            cacheAccessor.put(cache)(key, newPlan, userKey, secondsSinceReplan)
            (newPlan, true)
          case FineToReuse =>
            (plan, false)
        }

      }
    }
  }
}

class MonitoringCacheAccessor[K <: AnyRef, T <: AnyRef](monitor: CypherCacheHitMonitor[K]) extends CacheAccessor[K, T] {

  override def getOrElseUpdate(cache: LFUCache[K, T])(key: K, f: => T): T = {
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

  override def put(cache: LFUCache[K, T])(key: K, value: T, userKey: String, secondsSinceReplan: Int): T = {
    cache.put(key, value)
    monitor.cacheDiscard(key, userKey, secondsSinceReplan)
    value
  }
}
