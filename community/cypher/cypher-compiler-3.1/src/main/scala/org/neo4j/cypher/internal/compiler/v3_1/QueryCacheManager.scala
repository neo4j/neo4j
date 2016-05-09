/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1

/*
Manages the query cache, removing stale queries from it
 */
class QueryCacheManager[K, T](cacheAccessor: CacheAccessor[K, T], cache: LRUCache[K, T]) {
  def getOrElseUpdate(key: K, userKey: String, isStale: T => Boolean, produce: => T): (T, Boolean) = {
    if (cache.size == 0)
      (produce, false)
    else {
      var planned = false
      var isStalePlan = false
      var x: T = null.asInstanceOf[T]

      do {
        x = cacheAccessor.getOrElseUpdate(cache)(key, {
          planned = true
          produce
        })

        isStalePlan = !planned && isStale(x)
        if (isStalePlan) {
          cacheAccessor.remove(cache)(key, userKey)
        }
      } while (isStalePlan)

      (x, planned)
    }
  }
}
