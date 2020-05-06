/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.fabric.cache

import org.neo4j.cypher.internal.QueryCache
import org.neo4j.cypher.internal.QueryCache.ParameterTypeMap
import org.neo4j.cypher.internal.cache.LFUCache
import org.neo4j.fabric.planning.FabricPlan
import org.neo4j.values.virtual.MapValue

class FabricQueryCache(size: Int) {

  type Query = String
  type Params = MapValue
  type ParamTypes = ParameterTypeMap
  type DefaultContextName = String

  type Key = (Query, ParamTypes, DefaultContextName)
  type Value = FabricPlan

  private val cache = new LFUCache[Key, Value](size)

  private var hits: Long = 0
  private var misses: Long = 0

  def computeIfAbsent(query: Query, params: Params, defaultContextName: DefaultContextName,
                      compute: () => FabricPlan,
                      shouldCache: FabricPlan => Boolean
                     ): FabricPlan = {
    val paramTypes = QueryCache.extractParameterTypeMap(params)
    val key = (query, paramTypes, defaultContextName)
    cache.get(key) match {
      case None =>
        val result = compute()
        if (shouldCache(result))
          cache.put(key, result)

        misses += 1
        result

      case Some(result) =>
        hits += 1
        result
    }
  }

  def getHits: Long = hits

  def getMisses: Long = misses
}
