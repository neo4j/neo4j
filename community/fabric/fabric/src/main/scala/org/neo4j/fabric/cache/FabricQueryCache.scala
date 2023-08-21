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
package org.neo4j.fabric.cache

import org.neo4j.cypher.internal.InputQuery
import org.neo4j.cypher.internal.QueryCache
import org.neo4j.cypher.internal.QueryCache.ParameterTypeMap
import org.neo4j.cypher.internal.cache.CacheSize
import org.neo4j.cypher.internal.cache.CaffeineCacheFactory
import org.neo4j.cypher.internal.cache.LFUCache
import org.neo4j.fabric.planning.FabricPlan
import org.neo4j.values.virtual.MapValue

import scala.jdk.CollectionConverters.MapHasAsScala

class FabricQueryCache(cacheFactory: CaffeineCacheFactory, size: CacheSize) {

  type QueryKey = InputQuery.CacheKey
  type Params = MapValue
  type ParamTypes = ParameterTypeMap
  type ContextName = String

  type Key = (QueryKey, ParamTypes, ContextName)
  type Value = FabricPlan

  private val cache = new LFUCache[Key, Value](cacheFactory, size)

  private var hits: Long = 0
  private var misses: Long = 0

  def computeIfAbsent(
    query: QueryKey,
    params: Params,
    defaultContextName: ContextName,
    compute: () => FabricPlan,
    shouldCache: FabricPlan => Boolean,
    useParameterSizeHint: Boolean
  ): FabricPlan = {
    val paramTypes = QueryCache.extractParameterTypeMap(params, useParameterSizeHint)
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

  def getInnerCopy: Map[Key, Value] =
    cache.asMap().asScala.toMap

  // mutable implementation for resource efficiency
  def clearByContext(contextName: ContextName): Long = {
    var clearedCount = 0
    cache.asMap().forEach((key, _) =>
      if (key._3 == contextName) {
        cache.invalidate(key)
        clearedCount += 1
      }
    )
    clearedCount
  }
}
