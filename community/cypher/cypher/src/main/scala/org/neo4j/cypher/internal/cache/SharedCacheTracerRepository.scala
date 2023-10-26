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

import org.neo4j.cypher.internal.cache.CypherQueryCaches.PredefinedCacheTracers
import org.neo4j.kernel.impl.query.CacheMetrics

import java.util

import scala.jdk.CollectionConverters.MapHasAsJava

/**
 * To be used with [[SharedExecutorBasedCaffeineCacheFactory]].
 */
class SharedCacheTracerRepository extends CacheTracerRepository {

  private object cacheTracers extends PredefinedCacheTracers

  def metricsPerCacheKind(): util.Map[String, CacheMetrics] = {
    (cacheTracers.perCacheKind: Map[String, CacheMetrics]).asJava
  }

  override def tracerForCacheKind(cacheKind: String): CacheTracer[_] = {
    cacheTracers.perCacheKind.getOrElse(
      cacheKind,
      throw new IllegalArgumentException(s"No tracer for cache kind: $cacheKind")
    )
  }
}
