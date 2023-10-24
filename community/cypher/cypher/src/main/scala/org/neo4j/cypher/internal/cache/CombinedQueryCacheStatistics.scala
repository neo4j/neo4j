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

import org.neo4j.cypher.internal.util.helpers.MapSupport.PowerMap
import org.neo4j.kernel.impl.query.CacheMetrics
import org.neo4j.kernel.impl.query.QueryCacheStatistics

import java.lang

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala

class CombinedQueryCacheStatistics(a: QueryCacheStatistics, b: QueryCacheStatistics) extends QueryCacheStatistics {
  override def preParserCacheEntries(): lang.Long = a.preParserCacheEntries() + b.preParserCacheEntries()

  override def astCacheEntries(): lang.Long = a.astCacheEntries() + b.astCacheEntries()

  override def logicalPlanCacheEntries(): lang.Long = a.logicalPlanCacheEntries() + b.logicalPlanCacheEntries()

  override def executionPlanCacheEntries(): lang.Long = a.executionPlanCacheEntries + b.executionPlanCacheEntries

  override def executableQueryCacheEntries(): lang.Long =
    a.executableQueryCacheEntries() + b.executableQueryCacheEntries

  override def metricsPerCacheKind(): java.util.Map[String, CacheMetrics] = {
    val aMap = Map.from(a.metricsPerCacheKind().asScala)
    val bMap = Map.from(b.metricsPerCacheKind().asScala)
    aMap.fuse(bMap) {
      case (aMetrics, bMetrics) =>
        new CombinedCacheMetrics(aMetrics, bMetrics)
    }
  }.asJava
}

class CombinedCacheMetrics(a: CacheMetrics, b: CacheMetrics) extends CacheMetrics {
  override def cacheKind(): String = a.cacheKind()

  override def getHits: Long = a.getHits + b.getHits

  override def getMisses: Long = a.getMisses + b.getMisses

  override def getCompiled: Long = a.getCompiled + b.getCompiled

  override def getCompiledWithExpressionCodeGen: Long =
    a.getCompiledWithExpressionCodeGen + b.getCompiledWithExpressionCodeGen

  override def getDiscards: Long = a.getDiscards + b.getDiscards

  override def getStaleEntries: Long = a.getStaleEntries + b.getStaleEntries

  override def getCacheFlushes: Long = a.getCacheFlushes + b.getCacheFlushes
}
