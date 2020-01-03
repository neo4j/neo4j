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
package org.neo4j.cypher.internal.compatibility.v3_5.runtime.profiler

import org.neo4j.cypher.result.{OperatorProfile, QueryProfile}
import org.neo4j.cypher.internal.v3_5.util.attribution.Id

import scala.collection.mutable

class InterpretedProfileInformation extends QueryProfile {

  case class OperatorData(override val dbHits: Long,
                          override val rows: Long,
                          override val pageCacheHits: Long,
                          override val pageCacheMisses: Long) extends OperatorProfile {

    override def time: Long = OperatorProfile.NO_DATA
  }

  val pageCacheMap: mutable.Map[Id, PageCacheStats] = mutable.Map.empty
  val dbHitsMap: mutable.Map[Id, ProfilingPipeQueryContext] = mutable.Map.empty
  val rowMap: mutable.Map[Id, ProfilingIterator] = mutable.Map.empty

  def operatorProfile(operatorId: Int): OperatorProfile = {
    val id = Id(operatorId)
    val rows = rowMap.get(id).map(_.count).getOrElse(0L)
    val dbHits = dbHitsMap.get(id).map(_.count).getOrElse(0L)
    val pageCacheStats = pageCacheMap.getOrElse(id, PageCacheStats(0L, 0L))

    OperatorData(dbHits, rows, pageCacheStats.hits, pageCacheStats.misses)
  }
}

case class PageCacheStats(hits: Long, misses: Long)


