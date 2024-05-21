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
package org.neo4j.cypher.internal.runtime.interpreted.profiler

import org.neo4j.cypher.internal.runtime.memory.MemoryTrackerForOperatorProvider
import org.neo4j.cypher.internal.runtime.memory.NoOpQueryMemoryTracker
import org.neo4j.cypher.internal.runtime.memory.QueryMemoryTracker
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.result.OperatorProfile
import org.neo4j.cypher.result.QueryProfile

import java.util

import scala.collection.mutable

class InterpretedProfileInformation extends QueryProfile {

  case class OperatorData(
    dbHits: Long,
    rows: Long,
    pageCacheHits: Long,
    pageCacheMisses: Long,
    maxAllocatedMemory: Long
  ) extends OperatorProfile {

    override def time: Long = OperatorProfile.NO_DATA

    override def hashCode: Int = util.Arrays.hashCode(
      Array(this.time(), this.dbHits, this.rows, this.pageCacheHits, this.pageCacheMisses, this.maxAllocatedMemory)
    )

    override def equals(o: Any): Boolean = o match {
      case that: OperatorProfile =>
        this.time == that.time &&
        this.dbHits == that.dbHits &&
        this.rows == that.rows &&
        this.pageCacheHits == that.pageCacheHits &&
        this.pageCacheMisses == that.pageCacheMisses &&
        this.maxAllocatedMemory == that.maxAllocatedMemory()
      case _ => false
    }

    override def toString: String =
      s"Operator Profile { time: ${this.time}, dbHits: ${this.dbHits}, rows: ${this.rows}, page cache hits: ${this.pageCacheHits}, page cache misses: ${this.pageCacheMisses}, max allocated: ${this.maxAllocatedMemory} }"
  }

  val pageCacheMap: mutable.Map[Id, PageCacheStats] = mutable.Map.empty.withDefault(_ => PageCacheStats(0, 0))
  val dbHitsMap: mutable.Map[Id, Counter] = mutable.Map.empty
  val rowMap: mutable.Map[Id, ProfilingIterator] = mutable.Map.empty

  // Intended to be overridden by `setQueryMemoryTracker`
  private var memoryTracker: QueryMemoryTracker = NoOpQueryMemoryTracker

  def setQueryMemoryTracker(memoryTracker: QueryMemoryTracker): Unit = this.memoryTracker = memoryTracker

  def operatorProfile(operatorId: Int): OperatorProfile = {
    val id = Id(operatorId)
    val rows = rowMap.get(id).map(_.count).getOrElse(0L)
    val dbHits = dbHitsMap.get(id).map(_.count).getOrElse(0L)
    val pageCacheStats = pageCacheMap(id)
    val maxMemoryAllocated =
      MemoryTrackerForOperatorProvider.memoryAsProfileData(memoryTracker.heapHighWaterMarkOfOperator(operatorId))

    OperatorData(dbHits, rows, pageCacheStats.hits, pageCacheStats.misses, maxMemoryAllocated)
  }

  def snapshot: InterpretedProfileInformationSnapshot = {
    val currentDbHitsMap: collection.Map[Id, Long] = dbHitsMap.map { case (k, v) => (k, v.count) }.withDefaultValue(0)
    val currentRowsMap: collection.Map[Id, Long] = rowMap.map { case (k, v) => (k, v.count) }.withDefaultValue(0)
    InterpretedProfileInformationSnapshot(currentDbHitsMap, currentRowsMap)
  }

  def aggregatedSnapshot: InterpretedProfileInformationAggregatedSnapshot = {
    val aggregatedDbHits = dbHitsMap.values.map(_.count).sum
    InterpretedProfileInformationAggregatedSnapshot(aggregatedDbHits)
  }

  override def maxAllocatedMemory(): Long =
    MemoryTrackerForOperatorProvider.memoryAsProfileData(memoryTracker.heapHighWaterMark)

  def merge(other: InterpretedProfileInformation): Unit = {
    other.rowMap.foreach { case (id, otherIterator) =>
      rowMap.getOrElse(id, ProfilingIterator.empty).increment(otherIterator.count)
    }
    other.dbHitsMap.foreach { case (id, otherCounter) =>
      dbHitsMap.getOrElse(id, Counter()).increment(otherCounter.count)
    }
    other.pageCacheMap.foreach { case (id, otherPageCacheStats) =>
      pageCacheMap.getOrElse(id, PageCacheStats(0, 0)) + otherPageCacheStats
    }
  }
}

case class InterpretedProfileInformationSnapshot(dbHitsMap: collection.Map[Id, Long], rowsMap: collection.Map[Id, Long])

case class InterpretedProfileInformationAggregatedSnapshot(dbHits: Long)

case class PageCacheStats(hits: Long, misses: Long) {

  def -(other: PageCacheStats): PageCacheStats = {
    PageCacheStats(this.hits - other.hits, this.misses - other.misses)
  }

  def +(other: PageCacheStats): PageCacheStats = {
    PageCacheStats(this.hits + other.hits, this.misses + other.misses)
  }
}
