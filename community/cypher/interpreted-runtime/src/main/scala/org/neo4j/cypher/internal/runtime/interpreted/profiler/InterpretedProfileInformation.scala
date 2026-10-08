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
import org.neo4j.cypher.result.ConcurrentTransactionsProfile
import org.neo4j.cypher.result.OperatorProfile
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.kernel.api.query.QueryTransactionStatisticsAggregator.CommitPhaseStatisticsListener

import java.util.Optional

import scala.collection.mutable

class InterpretedProfileInformation extends QueryProfile {

  case class OperatorData(
    dbHits: Long,
    rows: Long,
    pageCacheHits: Long,
    pageCacheMisses: Long,
    maxAllocatedMemory: Long,
    indexes: Map[IndexDescriptor, Int]
  ) extends OperatorProfile {

    override def time: Long = OperatorProfile.NO_DATA

    override def indexesUsed(): Array[IndexDescriptor] = indexes.keys.toArray.sortBy(_.getName)

    override def indexUseCount(): Array[Int] = indexes.toArray.sortBy(_._1.getName).map(_._2)

    override def hashCode: Int = OperatorProfile.hashCode(this)

    override def equals(o: Any): Boolean = OperatorProfile.equals(this, o)

    override def toString: String = OperatorProfile.toString(this)
  }

  val pageCacheMap: mutable.Map[Id, PageCacheStats] = mutable.Map.empty.withDefault(_ => PageCacheStats(0, 0))
  val dbHitsMap: mutable.Map[Id, Counter] = mutable.Map.empty
  val indexSeeksMap: mutable.Map[Id, mutable.Map[IndexDescriptor, Int]] = mutable.Map.empty
  val rowMap: mutable.Map[Id, ProfilingIterator] = mutable.Map.empty

  // Per-operator CONCURRENT TRANSACTIONS runtime-concurrency snapshots, written once per operator on
  // the main thread at operator close (after all workers have finished), keyed by operator id.
  private[this] val concurrentTransactionsMap: mutable.Map[Id, ConcurrentTransactionsProfile] = mutable.Map.empty

  // Intended to be overridden by `setQueryMemoryTracker`
  private var memoryTracker: QueryMemoryTracker = NoOpQueryMemoryTracker

  def setQueryMemoryTracker(memoryTracker: QueryMemoryTracker): Unit = this.memoryTracker = memoryTracker

  def operatorProfile(operatorId: Int): OperatorProfile = {
    val id = Id(operatorId)
    val rows = rowMap.get(id).map(_.count).getOrElse(0L)
    val dbHits = dbHitsMap.get(id).map(_.count).getOrElse(0L)
    val indexSeeks = indexSeeksMap.get(id).map(_.toMap).getOrElse(Map.empty)
    val pageCacheStats = pageCacheMap(id)
    val maxMemoryAllocated =
      MemoryTrackerForOperatorProvider.memoryAsProfileData(memoryTracker.heapHighWaterMarkOfOperator(operatorId))

    OperatorData(dbHits, rows, pageCacheStats.hits, pageCacheStats.misses, maxMemoryAllocated, indexSeeks)
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

  override def concurrentTransactionsProfile(operatorId: Int): Optional[ConcurrentTransactionsProfile] =
    concurrentTransactionsMap.get(Id(operatorId)) match {
      case Some(profile) => Optional.of(profile)
      case None          => Optional.empty()
    }

  def recordConcurrentTransactions(id: Id, profile: ConcurrentTransactionsProfile): Unit =
    if (profile != null) {
      concurrentTransactionsMap(id) = profile
    }

  override def maxAllocatedMemory(): Long =
    MemoryTrackerForOperatorProvider.memoryAsProfileData(memoryTracker.heapHighWaterMark)

  override def numberOfAvailableWorkers(): Int = OperatorProfile.NO_DATA.toInt

  override def numberOfAvailableProcessors: Int = OperatorProfile.NO_DATA.toInt

  def merge(other: InterpretedProfileInformation): Unit = {
    other.rowMap.foreach { case (id, otherIterator) =>
      rowMap.getOrElseUpdate(id, ProfilingIterator.empty).increment(otherIterator.count)
    }
    other.dbHitsMap.foreach { case (id, otherCounter) =>
      dbHitsMap.getOrElseUpdate(id, Counter()).increment(otherCounter.count)
    }
    other.pageCacheMap.foreach { case (id, otherPageCacheStats) =>
      pageCacheMap(id) += otherPageCacheStats
    }
  }

  def commitPhaseStatisticsListenerFor(id: Id): CommitPhaseStatisticsListener = {
    (pageHits: Long, pageFaults: Long, _: Long) =>
      {
        pageCacheMap(id) += PageCacheStats(pageHits, pageFaults)
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
