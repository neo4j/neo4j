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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.config.CUSTOM_MEMORY_TRACKING
import org.neo4j.cypher.internal.config.MEMORY_TRACKING
import org.neo4j.cypher.internal.config.MemoryTracking
import org.neo4j.cypher.internal.config.NO_TRACKING
import org.neo4j.cypher.internal.runtime.BoundedQueryMemoryTracker.MemoryTrackerPerOperator
import org.neo4j.cypher.internal.runtime.BoundedQueryMemoryTracker.OperatorMemoryTracker
import org.neo4j.cypher.result.OperatorProfile
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.MemoryTracker
import org.neo4j.memory.OptionalMemoryTracker

trait QueryMemoryTracker {
  /**
   * Get the total allocated memory of this query, in bytes.
   *
   * @return the total number of allocated memory bytes, or [[OptionalMemoryTracker]].ALLOCATIONS_NOT_TRACKED, if memory tracking was not enabled.
   */
  def totalAllocatedMemory: Long

  /**
   * Get the maximum allocated memory of this operator, in bytes.
   *
   * @return the maximum number of allocated memory bytes, or [[OptionalMemoryTracker]].ALLOCATIONS_NOT_TRACKED, if memory tracking was not enabled.
   */
  def maxMemoryOfOperator(operatorId: Int): Long

  def memoryTrackerForOperator(operatorId: Int): MemoryTracker
}

object QueryMemoryTracker {
  def apply(memoryTracking: MemoryTracking, transactionMemoryTracker: MemoryTracker): QueryMemoryTracker = {
    memoryTracking match {
      case NO_TRACKING => NoOpQueryMemoryTracker
      case MEMORY_TRACKING => BoundedQueryMemoryTracker(transactionMemoryTracker)
      case CUSTOM_MEMORY_TRACKING(decorator) => BoundedQueryMemoryTracker(decorator(transactionMemoryTracker))
    }
  }

  /**
   * Convert a value returned from `totalAllocatedMemory` or `maxMemoryOfOperator` to a value to be given to a QueryProfile.
   */
  def memoryAsProfileData(value: Long): Long = value match {
    case OptionalMemoryTracker.ALLOCATIONS_NOT_TRACKED => OperatorProfile.NO_DATA
    case x => x
  }
}

case object NoOpQueryMemoryTracker extends QueryMemoryTracker {

  override def totalAllocatedMemory: Long = OptionalMemoryTracker.ALLOCATIONS_NOT_TRACKED

  override def maxMemoryOfOperator(operatorId: Int): Long = OptionalMemoryTracker.ALLOCATIONS_NOT_TRACKED

  override def memoryTrackerForOperator(operatorId: Int): MemoryTracker = EmptyMemoryTracker.INSTANCE
}

object BoundedQueryMemoryTracker {
  def apply(transactionMemoryTracker: MemoryTracker): BoundedQueryMemoryTracker = {
    new BoundedQueryMemoryTracker(transactionMemoryTracker)
  }

  class OperatorMemoryTracker(queryMemoryTracker: MemoryTracker) extends HighWaterScopedMemoryTracker(queryMemoryTracker)

  class MemoryTrackerPerOperator(memoryTracker: MemoryTracker) extends GrowingArray[MemoryTracker](memoryTracker)

}

class BoundedQueryMemoryTracker(transactionMemoryTracker: MemoryTracker) extends HighWaterScopedMemoryTracker(transactionMemoryTracker)
                                                                            with QueryMemoryTracker {
  private[this] val memoryTrackerPerOperator: MemoryTrackerPerOperator = new MemoryTrackerPerOperator(this)
  private[this] val newTracker = () => new OperatorMemoryTracker(this)

  override def totalAllocatedMemory: Long = heapHighWaterMark()

  override def maxMemoryOfOperator(operatorId: Int): Long = {
    if (memoryTrackerPerOperator.isDefinedAt(operatorId)) {
      memoryTrackerPerOperator.get(operatorId).heapHighWaterMark
    } else {
      OptionalMemoryTracker.ALLOCATIONS_NOT_TRACKED
    }
  }

  override def memoryTrackerForOperator(operatorId: Int): MemoryTracker = {
    memoryTrackerPerOperator.computeIfAbsent(operatorId, newTracker)
  }
}
