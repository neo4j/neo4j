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

import org.neo4j.cypher.internal.runtime.BoundedQueryMemoryTracker.MemoryTrackerPerOperator
import org.neo4j.cypher.internal.runtime.BoundedQueryMemoryTracker.OperatorMemoryTracker
import org.neo4j.cypher.internal.runtime.MemoryTrackingController.MemoryTrackerDecorator
import org.neo4j.cypher.result.OperatorProfile
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.Measurable
import org.neo4j.memory.MemoryTracker
import org.neo4j.memory.OptionalMemoryTracker
import org.neo4j.values.AnyValue

trait QueryMemoryTracker {

  /**
   * Returns true if memory tracking is enabled, false otherwise
   */
  def isEnabled: Boolean

  /**
   * Record allocation of bytes
   *
   * @param bytes number of allocated bytes
   */
  def allocated(bytes: Long, operatorId: Int): Unit

  /**
   * Record allocation of value
   *
   * @param value that value which was allocated
   */
  def allocated(value: AnyValue, operatorId: Int) : Unit

  /**
   * Record allocation of instance with heap usage estimation
   *
   * @param instance the allocated instance
   */
  def allocated(instance: Measurable, operatorId: Int): Unit

  /**
   * Record de-allocation of bytes
   *
   * @param bytes number of de-allocated bytes
   */
  def deallocated(bytes: Long, operatorId: Int): Unit

  /**
   * Record de-allocation of value
   *
   * @param value that value which was de-allocated
   */
  def deallocated(value: AnyValue, operatorId: Int): Unit

  /**
   * Record de-allocation of instance with heap usage estimation
   *
   * @param instance the de-allocated instance
   */
  def deallocated(instance: Measurable, operatorId: Int): Unit

  /**
   * Returns an Iterator that, given the memory config settings, might throw an exception if the
   * memory used by the query grows too large.
   */
  def memoryTrackingIterator[T<: CypherRow](input: Iterator[T], operatorId: Int): Iterator[T]

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
      case CUSTOM_MEMORY_TRACKING(decorator: MemoryTrackerDecorator) => BoundedQueryMemoryTracker(decorator(transactionMemoryTracker))
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
  override val isEnabled: Boolean = false

  override def memoryTrackingIterator[T](input: Iterator[T], operatorId: Int): Iterator[T] = input

  override def allocated(value: AnyValue, operatorId: Int): Unit = {}

  override def allocated(bytes: Long, operatorId: Int): Unit = {}

  override def allocated(instance: Measurable, operatorId: Int): Unit = {}

  override def deallocated(bytes: Long, operatorId: Int): Unit = {}

  override def deallocated(value: AnyValue, operatorId: Int): Unit = {}

  override def deallocated(instance: Measurable, operatorId: Int): Unit = {}

  override def totalAllocatedMemory: Long = OptionalMemoryTracker.ALLOCATIONS_NOT_TRACKED

  override def maxMemoryOfOperator(operatorId: Int): Long = OptionalMemoryTracker.ALLOCATIONS_NOT_TRACKED

  override def memoryTrackerForOperator(operatorId: Int): MemoryTracker = EmptyMemoryTracker.INSTANCE
}

object BoundedQueryMemoryTracker {
  def apply(transactionMemoryTracker: MemoryTracker): BoundedQueryMemoryTracker = {
    new BoundedQueryMemoryTracker(transactionMemoryTracker, new MemoryTrackerPerOperator)
  }

  class OperatorMemoryTracker(queryMemoryTracker: MemoryTracker) extends HighWaterScopedMemoryTracker(queryMemoryTracker)

  class MemoryTrackerPerOperator extends GrowingArray[MemoryTracker]

}

class BoundedQueryMemoryTracker(transactionMemoryTracker: MemoryTracker, memoryTrackerPerOperator: MemoryTrackerPerOperator) extends HighWaterScopedMemoryTracker(transactionMemoryTracker)
                                                                                                                             with QueryMemoryTracker {
  override val isEnabled: Boolean = true

  private val newTracker = () => new OperatorMemoryTracker(this)

  def allocated(bytes: Long, operatorId: Int): Unit = {
    memoryTrackerPerOperator.computeIfAbsent(operatorId, newTracker).allocateHeap(bytes)
  }

  override def allocated(value: AnyValue, operatorId: Int): Unit = allocated(value.estimatedHeapUsage(), operatorId)

  override def allocated(instance: Measurable, operatorId: Int): Unit = allocated(instance.estimatedHeapUsage, operatorId)

  override def deallocated(bytes: Long, operatorId: Int): Unit = {
    memoryTrackerPerOperator.computeIfAbsent(operatorId, newTracker).releaseHeap(bytes)
  }

  override def deallocated(value: AnyValue, operatorId: Int): Unit = deallocated(value.estimatedHeapUsage(), operatorId)

  override def deallocated(instance: Measurable, operatorId: Int): Unit = deallocated(instance.estimatedHeapUsage, operatorId)

  override def totalAllocatedMemory: Long = heapHighWaterMark()

  override def maxMemoryOfOperator(operatorId: Int): Long = {
    if (memoryTrackerPerOperator.isDefinedAt(operatorId)) {
      memoryTrackerPerOperator.get(operatorId).heapHighWaterMark()
    } else {
      OptionalMemoryTracker.ALLOCATIONS_NOT_TRACKED
    }
  }

  override def memoryTrackerForOperator(operatorId: Int): MemoryTracker = {
    memoryTrackerPerOperator.computeIfAbsent(operatorId, newTracker)
  }

  override def memoryTrackingIterator[T <: Measurable](input: Iterator[T], operatorId: Int): Iterator[T] = new MemoryTrackingIterator[T](input, operatorId)

  private class MemoryTrackingIterator[T <: Measurable](input: Iterator[T], operatorId: Int) extends Iterator[T] {
    private val operatorMemoryTracker = memoryTrackerForOperator(operatorId)

    override def hasNext: Boolean = input.hasNext

    override def next(): T = {
      val t = input.next()
      val rowHeapUsage = t.estimatedHeapUsage
      operatorMemoryTracker.allocateHeap(rowHeapUsage)
      t
    }
  }
}

/**
 * Logical description of memory tracking behaviour
 */
sealed trait MemoryTracking
case object NO_TRACKING extends MemoryTracking
case object MEMORY_TRACKING extends MemoryTracking
case class CUSTOM_MEMORY_TRACKING(decorator: MemoryTrackerDecorator) extends MemoryTracking

/**
 * Controller of memory tracking. Needed to make memory tracking dynamically configurable.
 */
trait MemoryTrackingController {
  def memoryTracking(doProfile: Boolean): MemoryTracking
}

object MemoryTrackingController {
  type MemoryTrackerDecorator = MemoryTracker => MemoryTracker
}

case class CUSTOM_MEMORY_TRACKING_CONTROLLER(decorator: MemoryTrackerDecorator) extends MemoryTrackingController {
  override def memoryTracking(doProfile: Boolean): MemoryTracking = CUSTOM_MEMORY_TRACKING(decorator)
}
