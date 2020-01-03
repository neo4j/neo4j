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

import java.lang
import java.util.Optional

import org.neo4j.exceptions.TransactionOutOfMemoryException
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
  def allocated(bytes: Long): Unit

  /**
    * Record allocation of value
    *
    * @param value that value which was allocated
    */
  def allocated(value: AnyValue): Unit

  /**
    * Record allocation of instance with heap usage estimation
    *
    * @param instance the allocated instance
    */
  def allocated(instance: WithHeapUsageEstimation): Unit

  /**
    * Record de-allocation of bytes
    *
    * @param bytes number of de-allocated bytes
    */
  def deallocated(bytes: Long): Unit

  /**
    * Record de-allocation of value
    *
    * @param value that value which was de-allocated
    */
  def deallocated(value: AnyValue): Unit

  /**
    * Record de-allocation of instance with heap usage estimation
    *
    * @param instance the de-allocated instance
    */
  def deallocated(instance: WithHeapUsageEstimation): Unit

  /**
    * Returns an Iterator that, given the memory config settings, might throw an exception if the
    * memory used by the query grows too large.
    */
  def memoryTrackingIterator[T<: ExecutionContext](input: Iterator[T]): Iterator[T]

  /**
    * Get the total allocated memory of this query, in bytes.
    *
    * @return the total number of allocated memory bytes, or None, if memory tracking was not enabled.
    */
  def totalAllocatedMemory: Optional[lang.Long]
}

object QueryMemoryTracker {
  def apply(memoryTracking: MemoryTracking): QueryMemoryTracker = {
    memoryTracking match {
      case NO_TRACKING => NoMemoryTracker
      case MEMORY_TRACKING => new BoundedMemoryTracker(Long.MaxValue)
      case MEMORY_BOUND(maxAllocatedBytes) => new BoundedMemoryTracker(maxAllocatedBytes)
    }
  }
}

case object NoMemoryTracker extends QueryMemoryTracker {
  override val isEnabled: Boolean = false

  override def memoryTrackingIterator[T](input: Iterator[T]): Iterator[T] = input

  override def allocated(bytes: Long): Unit = {}

  override def allocated(value: AnyValue): Unit = {}

  override def allocated(instance: WithHeapUsageEstimation): Unit = {}

  override def deallocated(bytes: Long): Unit = {}

  override def deallocated(value: AnyValue): Unit = {}

  override def deallocated(instance: WithHeapUsageEstimation): Unit = {}

  override def totalAllocatedMemory: Optional[lang.Long] = Optional.empty()
}

class BoundedMemoryTracker(val threshold: Long) extends QueryMemoryTracker {
  private var allocatedBytes = 0L
  private var highWaterMark = 0L

  override val isEnabled: Boolean = true

  override def allocated(bytes: Long): Unit = {
    allocatedBytes += bytes
    if (allocatedBytes > threshold) {
      throw new TransactionOutOfMemoryException
    }
    if (allocatedBytes > highWaterMark) {
      highWaterMark = allocatedBytes
    }
  }

  override def allocated(value: AnyValue): Unit = allocated(value.estimatedHeapUsage())

  override def allocated(instance: WithHeapUsageEstimation): Unit = allocated(instance.estimatedHeapUsage)

  override def deallocated(bytes: Long): Unit = {
    allocatedBytes -= bytes
  }

  override def deallocated(value: AnyValue): Unit = deallocated(value.estimatedHeapUsage())

  override def deallocated(instance: WithHeapUsageEstimation): Unit = deallocated(instance.estimatedHeapUsage)

  override def totalAllocatedMemory: Optional[lang.Long] = Optional.of(highWaterMark)

  override def memoryTrackingIterator[T <: ExecutionContext](input: Iterator[T]): Iterator[T] = new MemoryTrackingIterator[T](input)

  private class MemoryTrackingIterator[T <: ExecutionContext](input: Iterator[T]) extends Iterator[T] {
    override def hasNext: Boolean = input.hasNext

    override def next(): T = {
      val t = input.next()
      val rowHeapUsage = t.estimatedHeapUsage
      allocated(rowHeapUsage)
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
case class MEMORY_BOUND(maxAllocatedBytes: Long) extends MemoryTracking

/**
  * Controller of memory tracking. Needed to make memory tracking dynamically configurable.
  */
trait MemoryTrackingController {
  def memoryTracking: MemoryTracking
}

object NO_TRACKING_CONTROLLER extends MemoryTrackingController {
  override def memoryTracking: MemoryTracking = NO_TRACKING
}
