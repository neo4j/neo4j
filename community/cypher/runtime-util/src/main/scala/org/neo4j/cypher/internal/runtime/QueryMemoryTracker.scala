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

import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.TransactionOutOfMemoryException
import org.neo4j.values.AnyValue
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap
import org.neo4j.cypher.internal.runtime.BoundedMemoryTracker.MemoryTracker

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
  def allocated(instance: WithHeapUsageEstimation, operatorId: Int): Unit

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
  def deallocated(instance: WithHeapUsageEstimation, operatorId: Int): Unit

  /**
    * Returns an Iterator that, given the memory config settings, might throw an exception if the
    * memory used by the query grows too large.
    */
  def memoryTrackingIterator[T<: ExecutionContext](input: Iterator[T], operatorId: Int): Iterator[T]

  /**
    * Get the total allocated memory of this query, in bytes.
    *
    * @return the total number of allocated memory bytes, or None, if memory tracking was not enabled.
    */
  def totalAllocatedMemory: Optional[lang.Long]

  /**
   * Get the maximum allocated memory of this operator, in bytes.
   *
   * @return the maximum number of allocated memory bytes, or None, if memory tracking was not enabled.
   */
  def maxMemoryOfOperator(operatorId: Int): Optional[lang.Long]
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

  override def memoryTrackingIterator[T](input: Iterator[T], operatorId: Int): Iterator[T] = input

  override def allocated(value: AnyValue, operatorId: Int): Unit = {}

  override def allocated(bytes: Long, operatorId: Int): Unit = {}

  override def allocated(instance: WithHeapUsageEstimation, operatorId: Int): Unit = {}

  override def deallocated(bytes: Long, operatorId: Int): Unit = {}

  override def deallocated(value: AnyValue, operatorId: Int): Unit = {}

  override def deallocated(instance: WithHeapUsageEstimation, operatorId: Int): Unit = {}

  override def totalAllocatedMemory: Optional[lang.Long] = Optional.empty()

  override def maxMemoryOfOperator(operatorId: Int): Optional[lang.Long] = Optional.empty()
}

object BoundedMemoryTracker {
  class MemoryTracker private[BoundedMemoryTracker]() {
    protected[this] var _allocatedBytes = 0L
    private var _highWaterMark = 0L

    protected[BoundedMemoryTracker] def allocatedBytes: Long = _allocatedBytes
    protected[BoundedMemoryTracker] def highWaterMark: Long = _highWaterMark

    protected[BoundedMemoryTracker] def allocated(bytes: Long): Unit = {
      _allocatedBytes += bytes
      if (_allocatedBytes > _highWaterMark) {
        _highWaterMark = _allocatedBytes
      }
    }

    protected[BoundedMemoryTracker] def deallocated(bytes: Long): Unit = {
      _allocatedBytes -= bytes
    }
  }
}

class BoundedMemoryTracker(val threshold: Long) extends MemoryTracker with QueryMemoryTracker {
  override val isEnabled: Boolean = true

  private val maxMemoryPerOperator = new IntObjectHashMap[MemoryTracker]()

  private def allocateAndCheckThreshold(bytes: Long): Unit = {
    allocated(bytes)
    if (allocatedBytes > threshold) {
      throw new TransactionOutOfMemoryException
    }
  }

  def allocated(bytes: Long, operatorId: Int): Unit = {
    allocateAndCheckThreshold(bytes)
    maxMemoryPerOperator.getIfAbsentPutWithKey(operatorId, _ => new MemoryTracker()).allocated(bytes)
  }

  override def allocated(value: AnyValue, operatorId: Int): Unit = allocated(value.estimatedHeapUsage(), operatorId)

  override def allocated(instance: WithHeapUsageEstimation, operatorId: Int): Unit = allocated(instance.estimatedHeapUsage, operatorId)

  override def deallocated(bytes: Long, operatorId: Int): Unit = {
    _allocatedBytes -= bytes
    maxMemoryPerOperator.getIfAbsentPutWithKey(operatorId, _ => new MemoryTracker()).deallocated(bytes)
  }

  override def deallocated(bytes: Long): Unit = allocated(-bytes, Id.INVALID_ID.x)

  override def deallocated(value: AnyValue, operatorId: Int): Unit = deallocated(value.estimatedHeapUsage(), operatorId)

  override def deallocated(instance: WithHeapUsageEstimation, operatorId: Int): Unit = deallocated(instance.estimatedHeapUsage, operatorId)

  override def totalAllocatedMemory: Optional[lang.Long] = Optional.of(highWaterMark)

  override def maxMemoryOfOperator(operatorId: Int): Optional[lang.Long] = Optional.ofNullable(maxMemoryPerOperator.get(operatorId)).map(_.highWaterMark)

  override def memoryTrackingIterator[T <: WithHeapUsageEstimation](input: Iterator[T], operatorId: Int): Iterator[T] = new MemoryTrackingIterator[T](input, operatorId)

  private class MemoryTrackingIterator[T <: WithHeapUsageEstimation](input: Iterator[T], operatorId: Int) extends Iterator[T] {
    override def hasNext: Boolean = input.hasNext

    override def next(): T = {
      val t = input.next()
      val rowHeapUsage = t.estimatedHeapUsage
      allocated(rowHeapUsage, operatorId)
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
  def memoryTracking(doProfile: Boolean): MemoryTracking
}

object NO_TRACKING_CONTROLLER extends MemoryTrackingController {
  override def memoryTracking(doProfile: Boolean): MemoryTracking = NO_TRACKING
}
