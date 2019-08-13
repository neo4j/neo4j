/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.exceptions.TransactionOutOfMemoryException
import java.lang
import java.util.Optional

trait QueryMemoryTracker {

  /**
    * Record allocation of bytes
    *
    * @param bytes number of allocated bytes
    */
  def allocated(bytes: => Long): Unit

  /**
    * Record de-allocation of bytes
    *
    * @param bytes number of de-allocated bytes
    */
  def deallocated(bytes: => Long): Unit

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
  def apply(transactionMaxMemory: Long): QueryMemoryTracker = {
    if (transactionMaxMemory == 0L) {
      NoMemoryTracker
    } else {
      new BoundedMemoryTracker(transactionMaxMemory)
    }
  }
}

case object NoMemoryTracker extends QueryMemoryTracker {
  override def memoryTrackingIterator[T](input: Iterator[T]): Iterator[T] = input

  override def allocated(bytes: => Long): Unit = {}

  override def deallocated(bytes: => Long): Unit = {}

  override def totalAllocatedMemory: Optional[lang.Long] = Optional.empty()
}

class BoundedMemoryTracker(val threshold: Long) extends QueryMemoryTracker {
  private var allocatedBytes = 0L
  private var highWaterMark = 0L

  override def allocated(bytes: => Long): Unit = {
    allocatedBytes += bytes
    if (allocatedBytes > threshold) {
      throw new TransactionOutOfMemoryException
    }
    if (allocatedBytes > highWaterMark) {
      highWaterMark = allocatedBytes
    }
  }

  override def deallocated(bytes: => Long): Unit = {
    allocatedBytes -= bytes
  }

  override def totalAllocatedMemory: Optional[lang.Long] = Optional.of(highWaterMark)

  override def memoryTrackingIterator[T <: ExecutionContext](input: Iterator[T]): Iterator[T] = new MemoryTrackingIterator2[T](input)

  private class MemoryTrackingIterator2[T <: ExecutionContext](input: Iterator[T]) extends Iterator[T] {
    override def hasNext: Boolean = input.hasNext

    override def next(): T = {
      val t = input.next()
      val rowHeapUsage = t.estimatedHeapUsage
      allocated(rowHeapUsage)
      t
    }
  }
}
