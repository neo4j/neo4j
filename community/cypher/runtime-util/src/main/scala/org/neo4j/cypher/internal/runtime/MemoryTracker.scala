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

import org.neo4j.cypher.internal.v4_0.util.TransactionOutOfMemoryException

trait MemoryTracker {
  /**
    * Returns an Iterator that, given the memory config settings, might throw an exception if the
    * memory used by the query grows too large.
    */
  def memoryTrackingIterator[T<: ExecutionContext](input: Iterator[T]): Iterator[T]

  /**
    * Given the size of a collection, this method can throw an Exception if the size exceeds the configured
    * limit for the query.
    */
  def checkMemoryRequirement(estimatedHeapUsage: => Long): Unit
}

object MemoryTracker {
  def apply(transactionMaxMemory: Long): MemoryTracker = {
    if (transactionMaxMemory == 0L) {
      NoMemoryTracker
    } else {
      StandardMemoryTracker(transactionMaxMemory)
    }
  }
}

case object NoMemoryTracker extends MemoryTracker {
  override def memoryTrackingIterator[T](input: Iterator[T]): Iterator[T] = input

  override def checkMemoryRequirement(size: => Long): Unit = {}
}

case class StandardMemoryTracker(transactionMaxMemory: Long) extends MemoryTracker {
  override def memoryTrackingIterator[T<: ExecutionContext](input: Iterator[T]): Iterator[T] = new MemoryTrackingIterator[T](input, transactionMaxMemory)

  override def checkMemoryRequirement(size: => Long): Unit = {
    if (size >= transactionMaxMemory) {
      throw new TransactionOutOfMemoryException()
    }
  }

  /**
    * Iterator that throws a [[TransactionOutOfMemoryException]] when the input uses more memory than transactionMaxMemory allows.
    */
  private class MemoryTrackingIterator[T <: ExecutionContext](input: Iterator[T], transactionMaxMemory: Long) extends Iterator[T] {
    private var heapUsage = 0L

    override def hasNext: Boolean = input.hasNext

    override def next(): T = {
      val t = input.next()
      heapUsage += t.estimatedHeapUsage
      if (heapUsage > transactionMaxMemory) {
        throw new TransactionOutOfMemoryException()
      }
      t
    }
  }
}
