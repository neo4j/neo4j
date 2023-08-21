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
package org.neo4j.cypher.internal.runtime.memory

import org.neo4j.memory.HeapMemoryTracker

/**
 * A [[HeapMemoryTracker]] that keeps its own highWaterMark  for heap memory
 * It also forwards all allocate and deallocate calls to a delegate.
 */
class DelegatingScopedHeapMemoryTracker(delegate: HeapMemoryTracker) extends HeapMemoryTracker {

  /**
   * The current size of the tracked heap in this class. May be less than the size of the delegate.
   */
  private var allocatedBytesHeap = 0L

  /**
   * The heap high water mark, i.e. the maximum observed allocated heap bytes.
   */
  private var _heapHighWaterMark = 0L

  override def allocateHeap(bytes: Long): Unit = {
    delegate.allocateHeap(bytes)
    allocatedBytesHeap += bytes
    if (allocatedBytesHeap > heapHighWaterMark) {
      _heapHighWaterMark = allocatedBytesHeap
    }
  }

  override def releaseHeap(bytes: Long): Unit = {
    delegate.releaseHeap(bytes)
    allocatedBytesHeap -= bytes
  }

  override def heapHighWaterMark: Long = _heapHighWaterMark
}
