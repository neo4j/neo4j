/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.collection.trackable.HeapTrackingCollections
import org.neo4j.collection.trackable.HeapTrackingLongHashSet
import org.neo4j.memory.HeapEstimator.shallowSizeOfInstance
import org.neo4j.memory.MemoryTracker

/**
 * A heap tracking variant of scala.collection.immutable.Set[Long]
 */
trait HeapTrackingLongImmutableSet {
  def contains(value: Long): Boolean
  def +(other: Long): HeapTrackingLongImmutableSet
  def size: Int
  def close(): Unit
}

object HeapTrackingLongImmutableSet {

  private[HeapTrackingLongImmutableSet] val SHALLOW_SIZE_0: Long =
    shallowSizeOfInstance(classOf[EmptyHeapTrackingImmutableSet])

  private[HeapTrackingLongImmutableSet] val SHALLOW_SIZE_1: Long =
    shallowSizeOfInstance(classOf[HeapTrackingImmutableSet1])

  private[HeapTrackingLongImmutableSet] val SHALLOW_SIZE_2: Long =
    shallowSizeOfInstance(classOf[HeapTrackingImmutableSet2])

  private[HeapTrackingLongImmutableSet] val SHALLOW_SIZE_3: Long =
    shallowSizeOfInstance(classOf[HeapTrackingImmutableSet3])

  private[HeapTrackingLongImmutableSet] val SHALLOW_SIZE_4: Long =
    shallowSizeOfInstance(classOf[HeapTrackingImmutableSet4])

  private[HeapTrackingLongImmutableSet] val SHALLOW_SIZE_SET: Long =
    shallowSizeOfInstance(classOf[HeapTrackingImmutableHashSet])

  def emptySet(memoryTracker: MemoryTracker): HeapTrackingLongImmutableSet = {
    memoryTracker.allocateHeap(SHALLOW_SIZE_0)
    new EmptyHeapTrackingImmutableSet(memoryTracker)
  }

  final private class EmptyHeapTrackingImmutableSet private[HeapTrackingLongImmutableSet] (memoryTracker: MemoryTracker)
      extends HeapTrackingLongImmutableSet {
    override def contains(elem: Long): Boolean = false

    override def +(elem: Long): HeapTrackingLongImmutableSet = {
      memoryTracker.allocateHeap(SHALLOW_SIZE_1)
      new HeapTrackingImmutableSet1(memoryTracker, elem)
    }

    override def close(): Unit = {
      memoryTracker.releaseHeap(SHALLOW_SIZE_0)
    }

    override def size: Int = 0
  }

  final class HeapTrackingImmutableSet1 private[HeapTrackingLongImmutableSet] (
    memoryTracker: MemoryTracker,
    elem1: Long
  ) extends HeapTrackingLongImmutableSet {
    override def contains(elem: Long): Boolean = elem == elem1

    override def +(elem: Long): HeapTrackingLongImmutableSet =
      if (contains(elem)) this
      else {
        memoryTracker.allocateHeap(SHALLOW_SIZE_2)
        new HeapTrackingImmutableSet2(memoryTracker, elem1, elem)
      }

    override def close(): Unit = {
      memoryTracker.releaseHeap(SHALLOW_SIZE_1)
    }

    override def size: Int = 1
  }

  final class HeapTrackingImmutableSet2 private[HeapTrackingLongImmutableSet] (
    memoryTracker: MemoryTracker,
    elem1: Long,
    elem2: Long
  ) extends HeapTrackingLongImmutableSet {
    override def contains(elem: Long): Boolean = elem == elem1 || elem == elem2

    override def +(elem: Long): HeapTrackingLongImmutableSet =
      if (contains(elem)) this
      else {
        memoryTracker.allocateHeap(SHALLOW_SIZE_3)
        new HeapTrackingImmutableSet3(memoryTracker, elem1, elem2, elem)
      }

    override def close(): Unit = {
      memoryTracker.releaseHeap(SHALLOW_SIZE_2)
    }

    override def size: Int = 2
  }

  final class HeapTrackingImmutableSet3 private[HeapTrackingLongImmutableSet] (
    memoryTracker: MemoryTracker,
    elem1: Long,
    elem2: Long,
    elem3: Long
  ) extends HeapTrackingLongImmutableSet {
    override def contains(elem: Long): Boolean = elem == elem1 || elem == elem2 || elem == elem3

    override def +(elem: Long): HeapTrackingLongImmutableSet =
      if (contains(elem)) this
      else {
        memoryTracker.allocateHeap(SHALLOW_SIZE_4)
        new HeapTrackingImmutableSet4(memoryTracker, elem1, elem2, elem3, elem)
      }

    override def close(): Unit = {
      memoryTracker.releaseHeap(SHALLOW_SIZE_3)
    }

    override def size: Int = 3
  }

  final class HeapTrackingImmutableSet4 private[HeapTrackingLongImmutableSet] (
    memoryTracker: MemoryTracker,
    elem1: Long,
    elem2: Long,
    elem3: Long,
    elem4: Long
  ) extends HeapTrackingLongImmutableSet {
    override def contains(elem: Long): Boolean = elem == elem1 || elem == elem2 || elem == elem3 || elem == elem4

    override def +(elem: Long): HeapTrackingLongImmutableSet =
      if (contains(elem)) this
      else {
        memoryTracker.allocateHeap(SHALLOW_SIZE_SET)
        val set = HeapTrackingCollections.newLongSet(memoryTracker)
        set.addAll(elem1, elem2, elem3, elem4, elem)
        new HeapTrackingImmutableHashSet(memoryTracker, set)
      }

    override def close(): Unit = {
      memoryTracker.releaseHeap(SHALLOW_SIZE_4)
    }

    override def size: Int = 4
  }

  final class HeapTrackingImmutableHashSet private[HeapTrackingLongImmutableSet] (
    memoryTracker: MemoryTracker,
    set: HeapTrackingLongHashSet
  ) extends HeapTrackingLongImmutableSet {
    override def contains(elem: Long): Boolean = set.contains(elem)

    override def +(elem: Long): HeapTrackingLongImmutableSet =
      if (contains(elem)) this
      else {
        memoryTracker.allocateHeap(SHALLOW_SIZE_SET)
        val newSet = HeapTrackingCollections.newLongSet(memoryTracker, set)
        newSet.add(elem)
        new HeapTrackingImmutableHashSet(memoryTracker, newSet)
      }

    override def close(): Unit = {
      memoryTracker.releaseHeap(SHALLOW_SIZE_SET)
      set.close()
    }

    override def size: Int = {
      set.size()
    }
  }

}
