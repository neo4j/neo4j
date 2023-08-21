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
package org.neo4j.cypher.internal.runtime

import org.neo4j.collection.trackable.HeapTrackingCollections
import org.neo4j.collection.trackable.HeapTrackingLongHashSet
import org.neo4j.cypher.internal.runtime.HeapTrackingLongImmutableSet.CombinedImmutableHashSet.combinedSet
import org.neo4j.cypher.internal.runtime.HeapTrackingLongImmutableSet.HeapTrackingImmutableArraySet.newArraySet
import org.neo4j.cypher.internal.runtime.HeapTrackingLongImmutableSet.HeapTrackingImmutableSet1.newSet1
import org.neo4j.cypher.internal.runtime.HeapTrackingLongImmutableSet.HeapTrackingImmutableSet2.newSet2
import org.neo4j.cypher.internal.runtime.HeapTrackingLongImmutableSet.HeapTrackingImmutableSet3.newSet3
import org.neo4j.cypher.internal.runtime.HeapTrackingLongImmutableSet.HeapTrackingImmutableSet4.newSet4
import org.neo4j.cypher.internal.runtime.HeapTrackingLongImmutableSet.SharedArray.newSharedArray
import org.neo4j.cypher.internal.runtime.HeapTrackingLongImmutableSet.SharedHashSet.copySharedSet
import org.neo4j.cypher.internal.runtime.HeapTrackingLongImmutableSet.SharedHashSet.newSharedSet
import org.neo4j.memory.HeapEstimator.shallowSizeOfInstance
import org.neo4j.memory.HeapEstimator.sizeOfLongArray
import org.neo4j.memory.MemoryTracker

/**
 * A heap tracking variant inspired by scala.collection.immutable.Set[Long]
 * 
 * For smaller sizes, up to size 4, it is an exact replica of what scala.collection.immutable.Set does. For intermediate
 * sizes, [5, MAX_ARRAY_SIZE] the set is backed by and long array and hence operations are linear. For larger sets we combine a 
 * HeapTrackingLongSet and a HeapTrackingLongImmutableSet, when the set grows it grows by adding to the immutable set until we hit 
 * MAX_ARRAY_SIZE. At that point we clone the HeapTrackingLongSet and add all values the the cloned set and start over.
 */
trait HeapTrackingLongImmutableSet {
  def contains(value: Long): Boolean
  def +(other: Long): HeapTrackingLongImmutableSet
  def size: Int
  def close(): Unit

  def addTo(set: HeapTrackingLongHashSet): Unit
}

object HeapTrackingLongImmutableSet {
  private[HeapTrackingLongImmutableSet] val MAX_ARRAY_SIZE = 128

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

  private[HeapTrackingLongImmutableSet] val SHALLOW_SIZE_ARRAY_SET: Long =
    shallowSizeOfInstance(classOf[HeapTrackingImmutableArraySet])

  private[HeapTrackingLongImmutableSet] val SHALLOW_SIZE_COMBINED_SET: Long =
    shallowSizeOfInstance(classOf[CombinedImmutableHashSet])

  private[HeapTrackingLongImmutableSet] val SHALLOW_SIZE_SHARED_ARRAY: Long =
    shallowSizeOfInstance(classOf[SharedArray])

  private[HeapTrackingLongImmutableSet] val SHALLOW_SIZE_SHARED_SET: Long =
    shallowSizeOfInstance(classOf[SharedHashSet])

  private[HeapTrackingLongImmutableSet] val SIZE_OF_LONG_ARRAY: Long =
    sizeOfLongArray(MAX_ARRAY_SIZE)

  def emptySet(memoryTracker: MemoryTracker): HeapTrackingLongImmutableSet = {
    memoryTracker.allocateHeap(SHALLOW_SIZE_0)
    new EmptyHeapTrackingImmutableSet(memoryTracker)
  }

  final private[HeapTrackingLongImmutableSet] class EmptyHeapTrackingImmutableSet private[HeapTrackingLongImmutableSet] (
    memoryTracker: MemoryTracker
  ) extends HeapTrackingLongImmutableSet {
    override def contains(elem: Long): Boolean = false

    override def +(elem: Long): HeapTrackingLongImmutableSet = {
      newSet1(memoryTracker, elem)
    }

    override def close(): Unit = {
      memoryTracker.releaseHeap(SHALLOW_SIZE_0)
    }

    override def size: Int = 0

    override def addTo(set: HeapTrackingLongHashSet): Unit = {}
  }

  final private[HeapTrackingLongImmutableSet] class HeapTrackingImmutableSet1 private[HeapTrackingLongImmutableSet] (
    memoryTracker: MemoryTracker,
    elem1: Long
  ) extends HeapTrackingLongImmutableSet {
    override def contains(elem: Long): Boolean = elem == elem1

    override def +(elem: Long): HeapTrackingLongImmutableSet =
      if (contains(elem)) this
      else newSet2(memoryTracker, elem1, elem)

    override def close(): Unit = {
      memoryTracker.releaseHeap(SHALLOW_SIZE_1)
    }

    override def size: Int = 1

    override def addTo(set: HeapTrackingLongHashSet): Unit = set.add(elem1)
  }

  object HeapTrackingImmutableSet1 {

    def newSet1(memoryTracker: MemoryTracker, elem: Long): HeapTrackingLongImmutableSet = {
      memoryTracker.allocateHeap(SHALLOW_SIZE_1)
      new HeapTrackingImmutableSet1(memoryTracker, elem)
    }
  }

  final private[HeapTrackingLongImmutableSet] class HeapTrackingImmutableSet2 private[HeapTrackingLongImmutableSet] (
    memoryTracker: MemoryTracker,
    elem1: Long,
    elem2: Long
  ) extends HeapTrackingLongImmutableSet {
    override def contains(elem: Long): Boolean = elem == elem1 || elem == elem2

    override def +(elem: Long): HeapTrackingLongImmutableSet =
      if (contains(elem)) this
      else newSet3(memoryTracker, elem1, elem2, elem)

    override def close(): Unit = {
      memoryTracker.releaseHeap(SHALLOW_SIZE_2)
    }

    override def size: Int = 2

    override def addTo(set: HeapTrackingLongHashSet): Unit = set.addAll(elem1, elem2)

  }

  object HeapTrackingImmutableSet2 {

    def newSet2(memoryTracker: MemoryTracker, elem1: Long, elem2: Long): HeapTrackingLongImmutableSet = {
      memoryTracker.allocateHeap(SHALLOW_SIZE_2)
      new HeapTrackingImmutableSet2(memoryTracker, elem1, elem2)
    }
  }

  final private[HeapTrackingLongImmutableSet] class HeapTrackingImmutableSet3 private[HeapTrackingLongImmutableSet] (
    memoryTracker: MemoryTracker,
    elem1: Long,
    elem2: Long,
    elem3: Long
  ) extends HeapTrackingLongImmutableSet {
    override def contains(elem: Long): Boolean = elem == elem1 || elem == elem2 || elem == elem3

    override def +(elem: Long): HeapTrackingLongImmutableSet =
      if (contains(elem)) this
      else newSet4(memoryTracker, elem1, elem2, elem3, elem)

    override def close(): Unit = {
      memoryTracker.releaseHeap(SHALLOW_SIZE_3)
    }

    override def size: Int = 3

    override def addTo(set: HeapTrackingLongHashSet): Unit = set.addAll(elem1, elem2, elem3)
  }

  object HeapTrackingImmutableSet3 {

    def newSet3(memoryTracker: MemoryTracker, elem1: Long, elem2: Long, elem3: Long): HeapTrackingLongImmutableSet = {
      memoryTracker.allocateHeap(SHALLOW_SIZE_3)
      new HeapTrackingImmutableSet3(memoryTracker, elem1, elem2, elem3)
    }
  }

  final private[HeapTrackingLongImmutableSet] class HeapTrackingImmutableSet4 private[HeapTrackingLongImmutableSet] (
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
        val array = newSharedArray(memoryTracker)
        array.set(0, elem1)
        array.set(1, elem2)
        array.set(2, elem3)
        array.set(3, elem4)
        array.set(4, elem)
        newArraySet(memoryTracker, array, 5)
      }

    override def close(): Unit = {
      memoryTracker.releaseHeap(SHALLOW_SIZE_4)
    }

    override def size: Int = 4

    override def addTo(set: HeapTrackingLongHashSet): Unit = set.addAll(elem1, elem2, elem3, elem4)
  }

  object HeapTrackingImmutableSet4 {

    def newSet4(
      memoryTracker: MemoryTracker,
      elem1: Long,
      elem2: Long,
      elem3: Long,
      elem4: Long
    ): HeapTrackingLongImmutableSet = {
      memoryTracker.allocateHeap(SHALLOW_SIZE_4)
      new HeapTrackingImmutableSet4(memoryTracker, elem1, elem2, elem3, elem4)
    }
  }

  final private[HeapTrackingLongImmutableSet] class HeapTrackingImmutableArraySet private[HeapTrackingLongImmutableSet] (
    memoryTracker: MemoryTracker,
    sharedArray: SharedArray,
    val size: Int
  ) extends HeapTrackingLongImmutableSet {

    override def contains(elem: Long): Boolean = {
      var i = 0
      while (i < size) {
        if (sharedArray(i) == elem) {
          return true
        }
        i += 1
      }
      false
    }

    override def +(elem: Long): HeapTrackingLongImmutableSet =
      if (contains(elem)) this
      else if (size < sharedArray.length) {
        val claimedArray = sharedArray.tryClaim(size)
        claimedArray.set(size, elem)
        newArraySet(memoryTracker, claimedArray, size + 1)
      } else {
        combinedSet(memoryTracker, newSharedSet(memoryTracker, this, elem), emptySet(memoryTracker))
      }

    override def close(): Unit = {
      memoryTracker.releaseHeap(SHALLOW_SIZE_ARRAY_SET)
      sharedArray.release()
    }

    override def addTo(set: HeapTrackingLongHashSet): Unit = {
      var i = 0
      while (i < size) {
        set.add(sharedArray(i))
        i += 1
      }
    }

  }

  object HeapTrackingImmutableArraySet {

    def newArraySet(memoryTracker: MemoryTracker, sharedArray: SharedArray, size: Int): HeapTrackingLongImmutableSet = {
      memoryTracker.allocateHeap(SHALLOW_SIZE_ARRAY_SET)
      new HeapTrackingImmutableArraySet(memoryTracker, sharedArray, size)
    }
  }

  final private[HeapTrackingLongImmutableSet] class CombinedImmutableHashSet(
    memoryTracker: MemoryTracker,
    sharedSet: SharedHashSet,
    set2: HeapTrackingLongImmutableSet
  ) extends HeapTrackingLongImmutableSet {
    override def contains(value: Long): Boolean = sharedSet.contains(value) || set2.contains(value)

    override def +(elem: Long): HeapTrackingLongImmutableSet =
      if (contains(elem)) this
      else if (set2.size < MAX_ARRAY_SIZE) {
        combinedSet(memoryTracker, sharedSet.claim(), set2 + elem)
      } else {
        combinedSet(memoryTracker, copySharedSet(memoryTracker, sharedSet, set2, elem), emptySet(memoryTracker))
      }

    override def size: Int = sharedSet.size + set2.size

    override def close(): Unit = {
      memoryTracker.releaseHeap(SHALLOW_SIZE_COMBINED_SET)
      sharedSet.release()
      set2.close()
    }

    override def addTo(set: HeapTrackingLongHashSet): Unit = {
      set.addAll(sharedSet.set)
      set2.addTo(set)
    }
  }

  object CombinedImmutableHashSet {

    def combinedSet(
      memoryTracker: MemoryTracker,
      sharedSet: SharedHashSet,
      appendSet: HeapTrackingLongImmutableSet
    ): HeapTrackingLongImmutableSet = {
      memoryTracker.allocateHeap(SHALLOW_SIZE_COMBINED_SET)
      new CombinedImmutableHashSet(memoryTracker, sharedSet, appendSet)
    }
  }

  final private[HeapTrackingLongImmutableSet] class SharedArray(private[this] val memoryTracker: MemoryTracker) {
    memoryTracker.allocateHeap(SIZE_OF_LONG_ARRAY)
    private val array = new Array[Long](MAX_ARRAY_SIZE)
    private[this] var owners = 1
    private var lastIndex = 0

    def apply(i: Int): Long = array(i)

    def set(i: Int, v: Long): Unit = {
      array(i) = v
      if (i > lastIndex) {
        lastIndex = i
      }
    }

    def length: Int = array.length

    def release(): Unit = {
      owners -= 1
      if (owners == 0) {
        memoryTracker.releaseHeap(SHALLOW_SIZE_SHARED_ARRAY)
        memoryTracker.releaseHeap(SIZE_OF_LONG_ARRAY)
      }
    }

    def tryClaim(size: Int): SharedArray = {
      if (size == lastIndex + 1) {
        owners += 1
        this
      } else if (size < array.length) {
        copy(size)
      } else {
        throw new IllegalStateException
      }
    }

    private[this] def copy(size: Int): SharedArray = {
      val cp = newSharedArray(memoryTracker)
      cp.lastIndex = size - 1
      System.arraycopy(array, 0, cp.array, 0, size)
      cp
    }
  }

  object SharedArray {

    def newSharedArray(memoryTracker: MemoryTracker): SharedArray = {
      memoryTracker.allocateHeap(SHALLOW_SIZE_SHARED_ARRAY)
      new SharedArray(memoryTracker)
    }
  }

  final private[HeapTrackingLongImmutableSet] class SharedHashSet(
    private[this] val memoryTracker: MemoryTracker,
    private[HeapTrackingLongImmutableSet] val set: HeapTrackingLongHashSet
  ) {
    private[this] var owners: Int = 1

    def claim(): SharedHashSet = {
      owners += 1
      this
    }

    def release(): Unit = {
      owners -= 1
      if (owners == 0) {
        set.close()
        memoryTracker.releaseHeap(SHALLOW_SIZE_SHARED_SET)
      }
    }

    def contains(elem: Long): Boolean = set.contains(elem)

    def size: Int = set.size()

    def copyWith(data: HeapTrackingLongImmutableSet, elem: Long): HeapTrackingLongImmutableSet = {
      combinedSet(memoryTracker, newSharedSet(memoryTracker, data, elem), emptySet(memoryTracker))
    }
  }

  object SharedHashSet {

    def newSharedSet(memoryTracker: MemoryTracker, data: HeapTrackingLongImmutableSet, elem: Long): SharedHashSet = {
      memoryTracker.allocateHeap(SHALLOW_SIZE_SHARED_SET)
      val newSet = HeapTrackingCollections.newLongSet(memoryTracker, data.size + 1)
      data.addTo(newSet)
      newSet.add(elem)
      new SharedHashSet(memoryTracker, newSet)
    }

    def copySharedSet(
      memoryTracker: MemoryTracker,
      original: SharedHashSet,
      data: HeapTrackingLongImmutableSet,
      elem: Long
    ): SharedHashSet = {
      memoryTracker.allocateHeap(SHALLOW_SIZE_SHARED_SET)
      val newSet = HeapTrackingCollections.newLongSet(memoryTracker, original.set)
      data.addTo(newSet)
      newSet.add(elem)
      new SharedHashSet(memoryTracker, newSet)
    }
  }
}
