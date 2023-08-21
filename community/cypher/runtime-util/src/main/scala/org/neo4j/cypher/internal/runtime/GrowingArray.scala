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

import org.neo4j.cypher.internal.runtime.GrowingArray.DEFAULT_SIZE
import org.neo4j.cypher.internal.runtime.GrowingArray.SHALLOW_SIZE
import org.neo4j.memory.HeapEstimator.shallowSizeOfInstance
import org.neo4j.memory.HeapEstimator.shallowSizeOfObjectArray
import org.neo4j.memory.HeapMemoryTracker

/**
 * Random access data structure which grows dynamically as elements are added.
 */
class GrowingArray[T <: AnyRef](memoryTracker: HeapMemoryTracker) extends AutoCloseable {

  private var trackedMemory = shallowSizeOfObjectArray(DEFAULT_SIZE)
  memoryTracker.allocateHeap(trackedMemory + SHALLOW_SIZE)
  private var highWaterMark: Int = 0
  private var array: Array[AnyRef] = new Array[AnyRef](DEFAULT_SIZE)

  /**
   * Set an element at a given index, and grows the underlying structure if needed.
   */
  def set(index: Int, t: T): Unit = {
    ensureCapacity(index + 1)
    array(index) = t
  }

  /**
   * Get the element at a given index.
   */
  def get(index: Int): T = {
    array(index).asInstanceOf[T]
  }

  /**
   * Get the element at a given index or null if it doesn't exist.
   */
  def getOrNull(index: Int): T = {
    if (index < highWaterMark) {
      array(index).asInstanceOf[T]
    } else {
      null.asInstanceOf[T]
    }
  }

  /**
   * Get the element at a given index. If the element at that index is `null`,
   * instead compute a new element, set it at the index, and return it.
   *
   * This is useful for storing resources that can be reused depending on their index.
   */
  def computeIfAbsent(index: Int, compute: () => T): T = {
    ensureCapacity(index + 1)
    var t = array(index)
    if (t == null) {
      t = compute()
      array(index) = t
    }
    t.asInstanceOf[T]
  }

  /**
   * Apply the given function `f` once for each element.
   *
   * `f` will not be called for gaps or `null` elements.
   */
  def foreach(f: T => Unit): Unit = {
    var i = 0
    while (i < highWaterMark) {
      val t = get(i)
      if (t != null) {
        f(t)
      }
      i += 1
    }
  }

  /**
   * Return `true` if any element has ever been set.
   */
  def hasNeverSeenData: Boolean = highWaterMark == 0

  /**
   * Return `true` if array contains a non-null element at the given index.
   */
  def isDefinedAt(index: Int): Boolean = array.isDefinedAt(index) && array(index) != null

  override def close(): Unit = {
    if (array != null) {
      memoryTracker.releaseHeap(trackedMemory + SHALLOW_SIZE)
      array = null
    }
  }

  def isClosed(): Boolean = {
    array == null
  }

  private def ensureCapacity(size: Int): Unit = {
    if (this.highWaterMark < size) {
      this.highWaterMark = size
    }

    if (array.length < size) {
      val temp = array
      val oldHeapUsage = trackedMemory
      val newLength = math.max(array.length * 2, size)
      trackedMemory = shallowSizeOfObjectArray(newLength)
      memoryTracker.allocateHeap(trackedMemory)
      array = new Array[AnyRef](newLength)
      System.arraycopy(temp, 0, array, 0, temp.length)
      memoryTracker.releaseHeap(oldHeapUsage)
    }
  }
}

object GrowingArray {
  val DEFAULT_SIZE: Int = 4
  val SHALLOW_SIZE: Long = shallowSizeOfInstance(classOf[GrowingArray[_]])
}
