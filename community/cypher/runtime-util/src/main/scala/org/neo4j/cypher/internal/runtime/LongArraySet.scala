/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.lang.Integer.highestOneBit
import java.util

import org.neo4j.cypher.internal.runtime.LongArraySet._

/**
  * When you need to have a set of arrays of longs representing entities - look no further
  *
  * This set will keep all it's state in a single long[] array, marking unused slots
  * using 0xF000000000000000L, a value that should never be used for node or relationship id's.
  *
  * @param capacity The initial capacity for the set. Must be a power of 2
  * @param longsPerEntry All arrays in the set must be of this length
  */
class LongArraySet(capacity: Int = 32, longsPerEntry: Int) {

  assert((capacity & (capacity - 1)) == 0, "Size must be a power of 2")
  assert(longsPerEntry > 0, "Number of elements must be larger than 0")

  private var table = new Table(capacity)

  /***
    * Returns true if the value is in the set.
    * @param value The value to check for
    * @return whether the value is in the set or not.
    */
  def contains(value: Array[Long]): Boolean = {
    var key = offsetFor(value)
    var result = 0
    do {
      result = table.checkSlot(key, value)
      key = (key + 1) & table.tableMask
    } while (result == CONTINUE_PROBING)
    result == VALUE_FOUND
  }

  /**
    * Adds a value to the set.
    * @param value The new value to be added to the set
    * @return The method returns true if the value was added and false if it already existed in the set.
    */
  def add(value: Array[Long]): Boolean = {
    assert(value.length == longsPerEntry)
    var offset = offsetFor(value)

    while (true) {
      table.checkSlot(offset, value) match {
        case VALUE_FOUND =>
          // Set already contains value - do nothing
          return false

        case SLOT_EMPTY if table.timeToResize =>
          // We know that the value does not yet exist in the set, but there is not space for it
          resize()
          // Need to re-start the linear probing after resizing
          offset = offsetFor(value)

        case SLOT_EMPTY =>
          // The value does not yet exist in the set, and here is a free spot
          table.addValueToSet(value, offset)
          return true

        case CONTINUE_PROBING =>
          // Spot already taken. Continue linear probe looking for an empty spot
          offset = (offset + 1) & table.tableMask
      }
    }

    throw new RuntimeException("This will never be reached. Just here to stop the compiler from complaining.")
  }

  private def resize(): Unit = {
    val oldSize = table.capacity
    val oldTable = table
    table = new Table(oldSize * 2)

    // Creating the key array outside of the copy loop allows us to reuse the same array for all elements
    val currentValue = new Array[Long](longsPerEntry)

    var i = 0
    while (i < oldSize) {
      val fromIdx = i * longsPerEntry
      if (oldTable.inner(fromIdx) != NOT_IN_USE) {
        // Copy over the longs from the source to the key array
        System.arraycopy(oldTable.inner, fromIdx, currentValue, 0, longsPerEntry)
        var slotOffset = offsetFor(currentValue)
        var statusForSlot = table.checkSlot(slotOffset, currentValue)

        // Linear probe until we find an unused slot. No need to check for size here - we are already inside of resize()
        while (statusForSlot != SLOT_EMPTY) {
          slotOffset = (slotOffset + 1) & table.tableMask
          statusForSlot = table.checkSlot(slotOffset, currentValue)
        }

        table.addValueToSet(currentValue, slotOffset)
      }
      i += 1
    }
  }

  private def offsetFor(value: Array[Long]): Int =
    util.Arrays.hashCode(value) & table.tableMask

  private class Table(val capacity: Int) {
    private var numberOfEntries: Int = 0
    private val resizeLimit = (capacity * 0.75).toInt


    val tableMask: Int = highestOneBit(this.capacity) - 1
    val inner: Array[Long] = new Array[Long](capacity * longsPerEntry)

    java.util.Arrays.fill(inner, NOT_IN_USE)

    def timeToResize: Boolean = numberOfEntries >= resizeLimit

    def checkSlot(offset: Int, value: Array[Long]): Int = {
      val startOffset = offset * longsPerEntry

      if (inner(startOffset) == NOT_IN_USE)
        return SLOT_EMPTY
      else {
        var i = 0
        while (i < longsPerEntry) {
          if (inner(startOffset + i) != value(i)) return CONTINUE_PROBING
          i += 1
        }
      }

      VALUE_FOUND
    }

    def addValueToSet(value: Array[Long], offset: Int): Unit = {
      val startOffset = offset * longsPerEntry
      System.arraycopy(value, 0, inner, startOffset, longsPerEntry)
      numberOfEntries += 1
    }
  }
}

object LongArraySet {
  // Constants
  val NOT_IN_USE: Long = 0xF000000000000000L
  val SLOT_EMPTY: Int = 0
  val VALUE_FOUND: Int = 1
  val CONTINUE_PROBING: Int = -1
}