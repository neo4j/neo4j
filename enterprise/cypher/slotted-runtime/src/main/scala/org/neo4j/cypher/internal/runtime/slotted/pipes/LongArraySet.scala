/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import java.lang.Integer.highestOneBit
import java.util

class LongArraySet(capacity: Int = 32, longsPerEntry: Int) {

  val NO_ID = 0xF000000000000000L

  assert((capacity & (capacity - 1)) == 0, "Size must be a power of 2")

  private var table = new Table(capacity)

  def contains(value: Array[Long]): Boolean = {
    var key = offsetFor(value)
    var result = 0
    do {
      result = table.checkSlot(key, value)
      key = (key + 1) & table.tableMask
    } while (result == -1)
    result == 1
  }

  private def resize(): Unit = {
    val oldSize = table.capacity
    val oldTable = table
    table = new Table(oldSize * 2)
    // Instead of using a lot of small arrays, we use this one for most everything
    val currentValue = new Array[Long](longsPerEntry)

    var i = 0
    while (i < oldSize) {
      val fromIdx = i * longsPerEntry
      if (oldTable.inner(fromIdx) != NO_ID) {
        System.arraycopy(oldTable.inner, fromIdx, currentValue, 0, longsPerEntry)
        var slotOffset = offsetFor(currentValue)
        var statusForSlot = table.checkSlot(slotOffset, currentValue)

        while (statusForSlot != 0) {
          slotOffset = (slotOffset + 1) & table.tableMask
          statusForSlot = table.checkSlot(slotOffset, currentValue)
        }

        table.addValueToSet(currentValue, slotOffset)
      }
      i += 1
    }
  }

  def add(value: Array[Long]): Unit = {
    assert(value.size == longsPerEntry)
    var offset = offsetFor(value)

    while (true) {
      val i = table.checkSlot(offset, value)
      i match {
        case 1 =>
          // Set already contains value - do nothing
          return

        case 0 if table.timeToResize =>
          // We know that the value does not yet exist in the set, but there is not space for it
          resize()
          // Need to re-start the linear probing after resizing
          offset = offsetFor(value)

        case 0 =>
          table.addValueToSet(value, offset)
          return

        case _ =>
          // Spot already taken. Continue linear probe looking for an empty spot
          offset = (offset + 1) & table.tableMask
      }
    }
  }

  private def offsetFor(value: Array[Long]) = {
    util.Arrays.hashCode(value) & table.tableMask
  }

  class Table(val capacity: Int) {
    private val resizeLimit = (capacity * 0.75).toInt

    val tableMask: Int = highestOneBit(this.capacity) - 1

    val inner: Array[Long] = new Array[Long](capacity * longsPerEntry)
    java.util.Arrays.fill(inner, NO_ID)

    private var numberOfEntries: Int = 0

    def timeToResize: Boolean = numberOfEntries >= resizeLimit

    /**
      * Return values mean:
      * 0   Slot is empty
      * 1   Slot contains value
      * -1  Slot occupied by something else
      */
    def checkSlot(offset: Int, value: Array[Long]): Int = {

      val startOffset = offset * longsPerEntry

      if (inner(startOffset) == NO_ID)
        return 0
      else {
        var i = 0
        while (i < longsPerEntry) {
          if (inner(startOffset + i) != value(i)) return -1
          i += 1
        }
      }

      1
    }

    def addValueToSet(value: Array[Long], offset: Int): Unit = {
      val startOffset = offset * longsPerEntry
      System.arraycopy(value, 0, inner, startOffset, longsPerEntry)

      numberOfEntries += 1
    }

  }


}

