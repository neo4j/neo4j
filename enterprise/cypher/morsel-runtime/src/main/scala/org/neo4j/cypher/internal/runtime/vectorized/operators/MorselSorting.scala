/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import java.util.Comparator

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.{LongSlot, RefSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.slotted.pipes.ColumnOrder
import org.neo4j.cypher.internal.runtime.vectorized.Morsel
import org.neo4j.values.AnyValue

object MorselSorting {

  def compareMorselIndexesByColumnOrder(data: Morsel, slots: SlotConfiguration)(order: ColumnOrder): Comparator[Integer] = order.slot match {
    case LongSlot(offset, _, _) =>
      new Comparator[Integer] {
        override def compare(idx1: Integer, idx2: Integer): Int = {
          val longs = slots.numberOfLongs
          val aIdx = longs * idx1 + offset
          val bIdx = longs * idx2 + offset
          val aVal = data.longs(aIdx)
          val bVal = data.longs(bIdx)
          order.compareLongs(aVal, bVal)
        }
      }

    case RefSlot(offset, _, _) =>
      new Comparator[Integer] {
        override def compare(idx1: Integer, idx2: Integer): Int = {
          val refs = slots.numberOfReferences
          val aIdx = refs * idx1 + offset
          val bIdx = refs * idx2 + offset
          val aVal = data.refs(aIdx)
          val bVal = data.refs(bIdx)
          order.compareValues(aVal, bVal)
        }
      }
  }

  def createMorselIndexesArray(data: Morsel): Array[Integer] = {
    val rows = data.validRows
    val list = new Array[Integer](rows)
    var idx = 0
    while (idx < rows) {
      list(idx) = idx
      idx += 1
    }
    list
  }

  def createSortedMorselData(data: Morsel, arrayToSort: Array[Integer], slots: SlotConfiguration): (Array[Long], Array[AnyValue]) = {
    val longCount = slots.numberOfLongs
    val refCount = slots.numberOfReferences
    val newLongs = new Array[Long](data.validRows * longCount)
    val newRefs = new Array[AnyValue](data.validRows * refCount)

    var toIndex = 0
    while (toIndex < data.validRows) {
      val fromIndex = arrayToSort(toIndex)

      val fromLong = fromIndex * longCount
      val fromRef = fromIndex * refCount
      val toLong = toIndex * longCount
      val toRef = toIndex * refCount

      System.arraycopy(data.longs, fromLong, newLongs, toLong, longCount)
      System.arraycopy(data.refs, fromRef, newRefs, toRef, refCount)

      toIndex += 1
    }

    (newLongs, newRefs)
  }

  class MorselWithReadPos(val m: Morsel, var pos: Int)

  def createMorselComparator(slots: SlotConfiguration)(order: ColumnOrder): Comparator[MorselWithReadPos] = order.slot match {
    case LongSlot(offset, _, _) =>
      new Comparator[MorselWithReadPos] {
        override def compare(m1: MorselWithReadPos, m2: MorselWithReadPos): Int = {
          val longs = slots.numberOfLongs
          val aIdx = longs * m1.pos + offset
          val bIdx = longs * m2.pos + offset
          val aVal = m1.m.longs(aIdx)
          val bVal = m2.m.longs(bIdx)
          order.compareLongs(aVal, bVal)
        }
      }
    case RefSlot(offset, _, _) =>
      new Comparator[MorselWithReadPos] {
        override def compare(m1: MorselWithReadPos, m2: MorselWithReadPos): Int = {
          val refs = slots.numberOfReferences
          val aIdx = refs * m1.pos + offset
          val bIdx = refs * m2.pos + offset
          val aVal = m1.m.refs(aIdx)
          val bVal = m2.m.refs(bIdx)
          order.compareValues(aVal, bVal)
        }
      }

  }

}
