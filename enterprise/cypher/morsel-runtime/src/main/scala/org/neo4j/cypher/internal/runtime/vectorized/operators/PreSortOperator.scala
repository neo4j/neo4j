/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{LongSlot, RefSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.slotted.pipes.ColumnOrder
import org.neo4j.cypher.internal.runtime.vectorized.{Iteration, MiddleOperator, Morsel, QueryState}
import org.neo4j.values.AnyValue

/*
Responsible for sorting the Morsel in place, which will then be merged together with other sorted Morsels
 */
class PreSortOperator(orderBy: Seq[ColumnOrder], slots: SlotConfiguration) extends MiddleOperator {

  override def operate(iterationState: Iteration, data: Morsel, context: QueryContext, state: QueryState): Unit = {

    val comparator: Comparator[Object] = orderBy
      .map(createComparator(data))
      .reduce((a, b) => a.thenComparing(b))

    // First we create an array of the same size as the rows in the morsel that we'll sort.
    // This array contains only the pointers to the morsel rows
    val arrayToSort: Array[Object] = createArray(data)
    java.util.Arrays.sort(arrayToSort, comparator)

    // Now that we have a sorted array, we need to shuffle the morsel rows around until they follow the same order
    // as the sorted array
    // TODO: Do this without creating extra arrays
    val (newLongs, newRefs) = createSortedMorselData(data, arrayToSort)

    // Copy the now sorted arrays into the Morsel
    System.arraycopy(newLongs, 0, data.longs, 0, newLongs.length)
    System.arraycopy(newRefs, 0, data.refs, 0, newRefs.length)
  }

  private def createSortedMorselData(data: Morsel, arrayToSort: Array[Object]): (Array[Long], Array[AnyValue]) = {
    val longCount = slots.numberOfLongs
    val refCount = slots.numberOfReferences
    val newLongs = new Array[Long](data.validRows * longCount)
    val newRefs = new Array[AnyValue](data.validRows * refCount)

    var idx = 0
    while (idx < data.validRows) {
      val to = arrayToSort(idx).asInstanceOf[Int]

      val fromLong = to * longCount
      val fromRef = to * refCount
      val toLong = idx * longCount
      val toRef = idx * refCount

      System.arraycopy(data.longs, fromLong, newLongs, toLong, longCount)
      System.arraycopy(data.refs, fromRef, newRefs, toRef, refCount)

      idx += 1
    }

    (newLongs, newRefs)
  }

  private def createArray(data: Morsel) = {
    val rows = data.validRows
    val list = new Array[Object](rows)
    var idx = 0
    while (idx < rows) {
      list(idx) = idx.asInstanceOf[Object]
      idx += 1
    }
    list
  }

  private def createComparator(data: Morsel)(order: ColumnOrder): Comparator[Object] = order.slot match {
    case LongSlot(offset, _, _) =>
      new Comparator[Object] {
        override def compare(idx1: Object, idx2: Object) = {
          val longs = slots.numberOfLongs
          val aIdx = longs * idx1.asInstanceOf[Int] + offset
          val bIdx = longs * idx2.asInstanceOf[Int] + offset
          val aVal = data.longs(aIdx)
          val bVal = data.longs(bIdx)
          order.compareLongs(aVal, bVal)
        }
      }

    case RefSlot(offset, _, _) =>
      new Comparator[Object] {
        override def compare(idx1: Object, idx2: Object) = {
          val refs = slots.numberOfReferences
          val aIdx = refs * idx1.asInstanceOf[Int] + offset
          val bIdx = refs * idx2.asInstanceOf[Int] + offset
          val aVal = data.refs(aIdx)
          val bVal = data.refs(bIdx)
          order.compareValues(aVal, bVal)
        }
      }
  }
}
