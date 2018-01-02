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
package org.neo4j.cypher.internal.runtime.vectorized.operators

import java.util.{Comparator, PriorityQueue}

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted.pipes.ColumnOrder
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{LongSlot, SlotConfiguration, RefSlot}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.vectorized._

// This operator takes pre-sorted inputs, and merges them together, producing a stream of Morsels with the sorted data
class MergeSortOperator(orderBy: Seq[ColumnOrder], slots: SlotConfiguration) extends Operator {

  private val comparator: Comparator[MorselWithReadPos] = orderBy
      .map(createComparator)
      .reduce((a,b) => a.thenComparing(b))

  /*
  This operator works by keeping the input morsels ordered by their current element. For each row that will be
  produced, we remove the first morsel and consume the current row. If there is more data left, we re-insert
  the morsel, now pointing to the next row.
   */
  override def operate(message: Message, output: Morsel, context: QueryContext, state: QueryState): Continuation = {

    var iterationState: Iteration = null
    var sortedInputs: PriorityQueue[MorselWithReadPos] = null
    var writePos = 0

    message match {
      case StartLoopWithEagerData(inputs, is) =>
        iterationState = is
        sortedInputs = new PriorityQueue[MorselWithReadPos](inputs.size, comparator)
        inputs.foreach { morsel =>
          if (morsel.validRows > 0) sortedInputs.add(new MorselWithReadPos(morsel, 0))
        }
      case ContinueLoopWith(ContinueWithSource(inputs: PriorityQueue[MorselWithReadPos], is, _)) =>
        sortedInputs = inputs
        iterationState = is
    }
    val longCount = slots.numberOfLongs
    val refCount = slots.numberOfReferences

    while(!sortedInputs.isEmpty && writePos < output.validRows) {
      val next: MorselWithReadPos = sortedInputs.poll()
      val fromLongIdx = next.pos * longCount
      val fromRefIdx = next.pos * refCount
      val toLongIdx = writePos * longCount
      val toRefIdx = writePos * refCount
      System.arraycopy(next.m.longs, fromLongIdx, output.longs, toLongIdx, longCount)
      System.arraycopy(next.m.refs, fromRefIdx, output.refs, toRefIdx, refCount)
      writePos += 1
      next.pos += 1

      // If there is more data in this Morsel, we'll re-insert it into the sortedInputs
      if (next.pos < next.m.validRows) {
        sortedInputs.add(next)
      }
    }

    val next = if (!sortedInputs.isEmpty) {
      ContinueWithSource(sortedInputs, iterationState, needsSameThread = false)
    } else
      EndOfLoop(iterationState)

    output.validRows = writePos
    next
  }

  override def addDependency(pipeline: Pipeline) = Eager(pipeline)

  private def createComparator(order: ColumnOrder): Comparator[MorselWithReadPos] = order.slot match {
    case LongSlot(offset, _, _) =>
      new Comparator[MorselWithReadPos] {
        override def compare(m1: MorselWithReadPos, m2: MorselWithReadPos) = {
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
        override def compare(m1: MorselWithReadPos, m2: MorselWithReadPos) = {
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

class MorselWithReadPos(val m: Morsel, var pos: Int)
