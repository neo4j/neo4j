/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.Comparator

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted.pipes.ColumnOrder
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{LongSlot, PipelineInformation, RefSlot}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.vectorized._

/*
Responsible for sorting the Morsel in place, which will then be merged together with other sorted Morsels
 */
class ParallelSortOperator(orderBy: Seq[ColumnOrder], pipelineInformation: PipelineInformation) extends Operator {

  override def operate(message: Message, data: Morsel, context: QueryContext, state: QueryState): Continuation = {

    var iterationState: Iteration = null
    var sortedValues: Array[MorselWithReadPos] = null
    var readPos = 0

    message match {
      case StartLoopWithEagerData(inputs: Seq[Morsel], is) =>
        iterationState = is
        sortedValues = parallelSort(inputs.toIndexedSeq)
      case ContinueLoopWith(ContinueWithSource((inputs: Array[MorselWithReadPos], pos: Int), i, _)) =>
        sortedValues = inputs
        readPos = pos
        iterationState = i
    }

    var writePos = 0
    val longCount = pipelineInformation.numberOfLongs
    val refCount = pipelineInformation.numberOfReferences
    while (readPos < sortedValues.length && writePos < data.validRows) {
      val value = sortedValues(readPos)
      val fromLongIdx = value.pos * longCount
      val fromRefIdx = value.pos * refCount
      val toLongIdx = writePos * longCount
      val toRefIdx = writePos * refCount
      System.arraycopy(value.m.longs, fromLongIdx, data.longs, toLongIdx, longCount)
      System.arraycopy(value.m.refs, fromRefIdx, data.refs, toRefIdx, refCount)
      writePos += 1
      readPos += 1
    }

    if (readPos < sortedValues.length)
      ContinueWithSource((sortedValues, readPos), iterationState, needsSameThread = false)
    else
      EndOfLoop(iterationState)
  }

  private def parallelSort(inputs: IndexedSeq[Morsel]): Array[MorselWithReadPos] = {
    val length = inputs.length
    val sizes = new Array[Int](length)
    var total = 0
    for (i <- 0 until length) {
      val thisLength = inputs(i).validRows
      total += thisLength
      sizes(i) = thisLength
    }

    /*
    To do the sorting as efficiently as possible, we'll use the parallelSort capabilities of the built in Java
    library. We don't want to copy all the data out to rows, so instead we'll create a comparator that can take
    references into Morsels at specific rows
     */
    val sortedValues = createLookupTable(total, inputs)

    val comparator: Comparator[MorselWithReadPos] = orderBy
      .map(createComparator(sortedValues))
      .reduce((a, b) => a.thenComparing(b))

    java.util.Arrays.parallelSort(sortedValues, comparator)

    sortedValues
  }

  private def createComparator(data: Array[MorselWithReadPos])(order: ColumnOrder): Comparator[MorselWithReadPos] = order.slot match {
    case LongSlot(offset, _, _) =>
      new Comparator[MorselWithReadPos] {
        override def compare(a: MorselWithReadPos, b: MorselWithReadPos): Int = {
          val longs = pipelineInformation.numberOfLongs
          val aIdx = longs * a.pos + offset
          val bIdx = longs * b.pos + offset
          val aVal = a.m.longs(aIdx)
          val bVal = b.m.longs(bIdx)
          order.compareLongs(aVal, bVal)
        }
      }

    case RefSlot(offset, _, _) =>
      new Comparator[MorselWithReadPos] {
        override def compare(a: MorselWithReadPos, b: MorselWithReadPos): Int = {
          val refs = pipelineInformation.numberOfReferences
          val aIdx = refs * a.pos + offset
          val bIdx = refs * b.pos + offset
          val aVal = a.m.refs(aIdx)
          val bVal = b.m.refs(bIdx)
          order.compareValues(aVal, bVal)
        }
      }
  }

  private def createLookupTable(total: Int, inputs: IndexedSeq[Morsel]): Array[MorselWithReadPos] = {
    val result = new Array[MorselWithReadPos](total)

    var writePos = 0

    for (current <- inputs) {
      for (offset <- 0 until current.validRows) {
        result(writePos + offset) = new MorselWithReadPos(current, offset)
      }
      writePos += current.validRows
    }

    result
  }

  override def addDependency(pipeline: Pipeline): Dependency = Eager(pipeline)
}
