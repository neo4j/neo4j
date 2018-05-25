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

import java.util.{Comparator, PriorityQueue}

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.slotted.pipes.ColumnOrder
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.runtime.vectorized.operators.MorselSorting.MorselWithReadPos
import org.neo4j.values.storable.NumberValue
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}

/**
  * This operator takes pre-sorted inputs, and merges them together, producing a stream of Morsels with the sorted data
  * If countExpression != None, this expression evaluates to a limit for TopN
  */
class MergeSortOperator(orderBy: Seq[ColumnOrder], slots: SlotConfiguration, countExpression: Option[Expression] = None) extends Operator {

  private val comparator: Comparator[MorselWithReadPos] = orderBy
    .map(MorselSorting.createMorselComparator(slots))
    .reduce((a: Comparator[MorselWithReadPos], b: Comparator[MorselWithReadPos]) => a.thenComparing(b))

  /*
  This operator works by keeping the input morsels ordered by their current element. For each row that will be
  produced, we remove the first morsel and consume the current row. If there is more data left, we re-insert
  the morsel, now pointing to the next row.
   */
  override def operate(message: Message, output: Morsel, context: QueryContext, state: QueryState): Continuation = {

    var iterationState: Iteration = null
    var sortedInputs: PriorityQueue[MorselWithReadPos] = null
    var writePos = 0
    var totalPos = 0

    message match {
      case StartLoopWithEagerData(inputs, is) =>
        iterationState = is
        sortedInputs = new PriorityQueue[MorselWithReadPos](inputs.length, comparator)
        inputs.foreach { morsel =>
          if (morsel.validRows > 0) sortedInputs.add(new MorselWithReadPos(morsel, 0))
        }
      case ContinueLoopWith(ContinueWithSource((inputs: PriorityQueue[_], lastTotalPos: Int), is)) =>
        sortedInputs = inputs.asInstanceOf[PriorityQueue[MorselWithReadPos]]
        totalPos = lastTotalPos
        iterationState = is
      case _ => throw new IllegalStateException()

    }
    val longCount = slots.numberOfLongs
    val refCount = slots.numberOfReferences

    // potentially calculate the limit
    val limit = countExpression.map { count =>
      val firstRow = new MorselExecutionContext(sortedInputs.peek().m, longCount, refCount, currentRow = 0)
      val queryState = new OldQueryState(context, resources = null, params = state.params)
      count(firstRow, queryState).asInstanceOf[NumberValue].longValue()
    }
    def limitNotReached = limit.fold(true){l:Long => totalPos < l}

    while(!sortedInputs.isEmpty && writePos < output.validRows && limitNotReached) {
      val nextMorsel: MorselWithReadPos = sortedInputs.poll()
      val fromLongIdx = nextMorsel.pos * longCount
      val fromRefIdx = nextMorsel.pos * refCount
      val toLongIdx = writePos * longCount
      val toRefIdx = writePos * refCount
      System.arraycopy(nextMorsel.m.longs, fromLongIdx, output.longs, toLongIdx, longCount)
      System.arraycopy(nextMorsel.m.refs, fromRefIdx, output.refs, toRefIdx, refCount)
      writePos += 1
      totalPos += 1
      nextMorsel.pos += 1

      // If there is more data in this Morsel, we'll re-insert it into the sortedInputs
      if (nextMorsel.pos < nextMorsel.m.validRows) {
        sortedInputs.add(nextMorsel)
      }
    }

    val continuation = if (!sortedInputs.isEmpty && limitNotReached) {
      ContinueWithSource((sortedInputs, totalPos), iterationState)
    } else
      EndOfLoop(iterationState)

    output.validRows = writePos
    continuation
  }

  override def addDependency(pipeline: Pipeline) = Eager(pipeline)

}

