/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.ListSupport
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.values.AnyValue

class UnwindOperator(collection: Expression,
                     offset: Int,
                     fromSlots: SlotConfiguration,
                     toSlots: SlotConfiguration)
  extends Operator with ListSupport {

  override def operate(source: Message,
                       output: Morsel,
                       context: QueryContext,
                       state: QueryState): Continuation = {
    var readPos = 0
    var unwoundValues: java.util.Iterator[AnyValue] = null
    var input: Morsel = null
    var iterationState: Iteration = null
    var currentRow: MorselExecutionContext = null

    val inputLongCount = fromSlots.numberOfLongs
    val inputRefCount = fromSlots.numberOfReferences
    val outputLongCount = toSlots.numberOfLongs
    val outputRefCount = toSlots.numberOfReferences
    val queryState = new OldQueryState(context, resources = null, params = state.params)

    source match {
      case StartLoopWithSingleMorsel(data, is) =>
        input = data
        iterationState = is
        currentRow = new MorselExecutionContext(data, inputLongCount, inputRefCount, currentRow = readPos)
        val value = collection(currentRow, queryState)
        unwoundValues = makeTraversable(value).iterator

      case ContinueLoopWith(ContinueWithDataAndSource(data, index, CurrentState(inValues), is)) =>
        input = data
        readPos = index
        iterationState = is
        unwoundValues = inValues
        currentRow = new MorselExecutionContext(data, inputLongCount, inputRefCount, currentRow = readPos)
      case _ => throw new IllegalStateException()
    }

    var writePos = 0

    do {
      if (unwoundValues == null) {
        currentRow.currentRow = readPos
        val value = collection(currentRow, queryState)
        unwoundValues = makeTraversable(value).iterator
      }

      while (unwoundValues.hasNext && writePos < output.validRows) {
        val thisValue = unwoundValues.next()
        System.arraycopy(input.longs, readPos * inputLongCount, output.longs, writePos * outputLongCount, inputLongCount)
        System.arraycopy(input.refs, readPos * inputRefCount, output.refs, writePos * outputRefCount, inputRefCount)
        output.refs(writePos * outputRefCount + offset) = thisValue
        writePos += 1
      }

      if (!unwoundValues.hasNext) {
        readPos += 1
        unwoundValues = null
      }
    } while (readPos < input.validRows && writePos < output.validRows)

    output.validRows = writePos

    if (unwoundValues != null)
      ContinueWithDataAndSource(input, readPos, CurrentState(unwoundValues), iterationState)
    else if (readPos < input.validRows)
      ContinueWithData(input, readPos, iterationState)
    else
      EndOfLoop(iterationState)
  }

  override def addDependency(pipeline: Pipeline): Dependency = Lazy(pipeline)

  private case class CurrentState(unwoundValues: java.util.Iterator[AnyValue])

}
