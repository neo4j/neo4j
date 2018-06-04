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

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.ListSupport
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.values.AnyValue

class UnwindOperator(collection: Expression,
                     offset: Int)
  extends Operator with ListSupport {

  override def operate(source: Message,
                       outputRow: MorselExecutionContext,
                       context: QueryContext,
                       state: QueryState): Continuation = {
    var unwoundValues: java.util.Iterator[AnyValue] = null
    var iterationState: Iteration = null
    var inputRow: MorselExecutionContext = null
    val queryState = new OldQueryState(context, resources = null, params = state.params)

    source match {
      case StartLoopWithSingleMorsel(ir, is) =>
        inputRow = ir
        iterationState = is
        val value = collection(inputRow, queryState)
        unwoundValues = makeTraversable(value).iterator

      case ContinueLoopWith(ContinueWithDataAndSource(ir, CurrentState(inValues), is)) =>
        inputRow = ir
        iterationState = is
        unwoundValues = inValues
      case _ => throw new IllegalStateException()
    }

    do {
      if (unwoundValues == null) {
        val value = collection(inputRow, queryState)
        unwoundValues = makeTraversable(value).iterator
      }

      while (unwoundValues.hasNext && outputRow.hasMoreRows) {
        val thisValue = unwoundValues.next()
        outputRow.copyFrom(inputRow)
        outputRow.setRefAt(offset, thisValue)
        outputRow.moveToNextRow()
      }

      if (!unwoundValues.hasNext) {
        inputRow.moveToNextRow()
        unwoundValues = null
      }
    } while (inputRow.hasMoreRows && outputRow.hasMoreRows)

    outputRow.finishedWriting()

    if (unwoundValues != null)
      ContinueWithDataAndSource(inputRow, CurrentState(unwoundValues), iterationState)
    else if (inputRow.hasMoreRows)
      ContinueWithData(inputRow, iterationState)
    else
      EndOfLoop(iterationState)
  }

  override def addDependency(pipeline: Pipeline): Dependency = Lazy(pipeline)

  private case class CurrentState(unwoundValues: java.util.Iterator[AnyValue])

}
