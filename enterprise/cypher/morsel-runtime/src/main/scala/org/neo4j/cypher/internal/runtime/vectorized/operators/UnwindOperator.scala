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
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => InterpretedQueryState}
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.values.AnyValue

class UnwindOperator(collection: Expression,
                     offset: Int)
  extends StreamingOperator with ListSupport {

  override def init(context: QueryContext, state: QueryState, inputRow: MorselExecutionContext): ContinuableOperatorTask = {
    val queryState = new InterpretedQueryState(context, resources = null, params = state.params)
    val value = collection(inputRow, queryState)
    val unwoundValues = makeTraversable(value).iterator
    new OTask(inputRow, unwoundValues)
  }

  class OTask(var inputRow: MorselExecutionContext,
              var unwoundValues: java.util.Iterator[AnyValue]
             ) extends ContinuableOperatorTask {

    override def operate(outputRow: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState): Unit = {

      val queryState = new InterpretedQueryState(context, resources = null, params = state.params)

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
    }

    override def canContinue: Boolean = unwoundValues != null || inputRow.hasMoreRows
  }

  private case class CurrentState(unwoundValues: java.util.Iterator[AnyValue])
}
