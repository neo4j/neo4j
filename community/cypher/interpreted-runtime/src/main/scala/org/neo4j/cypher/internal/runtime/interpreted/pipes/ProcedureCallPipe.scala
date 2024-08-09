/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingIterator.ResourceRawIteratorAsClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ProcedureCallMode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.util.attribution.Id

object ProcedureCallPipe {

  def apply(
    id: Id,
    source: Pipe,
    pId: Int,
    callMode: ProcedureCallMode,
    args: Array[Expression],
    outputColumns: Seq[(Int, String, String)]
  ): ProcedureCallPipe = {
    val (outputIndex, outputCols, origFields) = outputColumns.view.unzip3
    new ProcedureCallPipe(source, pId, callMode, args, outputCols.toArray, outputIndex.toArray, origFields.toArray)(id)
  }
}

case class ProcedureCallPipe(
  source: Pipe,
  pId: Int,
  callMode: ProcedureCallMode,
  argExprs: Array[Expression],
  outputColumnNames: Array[String],
  outputResultIndex: Array[Int],
  originalFieldNames: Array[String]
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val qtx = state.query
    input.flatMap { input =>
      val args = argExprs.map(_.apply(input, state))
      callMode
        .callProcedure(qtx, pId, args, qtx.procedureCallContext(pId, originalFieldNames))
        .asClosingIterator
        .map { resultValues =>
          val output = rowFactory.copyWith(input)
          var i = 0
          while (i < outputResultIndex.length) {
            output.set(outputColumnNames(i), resultValues(outputResultIndex(i)))
            i += 1
          }
          output
        }
    }
  }
}

case class VoidProcedureCallPipe(
  source: Pipe,
  pId: Int,
  callMode: ProcedureCallMode,
  argExprs: Array[Expression],
  originalFieldNames: Array[String]
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val qtx = state.query
    input map { input =>
      val args = argExprs.map(arg => arg(input, state))
      val results = callMode.callProcedure(qtx, pId, args, qtx.procedureCallContext(pId, originalFieldNames))
      // the iterator here should be empty; we'll drain just in case
      while (results.hasNext) results.next()
      input
    }
  }
}
