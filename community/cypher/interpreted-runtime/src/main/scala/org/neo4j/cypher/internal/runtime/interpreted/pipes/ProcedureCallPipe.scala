/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, ValueConversion}
import org.neo4j.cypher.internal.runtime.{ProcedureCallMode, QueryContext}
import org.opencypher.v9_0.util.attribution.Id
import org.opencypher.v9_0.util.symbols.CypherType
import org.neo4j.cypher.internal.v3_5.logical.plans.ProcedureSignature
import org.neo4j.values.AnyValue

object ProcedureCallRowProcessing {
  def apply(signature: ProcedureSignature): ProcedureCallRowProcessing =
    if (signature.isVoid) PassThroughRow else FlatMapAndAppendToRow
}

sealed trait ProcedureCallRowProcessing

case object FlatMapAndAppendToRow extends ProcedureCallRowProcessing
case object PassThroughRow extends ProcedureCallRowProcessing

case class ProcedureCallPipe(source: Pipe,
                             signature: ProcedureSignature,
                             callMode: ProcedureCallMode,
                             argExprs: Seq[Expression],
                             rowProcessing: ProcedureCallRowProcessing,
                             resultSymbols: Seq[(String, CypherType)],
                             resultIndices: Seq[(Int, String)])
                            (val id: Id = Id.INVALID_ID)

  extends PipeWithSource(source) {

  argExprs.foreach(_.registerOwningPipe(this))

  private val maybeConverter = signature.outputSignature.map(_.map(v => ValueConversion.getValueConverter(v.typ)).toArray)
  private val rowProcessor = rowProcessing match {
    case FlatMapAndAppendToRow => internalCreateResultsByAppending _
    case PassThroughRow => internalCreateResultsByPassingThrough _
  }

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = rowProcessor(input, state)

  private def internalCreateResultsByAppending(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    val qtx = state.query
    val builder = Seq.newBuilder[(String, AnyValue)]
    builder.sizeHint(resultIndices.length)
    input flatMap { input =>
      val argValues = argExprs.map(arg => qtx.asObject(arg(input, state)))
      val results = call(qtx, argValues)
      results map { resultValues =>
        resultIndices foreach { case (k, v) =>
          val javaValue = maybeConverter.get(k)(resultValues(k))
          builder += v -> javaValue
        }
        val rowEntries = builder.result()
        val output = executionContextFactory.copyWith(input, rowEntries)
        builder.clear()
        output
      }
    }
  }

  private def call(qtx: QueryContext,
                   argValues: Seq[Any]) =
    if (signature.id.nonEmpty) callMode.callProcedure(qtx, signature.id.get, argValues)
    else callMode.callProcedure(qtx, signature.name, argValues)

  private def internalCreateResultsByPassingThrough(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    val qtx = state.query
    input map { input =>
      val argValues = argExprs.map(arg => qtx.asObject(arg(input, state)))
      val results = call(qtx, argValues)
      // the iterator here should be empty; we'll drain just in case
      while (results.hasNext) results.next()
      input
    }
  }
}
