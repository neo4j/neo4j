/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_2.pipes

import org.neo4j.cypher.internal.compiler.v3_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_2.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.ProcedureCallMode
import org.neo4j.cypher.internal.compiler.v3_2.helpers.{ListSupport, RuntimeJavaValueConverter, RuntimeScalaValueConverter}
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.Id
import org.neo4j.cypher.internal.compiler.v3_2.spi.{ProcedureSignature, QualifiedName}
import org.neo4j.cypher.internal.frontend.v3_2.symbols.CypherType

object ProcedureCallRowProcessing {
  def apply(signature: ProcedureSignature) =
    if (signature.isVoid) PassThroughRow else FlatMapAndAppendToRow
}

sealed trait ProcedureCallRowProcessing

case object FlatMapAndAppendToRow extends ProcedureCallRowProcessing
case object PassThroughRow extends ProcedureCallRowProcessing

case class ProcedureCallPipe(source: Pipe,
                             name: QualifiedName,
                             callMode: ProcedureCallMode,
                             argExprs: Seq[Expression],
                             rowProcessing: ProcedureCallRowProcessing,
                             resultSymbols: Seq[(String, CypherType)],
                             resultIndices: Seq[(Int, String)])
                            (val id: Id = new Id)
                            (implicit monitor: PipeMonitor)
  extends PipeWithSource(source, monitor) with ListSupport {

  private val rowProcessor = rowProcessing match {
    case FlatMapAndAppendToRow => internalCreateResultsByAppending _
    case PassThroughRow => internalCreateResultsByPassingThrough _
  }

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    //register as parent so that stats are associated with this pipe
    state.decorator.registerParentPipe(this)
    val converter = new RuntimeJavaValueConverter(state.query.isGraphKernelResultValue, state.typeConverter.asPublicType)

    rowProcessor(input, state, converter)
  }

  private def internalCreateResultsByAppending(input: Iterator[ExecutionContext], state: QueryState,
                                               converter: RuntimeJavaValueConverter): Iterator[ExecutionContext] = {
    val qtx = state.query
    val builder = Seq.newBuilder[(String, Any)]
    builder.sizeHint(resultIndices.length)

    val isGraphKernelResultValue = qtx.isGraphKernelResultValue _
    val scalaValues = new RuntimeScalaValueConverter(isGraphKernelResultValue, state.typeConverter.asPrivateType)

    input flatMap { input =>
      val argValues = argExprs.map(arg => converter.asDeepJavaValue(arg(input)(state)))
      val results = callMode.callProcedure(qtx, name, argValues)
      results map { resultValues =>
        resultIndices foreach { case (k, v) =>
          val javaValue = resultValues(k)
          val scalaValue = scalaValues.asDeepScalaValue(javaValue)
          builder += v -> scalaValue
        }
        val rowEntries = builder.result()
        val output = input.newWith(rowEntries)
        builder.clear()
        output
      }
    }
  }

  private def internalCreateResultsByPassingThrough(input: Iterator[ExecutionContext], state: QueryState, converter: RuntimeJavaValueConverter): Iterator[ExecutionContext] = {
    val qtx = state.query
    input map { input =>
      val argValues = argExprs.map(arg => converter.asDeepJavaValue(arg(input)(state)))
      val results = callMode.callProcedure(qtx, name, argValues)
      // the iterator here should be empty; we'll drain just in case
      while (results.hasNext) results.next()
      input
    }
  }

  override def symbols = {
    val sourceSymbols = source.symbols
    val outputSymbols = resultSymbols.foldLeft(sourceSymbols) {
      case (symbols, (symbolName, symbolType)) =>
        symbols.add(symbolName, symbolType)
    }
    outputSymbols
  }

  override def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)(id)
  }
}
