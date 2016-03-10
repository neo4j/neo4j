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
package org.neo4j.cypher.internal.compiler.v3_0.pipes

import org.neo4j.cypher.internal.compiler.v3_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{AllEffects, ProcedureCallMode}
import org.neo4j.cypher.internal.compiler.v3_0.helpers.ScalaCompatibility.asScalaCompatible
import org.neo4j.cypher.internal.compiler.v3_0.helpers.{CollectionSupport, JavaResultValueConverter}
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription.Arguments.Signature
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.{InternalPlanDescription, PlanDescriptionImpl, SingleChild}
import org.neo4j.cypher.internal.compiler.v3_0.spi.{ProcedureSignature, QualifiedProcedureName}
import org.neo4j.cypher.internal.frontend.v3_0.symbols.CypherType

object ProcedureCallRowProcessing {
  def apply(signature: ProcedureSignature) =
    if (signature.isVoid) PassThroughRow else FlatMapAndAppendToRow
}

sealed trait ProcedureCallRowProcessing

case object FlatMapAndAppendToRow extends ProcedureCallRowProcessing
case object PassThroughRow extends ProcedureCallRowProcessing

case class ProcedureCallPipe(source: Pipe,
                             name: QualifiedProcedureName,
                             callMode: ProcedureCallMode,
                             argExprs: Seq[Expression],
                             rowProcessing: ProcedureCallRowProcessing,
                             resultSymbols: Seq[(String, CypherType)],
                             resultIndices: Seq[(Int, String)])
                            (val estimatedCardinality: Option[Double] = None)
                            (implicit monitor: PipeMonitor)
  extends PipeWithSource(source, monitor) with CollectionSupport with RonjaPipe {

  private val rowProcessor = rowProcessing match {
    case FlatMapAndAppendToRow => internalCreateResultsByAppending _
    case PassThroughRow => internalCreateResultsByPassingThrough _
  }

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    //register as parent so that stats are associated with this pipe
    state.decorator.registerParentPipe(this)
    val converter = new JavaResultValueConverter(state.query.isGraphKernelResultValue)

    rowProcessor(input, state, converter)
  }

  private def internalCreateResultsByAppending(input: Iterator[ExecutionContext], state: QueryState, converter: JavaResultValueConverter): Iterator[ExecutionContext] = {
    val qtx = state.query
    val builder = Seq.newBuilder[(String, Any)]
    builder.sizeHint(resultIndices.length)

    input flatMap { input =>
      val argValues = argExprs.map(arg => converter.asDeepJavaResultValue(arg(input)(state)))
      val results = callMode.call(qtx, name, argValues)
      results map { resultValues =>
        resultIndices foreach { case (k, v) =>
          val javaValue = resultValues(k)
          val scalaValue = asScalaCompatible(javaValue)
          builder += v -> scalaValue
        }
        val rowEntries = builder.result()
        val output = input.newWith(rowEntries)
        builder.clear()
        output
      }
    }
  }

  private def internalCreateResultsByPassingThrough(input: Iterator[ExecutionContext], state: QueryState, converter: JavaResultValueConverter): Iterator[ExecutionContext] = {
    val qtx = state.query
    input map { input =>
      val argValues = argExprs.map(arg => converter.asDeepJavaResultValue(arg(input)(state)))
      val results = callMode.call(qtx, name, argValues)
      // the iterator here should be empty; we'll drain just in case
      while (results.hasNext) results.next()
      input
    }
  }

    def planDescriptionWithoutCardinality: InternalPlanDescription = {
    PlanDescriptionImpl(this.id, "ProcedureCall", SingleChild(source.planDescription), Seq(
      Signature(QualifiedProcedureName(name.namespace, name.name), argExprs, resultSymbols)
    ), variables)
  }

  def symbols = resultSymbols.foldLeft(source.symbols) {
      case (symbols, (symbolName, symbolType)) =>
        symbols.add(symbolName, symbolType.legacyIteratedType)
    }

  override def localEffects = AllEffects

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)(estimatedCardinality)
  }

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))
}
