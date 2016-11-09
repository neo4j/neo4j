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
import org.neo4j.cypher.internal.compiler.v3_2.commands.expressions.{Expression, NumericHelper}
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription.Arguments.LegacyExpression
import org.neo4j.cypher.internal.compiler.v3_2.symbols.SymbolTable

case class SkipPipe(source: Pipe, exp: Expression)
                   (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) with NumericHelper with RonjaPipe {
  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    //register as parent so that stats are associated with this pipe
    state.decorator.registerParentPipe(this)

    if(input.isEmpty)
      return Iterator.empty

    implicit val s = state

    val first: ExecutionContext = input.next()

    val count = asInt(exp(first))

    new HeadAndTail(first, input).drop(count)
  }

  def planDescriptionWithoutCardinality = source
    .planDescription
    .andThen(this.id, "Skip", variables, LegacyExpression(exp))

  def symbols: SymbolTable = source.symbols

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)(estimatedCardinality)
  }

  override def localEffects = Effects()

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))
}

class HeadAndTail[T](head: T, tail: Iterator[T]) extends Iterator[T] {
  var usedHead: Boolean = false

  def headUnused: Boolean = !usedHead

  def hasNext: Boolean = headUnused || tail.hasNext

  def next(): T = if (headUnused) {
    usedHead = true
    head
  } else {
    tail.next()
  }
}
