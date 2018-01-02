/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.LegacyExpression
import org.neo4j.helpers.ThisShouldNotHappenError

case class SlicePipe(source: Pipe, skip: Option[Expression], limit: Option[Expression])
                    (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) {

  val symbols = source.symbols

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    //register as parent so that stats are associated with this pipe
    state.decorator.registerParentPipe(this)

    implicit val s = state

    if(input.isEmpty)
      return Iterator.empty

    val first: ExecutionContext = input.next()

    val sourceIter: Iterator[ExecutionContext] = new HeadAndTail(first, input)

    def asInt(v: Expression): Int = v(first).asInstanceOf[Number].intValue()

    (skip, limit) match {
      case (Some(x), None) => sourceIter.drop(asInt(x))
      case (None, Some(x)) => sourceIter.take(asInt(x))

      case (Some(startAt), Some(count)) =>
        val start = asInt(startAt)
        sourceIter.slice(start, start + asInt(count))

      case (None, None) =>
        throw new ThisShouldNotHappenError("Andres Taylor", "A slice pipe that doesn't slice should never exist.")
    }
  }

  override def planDescription = {

    val args: Seq[(String, Expression)] = (skip, limit) match {
      case (None, Some(l)) => Seq("limit" -> l)
      case (Some(s), None) => Seq("skip" -> s)
      case (Some(s), Some(l)) => Seq("skip" -> s, "limit" -> l)
      case (None, None)=>throw new ThisShouldNotHappenError("Andres Taylor", "A slice pipe that doesn't slice should never exist.")
    }
    source
      .planDescription
      .andThen(this.id, "Slice", identifiers, skip.map(LegacyExpression).toSeq ++ limit.map(LegacyExpression).toSeq:_*)
  }

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)
  }

  override def localEffects = Effects()
}

class HeadAndTail[T](head:T, tail:Iterator[T]) extends Iterator[T] {
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
