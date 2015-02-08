/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v1_9.pipes

import org.neo4j.cypher.internal.compiler.v1_9.commands.expressions.Expression
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.compiler.v1_9.ExecutionContext
import org.neo4j.cypher.internal.compiler.v1_9.data.SimpleVal
import org.neo4j.cypher.internal.compiler.v1_9.symbols.SymbolTable

class SlicePipe(source:Pipe, skip:Option[Expression], limit:Option[Expression]) extends PipeWithSource(source) {

  val symbols = source.symbols

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
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

  override def executionPlanDescription = {

    val args: Seq[(String, Expression)] = (skip, limit) match {
      case (None, Some(l)) => Seq("limit" -> l)
      case (Some(s), None) => Seq("skip" -> s)
      case (Some(s), Some(l)) => Seq("skip" -> s, "limit" -> l)
      case (None, None)=>throw new ThisShouldNotHappenError("Andres Taylor", "A slice pipe that doesn't slice should never exist.")
    }
    source.executionPlanDescription.andThen(this, "Slice", args.toMap.mapValues(SimpleVal.fromStr).toSeq: _*)
  }

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    skip.foreach(_.throwIfSymbolsMissing(symbols))
    limit.foreach(_.throwIfSymbolsMissing(symbols))
  }
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
