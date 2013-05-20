/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.pipes

import org.neo4j.cypher.internal.commands.expressions.Expression
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.data.SimpleVal
import org.neo4j.cypher.internal.symbols.SymbolTable

class SlicePipe(source:Pipe, skip:Option[Expression], limit:Option[Expression]) extends PipeWithSource(source) {

  val symbols = source.symbols

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) : Iterator[ExecutionContext] = {
    implicit val s = state

    if(input.isEmpty)
      return Iterator.empty

    val first: ExecutionContext = input.next()

    val sourceIter: Iterator[ExecutionContext] = new HeadAndTail[ExecutionContext](first, input)

    def asInt(v: Expression): Int = v(first)(state).asInstanceOf[Int]

    (skip, limit) match {
      case (Some(x), None) => sourceIter.drop(asInt(x))
      case (None, Some(x)) => sourceIter.take(asInt(x))
      case (Some(startAt), Some(count)) => {
        val start = asInt(startAt)
        sourceIter.slice(start, start + asInt(count))
      }
      case (None, None)=>throw new ThisShouldNotHappenError("Andres Taylor", "A slice pipe that doesn't slice should never exist.")
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
    // TODO do it
  }
}

class HeadAndTail[T](head:T, tail:Iterator[T]) extends Iterator[T] {
  var usedHead = false
  def headUnused = !usedHead

  def hasNext = headUnused || tail.hasNext

  def next() = if (headUnused) {
    usedHead = true
    head
  } else {
    tail.next()
  }
}