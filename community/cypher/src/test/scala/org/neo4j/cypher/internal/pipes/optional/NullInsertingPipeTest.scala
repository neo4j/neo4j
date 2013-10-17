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
package org.neo4j.cypher.internal.pipes.optional

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.scalatest.Assertions
import org.junit.Test
import org.junit.runners.Parameterized.Parameters
import org.neo4j.cypher.internal.pipes._
import org.neo4j.cypher.internal.symbols.{StringType, SymbolTable, NumberType}
import org.neo4j.cypher.internal.{PlanDescription, ExecutionContext}

@RunWith(value = classOf[Parameterized])
class NullInsertingPipeTest(name: String,
                            sourceIter: List[Map[String, Any]],
                            mapF: Iterator[ExecutionContext] => Iterator[ExecutionContext],
                            expected: List[Map[String, Any]]) extends Assertions {

  @Test
  def test() {
    val sourcePipe = new FakePipe(sourceIter, "x" -> NumberType())
    val builder = (source: Pipe) => MapPipe(source, mapF)

    val nullInsertingPipe = new NullInsertingPipe(sourcePipe, builder)
    val results = nullInsertingPipe.createResults(QueryStateHelper.empty).toList

    assert(expected === results)
  }

  case class MapPipe(sourcePipe: Pipe, mapF: Iterator[ExecutionContext] => Iterator[ExecutionContext]) extends PipeWithSource(sourcePipe) {
    def symbols = sourcePipe.symbols.add("z", StringType())

    def throwIfSymbolsMissing(symbols: SymbolTable) {}

    protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) =
      mapF(input).map(m => m.newWith("z" -> m("x").toString))

    def executionPlanDescription: PlanDescription = ???
  }

}

object NullInsertingPipeTest {

  @Parameters(name = "{0}")
  def parameters: java.util.Collection[Array[AnyRef]] = {
    val list = new java.util.ArrayList[Array[AnyRef]]()
    def add(name: String,
            xIterator: List[Map[String, Any]],
            innerBuilder: Iterator[ExecutionContext] => Iterator[ExecutionContext],
            expected: List[Map[String, Any]]) {
      list.add(Array(name, xIterator, innerBuilder, expected))
    }

    add(name = "should_handle_filter",
      xIterator = List(Map("x" -> 1), Map("x" -> 2), Map("x" -> 3), Map("x" -> 4)),
      innerBuilder = in => in.filter(m => m("x").asInstanceOf[Int] > 2),
      expected = List(Map("x" -> 1, "z" -> null), Map("x" -> 2, "z" -> null), Map("x" -> 3, "z" -> "3"), Map("x" -> 4, "z" -> "4")))

    add(name = "should_handle_expand",
      xIterator = List(Map("x" -> 1), Map("x" -> 2)),
      innerBuilder = in => in.flatMap(element => Seq(element, element)),
      expected = List(Map("x" -> 1, "z" -> "1"), Map("x" -> 1, "z" -> "1"), Map("x" -> 2, "z" -> "2"), Map("x" -> 2, "z" -> "2")))

    add(name = "should_handle_duplicates",
      xIterator = List(Map("x" -> 1), Map("x" -> 1), Map("x" -> 2)),
      innerBuilder = in => in,
      expected = List(Map("x" -> 1, "z" -> "1"), Map("x" -> 1, "z" -> "1"), Map("x" -> 2, "z" -> "2")))

    add(name = "should_handle_empties",
      xIterator = List(Map("x" -> 1), Map("x" -> 2), Map("x" -> 3)),
      innerBuilder = in => {
        // the incoming iterator must be emptied so the listener can see it pass through
        in.toList
        List[ExecutionContext]().toIterator
      },
      expected = List(Map("x" -> 1, "z" -> null), Map("x" -> 2, "z" -> null), Map("x" -> 3, "z" -> null)))

    list
  }
}
