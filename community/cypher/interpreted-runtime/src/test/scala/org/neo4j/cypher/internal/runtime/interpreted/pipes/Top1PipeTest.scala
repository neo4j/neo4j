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

import org.neo4j.cypher.internal.runtime.interpreted.Ascending
import org.neo4j.cypher.internal.runtime.interpreted.Descending
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedExecutionContextOrdering
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.util.ValueUtils

import scala.util.Random

class Top1PipeTest extends CypherFunSuite {

  test("returning top 1 from 5 possible should return lowest") {
    val input = createFakePipeWith(5)
    val pipe = Top1Pipe(input, InterpretedExecutionContextOrdering.asComparator(List(Ascending("a"))))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx.getByName("a")).toList

    result should equal(list(0))
  }

  test("returning top 1 descending from 3 possible should return all") {
    val input = createFakePipeWith(3)
    val pipe = Top1Pipe(input, InterpretedExecutionContextOrdering.asComparator(List(Descending("a"))))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx.getByName("a")).toList

    result should equal(list(2))
  }

  test("returning top 1 from 20 possible should return 5 with lowest value") {
    val input = createFakePipeWith(20)
    val pipe = Top1Pipe(input, InterpretedExecutionContextOrdering.asComparator(List(Ascending("a"))))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx.getByName("a")).toList

    result should equal(list(0))
  }

  test("returning top 1 descending from 10 possible values should return three highest values") {
    val input = createFakePipeWith(10)
    val pipe = Top1Pipe(input, InterpretedExecutionContextOrdering.asComparator(List(Descending("a"))))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx.getByName("a")).toList

    result should equal(list(9))
  }

  test("returning top 1 from a reversed pipe should work correctly") {
    val in = (0 until 100).map(i => Map("a" -> i)).reverse
    val input = new FakePipe(in)

    val pipe = Top1Pipe(input, InterpretedExecutionContextOrdering.asComparator(List(Ascending("a"))))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx.getByName("a")).toList

    result should equal(list(0))
  }

  test("duplicates should be sorted correctly with top 1") {
    val in = ((0 until 5) ++ (0 until 5)).map(i => Map("a" -> i)).reverse
    val input = new FakePipe(in)

    val pipe = Top1Pipe(input, InterpretedExecutionContextOrdering.asComparator(List(Descending("a"))))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx.getByName("a")).toList

    result should equal(list(4))
  }

  test("duplicates should be sorted correctly for small lists with top 1") {
    val in = List(Map("a" -> 0), Map("a" -> 1), Map("a" -> 1))
    val input = new FakePipe(in)

    val pipe = Top1Pipe(input, InterpretedExecutionContextOrdering.asComparator(List(Descending("a"))))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx.getByName("a")).toList

    result should equal(list(1))
  }

  test("top 1 should handle empty input with") {
    val input = new FakePipe(Iterator.empty)

    val pipe = Top1Pipe(input, InterpretedExecutionContextOrdering.asComparator(List(Ascending("a"))))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx.getByName("a")).toList

    result should equal(List.empty)
  }

  test("top 1 should handle null input") {
    val input = new FakePipe(Seq(Map("a" -> 10), Map("a" -> null)))

    val pipe = TopNPipe(input, literal(5), InterpretedExecutionContextOrdering.asComparator(List(Ascending("a"))))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx.getByName("a")).toList

    result should equal(list(10, null))
  }

  private def list(a: Any*) = a.map(ValueUtils.of).toList

  private def createFakePipeWith(count: Int): FakePipe = {

    val r = new Random(1337)

    val in = (0 until count).map(i => Map("a" -> i)).sortBy(_ => 50 - r.nextInt(100))
    new FakePipe(in)
  }
}
