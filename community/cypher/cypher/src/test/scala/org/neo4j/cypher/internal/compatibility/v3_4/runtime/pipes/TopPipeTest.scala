/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions.Literal
import org.neo4j.cypher.internal.frontend.v3_4.symbols._
import org.neo4j.helpers.ValueUtils
import org.neo4j.cypher.internal.apa.v3_4.test_helpers.CypherFunSuite

import scala.util.Random

class TopPipeTest extends CypherFunSuite {

  test("returning top 10 from 5 possible should return all") {
    val input = createFakePipeWith(5)
    val pipe = TopNPipe(input, List(Ascending("a")), Literal(10))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx("a")).toList

    result should equal(list(0, 1, 2, 3, 4))
  }

  test("returning top 10 descending from 3 possible should return all") {
    val input = createFakePipeWith(3)
    val pipe = TopNPipe(input, List(Descending("a")), Literal(10))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx("a")).toList

    result should equal(list(2, 1, 0))
  }

  test("returning top 5 from 20 possible should return 5 with lowest value") {
    val input = createFakePipeWith(20)
    val pipe = TopNPipe(input, List(Ascending("a")), Literal(5))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx("a")).toList

    result should equal(list(0, 1, 2, 3, 4))
  }

  test("returning top 3 descending from 10 possible values should return three highest values") {
    val input = createFakePipeWith(10)
    val pipe = TopNPipe(input, List(Descending("a")), Literal(3))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx("a")).toList

    result should equal(list(9, 8, 7))
  }

  test("returning top 5 from a reversed pipe should work correctly") {
    val in = (0 until 100).map(i => Map("a" -> i)).reverse
    val input = new FakePipe(in, "a" -> CTInteger)

    val pipe = TopNPipe(input, List(Ascending("a")), Literal(5))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx("a")).toList

    result should equal(list(0, 1, 2, 3, 4))
  }

  test("duplicates should be sorted correctly") {
    val in = ((0 until 5) ++ (0 until 5)).map(i => Map("a" -> i)).reverse
    val input = new FakePipe(in, "a" -> CTInteger)

    val pipe = TopNPipe(input, List(Descending("a")), Literal(5))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx("a")).toList

    result should equal(list(4, 4, 3, 3, 2))
  }

  test("duplicates should be sorted correctly for small lists") {
    val in = List(Map("a" -> 0),Map("a" -> 1),Map("a" -> 1))
    val input = new FakePipe(in, "a" -> CTInteger)

    val pipe = TopNPipe(input, List(Descending("a")), Literal(2))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx("a")).toList

    result should equal(list(1,1))
  }

  test("should handle empty input") {
    val input = new FakePipe(Iterator.empty, "a" -> CTInteger)

    val pipe = TopNPipe(input, List(Ascending("a")), Literal(5))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx("a")).toList

    result should equal(List.empty)
  }

  test("should handle null input") {
    val input = new FakePipe(Seq(Map("a"->10),Map("a"->null)), "a" -> CTInteger)

    val pipe = TopNPipe(input, List(Ascending("a")), Literal(5))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx("a")).toList

    result should equal(list(10,null))
  }

  test("returning top 1 from 5 possible should return lowest") {
    val input = createFakePipeWith(5)
    val pipe = Top1Pipe(input, List(Ascending("a")))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx("a")).toList

    result should equal(list(0))
  }

  test("returning top 1 descending from 3 possible should return all") {
    val input = createFakePipeWith(3)
    val pipe = Top1Pipe(input, List(Descending("a")))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx("a")).toList

    result should equal(list(2))
  }

  test("returning top 1 from 20 possible should return 5 with lowest value") {
    val input = createFakePipeWith(20)
    val pipe = Top1Pipe(input, List(Ascending("a")))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx("a")).toList

    result should equal(list(0))
  }

  test("returning top 1 descending from 10 possible values should return three highest values") {
    val input = createFakePipeWith(10)
    val pipe = Top1Pipe(input, List(Descending("a")))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx("a")).toList

    result should equal(list(9))
  }

  test("returning top 1 from a reversed pipe should work correctly") {
    val in = (0 until 100).map(i => Map("a" -> i)).reverse
    val input = new FakePipe(in, "a" -> CTInteger)

    val pipe = Top1Pipe(input, List(Ascending("a")))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx("a")).toList

    result should equal(list(0))
  }

  test("duplicates should be sorted correctly with top 1") {
    val in = ((0 until 5) ++ (0 until 5)).map(i => Map("a" -> i)).reverse
    val input = new FakePipe(in, "a" -> CTInteger)

    val pipe = Top1Pipe(input, List(Descending("a")))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx("a")).toList

    result should equal(list(4))
  }

  test("duplicates should be sorted correctly for small lists with top 1") {
    val in = List(Map("a" -> 0),Map("a" -> 1),Map("a" -> 1))
    val input = new FakePipe(in, "a" -> CTInteger)

    val pipe = Top1Pipe(input, List(Descending("a")))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx("a")).toList

    result should equal(list(1))
  }

  test("top 1 should handle empty input with") {
    val input = new FakePipe(Iterator.empty, "a" -> CTInteger)

    val pipe = Top1Pipe(input, List(Ascending("a")))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx("a")).toList

    result should equal(List.empty)
  }

  test("top 1 should handle null input") {
    val input = new FakePipe(Seq(Map("a"->10),Map("a"->null)), "a" -> CTInteger)

    val pipe = TopNPipe(input, List(Ascending("a")), Literal(5))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx("a")).toList

    result should equal(list(10,null))
  }

  private def list(a: Any*) = a.map(ValueUtils.of).toList

  private def createFakePipeWith(count: Int): FakePipe = {

    val r = new Random(1337)

    val in = (0 until count).map(i => Map("a" -> i)).sortBy( x => 50 - r.nextInt(100))
    new FakePipe(in, "a" -> CTInteger)
  }
}
