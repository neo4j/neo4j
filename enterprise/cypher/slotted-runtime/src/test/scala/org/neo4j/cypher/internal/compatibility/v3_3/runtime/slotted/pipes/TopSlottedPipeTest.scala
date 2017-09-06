/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.PipelineInformation
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Literal
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryStateHelper
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.PrimitiveExecutionContext
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.javacompat.ValueUtils

import scala.util.Random

class TopSlottedPipeTest extends CypherFunSuite {

  private sealed trait _ColumnOrder
  private case object _Ascending extends _ColumnOrder
  private case object _Descending extends _ColumnOrder

  test("returning top 10 from 5 possible should return all") {
    val input = randomlyShuffledIntDataFromZeroUntil(5)
    val result = singleColumnTopWithInput(
      input, orderBy = _Ascending, limit = 10
    )
    result should equal(list(0, 1, 2, 3, 4))
  }

  test("returning top 10 descending from 3 possible should return all") {
    val result = singleColumnTopWithInput(
      randomlyShuffledIntDataFromZeroUntil(3), orderBy = _Descending, limit = 10
    )
    result should equal(list(2, 1, 0))
  }

  test("returning top 5 from 20 possible should return 5 with lowest value") {
    val result = singleColumnTopWithInput(
      randomlyShuffledIntDataFromZeroUntil(20), orderBy = _Ascending, limit = 5
    )
    result should equal(list(0, 1, 2, 3, 4))
  }

  test("returning top 3 descending from 10 possible values should return three highest values") {
    val result = singleColumnTopWithInput(
      randomlyShuffledIntDataFromZeroUntil(10), orderBy = _Descending, limit = 3
    )
    result should equal(list(9, 8, 7))
  }

  test("returning top 5 from a reversed pipe should work correctly") {
    val input = (0 until 100).reverse
    val result = singleColumnTopWithInput(
      input, orderBy = _Ascending, limit = 5
    )
    result should equal(list(0, 1, 2, 3, 4))
  }

  test("duplicates should be sorted correctly") {
    val input = ((0 until 5) ++ (0 until 5)).reverse
    val result = singleColumnTopWithInput(
      input, orderBy = _Descending, limit = 5
    )
    result should equal(list(4, 4, 3, 3, 2))
  }

  test("duplicates should be sorted correctly for small lists") {
    val input = List(0, 1, 1)
    val result = singleColumnTopWithInput(
      input, orderBy = _Descending, limit = 2
    )
    result should equal(list(1,1))
  }

  test("should handle empty input") {
    val input = Seq.empty
    val result = singleColumnTopWithInput(
      input, orderBy = _Ascending, limit = 5
    )
    result should equal(List.empty)
  }

  test("should handle null input") {
    val input = Seq(10, null)
    val result = singleColumnTopWithInput(
      input, orderBy = _Ascending, limit = 5
    )
    result should equal(list(10, null))
  }

  test("returning top 1 from 5 possible should return lowest") {
    val result = singleColumnTopWithInput(
      randomlyShuffledIntDataFromZeroUntil(5), orderBy = _Ascending, limit = 1
    )
    result should equal(list(0))
  }

  test("returning top 1 descending from 3 possible should return all") {
    val result = singleColumnTopWithInput(
      randomlyShuffledIntDataFromZeroUntil(3), orderBy = _Descending, limit = 1
    )
    result should equal(list(2))
  }

  test("returning top 1 from 20 possible should return 5 with lowest value") {
    val result = singleColumnTopWithInput(
      randomlyShuffledIntDataFromZeroUntil(20), orderBy = _Ascending, limit = 1
    )
    result should equal(list(0))
  }

  test("returning top 1 descending from 10 possible values should return three highest values") {
    val result = singleColumnTopWithInput(
      randomlyShuffledIntDataFromZeroUntil(10), orderBy = _Descending, limit = 1
    )
    result should equal(list(9))
  }

  test("returning top 1 from a reversed pipe should work correctly") {
    val input = (0 until 100).reverse
    val result = singleColumnTopWithInput(
      input, orderBy = _Ascending, limit = 1
    )
    result should equal(list(0))
  }

  test("duplicates should be sorted correctly with top 1") {
    val input = ((0 until 5) ++ (0 until 5)).reverse
    val result = singleColumnTopWithInput(
      input, orderBy = _Descending, limit = 1
    )
    result should equal(list(4))
  }

  test("duplicates should be sorted correctly for small lists with top 1") {
    val input = Seq(0, 1, 1)
    val result = singleColumnTopWithInput(
      input, orderBy = _Descending, limit = 1
    )
    result should equal(list(1))
  }

  test("top 1 should handle empty input with") {
    val result = singleColumnTopWithInput(
      Seq.empty, orderBy = _Descending, limit = 1
    )
    result should equal(List.empty)
  }

  test("top 1 should handle null input") {
    val input = Seq(10, null)
    val result = singleColumnTopWithInput(
      input, orderBy = _Ascending, limit = 1
    )
    result should equal(list(10))
  }

  test("top 5 from multi column values") {
    val input = List((2, 0), (1, 2), (0, 4), (1, 1), (0, 2), (1, 2), (0, 5), (2, 1))
    val result = twoColumnTopWithInput(
      input, orderBy = (_Ascending, _Descending), limit = 5
    )
    result should equal(list((0, 5), (0, 4), (0, 2), (1, 2), (1, 2)))
  }

  test("top 1 from multi column values") {
    val input = List((2, 0), (1, 2), (0, 4), (1, 1), (0, 2), (1, 2), (0, 5), (2, 1))
    val result = twoColumnTopWithInput(
      input, orderBy = (_Ascending, _Descending), limit = 1
    )
    result should equal(list((0, 5)))
  }

  private def list(a: Any*) = a.map {
    case (x: Number, y: Number) => (ValueUtils.of(x.longValue()), ValueUtils.of(y.longValue()))
    case x: Number => ValueUtils.of(x.longValue())
    case x => ValueUtils.of(x)
  }.toList

  private def randomlyShuffledIntDataFromZeroUntil(count: Int): Seq[Int] = {
    val r = new Random(1337)
    val data = (0 until count).sortBy( x => 50 - r.nextInt(100))
    data
  }

  private def singleColumnTopWithInput(data: Traversable[Any], orderBy: _ColumnOrder, limit: Int) = {
    val pipeline = PipelineInformation.empty
      .newReference("a", nullable = true, CTAny)

    val slotOffset = pipeline.getReferenceOffsetFor("a")

    val source = FakeSlottedPipe(data.map(v => Map("a" -> v)).toIterator, pipeline)

    val topOrderBy = orderBy match {
      case `_Ascending` => List(Ascending(slotOffset))
      case `_Descending` => List(Descending(slotOffset))
    }

    val topPipe =
      if (limit == 1)
        Top1SlottedPipe(source, topOrderBy)()
      else
        TopNSlottedPipe(source, topOrderBy, Literal(limit))()

    val results = topPipe.createResults(QueryStateHelper.empty)
    results.map {
      case c: PrimitiveExecutionContext =>
        c.getRefAt(slotOffset)
    }.toList
  }

  private def twoColumnTopWithInput(data: Traversable[(Any, Any)], orderBy: (_ColumnOrder, _ColumnOrder), limit: Int) = {
    val pipeline = PipelineInformation.empty
      .newReference("a", nullable = true, CTAny)
      .newReference("b", nullable = true, CTAny)

    val slotOffset1 = pipeline.getReferenceOffsetFor("a")
    val slotOffset2 = pipeline.getReferenceOffsetFor("b")

    val source = FakeSlottedPipe(data.map { case (v1, v2) => Map("a" -> v1, "b" -> v2) }.toIterator, pipeline)

    val topOrderBy = List((orderBy._1, slotOffset1), (orderBy._2, slotOffset2)).map {
      case (`_Ascending`, offset) => Ascending(offset)
      case (`_Descending`, offset) => Descending(offset)
    }

    val topPipe =
      if (limit == 1)
        Top1SlottedPipe(source, topOrderBy)()
      else
        TopNSlottedPipe(source, topOrderBy, Literal(limit))()

    topPipe.createResults(QueryStateHelper.empty).map {
      case c: PrimitiveExecutionContext =>
        (c.getRefAt(slotOffset1), c.getRefAt(slotOffset2))
    }.toList
  }
}
