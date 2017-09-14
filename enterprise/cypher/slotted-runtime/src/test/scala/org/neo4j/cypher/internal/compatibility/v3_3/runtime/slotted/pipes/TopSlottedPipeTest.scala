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

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Literal
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{Pipe, QueryStateHelper}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.PrimitiveExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.pipes.TopSlottedPipeTestSupport._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{LongSlot, PipelineInformation, RefSlot}
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.helpers.ValueUtils

import scala.util.Random

class TopSlottedPipeTest extends CypherFunSuite {

  test("returning top 10 from 5 possible should return all") {
    val input = randomlyShuffledIntDataFromZeroUntil(5)
    val result = singleColumnTopWithInput(
      input, orderBy = AscendingOrder, limit = 10
    )
    result should equal(list(0, 1, 2, 3, 4))
  }

  test("returning top 10 descending from 3 possible should return all") {
    val result = singleColumnTopWithInput(
      randomlyShuffledIntDataFromZeroUntil(3), orderBy = DescendingOrder, limit = 10
    )
    result should equal(list(2, 1, 0))
  }

  test("returning top 5 from 20 possible should return 5 with lowest value") {
    val result = singleColumnTopWithInput(
      randomlyShuffledIntDataFromZeroUntil(20), orderBy = AscendingOrder, limit = 5
    )
    result should equal(list(0, 1, 2, 3, 4))
  }

  test("returning top 3 descending from 10 possible values should return three highest values") {
    val result = singleColumnTopWithInput(
      randomlyShuffledIntDataFromZeroUntil(10), orderBy = DescendingOrder, limit = 3
    )
    result should equal(list(9, 8, 7))
  }

  test("returning top 5 from a reversed pipe should work correctly") {
    val input = (0 until 100).reverse
    val result = singleColumnTopWithInput(
      input, orderBy = AscendingOrder, limit = 5
    )
    result should equal(list(0, 1, 2, 3, 4))
  }

  test("duplicates should be sorted correctly") {
    val input = ((0 until 5) ++ (0 until 5)).reverse
    val result = singleColumnTopWithInput(
      input, orderBy = DescendingOrder, limit = 5
    )
    result should equal(list(4, 4, 3, 3, 2))
  }

  test("duplicates should be sorted correctly for small lists") {
    val input = List(0, 1, 1)
    val result = singleColumnTopWithInput(
      input, orderBy = DescendingOrder, limit = 2
    )
    result should equal(list(1,1))
  }

  test("should handle empty input") {
    val input = Seq.empty
    val result = singleColumnTopWithInput(
      input, orderBy = AscendingOrder, limit = 5
    )
    result should equal(List.empty)
  }

  test("should handle null input") {
    val input = Seq(10, null)
    val result = singleColumnTopWithInput(
      input, orderBy = AscendingOrder, limit = 5
    )
    result should equal(list(10, null))
  }

  test("returning top 1 from 5 possible should return lowest") {
    val result = singleColumnTopWithInput(
      randomlyShuffledIntDataFromZeroUntil(5), orderBy = AscendingOrder, limit = 1
    )
    result should equal(list(0))
  }

  test("returning top 1 descending from 3 possible should return all") {
    val result = singleColumnTopWithInput(
      randomlyShuffledIntDataFromZeroUntil(3), orderBy = DescendingOrder, limit = 1
    )
    result should equal(list(2))
  }

  test("returning top 1 from 20 possible should return 5 with lowest value") {
    val result = singleColumnTopWithInput(
      randomlyShuffledIntDataFromZeroUntil(20), orderBy = AscendingOrder, limit = 1
    )
    result should equal(list(0))
  }

  test("returning top 1 descending from 10 possible values should return three highest values") {
    val result = singleColumnTopWithInput(
      randomlyShuffledIntDataFromZeroUntil(10), orderBy = DescendingOrder, limit = 1
    )
    result should equal(list(9))
  }

  test("returning top 1 from a reversed pipe should work correctly") {
    val input = (0 until 100).reverse
    val result = singleColumnTopWithInput(
      input, orderBy = AscendingOrder, limit = 1
    )
    result should equal(list(0))
  }

  test("duplicates should be sorted correctly with top 1") {
    val input = ((0 until 5) ++ (0 until 5)).reverse
    val result = singleColumnTopWithInput(
      input, orderBy = DescendingOrder, limit = 1
    )
    result should equal(list(4))
  }

  test("duplicates should be sorted correctly for small lists with top 1") {
    val input = Seq(0, 1, 1)
    val result = singleColumnTopWithInput(
      input, orderBy = DescendingOrder, limit = 1
    )
    result should equal(list(1))
  }

  test("top 1 should handle empty input with") {
    val result = singleColumnTopWithInput(
      Seq.empty, orderBy = DescendingOrder, limit = 1
    )
    result should equal(List.empty)
  }

  test("top 1 should handle null input") {
    val input = Seq(10, null)
    val result = singleColumnTopWithInput(
      input, orderBy = AscendingOrder, limit = 1
    )
    result should equal(list(10))
  }

  test("top 5 from multi column values") {
    val input = List((2, 0), (1, 2), (0, 4), (1, 1), (0, 2), (1, 2), (0, 5), (2, 1))
    val result = twoColumnTopWithInput(
      input, orderBy = Seq(AscendingOrder, DescendingOrder), limit = 5
    )
    result should equal(list((0, 5), (0, 4), (0, 2), (1, 2), (1, 2)))
  }

  test("top 1 from multi column values") {
    val input = List((2, 0), (1, 2), (0, 4), (1, 1), (0, 2), (1, 2), (0, 5), (2, 1))
    val result = twoColumnTopWithInput(
      input, orderBy = Seq(AscendingOrder, DescendingOrder), limit = 1
    )
    result should equal(list((0, 5)))
  }
}

object TopSlottedPipeTestSupport {
  sealed trait TestColumnOrder
  case object AscendingOrder extends TestColumnOrder
  case object DescendingOrder extends TestColumnOrder

  def list(a: Any*) = a.map {
    case (x: Number, y: Number) => (ValueUtils.of(x.longValue()), ValueUtils.of(y.longValue()))
    case x: Number => ValueUtils.of(x.longValue())
    case (x, y) => (ValueUtils.of(x), ValueUtils.of(y))
    case x => ValueUtils.of(x)
  }.toList

  def randomlyShuffledIntDataFromZeroUntil(count: Int): Seq[Int] = {
    val data = Random.shuffle((0 until count).toList)
    data
  }

  private def createTopPipe(source: Pipe, orderBy: List[ColumnOrder], limit: Int, withTies: Boolean) = {
    if (withTies) {
      assert(limit == 1)
      Top1WithTiesSlottedPipe(source, orderBy)()
    }
    else if (limit == 1) {
      Top1SlottedPipe(source, orderBy)()
    }
    else {
      TopNSlottedPipe(source, orderBy, Literal(limit))()
    }
  }

  def singleColumnTopWithInput(data: Traversable[Any], orderBy: TestColumnOrder, limit: Int, withTies: Boolean = false) = {
    val pipeline = PipelineInformation.empty
      .newReference("a", nullable = true, CTAny)

    val slot = pipeline("a")

    val source = FakeSlottedPipe(data.map(v => Map("a" -> v)).toIterator, pipeline)

    val topOrderBy = orderBy match {
      case AscendingOrder => List(Ascending(slot))
      case DescendingOrder => List(Descending(slot))
    }

    val topPipe = createTopPipe(source, topOrderBy, limit, withTies)

    val results = topPipe.createResults(QueryStateHelper.empty)
    results.map {
      case c: PrimitiveExecutionContext =>
        slot match {
          case RefSlot(offset, _, _, _) =>
            c.getRefAt(offset)
          case LongSlot(offset, _, _, _) =>
            c.getLongAt (offset)
        }
    }.toList
  }

  def twoColumnTopWithInput(data: Traversable[(Any, Any)], orderBy: Seq[TestColumnOrder], limit: Int, withTies: Boolean = false) = {
    val pipeline = PipelineInformation.empty
      .newReference("a", nullable = true, CTAny)
      .newReference("b", nullable = true, CTAny)

    val slots = Seq(pipeline("a"), pipeline("b"))

    val source = FakeSlottedPipe(data.map { case (v1, v2) => Map("a" -> v1, "b" -> v2) }.toIterator, pipeline)

    val topOrderBy = orderBy.zip(slots).map {
      case (AscendingOrder, slot) => Ascending(slot)
      case (DescendingOrder, slot) => Descending(slot)
    }.toList

    val topPipe = createTopPipe(source, topOrderBy, limit, withTies)

    topPipe.createResults(QueryStateHelper.empty).map {
      case c: PrimitiveExecutionContext =>
        (slots(0), slots(1)) match {
          case (RefSlot(offset1, _, _, _), RefSlot(offset2, _, _, _)) =>
            (c.getRefAt(offset1), c.getRefAt(offset2))
          case _ =>
            throw new InternalException("LongSlot not yet supported in the test framework")
        }
    }.toList
  }

  def singleColumnTop1WithTiesWithInput(data: Traversable[Any], orderBy: TestColumnOrder) = {
    singleColumnTopWithInput(data, orderBy, limit = 1, withTies = true)
  }

  def twoColumnTop1WithTiesWithInput(data: Traversable[(Any, Any)], orderBy: Seq[TestColumnOrder]) = {
    twoColumnTopWithInput(data, orderBy, limit = 1, withTies = true)
  }
}