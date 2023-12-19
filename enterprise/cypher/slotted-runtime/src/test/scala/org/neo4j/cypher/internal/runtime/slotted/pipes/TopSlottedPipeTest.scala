/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import java.util.Comparator

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{LongSlot, RefSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, QueryStateHelper}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, Top1Pipe, Top1WithTiesPipe, TopNPipe}
import org.neo4j.cypher.internal.runtime.slotted.{ExecutionContextOrdering, SlottedExecutionContext}
import org.neo4j.cypher.internal.runtime.slotted.pipes.TopSlottedPipeTestSupport._
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue

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
    val input = Seq[Any](10, null)
    val result = singleColumnTopWithInput(
      input, orderBy = AscendingOrder, limit = 5
    )
    result should equal(list(10, null))
  }

  test("should handle limit 0") {
    val input = randomlyShuffledIntDataFromZeroUntil(5)
    val result = singleColumnTopWithInput(
      input, orderBy = AscendingOrder, limit = 0
    )
    result should equal(List.empty)
  }

  test("should handle negative limit") {
    val input = randomlyShuffledIntDataFromZeroUntil(5)
    val result = singleColumnTopWithInput(
      input, orderBy = AscendingOrder, limit = -1
    )
    result should equal(List.empty)
  }

  test("should handle limit of Int.MaxValue") {
    val input = randomlyShuffledIntDataFromZeroUntil(5)
    val result = singleColumnTopWithInput(
      input, orderBy = AscendingOrder, limit = Int.MaxValue
    )
    result should equal(list(0, 1, 2, 3, 4))
  }

  ignore("should handle limit larger than Int.MaxValue") {
    val input = randomlyShuffledIntDataFromZeroUntil(5)
    val result = singleColumnTopWithInput(
      input, orderBy = AscendingOrder, limit = Int.MaxValue.asInstanceOf[Long] * 2
    )
    result should equal(list(0, 1, 2, 3, 4))
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
    val input = Seq[Any](10, null)
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

  def list(a: Any*): List[Object] = a.map {
    case (x: Number, y: Number) => (ValueUtils.of(x.longValue()), ValueUtils.of(y.longValue()))
    case x: Number => ValueUtils.of(x.longValue())
    case (x, y) => (ValueUtils.of(x), ValueUtils.of(y))
    case x => ValueUtils.of(x)
  }.toList

  def randomlyShuffledIntDataFromZeroUntil(count: Int): Seq[Int] = {
    val data = Random.shuffle((0 until count).toList)
    data
  }

  private def createTopPipe(source: Pipe, orderBy: List[ColumnOrder], limit: Long, withTies: Boolean) = {
    val comparator = ExecutionContextOrdering.asComparator(orderBy)
    if (withTies) {
      assert(limit == 1)
      Top1WithTiesPipe(source, comparator)()
    }
    else if (limit == 1) {
      Top1Pipe(source, comparator)()
    }
    else {
      TopNPipe(source, Literal(limit), comparator)()
    }
  }

  def singleColumnTopWithInput(data: Traversable[Any], orderBy: TestColumnOrder, limit: Long, withTies: Boolean = false): List[Any] = {
    val slots = SlotConfiguration.empty
      .newReference("a", nullable = true, CTAny)

    val slot = slots("a")

    val source = FakeSlottedPipe(data.map(v => Map("a" -> v)).toIterator, slots)

    val topOrderBy = orderBy match {
      case AscendingOrder => List(Ascending(slot))
      case DescendingOrder => List(Descending(slot))
    }

    val topPipe = createTopPipe(source, topOrderBy, limit, withTies)

    val results = topPipe.createResults(QueryStateHelper.empty)
    results.map {
      case c: SlottedExecutionContext =>
        slot match {
          case RefSlot(offset, _, _) =>
            c.getRefAt(offset)
          case LongSlot(offset, _, _) =>
            c.getLongAt (offset)
        }
    }.toList
  }

  def twoColumnTopWithInput(data: Traversable[(Any, Any)], orderBy: Seq[TestColumnOrder], limit: Int, withTies: Boolean = false): List[(AnyValue, AnyValue)] = {
    val slotConfiguration = SlotConfiguration.empty
      .newReference("a", nullable = true, CTAny)
      .newReference("b", nullable = true, CTAny)

    val slots = Seq(slotConfiguration("a"), slotConfiguration("b"))

    val source = FakeSlottedPipe(data.map { case (v1, v2) => Map("a" -> v1, "b" -> v2) }.toIterator, slotConfiguration)

    val topOrderBy = orderBy.zip(slots).map {
      case (AscendingOrder, slot) => Ascending(slot)
      case (DescendingOrder, slot) => Descending(slot)
    }.toList

    val topPipe = createTopPipe(source, topOrderBy, limit, withTies)

    topPipe.createResults(QueryStateHelper.empty).map {
      case c: SlottedExecutionContext =>
        (slots(0), slots(1)) match {
          case (RefSlot(offset1, _, _), RefSlot(offset2, _, _)) =>
            (c.getRefAt(offset1), c.getRefAt(offset2))
          case _ =>
            throw new InternalException("LongSlot not yet supported in the test framework")
        }
    }.toList
  }

  def singleColumnTop1WithTiesWithInput(data: Traversable[Any], orderBy: TestColumnOrder): List[Any] = {
    singleColumnTopWithInput(data, orderBy, limit = 1, withTies = true)
  }

  def twoColumnTop1WithTiesWithInput(data: Traversable[(Any, Any)], orderBy: Seq[TestColumnOrder]): List[(AnyValue, AnyValue)] = {
    twoColumnTopWithInput(data, orderBy, limit = 1, withTies = true)
  }
}
