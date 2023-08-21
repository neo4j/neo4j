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

import org.neo4j.cypher.internal.collection.DefaultComparatorTopTable
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.Ascending
import org.neo4j.cypher.internal.runtime.interpreted.Descending
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedExecutionContextOrdering
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.util.ValueUtils

import scala.util.Random

class TopNPipeTest extends CypherFunSuite {

  test("returning top 10 from 5 possible should return all") {
    val input = createFakePipeWith(5)
    val pipe = TopNPipe(input, literal(10), InterpretedExecutionContextOrdering.asComparator(List(Ascending("a"))))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx.getByName("a")).toList

    result should equal(list(0, 1, 2, 3, 4))
  }

  test("returning top 10 descending from 3 possible should return all") {
    val input = createFakePipeWith(3)
    val pipe = TopNPipe(input, literal(10), InterpretedExecutionContextOrdering.asComparator(List(Descending("a"))))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx.getByName("a")).toList

    result should equal(list(2, 1, 0))
  }

  test("returning top 5 from 20 possible should return 5 with lowest value") {
    val input = createFakePipeWith(20)
    val pipe = TopNPipe(input, literal(5), InterpretedExecutionContextOrdering.asComparator(List(Ascending("a"))))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx.getByName("a")).toList

    result should equal(list(0, 1, 2, 3, 4))
  }

  test("returning top 3 descending from 10 possible values should return three highest values") {
    val input = createFakePipeWith(10)
    val pipe = TopNPipe(input, literal(3), InterpretedExecutionContextOrdering.asComparator(List(Descending("a"))))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx.getByName("a")).toList

    result should equal(list(9, 8, 7))
  }

  test("returning top 5 from a reversed pipe should work correctly") {
    val in = (0 until 100).map(i => Map("a" -> i)).reverse
    val input = new FakePipe(in)

    val pipe = TopNPipe(input, literal(5), InterpretedExecutionContextOrdering.asComparator(List(Ascending("a"))))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx.getByName("a")).toList

    result should equal(list(0, 1, 2, 3, 4))
  }

  test("duplicates should be sorted correctly") {
    val in = ((0 until 5) ++ (0 until 5)).map(i => Map("a" -> i)).reverse
    val input = new FakePipe(in)

    val pipe = TopNPipe(input, literal(5), InterpretedExecutionContextOrdering.asComparator(List(Descending("a"))))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx.getByName("a")).toList

    result should equal(list(4, 4, 3, 3, 2))
  }

  test("duplicates should be sorted correctly for small lists") {
    val in = List(Map("a" -> 0), Map("a" -> 1), Map("a" -> 1))
    val input = new FakePipe(in)

    val pipe = TopNPipe(input, literal(2), InterpretedExecutionContextOrdering.asComparator(List(Descending("a"))))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx.getByName("a")).toList

    result should equal(list(1, 1))
  }

  test("should handle empty input") {
    val input = new FakePipe(Iterator.empty)

    val pipe = TopNPipe(input, literal(5), InterpretedExecutionContextOrdering.asComparator(List(Ascending("a"))))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx.getByName("a")).toList

    result should equal(List.empty)
  }

  test("should handle null input") {
    val input = new FakePipe(Seq(Map("a" -> 10), Map("a" -> null)))

    val pipe = TopNPipe(input, literal(5), InterpretedExecutionContextOrdering.asComparator(List(Ascending("a"))))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx.getByName("a")).toList

    result should equal(list(10, null))
  }

  // This test is really representing that all pipes should close their input
  // under special circumstances (here: early abort because of limit 0).
  // This test is _not_ repeated for every pipe since the solution was implemented in PipeWithSource.
  test("limit 0 should close input") {
    val input = new FakePipe(Seq(Map("a" -> 10), Map("a" -> null)))
    val pipe = TopNPipe(input, literal(0), InterpretedExecutionContextOrdering.asComparator(List(Ascending("a"))))()
    val result = pipe.createResults(QueryStateHelper.emptyWithValueSerialization).map(ctx => ctx.getByName("a"))
    result.hasNext
    input.wasClosed shouldBe true
  }

  test("close should close input and table") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val input = new FakePipe(Seq(Map("a" -> 10), Map("a" -> null)))
    val pipe = TopNPipe(input, literal(1), InterpretedExecutionContextOrdering.asComparator(List(Ascending("a"))))()
    val result =
      pipe.createResults(QueryStateHelper.emptyWithResourceManager(resourceManager)).map(ctx => ctx.getByName("a"))
    result.close()
    input.wasClosed shouldBe true
    monitor.closedResources.collect { case t: DefaultComparatorTopTable[_] => t } should have size (1)
  }

  test("exhaust should close input and table") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val input = new FakePipe(Seq(Map("a" -> 10), Map("a" -> null)))
    val pipe = TopNPipe(input, literal(1), InterpretedExecutionContextOrdering.asComparator(List(Ascending("a"))))()
    // exhaust
    pipe.createResults(QueryStateHelper.emptyWithResourceManager(resourceManager)).map(ctx => ctx.getByName("a")).toList
    input.wasClosed shouldBe true
    monitor.closedResources.collect { case t: DefaultComparatorTopTable[_] => t } should have size (1)
  }

  private def list(a: Any*) = a.map(ValueUtils.of).toList

  private def createFakePipeWith(count: Int): FakePipe = {

    val r = new Random(1337)

    val in = (0 until count).map(i => Map("a" -> i)).sortBy(_ => 50 - r.nextInt(100))
    new FakePipe(in)
  }
}
