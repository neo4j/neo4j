/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.Descending
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.TestSubscriber
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

abstract class PartialSortTestBase[CONTEXT <: RuntimeContext](
                                                               edition: Edition[CONTEXT],
                                                               runtime: CypherRuntime[CONTEXT],
                                                               sizeHint: Int
                                                             ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("empty input gives empty output") {
    // when
    val input = inputValues()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialSort(Seq(Ascending("x")), Seq(Ascending("y")))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("partial sort with one column already sorted with only one distinct value") {
    // when
    val input = inputColumns(nBatches = sizeHint / 10, batchSize = 10, _ => 1, row => row % 10)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialSort(Seq(Ascending("x")), Seq(Ascending("y")))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(input.flatten.sortBy(_(1).asInstanceOf[Int])))
  }

  test("partial sort with one column already sorted") {
    // when
    val input = inputColumns(nBatches = sizeHint / 10, batchSize = 10, row => row / 10, row => row % 10)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialSort(Seq(Ascending("x")), Seq(Ascending("y")))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(input.flatten.sortBy(row => (row(0).asInstanceOf[Int], row(1).asInstanceOf[Int]))))
  }

  test("partial sort with chunk size 1") {
    // when
    val input = inputColumns(nBatches = sizeHint / 10, batchSize = 10, row => row, row => row % 10)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialSort(Seq(Ascending("x")), Seq(Ascending("y")))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(input.flatten.sortBy(row => (row(0).asInstanceOf[Int], row(1).asInstanceOf[Int]))))
  }

  test("partial sort with two sorted and two unsorted columns") {
    // when
    val input = inputColumns(nBatches = sizeHint / 10, batchSize = 10,
      row => row / 20,
      row => -row / 4,
      row => row % 13,
      row => row % 7)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z", "a")
      .partialSort(Seq(Ascending("x"), Descending("y")), Seq(Ascending("z"), Descending("a")))
      .input(variables = Seq("x", "y", "z", "a"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y", "z", "a").withRows(inOrder(input.flatten.sortBy(row =>
      (row(0).asInstanceOf[Int], -row(1).asInstanceOf[Int], row(2).asInstanceOf[Int], -row(3).asInstanceOf[Int]))))  }

  test("should handle null values") {
    // when
    val batchSize = 10
    val input = inputColumns(nBatches = sizeHint / batchSize, batchSize = batchSize,
      row => if (row < sizeHint - batchSize) row / batchSize else null,
      row => if (row % 3 != 0) row % 10 else null)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialSort(Seq(Ascending("x")), Seq(Ascending("y")))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    def sortKey(col: Any): Int = col match {
      case i: Int => i
      case null => Integer.MAX_VALUE
    }

    runtimeResult should beColumns("x", "y").withRows(inOrder(input.flatten.sortBy(row => (sortKey(row(0)), sortKey(row(1))))))
  }

  test("should work on RHS of apply") {
    val propValue = 10
    index("B", "prop")
    val (aNodes, bNodes) = given {
      val aNodes = nodeGraph(5, "A")
      val bNodes = nodePropertyGraph(100, {
        case i: Int => Map("prop" -> (if (i % 2 == 0) propValue else 0))
      }, "B")
      (aNodes, bNodes)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .apply()
      .|.partialSort(Seq(Ascending("b")), Seq(Ascending("c")))
      .|.unwind("range(b.prop, 1, -1) AS c")
      .|.nodeIndexOperator("b:B(prop >= 0)", indexOrder = IndexOrderAscending, argumentIds = Set("a"))
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = for {
      a <- aNodes
      b <- bNodes if b.getProperty("prop").asInstanceOf[Int] == propValue
      c <- Range.inclusive(1, propValue)
    }  yield Array[Any](a, b, c)

    runtimeResult should beColumns("a", "b", "c").withRows(inOrder(expected))
  }

  test("partial sort should not exhaust input when there is no demand") {
    val input = inputColumns(nBatches = sizeHint / 10, batchSize = 10, row => row / 10, row => row % 10)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialSort(Seq(Ascending("x")), Seq(Ascending("y")))
      .input(variables = Seq("x", "y"))
      .build()

    val stream = input.stream()
    // When
    val result = execute(logicalQuery, runtime, stream, TestSubscriber.concurrent)

    // Then
    result.request(1)
    result.await() shouldBe true
    //we shouldn't have exhausted the entire input
    stream.hasMore shouldBe true
  }
}
