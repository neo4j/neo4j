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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.TestSubscriber
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.tests.PartialSortTestBase.firstTwoColumns
import org.neo4j.cypher.internal.runtime.spec.tests.PartialSortTestBase.secondColumn

abstract class PartialSortTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  private def unsortedPrefixThenSort(
    input: IndexedSeq[Array[Any]],
    chunkSize: Int,
    skip: Int
  ): IndexedSeq[Array[Any]] = {
    val skipPrefixLength = skip - skip % chunkSize
    val allSorted = input.sortBy(firstTwoColumns)
    val skipPrefixThenSorted = input.take(skipPrefixLength) ++ input.drop(skipPrefixLength).sortBy(firstTwoColumns)
    allSorted should not equal skipPrefixThenSorted // Let's make sure the test can show the difference in not sorting the first rows.
    skipPrefixThenSorted
  }

  test("empty input gives empty output") {
    // when
    val input = inputValues()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialSort(Seq("x ASC"), Seq("y ASC"))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("empty input gives empty output with skip") {
    // when
    val input = inputValues()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialSort(Seq("x ASC"), Seq("y ASC"), 10)
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
      .partialSort(Seq("x ASC"), Seq("y ASC"))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(input.flatten.sortBy(secondColumn)))
  }

  test("partial sort with one column already sorted with only one distinct value with skip") {
    // when
    val input = inputColumns(nBatches = sizeHint / 10, batchSize = 10, _ => 1, row => row % 10)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialSort(Seq("x ASC"), Seq("y ASC"), sizeHint / 2)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(input.flatten.sortBy(secondColumn)))
  }

  test("partial sort with one column already sorted") {
    // when
    val input = inputColumns(nBatches = sizeHint / 10, batchSize = 10, row => row / 10, row => row % 5)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialSort(Seq("x ASC"), Seq("y ASC"))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(input.flatten.sortBy(firstTwoColumns)))
  }

  test("partial sort with one column already sorted with skip") {
    // when
    val chunkSize = 10
    val skip = 55
    val input = inputColumns(
      nBatches = sizeHint / chunkSize,
      batchSize = chunkSize,
      row => row / chunkSize,
      row => row % (chunkSize / 2)
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialSort(Seq("x ASC"), Seq("y ASC"), skip)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = unsortedPrefixThenSort(input.flatten, chunkSize, skip)
    runtimeResult should beColumns("x", "y").withRows(inOrder(expected))
  }

  test("partial sort with one column already sorted with skip aligned with chunk boundary") {
    // when
    val chunkSize = 10
    val skip = 5 * chunkSize
    val input = inputColumns(
      nBatches = sizeHint / chunkSize,
      batchSize = chunkSize,
      row => row / chunkSize,
      row => row % (chunkSize / 2)
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialSort(Seq("x ASC"), Seq("y ASC"), skip)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = unsortedPrefixThenSort(input.flatten, chunkSize, skip)
    runtimeResult should beColumns("x", "y").withRows(inOrder(expected))
  }

  test("partial sort with one column already sorted with skipping all but 1 row") {
    // when
    val chunkSize = 10
    val skip = sizeHint - 1
    val input = inputColumns(
      nBatches = sizeHint / chunkSize,
      batchSize = chunkSize,
      row => row / chunkSize,
      row => row % (chunkSize / 2)
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialSort(Seq("x ASC"), Seq("y ASC"), skip)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = unsortedPrefixThenSort(input.flatten, chunkSize, skip)
    runtimeResult should beColumns("x", "y").withRows(inOrder(expected))
  }

  test("partial sort with one column already sorted with skipping all rows") {
    // when
    val chunkSize = 10
    val input = inputColumns(
      nBatches = sizeHint / chunkSize,
      batchSize = chunkSize,
      row => row / chunkSize,
      row => row % (chunkSize / 2)
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialSort(Seq("x ASC"), Seq("y ASC"), sizeHint)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(input.flatten))
  }

  test("partial sort with chunk size 1") {
    // when
    val input = inputColumns(nBatches = sizeHint / 10, batchSize = 10, row => row, row => row % 10)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialSort(Seq("x ASC"), Seq("y ASC"))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(input.flatten.sortBy(firstTwoColumns)))
  }

  test("partial sort with chunk size 1 with skip") {
    // when
    val input = inputColumns(nBatches = sizeHint / 10, batchSize = 10, row => row, row => row % 10)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialSort(Seq("x ASC"), Seq("y ASC"), 55)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(input.flatten.sortBy(firstTwoColumns)))
  }

  test("partial sort with two sorted and two unsorted columns") {
    // when
    val input = inputColumns(
      nBatches = sizeHint / 10,
      batchSize = 10,
      row => row / 20,
      row => -row / 4,
      row => row % 13,
      row => row % 7
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z", "a")
      .partialSort(Seq("x ASC", "y DESC"), Seq("z ASC", "a DESC"))
      .input(variables = Seq("x", "y", "z", "a"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y", "z", "a").withRows(inOrder(input.flatten.sortBy(row =>
      (row(0).asInstanceOf[Int], -row(1).asInstanceOf[Int], row(2).asInstanceOf[Int], -row(3).asInstanceOf[Int])
    )))
  }

  test("should handle null values") {
    // when
    val batchSize = 10
    val input = inputColumns(
      nBatches = sizeHint / batchSize,
      batchSize = batchSize,
      row => if (row < sizeHint - batchSize) row / batchSize else null,
      row => if (row % 3 != 0) row % 10 else null
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialSort(Seq("x ASC"), Seq("y ASC"))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    def sortKey(col: Any): Int = col match {
      case i: Int => i
      case null   => Integer.MAX_VALUE
    }

    runtimeResult should beColumns("x", "y").withRows(inOrder(input.flatten.sortBy(row =>
      (sortKey(row(0)), sortKey(row(1)))
    )))
  }

  test("should work on RHS of apply") {
    val chunkSize = 17
    nodeIndex("B", "x")
    val aNodes = given {
      val aNodes = nodeGraph(2, "A")
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("x" -> i / chunkSize, "y" -> -i)
        },
        "B"
      )
      aNodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "x", "y")
      .apply()
      .|.partialSort(Seq("x ASC"), Seq("y ASC"))
      .|.projection("b.x AS x", "b.y AS y")
      .|.nodeIndexOperator("b:B(x)", indexOrder = IndexOrderAscending, argumentIds = Set("a"))
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val rhs = for (i <- 0 until sizeHint) yield Array[Any](i / chunkSize, -i)

    val expected = for {
      a <- aNodes
      row <- rhs.sortBy(firstTwoColumns)
    } yield Array[Any](a, row(0), row(1))

    runtimeResult should beColumns("a", "x", "y").withRows(inOrder(expected))
  }

  test("should work on RHS of apply with skip") {
    val chunkSize = 17
    val skip = chunkSize + 1
    nodeIndex("B", "x")
    val aNodes = given {
      val aNodes = nodeGraph(2, "A")
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("x" -> i / chunkSize, "y" -> -i)
        },
        "B"
      )
      aNodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "x", "y")
      .apply()
      .|.partialSort(Seq("x ASC"), Seq("y ASC"), skip)
      .|.projection("b.x AS x", "b.y AS y")
      .|.nodeIndexOperator("b:B(x)", indexOrder = IndexOrderAscending, argumentIds = Set("a"))
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val rhs = unsortedPrefixThenSort(for (i <- 0 until sizeHint) yield Array[Any](i / chunkSize, -i), chunkSize, skip)

    val expected = for {
      a <- aNodes
      row <- rhs
    } yield Array[Any](a, row(0), row(1))

    runtimeResult should beColumns("a", "x", "y").withRows(inOrder(expected))
  }

  test("partial sort should not exhaust input when there is no demand") {
    val input = inputColumns(nBatches = sizeHint / 10, batchSize = 10, row => row / 10, row => row % 10)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialSort(Seq("x ASC"), Seq("y ASC"))
      .input(variables = Seq("x", "y"))
      .build()

    val stream = input.stream()
    // When
    val result = execute(logicalQuery, runtime, stream, TestSubscriber.concurrent)

    // Then
    result.request(1)
    result.await() shouldBe true
    // we shouldn't have exhausted the entire input
    stream.hasMore shouldBe true
  }
}

object PartialSortTestBase {
  def secondColumn(row: Array[Any]): Int = row(1).asInstanceOf[Int]
  def firstTwoColumns(row: Array[Any]): (Int, Int) = (row(0).asInstanceOf[Int], row(1).asInstanceOf[Int])
}
