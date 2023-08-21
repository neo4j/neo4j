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
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.TestSubscriber
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.tests.PartialSortTestBase.firstTwoColumns

import scala.util.Random

abstract class PartialTopNTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  private def unsortedPrefixThenTop(
    input: IndexedSeq[Array[Any]],
    chunkSize: Int,
    skip: Int,
    topLimit: Int
  ): IndexedSeq[Array[Any]] = {
    val skipPrefixLength = skip - skip % chunkSize
    val allSorted = input.sortBy(firstTwoColumns)
    val skipPrefixThenSortedTop =
      input.take(skipPrefixLength) ++
        allSorted.slice(skipPrefixLength, topLimit)
    allSorted should not equal skipPrefixThenSortedTop // Let's make sure the test can show the difference in not sorting the first rows.
    skipPrefixThenSortedTop
  }

  test("with limit 0") {
    // when
    val sortedInput = for (x <- 0 until sizeHint) yield Array[Any]("A", x)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(0, Seq("x ASC"), Seq("y ASC"))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Random.shuffle(sortedInput): _*))

    // then
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("empty input gives empty output") {
    // when
    val input = inputValues()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(5, Seq("x ASC"), Seq("y ASC"))
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
      .partialTop(Seq(Ascending(varFor("x"))), Seq(Ascending(varFor("y"))), 5, 17)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("simple top n if sorted column only has one value") {
    // when
    val topLimit = sizeHint / 10
    val sortedInput = for (x <- 0 until sizeHint) yield Array[Any]("A", x)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(Seq(Ascending(varFor("x"))), Seq(Ascending(varFor("y"))), topLimit)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Random.shuffle(sortedInput): _*))

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(sortedInput.take(topLimit)))
  }

  test("simple top n if sorted column only has one value with skip") {
    // when
    val topLimit = sizeHint / 10
    val skip = 7
    val sortedInput = for (x <- 0 until sizeHint) yield Array[Any]("A", x)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(Seq(Ascending(varFor("x"))), Seq(Ascending(varFor("y"))), topLimit, skip)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Random.shuffle(sortedInput): _*))

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(sortedInput.take(topLimit)))
  }

  test("simple top n for chunk size 1") {
    // when
    val topLimit = sizeHint / 10
    val input = for (i <- 0 until sizeHint) yield Array[Any](i, i)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(Seq(Ascending(varFor("x"))), Seq(Ascending(varFor("y"))), topLimit)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(input.take(topLimit)))
  }

  test("simple top n for chunk size 1 with skip") {
    // when
    val topLimit = sizeHint / 10
    val skip = 17
    val input = for (i <- 0 until sizeHint) yield Array[Any](i, i)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(Seq(Ascending(varFor("x"))), Seq(Ascending(varFor("y"))), topLimit, skip)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(input.take(topLimit)))
  }

  test("simple top n if sorted column has more values - return subset of first block") {
    // when
    val chunkSize = sizeHint / 10
    val topLimit = chunkSize / 2
    val input = for (i <- 0 until sizeHint) yield Array[Any](i / chunkSize, sizeHint - i)
    val sortedInput = input.sortBy(firstTwoColumns)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(Seq(Ascending(varFor("x"))), Seq(Ascending(varFor("y"))), topLimit)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(sortedInput.take(topLimit)))
  }

  test("simple top n if sorted column has more values - return whole first block") {
    // when
    val chunkSize = sizeHint / 10
    val topLimit = chunkSize
    val input = for (i <- 0 until sizeHint) yield Array[Any](i / chunkSize, sizeHint - i)
    val sortedInput = input.sortBy(firstTwoColumns)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(Seq(Ascending(varFor("x"))), Seq(Ascending(varFor("y"))), topLimit)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(sortedInput.take(topLimit)))
  }

  test("simple top n if sorted column has multiple values, skip = chunk size") {
    // when
    val chunkSize = sizeHint / 10
    val skip = chunkSize
    val topLimit = chunkSize + skip
    val input = for (i <- 0 until sizeHint) yield Array[Any](i / chunkSize, sizeHint - i)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(Seq(Ascending(varFor("x"))), Seq(Ascending(varFor("y"))), topLimit, skip)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    val expectedResult = unsortedPrefixThenTop(input, chunkSize, skip, topLimit)
    runtimeResult should beColumns("x", "y").withRows(inOrder(expectedResult))
  }

  test("simple top n if sorted column has multiple values, skip greater than chunk size") {
    // when
    val chunkSize = sizeHint / 10
    val skip = chunkSize + 1
    val topLimit = chunkSize + skip
    val input = for (i <- 0 until sizeHint) yield Array[Any](i / chunkSize, sizeHint - i)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(Seq(Ascending(varFor("x"))), Seq(Ascending(varFor("y"))), topLimit, skip)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    val expectedResult = unsortedPrefixThenTop(input, chunkSize, skip, topLimit)
    runtimeResult should beColumns("x", "y").withRows(inOrder(expectedResult))
  }

  test("simple top n if sorted column has multiple values, skip all but one") {
    // when
    val chunkSize = sizeHint / 10
    val skip = sizeHint - 1
    val topLimit = chunkSize + skip
    val input = for (i <- 0 until sizeHint) yield Array[Any](i / chunkSize, sizeHint - i)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(Seq(Ascending(varFor("x"))), Seq(Ascending(varFor("y"))), topLimit, skip)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    val expectedResult = unsortedPrefixThenTop(input, chunkSize, skip, topLimit)
    runtimeResult should beColumns("x", "y").withRows(inOrder(expectedResult))
  }

  test("simple top n if sorted column has multiple values, skip all rows") {
    // when
    val chunkSize = sizeHint / 10
    val skip = sizeHint
    val topLimit = chunkSize + skip
    val input = for (i <- 0 until sizeHint) yield Array[Any](i / chunkSize, sizeHint - i)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(Seq(Ascending(varFor("x"))), Seq(Ascending(varFor("y"))), topLimit, skip)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    val expectedResult = unsortedPrefixThenTop(input, chunkSize, skip, topLimit)
    runtimeResult should beColumns("x", "y").withRows(inOrder(expectedResult))
  }

  test("simple top n if results from two blocks must be returned - one from second block") {
    // when
    val chunkSize = sizeHint / 10
    val topLimit = chunkSize + 1
    val input = for (i <- 0 until sizeHint) yield Array[Any](i / chunkSize, sizeHint - i)
    val sortedInput = input.sortBy(firstTwoColumns)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(Seq(Ascending(varFor("x"))), Seq(Ascending(varFor("y"))), topLimit)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(sortedInput.take(topLimit)))
  }

  test("simple top n if results from two blocks must be returned - two from second block") {
    // when
    val chunkSize = sizeHint / 10
    val topLimit = chunkSize + 2
    val input = for (i <- 0 until sizeHint) yield Array[Any](i / chunkSize, sizeHint - i)
    val sortedInput = input.sortBy(firstTwoColumns)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(Seq(Ascending(varFor("x"))), Seq(Ascending(varFor("y"))), topLimit)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(sortedInput.take(topLimit)))
  }

  test("Top n with limit > Integer.Max works") {
    // when
    val chunkSize = sizeHint / 10
    val input = for (i <- 0 until sizeHint) yield Array[Any](i / chunkSize, sizeHint - i)
    val sortedInput = input.sortBy(firstTwoColumns)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(Seq(Ascending(varFor("x"))), Seq(Ascending(varFor("y"))), 1L + Int.MaxValue)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(sortedInput))
  }

  test("partial top should not exhaust input when limit is lower then the input size") {
    val input = inputColumns(nBatches = sizeHint / 10, batchSize = 10, row => row / 10, row => row % 10)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(5, Seq("x ASC"), Seq("y ASC"))
      .input(variables = Seq("x", "y"))
      .build()

    val stream = input.stream()
    // When
    val result = execute(logicalQuery, runtime, stream, TestSubscriber.concurrent)
    // Then
    result.request(Long.MaxValue)
    result.await() shouldBe false
    // we shouldn't have exhausted the entire input
    stream.hasMore shouldBe true
  }

  test("should work on RHS of apply") {
    val chunkSize = 17
    val topLimit = chunkSize + 1
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
      .|.partialTop(Seq(Ascending(varFor("x"))), Seq(Ascending(varFor("y"))), topLimit)
      .|.projection("b.x AS x", "b.y AS y")
      .|.nodeIndexOperator("b:B(x)", indexOrder = IndexOrderAscending, argumentIds = Set("a"))
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val rhs = for (i <- 0 until sizeHint) yield Array[Any](i / chunkSize, -i)

    val expected = for {
      a <- aNodes
      row <- rhs.sortBy(firstTwoColumns).take(topLimit)
    } yield Array[Any](a, row(0), row(1))

    runtimeResult should beColumns("a", "x", "y").withRows(inOrder(expected))
  }

  test("should work on RHS of apply with skip") {
    val chunkSize = 17
    val skip = chunkSize + 1
    val topLimit = chunkSize + skip
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
      .|.partialTop(Seq(Ascending(varFor("x"))), Seq(Ascending(varFor("y"))), topLimit, skip)
      .|.projection("b.x AS x", "b.y AS y")
      .|.nodeIndexOperator("b:B(x)", indexOrder = IndexOrderAscending, argumentIds = Set("a"))
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val rhs =
      unsortedPrefixThenTop(for (i <- 0 until sizeHint) yield Array[Any](i / chunkSize, -i), chunkSize, skip, topLimit)

    val expected = for {
      a <- aNodes
      row <- rhs
    } yield Array[Any](a, row(0), row(1))

    runtimeResult should beColumns("a", "x", "y").withRows(inOrder(expected))
  }

  test("should work on RHS of apply with variable number of rows per argument") {
    val inputSize = 100
    val topLimit = 17
    val input = for (a <- 0 until inputSize) yield Array[Any](a)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .apply()
      .|.partialTop(Seq(Ascending(varFor("b"))), Seq(Ascending(varFor("c"))), topLimit)
      .|.unwind("range(a, b, -1) AS c")
      .|.unwind("range(0, a) AS b")
      .|.argument("a")
      .input(variables = Seq("a"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    val expected = for {
      a <- 0 until inputSize
      rhs = for {
        b <- 0 to a
        c <- b to a
      } yield (b, c)
      (b, c) <- rhs.take(topLimit)
    } yield Array[Any](a, b, c)

    runtimeResult should beColumns("a", "b", "c").withRows(inOrder(expected))
  }
}
