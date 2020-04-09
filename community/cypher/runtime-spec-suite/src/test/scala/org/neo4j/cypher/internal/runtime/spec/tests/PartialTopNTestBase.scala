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
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.TestSubscriber
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

import scala.util.Random

abstract class PartialTopNTestBase[CONTEXT <: RuntimeContext](
                                                               edition: Edition[CONTEXT],
                                                               runtime: CypherRuntime[CONTEXT],
                                                               sizeHint: Int
                                                             ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("empty input gives empty output") {
    // when
    val input = inputValues()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(Seq(Ascending("x")), Seq(Ascending("y")), 5)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("simple top n if sorted column only has one value") {
    // when
    val topLimit = sizeHint / 10
    val sortedInput = for (x <- 1 to sizeHint) yield Array[Any]("A", x)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(Seq(Ascending("x")), Seq(Ascending("y")), topLimit)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Random.shuffle(sortedInput):_*))

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(sortedInput.take(topLimit)))
  }

  test("simple top n for chunk size 1") {
    // when
    val topLimit = sizeHint / 10
    val input = for (i <- 1 to sizeHint) yield Array[Any](i, i)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(Seq(Ascending("x")), Seq(Ascending("y")), topLimit)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input:_*))

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(input.take(topLimit)))
  }

  test("simple top n if sorted column has more values - return subset of first block") {
    // when
    val chunkSize = sizeHint / 10
    val topLimit = chunkSize / 2
    val input = for (i <- 1 to sizeHint) yield Array[Any](i / chunkSize, sizeHint - i)
    val sortedInput = input.sortBy(row => (row(0).asInstanceOf[Int], row(1).asInstanceOf[Int]))

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(Seq(Ascending("x")), Seq(Ascending("y")), topLimit)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input:_*))

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(sortedInput.take(topLimit)))
  }

  test("simple top n if sorted column has more values - return whole first block") {
    // when
    val chunkSize = sizeHint / 10
    val topLimit = chunkSize
    val input = for (i <- 1 to sizeHint) yield Array[Any](i / chunkSize, sizeHint - i)
    val sortedInput = input.sortBy(row => (row(0).asInstanceOf[Int], row(1).asInstanceOf[Int]))

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(Seq(Ascending("x")), Seq(Ascending("y")), topLimit)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input:_*))

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(sortedInput.take(topLimit)))
  }

  test("simple top n if results from two blocks must be returned - one from second block") {
    // when
    val chunkSize = sizeHint / 10
    val topLimit = chunkSize + 1
    val input = for (i <- 1 to sizeHint) yield Array[Any](i / chunkSize, sizeHint - i)
    val sortedInput = input.sortBy(row => (row(0).asInstanceOf[Int], row(1).asInstanceOf[Int]))

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(Seq(Ascending("x")), Seq(Ascending("y")), topLimit)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input:_*))

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(sortedInput.take(topLimit)))
  }

  test("simple top n if results from two blocks must be returned - two from second block") {
    // when
    val chunkSize = sizeHint / 10
    val topLimit = chunkSize + 2
    val input = for (i <- 1 to sizeHint) yield Array[Any](i / chunkSize, sizeHint - i)
    val sortedInput = input.sortBy(row => (row(0).asInstanceOf[Int], row(1).asInstanceOf[Int]))

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(Seq(Ascending("x")), Seq(Ascending("y")), topLimit)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input:_*))

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(sortedInput.take(topLimit)))
  }

  test("Top n with limit > Integer.Max works") {
    // when
    val chunkSize = sizeHint / 10
    val input = for (i <- 1 to sizeHint) yield Array[Any](i / chunkSize, sizeHint - i)
    val sortedInput = input.sortBy(row => (row(0).asInstanceOf[Int], row(1).asInstanceOf[Int]))

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(Seq(Ascending("x")), Seq(Ascending("y")), 1L + Int.MaxValue)
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input:_*))

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(sortedInput))
  }

  test("partial top should not exhaust input when limit is lower then the input size") {
    val input = inputColumns(nBatches = sizeHint / 10, batchSize = 10, row => row / 10, row => row % 10)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialTop(Seq(Ascending("x")), Seq(Ascending("y")), 5)
      .input(variables = Seq("x", "y"))
      .build()

    val stream = input.stream()
    // When
    val result = execute(logicalQuery, runtime, stream, TestSubscriber.concurrent)
    // Then
    result.request(Long.MaxValue)
    result.await() shouldBe false
    //we shouldn't have exhausted the entire input
    stream.hasMore shouldBe true
  }

  test("should work on RHS of apply") {
    val propValue = 10
    val topLimit = 42
    index("B", "prop")
    val (aNodes, bNodes) = given {
      val aNodes = nodeGraph(2, "A")
      val bNodes = nodePropertyGraph(4, {
        case i: Int => Map("prop" -> (if (i % 2 == 0) propValue else 0))
      }, "B")
      (aNodes, bNodes)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .apply()
      .|.partialTop(Seq(Ascending("b")), Seq(Ascending("c")), topLimit)
      .|.unwind("range(b.prop, 1, -1) AS c")
      .|.nodeIndexOperator("b:B(prop > 0)", indexOrder = IndexOrderAscending, argumentIds = Set("a"))
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val rhs = for {
      b <- bNodes if b.getProperty("prop").asInstanceOf[Int] > 0
      c <- Range.inclusive(1, propValue)
    } yield (b, c)

    val expected = for {
      a <- aNodes
      (b, c) <- rhs.take(topLimit)
    } yield Array[Any](a, b, c)

    runtimeResult should beColumns("a", "b", "c").withRows(inOrder(expected))
  }

  test("should work on RHS of apply with variable number of rows per argument") {
    val inputSize = 100
    val topLimit = 17
    val input = for (a <- 1 to inputSize) yield Array[Any](a)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .apply()
      .|.partialTop(Seq(Ascending("b")), Seq(Ascending("c")), topLimit)
      .|.unwind("range(a, b, -1) AS c")
      .|.unwind("range(1, a) AS b")
      .|.argument("a")
      .input(variables = Seq("a"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input:_*))

    // then
    val expected = for {
      a <- 1 to inputSize
      rhs = for {
        b <- 1 to a
        c <- b to a
      } yield (b, c)
      (b, c) <- rhs.take(topLimit)
    } yield Array[Any](a, b, c)

    runtimeResult should beColumns("a", "b", "c").withRows(inOrder(expected))
  }
}
