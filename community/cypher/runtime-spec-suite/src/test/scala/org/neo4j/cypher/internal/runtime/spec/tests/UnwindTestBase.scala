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

import java.util
import java.util.Collections

import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}

import scala.collection.JavaConverters._

abstract class UnwindTestBase[CONTEXT <: RuntimeContext](
                                                          edition: Edition[CONTEXT],
                                                          runtime: CypherRuntime[CONTEXT],
                                                          sizeHint: Int
                                                        ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should unwind on top of argument with no IDs") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i")
      .unwind("[1,2,3,4,5] AS i")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("i").withRows(singleColumn(1 to 5))
  }

  test("should produce one rows if expression is not a list") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i")
      .unwind("'lalala' AS i")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("i").withSingleRow("lalala")
  }

  test("should produce no rows if expression is null") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i")
      .unwind("null AS i")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("i").withNoRows()
  }

  test("should produce no rows if expression is an empty list") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i")
      .unwind("[] AS i")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("i").withNoRows()
  }

  test("should unwind a list of lists") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i")
      .unwind("[[1,2,3],[4,5,6]] AS i")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("i").withRows(singleColumn(Seq(util.Arrays.asList(1, 2, 3), util.Arrays.asList(4, 5, 6))))
  }

  test("should produce no rows on top of empty input") {
    // given
    val input = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "i")
      .unwind("[1,2,3,4,5] AS i")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "i").withNoRows()
  }

  test("should produce rows on top of non-empty input") {
    // given
    val input = inputValues((0 until sizeHint).map(Array[Any](_)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "i")
      .unwind("[1,2,3,4,5] AS i")
      .input(variables = Seq("x"))
      .build()
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = for {
      x <- 0 until sizeHint
      i <- 1 to 5
    } yield Array(x, i)
    runtimeResult should beColumns("x", "i").withRows(expected)
  }

  test("should unwind a variable") {
    // given
    val input: InputValues = inputValues(
      Array(util.Arrays.asList(10, 20, 30)),
      Array(util.Arrays.asList(100, 200, 300)))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i")
      .unwind("x AS i")
      .input(variables = Seq("x"))
      .build()
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = for {
      Array(xs: util.List[Int]) <- input.flatten
      i <- xs.asScala
    } yield Array(i)
    runtimeResult should beColumns("i").withRows(expected)
  }

  test("should handle unwind with non-fused limit under apply") {
    // given
    val limit = 1
    val input = for (a <- 1 to sizeHint) yield Array[Any](a)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .apply()
      .|.limit(limit) // assuming this won't be fused
      .|.unwind("range(1, 10) AS b")
      .|.argument("a")
      .input(variables = Seq("a"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input:_*))

    // then
    val expected = for {
      a <- 1 to sizeHint
      b <- (1 to 10).take(limit)
    } yield Array[Any](a, b)

    runtimeResult should beColumns("a", "b").withRows(expected)
  }

  test("should handle nested unwinds with non-fused limit under apply") {
    // given
    val limit = 1
    val input = for (a <- 1 to sizeHint) yield Array[Any](a)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .apply()
      .|.limit(limit) // assuming this won't be fused
      .|.unwind("range(-a*10, -a*10 + 10) AS c")
      .|.unwind("range( a*10,  a*10 + 10) AS b")
      .|.argument("a")
      .input(variables = Seq("a"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input:_*))

    // then
    val expected = for {
      a <- 1 to sizeHint
      rhs = for {
        b <- ( a*10) to ( a*10 + 10)
        c <- (-a*10) to (-a*10 + 10)
      } yield (b, c)
      (b, c) <- rhs.take(limit)
    } yield Array[Any](a, b, c)

    runtimeResult should beColumns("a", "b", "c").withRows(expected)
    System.out.flush()
  }
}
