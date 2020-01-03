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
}
