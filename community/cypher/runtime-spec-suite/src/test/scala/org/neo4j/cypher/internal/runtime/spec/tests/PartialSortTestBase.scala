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

import org.neo4j.cypher.internal.logical.plans.{Ascending, Descending}
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}

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
    val input = inputValues(
      Array(3, 1),
      Array(3, 3),
      Array(3, 2)
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialSort(Seq(Ascending("x")), Seq(Ascending("y")))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(Seq(
      Array(3, 1),
      Array(3, 2),
      Array(3, 3)
    )))
  }

  test("partial sort with one column already sorted") {
    // when
    val input = inputValues(
      Array(3, 1),
      Array(3, 3),
      Array(3, 2),
      Array(5, 9),
      Array(5, 9),
      Array(5, 0),
      Array(5, 7)
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialSort(Seq(Ascending("x")), Seq(Ascending("y")))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(Seq(
      Array(3, 1),
      Array(3, 2),
      Array(3, 3),
      Array(5, 0),
      Array(5, 7),
      Array(5, 9),
      Array(5, 9)
    )))
  }

  test("partial sort with chunk size 1") {
    // when
    val input = inputValues(
      Array(1, 1),
      Array(2, 3),
      Array(3, 2),
      Array(4, 9),
      Array(5, 9),
      Array(6, 0),
      Array(7, 7)
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialSort(Seq(Ascending("x")), Seq(Ascending("y")))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(Seq(
      Array(1, 1),
      Array(2, 3),
      Array(3, 2),
      Array(4, 9),
      Array(5, 9),
      Array(6, 0),
      Array(7, 7)
    )))
  }

  test("partial sort with two sorted and two unsorted columns") {
    // when
    val input = inputValues(
      Array(3, 3, 0, 1),
      Array(3, 1, 9, 2),
      Array(3, 1, 3, 3),
      Array(5, 5, 4, 0),
      Array(5, 5, 2, 0),
      Array(5, 5, 2, 6),
      Array(5, 0, 2, 0)
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z", "a")
      .partialSort(Seq(Ascending("x"), Descending("y")), Seq(Ascending("z"), Descending("a")))
      .input(variables = Seq("x", "y", "z", "a"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y", "z", "a").withRows(inOrder(Seq(
      Array(3, 3, 0, 1),
      Array(3, 1, 3, 3),
      Array(3, 1, 9, 2),
      Array(5, 5, 2, 6),
      Array(5, 5, 2, 0),
      Array(5, 5, 4, 0),
      Array(5, 0, 2, 0)
    )))
  }

  test("should handle null values") {
    // when
    val input = inputValues(
      Array(3, 1),
      Array(3, null),
      Array(3, 2),
      Array(5, null),
      Array(5, 9),
      Array(null, 0),
      Array(null, null)
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .partialSort(Seq(Ascending("x")), Seq(Ascending("y")))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withRows(inOrder(Seq(
      Array(3, 1),
      Array(3, 2),
      Array(3, null),
      Array(5, 9),
      Array(5, null),
      Array(null, 0),
      Array(null, null)
    )))
  }
}
