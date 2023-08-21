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
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

abstract class FilterTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should filter with (cached) IN expression") {
    // given
    given {
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("list" -> Array(1, i), "key" -> 1)
        }
      )
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .optionalExpandAll("(n)--(y)") // break pipeline (to force parallelism)
      .nonFuseable() // break pipeline, even with fusing across pipelines (to force parallelism)
      .filter("n.key IN [1]")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expectedRowCount = sizeHint
    runtimeResult should beColumns("n").withRows(rowCount(expectedRowCount))
  }

  test("should filter by one predicate") {
    // given
    val input = inputValues((0 until sizeHint).map(Array[Any](_)): _*)
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i")
      .filter(s"i >= ${sizeHint / 2}")
      .input(variables = Seq("i"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = (0 until sizeHint).filter(i => i >= sizeHint / 2)
    runtimeResult should beColumns("i").withRows(singleColumn(expected))
  }

  test("should filter by multiple predicate") {
    // given
    val input = inputValues((0 until sizeHint).map(Array[Any](_)): _*)
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i")
      .filter(s"i >= ${sizeHint / 2}", "i % 2 = 0")
      .input(variables = Seq("i"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = (0 until sizeHint).filter(i => i >= sizeHint / 2 && i % 2 == 0)
    runtimeResult should beColumns("i").withRows(singleColumn(expected))
  }

  test("should work on empty input") {
    // given
    val input = inputValues()
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i")
      .filter(s"i >= ${sizeHint / 2}", "i % 2 = 0")
      .input(variables = Seq("i"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("i").withNoRows()
  }

  test("should filter on cached property predicate") {
    // given
    given {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("prop" -> i)
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("cache[n.prop] AS prop")
      .filter(s"cache[n.prop] < ${sizeHint / 2}")
      .cacheProperties("cache[n.prop]")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = 0 until sizeHint / 2
    runtimeResult should beColumns("prop").withRows(singleColumn(expected))
  }

  test("should handle filter + limit on the RHS of an apply") {
    given(nodeGraph(sizeHint))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .apply()
      .|.limit(5)
      .|.filter("id(x) % 2 = 0")
      .|.allNodeScan("x")
      .input(variables = Seq("y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues((1 to 5).map(i => Array[Any](i)): _*))

    // then
    runtimeResult should beColumns("y").withRows(rowCount(25))
  }
}
