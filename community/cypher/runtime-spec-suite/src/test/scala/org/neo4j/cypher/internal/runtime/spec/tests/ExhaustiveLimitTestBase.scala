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
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.exceptions.ArithmeticException
import org.neo4j.exceptions.InvalidArgumentException

abstract class ExhaustiveLimitTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("exhaustive limit 0") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .exhaustiveLimit(0)
      .input(variables = Seq("x"))
      .build()

    val input = inputColumns(sizeHint, 3, identity).stream()

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)
    runtimeResult should beColumns("x").withNoRows()

    input.hasMore should be(false)
  }

  test("exhaustive limit -1") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .exhaustiveLimit(-1)
      .input(variables = Seq("x"))
      .build()

    val input = inputColumns(sizeHint, 3, identity).stream()

    // then
    val exception = intercept[InvalidArgumentException] {
      consume(execute(logicalQuery, runtime, input))
    }
    exception.getMessage should include("Must be a non-negative integer")
  }

  test("exhaustive limit -1 on an empty input") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .exhaustiveLimit(-1)
      .input(variables = Seq("x"))
      .build()

    val input = inputValues()

    // then
    val exception = intercept[InvalidArgumentException] {
      consume(execute(logicalQuery, runtime, input))
    }
    exception.getMessage should include("Must be a non-negative integer")
  }

  test("exhaustive limit higher than amount of rows") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .exhaustiveLimit(Int.MaxValue)
      .input(variables = Seq("x"))
      .build()

    val input = inputColumns(sizeHint, 3, identity)
    val stream = input.stream()

    // then
    val runtimeResult = execute(logicalQuery, runtime, stream)
    runtimeResult should beColumns("x").withRows(input.flatten)
    stream.hasMore should be(false)
  }

  test("should support exhaustive limit") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .exhaustiveLimit(10)
      .input(variables = Seq("x"))
      .build()

    val input = inputColumns(sizeHint, 3, identity).stream()

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)
    runtimeResult should beColumns("x").withRows(rowCount(10))

    input.hasMore should be(false)
  }

  test("should support exhaustive limit with null values") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .exhaustiveLimit(10)
      .input(variables = Seq("x"))
      .build()

    val input = inputColumns(sizeHint, 3, x => if (x % 2 == 0) x else null).stream()

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)
    runtimeResult should beColumns("x")
      .withRows(rowCount(10))

    input.hasMore should be(false)
  }

  test("should support exhaustive limit in the first of two pipelines") {
    // given
    val nodesPerLabel = 100
    val (aNodes, _) = given {
      bipartiteGraph(nodesPerLabel, "A", "B", "R")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandAll("(x)-->(y)")
      .exhaustiveLimit(9)
      .input(nodes = Seq("x"))
      .build()

    val input = inputValues(aNodes.map(n => Array[Any](n)): _*)
    val stream = input.stream()

    // then
    val runtimeResult = execute(logicalQuery, runtime, stream)
    runtimeResult should beColumns("x", "y")
      .withRows(rowCount(900))

    stream.hasMore should be(false)
  }

  test("should support apply-exhaustive limit") {
    // given
    val nodesPerLabel = 100
    given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.exhaustiveLimit(0)
      .|.filter("1/0 > 1")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    // then
    an[ArithmeticException] should be thrownBy consume(execute(logicalQuery, runtime))
  }

  test("should support exhaustive exhaustive limit on top of apply") {
    // given
    val nodesPerLabel = 50
    val (aNodes, _) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }
    val limit = nodesPerLabel * nodesPerLabel - 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .exhaustiveLimit(limit)
      .apply()
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .input(nodes = Seq("x"))
      .build()

    val input = inputValues(aNodes.map(n => Array[Any](n)): _*)
    val stream = input.stream()

    // then
    val runtimeResult = execute(logicalQuery, runtime, stream)

    runtimeResult should beColumns("x").withRows(rowCount(limit))
    stream.hasMore should be(false)
  }
}
