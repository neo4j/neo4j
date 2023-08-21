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

abstract class AntiSemiApplyTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("empty lhs should produce no rows") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.unwind("[1,2,3] AS y")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x").withNoRows()
  }

  test("non-empty lhs, empty rhs, should produce all lhs rows") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.filter("false")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(inputRows)
  }

  test("non-empty lhs, non-empty rhs should no lhs") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should only let through rows that do not match") {
    val inputRows = for {
      i <- 0 until sizeHint
    } yield Array[Any](i.toLong)

    val expectedRows = for {
      i <- 0 until sizeHint if i % 2 == 1
    } yield Array[Any](i.toLong)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.filter("x % 2 = 0")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(expectedRows)
  }

  test("aggregation on lhs, empty rhs") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .antiSemiApply()
      .|.filter("false")
      .|.argument("c")
      .aggregation(Seq.empty, Seq("count(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("c").withRows(Seq(Array[Any](0)))
  }

  test("aggregation on lhs, non-empty rhs") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .antiSemiApply()
      .|.argument("c")
      .aggregation(Seq.empty, Seq("count(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("c").withNoRows()
  }

  test("non-empty cartesian on rhs") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.cartesianProduct()
      .|.|.argument("x")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withNoRows()
  }

  test("empty cartesian on rhs") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.cartesianProduct()
      .|.|.argument("x")
      .|.filter("false")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(inputRows)
  }

  test("empty optional on rhs") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.optional("x")
      .|.allNodeScan("a", "x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withNoRows()
  }

  test("non-empty optional on rhs") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.optional("x")
      .|.allNodeScan("a", "x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withNoRows()
  }

  test("non-empty expand on rhs") {
    given {
      circleGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.expandAll("(x)-[r]->(y)")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x").withNoRows()
  }

  test("empty expand on rhs") {
    given {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.expandAll("(x)-[r]->(y)")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x").withRows(rowCount(sizeHint))
  }

  test("limit 1 on rhs") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.limit(1)
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withNoRows()
  }

  test("limit 0 on rhs") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.limit(0)
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(inputRows)
  }

  test("non-empty distinct on rhs") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.distinct("y AS y")
      .|.unwind("[1,2,3] AS y")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withNoRows()
  }

  test("empty distinct on rhs") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.distinct("y AS y")
      .|.unwind("[] AS y")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(inputRows)
  }

  test("anti-semi-apply under apply") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.antiSemiApply()
      .|.|.filter("false")
      .|.|.argument("x")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(inputRows)
  }

  test("nested anti-semi-apply") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.filter("false")
      .|.antiSemiApply()
      .|.|.filter("false")
      .|.|.argument("x")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(inputRows)
  }

  test("empty sort on rhs") {
    given {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.limit(0)
      .|.sort("y ASC")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x").withRows(rowCount(sizeHint))
  }

  test("non-empty sort on rhs") {
    given {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.sort("y ASC")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x").withNoRows()
  }

  test("empty top on rhs") {
    given {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.limit(0)
      .|.top(0, "x ASC")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x").withRows(rowCount(sizeHint))
  }

  test("non-empty top on rhs") {
    given {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.top(10, "x ASC")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should only let through rows that do not match, with RHS aggregation") {
    val inputRows = for {
      i <- 0 until sizeHint
    } yield Array[Any](i.toLong)

    val expectedValues = inputRows.filter(_(0).asInstanceOf[Long] % 2 == 1)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.filter("s % 2 = 0")
      .|.aggregation(Seq.empty, Seq("sum(x) AS s"))
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(expectedValues)
  }

  test("should only let through rows that do not match, with RHS grouping and aggregation") {
    val inputRows = for {
      i <- 0 until sizeHint
    } yield Array[Any](i.toLong)

    val expectedValues = inputRows.filter(_(0).asInstanceOf[Long] % 2 == 1)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.filter("s % 2 = 0")
      .|.aggregation(Seq("x AS x"), Seq("sum(x) AS s"))
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(expectedValues)
  }

  test("aggregation on lhs, empty rhs, with RHS aggregation") {
    val inputRows = for {
      i <- 0 until sizeHint
    } yield Array[Any](i.toLong)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .antiSemiApply()
      .|.filter("false")
      .|.aggregation(Seq.empty, Seq("sum(c) AS s"))
      .|.argument("c")
      .aggregation(Seq.empty, Seq("count(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("c").withRows(Seq(Array(inputRows.size)))
  }

  test("single row rhs, aggregation on top") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("s")
      .aggregation(Seq.empty, Seq("count(*) AS s"))
      .antiSemiApply()
      .|.argument()
      .argument()
      .build()

    // then
    val result = execute(logicalQuery, runtime)
    result should beColumns("s").withSingleRow(0)
  }

  test("with column introduced after apply") {
    // flaky
    val (nodes, _) = given {
      circleGraph(sizeHint)
    }
    val nodeId = nodes.head.getId

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "extra")
      .projection("1 AS extra")
      .antiSemiApply()
      .|.nodeByIdSeek("n", Set("x"), nodeId)
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x", "extra").withNoRows()
  }

  test("single row (of many filtered out) rhs, sort on top") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort("x ASC")
      .antiSemiApply()
      .|.filter(s"NOT x=${sizeHint / 2}")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withSingleRow(sizeHint / 2)
  }
}
