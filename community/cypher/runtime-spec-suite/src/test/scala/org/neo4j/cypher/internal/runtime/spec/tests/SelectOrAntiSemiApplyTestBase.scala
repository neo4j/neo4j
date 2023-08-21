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

abstract class SelectOrAntiSemiApplyTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should only let through the one that matches when the expression is false") {
    // given
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false")
      .|.filter("x <> 1")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withSingleRow(1)
  }

  test("should not let anything through if rhs is non-empty and expression is false") {
    // given
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false")
      .|.filter("true")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should let everything through if rhs is empty and the expression is false") {
    // given
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false")
      .|.filter("false")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(inputRows)
  }

  test("if lhs is empty, rhs should not be touched regardless the given expression") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false")
      .|.filter("1/0 > 1")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then should not throw "/ by zero"
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should let pass the one satisfying the expression even if the rhs is empty") {
    // given
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("x = 1")
      .|.filter("true")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withSingleRow(1)
  }

  test("should let through the one that matches and the one satisfying the expression") {
    // given
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("x = 2")
      .|.filter("x <> 1")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(Seq(Array(1), Array(2)))
  }

  test("aggregation on lhs, non-empty rhs") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .selectOrAntiSemiApply("false")
      .|.filter("c % 2 = 0")
      .|.argument("c")
      .aggregation(Seq.empty, Seq("count(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("c").withNoRows()
  }

  test("aggregation on lhs, empty rhs") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .selectOrAntiSemiApply("false")
      .|.filter("c % 2 = 1")
      .|.argument("c")
      .aggregation(Seq.empty, Seq("count(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("c").withRows(Seq(Array[Any](0)))
  }

  test("empty cartesian on rhs") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false")
      .|.cartesianProduct()
      .|.|.allNodeScan("a", "x")
      .|.allNodeScan("b", "x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(inputRows)
  }

  test("non-empty cartesian on rhs") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false")
      .|.cartesianProduct()
      .|.|.argument("x")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withNoRows()
  }

  test("empty optional on rhs") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false")
      .|.optional("x")
      .|.allNodeScan("a", "x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withNoRows()
  }

  test("non-empty optional on rhs") {
    given { nodeGraph(sizeHint) }
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false")
      .|.optional("x")
      .|.allNodeScan("a", "x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withNoRows()
  }

  test("empty expand on rhs") {
    val nodes = given {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false")
      .|.expandAll("(x)-[r]->(y)")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x").withRows(nodes.map(Array[Any](_)))
  }

  test("non-empty expand on rhs") {
    given {
      circleGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false")
      .|.expandAll("(x)-[r]->(y)")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x").withNoRows()
  }

  test("limit 0 on rhs") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false")
      .|.limit(0)
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(inputRows)
  }

  test("limit 1 on rhs") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false")
      .|.limit(1)
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withNoRows()
  }

  test("semi-apply under apply") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.selectOrAntiSemiApply("false")
      .|.|.filter("false")
      .|.|.argument("x")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(inputRows)
  }

  test("nested semi-apply") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false")
      .|.filter("false")
      .|.selectOrAntiSemiApply("false")
      .|.|.filter("false")
      .|.|.argument("x")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(inputRows)
  }

  test("sort on rhs") {
    given {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false")
      .|.filter("false")
      .|.sort("y ASC")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x").withRows(rowCount(sizeHint))
  }

  test("top on rhs") {
    given {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false")
      .|.filter("false")
      .|.top(0, "x ASC")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x").withRows(rowCount(sizeHint))
  }

  test("should only let through rows that match, with RHS aggregation") {
    val inputRows = for {
      i <- 0 until sizeHint
    } yield Array[Any](i.toLong)

    val expectedValues = inputRows.filter(_(0).asInstanceOf[Long] % 2 != 0)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false")
      .|.filter("s % 2 = 0")
      .|.aggregation(Seq.empty, Seq("sum(x) AS s"))
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(expectedValues)
  }

  test("should only let through rows that match, with RHS grouping and aggregation") {
    val inputRows = for {
      i <- 0 until sizeHint
    } yield Array[Any](i.toLong)

    val expectedValues = inputRows.filter(_(0).asInstanceOf[Long] % 2 != 0)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false")
      .|.filter("s % 2 = 0")
      .|.aggregation(Seq("x AS x"), Seq("sum(x) AS s"))
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(expectedValues)
  }

  test("aggregation on lhs, non-empty rhs, with RHS aggregation") {
    val inputRows = for {
      i <- 0 until sizeHint
    } yield Array[Any](i.toLong)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .selectOrAntiSemiApply("false")
      .|.filter("s % 2 <> 0")
      .|.aggregation(Seq.empty, Seq("sum(c) AS s"))
      .|.argument("c")
      .aggregation(Seq.empty, Seq("count(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("c").withRows(Seq(Array(inputRows.size)))
  }

  test("aggregation on lhs, non-empty rhs, with RHS sort") {
    val inputRows = for {
      i <- 0 until sizeHint
    } yield Array[Any](i.toLong)

    val expectedValues = inputRows.filter(_(0).asInstanceOf[Long] % 2 == 0)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false")
      .|.filter("x % 2 <> 0")
      .|.sort("x ASC")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(expectedValues)
  }

  test("should handle cached properties in selectOrAntiSemiApply") {
    given {
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("cache[n.prop] AS prop")
      .selectOrAntiSemiApply("cache[n.prop] < 20")
      .|.expand("(n)-[r*]->(m)")
      .|.argument("n")
      .allNodeScan("n")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("prop").withRows((0 until sizeHint).map(Array[Any](_)))
  }

  test("should handle cached properties in selectOrAntiSemiApply, include start node") {
    given {
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("cache[n.prop] AS prop")
      .selectOrAntiSemiApply("cache[n.prop] < 20")
      .|.expand("(n)-[r*0..]->(m)")
      .|.argument("n")
      .allNodeScan("n")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("prop").withRows((0 until 20).map(Array[Any](_)))
  }

  test("limit after selectOrAntiSemiApply on the RHS of apply") {
    // given
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.exhaustiveLimit(1)
      .|.apply()
      .|.|.selectOrAntiSemiApply("i % 2 = 0")
      .|.|.|.filter("false")
      .|.|.|.argument("x", "i")
      .|.|.argument("x", "i")
      .|.unwind("[1, 2] AS i")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(singleColumn(inputRows.map(_(0))))
  }

}

trait OrderedSelectOrAntiSemiApplyTestBase[CONTEXT <: RuntimeContext] {
  self: SelectOrAntiSemiApplyTestBase[CONTEXT] =>

  test("should only let through the one that matches when the expression is false - with leveraged order") {
    // given
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false").withLeveragedOrder()
      .|.filter("x <> 1")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withSingleRow(1)
  }

  test("should not let anything through if rhs is non-empty and expression is false - with leveraged order") {
    // given
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false").withLeveragedOrder()
      .|.filter("true")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should let everything through if rhs is empty and the expression is false - with leveraged order") {
    // given
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false").withLeveragedOrder()
      .|.filter("false")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(inputRows)
  }

  test("if lhs is empty, rhs should not be touched regardless the given expression - with leveraged order") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false").withLeveragedOrder()
      .|.filter("1/0 > 1")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then should not throw "/ by zero"
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should let pass the one satisfying the expression even if the rhs is empty - with leveraged order") {
    // given
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("x = 1").withLeveragedOrder()
      .|.filter("true")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withSingleRow(1)
  }

  test("should let through the one that matches and the one satisfying the expression - with leveraged order") {
    // given
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("x = 2").withLeveragedOrder()
      .|.filter("x <> 1")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(Seq(Array(1), Array(2)))
  }

  test("aggregation on lhs, non-empty rhs - with leveraged order") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .selectOrAntiSemiApply("false").withLeveragedOrder()
      .|.filter("c % 2 = 0")
      .|.argument("c")
      .aggregation(Seq.empty, Seq("count(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("c").withNoRows()
  }

  test("aggregation on lhs, empty rhs - with leveraged order") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .selectOrAntiSemiApply("false").withLeveragedOrder()
      .|.filter("c % 2 = 1")
      .|.argument("c")
      .aggregation(Seq.empty, Seq("count(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("c").withRows(Seq(Array[Any](0)))
  }

  test("empty cartesian on rhs - with leveraged order") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false").withLeveragedOrder()
      .|.cartesianProduct()
      .|.|.allNodeScan("a", "x")
      .|.allNodeScan("b", "x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(inputRows)
  }

  test("non-empty cartesian on rhs - with leveraged order") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false").withLeveragedOrder()
      .|.cartesianProduct()
      .|.|.argument("x")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withNoRows()
  }

  test("empty optional on rhs - with leveraged order") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false").withLeveragedOrder()
      .|.optional("x")
      .|.allNodeScan("a", "x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withNoRows()
  }

  test("non-empty optional on rhs - with leveraged order") {
    given {
      nodeGraph(sizeHint)
    }
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false").withLeveragedOrder()
      .|.optional("x")
      .|.allNodeScan("a", "x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withNoRows()
  }

  test("empty expand on rhs - with leveraged order") {
    val nodes = given {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false").withLeveragedOrder()
      .|.expandAll("(x)-[r]->(y)")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x").withRows(nodes.map(Array[Any](_)))
  }

  test("non-empty expand on rhs - with leveraged order") {
    given {
      circleGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false").withLeveragedOrder()
      .|.expandAll("(x)-[r]->(y)")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x").withNoRows()
  }

  test("limit 0 on rhs - with leveraged order") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false").withLeveragedOrder()
      .|.limit(0)
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(inputRows)
  }

  test("limit 1 on rhs - with leveraged order") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false").withLeveragedOrder()
      .|.limit(1)
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withNoRows()
  }

  test("semi-apply under apply - with leveraged order") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.selectOrAntiSemiApply("false").withLeveragedOrder()
      .|.|.filter("false")
      .|.|.argument("x")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(inputRows)
  }

  test("nested semi-apply - with leveraged order") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false").withLeveragedOrder()
      .|.filter("false")
      .|.selectOrAntiSemiApply("false").withLeveragedOrder()
      .|.|.filter("false")
      .|.|.argument("x")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(inputRows)
  }

  test("sort on rhs - with leveraged order") {
    given {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false").withLeveragedOrder()
      .|.filter("false")
      .|.sort("y ASC")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x").withRows(rowCount(sizeHint))
  }

  test("top on rhs - with leveraged order") {
    given {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false").withLeveragedOrder()
      .|.filter("false")
      .|.top(0, "x ASC")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x").withRows(rowCount(sizeHint))
  }

  test("should only let through rows that match, with RHS aggregation - with leveraged order") {
    val inputRows = for {
      i <- 0 until sizeHint
    } yield Array[Any](i.toLong)

    val expectedValues = inputRows.filter(_(0).asInstanceOf[Long] % 2 != 0)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false").withLeveragedOrder()
      .|.filter("s % 2 = 0")
      .|.aggregation(Seq.empty, Seq("sum(x) AS s"))
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(expectedValues)
  }

  test("should only let through rows that match, with RHS grouping and aggregation - with leveraged order") {
    val inputRows = for {
      i <- 0 until sizeHint
    } yield Array[Any](i.toLong)

    val expectedValues = inputRows.filter(_(0).asInstanceOf[Long] % 2 != 0)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false").withLeveragedOrder()
      .|.filter("s % 2 = 0")
      .|.aggregation(Seq("x AS x"), Seq("sum(x) AS s"))
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(expectedValues)
  }

  test("aggregation on lhs, non-empty rhs, with RHS aggregation - with leveraged order") {
    val inputRows = for {
      i <- 0 until sizeHint
    } yield Array[Any](i.toLong)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .selectOrAntiSemiApply("false").withLeveragedOrder()
      .|.filter("s % 2 <> 0")
      .|.aggregation(Seq.empty, Seq("sum(c) AS s"))
      .|.argument("c")
      .aggregation(Seq.empty, Seq("count(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("c").withRows(Seq(Array(inputRows.size)))
  }

  test("aggregation on lhs, non-empty rhs, with RHS sort - with leveraged order") {
    val inputRows = for {
      i <- 0 until sizeHint
    } yield Array[Any](i.toLong)

    val expectedValues = inputRows.filter(_(0).asInstanceOf[Long] % 2 == 0)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .selectOrAntiSemiApply("false").withLeveragedOrder()
      .|.filter("x % 2 <> 0")
      .|.sort("x ASC")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(expectedValues)
  }

  test("should handle cached properties in selectOrAntiSemiApply - with leveraged order") {
    given {
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("cache[n.prop] AS prop")
      .selectOrAntiSemiApply("cache[n.prop] < 20").withLeveragedOrder()
      .|.expand("(n)-[r*]->(m)")
      .|.argument("n")
      .allNodeScan("n")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("prop").withRows((0 until sizeHint).map(Array[Any](_)))
  }

  test("should handle cached properties in selectOrAntiSemiApply, include start node - with leveraged order") {
    given {
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .projection("cache[n.prop] AS prop")
      .selectOrAntiSemiApply("cache[n.prop] < 20").withLeveragedOrder()
      .|.expand("(n)-[r*0..]->(m)")
      .|.argument("n")
      .allNodeScan("n")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("prop").withRows((0 until 20).map(Array[Any](_)))
  }

  test("limit after selectOrAntiSemiApply on the RHS of apply - with leveraged order") {
    // given
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.exhaustiveLimit(1)
      .|.apply()
      .|.|.selectOrAntiSemiApply("i % 2 = 0").withLeveragedOrder()
      .|.|.|.filter("false")
      .|.|.|.argument("x", "i")
      .|.|.argument("x", "i")
      .|.unwind("[1, 2] AS i")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(singleColumn(inputRows.map(_(0))))
  }

}
