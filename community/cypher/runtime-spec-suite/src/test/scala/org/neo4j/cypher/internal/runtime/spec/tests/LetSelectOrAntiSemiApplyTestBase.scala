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
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

abstract class LetSelectOrAntiSemiApplyTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                                           runtime: CypherRuntime[CONTEXT],
                                                                           val sizeHint: Int
                                                                      ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("if predicate is false, idName should only be true when RHS is empty") {
    //given
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    //when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "idName")
      .letSelectOrAntiSemiApply("idName", "false")
      .|.filter("x % 3 = 1")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    //then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    val expectedValues = (0 until sizeHint).map(i => Array(i, i % 3 != 1))
    runtimeResult should beColumns("x", "idName").withRows(expectedValues)
  }

  test("idName should always be true if rhs is empty and predicate is false") {
    //given
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    //when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "idName")
      .letSelectOrAntiSemiApply("idName", "false")
      .|.filter("false")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    //then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    val expectedValues = (0 until sizeHint).map(i => Array(i, true))
    runtimeResult should beColumns("x", "idName").withRows(expectedValues)
  }

  test("idName should always be false if rhs is nonEmpty and the predicate is false") {
    //given
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    //when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "idName")
      .letSelectOrAntiSemiApply("idName", "false")
      .|.filter("true")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    //then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    val expectedValues = (0 until sizeHint).map(i => Array(i, false))
    runtimeResult should beColumns("x", "idName").withRows(expectedValues)
  }

  test("if lhs is empty, rhs should not be touched regardless the given predicate") {
    //when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "idName")
      .letSelectOrAntiSemiApply("idName", "false")
      .|.filter("1/0 > 1")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    //then should not throw "/ by zero"
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x", "idName").withNoRows()
  }

  test("if the predicate is true, then rhs should not be touched") {
    //given
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    //when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .letSelectOrAntiSemiApply("idName", "true")
      .|.filter("1/0 > 1")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    //then should not throw "/ by zero"
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(inputRows)
  }

  test("idName should be true for the row which are satisfying the predicate even if the rhs is non-empty") {
    //given
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    //when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "idName")
      .letSelectOrAntiSemiApply("idName", "x < 11")
      .|.filter("true")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    //then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    val expectedValues = (0 until sizeHint).map(i => Array(i, i < 11L))
    runtimeResult should beColumns("x", "idName").withRows(expectedValues)
  }

  test("idName should be true for rows satisfying the predicate or where rhs is empty") {
    //given
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    //when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "idName")
      .letSelectOrAntiSemiApply("idName", "x = 12")
      .|.filter("x < 20")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    //then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    val expectedValues = (0 until sizeHint).map(i => Array(i, i >= 20L || i == 12L))
    runtimeResult should beColumns("x", "idName").withRows(expectedValues)
  }

  test("aggregation on lhs, non-empty rhs") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c", "idName")
      .letSelectOrAntiSemiApply("idName", "false")
      .|.filter("c % 2 = 0")
      .|.argument("c")
      .aggregation(Seq.empty, Seq("count(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, NO_INPUT)
    runtimeResult should beColumns("c", "idName").withSingleRow(0, false)
  }

  test("aggregation on lhs, empty rhs") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c", "idName")
      .letSelectOrAntiSemiApply("idName", "false")
      .|.filter("c % 2 = 1")
      .|.argument("c")
      .aggregation(Seq.empty, Seq("count(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, NO_INPUT)
    runtimeResult should beColumns("c", "idName").withSingleRow(0L, true)
  }

  test("empty cartesian on rhs") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "idName")
      .letSelectOrAntiSemiApply("idName", "false")
      .|.cartesianProduct()
      .|.|.allNodeScan("a", "x")
      .|.allNodeScan("b", "x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    val expectedValues = (0 until sizeHint).map(i => Array(i, true))
    runtimeResult should beColumns("x", "idName").withRows(expectedValues)
  }

  test("non-empty cartesian on rhs") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "idName")
      .letSelectOrAntiSemiApply("idName", "false")
      .|.cartesianProduct()
      .|.|.argument("x")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    val expectedValues = (0 until sizeHint).map(i => Array(i, false))
    runtimeResult should beColumns("x", "idName").withRows(expectedValues)
  }

  test("empty optional on rhs") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "idName")
      .letSelectOrAntiSemiApply("idName", "false")
      .|.optional("x")
      .|.allNodeScan("a", "x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    val expectedValues = (0 until sizeHint).map(i => Array(i, false))
    runtimeResult should beColumns("x", "idName").withRows(expectedValues)
  }

  test("non-empty optional on rhs") {
    given {
      nodeGraph(sizeHint)
    }
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "idName")
      .letSelectOrAntiSemiApply("idName", "false")
      .|.optional("x")
      .|.allNodeScan("a", "x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    val expectedValues = (0 until sizeHint).map(i => Array(i, false))
    runtimeResult should beColumns("x", "idName").withRows(expectedValues)
  }

  test("limit 0 on rhs") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "idName")
      .letSelectOrAntiSemiApply("idName", "false")
      .|.limit(0)
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    val expectedValues = (0 until sizeHint).map(i => Array(i, true))
    runtimeResult should beColumns("x", "idName").withRows(expectedValues)
  }

  test("limit 1 on rhs") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "idName")
      .letSelectOrAntiSemiApply("idName", "false")
      .|.limit(1)
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    val expectedValues = (0 until sizeHint).map(i => Array(i, false))
    runtimeResult should beColumns("x", "idName").withRows(expectedValues)
  }

  test("letSelectOrAntiSemiApply under apply") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "idName")
      .apply()
      .|.letSelectOrAntiSemiApply("idName", "false")
      .|.|.argument("x")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    val expectedValues = (0 until sizeHint).map(i => Array(i, false))
    runtimeResult should beColumns("x", "idName").withRows(expectedValues)
  }

  test("nested letSelectOrAntiSemiApply") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "idName")
      .letSelectOrAntiSemiApply("idName", "false")
      .|.letSelectOrAntiSemiApply("idName", "false")
      .|.|.argument("x")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    val expectedValues = (0 until sizeHint).map(i => Array(i, false))
    runtimeResult should beColumns("x", "idName").withRows(expectedValues)
  }

  test("sort on rhs") {
    val nodes = given {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "idName")
      .letSelectOrAntiSemiApply("idName", "false")
      .|.sort(Seq(Ascending("y")))
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x", "idName").withRows(nodes.map(n => Array[Any](n, false)))
  }

  test("top on rhs") {
    val nodes = given {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "idName")
      .letSelectOrAntiSemiApply("idName", "false")
      .|.top(Seq(Ascending("x")), 10)
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x", "idName").withRows(nodes.map(n => Array[Any](n, false)))
  }

  test("should only have idName equals true for rows that match with RHS aggregation") {
    val inputRows = for {
      i <- 0 until sizeHint
    } yield Array[Any](i.toLong)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "idName")
      .letSelectOrAntiSemiApply("idName", "false")
      .|.filter("s % 2 = 0")
      .|.aggregation(Seq.empty, Seq("sum(x) AS s"))
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    val expectedValues = (0 until sizeHint).map(i => Array(i, i % 2 != 0))
    runtimeResult should beColumns("x", "idName").withRows(expectedValues)
  }

  test("idName should only be true for rows that match with RHS grouping and aggregation") {
    val inputRows = for {
      i <- 0 until sizeHint
    } yield Array[Any](i.toLong)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "idName")
      .letSelectOrAntiSemiApply("idName", "false")
      .|.filter("s % 2 = 0")
      .|.aggregation(Seq("x AS x"), Seq("sum(x) AS s"))
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    val expectedValues = (0 until sizeHint).map(i => Array(i, i % 2 != 0))
    runtimeResult should beColumns("x", "idName").withRows(expectedValues)
  }

  test("aggregation on lhs, non-empty rhs, with RHS aggregation") {
    val inputRows = for {
      i <- 0 until sizeHint
    } yield Array[Any](i.toLong)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c", "idName")
      .letSelectOrAntiSemiApply("idName", "false")
      .|.filter("s % 2 = 0")
      .|.aggregation(Seq.empty, Seq("sum(c) AS s"))
      .|.argument("c")
      .aggregation(Seq.empty, Seq("count(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("c", "idName").withSingleRow(inputRows.size, false)
  }

  test("aggregation on lhs, non-empty rhs, with RHS sort") {
    val inputRows = for {
      i <- 0 until sizeHint
    } yield Array[Any](i.toLong)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "idName")
      .letSelectOrAntiSemiApply("idName", "false")
      .|.filter("x % 2 = 0")
      .|.sort(Seq(Ascending("x")))
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    val expectedValues = (0 until sizeHint).map(i => Array(i, i % 2 != 0))
    runtimeResult should beColumns("x", "idName").withRows(expectedValues)
  }
}
