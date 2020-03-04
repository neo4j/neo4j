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

import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}

abstract class DistinctTestBase[CONTEXT <: RuntimeContext](
                                                            edition: Edition[CONTEXT],
                                                            runtime: CypherRuntime[CONTEXT],
                                                            sizeHint: Int
                                                          ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should work on distinct input with no projection, one column") {
    // given
    val input = inputValues((0 until sizeHint).map(Array[Any](_)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .distinct("x AS x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x").withRows(input.flatten)
  }

  test("should work on input with no projection, one column") {
    // given
    val input = inputValues((0 until sizeHint).map(i => Array[Any](i % 10)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .distinct("x AS x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(0 until 10))
  }

  test("should work on input with projection, one column") {
    // given
    val input = inputValues((0 until sizeHint).map(i => Array[Any](i % 10)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("2 * x AS y")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("y").withRows(singleColumn(0 until 20 by 2))
  }

  test("should work on input with projection, two columns") {
    // given
    val input = inputValues((0 until sizeHint).map(i => Array[Any](i % 10, 100 + (i % 5))): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x1", "y1")
      .distinct("2 * x AS x1", "1 + y AS y1")
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val s: Iterable[Array[_]] = (0 until 10).map(i => Array[Any](i * 2, 101 + (i % 5)))
    runtimeResult should beColumns("x1", "y1").withRows(s)
  }

  test("should return no rows for empty input") {
    // given
    val input = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x1", "y1")
      .distinct("2 * x AS x1", "1 + y AS y1")
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x1", "y1").withNoRows()
  }

  test("should keep input order") {
    // given
    // (This is a sawtooth pattern)
    val f: Int => Int = i => i - ((i / 10) * 10) / 2
    val input = inputColumns(10, sizeHint / 10, f)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .distinct("x AS x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x")
      .withRows(inOrder((0 until sizeHint).map(f).distinct.sorted.map(Array[Any](_))))
  }

  test("should work on RHS of apply") {

    val as = Seq(10,11,12,12,12)
    val bs = Seq(1,1,2,3,4,4,5)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("b")
      .apply()
      .|.distinct("b AS b")
      .|.unwind(s"[${bs.mkString(",")}] AS b")
      .|.argument()
      .unwind(s"[${as.mkString(",")}] AS a")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {a <- as; b <- bs.distinct} yield b
    runtimeResult should beColumns("b").withRows(singleColumn(expected))
  }

  test("should support distinct on top of apply") {
    // given
    val nodesPerLabel = 50
    val (aNodes, _) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .distinct("x AS x")
      .apply()
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("x").withRows(aNodes.map(a => Array(a)))
  }

  test("should support filter after a distinct") {
    // given
    val n = sizeHint
    val relTuples = (for(i <- 0 until n) yield {
      Seq((i, (i + 1) % n, "NEXT"))
    }).reduce(_ ++ _)
    given {
      //prop = [0, 0, 1, 1, 2, 2,..]
      val nodes = nodePropertyGraph(n, { case i => Map("prop" -> i / 2) })
      connect(nodes, relTuples)
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("yprop")
      .projection("y.prop AS yprop")
      .expandAll("(x)-->(y)")
      .filter("xprop = 11")
      .distinct("x.prop AS xprop")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("yprop").withRows(rowCount(1))
  }
}
