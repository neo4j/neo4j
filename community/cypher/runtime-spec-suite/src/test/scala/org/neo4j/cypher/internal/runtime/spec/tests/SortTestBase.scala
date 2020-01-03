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

abstract class SortTestBase[CONTEXT <: RuntimeContext](
                                                        edition: Edition[CONTEXT],
                                                        runtime: CypherRuntime[CONTEXT],
                                                        sizeHint: Int
                                                      ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("empty input gives empty output") {
    // when
    val input = inputValues()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .sort(Seq(Ascending("x"), Ascending("y")))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("should handle null values, one column") {
    val unfilteredNodes = given { nodeGraph(sizeHint) }
    val nodes = select(unfilteredNodes, nullProbability = 0.52)
    val input = inputValues(nodes.map(n => Array[Any](n)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort(Seq(Ascending("x")))
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = nodes.sortBy(n => if (n == null) Long.MaxValue else n.getId)
    runtimeResult should beColumns("x").withRows(inOrder(expected.map(x => Array[Any](x))))
  }

  test("should sort many columns") {
    val input = inputValues((0 until sizeHint).map(i => Array[Any](i % 2, i % 4, i)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .sort(Seq(Descending("a"), Ascending("b"), Descending("c")))
      .input(variables = Seq("a", "b", "c"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = input.flatten.sortBy(arr => (-arr(0).asInstanceOf[Int], arr(1).asInstanceOf[Int], -arr(2).asInstanceOf[Int]))
    runtimeResult should beColumns("a", "b", "c").withRows(inOrder(expected))
  }

  test("should sort under apply") {
    val nodesPerLabel = 100
    val (aNodes, bNodes) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .apply()
      .|.sort(Seq(Ascending("b")))
      .|.expandAll("(a)-->(b)")
      .|.argument("a")
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for{
      x <- aNodes
      y <- bNodes.sortBy(_.getId)
    } yield Array[Any](x, y)

    runtimeResult should beColumns("a", "b").withRows(expected)
  }

  test("should sort twice in a row") {
    // given
    val nodes = given { nodeGraph(1000) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort(sortItems = Seq(Ascending("x")))
      .sort(sortItems = Seq(Descending("x")))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("x").withRows(singleColumnInOrder(nodes))
  }

  test("should sort on top of apply with all node scan and sort on rhs of apply") {
    // given
    val nodes = given { nodeGraph(10) }
    val inputRows = inputValues(nodes.map(node => Array[Any](node)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort(sortItems = Seq(Descending("x")))
      .apply()
      .|.sort(sortItems = Seq(Descending("x")))
      .|.allNodeScan("x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputRows)

    runtimeResult should beColumns("x").withRows(rowCount(100))
  }

  test("should sort on top of apply") {
    given { circleGraph(1000) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .sort(Seq(Descending("y")))
      .apply()
      .|.expandAll("(x)--(y)")
      .|.argument()
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y").withRows(sortedDesc("y"))
  }

  test("should apply apply sort") {
    given { circleGraph(1000) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .apply()
      .|.apply()
      .|.|.sort(Seq(Ascending("z")))
      .|.|.expandAll("(y)--(z)")
      .|.|.argument()
      .|.sort(Seq(Descending("y")))
      .|.expandAll("(x)--(y)")
      .|.argument()
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "z").withRows(groupedBy("x", "y").asc("z"))
  }

  test("should handle sort after distinct removed rows (FilteringExecutionContext and cancelled rows) I") {
    //this test is mostly testing pipelined runtime and assumes a morsel/batch size of 4
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("A")
      .sort(Seq(Ascending("A")))
      .distinct("x AS A")
      .input(variables = Seq("x"))
      .build()

    // when (one element in each batch will be cancelled)
    val runtimeResult = execute(logicalQuery, runtime,
                                inputValues(Array(4), Array(2), Array(4), Array(1), Array(3), Array(5), Array(3)))

    //then
    runtimeResult should beColumns("A").withRows(List(Array(1), Array(2), Array(3), Array(4), Array(5)))
  }

  test("should handle sort after distinct removed rows (FilteringExecutionContext and cancelled rows) II") {
    //this test is mostly testing pipelined runtime and assumes a morsel/batch size of 4
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("A")
      .sort(Seq(Ascending("B")))
      .projection("A AS A", "A AS B")
      .distinct("x AS A")
      .input(variables = Seq("x"))
      .build()

    //when (the first element of the second batch will be cancelled)
    val runtimeResult = execute(logicalQuery, runtime,
                                inputValues(Array(1), Array(2), Array(3), Array(4), Array(4), Array(5), Array(6)))

    //then
    runtimeResult should beColumns("A").withRows(List(Array(1), Array(2), Array(3), Array(4), Array(5), Array(6)))
  }
}
