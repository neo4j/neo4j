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
import org.neo4j.graphdb.Node

abstract class OrderedDistinctTestBase[CONTEXT <: RuntimeContext](
                                                                      edition: Edition[CONTEXT],
                                                                      runtime: CypherRuntime[CONTEXT],
                                                                      sizeHint: Int
                                                                    ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should work on distinct input with no projection, one column, sorted") {
    // given
    val input = inputValues((0 until sizeHint).map(Array[Any](_)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .orderedDistinct(Seq(varFor("x")), "x AS x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x").withRows(input.flatten)
  }

  test("should work on input with no projection, one column, sorted") {
    // given
    val input = inputValues((0 until sizeHint).map(i => Array[Any](i % 10)).sortBy(_.head.asInstanceOf[Int]): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .orderedDistinct(Seq(varFor("x")), "x AS x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(0 until 10))
  }

  test("should work on input with no projection, one primitive column, sorted") {
    // given
    val nodes = given { nodeGraph(10) }
    val input = inputValues((0 until sizeHint).map(i => Array[Any](nodes(i % 10))).sortBy(_.head.asInstanceOf[Node].getId): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .orderedDistinct(Seq(varFor("x")), "x AS x")
      .input(nodes = Seq("x"), nullable = false)
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(nodes))
  }

  test("should work on input with projection, one column, sorted") {
    // given
    val input = inputValues((0 until sizeHint).map(i => Array[Any](i % 10)).sortBy(_.head.asInstanceOf[Int]): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .orderedDistinct(Seq(varFor("x")), "x AS y")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("y").withRows(singleColumn(0 until 10))
  }

  test("should work on input with projection, two columns, one sorted") {
    // given
    val input = inputValues((0 until sizeHint).map(i => Array[Any](i % 5, 100 + (i % 10))).sortBy(_.head.asInstanceOf[Int]): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x1", "y1")
      .orderedDistinct(Seq(varFor("x")),"x AS x1", "1 + y AS y1")
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val s: Iterable[Array[_]] = (0 until 10).map(i => Array[Any](i % 5, 101 + (i % 10)))
    runtimeResult should beColumns("x1", "y1").withRows(s)
  }

  test("should work on input with projection, two columns, both sorted") {
    // given
    val input = inputValues((0 until sizeHint).map(i => Array[Any](i % 5, 100 + (i % 10))).sortBy(a => (a(0).asInstanceOf[Int], a(1).asInstanceOf[Int])): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x1", "y1")
      .orderedDistinct(Seq(varFor("x"),varFor("y")),"x AS x1", "y AS y1")
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val s: Iterable[Array[_]] = (0 until 10).map(i => Array[Any](i % 5, 100 + (i % 10)))
    runtimeResult should beColumns("x1", "y1").withRows(s)
  }

  test("should work on input with projection, two primitive columns, one sorted") {
    // given
    val nodes = given { nodeGraph(110) }
    val input = inputValues((0 until sizeHint).map(i => Array[Any](nodes(i % 5), nodes(100 + (i % 10)))).sortBy(_.head.asInstanceOf[Node].getId): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x1", "y1")
      .orderedDistinct(Seq(varFor("x")),"x AS x1", "y AS y1")
      .input(nodes = Seq("x", "y"), nullable = false)
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val s: Iterable[Array[_]] = (0 until 10).map(i => Array[Any](nodes(i % 5), nodes(100 + (i % 10))))
    runtimeResult should beColumns("x1", "y1").withRows(s)
  }

  test("should work on input with projection, two primitive columns, both sorted") {
    // given
    val nodes = given { nodeGraph(110) }
    val input = inputValues((0 until sizeHint).map(i => Array[Any](nodes(i % 5), nodes(100 + (i % 10)))).sortBy(a => (a(0).asInstanceOf[Node].getId, a(1).asInstanceOf[Node].getId)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x1", "y1")
      .orderedDistinct(Seq(varFor("x"),varFor("y")),"x AS x1", "y AS y1")
      .input(nodes = Seq("x", "y"), nullable = false)
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val s: Iterable[Array[_]] = (0 until 10).map(i => Array[Any](nodes(i % 5), nodes(100 + (i % 10))))
    runtimeResult should beColumns("x1", "y1").withRows(s)
  }

  test("should work on input with projection, three columns, one sorted") {
    // given
    val nodes = given { nodeGraph(110) }
    val input = inputValues((0 until sizeHint).map(i => Array[Any](nodes(i % 5), nodes(100 + (i % 10)), nodes(i % 20))).sortBy(_.head.asInstanceOf[Node].getId): _*)


    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x1", "y1", "z1")
      .orderedDistinct(Seq(varFor("x")),"x AS x1", "y AS y1", "z AS z1")
      .input(nodes = Seq("x", "y", "z"), nullable = false)
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val s: Iterable[Array[_]] = (0 until 20).map(i => Array[Any](nodes(i % 5), nodes(100 + (i % 10)), nodes(i % 20)))
    runtimeResult should beColumns("x1", "y1", "z1").withRows(s)
  }

  test("should work on input with projection, four columns, two sorted") {
    // given
    val nodes = given { nodeGraph(110) }
    val input = inputValues((0 until sizeHint).map(i => Array[Any](nodes(i % 5), nodes(100 + (i % 10)), nodes(i % 20), nodes(i % 4))).sortBy(a => (a(0).asInstanceOf[Node].getId, a(3).asInstanceOf[Node].getId)): _*)


    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x1", "y1", "z1", "w1")
      .orderedDistinct(Seq(varFor("x"), varFor("w")),"x AS x1", "y AS y1", "z AS z1", "w AS w1")
      .input(nodes = Seq("x", "y", "z", "w"), nullable = false)
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val s: Iterable[Array[_]] = (0 until 20).map(i => Array[Any](nodes(i % 5), nodes(100 + (i % 10)), nodes(i % 20), nodes(i % 4)))
    runtimeResult should beColumns("x1", "y1", "z1", "w1").withRows(s)
  }

  test("should return no rows for empty input") {
    // given
    val input = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x1", "y1")
      .orderedDistinct(Seq(varFor("x"),varFor("y")),"x AS x1", "y AS y1")
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x1", "y1").withNoRows()
  }

  test("should keep input order") {
    // given
    // (x is always 0, y is a sawtooth pattern)
    val f: Int => Int = i => i - ((i / 10) * 10) / 2
    val input = inputColumns(10, sizeHint / 10, _ => 0, f)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .orderedDistinct(Seq(varFor("x")),"x AS x", "y AS y")
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("y")
      .withRows(inOrder((0 until sizeHint).map(f).distinct.sorted.map(Array[Any](_))))
  }
}
