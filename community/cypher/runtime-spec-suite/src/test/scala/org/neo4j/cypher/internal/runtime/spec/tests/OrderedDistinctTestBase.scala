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
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
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
      .orderedDistinct(Seq("x"), "x AS x")
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
      .orderedDistinct(Seq("x"), "x AS x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(0 until 10))
  }

  test("should work on input with no projection, one primitive column, sorted") {
    // given
    val nodes = givenGraph { nodeGraph(10) }
    val input =
      inputValues((0 until sizeHint).map(i => Array[Any](nodes(i % 10))).sortBy(_.head.asInstanceOf[Node].getId): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .orderedDistinct(Seq("x"), "x AS x")
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
      .orderedDistinct(Seq("x"), "x AS y")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("y").withRows(singleColumn(0 until 10))
  }

  test("should work on input with projection, two columns, one sorted") {
    // given
    val input =
      inputValues((0 until sizeHint).map(i => Array[Any](i % 5, 100 + (i % 10))).sortBy(_.head.asInstanceOf[Int]): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x1", "y1")
      .orderedDistinct(Seq("x"), "x AS x1", "1 + y AS y1")
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val s: Iterable[Array[_]] = (0 until 10).map(i => Array[Any](i % 5, 101 + (i % 10)))
    runtimeResult should beColumns("x1", "y1").withRows(s)
  }

  test("should work on input with projection, two columns, both sorted") {
    // given
    val input = inputValues((0 until sizeHint).map(i => Array[Any](i % 5, 100 + (i % 10))).sortBy(a =>
      (a(0).asInstanceOf[Int], a(1).asInstanceOf[Int])
    ): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x1", "y1")
      .orderedDistinct(Seq("x", "y"), "x AS x1", "y AS y1")
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val s: Iterable[Array[_]] = (0 until 10).map(i => Array[Any](i % 5, 100 + (i % 10)))
    runtimeResult should beColumns("x1", "y1").withRows(s)
  }

  test("should work on input with projection, two primitive columns, one sorted") {
    // given
    val nodes = givenGraph { nodeGraph(110) }
    val input = inputValues((0 until sizeHint).map(i => Array[Any](nodes(i % 5), nodes(100 + (i % 10)))).sortBy(
      _.head.asInstanceOf[Node].getId
    ): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x1", "y1")
      .orderedDistinct(Seq("x"), "x AS x1", "y AS y1")
      .input(nodes = Seq("x", "y"), nullable = false)
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val s: Iterable[Array[_]] = (0 until 10).map(i => Array[Any](nodes(i % 5), nodes(100 + (i % 10))))
    runtimeResult should beColumns("x1", "y1").withRows(s)
  }

  test("should work on input with projection, two primitive columns, both sorted") {
    // given
    val nodes = givenGraph { nodeGraph(110) }
    val input = inputValues((0 until sizeHint).map(i => Array[Any](nodes(i % 5), nodes(100 + (i % 10)))).sortBy(a =>
      (a(0).asInstanceOf[Node].getId, a(1).asInstanceOf[Node].getId)
    ): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x1", "y1")
      .orderedDistinct(Seq("x", "y"), "x AS x1", "y AS y1")
      .input(nodes = Seq("x", "y"), nullable = false)
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val s: Iterable[Array[_]] = (0 until 10).map(i => Array[Any](nodes(i % 5), nodes(100 + (i % 10))))
    runtimeResult should beColumns("x1", "y1").withRows(s)
  }

  test("should work on input with projection, three columns, one sorted") {
    // given
    val nodes = givenGraph { nodeGraph(110) }
    val input = inputValues((0 until sizeHint).map(i =>
      Array[Any](nodes(i % 5), nodes(100 + (i % 10)), nodes(i % 20))
    ).sortBy(_.head.asInstanceOf[Node].getId): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x1", "y1", "z1")
      .orderedDistinct(Seq("x"), "x AS x1", "y AS y1", "z AS z1")
      .input(nodes = Seq("x", "y", "z"), nullable = false)
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val s: Iterable[Array[_]] = (0 until 20).map(i => Array[Any](nodes(i % 5), nodes(100 + (i % 10)), nodes(i % 20)))
    runtimeResult should beColumns("x1", "y1", "z1").withRows(s)
  }

  test("should work on input with projection, four columns, two sorted") {
    // given
    val nodes = givenGraph { nodeGraph(110) }
    val input = inputValues((0 until sizeHint).map(i =>
      Array[Any](nodes(i % 5), nodes(100 + (i % 10)), nodes(i % 20), nodes(i % 4))
    ).sortBy(a => (a(0).asInstanceOf[Node].getId, a(3).asInstanceOf[Node].getId)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x1", "y1", "z1", "w1")
      .orderedDistinct(Seq("x", "w"), "x AS x1", "y AS y1", "z AS z1", "w AS w1")
      .input(nodes = Seq("x", "y", "z", "w"), nullable = false)
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val s: Iterable[Array[_]] =
      (0 until 20).map(i => Array[Any](nodes(i % 5), nodes(100 + (i % 10)), nodes(i % 20), nodes(i % 4)))
    runtimeResult should beColumns("x1", "y1", "z1", "w1").withRows(s)
  }

  test("should return no rows for empty input") {
    // given
    val input = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x1", "y1")
      .orderedDistinct(Seq("x", "y"), "x AS x1", "y AS y1")
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
      .orderedDistinct(Seq("x"), "x AS x", "y AS y")
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("y")
      .withRows(inOrder((0 until sizeHint).map(f).distinct.sorted.map(Array[Any](_))))
  }

  test("should work on cached property") {
    givenGraph {
      nodeIndex("A", "prop")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "A"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop")
      .orderedDistinct(Seq("cache[x.prop]"), "cache[x.prop] AS prop")
      .nodeIndexOperator(s"x:A(prop > ${sizeHint / 2})", _ => GetValue, indexOrder = IndexOrderAscending)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = ((sizeHint / 2 + 1) until sizeHint).distinct.sorted
    runtimeResult should beColumns("prop").withRows(singleColumn(expected))
  }

  test("should work under apply, one column, one sorted") {
    // given
    val input = for (x <- 0 until sizeHint) yield Array[Any](x)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.orderedDistinct(Seq("y"), "y AS y")
      .|.unwind("[1,1,2,2,3,3] AS y")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    val expectedRows = for {
      x <- 0 until sizeHint
      y <- 1 to 3
    } yield Array[Any](x, y)

    runtimeResult should beColumns("x", "y").withRows(expectedRows)
  }

  test("should work under apply, two columns, one sorted") {
    // given
    val input = for (x <- 0 until sizeHint) yield Array[Any](x)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "a", "b")
      .apply()
      .|.orderedDistinct(Seq("x"), "x AS a", "y AS b")
      .|.unwind("[1,2,3,1,2,3] AS y")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    val expectedRows = for {
      x <- 0 until sizeHint
      y <- 1 to 3
    } yield Array[Any](x, x, y)

    runtimeResult should beColumns("x", "a", "b").withRows(expectedRows)
  }

  test("should work under apply, two columns, two sorted") {
    // given
    val input = for (x <- 0 until sizeHint) yield Array[Any](x)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "a", "b")
      .apply()
      .|.orderedDistinct(Seq("x", "y"), "x AS a", "y AS b")
      .|.unwind("[1,1,2,2,3,3] AS y")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    val expectedRows = for {
      x <- 0 until sizeHint
      y <- 1 to 3
    } yield Array[Any](x, x, y)

    runtimeResult should beColumns("x", "a", "b").withRows(expectedRows)
  }
}
