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
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.Node

import scala.util.Try

abstract class OrderedDistinctTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
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

  test("should work with limit below") {
    // given
    val nodes = givenGraph {
      nodeGraph(10)
    }
    val input =
      inputValues((0 until sizeHint).map(i => Array[Any](nodes(i % 10))).sortBy(_.head.asInstanceOf[Node].getId): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .orderedDistinct(Seq("x"), "x AS x")
      .limit(1)
      .input(nodes = Seq("x"), nullable = false)
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(nodes.head)))
  }

  test("should work with limit on top") {
    // given
    val nodes = givenGraph {
      nodeGraph(10)
    }
    val input =
      inputValues((0 until sizeHint).map(i => Array[Any](nodes(i % 10))).sortBy(_.head.asInstanceOf[Node].getId): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .limit(1)
      .orderedDistinct(Seq("x"), "x AS x")
      .input(nodes = Seq("x"), nullable = false)
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(nodes.head)))
  }

  test("should work with chained distincts, one column, sorted") {
    // given
    val nodes = givenGraph {
      nodeGraph(10)
    }
    val input =
      inputValues((0 until sizeHint).map(i => Array[Any](nodes(i % 10))).sortBy(_.head.asInstanceOf[Node].getId): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x3")
      .orderedDistinct(Seq("x2"), "x2 as x3")
      .orderedDistinct(Seq("x"), "x AS x2")
      .input(nodes = Seq("x"), nullable = false)
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x3").withRows(singleColumn(nodes))
  }

  test("should work with chained distincts, two columns, one sorted") {
    // given
    val input = for (x <- 0 until sizeHint) yield Array[Any](x)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x3", "y3")
      .orderedDistinct(Seq("x2"), "x2 as x3", "y2 as y3")
      .orderedDistinct(Seq("x"), "x AS x2", "y as y2")
      .unwind("[1,2,3,1,2,3] as y")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    val expectedRows = for {
      x <- 0 until sizeHint
      y <- 1 to 3
    } yield Array[Any](x, y)

    runtimeResult should beColumns("x3", "y3").withRows(expectedRows)
  }

  test("should work with chained distincts, two columns, both sorted") {
    // given
    val input = for (x <- 0 until sizeHint) yield Array[Any](x)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x3", "y3")
      .orderedDistinct(Seq("x2", "y2"), "x2 as x3", "y2 as y3")
      .orderedDistinct(Seq("x", "y"), "x AS x2", "y as y2")
      .unwind("[1,1,2,2,3,3] as y")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    val expectedRows = for {
      x <- 0 until sizeHint
      y <- 1 to 3
    } yield Array[Any](x, y)

    runtimeResult should beColumns("x3", "y3").withRows(expectedRows)
  }

  test("should work with chained distincts, one, two and three columns") {
    // given
    val input = for (x <- 0 until sizeHint) yield Array[Any](x)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x3", "y3", "z2")
      .orderedDistinct(Seq("x2", "y2"), "x2 as x3", "y2 as y3", "z as z2")
      .unwind("[4,5,6,4,5,6] as z")
      .orderedDistinct(Seq("x", "y"), "x AS x2", "y as y2")
      .unwind("[1,1,2,2,3,3] as y")
      .orderedDistinct(Seq("x"), "x as x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    val expectedRows = for {
      x <- 0 until sizeHint
      y <- 1 to 3
      z <- 4 to 6
    } yield Array[Any](x, y, z)

    runtimeResult should beColumns("x3", "y3", "z2").withRows(expectedRows)
  }

  test("should work with aggregation") {
    // given
    val input = for (x <- 0 until sizeHint) yield Array[Any](x)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("group", "c")
      .aggregation(Seq("x as group"), Seq("count(y) as c"))
      .orderedDistinct(Seq("x", "y"), "x as x", "y AS y")
      .unwind("[1,1,2,2,3,3] AS y")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    val expectedRows = for {
      x <- 0 until sizeHint
    } yield Array[Any](x, 3)

    runtimeResult should beColumns("group", "c").withRows(expectedRows)
  }

  Seq(
    0,
    1,
    10,
    Int.MaxValue
  ).foreach {
    limit: Int =>
      {
        test(s"should work with limit = $limit on top under apply, one column, one sorted") {
          // given
          val input = for (x <- 0 until sizeHint) yield Array[Any](x)

          // when
          val logicalQuery = new LogicalQueryBuilder(this)
            .produceResults("x", "y")
            .apply()
            .|.limit(limit)
            .|.orderedDistinct(Seq("y"), "y AS y")
            .|.unwind("[1,1,2,2,3,3] AS y")
            .|.argument("x")
            .input(variables = Seq("x"))
            .build()

          val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

          // then
          val expectedRows = for {
            x <- 0 until sizeHint
            y <- (1 to 3).take(limit)
          } yield Array[Any](x, y)

          runtimeResult should beColumns("x", "y").withRows(expectedRows)
        }

        test(s"should work with limit = $limit on top under apply, two columns, one sorted") {
          // given
          val input = for (x <- 0 until sizeHint) yield Array[Any](x)

          // when
          val logicalQuery = new LogicalQueryBuilder(this)
            .produceResults("x", "a", "b")
            .apply()
            .|.limit(limit)
            .|.orderedDistinct(Seq("x"), "x AS a", "y AS b")
            .|.unwind("[1,2,3,1,2,3] AS y")
            .|.argument("x")
            .input(variables = Seq("x"))
            .build()

          val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

          // then
          val expectedRows = for {
            x <- 0 until sizeHint
            y <- (1 to 3).take(limit)
          } yield Array[Any](x, x, y)

          runtimeResult should beColumns("x", "a", "b").withRows(expectedRows)
        }

        test(s"should work with limit = $limit on top under apply, two columns, two sorted") {
          // given
          val input = for (x <- 0 until sizeHint) yield Array[Any](x)

          // when
          val logicalQuery = new LogicalQueryBuilder(this)
            .produceResults("x", "a", "b")
            .apply()
            .|.limit(limit)
            .|.orderedDistinct(Seq("x", "y"), "x AS a", "y AS b")
            .|.unwind("[1,1,2,2,3,3] AS y")
            .|.argument("x")
            .input(variables = Seq("x"))
            .build()

          val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

          // then
          val expectedRows = for {
            x <- 0 until sizeHint
            y <- (1 to 3).take(limit)
          } yield Array[Any](x, x, y)

          runtimeResult should beColumns("x", "a", "b").withRows(expectedRows)
        }

        test(s"should work under apply, with limit = $limit on top of apply, one column, one sorted") {
          // given
          val input = for (x <- 0 until sizeHint) yield Array[Any](x)

          // when
          val logicalQuery = new LogicalQueryBuilder(this)
            .produceResults("x", "y")
            .limit(limit)
            .apply()
            .|.orderedDistinct(Seq("y"), "y AS y")
            .|.unwind("[1,1,2,2,3,3] AS y")
            .|.argument("x")
            .input(variables = Seq("x"))
            .build()

          val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

          // then
          val expectedRows = (for {
            x <- 0 until sizeHint
            y <- 1 to 3
          } yield Array[Any](x, y)).take(limit)

          runtimeResult should beColumns("x", "y").withRows(expectedRows)
        }

        test(s"should work under apply, with limit = $limit on top of apply, two columns, one sorted") {
          // given
          val input = for (x <- 0 until sizeHint) yield Array[Any](x)

          // when
          val logicalQuery = new LogicalQueryBuilder(this)
            .produceResults("x", "a", "b")
            .limit(limit)
            .apply()
            .|.orderedDistinct(Seq("x"), "x AS a", "y AS b")
            .|.unwind("[1,2,3,1,2,3] AS y")
            .|.argument("x")
            .input(variables = Seq("x"))
            .build()

          val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

          // then
          val expectedRows = (for {
            x <- 0 until sizeHint
            y <- 1 to 3
          } yield Array[Any](x, x, y)).take(limit)

          runtimeResult should beColumns("x", "a", "b").withRows(expectedRows)
        }

        test(s"should work under apply, with limit = $limit on top of apply, two columns, two sorted") {
          // given
          val input = for (x <- 0 until sizeHint) yield Array[Any](x)

          // when
          val logicalQuery = new LogicalQueryBuilder(this)
            .produceResults("x", "a", "b")
            .limit(limit)
            .apply()
            .|.orderedDistinct(Seq("x", "y"), "x AS a", "y AS b")
            .|.unwind("[1,1,2,2,3,3] AS y")
            .|.argument("x")
            .input(variables = Seq("x"))
            .build()

          val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

          // then
          val expectedRows = (for {
            x <- 0 until sizeHint
            y <- 1 to 3
          } yield Array[Any](x, x, y)).take(limit)

          runtimeResult should beColumns("x", "a", "b").withRows(expectedRows)
        }

        test(s"should work under apply, with limit = $limit on top of apply and on rhs, one column, one sorted") {
          // given
          val input = for (x <- 0 until sizeHint) yield Array[Any](x)

          // when
          val logicalQuery = new LogicalQueryBuilder(this)
            .produceResults("x", "y")
            .limit(limit)
            .apply()
            .|.limit(limit)
            .|.orderedDistinct(Seq("y"), "y AS y")
            .|.unwind("[1,1,2,2,3,3] AS y")
            .|.argument("x")
            .input(variables = Seq("x"))
            .build()

          val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

          // then
          val expectedRows = (for {
            x <- 0 until sizeHint
            y <- (1 to 3).take(limit)
          } yield Array[Any](x, y)).take(limit)

          runtimeResult should beColumns("x", "y").withRows(expectedRows)
        }

        test(s"should work under apply, with limit = $limit on top of apply and on rhs, two columns, one sorted") {
          // given
          val input = for (x <- 0 until sizeHint) yield Array[Any](x)

          // when
          val logicalQuery = new LogicalQueryBuilder(this)
            .produceResults("x", "a", "b")
            .limit(limit)
            .apply()
            .|.limit(limit)
            .|.orderedDistinct(Seq("x"), "x AS a", "y AS b")
            .|.unwind("[1,2,3,1,2,3] AS y")
            .|.argument("x")
            .input(variables = Seq("x"))
            .build()

          val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

          // then
          val expectedRows = (for {
            x <- 0 until sizeHint
            y <- (1 to 3).take(limit)
          } yield Array[Any](x, x, y)).take(limit)

          runtimeResult should beColumns("x", "a", "b").withRows(expectedRows)
        }

        test(s"should work under apply, with limit = $limit on top of apply and on rhs, two columns, two sorted") {
          // given
          val input = for (x <- 0 until sizeHint) yield Array[Any](x)

          // when
          val logicalQuery = new LogicalQueryBuilder(this)
            .produceResults("x", "a", "b")
            .limit(limit)
            .apply()
            .|.limit(limit)
            .|.orderedDistinct(Seq("x", "y"), "x AS a", "y AS b")
            .|.unwind("[1,1,2,2,3,3] AS y")
            .|.argument("x")
            .input(variables = Seq("x"))
            .build()

          val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

          // then
          val expectedRows = (for {
            x <- 0 until sizeHint
            y <- (1 to 3).take(limit)
          } yield Array[Any](x, x, y)).take(limit)

          runtimeResult should beColumns("x", "a", "b").withRows(expectedRows)
        }

        test(s"should work with sort and limit = $limit on top under apply") {
          // given
          val input = for (x <- 0 until sizeHint) yield Array[Any](x)

          // when
          val logicalQuery = new LogicalQueryBuilder(this)
            .produceResults("a", "b")
            .apply()
            .|.sort("a ASC", "b ASC")
            .|.limit(limit)
            .|.orderedDistinct(Seq("x"), "x AS a", "y AS b")
            .|.unwind("[1,2,3,1,2,3] AS y")
            .|.argument("x")
            .input(variables = Seq("x"))
            .build()

          val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

          // then
          val expectedRows = for {
            x <- 0 until sizeHint
            y <- (1 to 3).take(limit)
          } yield Array[Any](x, y)

          runtimeResult should beColumns("a", "b").withRows(inOrder(expectedRows))
        }

        test(s"should work with top = $limit and aggregation under nested applys") {
          // given
          val nodes = givenGraph {
            nodeGraph(sizeHint, "X", "Y", "Z")
          }

          val argSize = Math.max(sizeHint / 100, 10)

          // when
          val logicalQuery = new LogicalQueryBuilder(this)
            .produceResults("n", "a", "b", "c")
            .apply().withLeveragedOrder()
            .|.orderedDistinct(Seq("y"), "y as a", "z as b", "c as c")
            .|.top(Seq(Ascending(varFor("y"))), limit)
            .|.aggregation(Seq("y as y", "z as z"), Seq("collect(u) as c"))
            .|.apply()
            .|.|.orderedDistinct(Seq("z"), "z as z", "u as u")
            .|.|.limit(limit)
            .|.|.unwind("[1,2,3] as u")
            .|.|.apply()
            .|.|.|.orderedDistinct(Seq("z"), "z as z")
            .|.|.|.top(Seq(Ascending(varFor("z"))), limit)
            .|.|.|.aggregation(Seq.empty, Seq("collect(y) as z"))
            .|.|.|.filter("y:Y")
            .|.|.|.argument("y")
            .|.|.argument("y")
            .|.projection("x as y")
            .|.filter("x:X")
            .|.sort("x ASC")
            .|.allNodeScan("x")
            .unwind(s"range(1,$argSize) as n")
            .argument()
            .build()

          val runtimeResult = execute(logicalQuery, runtime)

          val expectedRows =
            (1 to argSize).flatMap(n =>
              (for {
                a <- nodes
                b = Array(a)
                c = Array(1, 2, 3).take(limit)
              } yield Array[Any](n, a, b, c)).take(limit)
            )

          // then
          runtimeResult should beColumns("n", "a", "b", "c").withRows(expectedRows)
        }

      }
  }

  test("should work with work with optional and node hash join on rhs") {
    // given
    val nodes = givenGraph {
      lineGraph(sizeHint, "R", "LABEL", "LABEL_2")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "r")
      .apply()
      .|.orderedDistinct(Seq("x"), "x AS a", "y AS b", "r as r")
      .|.optional("x")
      .|.nodeHashJoin("x")
      .|.|.filter("x:LABEL_2")
      .|.|.nodeByLabelScan("x", "LABEL", IndexOrderAscending)
      .|.expandAll("(x)-[r]->(y)")
      .|.argument("x")
      .nodeByLabelScan("x", "LABEL", IndexOrderAscending)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val (n, r) = nodes
    val expectedRows = for {
      x <- n.indices
      y = x + 1
    } yield Array[Any](n(x), Try(n(y)).getOrElse(null), Try(r(x)).getOrElse(null))

    runtimeResult should beColumns("a", "b", "r").withRows(expectedRows)
  }

  test("should work on nested ordered unions") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint, "X", "Y", "Z")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .orderedDistinct(Seq("x"), "x AS a")
      .orderedUnion("x ASC")
      .|.orderedDistinct(Seq("y"), "y as x")
      .|.orderedUnion("y ASC")
      .|.|.orderedDistinct(Seq("z"), "z as y")
      .|.|.sort("z ASC")
      .|.|.allNodeScan("z")
      .|.sort("y ASC")
      .|.allNodeScan("y")
      .sort("x ASC")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expectedRows = nodes.map(Array[Any](_))
    // then
    runtimeResult should beColumns("a").withRows(expectedRows)
  }

  test("should work with conditional apply on rhs") {
    // given
    givenGraph {
      nodeGraph(sizeHint, "Z")
    }

    val argSize = Math.max(sizeHint / 100, 10)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.orderedDistinct(Seq("x"), "x as x", "y as y")
      .|.conditionalApply("y")
      .|.|.nodeByLabelScan("z", "Z", IndexOrderAscending)
      .|.skip(4)
      .|.unwind("[1,2,3,4,1,2,3,1,2,3] as y")
      .|.argument("x")
      .unwind(s"range(1,$argSize) as x")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expectedRows =
      for {
        x <- 1 to argSize
        y <- 1 to 3
      } yield Array[Any](x, y)

    // then
    runtimeResult should beColumns("x", "y").withRows(expectedRows)
  }

  test("should work with selectOrAntiSemiApply apply on rhs") {
    // given
    givenGraph {
      nodeGraph(sizeHint, "Z")
    }

    val argSize = Math.max(sizeHint / 100, 10)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.orderedDistinct(Seq("x"), "x as x", "y as y")
      .|.selectOrAntiSemiApply("x > 5").withLeveragedOrder()
      .|.|.projection("1 as a")
      .|.|.argument()
      .|.unwind("[1,2,3,1,2,3] as y")
      .|.argument("x")
      .unwind(s"range(1,$argSize) as x")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expectedRows =
      for {
        x <- (1 to argSize) if x > 5
        y <- 1 to 3
      } yield Array[Any](x, y)

    // then
    runtimeResult should beColumns("x", "y").withRows(expectedRows)
  }
}
