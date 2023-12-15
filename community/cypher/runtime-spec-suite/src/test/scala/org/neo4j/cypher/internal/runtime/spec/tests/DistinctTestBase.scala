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
import org.neo4j.graphdb.Direction.OUTGOING

import scala.jdk.CollectionConverters.IterableHasAsScala

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

  test("should work on distinct on single primitive node column") {
    // given
    val nodes = givenGraph { nodeGraph(sizeHint) }
    val inputNodes = inputValues(nodes.flatMap(n => Seq.fill(11)(n)).map(Array[Any](_)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .distinct("x AS x")
      .input(nodes = Seq("x"), nullable = false)
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputNodes)

    // then
    runtimeResult should beColumns("x").withRows(nodes.map(Array[Any](_)))
  }

  test("should work on distinct on single primitive relationship column") {
    // given
    val (_, relationships) = givenGraph { circleGraph(sizeHint) }
    val inputNodes = inputValues(relationships.flatMap(n => Seq.fill(11)(n)).map(Array[Any](_)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .distinct("x AS x")
      .input(relationships = Seq("x"), nullable = false)
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputNodes)

    // then
    runtimeResult should beColumns("x").withRows(relationships.map(Array[Any](_)))
  }

  test("should work on distinct on multiple primitive columns") {
    // given
    val (nodes, relationships) = givenGraph { circleGraph(sizeHint) }
    val inputNodes = inputValues(nodes.flatMap(n => Seq.fill(11)(n)).map(Array[Any](_)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r")
      .distinct("x AS x", "r AS r")
      .expand("(x)-[r]->(y)")
      .input(nodes = Seq("x"), nullable = false)
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputNodes)

    val expected = for {
      n <- nodes
      r <- n.getRelationships(OUTGOING).asScala
    } yield Array[Any](n, r)
    // then
    runtimeResult should beColumns("x", "r").withRows(expected)
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
    assume(!isParallel) // Parallel runtime cannot maintain provided order
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

    val as = Seq(10, 11, 12, 13)
    val bs = Seq(1, 1, 2, 3, 4, 4, 5, 1, 3, 2, 5, 4, 1)

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
    val expected = for { _ <- as; b <- bs.distinct } yield b
    runtimeResult should beColumns("b").withRows(singleColumn(expected))
  }

  test("should work on RHS of apply, not in final pipeline") {

    val as = Seq(10, 11, 12, 13)
    val bs = Seq(1, 1, 2, 3, 4, 4, 5, 1, 3, 2, 5, 4, 1)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("b")
      .eager()
      .apply()
      .|.distinct("b AS b")
      .|.unwind(s"[${bs.mkString(",")}] AS b")
      .|.argument()
      .unwind(s"[${as.mkString(",")}] AS a")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for { _ <- as; b <- bs.distinct } yield b
    runtimeResult should beColumns("b").withRows(singleColumn(expected))
  }

  test("should work with distinct on single primitive node column on RHS of apply") {
    // given
    val nodes = givenGraph { nodeGraph(sizeHint) }
    val inputNodes = inputValues(nodes.flatMap(n => Seq.fill(11)(n)).map(Array[Any](_)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .apply()
      .|.distinct("x AS y")
      .|.argument("x")
      .input(nodes = Seq("x"), nullable = false)
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputNodes)

    // then
    val expected = for { _ <- 1 to 11; n <- nodes.distinct } yield n
    runtimeResult should beColumns("y").withRows(singleColumn(expected))
  }

  test("should work with distinct on single primitive node column on RHS of apply, not in final pipeline") {
    // given
    val nodes = givenGraph { nodeGraph(sizeHint) }
    val inputNodes = inputValues(nodes.flatMap(n => Seq.fill(11)(n)).map(Array[Any](_)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .eager()
      .apply()
      .|.distinct("x AS y")
      .|.argument("x")
      .input(nodes = Seq("x"), nullable = false)
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputNodes)

    // then
    val expected = for { _ <- 1 to 11; n <- nodes.distinct } yield n
    runtimeResult should beColumns("y").withRows(singleColumn(expected))
  }

  test("should work with distinct on multiple primitive columns on RHS of apply") {
    // given
    val (nodes, _) = givenGraph { circleGraph(sizeHint) }
    val inputNodes = inputValues(nodes.flatMap(n => Seq.fill(11)(n)).map(Array[Any](_)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("z", "r")
      .apply()
      .|.distinct("x AS z", "r AS r")
      .|.expand("(x)-[r]->(y)")
      .|.argument("x")
      .input(nodes = Seq("x"), nullable = false)
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputNodes)

    val expected = for {
      _ <- 1 to 11
      n <- nodes
      r <- n.getRelationships(OUTGOING).asScala
    } yield Array[Any](n, r)
    // then
    runtimeResult should beColumns("z", "r").withRows(expected)
  }

  test("should work with distinct on multiple primitive columns on RHS of apply, not in final pipeline") {
    // given
    val (nodes, _) = givenGraph { circleGraph(sizeHint) }
    val inputNodes = inputValues(nodes.flatMap(n => Seq.fill(11)(n)).map(Array[Any](_)): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("z", "r")
      .eager()
      .apply()
      .|.distinct("x AS z", "r AS r")
      .|.expand("(x)-[r]->(y)")
      .|.argument("x")
      .input(nodes = Seq("x"), nullable = false)
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputNodes)

    val expected = for {
      _ <- 1 to 11
      n <- nodes
      r <- n.getRelationships(OUTGOING).asScala
    } yield Array[Any](n, r)
    // then
    runtimeResult should beColumns("z", "r").withRows(expected)
  }

  test("should support distinct on top of apply") {
    // given
    val nodesPerLabel = 50
    val (aNodes, _) = givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

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

  test("should work on cached property, one column") {
    // given
    val nodes = givenGraph {
      nodePropertyGraph(
        sizeHint,
        properties = {
          case i: Int => Map("foo" -> s"bar${i % 10}")
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("bar")
      .distinct("cache[n.foo] AS bar")
      .cacheProperties("cache[n.foo]")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = nodes.map(n => n.getProperty("foo")).distinct

    // then
    runtimeResult should beColumns("bar").withRows(singleColumn(expected))
  }

  test("should support filter after a distinct") {
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n) yield {
      Seq((i, (i + 1) % n, "NEXT"))
    }).reduce(_ ++ _)
    givenGraph {
      // prop = [0, 0, 1, 1, 2, 2,..]
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

  test("should work with aggregation") {
    // given
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        properties = {
          case i: Int => Map("foo" -> s"bar${i % 10}")
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("group", "c")
      .aggregation(Seq("bar AS group"), Seq("count(n) AS c"))
      .distinct("n.foo AS bar")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = (0 until 10).map(i => Array[Any](s"bar$i", 1))

    // then
    runtimeResult should beColumns("group", "c").withRows(expected)
  }

  test("should work after multiple streaming operators") {
    // given
    val nodeCount = 100
    givenGraph {
      bipartiteGraph(
        nodeCount,
        "A",
        "B",
        "R",
        aProperties = {
          case i: Int => Map("foo" -> s"bar${i % 10}")
        },
        bProperties = {
          case i: Int => Map("foo" -> s"bar${i % 10}")
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("bar")
      .distinct("b.foo AS bar")
      .expandAll("(a)--(b)")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("bar").withRows(singleColumn((0 until 10).map(i => s"bar$i")))
  }

  test("should work between streaming operators with aggregation") {
    val nodeCount = 100
    givenGraph {
      bipartiteGraph(
        nodeCount,
        "A",
        "B",
        "R",
        aProperties = {
          case i: Int => Map("foo" -> s"bar${i % 10}")
        },
        bProperties = {
          case i: Int => Map("foo" -> s"bar${i % 10}")
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("group", "c")
      .aggregation(Seq("b.foo AS group"), Seq("count(b) AS c"))
      .expandAll("(a)--(b)")
      .distinct("a AS a")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expectedGroupSize = nodeCount * nodeCount / 10
    val expected = (0 until 10).map(i => Array[Any](s"bar$i", expectedGroupSize))

    // then
    runtimeResult should beColumns("group", "c").withRows(expected)
  }

  test("should work with chained distincts") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        properties = {
          case i: Int => Map("foo" -> s"bar${i % 10}")
        },
        "A"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("bar")
      .distinct("bar AS bar")
      .distinct("a.foo AS bar")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("bar").withRows(singleColumn((0 until 10).map(i => s"bar$i")))
  }

  test("should work with aggregation on the RHS of an apply") {
    // given
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        properties = {
          case i: Int => Map("foo" -> s"bar${i % 10}")
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("group", "c")
      .apply()
      .|.aggregation(Seq("bar AS group"), Seq("count(n) AS c"))
      .|.distinct("n.foo AS bar")
      .|.allNodeScan("n", "x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues((1 to 10).map(Array[Any](_)): _*))

    val expected = for {
      _ <- 1 to 10
      i <- 0 until 10
    } yield Array[Any](s"bar$i", 1)

    // then
    runtimeResult should beColumns("group", "c").withRows(expected)
  }

  test("should work with chained distincts on the RHS of an apply") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        properties = {
          case i: Int => Map("foo" -> s"bar${i % 10}")
        },
        "A"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("bar")
      .apply()
      .|.distinct("bar AS bar")
      .|.distinct("a.foo AS bar")
      .|.nodeByLabelScan("a", "A", IndexOrderNone, "x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues((1 to 10).map(Array[Any](_)): _*))

    // then
    val expected = for {
      _ <- 1 to 10
      i <- 0 until 10
    } yield s"bar$i"
    runtimeResult should beColumns("bar").withRows(singleColumn(expected))
  }
}
