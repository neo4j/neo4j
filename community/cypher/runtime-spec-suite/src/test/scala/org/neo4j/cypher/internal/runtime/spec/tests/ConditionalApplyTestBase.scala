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

import scala.jdk.CollectionConverters.IterableHasAsScala

abstract class ConditionalApplyTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("conditional apply should not run rhs if lhs is empty") {
    // given
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .conditionalApply("x")
      .|.filter("1/0 > 0")
      .|.allNodeScan("y", "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // should not throw 1/0 exception
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("conditional apply on nonempty lhs and empty rhs, where condition(lhs) always is true") {
    // given
    val nodes = givenGraph {
      nodeGraph(19, "RHS")
      nodeGraph(sizeHint)
    }
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .conditionalApply("x")
      .|.expandInto("(y)--(x)")
      .|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // because graph contains no relationships, the expand will return no rows
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("conditional apply on nonempty lhs and empty rhs, where condition(lhs) always is true - with sorts") {
    // given
    val nodes = givenGraph {
      nodeGraph(19, "RHS")
      nodeGraph(sizeHint)
    }
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .sort("x ASC")
      .conditionalApply("x")
      .|.sort("y ASC")
      .|.expandInto("(y)--(x)")
      .|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // because graph contains no relationships, the expand will return no rows
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("conditional apply on nonempty lhs and empty rhs") {
    // given
    givenGraph {
      nodeGraph(19, "RHS")
      nodeGraph(sizeHint)
    }
    val lhsRows = inputValues(Array("42"), Array(null), Array("43"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .conditionalApply("x")
      .|.filter("false")
      .|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // will only return lhs where condition(lhs) is false
    runtimeResult should beColumns("x", "y").withSingleRow(null, null)
  }

  test("conditional apply on nonempty lhs and empty rhs - with sorts") {
    // given
    givenGraph {
      nodeGraph(19, "RHS")
      nodeGraph(sizeHint)
    }
    val lhsRows = inputValues(Array("42"), Array(null), Array("43"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .sort("x ASC")
      .conditionalApply("x")
      .|.sort("y ASC")
      .|.filter("false")
      .|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // will only return lhs where condition(lhs) is false
    runtimeResult should beColumns("x", "y").withSingleRow(null, null)
  }

  test("conditional apply on nonempty lhs and nonempty rhs") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint)
      nodeGraph(sizeHint, "RHS")
    }
    val lhsRows = inputValues(Array("42"), Array(null), Array("43"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .conditionalApply("x")
      .|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expected =
      (nodes.map(n => Array[Any]("42", n)) :+ Array[Any](null, null)) ++ nodes.map(n => Array[Any]("43", n))
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("conditional apply on nonempty lhs and nonempty rhs - with sorts") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint)
      nodeGraph(sizeHint, "RHS")
    }
    val lhsRows = inputValues(Array("42"), Array(null), Array("43"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .sort("x ASC")
      .conditionalApply("x")
      .|.sort("y ASC")
      .|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expected =
      (nodes.map(n => Array[Any]("42", n)) :+ Array[Any](null, null)) ++ nodes.map(n => Array[Any]("43", n))
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("conditional apply on non-nullable node") {
    val nodeCount = sizeHint
    val (nodes, _) = givenGraph {
      circleGraph(nodeCount, "L")
    }

    // when
    val limit = 2
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .conditionalApply("x")
      .|.limit(limit)
      .|.expand("(y)--(z)")
      .|.nodeByLabelScan("y", "L", IndexOrderNone, "x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(nodes.flatMap(n => Seq.fill(limit)(Array[Any](n))))
  }

  test("conditional apply on the RHS of an apply") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint)
      nodeGraph(sizeHint, "RHS")
    }
    val lhsRows = inputValues(Array("42"), Array(null), Array("43"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.conditionalApply("x")
      .|.|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .|.filter("x = '42' OR x IS NULL")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expected = nodes.map(n => Array[Any]("42", n)) :+ Array[Any](null, null)
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("conditional apply on the RHS of an apply - with sorts") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint)
      nodeGraph(sizeHint, "RHS")
    }
    val lhsRows = inputValues(Array("42"), Array(null), Array("43"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .sort("x ASC")
      .apply()
      .|.sort("x ASC")
      .|.conditionalApply("x")
      .|.|.sort("y ASC")
      .|.|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .|.filter("x = '42' OR x IS NULL")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expected = nodes.map(n => Array[Any]("42", n)) :+ Array[Any](null, null)
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("conditional apply with limit on rhs") {
    val limit = 10

    val unfilteredNodes = givenGraph {
      val size = 100
      val nodes = nodeGraph(size)
      randomlyConnect(nodes, Connectivity(1, limit, "REL"))
      nodes
    }

    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.3)
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .conditionalApply("x")
      .|.limit(limit)
      .|.expandInto("(y)--(x)")
      .|.allNodeScan("y", "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expectedRowCountsFromRhs = for {
      x <- nodes
      if x != null
      subquery = for {
        y <- unfilteredNodes
        rel <- y.getRelationships.asScala if rel.getOtherNode(y) == x
      } yield Array(x)
    } yield math.min(subquery.size, limit)

    val expectedRowCount = expectedRowCountsFromRhs.sum + nodes.count(_ == null)
    runtimeResult should beColumns("x").withRows(rowCount(expectedRowCount))
  }

  test("conditional apply with limit on rhs - with sorts") {
    val limit = 10

    val unfilteredNodes = givenGraph {
      val size = 100
      val nodes = nodeGraph(size)
      randomlyConnect(nodes, Connectivity(1, limit, "REL"))
      nodes
    }

    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.3)
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort("x ASC")
      .conditionalApply("x")
      .|.sort("y ASC")
      .|.limit(limit)
      .|.expandInto("(y)--(x)")
      .|.allNodeScan("y", "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expectedRowCountsFromRhs = for {
      x <- nodes
      if x != null
      subquery = for {
        y <- unfilteredNodes
        rel <- y.getRelationships.asScala if rel.getOtherNode(y) == x
      } yield Array(x)
    } yield math.min(subquery.size, limit)

    val expectedRowCount = expectedRowCountsFromRhs.sum + nodes.count(_ == null)
    runtimeResult should beColumns("x").withRows(rowCount(expectedRowCount))
  }

  test("should support limit on top of conditional apply") {
    // given
    val nodesPerLabel = 50
    val (nodes, _) = givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }
    val input = inputColumns(100000, 3, i => nodes(i % nodes.size)).stream()

    val limit = nodesPerLabel * nodesPerLabel - 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .limit(limit)
      .conditionalApply("x")
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .input(nodes = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)

    runtimeResult should beColumns("x").withRows(rowCount(limit))
    if (!isParallel) {
      // parallel runtime may exhaust input before cancellation kicks in (depends on fusing, morsel size, parallelism)
      input.hasMore shouldBe true
    }
  }

  test("should support reduce -> limit on the RHS of conditional apply") {
    // given
    val nodesPerLabel = 100
    val (aNodes, bNodes) = givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .conditionalApply("x")
      .|.limit(10)
      .|.sort("y ASC")
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)

    val expected = for {
      x <- aNodes
      _ <- 1 to 10
    } yield Array[Any](x)

    runtimeResult should beColumns("x").withRows(expected)
  }

  test("should aggregation on top of conditional apply with expand and limit and aggregation on rhs of apply") {
    // given
    val nodesPerLabel = 10
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val limit = nodesPerLabel / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("counts")
      .aggregation(Seq.empty, Seq("count(x) AS counts"))
      .conditionalApply("x")
      .|.aggregation(Seq.empty, Seq("count(*) AS c"))
      .|.limit(limit)
      .|.expand("(x)-[:R]->(y)")
      .|.argument("x")
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("counts").withSingleRow(nodesPerLabel)
  }

  test("should aggregate with no grouping on top of conditional apply with expand on RHS") {
    // given
    val nodesPerLabel = 10
    val (aNodes, bNodes) = givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("ys")
      .aggregation(Seq.empty, Seq("count(y) AS ys"))
      .conditionalApply("x")
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("ys").withSingleRow(aNodes.size * bNodes.size)
  }

  test("should aggregate on top of conditional apply with expand on RHS") {
    // given
    val nodesPerLabel = 10
    val (aNodes, bNodes) = givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "xs")
      .aggregation(Seq("x AS x"), Seq("count(x) AS xs"))
      .conditionalApply("x")
      .|.expandAll("(x)-->(y)")
      .|.argument("x")
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = aNodes.map(x => Array[Any](x, bNodes.size))
    runtimeResult should beColumns("x", "xs").withRows(expected)
  }

  test("should aggregate on top of conditional apply with expand on RHS with nulls") {
    // given
    val nodesPerLabel = 10
    val (aNodes, bNodes) = givenGraph {
      bipartiteGraph(
        nodesPerLabel,
        "A",
        "B",
        "R",
        { case i if i % 2 == 0 => Map("prop" -> i) }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "ys")
      .aggregation(Seq("x AS x"), Seq("count(y) AS ys"))
      .conditionalApply("prop")
      .|.expandAll("(x)-->(y)")
      .|.argument("x")
      .projection("x AS x", "x.prop AS prop")
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = aNodes.map {
      case x if x.hasProperty("prop") => Array[Any](x, bNodes.size)
      case x                          => Array[Any](x, 0)
    }
    runtimeResult should beColumns("x", "ys").withRows(expected)
  }

  test("should handle conditional apply on top of distinct") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .conditionalApply("x")
      .|.argument()
      .distinct("x AS x")
      .input(variables = Seq("x"))
      .build()

    val result = execute(logicalQuery, runtime, inputValues(Array(1), Array(2), Array(1), Array(4)))

    // then
    result should beColumns("x").withRows(Seq(Array(1), Array(2), Array(4)))
  }

  test("should handle cachedProperties on RHS") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 2 == 0 => Map("prop1" -> i)
          case i               => Map("prop1" -> i, "prop2" -> i)
        }
      )
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop1")
      .projection("cache[x.prop1] AS prop1")
      .conditionalApply("prop2")
      .|.cacheProperties("cache[x.prop1]")
      .|.argument("x")
      .projection("x.prop2 AS prop2")
      .allNodeScan("x")
      .build()

    val result = execute(logicalQuery, runtime)

    // then
    result should beColumns("prop1").withRows((0 until sizeHint).map(Array[Any](_)))
  }

  test("exhaustive limit after conditionalApply on the RHS of apply") {
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
      .|.|.conditionalApply("i")
      .|.|.|.argument("x", "i")
      .|.|.argument("x", "i")
      .|.unwind("[1, null] AS i")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(singleColumn(inputRows.map(_(0))))
  }

  test("limit after conditionalApply on the RHS of apply") {
    // given
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.limit(1)
      .|.apply()
      .|.|.conditionalApply("i")
      .|.|.|.argument("x", "i")
      .|.|.argument("x", "i")
      .|.unwind("[1, null] AS i")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(singleColumn(inputRows.map(_(0))))
  }

  test("node hash join on RHS of conditionalApply") {
    // given
    givenGraph {
      nodeGraph(10)
    }

    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.conditionalApply("i")
      // NOTE: leave commented out lines, they show how pipelined runtime rewrites this query
      //      .|.|.orderedUnion()
      //      .|.|.|.limit(1)
      //      .|.|.|.nodeHashJoin("n")
      //      .|.|.|.|.allNodeScan("n")
      //      .|.|.|.allNodeScan("n")
      //      .|.|.argument()
      .|.|.limit(1)
      .|.|.nodeHashJoin("n")
      .|.|.|.allNodeScan("n")
      .|.|.allNodeScan("n")
      .|.unwind("[1,null,2,null,3,null] AS i")
      .|.argument()
      .input(variables = Seq("x"))
      .build()

    val expected = for {
      x <- inputRows
      i <- Seq[Any](1, null, 2, null, 3, null)
    } yield x

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("union on RHS of conditionalApply") {
    // given
    givenGraph {
      nodeGraph(sizeHint)
    }

    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.conditionalApply("i")
      // NOTE: leave commented out lines, they show how pipelined runtime rewrites this query
      //      .|.|.orderedUnion()
      //      .|.|.|.limit(1)
      //      .|.|.|.union()
      //      .|.|.|.|.allNodeScan("n")
      //      .|.|.|.allNodeScan("n")
      //      .|.|.argument()
      .|.|.limit(1)
      .|.|.union()
      .|.|.|.allNodeScan("n")
      .|.|.allNodeScan("n")
      .|.unwind("[1,null,2,null,3,null] AS i")
      .|.argument()
      .input(variables = Seq("x"))
      .build()

    val expected = for {
      x <- inputRows
      i <- Seq[Any](1, null, 2, null, 3, null)
    } yield x

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("cartesian product on RHS of conditionalApply") {
    // given
    givenGraph {
      nodeGraph(sizeHint)
    }

    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.conditionalApply("i")
      // NOTE: leave commented out lines, they show how pipelined runtime rewrites this query
      //      .|.|.orderedUnion()
      //      .|.|.|.limit(1)
      //      .|.|.|.cartesianProduct()
      //      .|.|.|.|.allNodeScan("n")
      //      .|.|.|.allNodeScan("n")
      //      .|.|.argument()
      .|.|.limit(1)
      .|.|.cartesianProduct()
      .|.|.|.allNodeScan("m")
      .|.|.allNodeScan("n")
      .|.unwind("[1,null,2,null,3,null] AS i")
      .|.argument()
      .input(variables = Seq("x"))
      .build()

    val expected = for {
      x <- inputRows
      i <- Seq[Any](1, null, 2, null, 3, null)
    } yield x

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("node hash join on RHS of conditionalApply plus sorts") {
    // given
    givenGraph {
      nodeGraph(10)
    }

    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort("n ASC")
      .apply()
      .|.sort("n ASC")
      .|.conditionalApply("i")
      // NOTE: leave commented out lines, they show how pipelined runtime rewrites this query
      //      .|.|.orderedUnion()
      //      .|.|.|.limit(1)
      //      .|.|.|.nodeHashJoin("n")
      //      .|.|.|.|.allNodeScan("n")
      //      .|.|.|.allNodeScan("n")
      //      .|.|.argument()
      .|.|.sort("n ASC")
      .|.|.limit(1)
      .|.|.nodeHashJoin("n")
      .|.|.|.allNodeScan("n")
      .|.|.allNodeScan("n")
      .|.unwind("[1,null,2,null,3,null] AS i")
      .|.argument()
      .input(variables = Seq("x"))
      .build()

    val expected = for {
      x <- inputRows
      i <- Seq[Any](1, null, 2, null, 3, null)
    } yield x

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("union on RHS of conditionalApply plus sorts") {
    // given
    givenGraph {
      nodeGraph(sizeHint)
    }

    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort("n ASC")
      .apply()
      .|.sort("n ASC")
      .|.conditionalApply("i")
      // NOTE: leave commented out lines, they show how pipelined runtime rewrites this query
      //      .|.|.orderedUnion()
      //      .|.|.|.limit(1)
      //      .|.|.|.union()
      //      .|.|.|.|.allNodeScan("n")
      //      .|.|.|.allNodeScan("n")
      //      .|.|.argument()
      .|.|.sort("n ASC")
      .|.|.limit(1)
      .|.|.union()
      .|.|.|.allNodeScan("n")
      .|.|.allNodeScan("n")
      .|.unwind("[1,null,2,null,3,null] AS i")
      .|.argument()
      .input(variables = Seq("x"))
      .build()

    val expected = for {
      x <- inputRows
      i <- Seq[Any](1, null, 2, null, 3, null)
    } yield x

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("cartesian product on RHS of conditionalApply plus sorts") {
    // given
    givenGraph {
      nodeGraph(sizeHint)
    }

    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort("n ASC")
      .apply()
      .|.sort("n ASC")
      .|.conditionalApply("i")
      // NOTE: leave commented out lines, they show how pipelined runtime rewrites this query
      //      .|.|.orderedUnion()
      //      .|.|.|.limit(1)
      //      .|.|.|.cartesianProduct()
      //      .|.|.|.|.allNodeScan("n")
      //      .|.|.|.allNodeScan("n")
      //      .|.|.argument()
      .|.|.sort("n ASC")
      .|.|.limit(1)
      .|.|.cartesianProduct()
      .|.|.|.allNodeScan("m")
      .|.|.allNodeScan("n")
      .|.unwind("[1,null,2,null,3,null] AS i")
      .|.argument()
      .input(variables = Seq("x"))
      .build()

    val expected = for {
      x <- inputRows
      i <- Seq[Any](1, null, 2, null, 3, null)
    } yield x

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("chained node hash join on RHS of conditionalApply plus sorts") {
    // given
    givenGraph {
      nodeGraph(10)
    }

    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort("n ASC")
      .apply()
      .|.sort("m ASC")
      .|.conditionalApply("j")
      // NOTE: leave commented out lines, they show how pipelined runtime rewrites this query
      //      .|.|.orderedUnion()
      //      .|.|.|.limit(1)
      //      .|.|.|.nodeHashJoin("m")
      //      .|.|.|.|.allNodeScan("m")
      //      .|.|.|.allNodeScan("m")
      //      .|.|.argument()
      .|.|.sort("m ASC")
      .|.|.limit(1)
      .|.|.nodeHashJoin("m")
      .|.|.|.allNodeScan("m")
      .|.|.allNodeScan("m")
      .|.unwind("[1,null,2,null,3,null] AS j")
      .|.argument()
      .sort("n ASC")
      .apply()
      .|.sort("n ASC")
      .|.conditionalApply("i")
      // NOTE: leave commented out lines, they show how pipelined runtime rewrites this query
      //      .|.|.orderedUnion()
      //      .|.|.|.limit(1)
      //      .|.|.|.nodeHashJoin("n")
      //      .|.|.|.|.allNodeScan("n")
      //      .|.|.|.allNodeScan("n")
      //      .|.|.argument()
      .|.|.sort("n ASC")
      .|.|.limit(1)
      .|.|.nodeHashJoin("n")
      .|.|.|.allNodeScan("n")
      .|.|.allNodeScan("n")
      .|.unwind("[1,null,2,null,3,null] AS i")
      .|.argument()
      .input(variables = Seq("x"))
      .build()

    val expected = for {
      x <- inputRows
      _ <- Seq[Any](1, null, 2, null, 3, null)
      _ <- Seq[Any](1, null, 2, null, 3, null)
    } yield x

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("chained union on RHS of conditionalApply plus sorts") {
    // given
    givenGraph {
      nodeGraph(sizeHint)
    }

    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort("n ASC")
      .apply()
      .|.sort("m ASC")
      .|.conditionalApply("j")
      // NOTE: leave commented out lines, they show how pipelined runtime rewrites this query
      //      .|.|.orderedUnion()
      //      .|.|.|.limit(1)
      //      .|.|.|.union()
      //      .|.|.|.|.allNodeScan("m")
      //      .|.|.|.allNodeScan("m")
      //      .|.|.argument()
      .|.|.sort("m ASC")
      .|.|.limit(1)
      .|.|.union()
      .|.|.|.allNodeScan("m")
      .|.|.allNodeScan("m")
      .|.unwind("[1,null,2,null,3,null] AS j")
      .|.argument()
      .sort("n ASC")
      .apply()
      .|.sort("n ASC")
      .|.conditionalApply("i")
      // NOTE: leave commented out lines, they show how pipelined runtime rewrites this query
      //      .|.|.orderedUnion()
      //      .|.|.|.limit(1)
      //      .|.|.|.union()
      //      .|.|.|.|.allNodeScan("n")
      //      .|.|.|.allNodeScan("n")
      //      .|.|.argument()
      .|.|.sort("n ASC")
      .|.|.limit(1)
      .|.|.union()
      .|.|.|.allNodeScan("n")
      .|.|.allNodeScan("n")
      .|.unwind("[1,null,2,null,3,null] AS i")
      .|.argument()
      .input(variables = Seq("x"))
      .build()

    val expected = for {
      x <- inputRows
      _ <- Seq[Any](1, null, 2, null, 3, null)
      _ <- Seq[Any](1, null, 2, null, 3, null)
    } yield x

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("should work when nullable variable is aliased on RHS") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "y2")
      .conditionalApply("x")
      .|.projection("y AS y2")
      .|.argument()
      .projection("null AS x", "1 AS y")
      .argument()
      .build()

    execute(query, runtime) should beColumns("x", "y", "y2").withSingleRow(null, 1, null)
  }

  test("should work when nullable variable is aliased on RHS of Apply under ConditionalApply") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "y2")
      .conditionalApply("x")
      .|.apply()
      .|.|.projection("y AS y2")
      .|.|.argument()
      .|.argument()
      .projection("null AS x", "1 AS y")
      .argument()
      .build()

    execute(query, runtime) should beColumns("x", "y", "y2").withSingleRow(null, 1, null)
  }

  test("should work when nullable variable is aliased on RHS of ConditionalApply under Apply") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "y2")
      .apply()
      .|.conditionalApply("x")
      .|.|.projection("y AS y2")
      .|.|.argument()
      .|.argument()
      .projection("null AS x", "1 AS y")
      .argument()
      .build()

    execute(query, runtime) should beColumns("x", "y", "y2").withSingleRow(null, 1, null)
  }

  test("unwind on top with conditionalApply on the RHS of apply") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint)
      nodeGraph(sizeHint, "RHS")
    }
    val lhsRows = inputValues(Array("42"), Array(null), Array("43"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "x", "y")
      .unwind("[1,2] as a")
      .apply()
      .|.conditionalApply("x")
      .|.|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .|.filter("x = '42' OR x IS NULL")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expected = Seq(1, 2).flatMap(a => nodes.map(n => Array[Any](a, "42", n)) :+ Array[Any](a, null, null))
    runtimeResult should beColumns("a", "x", "y").withRows(expected)
  }
}

trait OrderedConditionalApplyTestBase[CONTEXT <: RuntimeContext] {
  self: ConditionalApplyTestBase[CONTEXT] =>

  test("conditional apply on the RHS of an apply should keep row order") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint)
      nodeGraph(sizeHint, "RHS")
    }
    val lhsRows = inputValues(Array("42"), Array(null), Array("43"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.conditionalApply("x")
      .|.|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .|.filter("x = '42' OR x IS NULL")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expected = nodes.map(n => Array[Any]("42", n)) :+ Array[Any](null, null)
    runtimeResult should beColumns("x", "y").withRows(inOrder(expected))
  }

  test("conditional apply should not run rhs if lhs is empty - with leveraged order") {
    // given
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .conditionalApply("x").withLeveragedOrder()
      .|.filter("1/0 > 0")
      .|.allNodeScan("y", "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // should not throw 1/0 exception
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("conditional apply on nonempty lhs and empty rhs, where condition(lhs) always is true - with leveraged order") {
    // given
    val nodes = givenGraph {
      nodeGraph(19, "RHS")
      nodeGraph(sizeHint)
    }
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .conditionalApply("x").withLeveragedOrder()
      .|.expandInto("(y)--(x)")
      .|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // because graph contains no relationships, the expand will return no rows
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("conditional apply on nonempty lhs and empty rhs - with leveraged order") {
    // given
    givenGraph {
      nodeGraph(19, "RHS")
      nodeGraph(sizeHint)
    }
    val lhsRows = inputValues(Array("42"), Array(null), Array("43"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .conditionalApply("x").withLeveragedOrder()
      .|.filter("false")
      .|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // will only return lhs where condition(lhs) is false
    runtimeResult should beColumns("x", "y").withSingleRow(null, null)
  }

  test("conditional apply on nonempty lhs and nonempty rhs - with leveraged order") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint)
      nodeGraph(sizeHint, "RHS")
    }
    val lhsRows = inputValues(Array("42"), Array(null), Array("43"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .conditionalApply("x").withLeveragedOrder()
      .|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expected =
      (nodes.map(n => Array[Any]("42", n)) :+ Array[Any](null, null)) ++ nodes.map(n => Array[Any]("43", n))
    runtimeResult should beColumns("x", "y").withRows(inOrder(expected))
  }

  test("conditional apply on non-nullable node - with leveraged order") {
    val nodeCount = sizeHint
    val (nodes, _) = givenGraph {
      circleGraph(nodeCount, "L")
    }

    // when
    val limit = 2
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .conditionalApply("x").withLeveragedOrder()
      .|.limit(limit)
      .|.expand("(y)--(z)")
      .|.nodeByLabelScan("y", "L", IndexOrderNone, "x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(inOrder(nodes.flatMap(n => Seq.fill(limit)(Array[Any](n)))))
  }

  test("conditional apply on the RHS of an apply - with leveraged order") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint)
      nodeGraph(sizeHint, "RHS")
    }
    val lhsRows = inputValues(Array("42"), Array(null), Array("43"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.conditionalApply("x").withLeveragedOrder()
      .|.|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .|.filter("x = '42' OR x IS NULL")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expected = nodes.map(n => Array[Any]("42", n)) :+ Array[Any](null, null)
    runtimeResult should beColumns("x", "y").withRows(inOrder(expected))
  }

  test("conditional apply with limit on rhs - with leveraged order") {
    val limit = 10

    val unfilteredNodes = givenGraph {
      val size = 100
      val nodes = nodeGraph(size)
      randomlyConnect(nodes, Connectivity(1, limit, "REL"))
      nodes
    }

    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.3)
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .conditionalApply("x").withLeveragedOrder()
      .|.limit(limit)
      .|.expandInto("(y)--(x)")
      .|.allNodeScan("y", "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expectedRowCountsFromRhs = for {
      x <- nodes
      if x != null
      subquery = for {
        y <- unfilteredNodes
        rel <- y.getRelationships.asScala if rel.getOtherNode(y) == x
      } yield Array(x)
    } yield math.min(subquery.size, limit)

    val expectedRowCount = expectedRowCountsFromRhs.sum + nodes.count(_ == null)
    runtimeResult should beColumns("x").withRows(rowCount(expectedRowCount))
  }

  test("should support limit on top of conditional apply - with leveraged order") {
    // given
    val nodesPerLabel = 50
    val (nodes, _) = givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }
    val input = inputColumns(100000, 3, i => nodes(i % nodes.size)).stream()

    val limit = nodesPerLabel * nodesPerLabel - 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .limit(limit)
      .conditionalApply("x").withLeveragedOrder()
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .input(nodes = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)

    runtimeResult should beColumns("x").withRows(rowCount(limit))
    if (!isParallel) {
      // parallel runtime may exhaust input before cancellation kicks in (depends on fusing, morsel size, parallelism)
      input.hasMore shouldBe true
    }
  }

  test("should support reduce -> limit on the RHS of conditional apply - with leveraged order") {
    // given
    val nodesPerLabel = 100
    val (aNodes, bNodes) = givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .conditionalApply("x").withLeveragedOrder()
      .|.limit(10)
      .|.sort("y ASC")
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)

    val expected = for {
      x <- aNodes
      _ <- 1 to 10
    } yield Array[Any](x)

    runtimeResult should beColumns("x").withRows(expected)
  }

  test(
    "should aggregation on top of conditional apply with expand and limit and aggregation on rhs of apply - with leveraged order"
  ) {
    // given
    val nodesPerLabel = 10
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val limit = nodesPerLabel / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("counts")
      .aggregation(Seq.empty, Seq("count(x) AS counts"))
      .conditionalApply("x").withLeveragedOrder()
      .|.aggregation(Seq.empty, Seq("count(*) AS c"))
      .|.limit(limit)
      .|.expand("(x)-[:R]->(y)")
      .|.argument("x")
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("counts").withSingleRow(nodesPerLabel)
  }

  test("should aggregate with no grouping on top of conditional apply with expand on RHS - with leveraged order") {
    // given
    val nodesPerLabel = 10
    val (aNodes, bNodes) = givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("ys")
      .aggregation(Seq.empty, Seq("count(y) AS ys"))
      .conditionalApply("x").withLeveragedOrder()
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("ys").withSingleRow(aNodes.size * bNodes.size)
  }

  test("should aggregate on top of conditional apply with expand on RHS - with leveraged order") {
    // given
    val nodesPerLabel = 10
    val (aNodes, bNodes) = givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "xs")
      .aggregation(Seq("x AS x"), Seq("count(x) AS xs"))
      .conditionalApply("x").withLeveragedOrder()
      .|.expandAll("(x)-->(y)")
      .|.argument("x")
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = aNodes.map(x => Array[Any](x, bNodes.size))
    runtimeResult should beColumns("x", "xs").withRows(expected)
  }

  test("should aggregate on top of conditional apply with expand on RHS with nulls - with leveraged order") {
    // given
    val nodesPerLabel = 10
    val (aNodes, bNodes) = givenGraph {
      bipartiteGraph(
        nodesPerLabel,
        "A",
        "B",
        "R",
        { case i if i % 2 == 0 => Map("prop" -> i) }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "ys")
      .aggregation(Seq("x AS x"), Seq("count(y) AS ys"))
      .conditionalApply("prop").withLeveragedOrder()
      .|.expandAll("(x)-->(y)")
      .|.argument("x")
      .projection("x AS x", "x.prop AS prop")
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = aNodes.map {
      case x if x.hasProperty("prop") => Array[Any](x, bNodes.size)
      case x                          => Array[Any](x, 0)
    }
    runtimeResult should beColumns("x", "ys").withRows(expected)
  }

  test("should handle conditional apply on top of distinct - with leveraged order") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .conditionalApply("x").withLeveragedOrder()
      .|.argument()
      .distinct("x AS x")
      .input(variables = Seq("x"))
      .build()

    val result = execute(logicalQuery, runtime, inputValues(Array(1), Array(2), Array(1), Array(4)))

    // then
    result should beColumns("x").withRows(Seq(Array(1), Array(2), Array(4)))
  }

  test("should handle cachedProperties on RHS - with leveraged order") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 2 == 0 => Map("prop1" -> i)
          case i               => Map("prop1" -> i, "prop2" -> i)
        }
      )
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("prop1")
      .projection("cache[x.prop1] AS prop1")
      .conditionalApply("prop2").withLeveragedOrder()
      .|.cacheProperties("cache[x.prop1]")
      .|.argument("x")
      .projection("x.prop2 AS prop2")
      .allNodeScan("x")
      .build()

    val result = execute(logicalQuery, runtime)

    // then
    result should beColumns("prop1").withRows((0 until sizeHint).map(Array[Any](_)))
  }

  test("limit after conditionalApply on the RHS of apply - with leveraged order") {
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
      .|.|.conditionalApply("i").withLeveragedOrder()
      .|.|.|.argument("x", "i")
      .|.|.argument("x", "i")
      .|.unwind("[1, null] AS i")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(singleColumn(inputRows.map(_(0))))
  }

  test("node hash join on RHS of conditionalApply plus sorts - with leveraged order") {
    // given
    givenGraph {
      nodeGraph(10)
    }

    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort("n ASC")
      .apply()
      .|.sort("n ASC")
      .|.conditionalApply("i").withLeveragedOrder()
      // NOTE: leave commented out lines, they show how pipelined runtime rewrites this query
      //      .|.|.orderedUnion()
      //      .|.|.|.limit(1)
      //      .|.|.|.nodeHashJoin("n")
      //      .|.|.|.|.allNodeScan("n")
      //      .|.|.|.allNodeScan("n")
      //      .|.|.argument()
      .|.|.sort("n ASC")
      .|.|.limit(1)
      .|.|.nodeHashJoin("n")
      .|.|.|.allNodeScan("n")
      .|.|.allNodeScan("n")
      .|.unwind("[1,null,2,null,3,null] AS i")
      .|.argument()
      .input(variables = Seq("x"))
      .build()

    val expected = for {
      x <- inputRows
      i <- Seq[Any](1, null, 2, null, 3, null)
    } yield x

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("ordered union on RHS of conditionalApply plus sorts - with leveraged order") {
    // given
    givenGraph {
      nodeGraph(sizeHint)
    }

    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort("n ASC")
      .apply()
      .|.sort("n ASC")
      .|.conditionalApply("i").withLeveragedOrder()
      // NOTE: leave commented out lines, they show how pipelined runtime rewrites this query
      //      .|.|.orderedUnion()
      //      .|.|.|.limit(1)
      //      .|.|.|.orderedUnion()
      //      .|.|.|.|.allNodeScan("n")
      //      .|.|.|.allNodeScan("n")
      //      .|.|.argument()
      .|.|.sort("n ASC")
      .|.|.limit(1)
      .|.|.orderedUnion("n ASC")
      .|.|.|.allNodeScan("n")
      .|.|.allNodeScan("n")
      .|.unwind("[1,null,2,null,3,null] AS i")
      .|.argument()
      .input(variables = Seq("x"))
      .build()

    val expected = for {
      x <- inputRows
      i <- Seq[Any](1, null, 2, null, 3, null)
    } yield x

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("cartesian product on RHS of conditionalApply plus sorts - with leveraged order") {
    // given
    givenGraph {
      nodeGraph(sizeHint)
    }

    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort("n ASC")
      .apply()
      .|.sort("n ASC")
      .|.conditionalApply("i").withLeveragedOrder()
      // NOTE: leave commented out lines, they show how pipelined runtime rewrites this query
      //      .|.|.orderedUnion()
      //      .|.|.|.limit(1)
      //      .|.|.|.cartesianProduct()
      //      .|.|.|.|.allNodeScan("n")
      //      .|.|.|.allNodeScan("n")
      //      .|.|.argument()
      .|.|.sort("n ASC")
      .|.|.limit(1)
      .|.|.cartesianProduct()
      .|.|.|.allNodeScan("m")
      .|.|.allNodeScan("n")
      .|.unwind("[1,null,2,null,3,null] AS i")
      .|.argument()
      .input(variables = Seq("x"))
      .build()

    val expected = for {
      x <- inputRows
      i <- Seq[Any](1, null, 2, null, 3, null)
    } yield x

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("chained node hash join on RHS of conditionalApply plus sorts - with leveraged order") {
    // given
    givenGraph {
      nodeGraph(10)
    }

    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort("n ASC")
      .apply()
      .|.sort("m ASC")
      .|.conditionalApply("j").withLeveragedOrder()
      // NOTE: leave commented out lines, they show how pipelined runtime rewrites this query
      //      .|.|.orderedUnion()
      //      .|.|.|.limit(1)
      //      .|.|.|.nodeHashJoin("m")
      //      .|.|.|.|.allNodeScan("m")
      //      .|.|.|.allNodeScan("m")
      //      .|.|.argument()
      .|.|.sort("m ASC")
      .|.|.limit(1)
      .|.|.nodeHashJoin("m")
      .|.|.|.allNodeScan("m")
      .|.|.allNodeScan("m")
      .|.unwind("[1,null,2,null,3,null] AS j")
      .|.argument()
      .sort("n ASC")
      .apply()
      .|.sort("n ASC")
      .|.conditionalApply("i").withLeveragedOrder()
      // NOTE: leave commented out lines, they show how pipelined runtime rewrites this query
      //      .|.|.orderedUnion()
      //      .|.|.|.limit(1)
      //      .|.|.|.nodeHashJoin("n")
      //      .|.|.|.|.allNodeScan("n")
      //      .|.|.|.allNodeScan("n")
      //      .|.|.argument()
      .|.|.sort("n ASC")
      .|.|.limit(1)
      .|.|.nodeHashJoin("n")
      .|.|.|.allNodeScan("n")
      .|.|.allNodeScan("n")
      .|.unwind("[1,null,2,null,3,null] AS i")
      .|.argument()
      .input(variables = Seq("x"))
      .build()

    val expected = for {
      x <- inputRows
      _ <- Seq[Any](1, null, 2, null, 3, null)
      _ <- Seq[Any](1, null, 2, null, 3, null)
    } yield x

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("chained ordered union on RHS of conditionalApply plus sorts - with leveraged order") {
    // given
    givenGraph {
      nodeGraph(sizeHint)
    }

    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort("n ASC")
      .apply()
      .|.sort("m ASC")
      .|.conditionalApply("j").withLeveragedOrder()
      // NOTE: leave commented out lines, they show how pipelined runtime rewrites this query
      //      .|.|.orderedUnion()
      //      .|.|.|.limit(1)
      //      .|.|.|.orderedUnion("m")
      //      .|.|.|.|.allNodeScan("m")
      //      .|.|.|.allNodeScan("m")
      //      .|.|.argument()
      .|.|.sort("m ASC")
      .|.|.limit(1)
      .|.|.orderedUnion("m ASC")
      .|.|.|.allNodeScan("m")
      .|.|.allNodeScan("m")
      .|.unwind("[1,null,2,null,3,null] AS j")
      .|.argument()
      .sort("n ASC")
      .apply()
      .|.sort("n ASC")
      .|.conditionalApply("i").withLeveragedOrder()
      // NOTE: leave commented out lines, they show how pipelined runtime rewrites this query
      //      .|.|.orderedUnion()
      //      .|.|.|.limit(1)
      //      .|.|.|.orderedUnion("n")
      //      .|.|.|.|.allNodeScan("n")
      //      .|.|.|.allNodeScan("n")
      //      .|.|.argument()
      .|.|.sort("n ASC")
      .|.|.limit(1)
      .|.|.orderedUnion("n ASC")
      .|.|.|.allNodeScan("n")
      .|.|.allNodeScan("n")
      .|.unwind("[1,null,2,null,3,null] AS i")
      .|.argument()
      .input(variables = Seq("x"))
      .build()

    val expected = for {
      x <- inputRows
      _ <- Seq[Any](1, null, 2, null, 3, null)
      _ <- Seq[Any](1, null, 2, null, 3, null)
    } yield x

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("unwind on top with conditionalApply on the RHS of apply - with leveraged order") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint)
      nodeGraph(sizeHint, "RHS")
    }
    val lhsRows = inputValues(Array("42"), Array(null), Array("43"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "x", "y")
      .unwind("[1,2] as a")
      .apply()
      .|.conditionalApply("x").withLeveragedOrder()
      .|.|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .|.filter("x = '42' OR x IS NULL")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expected =
      (for {
        y <- nodes
        a <- 1 to 2
        x = "42"
      } yield Array[Any](a, x, y)) :+ Array[Any](1, null, null) :+ Array[Any](2, null, null)
    runtimeResult should beColumns("a", "x", "y").withRows(inOrder(expected))
  }
}
