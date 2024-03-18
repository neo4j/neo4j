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
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.TestSubscriber
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriter
import org.neo4j.graphdb.Direction

import scala.jdk.CollectionConverters.IterableHasAsScala

abstract class CartesianProductTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("handle cached properties and cartesian product on LHS of apply") {
    // given
    nodeIndex("Label", "prop")
    val nodes = givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("prop" -> i)
        },
        "Label"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .apply()
      .|.cartesianProduct()
      .|.|.argument("n")
      .|.argument("n")
      .nodeIndexOperator("n:Label(prop)", getValue = _ => GetValue)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n").withRows(singleColumn(nodes))
  }

  test("should handle multiple columns ") {
    // given
    val size = 10 // sizehint is a bit too big here
    val nodes = givenGraph {
      val nodes = nodePropertyGraph(
        size,
        {
          case _ => Map("prop" -> "foo")
        }
      )
      val relTuples = (for (i <- 0 until size) yield {
        Seq(
          (i, (i + 1) % size, "R")
        )
      }).reduce(_ ++ _)
      connect(nodes, relTuples)
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .cartesianProduct()
      .|.expand("(e)-[r4]->(f)")
      .|.expand("(d)-[r3]->(e)")
      .|.allNodeScan("d")
      .expand("(b)-[r2]->(c)")
      .expand("(a)-[r1]->(b)")
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(c => (1 to size).map(_ => Array(c)))
    runtimeResult should beColumns("c").withRows(expected)
  }

  test("should handle different cached-properties on lhs and rhs of cartesian product") {
    givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("aProp", "bProp")
      .projection("cache[a.prop] AS aProp", "cache[b.prop] AS bProp")
      .cartesianProduct()
      .|.filter("cache[b.prop] < 5")
      .|.allNodeScan("b")
      .filter("cache[a.prop] < 10")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      i <- 0 to 9
      j <- 0 to 4
    } yield Array(i, j)
    runtimeResult should beColumns("aProp", "bProp").withRows(expected)
  }

  test("should handle cached properties from both lhs and rhs") {
    val nodes = givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        }
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("ab")
      .apply()
      .|.cartesianProduct()
      .|.|.filter("cache[ab.prop] = cache[b.prop]")
      .|.|.allNodeScan("b")
      .|.filter("cache[ab.prop] < 10")
      .|.argument()
      .projection("coalesce(a, null) AS ab")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    val expected = nodes.map(n => Array(n)).take(10)
    runtimeResult should beColumns("ab").withRows(expected)
  }

  test("cartesian product after expand on empty lhs") {
    // given
    givenGraph { circleGraph(sizeHint) }
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .cartesianProduct()
      .|.expand("(y)--(z)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x", "y", "z").withNoRows()
  }

  test("cartesian product with aggregation on RHS") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "ys")
      .cartesianProduct()
      .|.aggregation(Seq.empty, Seq("count(y) AS ys"))
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(x => Array[Any](x, sizeHint))
    runtimeResult should beColumns("x", "ys").withRows(expected)
  }

  test("cartesian product on empty rhs") {
    // given
    val nodes = givenGraph {
      nodeGraph(19, "RHS")
      nodeGraph(sizeHint)
    }
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .cartesianProduct()
      .|.expand("(y)--(z)")
      .|.nodeByLabelScan("y", "RHS", IndexOrderNone)
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // because graph contains no relationships, the expand will return no rows
    runtimeResult should beColumns("x", "y", "z").withNoRows()
  }

  test("cartesian product on empty lhs and rhs") {
    // given
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .cartesianProduct()
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("cartesian product after expand on rhs") {
    val (unfilteredNodes, _) = givenGraph { circleGraph(Math.sqrt(sizeHint).toInt) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.3)
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .cartesianProduct()
      .|.expand("(y)--(z)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expectedResultRows = for {
      x <- nodes
      y <- unfilteredNodes
      rel <- y.getRelationships.asScala
      z = rel.getOtherNode(y)
    } yield Array(x, y, z)

    runtimeResult should beColumns("x", "y", "z").withRows(expectedResultRows)
  }

  test("cartesian product after expand on lhs") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(Math.sqrt(sizeHint).toInt) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.3)
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .cartesianProduct()
      .|.allNodeScan("z")
      .expand("(y)--(x)")
      .input(nodes = Seq("y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expectedResultRows = for {
      y <- nodes if y != null
      rel <- y.getRelationships().asScala
      x = rel.getOtherNode(y)
      z <- unfilteredNodes
    } yield Array(x, y, z)

    runtimeResult should beColumns("x", "y", "z").withRows(expectedResultRows)
  }

  // This test mainly assert that we get the right slot configuration for Arraycopy and don't get IndexOutOfBounds there
  test("cartesian product with apply everywhere") {
    val (unfilteredNodes, _) = givenGraph { circleGraph(Math.sqrt(sizeHint).toInt) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.3)
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "z")
      .apply()
      .|.cartesianProduct()
      .|.|.apply()
      .|.|.|.expand("(z)-[q]->(w)")
      .|.|.|.argument("x", "y", "z")
      .|.|.allNodeScan("z")
      .|.expand("(x)-[p]->(v)")
      .|.argument("x", "y")
      .expand("(x)-[r]->(y)")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expectedResultRows = for {
      x <- nodes if x != null
      z <- unfilteredNodes
    } yield Array(x, z)

    runtimeResult should beColumns("x", "z").withRows(expectedResultRows)
  }

  test("cartesian product after optional on lhs") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(Math.sqrt(sizeHint).toInt) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.3)
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "z")
      .cartesianProduct()
      .|.allNodeScan("z")
      .apply()
      .|.optional("y")
      .|.expand("(y)-[r]->(a)")
      .|.argument("y")
      .input(nodes = Seq("y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expectedResultRows = for {
      y <- nodes
      z <- unfilteredNodes
    } yield Array(y, z)

    runtimeResult should beColumns("y", "z").withRows(expectedResultRows)
  }

  test("cartesian product with optional on top") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(Math.sqrt(sizeHint).toInt) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.3)
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "z")
      .apply()
      .|.optional("y", "z")
      .|.expand("(z)-[r]->(a)")
      .|.argument("y", "z")
      .cartesianProduct()
      .|.allNodeScan("z")
      .input(nodes = Seq("y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expectedResultRows = for {
      y <- nodes
      z <- unfilteredNodes
    } yield Array(y, z)

    runtimeResult should beColumns("y", "z").withRows(expectedResultRows)
  }

  test("cartesian product nested") {
    // given
    val n = Math.pow(sizeHint, 1.0 / 4).toInt * 2
    val (as, bs, cs, ds) = givenGraph {
      val as = nodeGraph(n, "A")
      val bs = nodeGraph(n, "B")
      val cs = nodeGraph(n, "C")
      val ds = nodeGraph(n, "D")
      (as, bs, cs, ds)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c", "d")
      .cartesianProduct()
      .|.cartesianProduct()
      .|.|.nodeByLabelScan("d", "D", IndexOrderNone)
      .|.nodeByLabelScan("c", "C", IndexOrderNone)
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B", IndexOrderNone)
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expectedResultRows = for {
      a <- as
      b <- bs
      c <- cs
      d <- ds
    } yield Array(a, b, c, d)

    runtimeResult should beColumns("a", "b", "c", "d").withRows(expectedResultRows)
  }

  test("cartesian product below an apply") {
    // given
    val n = Math.pow(sizeHint, 1.0 / 3).toInt * 2
    val (as, _) = givenGraph { bipartiteGraph(n, "A", "B", "REL") }
    val nodes = select(as, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "r1", "r2")
      .apply()
      .|.cartesianProduct()
      .|.|.expand("(a)-[r2]->(b2)")
      .|.|.argument("a")
      .|.expand("(a)-[r1]->(b1)")
      .|.argument("a")
      .input(nodes = Seq("a"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expectedResultRows = for {
      a <- nodes if a != null
      r1 <- a.getRelationships(Direction.OUTGOING).asScala
      r2 <- a.getRelationships(Direction.OUTGOING).asScala
    } yield Array(a, r1, r2)

    runtimeResult should beColumns("a", "r1", "r2").withRows(expectedResultRows)
  }

  test("cartesian product with limit on rhs") {
    val nodesPerLabel = Math.sqrt(sizeHint).toInt
    val limit = nodesPerLabel - 1
    val (aNodes, _) = givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }
    val nodes = select(aNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.3)
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .cartesianProduct()
      .|.limit(limit)
      .|.expand("(y)--(z)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y", "z").withRows(rowCount(nodes.size * limit))
  }

  test("cartesian product with double sort and limit after join") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(Math.sqrt(sizeHint).toInt) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5)
    val limitCount = nodes.size / 2
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .limit(count = limitCount)
      .sort("x DESC", "y ASC", "z ASC")
      .sort("x ASC", "y DESC", "z DESC")
      .cartesianProduct()
      .|.expand("(y)--(z)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val allRows = for {
      x <- nodes
      y <- unfilteredNodes
      rel <- y.getRelationships().asScala
      z = rel.getOtherNode(y)
    } yield Array(x, y, z)
    val expectedResultRows = allRows.sortBy(arr => (-arr(0).getId, arr(1).getId, arr(2).getId)).take(limitCount)

    runtimeResult should beColumns("x", "y", "z").withRows(inOrder(expectedResultRows))
  }

  test("cartesian product with limit on top of join") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(Math.sqrt(sizeHint).toInt) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5)
    val limitCount = nodes.size / 2
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .limit(count = limitCount)
      .cartesianProduct()
      .|.expand("(y)--(z)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y", "z").withRows(rowCount(limitCount))
  }

  test("should support cartesian product with hash-join on RHS") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(Math.sqrt(sizeHint).toInt) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5)
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y1", "y2", "z")
      .cartesianProduct()
      .|.nodeHashJoin("z")
      .|.|.expand("(y2)-[r2]-(z)")
      .|.|.allNodeScan("y2")
      .|.expand("(y1)-[r1]->(z)")
      .|.allNodeScan("y1")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expectedResultRows =
      for {
        x <- nodes
        y1 <- unfilteredNodes
        r1 <- y1.getRelationships(Direction.OUTGOING).asScala
        z = r1.getOtherNode(y1)
        r2 <- z.getRelationships(Direction.BOTH).asScala
        y2 = r2.getOtherNode(z)
      } yield Array(x, y1, y2, r1.getOtherNode(y1))

    runtimeResult should beColumns("x", "y1", "y2", "z").withRows(expectedResultRows)
  }

  // This test was useful in showcasing we cannot use the LHS slot configuration in fused pipelines in the RHS of cartesian product
  test("should support cartesian product with hash-join on RHS 2") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(Math.sqrt(sizeHint).toInt) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5)
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y1", "y2", "z")
      .cartesianProduct()
      .|.nodeHashJoin("z")
      .|.|.expand("(y2)-[r2]-(z)")
      .|.|.allNodeScan("y2")
      .|.expand("(y1)-[r1]->(z)")
      .|.allNodeScan("y1")
      .expand("(x)-[q4]->(w4)")
      .expand("(x)-[q3]->(w3)")
      .expand("(x)-[q2]->(w2)")
      .expand("(x)-[q1]->(w1)")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expectedResultRows =
      for {
        x <- nodes
        y1 <- unfilteredNodes
        r1 <- y1.getRelationships(Direction.OUTGOING).asScala
        z = r1.getOtherNode(y1)
        r2 <- z.getRelationships(Direction.BOTH).asScala
        y2 = r2.getOtherNode(z)
      } yield Array(x, y1, y2, r1.getOtherNode(y1))

    runtimeResult should beColumns("x", "y1", "y2", "z").withRows(expectedResultRows)
  }

  test("cartesian product with leftOuterHashJoin on RHS, with join-key as alias") {
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .cartesianProduct()
      .|.leftOuterHashJoin("x")
      .|.|.projection("y AS x")
      .|.|.allNodeScan("y")
      .|.allNodeScan("x")
      .argument()
      .build()

    val result = execute(logicalQuery, runtime)

    val expected = nodes.map(n => Array(n, n))

    result should beColumns("x", "y").withRows(expected)
  }

  test("nested cartesian product with leftOuterHashJoin on RHS, with join-key as alias") {
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .cartesianProduct()
      .|.cartesianProduct()
      .|.|.leftOuterHashJoin("y")
      .|.|.|.projection("x AS y")
      .|.|.|.allNodeScan("x")
      .|.|.allNodeScan("y")
      .|.argument()
      .argument()
      .build()

    val result = execute(logicalQuery, runtime)

    val expected = nodes.map(n => Array(n, n))

    result should beColumns("x", "y").withRows(expected)
  }

  test("cartesian product with rightOuterHashJoin on RHS, with join-key as alias") {
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .cartesianProduct()
      .|.rightOuterHashJoin("x")
      .|.|.allNodeScan("x")
      .|.projection("y AS x")
      .|.allNodeScan("y")
      .argument()
      .build()

    val result = execute(logicalQuery, runtime)

    val expected = nodes.map(n => Array(n, n))

    result should beColumns("x", "y").withRows(expected)
  }

  test("nested cartesian product with rightOuterHashJoin, with join-key as alias") {
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .cartesianProduct()
      .|.cartesianProduct()
      .|.|.rightOuterHashJoin("y")
      .|.|.|.allNodeScan("y")
      .|.|.projection("x AS y")
      .|.|.allNodeScan("x")
      .|.argument()
      .argument()
      .build()

    val result = execute(logicalQuery, runtime)

    val expected = nodes.map(n => Array(n, n))

    result should beColumns("x", "y").withRows(expected)
  }

  test("cartesian product with leftOuterHashJoin on RHS, with join-key as alias on both sides") {
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .cartesianProduct()
      .|.leftOuterHashJoin("x")
      .|.|.projection("y AS x")
      .|.|.allNodeScan("y")
      .|.projection("z as x")
      .|.allNodeScan("z")
      .argument()
      .build()

    val result = execute(logicalQuery, runtime)

    val expected = nodes.map(n => Array(n, n, n))

    result should beColumns("x", "y", "z").withRows(expected)
  }

  test("cartesian product with rightOuterHashJoin on RHS, with join-key as alias on both sides") {
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .cartesianProduct()
      .|.rightOuterHashJoin("x")
      .|.|.projection("y AS x")
      .|.|.allNodeScan("y")
      .|.projection("z as x")
      .|.allNodeScan("z")
      .argument()
      .build()

    val result = execute(logicalQuery, runtime)

    val expected = nodes.map(n => Array(n, n, n))

    result should beColumns("x", "y", "z").withRows(expected)
  }

  test("cartesian product with nodeHashJoin on RHS, with join-key as alias") {
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .cartesianProduct()
      .|.nodeHashJoin("x")
      .|.|.projection("y as x")
      .|.|.allNodeScan("y")
      .|.allNodeScan("x")
      .argument()
      .build()

    val result = execute(query, runtime)

    val expected = nodes.map(n => Array(n, n))

    result should beColumns("x", "y").withRows(expected)
  }

  test("cartesian product with valueHashJoin on RHS, with join-key as alias") {
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .cartesianProduct()
      .|.valueHashJoin("x=x")
      .|.|.projection("y as x")
      .|.|.allNodeScan("y")
      .|.allNodeScan("x")
      .argument()
      .build()

    val result = execute(query, runtime)

    val expected = nodes.map(n => Array(n, n))

    result should beColumns("x", "y").withRows(expected)
  }

  test("should support join with cartesian product on RHS") {
    // given
    val (unfilteredNodes, _) = givenGraph { circleGraph(Math.sqrt(sizeHint).toInt) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5)
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .nodeHashJoin("x")
      .|.cartesianProduct()
      .|.|.allNodeScan("x")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expectedResultRows =
      for {
        x <- nodes
        y <- unfilteredNodes
      } yield Array(x, y)

    runtimeResult should beColumns("x", "y").withRows(expectedResultRows)
  }

  test("preserves order with multiple index seeks") {
    assume(!isParallel) // Parallel runtime cannot maintain provided order
    // given
    val nValues = 14 // gives 819 results in the range 0-12
    val inputRows = inputValues((0 until nValues).map { i =>
      Array[Any](i.toLong, i.toLong)
    }.toArray: _*)

    nodeIndex("Label", "prop")
    givenGraph {
      nodePropertyGraph(
        nValues,
        {
          case i: Int =>
            Map(
              "prop" -> (nValues + 3 - i) % nValues
            ) // Reverse and offset when creating the values so we do not accidentally get them in order
        },
        "Label"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("nn", "mm")
      .projection("n.prop as nn", "m.prop as mm")
      .apply()
      .|.cartesianProduct()
      .|.|.nodeIndexOperator("m:Label(prop < ???)", paramExpr = Some(varFor("j")), getValue = _ => DoNotGetValue)
      .|.nodeIndexOperator("n:Label(prop < ???)", paramExpr = Some(varFor("i")), getValue = _ => GetValue)
      .input(variables = Seq("i", "j"))
      .build()

    // then
    val expected = for {
      i <- 0 until nValues
      j <- 0 until i
      k <- 0 until i
    } yield Array(j, k)
    val runtimeResult = execute(logicalQuery, runtime, inputRows)
    runtimeResult should beColumns("nn", "mm").withRows(inOrder(expected))
  }

  test("cartesian product should not eagerly schedule lhs - non-nested") {
    assume(!isParallel)

    // given
    val n = Math.pow(sizeHint, 1.0 / 4).toInt * 2
    val (as, bs) = givenGraph {
      val as = nodeGraph(n, "A")
      val bs = nodeGraph(n, "B")
      (as, bs)
    }

    require((n & 1) == 0)
    val nBatchSize = n / 2
    val inputStream = batchedInputValues(nBatchSize, as.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B", IndexOrderNone)
      .input(nodes = Seq("a"))
      .build()

    val subscriber = TestSubscriber.concurrent
    val result = execute(
      logicalQuery,
      runtime,
      inputStream,
      subscriber,
      testPlanCombinationRewriterHints = Set(TestPlanCombinationRewriter.NoEager)
    )

    result.request(1)
    result.await() shouldBe true

    inputStream.hasMore shouldBe true

    result.request(Long.MaxValue)
    result.await() shouldBe false

    inputStream.hasMore shouldBe false

    // then
    val expectedResultRows = for {
      a <- as
      b <- bs
    } yield Array(a, b)

    val runtimeResult = RecordingRuntimeResult(result, subscriber)
    runtimeResult should beColumns("a", "b").withRows(expectedResultRows)
  }

  test("cartesian product should not eagerly schedule lhs - nested") {
    assume(!isParallel)

    // given
    val n = Math.pow(sizeHint, 1.0 / 4).toInt * 2
    val (as, bs, cs, ds) = givenGraph {
      val as = nodeGraph(n, "A")
      val bs = nodeGraph(n, "B")
      val cs = nodeGraph(n, "C")
      val ds = nodeGraph(n, "D")
      (as, bs, cs, ds)
    }

    require((n & 1) == 0)
    val nBatchSize = n / 2
    val inputStream = batchedInputValues(nBatchSize, as.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c", "d")
      .cartesianProduct()
      .|.cartesianProduct()
      .|.|.nodeByLabelScan("d", "D", IndexOrderNone)
      .|.nodeByLabelScan("c", "C", IndexOrderNone)
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B", IndexOrderNone)
      .input(nodes = Seq("a"))
      .build()

    val subscriber = TestSubscriber.concurrent
    val result = execute(
      logicalQuery,
      runtime,
      inputStream,
      subscriber,
      testPlanCombinationRewriterHints = Set(TestPlanCombinationRewriter.NoEager)
    )

    result.request(1)
    result.await() shouldBe true

    inputStream.hasMore shouldBe true

    result.request(Long.MaxValue)
    result.await() shouldBe false

    inputStream.hasMore shouldBe false

    // then
    val expectedResultRows = for {
      a <- as
      b <- bs
      c <- cs
      d <- ds
    } yield Array(a, b, c, d)

    val runtimeResult = RecordingRuntimeResult(result, subscriber)
    runtimeResult should beColumns("a", "b", "c", "d").withRows(expectedResultRows)
  }

  test("should handle argument cancellation") {
    // given
    val lhsLimit = 3
    val rhsLimit = 3

    val prepareInput = for {
      downstreamRangeTo <- Range.inclusive(0, 2)
      lhsRangeTo <- Range.inclusive(0, lhsLimit * 2)
      rhsRangeTo <- Range.inclusive(0, rhsLimit * 2)
    } yield {
      Array(downstreamRangeTo, lhsRangeTo, rhsRangeTo)
    }
    val input = prepareInput ++ prepareInput ++ prepareInput ++ prepareInput

    val downstreamLimit = input.size / 2
    val upstreamLimit1 = (lhsLimit * rhsLimit) / 2
    val upstreamLimit2 = (0.75 * input.size).toInt

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z", "a", "b", "c")
      .limit(upstreamLimit2)
      .apply()
      .|.limit(upstreamLimit1)
      .|.cartesianProduct()
      .|.|.limit(rhsLimit)
      .|.|.unwind("range(0, z-1) as c")
      .|.|.argument()
      .|.limit(lhsLimit)
      .|.unwind("range(0, y-1) as b")
      .|.argument()
      .limit(downstreamLimit)
      .unwind("range(0, x-1) as a")
      .input(variables = Seq("x", "y", "z"))
      .build()

    val result = execute(logicalQuery, runtime, inputValues(input.map(_.toArray[Any]): _*))
    result.awaitAll()
  }

  test("aggregation on the lhs of an apply under cartesian product") {
    // given
    val input = batchedInputValues(sizeHint / 8, (1 to sizeHint).map(i => Array[Any](i)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("res")
      .projection("42 AS res")
      .cartesianProduct()
      .|.apply()
      .|.|.projection("1 AS count")
      .|.|.argument("n1")
      .|.aggregation(Seq.empty, Seq("min(0) AS n1"))
      .|.argument()
      .input(variables = Seq("n0"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)
    runtimeResult should beColumns("res").withRows(singleColumn(List.fill(sizeHint)(42)))
  }

  test("aggregations under nested cartesian product") {
    // given
    val input = batchedInputValues(sizeHint / 8, (1 to sizeHint).map(i => Array[Any](i)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("res")
      .projection("42 AS res")
      .cartesianProduct()
      .|.cartesianProduct()
      .|.|.aggregation(Seq.empty, Seq("min(0) AS n2"))
      .|.|.argument()
      .|.aggregation(Seq.empty, Seq("min(0) AS n1"))
      .|.argument()
      .input(variables = Seq("n0"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)
    runtimeResult should beColumns("res").withRows(singleColumn(List.fill(sizeHint)(42)))
  }

  test("aggregations under join under a cartesian product") {
    // given
    val input = batchedInputValues(sizeHint / 8, (1 to sizeHint).map(i => Array[Any](i)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("res")
      .projection("42 AS res")
      .cartesianProduct()
      .|.valueHashJoin("n1=n2")
      .|.|.aggregation(Seq.empty, Seq("min(0) AS n2"))
      .|.|.argument()
      .|.aggregation(Seq.empty, Seq("min(0) AS n1"))
      .|.argument()
      .input(variables = Seq("n0"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)
    runtimeResult should beColumns("res").withRows(singleColumn(List.fill(sizeHint)(42)))
  }

  test("aggregations under join under a cartesian product with additional projections") {
    // given
    val input = batchedInputValues(sizeHint / 8, (1 to sizeHint).map(i => Array[Any](i)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("res")
      .projection("42 AS res")
      .cartesianProduct()
      .|.valueHashJoin("n1=n2")
      .|.|.aggregation(Seq.empty, Seq("min(0) AS n2"))
      .|.|.projection("42 as somethingOrOther")
      .|.|.argument()
      .|.aggregation(Seq.empty, Seq("min(0) AS n1"))
      .|.projection("42 as whatever")
      .|.argument()
      .input(variables = Seq("n0"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)
    runtimeResult should beColumns("res").withRows(singleColumn(List.fill(sizeHint)(42)))
  }

  test("aggregation on the lhs of an apply followed by optional under cartesian product") {
    // given
    val input = batchedInputValues(sizeHint / 8, (1 to sizeHint).map(i => Array[Any](i)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("res")
      .projection("42 AS res")
      .cartesianProduct()
      .|.optional("n1")
      .|.apply()
      .|.|.projection("1 AS count")
      .|.|.argument("n1")
      .|.aggregation(Seq.empty, Seq("min(0) AS n1"))
      .|.argument()
      .input(variables = Seq("n0"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)
    runtimeResult should beColumns("res").withRows(singleColumn(List.fill(sizeHint)(42)))
  }

  test("aggregation on the lhs of an apply under cartesian product all under an apply") {
    // given
    val input = batchedInputValues(sizeHint / 8, (1 to sizeHint).map(i => Array[Any](i)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("res")
      .projection("42 AS res")
      .apply()
      .|.cartesianProduct()
      .|.|.optional("n1")
      .|.|.apply()
      .|.|.|.projection("1 AS count")
      .|.|.|.argument("n1")
      .|.|.aggregation(Seq.empty, Seq("min(0) AS n1"))
      .|.|.argument("n0")
      .|.unwind("range(1, 11) AS i")
      .|.argument("n0")
      .input(variables = Seq("n0"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)
    runtimeResult should beColumns("res").withRows(singleColumn(List.fill(sizeHint * 11)(42)))
  }

  test("argument projection on the rhs of a cartesian product") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("c")
      .apply()
      .|.cartesianProduct()
      .|.|.projection("a AS c")
      .|.|.argument("a")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    execute(query, runtime) should beColumns("c")
  }
}
