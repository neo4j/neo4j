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

import org.neo4j.cypher.internal.logical.plans.{Ascending, Descending, GetValue}
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}
import org.neo4j.graphdb.Direction

import scala.collection.JavaConverters._

abstract class CartesianProductTestBase[CONTEXT <: RuntimeContext](
                                                               edition: Edition[CONTEXT],
                                                               runtime: CypherRuntime[CONTEXT],
                                                               sizeHint: Int
                                                             ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {


  test("handle cached properties and cartesian product on LHS of apply") {
    // given
    index("Label", "prop")
    val nodes = given {
      nodePropertyGraph(sizeHint, {
        case i: Int => Map("prop" -> i)
      }, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .apply()
      .|.cartesianProduct()
      .|.|.argument("n")
      .|.argument("n")
      .nodeIndexOperator("n:Label(prop)", getValue = GetValue)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n").withRows(singleColumn(nodes))
  }

  test("should handle multiple columns ") {
    // given
    val size = 10 //sizehint is a bit too big here
    val nodes = given {
      val nodes = nodePropertyGraph(size, {
        case _ => Map("prop" -> "foo")
      })
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
    given {
      nodePropertyGraph(sizeHint, {
        case i => Map("prop" -> i)
      })
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
    val expected = for {i <- 0 to 9
                        j <- 0 to 4} yield Array(i, j)
    runtimeResult should beColumns("aProp", "bProp").withRows(expected)
  }

  test("should handle cached properties from both lhs and rhs") {
    val nodes = given {
      nodePropertyGraph(sizeHint, {
        case i => Map("prop" -> i)
      })
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
    given { circleGraph(sizeHint) }
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

  test("cartesian product on empty rhs") {
    // given
    val nodes = given {
      nodeGraph(19, "RHS")
      nodeGraph(sizeHint)
    }
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .cartesianProduct()
      .|.expand("(y)--(z)")
      .|.nodeByLabelScan("y", "RHS")
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
    val (unfilteredNodes, _) = given { circleGraph(Math.sqrt(sizeHint).toInt) }
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
    val expectedResultRows = for {x <- nodes
                                  y <- unfilteredNodes
                                  rel <- y.getRelationships.asScala
                                  z = rel.getOtherNode(y)
                                  } yield Array(x, y, z)

    runtimeResult should beColumns("x", "y", "z").withRows(expectedResultRows)
  }

  test("cartesian product after expand on lhs") {
    // given
    val (unfilteredNodes, _) = given { circleGraph(Math.sqrt(sizeHint).toInt) }
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
    val expectedResultRows = for {y <- nodes if y != null
                                  rel <- y.getRelationships().asScala
                                  x = rel.getOtherNode(y)
                                  z <- unfilteredNodes
                                  } yield Array(x, y, z)

    runtimeResult should beColumns("x", "y", "z").withRows(expectedResultRows)
  }

  // This test mainly assert that we get the right slot configuration for Arraycopy and don't get IndexOutOfBounds there
  test("cartesian product with apply everywhere") {
    val (unfilteredNodes, _) = given { circleGraph(Math.sqrt(sizeHint).toInt) }
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
    val expectedResultRows = for {x <- nodes if x != null
                                  z <- unfilteredNodes
                                  } yield Array(x, z)

    runtimeResult should beColumns("x", "z").withRows(expectedResultRows)
  }

  test("cartesian product after optional on lhs") {
    // given
    val (unfilteredNodes, _) = given { circleGraph(Math.sqrt(sizeHint).toInt) }
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
    val expectedResultRows = for {y <- nodes
                                  z <- unfilteredNodes
                                  } yield Array(y, z)

    runtimeResult should beColumns("y", "z").withRows(expectedResultRows)
  }

  test("cartesian product with optional on top") {
    // given
    val (unfilteredNodes, _) = given { circleGraph(Math.sqrt(sizeHint).toInt) }
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
    val expectedResultRows = for {y <- nodes
                                  z <- unfilteredNodes
                                  } yield Array(y, z)

    runtimeResult should beColumns("y", "z").withRows(expectedResultRows)
  }

  test("cartesian product nested") {
    // given
    val n = Math.pow(sizeHint, 1.0 / 4).toInt * 2
    val (as, bs, cs, ds) = given {
      val as = nodeGraph(n, "A")
      val bs = nodeGraph(n, "B")
      val cs = nodeGraph(n, "C")
      val ds = nodeGraph(n, "D")
      (as, bs, cs ,ds)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c", "d")
      .cartesianProduct()
      .|.cartesianProduct()
      .|.|.nodeByLabelScan("d", "D")
      .|.nodeByLabelScan("c", "C")
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B")
      .nodeByLabelScan("a", "A")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expectedResultRows = for {a <- as
                                  b <- bs
                                  c <- cs
                                  d <- ds
                                  } yield Array(a, b, c, d)

    runtimeResult should beColumns("a", "b", "c", "d").withRows(expectedResultRows)
  }

  test("cartesian product below an apply") {
    // given
    val n = Math.pow(sizeHint, 1.0 / 3).toInt * 2
    val (as, _) = given { bipartiteGraph(n, "A", "B", "REL") }
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
    val expectedResultRows = for {a <- nodes if a != null
                                  r1 <- a.getRelationships(Direction.OUTGOING).asScala
                                  r2 <- a.getRelationships(Direction.OUTGOING).asScala
    } yield Array(a, r1, r2)

    runtimeResult should beColumns("a", "r1", "r2").withRows(expectedResultRows)
  }

    test("cartesian product with limit on rhs") {
      val nodesPerLabel = Math.sqrt(sizeHint).toInt
      val limit = nodesPerLabel - 1
      val (aNodes, _) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }
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
    val (unfilteredNodes, _) = given { circleGraph(Math.sqrt(sizeHint).toInt) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5)
    val limitCount = nodes.size / 2
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .limit(count = limitCount)
      .sort(sortItems = Seq(Descending("x"), Ascending("y"), Ascending("z")))
      .sort(sortItems = Seq(Ascending("x"), Descending("y"), Descending("z")))
      .cartesianProduct()
      .|.expand("(y)--(z)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val allRows = for {x <- nodes
                       y <- unfilteredNodes
                       rel <- y.getRelationships().asScala
                       z = rel.getOtherNode(y)
                       } yield Array(x, y, z)
    val expectedResultRows = allRows.sortBy(arr => (-arr(0).getId, arr(1).getId, arr(2).getId)).take(limitCount)

    runtimeResult should beColumns("x", "y", "z").withRows(inOrder(expectedResultRows))
  }

  test("cartesian product with limit on top of join") {
    // given
    val (unfilteredNodes, _) = given { circleGraph(Math.sqrt(sizeHint).toInt) }
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

  // These tests are ignored because expand fused over pipelines is broken.

  test("should support cartesian product with hash-join on RHS") {
    // given
    val (unfilteredNodes, _) = given { circleGraph(Math.sqrt(sizeHint).toInt) }
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
    val (unfilteredNodes, _) = given { circleGraph(Math.sqrt(sizeHint).toInt) }
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

  test("should support join with cartesian product on RHS") {
    // given
    val (unfilteredNodes, _) = given { circleGraph(Math.sqrt(sizeHint).toInt) }
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
}
