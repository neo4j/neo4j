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
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

import scala.collection.JavaConverters.iterableAsScalaIterableConverter


abstract class ValueHashJoinTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                                runtime: CypherRuntime[CONTEXT],
                                                                sizeHint: Int
                                                               ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should support simple hash join between two identifiers") {
    // given
    val nodes = given {
      nodePropertyGraph(sizeHint, {
        case i => Map("prop" -> i)
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .valueHashJoin("a.prop=b.prop")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n, n))
    runtimeResult should beColumns("a", "b").withRows(expected)
  }

  test("should handle additional data when joining on two identifiers") {
    // given
    given {
      nodePropertyGraph(sizeHint, {
        case i => Map("prop" -> i, "otherProp" -> i)
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("aProp", "bProp")
      .valueHashJoin("a.prop=b.prop")
      .|.projection("b.otherProp AS bProp")
      .|.allNodeScan("b")
      .projection("a.otherProp AS aProp")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = (0 until sizeHint).map(n => Array(n, n))
    runtimeResult should beColumns("aProp", "bProp").withRows(expected)
  }

  test("should join on a cached-property") {
    // given
    val nodes = given {
      nodePropertyGraph(sizeHint, {
        case i => Map("prop" -> i)
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .valueHashJoin("cache[a.prop]=cache[b.prop]")
      .|.filter("cache[b.prop] < 10")
      .|.allNodeScan("b")
      .filter("cache[a.prop] < 20")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n, n)). take(10)
    runtimeResult should beColumns("a", "b").withRows(expected)
  }

  test("should handle multiple columns") {
    // given
    val relTuples = (for (i <- 0 until sizeHint) yield {
      Seq(
        (i, (i + 1) % sizeHint, "R")
      )
    }).reduce(_ ++ _)
    val nodes = given {
      val nodes = nodePropertyGraph(sizeHint, {
        case i => Map("prop" -> i)
      })
      connect(nodes, relTuples)
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .valueHashJoin("c.prop=f.prop")
      .|.expand("(e)-[r4]->(f)")
      .|.expand("(d)-[r3]->(e)")
      .|.allNodeScan("d")
      .expand("(b)-[r2]->(c)")
      .expand("(a)-[r1]->(b)")
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n))
    runtimeResult should beColumns("c").withRows(expected)
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
      .|.valueHashJoin("cache[ab.prop] = cache[b.prop]")
      .|.|.filter("cache[ab.prop] = cache[b.prop]")
      .|.|.allNodeScan("b")
      .|.argument()
      .projection("coalesce(a, null) AS ab")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n))
    runtimeResult should beColumns("ab").withRows(expected)
  }

  test("should join with alias on RHS") {
    // given
    val nodes = given {
      nodePropertyGraph(sizeHint, {
        case i => Map("prop" -> i)
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "b2")
      .valueHashJoin("a.prop=b.prop")
      .|.projection("b AS b2")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n, n, n))
    runtimeResult should beColumns("a", "b", "b2").withRows(expected)
  }

  test("should join with alias on LHS") {
    // given
    val nodes = given {
      nodePropertyGraph(sizeHint, {
        case i => Map("prop" -> i)
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "a2", "b")
      .valueHashJoin("a.prop=b.prop")
      .|.allNodeScan("b")
      .projection("a AS a2")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n, n, n))
    runtimeResult should beColumns("a", "a2", "b").withRows(expected)
  }

  test("should join after expand on empty lhs") {
    // given
    given {
      val (nodes, _) = circleGraph(sizeHint)
      nodes.foreach(n => n.setProperty("prop",  n.getId))
    }
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .valueHashJoin("x.prop = y.prop")
      .|.expand("(z)--(y)")
      .|.allNodeScan("z")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("should join on empty rhs") {
    // given
    val nodes = given {
      nodePropertyGraph(sizeHint, {
        case i => Map("prop" -> i)
      })
    }
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .valueHashJoin("x.prop = y.prop")
      .|.expand("(z)--(y)")
      .|.allNodeScan("z")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // because graph contains no relationships, the expand will return no rows
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("should join on empty lhs and rhs") {
    // given
    given {
      val (nodes, _) = circleGraph(sizeHint)
      nodes.foreach(n => n.setProperty("prop",  n.getId))
    }
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .valueHashJoin("x.prop = y.prop")
      .|.expand("(z)--(y)")
      .|.allNodeScan("z")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // because graph contains no relationships, the expand will return no rows
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("should join after expand on rhs") {
    // given
    val unfilteredNodes = given {
      val (nodes, _) = circleGraph(sizeHint)
      nodes.foreach(n => n.setProperty("prop",  n.getId))
      nodes
    }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .valueHashJoin("x.prop = z.prop")
      .|.expand("(y)--(z)")
      .|.allNodeScan("y")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expectedResultRows = for {node <- nodes if node != null
                                  rel <- node.getRelationships().asScala
                                  otherNode = rel.getOtherNode(node)
                                  } yield Array(node, otherNode)

    runtimeResult should beColumns("x", "y").withRows(expectedResultRows)
  }

  test("should join after expand on lhs") {
    // given
    val unfilteredNodes = given {
      val (nodes, _) = circleGraph(sizeHint)
      nodes.foreach(n => n.setProperty("prop",  n.getId))
      nodes
    }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.1)
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .valueHashJoin("x.prop = z.prop")
      .|.allNodeScan("z")
      .expand("(y)--(x)")
      .input(nodes = Seq("y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expectedResultRows = for {node <- nodes if node != null
                                  rel <- node.getRelationships().asScala
                                  otherNode = rel.getOtherNode(node)
                                  } yield Array(otherNode, node)

    runtimeResult should beColumns("x", "y").withRows(expectedResultRows)
  }

  test("should join nested") {
    val nodes = given {
      val (nodes, _) = circleGraph(sizeHint)
      nodes.foreach(n => n.setProperty("prop",  n.getId))
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .valueHashJoin("a.prop = b.prop")
      .|.valueHashJoin("a.prop = b.prop")
      .|.|.allNodeScan("b")
      .|.allNodeScan("a")
      .valueHashJoin("a.prop = b.prop")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then

    runtimeResult should beColumns("a").withRows(nodes.map(Array[Any](_)))
  }

  test("should support simple hash join with apply on lhs and rhs") {
    // given
    val nodes = given {
      nodePropertyGraph(sizeHint, {
        case i => Map("prop" -> i)
      })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .valueHashJoin("a.prop=b.prop")
      .|.apply()
      .|.|.argument("b")
      .|.allNodeScan("b")
      .apply()
      .|.argument("a")
      .allNodeScan("a")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n, n))
    runtimeResult should beColumns("a", "b").withRows(expected)
  }
}
