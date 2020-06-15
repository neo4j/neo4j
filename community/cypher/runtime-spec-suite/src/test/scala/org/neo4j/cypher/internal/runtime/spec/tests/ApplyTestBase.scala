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
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

abstract class ApplyTestBase[CONTEXT <: RuntimeContext](
                                                         edition: Edition[CONTEXT],
                                                         runtime: CypherRuntime[CONTEXT],
                                                         sizeHint: Int
                                                       ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("apply on empty lhs and rhs") {
    // given
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.allNodeScan("y", "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("apply on empty rhs") {
    // given
    val nodes = given {
      nodeGraph(19, "RHS")
      nodeGraph(sizeHint)
    }
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.expandInto("(y)--(x)")
      .|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // because graph contains no relationships, the expand will return no rows
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("apply on empty lhs") {
    // given
    given {
      nodeGraph(19, "RHS")
      nodeGraph(sizeHint)
    }
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("apply on aggregation should carry through argument variables") {
    // given
    val nodes = given {
      nodePropertyGraph(sizeHint, {
        case i: Int => Map("prop" -> i)
      }, "Label")
    }
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "xMax")
      .apply()
      .|.aggregation(Seq.empty, Seq("max(x.prop) as xMax"))
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x", "xMax").withRows(nodes.zipWithIndex.map(_.productIterator.toArray))
  }

  test("apply on grouped aggregation should carry through argument variables") {
    // given
    val nodes = given {
      nodePropertyGraph(sizeHint, {
        case i: Int => Map("prop" -> i, "group" -> i)
      }, "Label")
    }
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "xMax")
      .apply()
      .|.aggregation(Seq("x.group AS group"), Seq("max(x.prop) as xMax"))
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x", "xMax").withRows(nodes.zipWithIndex.map(_.productIterator.toArray))
  }

  test("apply on entity aggregation should carry through argument variables") {
    // given
    val nodes = given {
      nodePropertyGraph(sizeHint, {
        case i: Int => Map("prop" -> i, "group" -> i)
      }, "Label")
    }
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "xMax")
      .apply()
      .|.aggregation(Seq("x AS group"), Seq("max(x.prop) as xMax"))
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("x", "xMax").withRows(nodes.zipWithIndex.map(_.productIterator.toArray))
  }

  test("apply after expand on rhs") {
    val (unfilteredNodes, _) = given { circleGraph(Math.sqrt(sizeHint).toInt) }
    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.3)
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.expandInto("(y)--(x)")
      .|.allNodeScan("y", "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expectedResultRows = for {
      x <- nodes
      y <- unfilteredNodes
      rel <- y.getRelationships.asScala if rel.getOtherNode(y) == x
    } yield Array(x, y)

    runtimeResult should beColumns("x", "y").withRows(expectedResultRows)
  }

  test("apply with limit on rhs") {
    val limit = 10

    val unfilteredNodes = given {
      val size = 100
      val nodes = nodeGraph(size)
      randomlyConnect(nodes, Connectivity(1, limit, "REL"))
      nodes
    }

    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.3)
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.limit(limit)
      .|.expandInto("(y)--(x)")
      .|.allNodeScan("y", "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expectedRowCounts = for {
      x <- nodes
      subquery = for {
        y <- unfilteredNodes
        rel <- y.getRelationships.asScala if rel.getOtherNode(y) == x
      } yield Array(x, y)
    } yield math.min(subquery.size, limit)

    val expectedRowCount = expectedRowCounts.sum
    runtimeResult should beColumns("x", "y").withRows(rowCount(expectedRowCount))
  }

  test("apply union with aliased variables") {
    // given
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("z")
      .apply()
      .|.distinct("z AS z")
      .|.union()
      .|.|.projection("y AS z")
      .|.|.argument("y")
      .|.projection("x AS z")
      .|.argument("x")
      .projection("1 AS x", "2 AS y")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("z").withRows(Array(Array(1), Array(2)))
  }

  test("apply union with multiple aliased variables") {
    // given
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "m")
      .apply()
      .|.distinct("n AS n", "m AS m")
      .|.union()
      .|.|.projection("y AS n", "x AS m")
      .|.|.argument("x", "y")
      .|.projection("x AS n", "y AS m")
      .|.argument("x", "y")
      .projection("1 AS x", "2 AS y")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    runtimeResult should beColumns("n", "m").withRows(Array(Array(1, 2), Array(2, 1)))
  }

  test("nested apply with identical branches ending in optional multiple identifiers") {
    val numberOfNodes = 3
    val (nodes, _) = given {
      bipartiteGraph(numberOfNodes, "A", "B", "R")
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.apply()
      .|.|.optional()
      .|.|.expand("(x)-->(y)")
      .|.|.argument("x")
      .|.optional()
      .|.expand("(x)-->(y)")
      .|.argument("x")
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(n => Seq.fill(numberOfNodes * numberOfNodes)(Array[Any](n)))
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("nested apply with identical branches ending in optional single identifier") {
    val numberOfNodes = 3
    val nodes = given {
      nodeGraph(numberOfNodes)
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.apply()
      .|.|.optional()
      .|.|.filter("true")
      .|.|.allNodeScan("y", "x")
      .|.optional()
      .|.filter("true")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(n => Seq.fill(numberOfNodes * numberOfNodes)(Array[Any](n)))
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("cartesian product nested under apply") {
    // given
    val nodes = given {
      val (aNodes, bNodes, _, _) = bidirectionalBipartiteGraph(2, "A", "B", "AB", "BA")
      aNodes ++ bNodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .apply()
      .|.cartesianProduct()
      .|.|.expandAll("(x)-[r]->(y)")
      .|.|.argument("x")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    // then
    val expected = for {
      x <- nodes
      r <- x.getRelationships().asScala if r.getStartNodeId == x.getId
    } yield {
      val y = r.getEndNode
      Array[Any](x, r, y)
    }

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }
}
