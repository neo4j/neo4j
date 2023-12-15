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

import org.neo4j.configuration.GraphDatabaseInternalSettings.CypherOperatorEngine
import org.neo4j.configuration.GraphDatabaseInternalSettings.cypher_operator_engine
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.longValue

import scala.jdk.CollectionConverters.IterableHasAsScala

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
    val nodes = givenGraph {
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
    givenGraph {
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

  test("apply on empty lhs argument should preserve rhs order") {
    // given
    val nodes = givenGraph {
      nodeGraph(19, "RHS")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .apply()
      .|.nodeByLabelScan("y", "RHS", IndexOrderNone)
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(inOrder(nodes.map(Array(_))))
  }

  test("apply on aggregation should carry through argument variables") {
    // given
    val nodes = givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("prop" -> i)
        },
        "Label"
      )
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
    val nodes = givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("prop" -> i, "group" -> i)
        },
        "Label"
      )
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
    val nodes = givenGraph {
      nodePropertyGraph(
        sizeHint,
        {
          case i: Int => Map("prop" -> i, "group" -> i)
        },
        "Label"
      )
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
    val (unfilteredNodes, _) = givenGraph { circleGraph(Math.sqrt(sizeHint).toInt) }
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
    val (nodes, _) = givenGraph {
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
    val nodes = givenGraph {
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
    val nodes = givenGraph {
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

  test("discarding unused variables under nested applies") {
    val isFused = runtimeUsed == Pipelined &&
      edition.getSetting(cypher_operator_engine)
        .exists(_ != CypherOperatorEngine.INTERPRETED)
    assume(runtimeUsed != Parallel && !isFused)

    val size = 32
    val probe1 = recordingProbe("i", "a", "b", "c", "d", "e", "f", "g")
    val probe2 = recordingProbe("i", "a", "b", "c", "d", "e", "f", "g")
    val probe3 = recordingProbe("i", "a", "b", "c", "d", "e", "f", "g")
    val probe4 = recordingProbe("i", "a", "b", "c", "d", "e", "f", "g")
    val query = new LogicalQueryBuilder(this)
      .produceResults("a")
      .prober(probe1)
      .eager()
      .apply()
      .|.filter("c > 0")
      .|.prober(probe2)
      .|.eager()
      .|.apply()
      .|.|.filter("d > 0")
      .|.|.prober(probe3)
      .|.|.eager()
      .|.|.apply()
      .|.|.|.eager()
      .|.|.|.prober(probe4)
      .|.|.|.argument("a", "b", "c", "d")
      .|.|.eager()
      .|.|.argument("a", "b", "c", "d", "e")
      .|.eager()
      .|.argument("a", "b", "c", "d", "e", "f")
      .eager()
      .projection("i+1 AS a", "i+2 AS b", "i+3 AS c", "i+4 AS d", "i+5 AS e", "i+6 AS f", "i+7 AS g")
      .unwind(s"range(0,$size) AS i")
      .argument()
      .build()

    val result = execute(query, runtime)

    // result should be correct
    result should beColumns("a")
      .withRows(Range.inclusive(0, size).map(i => Array[Any](i + 1)))

    if (runtimeUsed != Interpreted && runtimeUsed != Parallel) {
      // unused variables should be discarded in the driving table
      probe1.seenRows shouldBe Range.inclusive(0, size)
        .map(i => Array[AnyValue](null, longValue(i + 1), null, null, null, null, null, null))
        .toArray

      probe2.seenRows shouldBe Range.inclusive(0, size)
        .map(i => Array[AnyValue](null, longValue(i + 1), null, longValue(i + 3), null, null, null, null))
        .toArray

      probe3.seenRows shouldBe Range.inclusive(0, size)
        .map(i => Array[AnyValue](null, longValue(i + 1), null, longValue(i + 3), longValue(i + 4), null, null, null))
        .toArray

      probe4.seenRows shouldBe Range.inclusive(0, size)
        .map(i =>
          Array[AnyValue](
            null,
            longValue(i + 1),
            longValue(i + 2),
            longValue(i + 3),
            longValue(i + 4),
            null,
            null,
            null
          )
        )
        .toArray
    }
  }
}
