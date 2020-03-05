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

import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.values.virtual.VirtualNodeValue

abstract class LimitTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                        runtime: CypherRuntime[CONTEXT],
                                                        sizeHint: Int
                                                       ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("limit 0") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .limit(0)
      .input(variables = Seq("x"))
      .build()

    val input = inputColumns(100000, 3, identity).stream()

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)
    runtimeResult should beColumns("x").withNoRows()

    input.hasMore should be(true)
  }

  test("limit -1") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .limit(-1)
      .input(variables = Seq("x"))
      .build()

    val input = inputColumns(100000, 3, identity).stream()

    // then
    val exception = intercept[InvalidArgumentException] {
      consume(execute(logicalQuery, runtime, input))
    }
    exception.getMessage should include("Must be a non-negative integer")
  }

  test("limit -1 on an empty input") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .limit(-1)
      .input(variables = Seq("x"))
      .build()

    val input = inputValues()

    // then
    val exception = intercept[InvalidArgumentException] {
      consume(execute(logicalQuery, runtime, input))
    }
    exception.getMessage should include("Must be a non-negative integer")
  }

  test("limit higher than amount of rows") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .limit(Int.MaxValue)
      .input(variables = Seq("x"))
      .build()

    val input = inputColumns(sizeHint, 3, identity)

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)
    runtimeResult should beColumns("x").withRows(input.flatten)
  }

  test("should support limit") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .limit(10)
      .input(variables = Seq("x"))
      .build()

    val input = inputColumns(100000, 3, identity).stream()

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)
    runtimeResult should beColumns("x").withRows(rowCount(10))

    input.hasMore should be(true)
  }

  test("should support limit with null values") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .limit(10)
      .input(variables = Seq("x"))
      .build()

    val input = inputColumns(100000, 3, x => if (x % 2 == 0) x else null).stream()

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)
    runtimeResult should beColumns("x").withRows(rowCount(10))

    input.hasMore should be(true)
  }

  test("should support limit in the first of two pipelines") {
    // given
    val nodesPerLabel = 100
    given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandAll("(x)-->(y)")
      .limit(9)
      .nodeByLabelScan("x", "A")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x", "y").withRows(rowCount(900))
  }

  test("should support apply-limit") {
    // given
    val nodesPerLabel = 100
    val (aNodes, _) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.limit(10)
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("x").withRows(singleColumn(aNodes.flatMap(n => List().padTo(10, n))))
  }

  test("should support limit on top of apply") {
    // given
    val nodesPerLabel = 50
    given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }
    val limit = nodesPerLabel * nodesPerLabel - 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .limit(limit)
      .apply()
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("x").withRows(rowCount(limit))
  }

  test("should support reduce -> limit on the RHS of apply") {
    // given
    val nodesPerLabel = 100
    val (aNodes, bNodes) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.limit(10)
      .|.sort(Seq(Ascending("y")))
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)

    val expected = for{
      x <- aNodes
      y <- bNodes.sortBy(_.getId).take(10)
    } yield Array[Any](x, y)

    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should support limit -> reduce on the RHS of apply") {
    // given
    val NODES_PER_LABEL = 100
    val LIMIT = 10
    given { bipartiteGraph(NODES_PER_LABEL, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.sort(Seq(Ascending("y")))
      .|.limit(LIMIT)
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x", "y").withRows(groupedBy(NODES_PER_LABEL, LIMIT, "x").asc("y"))
  }

  test("should support chained limits") {
    // given
    val nodesPerLabel = 100
    given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a2")
      .limit(10)
      .expandAll("(b2)<--(a2)")
      .limit(10)
      .expandAll("(a1)-->(b2)")
      .limit(10)
      .expandAll("(b1)<--(a1)")
      .limit(10)
      .expandAll("(x)-->(b1)")
      .nodeByLabelScan("x", "A")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a2").withRows(rowCount(10))
  }

  test("should support chained limits in the same pipeline") {
    // given
    val nodesPerLabel = 100
    given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    // This is a hypothetical query used to excersise some logic
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .limit(12)
      .limit(13)
      .limit(14)
      .expandAll("(b1)<--(a1)")
      .limit(15)
      .limit(16)
      .limit(17)
      .expandAll("(x)-->(b1)")
      .limit(18)
      .limit(19)
      .limit(20)
      .nodeByLabelScan("x", "A")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a1").withRows(rowCount(12))
  }

  test("should support limit with expand") {
    val nodeConnections = given {
      val nodes = nodeGraph(sizeHint)
      randomlyConnect(nodes, Connectivity(0, 5, "OTHER")).map {
        case NodeConnections(node, connections) => (node.getId, connections)
      }.toMap
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.limit(1)
      .|.expandAll("(x)-->(y)")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y").withRows(matching {
      case rows:Seq[Array[_]] if rows.forall {
        case Array(x, y) =>
          val xid = x.asInstanceOf[VirtualNodeValue].id()
          val connections = nodeConnections(xid)
          connections should not be empty
          withClue(s"x id: $xid --") {
            val yid = y match {
              case node: VirtualNodeValue => node.id()
              case _ => y shouldBe a[VirtualNodeValue]
            }
            connections.values.flatten.exists(_.getId == yid)
          }

      } && { // Assertion on the whole result
        val xs = rows.map(_(0))
        xs.distinct.size == xs.size // Check that there is at most one row per x
      } =>
    })
  }

  test("LIMIT combined with fused-over-pipelines") {
    val nodesPerLabel = 100
    given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "rel", "y")
      .cartesianProduct()
      .|.argument()
      .expand("(x)-[rel]->(y)")
      .limit(1)
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("x", "rel", "y").withRows(rowCount(nodesPerLabel))
  }
}
