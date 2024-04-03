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
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriter.NoRewrites
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.graphdb.Label.label
import org.neo4j.graphdb.RelationshipType.withName
import org.neo4j.values.virtual.VirtualNodeValue

import java.util.concurrent.ThreadLocalRandom

abstract class LimitTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
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

  test("limit on top of all node scan") {
    givenGraph {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .limit(10)
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x").withRows(rowCount(10))
  }

  test("limit on top of union of all node scans") {
    givenGraph {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .limit(10)
      .union()
      .|.allNodeScan("x")
      .allNodeScan("x")
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x").withRows(rowCount(10))
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
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandAll("(x)-->(y)")
      .limit(9)
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x", "y").withRows(rowCount(900))
  }

  test("should support apply-limit") {
    // given
    val nodesPerLabel = 100
    val (aNodes, _) = givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.limit(10)
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("x").withRows(singleColumn(aNodes.flatMap(n => List().padTo(10, n))))
  }

  test("should support apply-limit on top of union") {
    // given
    val nodesPerLabel = 100
    val (aNodes, _) = givenGraph {
      bipartiteGraph(nodesPerLabel, "A", "B", "R")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.limit(10)
      .|.union()
      .|.|.expandAll("(x)-->(y)")
      .|.|.argument()
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("x").withRows(singleColumn(aNodes.flatMap(n => List().padTo(10, n))))
  }

  test("should support apply-limit 0") {
    givenGraph {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .apply()
      .|.limit(0)
      .|.argument("n")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n").withNoRows()
  }

  test("should support limit on top of apply") {
    // given
    val nodesPerLabel = 50
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }
    val limit = nodesPerLabel * nodesPerLabel - 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .limit(limit)
      .apply()
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("x").withRows(rowCount(limit))
  }

  test("should support limit 0 on top of apply") {
    val nodesPerLabel = 50
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .limit(0)
      .apply()
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should support reduce -> limit on the RHS of apply") {
    // given
    val nodesPerLabel = 100
    val (aNodes, bNodes) = givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
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
      y <- bNodes.sortBy(_.getId).take(10)
    } yield Array[Any](x, y)

    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should support limit -> reduce on the RHS of apply") {
    // given
    val NODES_PER_LABEL = 100
    val LIMIT = 10
    givenGraph { bipartiteGraph(NODES_PER_LABEL, "A", "B", "R") }

    // NOTE: Parallel runtime does not guarantee order is preserved across an apply scope

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .planIf(isParallel)(_.sort("y ASC")) // Insert a top-level sort in parallel runtime
      .apply()
      .|.sort("y ASC")
      .|.limit(LIMIT)
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    val rowOrderMatcher = if (isParallel) sortedAsc("y") else groupedBy(NODES_PER_LABEL, LIMIT, "x").asc("y")
    runtimeResult should beColumns("x", "y").withRows(rowOrderMatcher)
  }

  test("should support chained limits") {
    // given
    val nodesPerLabel = 100
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

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
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a2").withRows(rowCount(10))
  }

  test("should support chained limits in the same pipeline") {
    // given
    val nodesPerLabel = 100
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

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
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a1").withRows(rowCount(12))
  }

  test("should support limit with expand") {
    val nodeConnections = givenGraph {
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
      case rows: Seq[_] if rows.forall {
          case Array(x, y) =>
            val xid = x.asInstanceOf[VirtualNodeValue].id()
            val connections = nodeConnections(xid)
            connections should not be empty
            withClue(s"x id: $xid --") {
              val yid = y match {
                case node: VirtualNodeValue => node.id()
                case _                      => y shouldBe a[VirtualNodeValue]
              }
              connections.values.flatten.exists(_.getId == yid)
            }

        } && { // Assertion on the whole result
          val xs = rows.map(_.asInstanceOf[Array[_]](0))
          xs.distinct.size == xs.size // Check that there is at most one row per x
        } =>
    })
  }

  test("LIMIT combined with fused-over-pipelines") {
    val nodesPerLabel = 100
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "rel", "y")
      .cartesianProduct()
      .|.argument()
      .expand("(x)-[rel]->(y)")
      .limit(1)
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("x", "rel", "y").withRows(rowCount(nodesPerLabel))
  }

  test("limit followed by aggregation") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("sum")
      .aggregation(Seq.empty, Seq("sum(x) AS sum"))
      .limit(10)
      .input(variables = Seq("x"))
      .build()

    val input = inputColumns(100000, 3, _ => 11).stream()

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)
    runtimeResult should beColumns("sum").withSingleRow(110)

    input.hasMore should be(true)
  }

  test("limit 0 followed by aggregation") {
    givenGraph {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(n) as c"))
      .limit(0)
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("c").withSingleRow(0)
  }

  test("should support allnodes + limit under apply") {
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }
    val limit = 3

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(limit)
      .|.allNodeScan("a2")
      .allNodeScan("a1")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(List.fill(limit)(_))
    runtimeResult should beColumns("a1").withRows(singleColumn(expected))
  }

  test("should support expand(all) + limit under apply") {
    val (nodes, _) = givenGraph {
      bipartiteGraph(10, "A", "B", "R")
    }
    val limit = 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(limit)
      .|.expand("(a1)-->(b)")
      .|.argument()
      .nodeByLabelScan("a1", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(List.fill(limit)(_))
    runtimeResult should beColumns("a1").withRows(singleColumn(expected))
  }

  test("should support varexpand + limit under apply") {
    val (nodes, _) = givenGraph {
      bipartiteGraph(10, "A", "B", "R")
    }
    val limit = 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(limit)
      .|.expand("(a1)-[*]->(b)")
      .|.argument()
      .nodeByLabelScan("a1", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(List.fill(limit)(_))
    runtimeResult should beColumns("a1").withRows(singleColumn(expected))
  }

  test("should support optional expand + limit under apply") {
    val (nodes, _) = givenGraph {
      bipartiteGraph(10, "A", "B", "R")
    }
    val limit = 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(limit)
      .|.optionalExpandAll("(a1)-->(b)")
      .|.argument()
      .nodeByLabelScan("a1", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(List.fill(limit)(_))
    runtimeResult should beColumns("a1").withRows(singleColumn(expected))
  }

  test("should support expand(into) + limit under apply") {
    val nodes = givenGraph {
      val (aNodes, bNodes) = bipartiteGraph(3, "A", "B", "R")
      for {
        a <- aNodes
        b <- bNodes
      } {
        a.createRelationshipTo(b, withName("R"))
      }
      aNodes
    }
    val limit = 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(limit)
      .|.expandInto("(a1)-->(b)")
      .|.nonFuseable()
      .|.nodeByLabelScan("b", "B", IndexOrderNone)
      .nodeByLabelScan("a1", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(List.fill(limit)(_))
    runtimeResult should beColumns("a1").withRows(singleColumn(expected))
  }

  test("should support optional expand(into) + limit under apply") {
    val nodes = givenGraph {
      val (aNodes, bNodes) = bipartiteGraph(3, "A", "B", "R")
      for {
        a <- aNodes
        b <- bNodes
      } {
        a.createRelationshipTo(b, withName("R"))
      }
      aNodes
    }
    val limit = 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(limit)
      .|.optionalExpandInto("(a1)-->(b)")
      .|.nonFuseable()
      .|.nodeByLabelScan("b", "B", IndexOrderNone)
      .nodeByLabelScan("a1", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(List.fill(limit)(_))
    runtimeResult should beColumns("a1").withRows(singleColumn(expected))
  }

  test("should support labelscan + limit under apply") {
    val nodes = givenGraph {
      nodeGraph(sizeHint, "A")
    }
    val limit = 3

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(limit)
      .|.nodeByLabelScan("a2", "A", IndexOrderNone)
      .allNodeScan("a1")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(List.fill(limit)(_))
    runtimeResult should beColumns("a1").withRows(singleColumn(expected))
  }

  test("should support unwind + limit under apply") {
    val nodes = givenGraph {
      nodeGraph(sizeHint, "A")
    }
    val limit = 3

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(limit)
      .|.unwind("[1, 2, 3, 4, 5] AS a2")
      .|.argument()
      .allNodeScan("a1")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(List.fill(limit)(_))
    runtimeResult should beColumns("a1").withRows(singleColumn(expected))
  }

  test("should support limit under semiApply") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "one", "two")
      .projection("1 AS one", "2 AS two")
      .semiApply()
      .|.limit(2)
      .|.unwind("[5,6,7,8] AS y")
      .|.argument()
      .unwind("[1,2,3,4] AS x")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = (1 to 4).map(x => Array[Any](x, 1, 2))
    runtimeResult should beColumns("x", "one", "two").withRows(expected)
  }

  test("should support single-nodeByIdSeek + limit under apply") {
    val nodes = givenGraph {
      nodeGraph(sizeHint, "A")
    }
    val limit = 0

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(limit)
      .|.nodeByIdSeek("a2", Set("a1"), nodes.head.getId)
      .allNodeScan("a1")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a1").withNoRows()
  }

  test("should support multi-nodeByIdSeek + limit under apply") {
    val nodes = givenGraph {
      nodeGraph(sizeHint, "A")
    }
    val limit = 3

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(limit)
      .|.nodeByIdSeek("a2", Set("a1"), nodes.take(2 * limit).map(_.getId): _*)
      .allNodeScan("a1")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(List.fill(limit)(_))
    runtimeResult should beColumns("a1").withRows(singleColumn(expected))
  }

  test("should support single-directedRelationshipByIdSeek + limit under apply") {
    val (nodes, relationships) = givenGraph {
      circleGraph(sizeHint, "A")
    }
    val limit = 0

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(limit)
      .|.directedRelationshipByIdSeek("r", "x", "y", Set("a1"), relationships.head.getId)
      .allNodeScan("a1")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a1").withNoRows()
  }

  test("should support multi-directedRelationshipByIdSeek + limit under apply") {
    val (nodes, relationships) = givenGraph {
      circleGraph(sizeHint, "A")
    }
    val limit = 3

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(limit)
      .|.directedRelationshipByIdSeek("r", "x", "y", Set("a1"), relationships.map(_.getId): _*)
      .allNodeScan("a1")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(List.fill(limit)(_))
    runtimeResult should beColumns("a1").withRows(singleColumn(expected))
  }

  test("should support single-undirectedRelationshipByIdSeek + limit under apply") {
    val (nodes, relationships) = givenGraph {
      circleGraph(sizeHint, "A")
    }
    val limit = 0

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(limit)
      .|.undirectedRelationshipByIdSeek("r", "x", "y", Set("a1"), relationships.head.getId)
      .allNodeScan("a1")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a1").withNoRows()
  }

  test("should support multi-undirectedRelationshipByIdSeek + limit under apply") {
    val (nodes, relationships) = givenGraph {
      circleGraph(sizeHint, "A")
    }
    val limit = 3

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(limit)
      .|.undirectedRelationshipByIdSeek("r", "x", "y", Set("a1"), relationships.map(_.getId): _*)
      .allNodeScan("a1")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(List.fill(limit)(_))
    runtimeResult should beColumns("a1").withRows(singleColumn(expected))
  }

  test("should support nodeIndexScan + limit under apply") {
    val nodes = givenGraph {
      nodeIndex("A", "prop")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "A"
      )
    }
    val limit = 3

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(limit)
      .|.nodeIndexOperator("x:A(prop)")
      .allNodeScan("a1")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(List.fill(limit)(_))
    runtimeResult should beColumns("a1").withRows(singleColumn(expected))
  }

  test("should support nodeIndexSeek + limit under apply") {
    val nodes = givenGraph {
      nodeIndex("A", "prop")
      nodePropertyGraph(
        sizeHint,
        {
          case _ => Map("prop" -> 42)
        },
        "A"
      )
    }
    val limit = 3

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(limit)
      .|.nodeIndexOperator("x:A(prop = 42)")
      .allNodeScan("a1")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(List.fill(limit)(_))
    runtimeResult should beColumns("a1").withRows(singleColumn(expected))
  }

  test("should support multi-nodeIndexSeek + limit under apply") {
    val nodes = givenGraph {
      nodeIndex("A", "prop")
      nodePropertyGraph(
        sizeHint,
        {
          case _ => Map("prop" -> 42)
        },
        "A"
      )
    }
    val limit = 3

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(limit)
      .|.nodeIndexOperator("x:A(prop = 42 OR 76)")
      .allNodeScan("a1")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(List.fill(limit)(_))
    runtimeResult should beColumns("a1").withRows(singleColumn(expected))
  }

  test("should support composite-nodeIndexSeek + limit under apply") {
    val nodes = givenGraph {
      nodeIndex("A", "prop1", "prop2")
      nodePropertyGraph(
        sizeHint,
        {
          case _ => Map("prop1" -> 42, "prop2" -> 1337)
        },
        "A"
      )
    }
    val limit = 3

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(limit)
      .|.nodeIndexOperator("x:A(prop1 = 42, prop2 = 1337)")
      .allNodeScan("a1")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(List.fill(limit)(_))
    runtimeResult should beColumns("a1").withRows(singleColumn(expected))
  }

  test("should support chained limits on RHS of Apply") {
    val nodes = givenGraph {
      val (aNodes, _) = bipartiteGraph(10, "A", "B", "R")
      aNodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .apply()
      .|.limit(2)
      .|.limit(10)
      .|.expandAll("(a)-->(b)")
      .|.argument()
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(List.fill(2)(_))
    runtimeResult should beColumns("a").withRows(singleColumn(expected))
  }

  test("should support multiple limits on RHS of Apply where only first limit is limiting") {
    val nodeCount = 10
    val nodes = givenGraph {
      val (aNodes, _) = bipartiteGraph(nodeCount, "A", "B", "R")
      aNodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(nodeCount + 1)
      .|.expandAll("(a2)-->(b)")
      .|.limit(1)
      .|.nodeByLabelScan("a2", "A", IndexOrderNone)
      .nodeByLabelScan("a1", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(List.fill(nodeCount)(_))

    runtimeResult should beColumns("a1").withRows(singleColumn(expected))
  }

  test("should support multiple limits on RHS of Apply where only second limit is limiting") {
    val nodeCount = 10
    val nodes = givenGraph {
      val (aNodes, _) = bipartiteGraph(nodeCount, "A", "B", "R")
      aNodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(2)
      .|.expandAll("(a2)-->(b)")
      .|.limit(nodeCount + 1)
      .|.nodeByLabelScan("a2", "A", IndexOrderNone)
      .nodeByLabelScan("a1", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(List.fill(2)(_))

    runtimeResult should beColumns("a1").withRows(singleColumn(expected))
  }

  test("should support limit under apply, with multiple input-rows per argument") {
    // given
    val aNodes = givenGraph {
      val a1 = tx.createNode(label("A"))
      a1.createRelationshipTo(tx.createNode(label("B")), withName("R"))
      a1.createRelationshipTo(tx.createNode(label("B")), withName("R"))
      val a2 = tx.createNode(label("A"))
      a2.createRelationshipTo(tx.createNode(label("B")), withName("R"))
      tx.createNode(label("A"))
      Seq(a1, a2)
    }

    val limit = 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(limit)
      .|.expandAll("(a1)-->(b2)")
      .|.nonFuseable()
      .|.expandAll("(a1)-->(b2)")
      .|.argument()
      .nodeByLabelScan("a1", "A", IndexOrderNone)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("a1").withRows(singleColumn(aNodes))
  }

  test("should support limit under apply, with multiple input-rows per argument, produce result not fused") {
    // given
    val aNodes = givenGraph {
      val a1 = tx.createNode(label("A"))
      a1.createRelationshipTo(tx.createNode(label("B")), withName("R"))
      a1.createRelationshipTo(tx.createNode(label("B")), withName("R"))
      val a2 = tx.createNode(label("A"))
      a2.createRelationshipTo(tx.createNode(label("B")), withName("R"))
      tx.createNode(label("A"))
      Seq(a1, a2)
    }

    val limit = 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .nonFuseable()
      .apply()
      .|.limit(limit)
      .|.expandAll("(a1)-->(b2)")
      .|.nonFuseable()
      .|.expandAll("(a1)-->(b2)")
      .|.argument()
      .nodeByLabelScan("a1", "A", IndexOrderNone)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("a1").withRows(singleColumn(aNodes))
  }

  test("should support limit under apply, with multiple input-rows per argument with random connections") {
    val nodeConnections = givenGraph {
      val nodes = nodeGraph(10, "A")
      randomlyConnect(nodes, Connectivity(0, 5, "OTHER")).map {
        case NodeConnections(node, connections) =>
          val neighbours = if (connections.isEmpty) Seq.empty else connections("OTHER")
          (node, neighbours)
      }.toMap
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(1)
      .|.expandAll("(a1)-->(b2)")
      .|.nonFuseable()
      .|.expandAll("(a1)-->(b2)")
      .|.argument()
      .nodeByLabelScan("a1", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodeConnections.keys.filter(node => nodeConnections(node).nonEmpty)
    runtimeResult should beColumns("a1").withRows(singleColumn(expected))
  }

  test(
    "should support limit under apply, with multiple input-rows per argument with random connections, produce result not fused"
  ) {
    val nodeConnections = givenGraph {
      val nodes = nodeGraph(10, "A")
      randomlyConnect(nodes, Connectivity(0, 5, "OTHER")).map {
        case NodeConnections(node, connections) =>
          val neighbours = if (connections.isEmpty) Seq.empty else connections("OTHER")
          (node, neighbours)
      }.toMap
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .nonFuseable()
      .apply()
      .|.limit(1)
      .|.expandAll("(a1)-->(b2)")
      .|.nonFuseable()
      .|.expandAll("(a1)-->(b2)")
      .|.argument()
      .nodeByLabelScan("a1", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodeConnections.keys.filter(node => nodeConnections(node).nonEmpty)
    runtimeResult should beColumns("a1").withRows(singleColumn(expected))
  }

  test("should support two limits at different different nesting levels - when RHS limit is lower than top limit") {
    val nodeCount = 10
    givenGraph {
      val (aNodes, _) = bipartiteGraph(nodeCount, "A", "B", "R")
      aNodes
    }

    val topLimit = ThreadLocalRandom.current().nextInt(1, nodeCount)
    val rhsLimit = ThreadLocalRandom.current().nextInt(1, nodeCount)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .limit(topLimit)
      .apply()
      .|.limit(rhsLimit)
      .|.expand("(a)-->(b1)")
      .|.nonFuseable()
      .|.expand("(a)-->(b0)")
      .|.argument("a")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    withClue(s"Top Limit = $topLimit , RHS Limit = $rhsLimit") {
      runtimeResult should beColumns("a").withRows(rowCount(topLimit))
    }
  }

  test("should support two limits at  different nesting levels - when RHS limit is higher than top limit") {
    val nodeCount = 10
    givenGraph {
      val (aNodes, _) = bipartiteGraph(nodeCount, "A", "B", "R")
      aNodes
    }

    val topLimit = ThreadLocalRandom.current().nextInt(1, nodeCount)
    val rhsLimit = ThreadLocalRandom.current().nextInt(1, nodeCount)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .limit(topLimit)
      .apply()
      .|.limit(rhsLimit)
      .|.expand("(a)-->(b1)")
      .|.nonFuseable()
      .|.expand("(a)-->(b0)")
      .|.argument("a")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    withClue(s"Top Limit = $topLimit , RHS Limit = $rhsLimit") {
      runtimeResult should beColumns("a").withRows(rowCount(topLimit))
    }
  }

  test("should let through all LHS rows of antiSemiApply") {
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .antiSemiApply()
      .|.limit(0)
      .|.argument("n")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n").withRows(nodes.map(Array[Any](_)))
  }

  test("should support reduce -> limit 0 on the RHS of apply") {
    val nodesPerLabel = 100
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.limit(0)
      .|.sort("y ASC")
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("should support limit 0-> reduce on the RHS of apply") {
    val nodesPerLabel = 100
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.sort("y ASC")
      .|.limit(0)
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("should support chained limit 0") {
    val nodesPerLabel = 100
    givenGraph { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a2")
      .limit(0)
      .expandAll("(b2)<--(a2)")
      .limit(0)
      .expandAll("(a1)-->(b2)")
      .limit(0)
      .expandAll("(b1)<--(a1)")
      .limit(0)
      .expandAll("(x)-->(b1)")
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a2").withNoRows()
  }

  test("should support optional expand(into) + limit 0 under apply") {
    givenGraph {
      val (aNodes, bNodes) = bipartiteGraph(3, "A", "B", "R")
      for {
        a <- aNodes
        b <- bNodes
      } {
        a.createRelationshipTo(b, withName("R"))
      }
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(0)
      .|.optionalExpandInto("(a1)-->(b)")
      .|.nonFuseable()
      .|.nodeByLabelScan("b", "B", IndexOrderNone)
      .nodeByLabelScan("a1", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a1").withNoRows()
  }

  test("should support unwind + limit 0 under apply") {
    givenGraph {
      nodeGraph(sizeHint, "A")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(0)
      .|.unwind("[1, 2, 3, 4, 5] AS a2")
      .|.argument()
      .allNodeScan("a1")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a1").withNoRows()
  }

  test("should support chained limit 0 on RHS of Apply") {
    givenGraph {
      bipartiteGraph(10, "A", "B", "R")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .apply()
      .|.limit(0)
      .|.limit(0)
      .|.expandAll("(a)-->(b)")
      .|.argument()
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a").withNoRows()
  }

  test("should not call downstream if limit 0") {
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .limit(0)
      .aggregation(Seq.empty, Seq("count(*) AS c"))
      .apply()
      .|.errorPlan(new RuntimeException("NO!"))
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult =
      execute(logicalQuery, runtime, inputValues(Array[Any](1)), testPlanCombinationRewriterHints = Set(NoRewrites))
    runtimeResult should beColumns("c").withNoRows()
  }
}
