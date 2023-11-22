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
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths.SkipSameNode
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RowCount
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.TestPath
import org.neo4j.graphdb.Direction.INCOMING
import org.neo4j.graphdb.Direction.OUTGOING
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.internal.helpers.collection.Iterables.single

import java.util

abstract class ShortestPathTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  protected val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("shortest path in a linked chain graph") {
    // given
    val chainCount = 4
    val chainDepth = 4
    // number of shortest paths = chainCount^chainDepth, i.e., 256 in this case
    val (start, end) = givenGraph {
      linkedChainGraph(chainCount, chainDepth)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*]->(y)", pathName = Some("path"), all = true)
      .cartesianProduct()
      .|.nodeByIdSeek("y", Set.empty, end.getId)
      .nodeByIdSeek("x", Set.empty, start.getId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expectedRowCount = Math.pow(chainCount, chainDepth).toInt

    runtimeResult should beColumns("path").withRows(rowCount(expectedRowCount))
  }

  test("shortest path in a sine graph") {
    // given
    val (start, end, rels) = givenGraph {
      val g = sineGraph()
      (g.start, g.end, Seq(g.startMiddle, g.endMiddle))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y", "path")
      .shortestPath("(x)-[r*]-(y)", Some("path"))
      .cartesianProduct()
      .|.nodeByLabelScan("y", "END", IndexOrderNone)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](start, util.Arrays.asList(rels.toSeq: _*), end, TestPath(start, rels))
    )

    runtimeResult should beColumns("x", "r", "y", "path").withRows(expected)
  }

  test("all shortest paths in a lollipop graph") {
    // given
    val (start, end, r1, r2, r3) = givenGraph {
      val (Seq(n1, _, n3), Seq(r1, r2, r3)) = lollipopGraph()
      n3.addLabel(Label.label("END"))
      (n1, n3, r1, r2, r3)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y", "path")
      .shortestPath("(x)-[r*]-(y)", Some("path"), all = true)
      .cartesianProduct()
      .|.nodeByLabelScan("y", "END", IndexOrderNone)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](start, util.Arrays.asList(r1, r3), end, TestPath(start, Seq(r1, r3))),
      Array[Object](start, util.Arrays.asList(r2, r3), end, TestPath(start, Seq(r2, r3)))
    )

    runtimeResult should beColumns("x", "r", "y", "path").withRows(expected)
  }

  // EXPANSION FILTERING, RELATIONSHIP TYPE

  test("should filter on relationship type A") {
    // given
    val (start, end, rels) = givenGraph {
      val g = sineGraph()
      val r2 = single(g.ea1.getRelationships(INCOMING))
      val r3 = single(g.ea1.getRelationships(OUTGOING))
      (g.start, g.end, Seq(g.startMiddle, r2, r3))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y", "path")
      .shortestPath("(x)-[r:A*]->(y)", Some("path"))
      .cartesianProduct()
      .|.nodeByLabelScan("y", "END", IndexOrderNone)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y", "path").withRows(Array(
      Array(start, util.Arrays.asList(rels.toSeq: _*), end, TestPath(start, rels))
    ))
  }

  test("should filter on relationship type B") {
    // given
    val (start, end, rels) = givenGraph {
      val g = sineGraph()
      val r1 = single(g.sb1.getRelationships(INCOMING))
      val r2 = single(g.sb2.getRelationships(INCOMING))
      val r3 = single(g.sb2.getRelationships(OUTGOING))
      val r4 = single(g.eb1.getRelationships(INCOMING))
      val r5 = single(g.eb2.getRelationships(INCOMING))
      val r6 = single(g.eb2.getRelationships(OUTGOING))
      (g.start, g.end, Seq(r1, r2, r3, r4, r5, r6))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y", "path")
      .shortestPath("(x)-[r:B*]->(y)", Some("path"))
      .cartesianProduct()
      .|.nodeByLabelScan("y", "END", IndexOrderNone)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y", "path").withRows(Array(
      Array(start, util.Arrays.asList(rels.toSeq: _*), end, TestPath(start, rels))
    ))
  }

  // EXPANSION FILTERING, NODE AND RELATIONSHIP PREDICATE

  test("should filter on node predicate") {
    // given
    val (start, end, forbidden, rels) = givenGraph {
      val g = sineGraph()
      val r2 = single(g.ec1.getRelationships(INCOMING))
      val r3 = single(g.ec2.getRelationships(INCOMING))
      val r4 = single(g.ec3.getRelationships(INCOMING))
      val r5 = single(g.ec3.getRelationships(OUTGOING))
      (g.start, g.end, g.ea1, Seq(g.startMiddle, r2, r3, r4, r5))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y", "path")
      .shortestPath(
        "(x)-[r:A*]->(y)",
        Some("path"),
        nodePredicates = Seq(Predicate("n", s"id(n) <> ${forbidden.getId}"))
      )
      .cartesianProduct()
      .|.nodeByLabelScan("y", "END", IndexOrderNone)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y", "path").withRows(Array(
      Array(start, util.Arrays.asList(rels.toSeq: _*), end, TestPath(start, rels))
    ))
  }

  test("should filter on node predicate on first node") {
    // given
    val start = givenGraph {
      val g = sineGraph()
      g.start
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .shortestPath(
        "(x)-[r:A*]->(y)",
        Some("path"),
        nodePredicates = Seq(Predicate("n", s"id(n) <> ${start.getId}"))
      )
      .cartesianProduct()
      .|.nodeByLabelScan("y", "END", IndexOrderNone)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withNoRows()
  }

  test("should filter on node predicate on first node from reference") {
    // given
    val start = givenGraph {
      val g = sineGraph()
      g.start
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .shortestPath(
        "(X)-[r:A*]->(y)",
        Some("path"),
        nodePredicates = Seq(Predicate("n", s"id(n) <> ${start.getId}"))
      )
      .projection("x AS X")
      .cartesianProduct()
      .|.nodeByLabelScan("y", "END", IndexOrderNone)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withNoRows()
  }

  test("should filter on relationship predicate") {
    // given
    val (start, end, forbidden, rels) = givenGraph {
      val g = sineGraph()
      val r1 = single(g.sa1.getRelationships(INCOMING))
      val r2 = single(g.sa1.getRelationships(OUTGOING))
      val r3 = single(g.ea1.getRelationships(INCOMING))
      val r4 = single(g.ea1.getRelationships(OUTGOING))
      (g.start, g.end, g.startMiddle, Seq(r1, r2, r3, r4))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y", "path")
      .shortestPath(
        "(x)-[r:A*]->(y)",
        Some("path"),
        relationshipPredicates = Seq(Predicate("rel", s"id(rel) <> ${forbidden.getId}"))
      ) // OBS: r != rel
      .cartesianProduct()
      .|.nodeByLabelScan("y", "END", IndexOrderNone)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y", "path").withRows(Array(
      Array(start, util.Arrays.asList(rels.toSeq: _*), end, TestPath(start, rels))
    ))
  }

  test("should filter on node predicat - don't fully fuse") {
    // given
    val (start, end, forbidden, rels) = givenGraph {
      val g = sineGraph()
      val r2 = single(g.ec1.getRelationships(INCOMING))
      val r3 = single(g.ec2.getRelationships(INCOMING))
      val r4 = single(g.ec3.getRelationships(INCOMING))
      val r5 = single(g.ec3.getRelationships(OUTGOING))
      (g.start, g.end, g.ea1, Seq(g.startMiddle, r2, r3, r4, r5))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y", "path")
      .nonFuseable()
      .filter("true")
      .shortestPath(
        "(x)-[r:A*]->(y)",
        Some("path"),
        nodePredicates = Seq(Predicate("n", s"id(n) <> ${forbidden.getId}"))
      )
      .cartesianProduct()
      .|.nodeByLabelScan("y", "END", IndexOrderNone)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y", "path").withRows(Array(
      Array(start, util.Arrays.asList(rels.toSeq: _*), end, TestPath(start, rels))
    ))
  }

  test("should filter on relationship predicate - don't fully fuse") {
    // given
    val (start, end, forbidden, rels) = givenGraph {
      val g = sineGraph()
      val r1 = single(g.sa1.getRelationships(INCOMING))
      val r2 = single(g.sa1.getRelationships(OUTGOING))
      val r3 = single(g.ea1.getRelationships(INCOMING))
      val r4 = single(g.ea1.getRelationships(OUTGOING))
      (g.start, g.end, g.startMiddle, Seq(r1, r2, r3, r4))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y", "path")
      .nonFuseable()
      .filter("true")
      .shortestPath(
        "(x)-[r:A*]->(y)",
        Some("path"),
        relationshipPredicates = Seq(Predicate("rel", s"id(rel) <> ${forbidden.getId}"))
      ) // OBS: r != rel
      .cartesianProduct()
      .|.nodeByLabelScan("y", "END", IndexOrderNone)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "r", "y", "path").withRows(Array(
      Array(start, util.Arrays.asList(rels.toSeq: _*), end, TestPath(start, rels))
    ))
  }

  test("should handle var expand + predicate on cached property") {
    // given
    val n = sizeHint / 6
    val paths = givenGraph {
      val ps = chainGraphs(n, "TO", "TO", "TO", "TOO", "TO")
      // set incrementing node property values along chain
      for {
        p <- ps
        i <- 0 until p.length()
        n = p.nodeAt(i)
      } n.setProperty("prop", i)
      // set property of last node to lowest value, so VarLength predicate fails
      for {
        p <- ps
        n = p.nodeAt(p.length())
      } n.setProperty("prop", -1)
      ps
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("b", "c")
      .expand("(b)-[*]->(c)", nodePredicates = Seq(Predicate("n", "n.prop > cache[a.prop]")))
      .expandAll("(a)-[:TO]->(b)")
      .nodeByLabelScan("a", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        path <- paths
        length <- 1 to 3
        p = path.slice(1, 1 + length)
      } yield Array(p.startNode, p.endNode())

    runtimeResult should beColumns("b", "c").withRows(expected)
  }

  test("should handle types missing on compile") {
    givenGraph {
      1 to 20 foreach { _ =>
        tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("BASE"))
      }
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .shortestPath("(x)-[rs:R|S|T]->(y)", Some("p"))
      .filter("y <> x")
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    execute(logicalQuery, runtime) should beColumns("x", "y").withRows(List.empty)

    // CREATE S
    givenGraph { tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("S")) }
    execute(logicalQuery, runtime) should beColumns("x", "y").withRows(RowCount(1))

    // CREATE R
    givenGraph { tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("R")) }
    execute(logicalQuery, runtime) should beColumns("x", "y").withRows(RowCount(2))

    // CREATE T
    givenGraph { tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("T")) }
    execute(logicalQuery, runtime) should beColumns("x", "y").withRows(RowCount(3))
  }

  test("cached plan should adapt to new relationship types") {
    givenGraph {
      1 to 20 foreach { _ =>
        tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("BASE"))
      }
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .shortestPath("(x)-[rs:R|S|T]->(y)", Some("p"))
      .filter("y <> x")
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val executablePlan = buildPlan(logicalQuery, runtime)

    execute(executablePlan) should beColumns("x", "y").withRows(List.empty)

    // CREATE S
    givenGraph { tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("S")) }
    execute(executablePlan) should beColumns("x", "y").withRows(RowCount(1))

    // CREATE R
    givenGraph { tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("R")) }
    execute(executablePlan) should beColumns("x", "y").withRows(RowCount(2))

    // CREATE T
    givenGraph { tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("T")) }
    execute(executablePlan) should beColumns("x", "y").withRows(RowCount(3))
  }

  test("Shortest path on limited RHS of apply") {

    val dim = 5
    givenGraph {
      gridGraph(dim, dim)
    }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .apply()
      .|.limit(2)
      .|.shortestPath(
        "(a)-[rs*1..]-(b)",
        pathName = Some("p"),
        all = true,
        sameNodeMode = SkipSameNode
      )
      .|.argument("a", "b")
      .filter("elementId(a) < elementId(b)")
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    def expectedPathCount(dim: Int) = {
      val nNodes = dim * dim
      val nPairs = (nNodes * (nNodes - 1)) / 2 // WHERE id(a) < id(b)

      // FIRST SEE [[gridGraph]]
      //
      // Due to the LIMIT 2, each pair of nodes will result in either 1 or 2 rows (no nodes are disconnected so 0 isn't
      // possible)
      //
      // We will fill the limit whenever two or more paths of shortest length exist between a pair of nodes.
      //
      // For the given graph,
      // a pair of nodes will have only one shortest path between them if and only if they are in the same column or row

      val nPairsWithOnlyOneShortestPath = {
        val nPairsOnAGivenRow = (dim * (dim - 1)) / 2
        val nPairsOnTheSameRow = dim * nPairsOnAGivenRow
        val nPairsInTheSameColumn = nPairsOnTheSameRow
        nPairsOnTheSameRow + nPairsInTheSameColumn
      }

      val nPairsWithAtleastTwoShortestPaths = nPairs - nPairsWithOnlyOneShortestPath

      2 * nPairsWithAtleastTwoShortestPaths + nPairsWithOnlyOneShortestPath
    }

    // then
    val expectedRowCount = expectedPathCount(dim)

    runtimeResult should beColumns("a", "b").withRows(rowCount(expectedRowCount))
  }

  test("Shortest path on eagerly aggregated RHS of apply") {

    val dim = 5
    givenGraph {
      gridGraph(dim, dim)
    }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .apply()
      .|.top(2, "`length(p)` ASC")
      .|.projection("length(p) AS `length(p)`")
      .|.shortestPath(
        "(a)-[rs*1..]-(b)",
        pathName = Some("p"),
        all = true,
        nodePredicates = Seq(),
        relationshipPredicates = Seq(),
        sameNodeMode = SkipSameNode
      )
      .|.argument("a", "b")
      .filter("elementId(a) < elementId(b)")
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    def expectedPathCount(dim: Int) = {
      val nNodes = dim * dim
      val nPairs = (nNodes * (nNodes - 1)) / 2 // WHERE id(a) < id(b)

      // See "Shortest path on limited RHS of apply" for info on cardinality calculation
      // This test returns exactly the same amount of rows.

      val nPairsWithOnlyOneShortestPath = {
        val nPairsOnAGivenRow = (dim * (dim - 1)) / 2
        val nPairsOnTheSameRow = dim * nPairsOnAGivenRow
        val nPairsInTheSameColumn = nPairsOnTheSameRow
        nPairsOnTheSameRow + nPairsInTheSameColumn
      }

      val nPairsWithAtleastTwoShortestPaths = nPairs - nPairsWithOnlyOneShortestPath

      2 * nPairsWithAtleastTwoShortestPaths + nPairsWithOnlyOneShortestPath
    }

    // then
    val expectedRowCount = expectedPathCount(dim)

    runtimeResult should beColumns("a", "b").withRows(rowCount(expectedRowCount))
  }

  test("Shortest path on eagerly aggregated RHS of apply with an inlined node filter") {
    val dim = 5
    givenGraph {
      gridGraph(dim, dim)
    }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .projection("nodes(p) AS nodes")
      .apply()
      .|.top(2, "`length(p)` ASC")
      .|.projection("length(p) AS `length(p)`")
      .|.shortestPath(
        "(a)-[rs*1..]-(b)",
        pathName = Some("p"),
        all = true,
        nodePredicates = Seq(Predicate("n", "NOT '0,0' IN labels(n)")),
        relationshipPredicates = Seq(),
        sameNodeMode = SkipSameNode
      )
      .|.argument("a", "b")
      .filter("elementId(a) < elementId(b)")
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    def expectedPathCount(dim: Int) = {
      val nNodes = dim * dim
      val nPairs = (nNodes * (nNodes - 1)) / 2 // WHERE id(a) < id(b)

      // See "Shortest path on limited RHS of apply" for info on cardinality calculation
      //
      // NEW FOR THIS TEST CASE:
      // Any shortest path containing topLeft must either originate from topLeft, or be between a
      // node on the top row and a node in the leftmost column.
      //
      // Any pair having topLeft as the start node won't yield any paths what so ever.
      //
      // For the (nodeInTopRow, nodeInLeftMostCol) pairs, even after excluding paths containing topLeft there
      // will be at least two shortest paths between nodeInTopRow, nodeInLeftMostCol when nodeInTopRow != 01 and
      // nodeInLeftMostCol != 10. In the (01, 10) case we go from having two shortest paths to one

      val nPairsExcludingTopLeft = nPairs - (nNodes - 1)

      val nPairsWithOnlyOneShortestPath = {
        val nPairsOnAGivenRow = (dim * (dim - 1)) / 2
        val nPairsOnTheSameRow = dim * nPairsOnAGivenRow
        val nPairsInTheSameColumn = nPairsOnTheSameRow

        // The n.o pairs on same row/col originating from topLeft
        val nOriginatingFromTopLeft = 2 * (dim - 1)

        // +1 due to the (01, 10) pair
        nPairsOnTheSameRow + nPairsInTheSameColumn - nOriginatingFromTopLeft + 1
      }

      val nPairsWithAtleastTwoShortestPaths = nPairsExcludingTopLeft - nPairsWithOnlyOneShortestPath

      2 * nPairsWithAtleastTwoShortestPaths + nPairsWithOnlyOneShortestPath
    }

    // then
    val expectedRowCount = expectedPathCount(dim)

    runtimeResult should beColumns("a", "b").withRows(rowCount(expectedRowCount))
  }
}
