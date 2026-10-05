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
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths.AllowSameNode
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths.DisallowSameNode
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths.SkipSameNode
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.TraversalPathMode.Walk
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint.Endpoint
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RowCount
import org.neo4j.cypher.internal.runtime.spec.Rows
import org.neo4j.cypher.internal.runtime.spec.RowsMatcher
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.TestPath
import org.neo4j.cypher.internal.runtime.spec.tests.ShortestPathTestBase.DbInstantiatedGraph
import org.neo4j.cypher.internal.util.test_helpers.graphtemplate.NamedEntites
import org.neo4j.exceptions.ShortestPathCommonEndNodesForbiddenException
import org.neo4j.graphdb.Direction.INCOMING
import org.neo4j.graphdb.Direction.OUTGOING
import org.neo4j.graphdb.Label.label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.internal.helpers.collection.Iterables.single
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.PathReference
import org.neo4j.values.virtual.VirtualPathValue
import org.neo4j.values.virtual.VirtualValues

import java.util

object ShortestPathTestBase {

  implicit class DbInstantiatedGraph(graph: NamedEntites[Node, Relationship]) {

    def pathReference(names: String*): PathReference = {
      val nodes = names.zipWithIndex.collect { case (n, i) if i % 2 == 0 => graph.node(n).getId }
      val rels = names.zipWithIndex.collect { case (r, i) if i % 2 == 1 => graph.rel(r).getId }

      VirtualValues.pathReference(
        nodes.toArray,
        rels.toArray
      )
    }
  }
}

//noinspection ZeroIndexToHead
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

  test("all shortest paths (length >= 1), AllowSameNode") {
    // given
    val (start, rels) = givenGraph {
      val n = tx.createNode()
      val r1 = n.createRelationshipTo(n, RelationshipType.withName("R"))
      val r2 = n.createRelationshipTo(n, RelationshipType.withName("R"))
      (n, Seq(r1, r2))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..]->(y)", pathName = Some("path"), all = true, sameNodeMode = AllowSameNode)
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      TestPath(start, Seq(rels(0))),
      TestPath(start, Seq(rels(1)))
    )
    runtimeResult should beColumns("path").withRows(singleColumn(expected))
  }

  test("all shortest paths (length >= 1), AllowSameNode, same variable") {
    // given
    val (start, rels) = givenGraph {
      val n = tx.createNode()
      val r1 = n.createRelationshipTo(n, RelationshipType.withName("R"))
      val r2 = n.createRelationshipTo(n, RelationshipType.withName("R"))
      (n, Seq(r1, r2))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..]->(x)", pathName = Some("path"), all = true, sameNodeMode = AllowSameNode)
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      TestPath(start, Seq(rels(0))),
      TestPath(start, Seq(rels(1)))
    )
    runtimeResult should beColumns("path").withRows(singleColumn(expected))
  }

  test("all shortest paths (length >= 1), AllowSameNode, longer shortest paths, directed") {
    // given
    val (start, r1, r2, r3, r4) = givenGraph {
      val n = tx.createNode()
      val m = tx.createNode()
      val r1 = n.createRelationshipTo(m, RelationshipType.withName("R"))
      val r2 = m.createRelationshipTo(n, RelationshipType.withName("R"))
      val r3 = n.createRelationshipTo(m, RelationshipType.withName("R"))
      val r4 = m.createRelationshipTo(n, RelationshipType.withName("R"))
      (n, r1, r2, r3, r4)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..]->(y)", pathName = Some("path"), all = true, sameNodeMode = AllowSameNode)
      .cartesianProduct()
      .|.nodeByElementIdSeek("y", Set.empty, start.getElementId)
      .nodeByElementIdSeek("x", Set.empty, start.getElementId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      TestPath(start, Seq(r1, r2)),
      TestPath(start, Seq(r3, r4)),
      TestPath(start, Seq(r1, r4)),
      TestPath(start, Seq(r3, r2))
    )
    runtimeResult should beColumns("path").withRows(singleColumn(expected))
  }

  test(
    "all shortest paths, AllowSameNode, same variable, does not drop rows when a branch exhausts its root set early"
  ) {
    // 6-cycle start-a-c-e-d-b-start plus a detour a-y-c. y and c share the start
    // relationship (start)-(a) at depth two, so an origin expanded early in a BFS level
    // sees a single root while the level as a whole still has two. Exhaustion must be
    // judged per level, not per origin, or the search stops before e is expanded and
    // the traces the DFS walks back from the meet are never recorded. Node creation
    // order is part of the repro: keep c before e.
    val (start, sa, ac, ce, ed, bd, bs) = givenGraph {
      val relType = RelationshipType.withName("R")
      val s = tx.createNode()
      val a = tx.createNode()
      val b = tx.createNode()
      val c = tx.createNode()
      val d = tx.createNode()
      val e = tx.createNode()
      val y = tx.createNode()
      val sa = s.createRelationshipTo(a, relType)
      val ac = a.createRelationshipTo(c, relType)
      val ce = c.createRelationshipTo(e, relType)
      val ed = e.createRelationshipTo(d, relType)
      val bd = b.createRelationshipTo(d, relType)
      val bs = b.createRelationshipTo(s, relType)
      a.createRelationshipTo(y, relType)
      y.createRelationshipTo(c, relType)
      (s, sa, ac, ce, ed, bd, bs)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..6]-(x)", pathName = Some("path"), all = true, sameNodeMode = AllowSameNode)
      .nodeByIdSeek("x", Set.empty, start.getId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      TestPath(start, Seq(sa, ac, ce, ed, bd, bs)),
      TestPath(start, Seq(bs, bd, ed, ce, ac, sa))
    )
    runtimeResult should beColumns("path").withRows(singleColumn(expected))
  }

  test("shortest path, AllowSameNode, same variable, finds an odd loop of length exactly maxDepth") {
    // Triangle start-a-b-start with maxDepth 3. The length-3 loop is detected at BFS
    // depth 1, the level whose shortest detectable loop (2 * depth + 1) equals maxDepth,
    // so the BFS must expand that level.
    val start = givenGraph {
      val relType = RelationshipType.withName("R")
      val s = tx.createNode()
      val a = tx.createNode()
      val b = tx.createNode()
      s.createRelationshipTo(a, relType)
      a.createRelationshipTo(b, relType)
      b.createRelationshipTo(s, relType)
      s
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..3]-(x)", pathName = Some("path"), all = false, sameNodeMode = AllowSameNode)
      .nodeByIdSeek("x", Set.empty, start.getId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("path").withRows(rowCount(1))
  }

  test("all shortest paths, AllowSameNode, same variable, does not duplicate rows after a same-root meet") {
    // Length-5 loop start-a-c-d-b-start (cross edge c-d) plus a same-root pair a-y-c.
    // y reaches c at the same depth on the same start relationship: not a loop, and c
    // must not be expanded a second time, which would record its traces twice and emit
    // each orientation twice. The cross edge keeps two roots alive so the search is not
    // cut short. Node creation order is part of the repro: keep c before y.
    val (start, sa, sb, ac, bd, cd) = givenGraph {
      val relType = RelationshipType.withName("R")
      val s = tx.createNode()
      val a = tx.createNode()
      val b = tx.createNode()
      val c = tx.createNode()
      val d = tx.createNode()
      val y = tx.createNode()
      val sa = s.createRelationshipTo(a, relType)
      val sb = s.createRelationshipTo(b, relType)
      val ac = a.createRelationshipTo(c, relType)
      val bd = b.createRelationshipTo(d, relType)
      val cd = c.createRelationshipTo(d, relType)
      a.createRelationshipTo(y, relType)
      y.createRelationshipTo(c, relType)
      (s, sa, sb, ac, bd, cd)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..6]-(x)", pathName = Some("path"), all = true, sameNodeMode = AllowSameNode)
      .nodeByIdSeek("x", Set.empty, start.getId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      TestPath(start, Seq(sa, ac, cd, bd, sb)),
      TestPath(start, Seq(sb, bd, cd, ac, sa))
    )
    runtimeResult should beColumns("path").withRows(singleColumn(expected))
  }

  test("shortest path, AllowSameNode, same variable, stays anchored at the start node after a same-root meet") {
    // Length-5 loop start-a-c-d-b-start (cross edge c-d) plus a same-root pair a-y-c. y is created
    // before c, so at depth two it is expanded first and reaches c on the same start relationship.
    // c's trace must keep pointing at a, or the path reconstructed through c does not begin at start.
    // Node creation order is part of the repro: keep y before c.
    val (start, sa, sb, ac, bd, cd) = givenGraph {
      val relType = RelationshipType.withName("R")
      val s = tx.createNode()
      val a = tx.createNode()
      val b = tx.createNode()
      val y = tx.createNode()
      val c = tx.createNode()
      val d = tx.createNode()
      val sa = s.createRelationshipTo(a, relType)
      val sb = s.createRelationshipTo(b, relType)
      val ac = a.createRelationshipTo(c, relType)
      val bd = b.createRelationshipTo(d, relType)
      val cd = c.createRelationshipTo(d, relType)
      a.createRelationshipTo(y, relType)
      y.createRelationshipTo(c, relType)
      (s, sa, sb, ac, bd, cd)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..6]-(x)", pathName = Some("path"), all = false, sameNodeMode = AllowSameNode)
      .nodeByIdSeek("x", Set.empty, start.getId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val forward = Seq(sa, ac, cd, bd, sb).map(_.getId)
    val backward = forward.reverse
    runtimeResult should beColumns("path").withRows(assertPaths(rowCount(1))(p =>
      p.startNodeId() == start.getId &&
        (p.relationshipIds().toSeq == forward || p.relationshipIds().toSeq == backward)
    ))
  }

  test("shortest path, AllowSameNode, same variable, prefers an odd loop over an even loop met earlier in the level") {
    // start-a-m-b-start has length four, start-p-q-start length three. Both are detected at BFS
    // depth one, where the frontier iterates in node id order: b meets m in the next frontier before
    // p or q is expanded. The cursor must not settle for the even loop. Node creation order is part
    // of the repro: keep a and b before p and q.
    val start = givenGraph {
      val relType = RelationshipType.withName("R")
      val s = tx.createNode()
      val a = tx.createNode()
      val b = tx.createNode()
      val p = tx.createNode()
      val q = tx.createNode()
      val m = tx.createNode()
      s.createRelationshipTo(a, relType)
      s.createRelationshipTo(b, relType)
      a.createRelationshipTo(m, relType)
      b.createRelationshipTo(m, relType)
      s.createRelationshipTo(p, relType)
      s.createRelationshipTo(q, relType)
      p.createRelationshipTo(q, relType)
      s
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..6]-(x)", pathName = Some("path"), all = false, sameNodeMode = AllowSameNode)
      .nodeByIdSeek("x", Set.empty, start.getId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("path").withRows(assertPaths(rowCount(1))(p => p.size() == 3))
  }

  test("all shortest paths, AllowSameNode, same variable, returns a lone self-relationship") {
    // The start node's only relationship is a self-relationship: one relationship, but a trail loop.
    val (start, self) = givenGraph {
      val s = tx.createNode()
      val self = s.createRelationshipTo(s, RelationshipType.withName("R"))
      (s, self)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..3]-(x)", pathName = Some("path"), all = true, sameNodeMode = AllowSameNode)
      .nodeByIdSeek("x", Set.empty, start.getId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("path").withRows(singleColumn(Seq(TestPath(start, Seq(self)))))
  }

  test("all shortest paths, AllowSameNode, same variable, ignores a longer odd loop through the start node") {
    // Triangle start-p-q-start and pentagon start-a-c-d-b-start. The pentagon's cross edge c-d is
    // met one level after the triangle and must not become an intersection, or the DFS overruns
    // its arrays.
    val (start, sp, pq, qs) = givenGraph {
      val relType = RelationshipType.withName("R")
      val s = tx.createNode()
      val p = tx.createNode()
      val q = tx.createNode()
      val a = tx.createNode()
      val b = tx.createNode()
      val c = tx.createNode()
      val d = tx.createNode()
      val sp = s.createRelationshipTo(p, relType)
      val pq = p.createRelationshipTo(q, relType)
      val qs = q.createRelationshipTo(s, relType)
      s.createRelationshipTo(a, relType)
      a.createRelationshipTo(c, relType)
      c.createRelationshipTo(d, relType)
      d.createRelationshipTo(b, relType)
      b.createRelationshipTo(s, relType)
      (s, sp, pq, qs)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..6]-(x)", pathName = Some("path"), all = true, sameNodeMode = AllowSameNode)
      .nodeByIdSeek("x", Set.empty, start.getId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(TestPath(start, Seq(sp, pq, qs)), TestPath(start, Seq(qs, pq, sp)))
    runtimeResult should beColumns("path").withRows(singleColumn(expected))
  }

  test("all shortest paths, AllowSameNode, same variable, ignores an edge between two intersections") {
    // Two 4-cycles start-a-i-b-start and start-c-j-d-start plus i-j. Both far nodes are
    // intersections of even loops; the edge between them can only lengthen a loop and must not be
    // followed by the DFS.
    val (start, sa, ai, ib, bs, sc, cj, jd, ds) = givenGraph {
      val relType = RelationshipType.withName("R")
      val s = tx.createNode()
      val a = tx.createNode()
      val b = tx.createNode()
      val i = tx.createNode()
      val c = tx.createNode()
      val d = tx.createNode()
      val j = tx.createNode()
      val sa = s.createRelationshipTo(a, relType)
      val ai = a.createRelationshipTo(i, relType)
      val ib = i.createRelationshipTo(b, relType)
      val bs = b.createRelationshipTo(s, relType)
      val sc = s.createRelationshipTo(c, relType)
      val cj = c.createRelationshipTo(j, relType)
      val jd = j.createRelationshipTo(d, relType)
      val ds = d.createRelationshipTo(s, relType)
      i.createRelationshipTo(j, relType)
      (s, sa, ai, ib, bs, sc, cj, jd, ds)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..6]-(x)", pathName = Some("path"), all = true, sameNodeMode = AllowSameNode)
      .nodeByIdSeek("x", Set.empty, start.getId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      TestPath(start, Seq(sa, ai, ib, bs)),
      TestPath(start, Seq(bs, ib, ai, sa)),
      TestPath(start, Seq(sc, cj, jd, ds)),
      TestPath(start, Seq(ds, jd, cj, sc))
    )
    runtimeResult should beColumns("path").withRows(singleColumn(expected))
  }

  test("all shortest paths (length >= 0), AllowSameNode") {
    // given
    val start = givenGraph {
      val n = tx.createNode()
      n.createRelationshipTo(n, RelationshipType.withName("R"))
      n.createRelationshipTo(n, RelationshipType.withName("R"))
      n
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*0..]->(y)", pathName = Some("path"), all = true, sameNodeMode = AllowSameNode)
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](
        TestPath(start, Seq())
      )
    )
    runtimeResult should beColumns("path").withRows(expected)
  }

  test("all shortest paths (length >= 0), AllowSameNode, same variable") {
    // given
    val start = givenGraph {
      val n = tx.createNode()
      n.createRelationshipTo(n, RelationshipType.withName("R"))
      n.createRelationshipTo(n, RelationshipType.withName("R"))
      n
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*0..]->(x)", pathName = Some("path"), all = true, sameNodeMode = AllowSameNode)
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](
        TestPath(start, Seq())
      )
    )
    runtimeResult should beColumns("path").withRows(expected)
  }

  test("shortest paths (length >= 1), AllowSameNode") {
    // given
    givenGraph {
      val n = tx.createNode()
      n.createRelationshipTo(n, RelationshipType.withName("R"))
      n.createRelationshipTo(n, RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..]->(y)", pathName = Some("path"), sameNodeMode = AllowSameNode)
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("path").withRows(rowCount(1))
  }

  test("shortest paths (length >= 1), AllowSameNode (single circle)") {
    // given
    val start = givenGraph {
      val (nodes, _) = circleGraph(10)
      nodes.foreach(_.createRelationshipTo(tx.createNode(), RelationshipType.withName("R")))
      nodes.head
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..]->(y)", pathName = Some("path"), sameNodeMode = AllowSameNode)
      .cartesianProduct()
      .|.nodeByElementIdSeek("y", Set.empty, start.getElementId)
      .nodeByElementIdSeek("x", Set.empty, start.getElementId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("path").withRows(assertPaths(rowCount(1))(p => p.size() == 10))
  }

  test("shortest paths (length >= 1), AllowSameNode (multi circle)") {
    // given
    val nNodes = 10
    val nCircles = 100
    val start = givenGraph {

      val startNode = tx.createNode()
      for (_ <- 0 until nCircles) {
        val nodes = startNode +: nodeGraph(nNodes - 1)
        for (i <- 0 until nNodes) {
          val a = nodes(i)
          val b = nodes((i + 1) % nNodes)
          a.createRelationshipTo(b, RelationshipType.withName("R"))
        }
      }
      startNode
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..]->(y)", pathName = Some("path"), sameNodeMode = AllowSameNode)
      .cartesianProduct()
      .|.nodeByElementIdSeek("y", Set.empty, start.getElementId)
      .nodeByElementIdSeek("x", Set.empty, start.getElementId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("path").withRows(assertPaths(rowCount(1))(p => p.size() == 10))
  }

  test("shortest paths (length >= 1), AllowSameNode, only one relationship") {
    // given
    val (start, r) = givenGraph {
      val n = tx.createNode()
      val r = n.createRelationshipTo(n, RelationshipType.withName("R"))
      (n, r)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..]->(y)", pathName = Some("path"), sameNodeMode = AllowSameNode)
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      TestPath(start, Seq(r))
    )
    runtimeResult should beColumns("path").withRows(singleColumn(expected))
  }

  test("shortest paths (length >= 1), AllowSameNode, longer shortest paths") {
    // given
    val (start, r1, r2) = givenGraph {
      val n = tx.createNode()
      val m = tx.createNode()
      val r1 = n.createRelationshipTo(m, RelationshipType.withName("R"))
      val r2 = m.createRelationshipTo(n, RelationshipType.withName("R"))
      (n, r1, r2)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..]->(y)", pathName = Some("path"), all = false, sameNodeMode = AllowSameNode)
      .cartesianProduct()
      .|.nodeByElementIdSeek("y", Set.empty, start.getElementId)
      .nodeByElementIdSeek("x", Set.empty, start.getElementId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      TestPath(start, Seq(r1, r2))
    )
    runtimeResult should beColumns("path").withRows(singleColumn(expected))
  }

  test("shortest paths (length >= 0), AllowSameNode") {
    // given
    val start = givenGraph {
      val n = tx.createNode()
      n.createRelationshipTo(n, RelationshipType.withName("R"))
      n.createRelationshipTo(n, RelationshipType.withName("R"))
      n
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*0..]->(y)", pathName = Some("path"), sameNodeMode = AllowSameNode)
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](
        TestPath(start, Seq())
      )
    )
    runtimeResult should beColumns("path").withRows(expected)
  }

  test("all shortest paths (length >= 1), DisallowSameNode") {
    // given
    givenGraph {
      val n = tx.createNode()
      n.createRelationshipTo(n, RelationshipType.withName("R"))
      n.createRelationshipTo(n, RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..]->(y)", pathName = Some("path"), all = true, sameNodeMode = DisallowSameNode)
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    a[ShortestPathCommonEndNodesForbiddenException] shouldBe thrownBy(consume(runtimeResult))
  }

  test("all shortest paths (length >= 1), DisallowSameNode, same variable") {
    // given
    givenGraph {
      val n = tx.createNode()
      n.createRelationshipTo(n, RelationshipType.withName("R"))
      n.createRelationshipTo(n, RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..]->(x)", pathName = Some("path"), all = true, sameNodeMode = DisallowSameNode)
      .allNodeScan("x")
      .build()

    // then
    a[ShortestPathCommonEndNodesForbiddenException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("all shortest paths (length >= 0), DisallowSameNode") {
    // given
    val start = givenGraph {
      val n = tx.createNode()
      n.createRelationshipTo(n, RelationshipType.withName("R"))
      n.createRelationshipTo(n, RelationshipType.withName("R"))
      n
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*0..]->(y)", pathName = Some("path"), all = true, sameNodeMode = DisallowSameNode)
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](
        TestPath(start, Seq())
      )
    )
    runtimeResult should beColumns("path").withRows(expected)
  }

  test("all shortest paths (length >= 0), DisallowSameNode, same variable") {
    // given
    givenGraph {
      val n = tx.createNode()
      n.createRelationshipTo(n, RelationshipType.withName("R"))
      n.createRelationshipTo(n, RelationshipType.withName("R"))
      n
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*0..]->(x)", pathName = Some("path"), all = true, sameNodeMode = DisallowSameNode)
      .allNodeScan("x")
      .build()

    // then
    a[ShortestPathCommonEndNodesForbiddenException] shouldBe thrownBy(consume(execute(logicalQuery, runtime)))
  }

  test("all shortest paths (length >= 1), SkipSameNode") {
    // given
    givenGraph {
      val n = tx.createNode()
      n.createRelationshipTo(n, RelationshipType.withName("R"))
      n.createRelationshipTo(n, RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..]->(y)", pathName = Some("path"), all = true, sameNodeMode = SkipSameNode)
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("path").withNoRows()
  }

  test("all shortest paths (length >= 1), SkipSameNode, same variable") {
    // given
    givenGraph {
      val n = tx.createNode()
      n.createRelationshipTo(n, RelationshipType.withName("R"))
      n.createRelationshipTo(n, RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..]->(x)", pathName = Some("path"), all = true, sameNodeMode = SkipSameNode)
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("path").withNoRows()
  }

  test("all shortest paths (length >= 0), SkipSameNode") {
    // given
    val start = givenGraph {
      val n = tx.createNode()
      n.createRelationshipTo(n, RelationshipType.withName("R"))
      n.createRelationshipTo(n, RelationshipType.withName("R"))
      n
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*0..]->(y)", pathName = Some("path"), all = true, sameNodeMode = SkipSameNode)
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](
        TestPath(start, Seq())
      )
    )
    runtimeResult should beColumns("path").withRows(expected)
  }

  test("all shortest paths (length >= 0), SkipSameNode, same variable") {
    // given
    val start = givenGraph {
      val n = tx.createNode()
      n.createRelationshipTo(n, RelationshipType.withName("R"))
      n.createRelationshipTo(n, RelationshipType.withName("R"))
      n
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*0..]->(x)", pathName = Some("path"), all = true, sameNodeMode = SkipSameNode)
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Seq(
      Array[Object](
        TestPath(start, Seq())
      )
    )
    runtimeResult should beColumns("path").withRows(expected)
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
      val (Seq(n1, _, n3), Seq(r1, r2, r3)) = lollipopGraph(): @unchecked
      n3.addLabel(label("END"))
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
      Array[Object](start, util.Arrays.asList(rels: _*), end, TestPath(start, rels))
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
      Array[Object](start, util.Arrays.asList(rels: _*), end, TestPath(start, rels))
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
      Array[Object](start, util.Arrays.asList(rels: _*), end, TestPath(start, rels))
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
      Array[Object](start, util.Arrays.asList(rels: _*), end, TestPath(start, rels))
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
      Array[Object](start, util.Arrays.asList(rels: _*), end, TestPath(start, rels))
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
      Array[Object](start, util.Arrays.asList(rels: _*), end, TestPath(start, rels))
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

  test("all shortest paths (length >= 1), AllowSameNode (single circle)") {
    // given
    val start = givenGraph {
      val (nodes, _) = circleGraph(10)
      nodes.foreach(_.createRelationshipTo(tx.createNode(), RelationshipType.withName("R")))
      nodes.head
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..]->(y)", pathName = Some("path"), all = true, sameNodeMode = AllowSameNode)
      .cartesianProduct()
      .|.nodeByElementIdSeek("y", Set.empty, start.getElementId)
      .nodeByElementIdSeek("x", Set.empty, start.getElementId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("path").withRows(assertPaths(rowCount(1))(p => p.size() == 10))
  }

  test("all shortest paths (length >= 1), AllowSameNode (multi circle)") {
    // given
    val nNodes = 10
    val nCircles = 100
    val start = givenGraph {

      val startNode = tx.createNode()
      for (_ <- 0 until nCircles) {
        val nodes = startNode +: nodeGraph(nNodes - 1)
        for (i <- 0 until nNodes) {
          val a = nodes(i)
          val b = nodes((i + 1) % nNodes)
          a.createRelationshipTo(b, RelationshipType.withName("R"))
        }
      }
      startNode
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..]->(y)", pathName = Some("path"), all = true, sameNodeMode = AllowSameNode)
      .cartesianProduct()
      .|.nodeByElementIdSeek("y", Set.empty, start.getElementId)
      .nodeByElementIdSeek("x", Set.empty, start.getElementId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("path").withRows(assertPaths(rowCount(100))(p => p.size() == 10))
  }

  test("all shortest path (length 0 allowed), AllowSameNode") {
    // given
    val start = givenGraph {
      //           (a)
      //         /  |  \
      // (start) - (c) (d)
      //         \ || /
      //          (b)
      val start = tx.createNode()
      val a = tx.createNode()
      val b = tx.createNode()
      val c = tx.createNode()
      val d = tx.createNode()
      start.createRelationshipTo(a, RelationshipType.withName("R"))
      start.createRelationshipTo(b, RelationshipType.withName("R"))
      start.createRelationshipTo(c, RelationshipType.withName("R"))

      a.createRelationshipTo(c, RelationshipType.withName("R"))
      a.createRelationshipTo(d, RelationshipType.withName("R"))

      c.createRelationshipTo(b, RelationshipType.withName("R"))
      c.createRelationshipTo(b, RelationshipType.withName("R"))

      d.createRelationshipTo(b, RelationshipType.withName("R"))
      start
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*0..]-(x)", pathName = Some("path"), all = true, sameNodeMode = AllowSameNode)
      .nodeByElementIdSeek("x", Set.empty, start.getElementId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("path").withRows(assertPaths(rowCount(1))(p => p.size() == 0))
  }

  test("all shortest path (length >= 1), AllowSameNode, undirected complicated case") {
    // given
    val start = givenGraph {
      //           (a)
      //         /  |  \
      // (start) - (c) (d)
      //         \ || /
      //          (b)
      val start = tx.createNode()
      val a = tx.createNode()
      val b = tx.createNode()
      val c = tx.createNode()
      val d = tx.createNode()
      start.createRelationshipTo(a, RelationshipType.withName("R"))
      start.createRelationshipTo(b, RelationshipType.withName("R"))
      start.createRelationshipTo(c, RelationshipType.withName("R"))

      a.createRelationshipTo(c, RelationshipType.withName("R"))
      a.createRelationshipTo(d, RelationshipType.withName("R"))

      c.createRelationshipTo(b, RelationshipType.withName("R"))
      c.createRelationshipTo(b, RelationshipType.withName("R"))

      d.createRelationshipTo(b, RelationshipType.withName("R"))
      start
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..]-(x)", pathName = Some("path"), all = true, sameNodeMode = AllowSameNode)
      .nodeByElementIdSeek("x", Set.empty, start.getElementId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("path").withRows(assertPaths(rowCount(6))(p => p.size() == 3))
  }

  test("single shortest path (length >= 1), AllowSameNode, undirected complicated case") {
    // given
    val start = givenGraph {
      //           (a)
      //         /  |  \
      // (start) - (c) (d)
      //         \ || /
      //          (b)
      val start = tx.createNode()
      val a = tx.createNode()
      val b = tx.createNode()
      val c = tx.createNode()
      val d = tx.createNode()
      start.createRelationshipTo(a, RelationshipType.withName("R"))
      start.createRelationshipTo(b, RelationshipType.withName("R"))
      start.createRelationshipTo(c, RelationshipType.withName("R"))

      a.createRelationshipTo(c, RelationshipType.withName("R"))
      a.createRelationshipTo(d, RelationshipType.withName("R"))

      c.createRelationshipTo(b, RelationshipType.withName("R"))
      c.createRelationshipTo(b, RelationshipType.withName("R"))

      d.createRelationshipTo(b, RelationshipType.withName("R"))
      start
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..]-(x)", pathName = Some("path"), sameNodeMode = AllowSameNode)
      .nodeByElementIdSeek("x", Set.empty, start.getElementId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("path").withRows(assertPaths(rowCount(1))(p => p.size() == 3))
  }

  test("all shortest path (length >= 1), AllowSameNode, undirected walk") {
    // given
    val start = givenGraph {
      //           (a)
      //         /  |  \
      // (start) - (c) (d)
      //         \ || /
      //          (b)
      val start = tx.createNode()
      val a = tx.createNode()
      val b = tx.createNode()
      val c = tx.createNode()
      val d = tx.createNode()
      start.createRelationshipTo(a, RelationshipType.withName("R"))
      start.createRelationshipTo(b, RelationshipType.withName("R"))
      start.createRelationshipTo(c, RelationshipType.withName("R"))

      a.createRelationshipTo(c, RelationshipType.withName("R"))
      a.createRelationshipTo(d, RelationshipType.withName("R"))

      c.createRelationshipTo(b, RelationshipType.withName("R"))
      c.createRelationshipTo(b, RelationshipType.withName("R"))

      d.createRelationshipTo(b, RelationshipType.withName("R"))
      start
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath(
        "(x)-[r*1..]-(x)",
        pathName = Some("path"),
        all = true,
        sameNodeMode = AllowSameNode,
        traversalPathMode = Walk
      )
      .nodeByElementIdSeek("x", Set.empty, start.getElementId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("path").withRows(assertPaths(rowCount(3))(p => p.size() == 2))
  }

  test("single shortest path (length >= 1), AllowSameNode, undirected walk") {
    // given
    val start = givenGraph {
      //           (a)
      //         /  |  \
      // (start) - (c) (d)
      //         \ || /
      //          (b)
      val start = tx.createNode()
      val a = tx.createNode()
      val b = tx.createNode()
      val c = tx.createNode()
      val d = tx.createNode()
      start.createRelationshipTo(a, RelationshipType.withName("R"))
      start.createRelationshipTo(b, RelationshipType.withName("R"))
      start.createRelationshipTo(c, RelationshipType.withName("R"))

      a.createRelationshipTo(c, RelationshipType.withName("R"))
      a.createRelationshipTo(d, RelationshipType.withName("R"))

      c.createRelationshipTo(b, RelationshipType.withName("R"))
      c.createRelationshipTo(b, RelationshipType.withName("R"))

      d.createRelationshipTo(b, RelationshipType.withName("R"))
      start
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPath("(x)-[r*1..]-(x)", pathName = Some("path"), sameNodeMode = AllowSameNode, traversalPathMode = Walk)
      .nodeByElementIdSeek("x", Set.empty, start.getElementId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("path").withRows(assertPaths(rowCount(1))(p => p.size() == 2))
  }

  test(
    "TraversalEndpoint(To) should resolve as the next node of a relationship traversal during predicate evaluation"
  ) {
    val graph = givenGraph {
      fromTemplate("""

        (a)<-[ab]-(b:TO)-[bc]->(c:TO)
         |                       ^
         '---------->()----------'

       """)
    }

    val relPredicates = Seq(
      VariablePredicate(varFor("r"), hasLabels(TraversalEndpoint(varFor("temp"), Endpoint.To), "TO"))
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPathExpr("(a)-[r*]-(c)", pathName = Some("path"), relationshipPredicates = relPredicates)
      .cartesianProduct()
      .|.nodeByIdSeek("a", Set.empty, graph.node("a").getId)
      .nodeByIdSeek("c", Set.empty, graph.node("c").getId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = inAnyOrder(Seq(
      Array(graph.pathReference("a", "ab", "b", "bc", "c"))
    ))

    runtimeResult should beColumns("path").withRows(expected)
  }

  test(
    "TraversalEndpoint(From) should resolve as the previous node of a relationship traversal during predicate evaluation"
  ) {
    val graph = givenGraph {
      fromTemplate("""
        (a:FROM)<-[ab]-(b:FROM)-[bc]->(c)
         |                             ^
         '------------->()-------------'
       """)
    }

    val relPredicates = Seq(
      VariablePredicate(varFor("r"), hasLabels(TraversalEndpoint(varFor("temp"), Endpoint.From), "FROM"))
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("path")
      .shortestPathExpr("(a)-[r*]-(c)", pathName = Some("path"), relationshipPredicates = relPredicates)
      .cartesianProduct()
      .|.nodeByIdSeek("a", Set.empty, graph.node("a").getId)
      .nodeByIdSeek("c", Set.empty, graph.node("c").getId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = inAnyOrder(Seq(
      Array(graph.pathReference("a", "ab", "b", "bc", "c"))
    ))

    runtimeResult should beColumns("path").withRows(expected)
  }

  test("shortest paths with node property predicate") {
    // given
    val (start, a, b, r1, r2) = givenGraph {

      /**
       *                 (t21)
       *               ↗
       *         (t11)
       *        ↗      ↘
       *                 (̶t̶2̶2̶)̶
       * (start)
       *                 (t23)
       *        ↘      ↗
       *          (̶t̶1̶2̶)̶)
       *               ↘
       *                 (̶t̶2̶4̶)̶
       *
       */
      val startNode = tx.createNode(label("Start"))
      startNode.setProperty("followMe", true)
      val t11 = tx.createNode(label("Neighbour"))
      t11.setProperty("followMe", true)
      val t12 = tx.createNode(label("Neighbour"))
      t12.setProperty("followMe", false)
      val r1 = startNode.createRelationshipTo(t11, RelationshipType.withName("R"))
      startNode.createRelationshipTo(t12, RelationshipType.withName("R"))

      val t21 = tx.createNode(label("Neighbour"))
      t21.setProperty("followMe", true)
      val t22 = tx.createNode(label("Neighbour"))
      t22.setProperty("followMe", false)
      val r2 = t11.createRelationshipTo(t21, RelationshipType.withName("R"))
      t11.createRelationshipTo(t22, RelationshipType.withName("R"))

      val t23 = tx.createNode(label("Neighbour"))
      t23.setProperty("followMe", true)
      val t24 = tx.createNode(label("Neighbour"))
      t24.setProperty("followMe", false)
      t12.createRelationshipTo(t23, RelationshipType.withName("R"))
      t12.createRelationshipTo(t24, RelationshipType.withName("R"))
      (startNode, t11, t21, r1, r2)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("nodes", "rels")
      .projection("relationships(p) AS rels", "nodes(p) AS nodes")
      .shortestPath(
        "(source)-[r*1..]-(target)",
        pathName = Some("p"),
        nodePredicates = Seq(Predicate("n", "n.followMe"))
      )
      .cartesianProduct()
      .|.nodeByLabelScan("target", "Neighbour")
      .nodeByLabelScan("source", "Start")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("nodes", "rels").withRows(Seq(
      Array(util.List.of(start, a), util.List.of(r1)),
      Array(util.List.of(start, a, b), util.List.of(r1, r2))
    ))
  }

  case class assertPaths(rowsMatcher: RowsMatcher)(check: VirtualPathValue => Boolean) extends RowsMatcher {
    override def toString: String = "All results should match the given path"

    override def matchesRaw(columns: IndexedSeq[String], rows: IndexedSeq[Array[AnyValue]]): Boolean = {

      rows.forall(row =>
        row.forall {
          case p: VirtualPathValue => check(p)
          case _                   => false
        }
      ) && rowsMatcher.matchesRaw(columns, rows)
    }

    override def formatRows(rows: IndexedSeq[Array[AnyValue]]): String = Rows.pretty(rows)
  }

}
