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

import org.neo4j.configuration.GraphDatabaseSettings.dense_node_threshold
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandAllTestBase.smallTestGraph
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType

import scala.jdk.CollectionConverters.IteratorHasAsScala

abstract class MergeIntoTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  protected val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("mergeInto match relationship - outgoing") {
    assume(supportFastExpandInto())
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n) yield {
      Seq(
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)
    val (nodes, rels) = givenGraph {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .mergeInto("(x)-[r:NEXT]->(y)")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relTuples.zip(rels).map {
      case ((f, t, _), rel) => Array(nodes(f), nodes(t), rel)
    }

    runtimeResult should beColumns("x", "y", "r").withRows(expected).withNoUpdates()
  }

  test("mergeInto create relationship - outgoing") {
    assume(supportFastExpandInto())
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n) yield {
      Seq(
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)
    givenGraph {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .mergeInto("(x)-[r:NOT_THERE_YET]->(y)")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "r").withRows(rowCount(sizeHint)).withStatistics(relationshipsCreated =
      sizeHint
    )
  }

  test("mergeInto match relationship - incoming") {
    assume(supportFastExpandInto())
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n) yield {
      Seq(
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)
    val (nodes, rels) = givenGraph {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .mergeInto("(y)<-[r:NEXT]-(x)")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relTuples.zip(rels).map {
      case ((f, t, _), rel) => Array(nodes(f), nodes(t), rel)
    }

    runtimeResult should beColumns("x", "y", "r").withRows(expected).withNoUpdates()
  }

  test("mergeInto create relationship - incoming") {
    assume(supportFastExpandInto())
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n) yield {
      Seq(
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)
    givenGraph {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .mergeInto("(y)<-[r:NOT_THERE_YET]-(x)")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "r").withRows(rowCount(sizeHint)).withStatistics(relationshipsCreated =
      sizeHint
    )
  }

  test("mergeInto match relationship - both") {
    assume(supportFastExpandInto())
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n) yield {
      Seq(
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)
    val (nodes, rels) = givenGraph {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .mergeInto("(x)-[r:NEXT]-(y)")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relTuples.zip(rels).map {
      case ((f, t, _), rel) => Array(nodes(f), nodes(t), rel)
    }

    runtimeResult should beColumns("x", "y", "r").withRows(expected).withNoUpdates()
  }

  test("mergeInto create relationship - both") {
    assume(supportFastExpandInto())
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n) yield {
      Seq(
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)
    givenGraph {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .mergeInto("(x)-[r:NOT_THERE_YET]-(y)")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "r").withRows(rowCount(sizeHint)).withStatistics(relationshipsCreated =
      sizeHint
    )
  }

  test("should expand and handle self loops") {
    assume(supportFastExpandInto())
    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n) yield {
      Seq(
        (i, i, "ME")
      )
    }).reduce(_ ++ _)
    val (nodes, rels) = givenGraph {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .mergeInto("(x)-[r:ME]->(y)")
      .expand("(x)--(y)")
      .allNodeScan("x")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relTuples.zip(rels).map {
      case ((f, t, _), rel) => Array(nodes(f), nodes(t), rel)
    }

    runtimeResult should beColumns("x", "y", "r").withRows(expected).withNoUpdates()
  }

  test("should not create given an empty input") {
    assume(supportFastExpandInto())
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .mergeInto("(x)-[r:R]->(y)")
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "r").withNoRows().withNoUpdates()
  }

  test("should handle mergeInto outgoing") {
    assume(supportFastExpandInto())
    // given
    givenGraph { circleGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .mergeInto("(x)-[r:R]->(y)")
      .eager()
      .expandAll("(x)--(y)")
      .allNodeScan("x")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y").withRows(rowCount(sizeHint * 2)).withStatistics(relationshipsCreated =
      sizeHint
    )
  }

  test("should handle mergeInto incoming") {
    assume(supportFastExpandInto())
    // given
    givenGraph { circleGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .mergeInto("(x)<-[r:R]-(y)")
      .eager()
      .expandAll("(x)--(y)")
      .allNodeScan("x")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y").withRows(rowCount(sizeHint * 2)).withStatistics(relationshipsCreated =
      sizeHint
    )
  }

  test("should handle mergeInto undirected") {
    assume(supportFastExpandInto())
    // given
    givenGraph { circleGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .mergeInto("(x)-[r:R]-(y)")
      .eager()
      .expandAll("(x)--(y)")
      .allNodeScan("x")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y").withRows(rowCount(sizeHint * 2)).withNoUpdates()
  }

  test("should handle types missing on compile") {
    assume(supportFastExpandInto())

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .mergeInto("(x)-[:R]->(y)")
      .expandAll("(x)--(y)")
      .allNodeScan("x")
      .build(readOnly = false)

    execute(logicalQuery, runtime) should beColumns("x", "y").withRows(List.empty).withNoUpdates()

    // CREATE R
    givenGraph { tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("R")) }
    execute(logicalQuery, runtime) should beColumns("x", "y").withRows(rowCount(2))
  }

  test("should handle arguments spanning two morsels") {
    assume(supportFastExpandInto())

    // NOTE: This is a specific test for pipelined runtime with morsel size _4_
    // where an argument will span two morsels that are put into a MorselBuffer

    // given
    val (a1, a2, b1, b2, b3, c) = givenGraph { smallTestGraph(tx) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .apply()
      .|.expandAll("(b)-[:R]->(c)")
      .|.mergeInto("(a)-[:R]->(b)")
      .|.nodeByLabelScan("b", "B", IndexOrderNone, "a")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = for {
      a <- Seq(a1, a2)
      b <- Seq(b1, b2, b3)
    } yield Array(a, b, c)

    // then
    runtimeResult should beColumns("a", "b", "c").withRows(expected).withNoUpdates()
  }

  test("should support mergeInto on RHS of apply - outgoing match") {
    assume(supportFastExpandInto())

    // given
    val size = sizeHint / 16
    val (as, bs) = givenGraph {
      bipartiteGraph(size, "A", "B", "R")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .apply()
      .|.mergeInto("(a)-[:R]->(b)")
      .|.nodeByLabelScan("b", "B", IndexOrderNone, "a")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = for {
      a <- as
      b <- bs
    } yield Array(a, b)

    // then
    runtimeResult should beColumns("a", "b").withRows(expected).withNoUpdates()
  }

  test("should support mergeInto on RHS of apply - incoming match") {
    assume(supportFastExpandInto())

    // given
    val size = sizeHint / 16
    val (as, bs) = givenGraph {
      bipartiteGraph(size, "A", "B", "R")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .apply()
      .|.mergeInto("(b)<-[:R]-(a)")
      .|.nodeByLabelScan("b", "B", IndexOrderNone, "a")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = for {
      a <- as
      b <- bs
    } yield Array(a, b)

    // then
    runtimeResult should beColumns("a", "b").withRows(expected).withNoUpdates()
  }

  test("should support mergeInto on RHS of apply - outgoing create") {
    assume(supportFastExpandInto())

    // given
    val size = sizeHint / 16
    givenGraph {
      bipartiteGraph(size, "A", "B", "R")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .apply()
      .|.mergeInto("(a)-[:RNEW]->(b)")
      .|.nodeByLabelScan("b", "B", IndexOrderNone, "a")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a", "b").withRows(rowCount(size * size)).withStatistics(relationshipsCreated =
      size * size
    )
  }

  test("should support mergeInto on RHS of apply - incoming create") {
    assume(supportFastExpandInto())

    // given
    val size = sizeHint / 16
    givenGraph {
      bipartiteGraph(size, "A", "B", "R")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .apply()
      .|.mergeInto("(b)<-[:RNEW]-(a)")
      .|.nodeByLabelScan("b", "B", IndexOrderNone, "a")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a", "b").withRows(rowCount(size * size)).withStatistics(relationshipsCreated =
      size * size
    )
  }

  test("mergeInto with dense nodes without the queried REL_TYPE") {
    assume(supportFastExpandInto())

    val relsToCreate = edition.getSetting(dense_node_threshold).getOrElse(dense_node_threshold.defaultValue()) + 1

    // given
    givenGraph {
      // Two A nodes, and one dense B node.
      val a1 = tx.createNode(Label.label("A"))
      tx.createNode(Label.label("A"))
      val b = tx.createNode(Label.label("B"))
      // b has to be a dense node, but not have any REL relationships
      for (_ <- 1 to relsToCreate) a1.createRelationshipTo(b, RelationshipType.withName("ANOTHER_REL"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .mergeInto("(a)-[:REL]->(b)")
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B", IndexOrderNone)
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("a", "b").withRows(rowCount(2)).withStatistics(relationshipsCreated = 2)
  }

  test("block format store cursor reused but not reset bug") {
    assume(supportFastExpandInto())

    // given
    givenGraph {
      val node1 = nodeGraph(1, "L1")
      val nodes2 = nodeGraph(2, "L2")
      val nodes = node1 ++ nodes2
      connect(
        nodes,
        Seq(
          (0, 1, "T"),
          (0, 2, "T")
        )
      )
    }

    // Create new nodes in the _same_ transaction as the read query below
    nodeGraph(1, "M1")
    nodeGraph(1, "M2")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c", "d", "r")
      .apply()
      .|.mergeInto("(c)-[r:T]->(d)")
      .|.cartesianProduct()
      .|.|.nodeByLabelScan("d", "M2")
      .|.nodeByLabelScan("c", "M1")
      .apply()
      .|.limit(1)
      .|.expandAll("(a)-->(b)")
      .|.argument("a")
      .nodeByLabelScan("a", "L1")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // UpsertRelationship should not accidentally match a relationship from the preceding ExpandAll
    runtimeResult should beColumns("c", "d", "r").withRows(rowCount(1)).withStatistics(relationshipsCreated = 1)
  }

  test("should lock nodes if no matches") {
    assume(supportFastExpandInto())

    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.mergeInto("(x)-[r:R]->(x)")
      .|.argument("x")
      .allNodeScan("x")
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x")
      .withRows(nodes.map(Array(_)))
      .withStatistics(relationshipsCreated = sizeHint)
      .withLockedNodes(nodes.map(_.getId).toSet, onlyCheckContains = true)
  }

  test("should not lock nodes if on matches") {
    assume(supportFastExpandInto())

    // given
    val nodes = givenGraph {
      val nodes = nodeGraph(sizeHint)
      nodes.foreach(n => n.createRelationshipTo(n, RelationshipType.withName("R")))
      nodes
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.mergeInto("(x)-[r:R]->(x)")
      .|.argument("x")
      .allNodeScan("x")
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x")
      .withRows(nodes.map(Array(_)))
      .withNoUpdates()
      .withLockedNodes(Set.empty)
  }

  test("should lock refslot nodes") {
    assume(supportFastExpandInto())

    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("xRef")
      .apply()
      .|.mergeInto("(xRef)-[r:R]->(xRef)")
      .|.argument("x")
      .unwind("[x] as xRef")
      .allNodeScan("x")
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("xRef")
      .withRows(nodes.map(Array(_)))
      .withStatistics(relationshipsCreated = sizeHint)
      .withLockedNodes(nodes.map(_.getId).toSet, onlyCheckContains = true)
  }

  test("mergeInto should perform on match side effect") {
    assume(supportFastExpandInto())

    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n) yield {
      Seq(
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)
    val (nodes, rels) = givenGraph {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .mergeInto(
        "(x)-[r:NEXT]->(y)",
        onMatch = Seq("p1" -> "true", "p2" -> "42"),
        onCreate = Seq("p1" -> "true", "p2" -> "42")
      )
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    rels.foreach(rel => {
      rel.getProperty("p1") should equal(true)
      rel.getProperty("p2") should equal(42)
    })
    // then
    val expected = relTuples.zip(rels).map {
      case ((f, t, _), rel) => Array(nodes(f), nodes(t), rel)
    }

    runtimeResult should beColumns("x", "y", "r").withRows(expected).withStatistics(propertiesSet = sizeHint * 2)
  }

  test("mergeInto should perform on create side effect") {
    assume(supportFastExpandInto())

    // given
    val n = sizeHint
    val relTuples = (for (i <- 0 until n) yield {
      Seq(
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)
    val (nodes, rels) = givenGraph {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .mergeInto(
        "(x)-[r:NEW]->(y)",
        onMatch = Seq("p1" -> "true", "p2" -> "42"),
        onCreate = Seq("p1" -> "false", "p2" -> "'forty-two'")
      )
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    rels.foreach(rel => {
      rel.hasProperty("p1") should equal(false)
      rel.hasProperty("p2") should equal(false)
    })
    givenGraph(
      tx.findRelationships(RelationshipType.withName("NEW")).asScala.foreach(rel => {
        rel.getProperty("p1") should equal(false)
        rel.getProperty("p2") should equal("forty-two")

      })
    )

    // then
    runtimeResult should beColumns("x", "y", "r").withRows(rowCount(sizeHint)).withStatistics(
      relationshipsCreated =
        sizeHint,
      propertiesSet = 2 * sizeHint
    )
  }

  test("should profile dbHits with mergeInto on match") {
    assume(supportFastExpandInto())
    // given
    givenGraph { circleGraph(sizeHint) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .mergeInto("(x)-[r:R]->(y)")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build(readOnly = false)

    val expected = runtimeUsed match {
      case Pipelined => sizeHint * 2 /*node lookup*/
      case _         => sizeHint * 2 /*node lookup*/ + 1
    }
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)
    // then

    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() should equal(expected)
  }

  test("should profile dbHits with mergeInto on create") {
    assume(supportFastExpandInto())
    // given
    givenGraph { circleGraph(sizeHint) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .mergeInto("(x)-[r:R_NEW]->(y)")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build(readOnly = false)

    val expected = runtimeUsed match {
      case Pipelined => sizeHint
      case _         => sizeHint + 1
    }
    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)
    // then

    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() should equal(expected)
  }

  test("should profile rows with mergeInto on match") {
    assume(supportFastExpandInto())
    // given
    givenGraph { circleGraph(sizeHint) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .mergeInto("(x)-[r:R]->(y)")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build(readOnly = false)

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).rows() shouldBe sizeHint
  }

  test("should profile rows with mergeInto on create") {
    assume(supportFastExpandInto())
    // given
    givenGraph { circleGraph(sizeHint) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .mergeInto("(x)-[r:R_NEW]->(y)")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build(readOnly = false)

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).rows() shouldBe sizeHint
  }

  test("mergeInto should perform with onMatch and onCreate using") {
    assume(supportFastExpandInto())

    // given
    givenGraph {
      nodeGraph(1, "A")
      nodeGraph(1, "B")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("res")
      .projection("r.p AS res")
      .apply()
      .|.mergeInto(
        "(x)-[r:NEXT]->(y)",
        onCreate = Seq("p" -> "ONE"),
        onMatch = Seq("p" -> "TWO")
      )
      .|.argument("x", "y", "ONE", "TWO")
      .projection("x AS x", "y AS y", "1 AS ONE", "2 AS TWO")
      .cartesianProduct()
      .|.nodeByLabelScan("y", "B")
      .nodeByLabelScan("x", "A")
      .build(readOnly = false)

    // then
    execute(
      logicalQuery,
      runtime
    ) should beColumns("res").withSingleRow(1).withStatistics(relationshipsCreated = 1, propertiesSet = 1)
    execute(logicalQuery, runtime) should beColumns("res").withSingleRow(2).withStatistics(propertiesSet = 1)
  }
}

object MergeIntoTestBase
