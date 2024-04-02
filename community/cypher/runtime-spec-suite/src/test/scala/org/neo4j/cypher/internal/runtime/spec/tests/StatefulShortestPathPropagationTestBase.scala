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
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.builder.TestNFABuilder
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.LoggingPPBFSHooks
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.RelationshipValue

import java.time.Instant

import scala.jdk.CollectionConverters.IterableHasAsScala

/**
 * These tests only test the propagation part of PGShortestPath, not the product graph part. That is why all patterns
 * look like ordinary var expands. The tests are essentially copied from PPBFS.
 */
abstract class StatefulShortestPathPropagationTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  protected val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  private val ENABLE_LOGS = false

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    PPBFSHooks.setInstance(if (ENABLE_LOGS) LoggingPPBFSHooks.debug else PPBFSHooks.NULL)
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    PPBFSHooks.setInstance(PPBFSHooks.NULL)
  }

  test("test logging is disabled in production") {
    ENABLE_LOGS shouldBe false
  }

  test("Infinity Fork") {

    givenGraph {

      // This is a graph that tests for over-propagation/under-pruning. Some bad implementations which don't prune
      // enough, or propagate to much, will end up propagating indefinitely and never terminate on this graph. More
      // specifically, the graph was used during development as a counter example to why some optimizations are
      // incorrect.
      //
      //   (n2: Target)      (n4: Target)
      //        ^                 ^
      //        |                 |
      //       (n1) <==========> (n3)
      //        ^
      //        |
      //   (n0: Source)
      //
      // The <=====> notation is meant to signify that there is one rel from n1 to n3,
      // and then one back from n3 to n1.

      val Seq(n0, n1, n2, n3, n4) = nodeGraph(5)

      val R = RelationshipType.withName("R")

      n0.createRelationshipTo(n1, R)
      n1.createRelationshipTo(n2, R)
      n1.createRelationshipTo(n3, R)
      n3.createRelationshipTo(n1, R)
      n3.createRelationshipTo(n4, R)

      n0.addLabel(Label.label("Source"))
      n2.addLabel(Label.label("Target"))
      n4.addLabel(Label.label("Target"))
    }
    // when
    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .filter("y: Target")
      .expand("(x)-[r*]->(y)")
      .nodeByLabelScan("x", "Source", IndexOrderNone)
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .varExpandAsShortest(
        "x",
        "r",
        "y",
        targetLabel = Some("Target")
      )
      .nodeByLabelScan("x", "Source", IndexOrderNone)
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "r", "y")
  }

  test("Undirected Infinity Fork") {

    givenGraph {

      // See previous test

      val Seq(n0, n1, n2, n3, n4) = nodeGraph(5)

      val R = RelationshipType.withName("R")

      n0.createRelationshipTo(n1, R)
      n1.createRelationshipTo(n2, R)
      n1.createRelationshipTo(n3, R)
      n3.createRelationshipTo(n1, R)
      n3.createRelationshipTo(n4, R)

      n0.addLabel(Label.label("Source"))
      n2.addLabel(Label.label("Target"))
      n4.addLabel(Label.label("Target"))
    }
    // when
    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .filter("y: Target")
      .expand("(x)-[r*]-(y)")
      .nodeByLabelScan("x", "Source", IndexOrderNone)
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .varExpandAsShortest(
        "x",
        "r",
        "y",
        targetLabel = Some("Target"),
        direction = SemanticDirection.BOTH
      )
      .nodeByLabelScan("x", "Source", IndexOrderNone)
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "r", "y")
  }

  test("Finity Fork") {

    givenGraph {
      // This test checks the opposite to what Infinity Fork™ checks. I.e this test checks that we don't over-prune or
      // under-propagate. Some bad implementations which do this will fail to return all paths over this graph
      // More specifically, the graph was used during development as a counter example to why some optimizations are
      // incorrect.
      //
      //   (n3: Target)      (n6: Target)
      //        ^                 ^
      //        |                 |
      //       (n2)              (n5)
      //      |    ^              ^
      //      |    |              |
      //      V    |              |
      //       (n1) -----------> (n4)
      //        ^
      //        |
      //   (n0: Source)

      val Seq(n0, n1, n2, n3, n4, n5, n6) = nodeGraph(7)

      val R = RelationshipType.withName("R")

      n0.createRelationshipTo(n1, R)
      n1.createRelationshipTo(n2, R)
      n1.createRelationshipTo(n4, R)
      n2.createRelationshipTo(n1, R)
      n2.createRelationshipTo(n3, R)
      n4.createRelationshipTo(n5, R)
      n5.createRelationshipTo(n6, R)

      n0.addLabel(Label.label("Source"))
      n3.addLabel(Label.label("Target"))
      n6.addLabel(Label.label("Target"))
    }
    // when
    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .filter("y: Target")
      .expand("(x)-[r*]->(y)")
      .nodeByLabelScan("x", "Source", IndexOrderNone)
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .varExpandAsShortest(
        "x",
        "r",
        "y",
        targetLabel = Some("Target")
      )
      .nodeByLabelScan("x", "Source", IndexOrderNone)
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "r", "y")
  }

  test("Undirected Finity Fork") {

    givenGraph {
      // See previous test

      val Seq(n0, n1, n2, n3, n4, n5, n6) = nodeGraph(7)

      val R = RelationshipType.withName("R")

      n0.createRelationshipTo(n1, R)
      n1.createRelationshipTo(n2, R)
      n1.createRelationshipTo(n4, R)
      n2.createRelationshipTo(n1, R)
      n2.createRelationshipTo(n3, R)
      n4.createRelationshipTo(n5, R)
      n5.createRelationshipTo(n6, R)

      n0.addLabel(Label.label("Source"))
      n3.addLabel(Label.label("Target"))
      n6.addLabel(Label.label("Target"))
    }
    // when
    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .filter("y: Target")
      .expand("(x)-[r*]-(y)")
      .nodeByLabelScan("x", "Source", IndexOrderNone)
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .varExpandAsShortest(
        "x",
        "r",
        "y",
        targetLabel = Some("Target"),
        direction = SemanticDirection.BOTH
      )
      .nodeByLabelScan("x", "Source", IndexOrderNone)
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "r", "y")
  }

  test("Finity Fork - The Shortening") {

    givenGraph {
      // Like Finity Fork™, except we also test propagation through targets
      //
      //   (n2: Target)      (n4: Target)
      //      |    ^              ^
      //      |    |              |
      //      V    |              |
      //       (n1) -----------> (n3)
      //        ^
      //        |
      //   (n0: Source)

      val Seq(n0, n1, n2, n3, n4) = nodeGraph(5)

      val R = RelationshipType.withName("R")

      n0.createRelationshipTo(n1, R)
      n1.createRelationshipTo(n2, R)
      n1.createRelationshipTo(n3, R)
      n2.createRelationshipTo(n1, R)
      n3.createRelationshipTo(n4, R)

      n0.addLabel(Label.label("Source"))
      n2.addLabel(Label.label("Target"))
      n4.addLabel(Label.label("Target"))
    }
    // when
    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .filter("y: Target")
      .expand("(x)-[r*]->(y)")
      .nodeByLabelScan("x", "Source", IndexOrderNone)
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .varExpandAsShortest(
        "x",
        "r",
        "y",
        targetLabel = Some("Target")
      )
      .nodeByLabelScan("x", "Source", IndexOrderNone)
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "r", "y")
  }

  test("Undirected Finity Fork - The Shortening") {

    givenGraph {
      // See previous test

      val Seq(n0, n1, n2, n3, n4) = nodeGraph(5)

      val R = RelationshipType.withName("R")

      n0.createRelationshipTo(n1, R)
      n1.createRelationshipTo(n2, R)
      n1.createRelationshipTo(n3, R)
      n2.createRelationshipTo(n1, R)
      n3.createRelationshipTo(n4, R)

      n0.addLabel(Label.label("Source"))
      n2.addLabel(Label.label("Target"))
      n4.addLabel(Label.label("Target"))
    }
    // when
    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .filter("y: Target")
      .expand("(x)-[r*]-(y)")
      .nodeByLabelScan("x", "Source", IndexOrderNone)
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .varExpandAsShortest(
        "x",
        "r",
        "y",
        targetLabel = Some("Target"),
        direction = SemanticDirection.BOTH
      )
      .nodeByLabelScan("x", "Source", IndexOrderNone)
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "r", "y")
  }

  test("Spoon Spinner") {

    givenGraph {
      // This test didn't terminate for an implementation which was over propagating.
      //
      //   (n1: Target)
      //        ^
      //        |
      //   (n0: Source)
      //          ^
      //     /     \
      //    |       |   this signifies 5 different loopback relationships from n0 to n0,
      //     \ ___ /
      //       5x

      val Seq(n0, n1) = nodeGraph(2)

      val R = RelationshipType.withName("R")

      n0.createRelationshipTo(n1, R)
      for (_ <- 0 until 5) {
        n0.createRelationshipTo(n0, R)
      }

      n0.addLabel(Label.label("Source"))
      n1.addLabel(Label.label("Target"))

    }
    // when
    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .filter("y: Target")
      .expand("(x)-[r*]->(y)")
      .nodeByLabelScan("x", "Source", IndexOrderNone)
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .varExpandAsShortest(
        "x",
        "r",
        "y",
        targetLabel = Some("Target")
      )
      .nodeByLabelScan("x", "Source", IndexOrderNone)
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "r", "y")
  }

  test("Undirected Spoon Spinner") {

    givenGraph {
      // See previous test

      val Seq(n0, n1) = nodeGraph(2)

      val R = RelationshipType.withName("R")

      n0.createRelationshipTo(n1, R)
      for (_ <- 0 until 5) {
        n0.createRelationshipTo(n0, R)
      }

      n0.addLabel(Label.label("Source"))
      n1.addLabel(Label.label("Target"))

    }
    // when
    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .filter("y: Target")
      .expand("(x)-[r*]-(y)")
      .nodeByLabelScan("x", "Source", IndexOrderNone)
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .varExpandAsShortest(
        "x",
        "r",
        "y",
        targetLabel = Some("Target"),
        direction = SemanticDirection.BOTH
      )
      .nodeByLabelScan("x", "Source", IndexOrderNone)
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "r", "y")
  }

  test("Infinity Spork") {

    givenGraph {
      // Finale of the cutlery testing series. This graph stress tests everything that we tested above by utilizing the
      // utensil of utmost utility - the spork.
      //
      //         (n2)              (n4)
      //        |    ^            |    ^
      //        |    |            |    |
      //        V    |            V    |
      //         (n1) <==========> (n3)
      //        |    ^
      //        |    |
      //        V    |
      //         (n0)
      //       /     ^
      //      V        \
      //    (n5)       (n7)        // No labels! Every node is both a source and a target.
      //      \        ^
      //       V      /
      //         (n6)

      val Seq(n0, n1, n2, n3, n4, n5, n6, n7) = nodeGraph(8)

      val R = RelationshipType.withName("R")

      n0.createRelationshipTo(n1, R)
      n1.createRelationshipTo(n0, R)

      n1.createRelationshipTo(n2, R)
      n2.createRelationshipTo(n1, R)

      n1.createRelationshipTo(n3, R)
      n3.createRelationshipTo(n1, R)

      n3.createRelationshipTo(n4, R)
      n4.createRelationshipTo(n3, R)

      n0.createRelationshipTo(n5, R)
      n5.createRelationshipTo(n6, R)
      n6.createRelationshipTo(n7, R)
      n7.createRelationshipTo(n0, R)
    }
    // when
    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*]->(y)")
      .allNodeScan("x")
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .varExpandAsShortest(
        "x",
        "r",
        "y"
      )
      .allNodeScan("x")
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "r", "y")
  }

  test("Undirected Infinity Spork") {

    givenGraph {
      // See previous test

      val Seq(n0, n1, n2, n3, n4, n5, n6, n7) = nodeGraph(8)

      val R = RelationshipType.withName("R")

      n0.createRelationshipTo(n1, R)
      n1.createRelationshipTo(n0, R)

      n1.createRelationshipTo(n2, R)
      n2.createRelationshipTo(n1, R)

      n1.createRelationshipTo(n3, R)
      n3.createRelationshipTo(n1, R)

      n3.createRelationshipTo(n4, R)
      n4.createRelationshipTo(n3, R)

      n0.createRelationshipTo(n5, R)
      n5.createRelationshipTo(n6, R)
      n6.createRelationshipTo(n7, R)
      n7.createRelationshipTo(n0, R)
    }
    // when
    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*]-(y)")
      .allNodeScan("x")
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .varExpandAsShortest(
        "x",
        "r",
        "y",
        direction = SemanticDirection.BOTH
      )
      .allNodeScan("x")
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "r", "y")
  }

  test("grid graph top left to bottom right directed") {

    // given
    givenGraph {
      gridGraph()
    }

    // when
    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .filter("'4,4' IN labels(y)")
      .expand("(x)-[r*]->(y)")
      .nodeByLabelScan("x", "0,0", IndexOrderNone)
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .varExpandAsShortest(
        "x",
        "r",
        "y",
        targetFilter = Some("'4,4' IN labels(y)")
      )
      .nodeByLabelScan("x", "0,0", IndexOrderNone)
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "r", "y")
  }

  test("grid graph top left to bottom row directed") {

    // given
    givenGraph {
      gridGraph()
    }

    // when
    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .filter(
        "'4,0' IN labels(y) OR '4,1' IN labels(y) OR '4,2' IN labels(y) OR '4,3' IN labels(y) OR '4,4' IN labels(y)"
      )
      .expand("(x)-[r*]->(y)")
      .nodeByLabelScan("x", "0,0", IndexOrderNone)
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .varExpandAsShortest(
        "x",
        "r",
        "y",
        targetFilter = Some(
          "'4,0' IN labels(y) OR '4,1' IN labels(y) OR '4,2' IN labels(y) OR '4,3' IN labels(y) OR '4,4' IN labels(y)"
        )
      )
      .nodeByLabelScan("x", "0,0", IndexOrderNone)
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "r", "y")
  }

  test(
    "grid graph top left to bottom right directed with one extra backward relationship in graph"
  ) {

    // given
    givenGraph {
      val (nodes, _) = gridGraph()
      Seq(
        11 -> 10
      ).map {
        case (i1, i2) =>
          val n1 = nodes(i1)
          val n2 = nodes(i2)
          n1.createRelationshipTo(n2, RelationshipType.withName("BACK"))
      }
    }

    // when
    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .filter("'4,4' IN labels(y)")
      .expand("(x)-[r*]->(y)")
      .nodeByLabelScan("x", "0,0", IndexOrderNone)
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .varExpandAsShortest(
        "x",
        "r",
        "y",
        targetFilter = Some("'4,4' IN labels(y)")
      )
      .nodeByLabelScan("x", "0,0", IndexOrderNone)
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "r", "y")
  }

  test(
    "grid graph top left to bottom row directed with added loop in graph"
  ) {

    // given
    givenGraph {
      val (nodes, _) = gridGraph(3, 3)
      Seq(
        4 -> 8,
        8 -> 4
      ).map {
        case (i1, i2) =>
          val n1 = nodes(i1)
          val n2 = nodes(i2)
          n1.createRelationshipTo(n2, RelationshipType.withName("BACK"))
      }
    }

    // when
    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .filter("'2,0' IN labels(y) OR '2,1' IN labels(y) OR '2,2' IN labels(y)")
      .expand("(x)-[r*]->(y)")
      .nodeByLabelScan("x", "0,0", IndexOrderNone)
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .varExpandAsShortest(
        "x",
        "r",
        "y",
        targetFilter = Some("'2,0' IN labels(y) OR '2,1' IN labels(y) OR '2,2' IN labels(y)")
      )
      .nodeByLabelScan("x", "0,0", IndexOrderNone)
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "r", "y")
  }

  test(
    "Configuration of loopbacks which resulted in a test which failed when all other passed"
  ) {

    // given
    givenGraph {
      val (nodes, _) = gridGraph()
      Seq(
        11 -> 10,
        4 -> 1
      ).map {
        case (i1, i2) =>
          val n1 = nodes(i1)
          val n2 = nodes(i2)
          n1.createRelationshipTo(n2, RelationshipType.withName("BACK"))
      }
    }

    // when
    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .filter(
        "'4,0' IN labels(y) OR '4,1' IN labels(y) OR '4,2' IN labels(y) OR '4,3' IN labels(y) OR '4,4' IN labels(y)"
      )
      .expand("(x)-[r*]->(y)")
      .nodeByLabelScan("x", "0,0", IndexOrderNone)
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .varExpandAsShortest(
        "x",
        "r",
        "y",
        targetFilter = Some(
          "'4,0' IN labels(y) OR '4,1' IN labels(y) OR '4,2' IN labels(y) OR '4,3' IN labels(y) OR '4,4' IN labels(y)"
        )
      )
      .nodeByLabelScan("x", "0,0", IndexOrderNone)
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "r", "y")
  }

  test(
    "Configuration of loopbacks which resulted in a test which failed when all other passed - 2"
  ) {

    // given
    givenGraph {
      val (nodes, _) = gridGraph()

      Seq(
        9 -> 4,
        4 -> 1,
        8 -> 4
      ).map {
        case (i1, i2) =>
          val n1 = nodes(i1)
          val n2 = nodes(i2)
          n1.createRelationshipTo(n2, RelationshipType.withName("BACK"))
      }
    }

    // when
    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .filter(
        "'4,4' IN labels(y)"
      )
      .expand("(x)-[r*]->(y)")
      .nodeByLabelScan("x", "0,0", IndexOrderNone)
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .varExpandAsShortest(
        "x",
        "r",
        "y",
        targetFilter = Some("'4,4' IN labels(y)")
      )
      .nodeByLabelScan("x", "0,0", IndexOrderNone)
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "r", "y")
  }

  test(
    "grid graph top left to bottom row directed with multiple extra relationships in graph"
  ) {

    // given
    givenGraph {
      val (nodes, _) = gridGraph()
      Seq(
        11 -> 10,
        22 -> 21,
        9 -> 4,
        15 -> 15,
        4 -> 1,
        8 -> 4
      ).map {
        case (i1, i2) =>
          val n1 = nodes(i1)
          val n2 = nodes(i2)
          n1.createRelationshipTo(n2, RelationshipType.withName("BACK"))
      }
    }

    // when
    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .filter(
        "'4,0' IN labels(y) OR '4,1' IN labels(y) OR '4,2' IN labels(y) OR '4,3' IN labels(y) OR '4,4' IN labels(y)"
      )
      .expand("(x)-[r*]->(y)")
      .nodeByLabelScan("x", "0,0", IndexOrderNone)
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .varExpandAsShortest(
        "x",
        "r",
        "y",
        targetFilter = Some(
          "'4,0' IN labels(y) OR '4,1' IN labels(y) OR '4,2' IN labels(y) OR '4,3' IN labels(y) OR '4,4' IN labels(y)"
        )
      )
      .nodeByLabelScan("x", "0,0", IndexOrderNone)
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "r", "y")
  }

  // this test causes OOMs in CI but is still useful/relevant
  ignore(
    "double grid graphs"
  ) {

    // given
    givenGraph {
      val (gridNodes1, _) = gridGraph()

      Seq(
        11 -> 10,
        22 -> 21,
        9 -> 4,
        15 -> 15,
        4 -> 1,
        8 -> 4
      ).map {
        case (i1, i2) =>
          val n1 = gridNodes1(i1)
          val n2 = gridNodes1(i2)
          n1.createRelationshipTo(n2, RelationshipType.withName("BACK"))
      }

      val (gridNodes2, _) = gridGraph()

      Seq(
        4 -> 8,
        8 -> 4
      ).map {
        case (i1, i2) =>
          val n1 = gridNodes2(i1)
          val n2 = gridNodes2(i2)
          n1.createRelationshipTo(n2, RelationshipType.withName("BACK"))
      }

      gridNodes1.zip(gridNodes2).foreach {

        case (n1, n2) => n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      }

      gridNodes1.zip(gridNodes2.reverse).foreach {
        case (n1, n2) => n1.createRelationshipTo(n2, RelationshipType.withName("R2"))
      }
    }

    // when
    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .projection("x as x", "size(r) as r", "y as y")
      .filter(
        "'4,0' IN labels(y) OR '4,1' IN labels(y) OR '4,2' IN labels(y) OR '4,3' IN labels(y) OR '4,4' IN labels(y)"
      )
      .expand("(x)-[r*]->(y)")
      .nodeByLabelScan("x", "0,0", IndexOrderNone)
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .projection("x as x", "size(r) as r", "y as y")
      .varExpandAsShortest(
        "x",
        "r",
        "y",
        targetFilter = Some(
          "'4,0' IN labels(y) OR '4,1' IN labels(y) OR '4,2' IN labels(y) OR '4,3' IN labels(y) OR '4,4' IN labels(y)"
        )
      )
      .nodeByLabelScan("x", "0,0", IndexOrderNone)
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "r", "y")
  }

  test("Nested PPBFS with post filter") {

    givenGraph {
      gridGraph(relationshipTypeNameCreationFunction =
        (n1, _) => if (n1._1 == 2 && n1._2 == 2) "SPECIAL" else "ORDINARY"
      )
    }
    // when
    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "rs", "y", "rs2", "z")
      .filter("ANY(r IN rs2 WHERE TYPE(r) = 'SPECIAL') AND NONE(r IN rs2 WHERE r IN rs)")
      .expand("(y)<-[rs2*]-(z)", projectedDir = SemanticDirection.INCOMING)
      .filter("'3,3' IN labels(y)")
      .expand("(x)-[rs*]->(y)")
      .nodeByLabelScan("x", "0,0", IndexOrderNone)
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "rs", "y", "rs2", "z")
      .varExpandAsShortest(
        "y",
        "rs2",
        "z",
        nonInlinablePrefilters = Some("ANY(r IN rs2 WHERE TYPE(r) = 'SPECIAL') AND NONE(r IN rs2 WHERE r IN rs)"),
        direction = SemanticDirection.INCOMING
      )
      .varExpandAsShortest(
        "x",
        "rs",
        "y",
        targetFilter = Some("'3,3' IN labels(y)")
      )
      .nodeByLabelScan("x", "0,0", IndexOrderNone)
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "rs", "y", "rs2", "z")
  }

  test("grid graph with inbound direction") {
    givenGraph {
      gridGraph(nRows = 3, nCols = 3)
    }
    // when
    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "rs", "y")
      .expand("(x)<-[rs*]-(y)", projectedDir = SemanticDirection.INCOMING)
      .nodeByLabelScan("x", "1,1", IndexOrderNone)
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "rs", "y")
      .varExpandAsShortest(
        "x",
        "rs",
        "y",
        direction = SemanticDirection.INCOMING
      )
      .nodeByLabelScan("x", "1,1", IndexOrderNone)
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "rs", "y")
  }

  test("grid graph all loops including 0 length") {

    givenGraph {
      val (ns, _) = gridGraph()
      ns.last.createRelationshipTo(ns.head, RelationshipType.withName("LOOP"))
    }
    // when
    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .filter("y = x")
      .expand("(x)-[r*0..]->(y)")
      .allNodeScan("x")
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .varExpandAsShortest(
        "x",
        "r",
        "y",
        targetFilter = Some("y = x"),
        includeZeroLength = true
      )
      .allNodeScan("x")
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "r", "y")
  }

  test("grid graph all loops of length 6 or longer") {

    givenGraph {
      val (ns, _) = gridGraph()
      ns.last.createRelationshipTo(ns.head, RelationshipType.withName("LOOP"))
    }
    // when
    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .filter("y = x")
      .expand("(x)-[r*6..]->(y)")
      .allNodeScan("x")
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .filter("size(r) >= 6")
      .varExpandAsShortest(
        "x",
        "r",
        "y",
        targetFilter = Some("x = y")
      )
      .allNodeScan("x")
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "r", "y")
  }

  test("propagated steps are not always seen at deeper depths") {
    givenGraph {

      // Test which was used to justify the removal of an assertion. Consider the graph
      //
      //     ,->(n5)->(n6)->(n7)-.
      //    /                    V
      // (n0: S)----->(n1)----->(n2)---->(n3)---->(n4: T)
      //     \         ^
      //      `->(n8)-´
      //
      // Steps will be registered with (n2) in the following order
      //  * first (n1)--> at lengthFromSource 2 found by BFS as shortest path,
      //  * then (n2)--> at lengthFromSource 4 found by bfs as non-shortest path,
      //  * then (n1)--> at lengthFromSource 3 by propagation when we're propagating to total length 5
      //
      // Notably, lengthFromSource isn't monotonically increasing when we're dealing with propagated steps
      // (which was something we tried asserting on)

      val Seq(n0, n1, n2, n3, n4, n5, n6, n7, n8) = nodeGraph(9)
      n0.addLabel(Label.label("S"))
      n4.addLabel(Label.label("T"))

      val R = RelationshipType.withName("R")

      n0.createRelationshipTo(n1, R)
      n1.createRelationshipTo(n2, R)
      n2.createRelationshipTo(n3, R)
      n3.createRelationshipTo(n4, R)

      n0.createRelationshipTo(n8, R)
      n8.createRelationshipTo(n1, R)

      n0.createRelationshipTo(n5, R)
      n5.createRelationshipTo(n6, R)
      n6.createRelationshipTo(n7, R)
      n7.createRelationshipTo(n2, R)
    }

    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .filter("y:T")
      .expand("(x)-[r*]->(y)")
      .nodeByLabelScan("x", "S")
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .varExpandAsShortest(
        "x",
        "r",
        "y",
        targetLabel = Some("T")
      )
      .nodeByLabelScan("x", "S")
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "r", "y")
  }

  test("grid graph 2x2 undirected top-left to bottom-right with loop") {

    // This test is very important! It challenges an assumption which was true in the data graph
    // but not in the product graph. The data graph in this test is given as
    //
    //   (n0:S)----[r0]--->(n1)
    //      |  ^            |
    //      |    \          |
    //     [r1]    [r4]    [r3]
    //      |          \    |
    //      V            \  V
    //     (n2)----[r2]--->(n3: T)
    //
    // and the pattern is (s: S) ((x1)--(x2))* (t: T)
    //
    // The problems are the paths (where we notate a traversed node juxtaposition (n, s0)-->(n, s1) as (n, s0->s1) )
    //
    //  (n0, s->x1)-[r0]->(n1, x2->x1)-[r2]->(n3, x2->x1)-[r4]->(n0, x2->x1)-[r1]->(n2, x2->x1)-[r3]->(n3, x2->t)
    //  (n0, s->x1)-[r1]->(n2, x2->x1)-[r3]->(n3, x2->x1)-[r4]->(n0, x2->x1)-[r0]->(n1, x2->x1)-[r2]->(n3, x2->t)
    //
    // These are the two shortest paths from (n0, s) to (n3, t) which includes the product graph relationship
    // (n3, x1)-[r4]->(n0, x2) and project down to trails in the data graph. However, the shortest path which
    // originates from (n0, s) and includes the product graph relationship (n3, x1)-[r4]->(n0, x2) is
    //
    //  (n0, s->x1)-[r4]->(n3, x2->x1)-[r4]->(n0, x2),
    //
    // and the shortest path which ends with (n3, t) and contains (n3, x1)-[r4]->(n0, x2) is
    //
    //  (n3, x1)-[r4]->(n0, x2->x1)-[r4]->(n3, x2->t)
    //
    // Neither of these paths are trails This means that the first time we trace (n3, x1)-[r4]->(n0, x2),
    // we won't register any corresponding reverse step with (n3, x1), and we will prune away the corresponding
    // forward step registered at (n0, x2). This means that we effectively purge the relationship from our bookkeeping
    // data, and are never able to trace the paths
    //
    //  (n0, s->x1)-[r0]->(n1, x2->x1)-[r2]->(n3, x2->x1)-[r4]->(n0, x2->x1)-[r1]->(n2, x2->x1)-[r3]->(n3, x2->t)
    //  (n0, s->x1)-[r1]->(n2, x2->x1)-[r3]->(n3, x2->x1)-[r4]->(n0, x2->x1)-[r0]->(n1, x2->x1)-[r2]->(n3, x2->t)
    //
    // To remedy this issue, we must register reverse path trace steps not only when the path to the target is a trail
    // in the data graph, but also when the path to the target is a trail in the product graph (which is a more relaxed
    // condition). Indeed, the path
    //
    //  (n3, x1)-[r4]->(n0, x2->x1)-[r4]->(n3, x2->t)
    //
    // is a trail in the product graph, which means that we, in this test, register a reverse step with (n3, x1),
    // which in turn allows us to propagate along that edge when we later find non shortest paths to (n3, x1) that
    // don't contain any relationships which project to the data graph relationship -[r4]->.

    givenGraph {
      val (ns, _) = gridGraph(2, 2)
      ns.last.createRelationshipTo(ns.head, RelationshipType.withName("Hypotenuse"))
    }
    val targetLabel = "`1,1`"
    // when
    val expandQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .filter("y: " + targetLabel)
      .expand("(x)-[r*]-(y)")
      .nodeByLabelScan("x", "0,0")
      .build()

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .varExpandAsShortest(
        "x",
        "r",
        "y",
        direction = SemanticDirection.BOTH,
        targetLabel = Some(targetLabel)
      )
      .nodeByLabelScan("x", "0,0")
      .build()

    assertSameResult(pgShortestQuery, expandQuery, "x", "r", "y")
  }

  // ignored because it blows up until we fix it (need to purge target signposts or plan 1:1)
  // see https://trello.com/c/0wthD9Mh/5030-updating-target-singposts-upon-target-count-saturation
  // or https://trello.com/c/LCKNu5CJ/5027-adapt-pgpathpropagatingbfs-to-support-one-to-one-flag
  ignore("one to one vs one to many discrepancy blow up") {

    givenGraph {

      val (grid, _) = gridGraph(10, 10) // 2 * 9 * 9 rels, so longest path is shorter than 200
      val (line, _) = lineGraph(300, "R")
      grid.head.createRelationshipTo(line.head, RelationshipType.withName("R"))

      // Loop to target to test purging rev steps when there are loops
      grid.last.createRelationshipTo(grid.last, RelationshipType.withName("R"))
      grid.last.createRelationshipTo(grid.last, RelationshipType.withName("R"))
      grid.last.createRelationshipTo(grid.last, RelationshipType.withName("R"))

      grid.head.addLabel(Label.label("S"))
      grid.last.addLabel(Label.label("T"))
      line.last.addLabel(Label.label("T"))

    }
    val targetLabel = "T"

    val pgShortestQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .varExpandAsShortest(
        "x",
        "r",
        "y",
        direction = SemanticDirection.BOTH,
        targetLabel = Some(targetLabel),
        selector = StatefulShortestPath.Selector.Shortest(1)
      )
      .nodeByLabelScan("x", "S")
      .build()

    val res = consume(execute(pgShortestQuery, runtime, NO_INPUT)) // Don't hang
    println(res.size)
  }

  def assertSameResult(query1: LogicalQuery, query2: LogicalQuery, columns: String*): Unit = {
    val debug = false // is there an env var or something which we can use instead?

    def printValue(value: AnyValue): String = {
      value match {
        case n: NodeValue         => "(" + n.id() + ")"
        case r: RelationshipValue => "-[" + r.id() + "]-"
        case l: ListValue         => l.asArray().map(printValue).mkString("{", ",", "}")
        case x                    => x.toString
      }
    }

    val started1 = Instant.now().toEpochMilli
    val res1 = execute(query1, runtime, NO_INPUT)
    var rows1: List[Array[AnyValue]] = null
    if (debug) {
      rows1 = consume(res1).toList
      val time = Instant.now().toEpochMilli - started1
      println(s"${time}ms: Finished executing query1")

      rows1.foreach { row =>
        row.foreach(value => print(printValue(value)))
        println()
      }
    }

    val started2 = Instant.now().toEpochMilli
    val res2 = execute(query2, runtime, NO_INPUT)
    val rows2 = consume(res2).toList

    if (debug) {
      val time = Instant.now().toEpochMilli - started2
      println(s"${time}ms: Finished executing query2")

      rows2.foreach { row =>
        row.foreach(value => print(printValue(value)))
        println()
      }

      println("======= in rows 1 but not rows 2 ========")
      diff(rows1, rows2).foreach {
        row =>
          {
            val x = row(0)
            val rs = row(1)
            printPath(x.asInstanceOf[NodeValue], rs.asInstanceOf[ListValue])
          }
      }
      println("======= in rows 2 but not rows 1 ========")
      diff(rows2, rows1).foreach {
        row =>
          {
            val x = row(0)
            val rs = row(1)
            printPath(x.asInstanceOf[NodeValue], rs.asInstanceOf[ListValue])
          }
      }
      println(s"n rows: ${rows2.length}")
    }

    res1 should beColumns(columns: _*).withRows(rows2)
  }

  def printPath(source: NodeValue, rs: ListValue): Unit = {
    def printRelTail(prevNodeId: Long, r: RelationshipValue): Long = {
      if (r.startNode().id() == prevNodeId) {
        print(s"-[${r.id()}]->(${r.endNode().id()})")
        r.endNode().id()
      } else {
        print(s"<-[${r.id()}]-(${r.startNode().id()})")
        r.startNode().id()
      }
    }

    print(s"(${source.id()})")
    var prevId = source.id()
    for (r <- rs.asScala) {
      prevId = printRelTail(prevId, r.asInstanceOf[RelationshipValue])
    }
    println()
  }

  def diff(rows1: List[Array[AnyValue]], rows2: List[Array[AnyValue]]): List[Array[AnyValue]] = {
    val nodes2 = rows2.map(r => r(1)).toSet

    rows1.filterNot(row => nodes2(row(1)))
  }

  implicit class PlanBuilderExt(planBuilder: LogicalQueryBuilder) {

    def varExpandAsShortest(
      sourceName: String,
      relName: String,
      targetName: String,
      targetFilter: Option[String] = None,
      targetLabel: Option[String] = None,
      nonInlinablePrefilters: Option[String] = None,
      direction: SemanticDirection = SemanticDirection.OUTGOING,
      includeZeroLength: Boolean = false,
      selector: StatefulShortestPath.Selector = StatefulShortestPath.Selector.Shortest(Int.MaxValue)
    ): LogicalQueryBuilder = {
      val targetLabelString = targetLabel.map(": " + _)
      // TODO FIXME: this is a dirty hack to make predicates work on inner (expression) variables
      val targetFilterString = targetFilter.map(" WHERE " + _.replace(targetName, s"${targetName}_inner"))
      val targetPredicateString = targetLabelString.getOrElse("") ++ targetFilterString.getOrElse("")
      val firstAnon = "__anon1"
      val secondAnon = "__anon2"

      val relPattern = direction match {
        case SemanticDirection.OUTGOING => s"-[${relName}_inner]->"
        case SemanticDirection.INCOMING => s"<-[${relName}_inner]-"
        case SemanticDirection.BOTH     => s"-[${relName}_inner]-"
      }

      val builder = new TestNFABuilder(0, s"$sourceName")
        .addTransition(0, 1, s"($sourceName) (${firstAnon}_inner)")
        .addTransition(1, 2, s"(${firstAnon}_inner)$relPattern(${secondAnon}_inner)")
        .addTransition(2, 1, s"(${secondAnon}_inner) (${firstAnon}_inner)")
        .addTransition(2, 3, s"(${secondAnon}_inner) (${targetName}_inner $targetPredicateString)")
        .setFinalState(3)
      if (includeZeroLength) {
        builder.addTransition(0, 3, s"($sourceName) (${targetName}_inner $targetPredicateString)")
      }
      val nfa = builder.build()

      planBuilder.statefulShortestPath(
        sourceName,
        targetName,
        "",
        nonInlinablePrefilters,
        Set(s"${firstAnon}_inner" -> firstAnon, s"${secondAnon}_inner" -> secondAnon),
        Set(s"${relName}_inner" -> relName),
        Set(s"${targetName}_inner" -> targetName),
        Set(),
        selector,
        nfa,
        ExpandAll,
        false
      )
    }
  }

}
