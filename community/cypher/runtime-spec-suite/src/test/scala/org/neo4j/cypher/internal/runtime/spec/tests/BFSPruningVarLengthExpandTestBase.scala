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
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint.Endpoint
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType

abstract class BFSPruningVarLengthExpandTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("var-length-expand with no relationships") {
    // given
    givenGraph { nodeGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .bfsPruningVarExpand("(x)-[*1..2]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withNoRows()
  }

  test("undirected var-length-expand with no relationships") {
    // given
    givenGraph { nodeGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .bfsPruningVarExpand("(x)-[*1..2]-(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withNoRows()
  }

  test("var-length-expand with max length") {
    // given
    val (Seq(_, n2, _), _) = givenGraph { lollipopGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[*..1]->(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array(Array(n2, 1))

    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("undirected var-length-expand with max length") {
    // given
    val (Seq(_, n2, _), _) = givenGraph { lollipopGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[*..1]-(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array(Array(n2, 1))

    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("var-length-expand with max length including start node") {
    // given
    val (Seq(n1, n2, _), _) = givenGraph { lollipopGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[*0..1]->(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      Array(
        Array(n1, 0),
        Array(n2, 1)
      )

    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("undirected var-length-expand with max length including start node") {
    // given
    val (Seq(n1, n2, _), _) = givenGraph { lollipopGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[*0..1]-(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      Array(
        Array(n1, 0),
        Array(n2, 1)
      )

    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("var-length-expand with min and max length") {
    // given
    val paths = givenGraph { chainGraphs(3, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[*1..4]->(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        path <- paths
        length <- 1 to 4
      } yield {
        val pathPrefix = path.take(length)
        Array[Any](pathPrefix.endNode(), length)
      }

    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("undirected var-length-expand with min and max length") {
    // given
    val paths = givenGraph { chainGraphs(3, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[*1..4]-(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        path <- paths
        length <- 1 to 4
      } yield {
        val pathPrefix = path.take(length)
        Array[Any](pathPrefix.endNode(), length)
      }

    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("var-length-expand with length 0") {
    // given
    val (Seq(n1, _, _), _) = givenGraph { lollipopGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[*0]->(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array(Array(n1, 0))
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("undirected var-length-expand with length 0") {
    // given
    val (Seq(n1, _, _), _) = givenGraph { lollipopGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[*0]-(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array(Array(n1, 0))
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("var-length-expand with length 0..2") {
    // given
    val (Seq(n1, n2, n3), _) = givenGraph { lollipopGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[*0..2]->(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array(
      Array(n1, 0),
      Array(n2, 1),
      Array(n3, 2)
    )
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("undirected var-length-expand with length 0..2") {
    // given
    val (Seq(n1, n2, n3), _) = givenGraph { lollipopGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[*0..2]-(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array(
      Array(n1, 0),
      Array(n2, 1),
      Array(n3, 2)
    )
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("var-length-expand with self-loop") {
    // given
    val (n1, n2) = givenGraph {
      val n1 = tx.createNode(Label.label("START"))
      val n2 = tx.createNode()
      val relType = RelationshipType.withName("R")
      n1.createRelationshipTo(n2, relType)
      n1.createRelationshipTo(n1, relType)
      (n1, n2)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[*..2]->(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array(
      Array(n1, 1),
      Array(n2, 1)
    )
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("undirected var-length-expand with self-loop") {
    // given
    val (n1, n2) = givenGraph {
      val n1 = tx.createNode(Label.label("START"))
      val n2 = tx.createNode()
      val relType = RelationshipType.withName("R")
      n1.createRelationshipTo(n2, relType)
      n1.createRelationshipTo(n1, relType)
      (n1, n2)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[*..2]-(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array(
      Array(n1, 1),
      Array(n2, 1)
    )
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("fixed length with shortcut") {
    /*
    n1 - ---- - n2 - n3 - n4
       - n1_2 -

    Even though n1-n2 is traversed first, the length 4 path over n1_2 should be found
     */

    // given
    val (n1, n1_2, n2, n3, n4) = givenGraph {
      val n1 = tx.createNode(Label.label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val n4 = tx.createNode()
      val n1_2 = tx.createNode()
      val relType = RelationshipType.withName("R")

      n1.createRelationshipTo(n1_2, relType)
      n1_2.createRelationshipTo(n2, relType)

      n1.createRelationshipTo(n2, relType)
      n2.createRelationshipTo(n3, relType)
      n3.createRelationshipTo(n4, relType)

      (n1, n1_2, n2, n3, n4)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[*..4]->(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array(
      Array(n1_2, 1),
      Array(n2, 1),
      Array(n3, 2),
      Array(n4, 3)
    )
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("fixed length with shortcut, undirected") {
    /*
    n1 - ---- - n2 - n3 - n4
       - n1_2 -

    Even though n1-n2 is traversed first, the length 4 path over n1_2 should be found
     */

    // given
    val (n1, n1_2, n2, n3, n4) = givenGraph {
      val n1 = tx.createNode(Label.label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val n4 = tx.createNode()
      val n1_2 = tx.createNode()
      val relType = RelationshipType.withName("R")

      n1.createRelationshipTo(n1_2, relType)
      n1_2.createRelationshipTo(n2, relType)

      n1.createRelationshipTo(n2, relType)
      n2.createRelationshipTo(n3, relType)
      n3.createRelationshipTo(n4, relType)

      (n1, n1_2, n2, n3, n4)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[*..4]-(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array(
      Array(n1_2, 1),
      Array(n2, 1),
      Array(n3, 2),
      Array(n1, 3),
      Array(n4, 3)
    )
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("fixed length with longer shortcut") {
    /*
    n1 - ----- - ----- - n2 - n3 - n4
       - n1_2a - n1_2b -

    Even though n1-n2 is traversed first, the length 4 path over n1_2 should be found
     */

    // given
    val (n1, n1_2a, n1_2b, n2, n3, n4) = givenGraph {
      val n1 = tx.createNode(Label.label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val n4 = tx.createNode()
      val n1_2a = tx.createNode()
      val n1_2b = tx.createNode()
      val relType = RelationshipType.withName("R")

      n1.createRelationshipTo(n1_2a, relType)
      n1_2a.createRelationshipTo(n1_2b, relType)
      n1_2b.createRelationshipTo(n2, relType)

      n1.createRelationshipTo(n2, relType)
      n2.createRelationshipTo(n3, relType)
      n3.createRelationshipTo(n4, relType)

      (n1, n1_2a, n1_2b, n2, n3, n4)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[*..5]->(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array(
      Array(n1_2a, 1),
      Array(n1_2b, 2),
      Array(n2, 1),
      Array(n3, 2),
      Array(n4, 3)
    )
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("fixed length with longer shortcut, undirected") {
    /*
    n1 - ----- - ----- - n2 - n3 - n4
       - n1_2a - n1_2b -

    Even though n1-n2 is traversed first, the length 4 path over n1_2 should be found
     */

    // given
    val (n1, n1_2a, n1_2b, n2, n3, n4) = givenGraph {
      val n1 = tx.createNode(Label.label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val n4 = tx.createNode()
      val n1_2a = tx.createNode()
      val n1_2b = tx.createNode()
      val relType = RelationshipType.withName("R")

      n1.createRelationshipTo(n1_2a, relType)
      n1_2a.createRelationshipTo(n1_2b, relType)
      n1_2b.createRelationshipTo(n2, relType)

      n1.createRelationshipTo(n2, relType)
      n2.createRelationshipTo(n3, relType)
      n3.createRelationshipTo(n4, relType)

      (n1, n1_2a, n1_2b, n2, n3, n4)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[*..5]-(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array(
      Array(n1_2a, 1),
      Array(n1_2b, 2),
      Array(n2, 1),
      Array(n3, 2),
      Array(n1, 4),
      Array(n4, 3)
    )
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("two ways to get to the same node - one inside and one outside the max") {
    /*
    (x)-[1]->()-[2]->()-[3]->()-[4]->(y)-[4]->(z)
      \                              ^
       \----------------------------/

     */

    // given
    val (n1, n2, n3, y, z) = givenGraph {
      val x = tx.createNode(Label.label("START"))
      val n1 = tx.createNode()
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val y = tx.createNode()
      val z = tx.createNode()
      val relType = RelationshipType.withName("R")

      x.createRelationshipTo(n1, relType)
      n1.createRelationshipTo(n2, relType)
      n2.createRelationshipTo(n3, relType)
      n3.createRelationshipTo(y, relType)
      y.createRelationshipTo(z, relType)

      x.createRelationshipTo(y, relType)

      (n1, n2, n3, y, z)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[*1..4]->(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array(
      Array(n1, 1),
      Array(n2, 2),
      Array(n3, 3),
      Array(y, 1),
      Array(z, 2)
    )
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("two ways to get to the same node - one inside and one outside the max, undirected") {
    /*
    (x)-[1]->()-[2]->()-[3]->()-[4]->(y)-[4]->(z)
      \                              ^
       \----------------------------/

     */

    // given
    val (n1, n2, n3, y, z) = givenGraph {
      val x = tx.createNode(Label.label("START"))
      val n1 = tx.createNode()
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val y = tx.createNode()
      val z = tx.createNode()
      val relType = RelationshipType.withName("R")

      x.createRelationshipTo(n1, relType)
      n1.createRelationshipTo(n2, relType)
      n2.createRelationshipTo(n3, relType)
      n3.createRelationshipTo(y, relType)
      y.createRelationshipTo(z, relType)

      x.createRelationshipTo(y, relType)

      (n1, n2, n3, y, z)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[*1..4]-(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array(
      Array(n1, 1),
      Array(n2, 2),
      Array(n3, 2),
      Array(y, 1),
      Array(z, 2)
    )
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  // NULL INPUT

  test("should handle null from-node") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .bfsPruningVarExpand("(x)-[*..2]->(y)")
      .input(nodes = Seq("x"))
      .build()

    val input = inputValues(Array(Array[Any](null)): _*)
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("y").withNoRows()
  }

  test("should handle null from-node, undirected") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .bfsPruningVarExpand("(x)-[*..2]-(y)")
      .input(nodes = Seq("x"))
      .build()

    val input = inputValues(Array(Array[Any](null)): _*)
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("y").withNoRows()
  }

  // EXPANSION FILTERING, DIRECTION

  test("should filter on outgoing direction") {
    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[*1..2]->(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y", "depth").withRows(Array(
      Array(g.sb1, 1),
      Array(g.sa1, 1),
      Array(g.middle, 1),
      Array(g.sb2, 2),
      Array(g.sc3, 2),
      Array(g.ea1, 2),
      Array(g.eb1, 2),
      Array(g.ec1, 2)
    ))
  }

  test("should filter on incoming direction") {
    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)<-[*1..2]-(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y", "depth").withRows(Array(
      Array(g.sc1, 1),
      Array(g.sc2, 2)
    ))
  }

  test("should expand on both direction") {
    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[*1..2]-(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y", "depth").withRows(Array(
      Array(g.sb1, 1), // outgoing only
      Array(g.sa1, 1),
      Array(g.middle, 1),
      Array(g.sb2, 2),
      Array(g.sc3, 2),
      Array(g.ea1, 2),
      Array(g.eb1, 2),
      Array(g.ec1, 2),
      Array(g.sc1, 1), // incoming only
      Array(g.sc2, 2),
      Array(g.end, 2)
    ))
  }

  // EXPANSION FILTERING, RELATIONSHIP TYPE

  test("should filter on relationship type A") {
    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[:A*1..2]->(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y", "depth").withRows(Array(
      Array(g.sa1, 1),
      Array(g.middle, 1),
      Array(g.sc3, 2),
      Array(g.ea1, 2),
      Array(g.ec1, 2)
    ))
  }

  test("should filter on relationship type B") {
    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[:B*1..2]->(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y", "depth").withRows(Array(
      Array(g.sb1, 1),
      Array(g.sb2, 2)
    ))
  }

  test("should filter on relationship type A, undirected") {
    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[:A*1..2]-(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y", "depth").withRows(Array(
      Array(g.sa1, 1),
      Array(g.sc1, 1),
      Array(g.middle, 1),
      Array(g.end, 2),
      Array(g.sc2, 2),
      Array(g.sc3, 2),
      Array(g.ea1, 2),
      Array(g.ec1, 2)
    ))
  }

  test("should filter on relationship type B, undirected") {
    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand("(x)-[:B*1..2]-(y)", depthName = Some("depth"))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y", "depth").withRows(Array(
      Array(g.sb1, 1),
      Array(g.sb2, 2)
    ))
  }

  // EXPANSION FILTERING, NODE AND RELATIONSHIP PREDICATE

  test("should filter on node predicate") {

    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand(
        "(x)-[*1..2]->(y)",
        nodePredicates = Seq(Predicate("n", "id(n) <> " + g.middle.getId)),
        depthName = Some("depth")
      )
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y", "depth").withRows(Array(
      Array(g.sa1, 1),
      Array(g.sb1, 1),
      Array(g.sb2, 2)
    ))
  }

  test("should filter on two node predicates") {
    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand(
        "(x)-[*1..3]-(y)",
        nodePredicates = Seq(
          Predicate("n", "id(n) <> " + g.middle.getId),
          Predicate("n2", "id(n2) <> " + g.sc3.getId)
        ),
        depthName = Some("depth")
      )
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y", "depth").withRows(Array(
      Array(g.sa1, 1),
      Array(g.sb1, 1),
      Array(g.sb2, 2),
      Array(g.sc1, 1),
      Array(g.sc2, 2)
    ))
  }

  test("should filter on node predicate on first node") {

    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand(
        "(x)-[*1..2]->(y)",
        nodePredicates = Seq(Predicate("n", "id(n) <> " + g.start.getId)),
        depthName = Some("depth")
      )
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y", "depth").withNoRows()
  }

  test("should filter on node predicate on first node, undirected") {

    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand(
        "(x)-[*1..2]-(y)",
        nodePredicates = Seq(Predicate("n", "id(n) <> " + g.start.getId)),
        depthName = Some("depth")
      )
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y", "depth").withNoRows()
  }

  test("should filter on node predicate on first node from reference") {

    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand(
        "(X)-[*1..2]->(y)",
        nodePredicates = Seq(Predicate("n", "id(n) <> " + g.start.getId)),
        depthName = Some("depth")
      )
      .projection("x AS X")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y", "depth").withNoRows()
  }

  test("should filter on node predicate on first node from reference, undirected") {

    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand(
        "(X)-[*1..2]-(y)",
        nodePredicates = Seq(Predicate("n", "id(n) <> " + g.start.getId)),
        depthName = Some("depth")
      )
      .projection("x AS X")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y", "depth").withNoRows()
  }

  test("should filter on relationship predicate") {
    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand(
        "(x)-[*1..2]->(y)",
        relationshipPredicates = Seq(Predicate("r", "id(r) <> " + g.startMiddle.getId)),
        depthName = Some("depth")
      )
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y", "depth").withRows(Array(
      Array(g.sa1, 1),
      Array(g.middle, 2),
      Array(g.sb1, 1),
      Array(g.sb2, 2)
    ))
  }

  test("should filter on two relationship predicates") {
    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand(
        "(x)-[*1..3]-(y)",
        relationshipPredicates = Seq(
          Predicate("r", "id(r) <> " + g.startMiddle.getId),
          Predicate("r2", "id(r2) <> " + g.endMiddle.getId)
        ),
        depthName = Some("depth")
      )
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y", "depth").withRows(Array(
      Array(g.sa1, 1),
      Array(g.sb1, 1),
      Array(g.sc1, 1),
      Array(g.middle, 2),
      Array(g.sb2, 2),
      Array(g.sc2, 2),
      Array(g.ea1, 3),
      Array(g.eb1, 3),
      Array(g.ec1, 3),
      Array(g.sc3, 3)
    ))
  }

  test("should filter on relationship predicate, undirected") {
    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand(
        "(x)-[*1..2]-(y)",
        relationshipPredicates = Seq(Predicate("r", "id(r) <> " + g.startMiddle.getId)),
        depthName = Some("depth")
      )
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y", "depth").withRows(Array(
      Array(g.sa1, 1),
      Array(g.middle, 2),
      Array(g.sb1, 1),
      Array(g.sc1, 1),
      Array(g.sb2, 2),
      Array(g.sc2, 2)
    ))
  }

  test("should filter on node and relationship predicate") {

    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand(
        "(x)-[*..2]->(y)",
        nodePredicates = Seq(Predicate("n", "id(n) <> " + g.sa1.getId)),
        relationshipPredicates = Seq(Predicate("r", "id(r) <> " + g.startMiddle.getId)),
        depthName = Some("depth")
      )
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y", "depth").withRows(Array(
      Array(g.sb1, 1),
      Array(g.sb2, 2)
    ))
  }

  test("should filter on node and relationship predicate, undirected") {

    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand(
        "(x)-[*..2]-(y)",
        nodePredicates = Seq(Predicate("n", "id(n) <> " + g.sa1.getId)),
        relationshipPredicates = Seq(Predicate("r", "id(r) <> " + g.startMiddle.getId)),
        depthName = Some("depth")
      )
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y", "depth").withRows(Array(
      Array(g.sb1, 1),
      Array(g.sc1, 1),
      Array(g.sb2, 2),
      Array(g.sc2, 2)
    ))
  }

  test("should handle predicate accessing start node") {

    // given
    val n = closestMultipleOf(10, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand(
        "(x)-[*..5]->(y)",
        nodePredicates = Seq(Predicate("n", "'START' IN labels(x)")),
        depthName = Some("depth")
      )
      .input(nodes = Seq("x"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 1 to 5
      } yield Array[Any](path.take(length).endNode(), length)
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("should handle predicate accessing start node, undirected") {

    // given
    val n = closestMultipleOf(10, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand(
        "(x)-[*..5]-(y)",
        nodePredicates = Seq(Predicate("n", "'START' IN labels(x)")),
        depthName = Some("depth")
      )
      .input(nodes = Seq("x"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 1 to 5
      } yield Array[Any](path.take(length).endNode(), length)
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("should handle predicate accessing start node including start node") {

    // given
    val n = closestMultipleOf(10, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand(
        "(x)-[*0..5]->(y)",
        nodePredicates = Seq(Predicate("n", "'START' IN labels(x)")),
        depthName = Some("depth")
      )
      .input(nodes = Seq("x"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 0 to 5
      } yield Array[Any](path.take(length).endNode(), length)
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("should handle predicate accessing start node including start node, undirected") {

    // given
    val n = closestMultipleOf(10, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand(
        "(x)-[*0..5]-(y)",
        nodePredicates = Seq(Predicate("n", "'START' IN labels(x)")),
        depthName = Some("depth")
      )
      .input(nodes = Seq("x"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 0 to 5
      } yield Array[Any](path.take(length).endNode(), length)
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("should handle predicate accessing reference in context") {

    // given
    val n = closestMultipleOf(10, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand(
        "(x)-[*..5]->(y)",
        nodePredicates = Seq(Predicate("n", "id(n) >= zero")),
        depthName = Some("depth")
      )
      .projection("0 AS zero")
      .input(nodes = Seq("x"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 1 to 5
      } yield Array[Any](path.take(length).endNode(), length)
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("should handle predicate accessing reference in context, undirected") {

    // given
    val n = closestMultipleOf(10, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand(
        "(x)-[*..5]-(y)",
        nodePredicates = Seq(Predicate("n", "id(n) >= zero")),
        depthName = Some("depth")
      )
      .projection("0 AS zero")
      .input(nodes = Seq("x"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 1 to 5
      } yield Array[Any](path.take(length).endNode(), length)
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("should handle predicate accessing reference in context and including start node") {

    // given
    val n = closestMultipleOf(10, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand(
        "(x)-[*0..5]->(y)",
        nodePredicates = Seq(Predicate("n", "id(n) >= zero")),
        depthName = Some("depth")
      )
      .projection("0 AS zero")
      .input(nodes = Seq("x"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 0 to 5
      } yield Array[Any](path.take(length).endNode(), length)
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("should handle predicate accessing reference in context and including start node, undirected") {

    // given
    val n = closestMultipleOf(10, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand(
        "(x)-[*0..5]-(y)",
        nodePredicates = Seq(Predicate("n", "id(n) >= zero")),
        depthName = Some("depth")
      )
      .projection("0 AS zero")
      .input(nodes = Seq("x"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 0 to 5
      } yield Array[Any](path.take(length).endNode(), length)
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("should handle predicate accessing node in context") {

    // given
    val n = closestMultipleOf(10, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand(
        "(x)-[*..5]->(y)",
        nodePredicates = Seq(Predicate("n", "id(other) >= 0")),
        depthName = Some("depth")
      )
      .projection("0 AS zero")
      .input(nodes = Seq("x", "other"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 1 to 5
      } yield Array[Any](path.take(length).endNode(), length)
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("should handle predicate accessing node in context, undirected") {

    // given
    val n = closestMultipleOf(10, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand(
        "(x)-[*..5]-(y)",
        nodePredicates = Seq(Predicate("n", "id(other) >= 0")),
        depthName = Some("depth")
      )
      .projection("0 AS zero")
      .input(nodes = Seq("x", "other"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 1 to 5
      } yield Array[Any](path.take(length).endNode(), length)
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("should handle predicate accessing node in context and including start node") {

    // given
    val n = closestMultipleOf(10, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand(
        "(x)-[*0..5]->(y)",
        nodePredicates = Seq(Predicate("n", "id(other) >= 0")),
        depthName = Some("depth")
      )
      .projection("0 AS zero")
      .input(nodes = Seq("x", "other"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 0 to 5
      } yield Array[Any](path.take(length).endNode(), length)
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("should handle predicate accessing node in context and including start node, undirected") {

    // given
    val n = closestMultipleOf(10, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "depth")
      .distinct("y AS y", "depth AS depth")
      .bfsPruningVarExpand(
        "(x)-[*0..5]-(y)",
        nodePredicates = Seq(Predicate("n", "id(other) >= 0")),
        depthName = Some("depth")
      )
      .projection("0 AS zero")
      .input(nodes = Seq("x", "other"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 0 to 5
      } yield Array[Any](path.take(length).endNode(), length)
    runtimeResult should beColumns("y", "depth").withRows(expected)
  }

  test("var-length-expand should only find start node once") {
    // given
    val nodes = givenGraph {
      val (nodes, _) = circleGraph(10)
      nodes.head.addLabel(Label.label("START"))
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      // currently we would plan a distinct here because different x may lead to the same y
      // so we cannot guarantee global uniqueness of y. However we still want bfsPruningVarExpand
      // to produce unique ys given an x which is what we test here
      .bfsPruningVarExpand("(x)-[*0..25]->(y)")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(singleColumn(nodes))
  }

  test("var-length-expand should only find start node once, undirected") {
    // given
    val nodes = givenGraph {
      val (nodes, _) = circleGraph(10)
      nodes.head.addLabel(Label.label("START"))
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      // currently we would plan a distinct here because different x may lead to the same y
      // so we cannot guarantee global uniqueness of y. However we still want bfsPruningVarExpand
      // to produce unique ys given an x which is what we test here
      .bfsPruningVarExpand("(x)-[*0..25]-(y)")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(singleColumn(nodes))
  }

  test("var-length-expand should only find start node once with node filtering") {

    // given
    val nodes = givenGraph {
      val (nodes, _) = circleGraph(10)
      nodes.head.addLabel(Label.label("START"))
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      // currently we would plan a distinct here because different x may lead to the same y
      // so we cannot guarantee global uniqueness of y. However we still want bfsPruningVarExpand
      // to produce unique ys given an x which is what we test here
      .bfsPruningVarExpand("(x)-[*0..25]->(y)", nodePredicates = Seq(Predicate("n", "id(n) <> -1")))
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(singleColumn(nodes))
  }

  test("var-length-expand should only find start node once with node filtering, undirected") {

    // given
    val nodes = givenGraph {
      val (nodes, _) = circleGraph(10)
      nodes.head.addLabel(Label.label("START"))
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      // currently we would plan a distinct here because different x may lead to the same y
      // so we cannot guarantee global uniqueness of y. However we still want bfsPruningVarExpand
      // to produce unique ys given an x which is what we test here
      .bfsPruningVarExpand("(x)-[*0..25]-(y)", nodePredicates = Seq(Predicate("n", "id(n) <> -1")))
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(singleColumn(nodes))
  }

  test("should work on the RHS of an apply") {

    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i", "y")
      .apply()
      .|.distinct("y AS y")
      .|.bfsPruningVarExpand(
        "(x)-[*..2]->(y)",
        nodePredicates = Seq(Predicate("n", "id(n) <> " + g.sa1.getId)),
        relationshipPredicates = Seq(Predicate("r", "id(r) <> " + g.startMiddle.getId))
      )
      .|.nodeByLabelScan("x", "START", IndexOrderNone)
      .input(variables = Seq("i"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues((1 to 10).map(i => Array[Any](i)): _*))

    val expected = (for (i <- 1 to 10) yield Seq(Array[Any](i, g.sb1), Array[Any](i, g.sb2))).flatten

    // then
    runtimeResult should beColumns("i", "y").withRows(expected)
  }

  test("should work on the RHS of an apply, undirected") {

    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i", "y")
      .apply()
      .|.distinct("y AS y")
      .|.bfsPruningVarExpand(
        "(x)-[*..2]-(y)",
        nodePredicates = Seq(Predicate("n", "id(n) <> " + g.sa1.getId)),
        relationshipPredicates = Seq(Predicate("r", "id(r) <> " + g.startMiddle.getId))
      )
      .|.nodeByLabelScan("x", "START", IndexOrderNone)
      .input(variables = Seq("i"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues((1 to 10).map(i => Array[Any](i)): _*))

    val expected =
      (for (i <- 1 to 10)
        yield Seq(Array[Any](i, g.sb1), Array[Any](i, g.sc1), Array[Any](i, g.sb2), Array[Any](i, g.sc2))).flatten

    // then
    runtimeResult should beColumns("i", "y").withRows(expected)
  }

  test("should work on the RHS of an apply including start node") {

    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i", "y")
      .apply()
      .|.distinct("y AS y")
      .|.bfsPruningVarExpand(
        "(x)-[*0..2]->(y)",
        nodePredicates = Seq(Predicate("n", "id(n) <> " + g.sa1.getId)),
        relationshipPredicates = Seq(Predicate("r", "id(r) <> " + g.startMiddle.getId))
      )
      .|.nodeByLabelScan("x", "START", IndexOrderNone)
      .input(variables = Seq("i"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues((1 to 10).map(i => Array[Any](i)): _*))

    val expected =
      (for (i <- 1 to 10) yield Seq(Array[Any](i, g.start), Array[Any](i, g.sb1), Array[Any](i, g.sb2))).flatten

    // then
    runtimeResult should beColumns("i", "y").withRows(expected)
  }

  test("should work on the RHS of an apply including start node, undirected") {

    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("i", "y")
      .apply()
      .|.distinct("y AS y")
      .|.bfsPruningVarExpand(
        "(x)-[*0..2]-(y)",
        nodePredicates = Seq(Predicate("n", "id(n) <> " + g.sa1.getId)),
        relationshipPredicates = Seq(Predicate("r", "id(r) <> " + g.startMiddle.getId))
      )
      .|.nodeByLabelScan("x", "START", IndexOrderNone)
      .input(variables = Seq("i"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues((1 to 10).map(i => Array[Any](i)): _*))

    val expected = (for (i <- 1 to 10)
      yield Seq(
        Array[Any](i, g.start),
        Array[Any](i, g.sb1),
        Array[Any](i, g.sc1),
        Array[Any](i, g.sb2),
        Array[Any](i, g.sc2)
      )).flatten

    // then
    runtimeResult should beColumns("i", "y").withRows(expected)
  }

  test("var-length-expand into") {
    // given
    val (nodes, _) = givenGraph {
      gridGraph()
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "depth")
      .bfsPruningVarExpand("(x)-[*]->(y)", mode = ExpandInto, depthName = Some("depth"))
      .cartesianProduct()
      .|.nodeByLabelScan("y", "1,1", IndexOrderNone)
      .nodeByLabelScan("x", "0,0", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("x", "y", "depth").withRows(Array(Array(nodes(0), nodes(6), 2)))
  }

  test("var-length-expand into self") {
    // given
    val (nodes, _) = givenGraph {
      gridGraph()
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "depth")
      .bfsPruningVarExpand("(x)-[*0..]->(x)", mode = ExpandInto, depthName = Some("depth"))
      .nodeByLabelScan("x", "0,0", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("x", "depth").withRows(Array(Array(nodes(0), 0)))
  }

  test("var-length-expand into self with min length") {
    // given
    val (nodes, _) = givenGraph {
      gridGraph()
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "depth")
      .bfsPruningVarExpand("(x)-[*]-(x)", mode = ExpandInto, depthName = Some("depth"))
      .nodeByLabelScan("x", "0,0", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("x", "depth").withRows(Array(Array(nodes.head, 4)))
  }

  test("var-length-expand into self via loop with min length") {
    // given
    val node = givenGraph {
      val Seq(node) = nodeGraph(1)
      node.createRelationshipTo(node, RelationshipType.withName("R"))
      node
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "depth")
      .bfsPruningVarExpand("(x)-[*]-(x)", mode = ExpandInto, depthName = Some("depth"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("x", "depth").withRows(Array(Array(node, 1)))
  }

  test(
    "TraversalEndpoint(To) should resolve as the next node of a relationship traversal during predicate evaluation"
  ) {
    val (a, b, c) = givenGraph {
      val graph = fromTemplate("""
        (c:TO)-->(a)-->(b:TO)
                  |
                  v
              (ignored)
       """)
      (graph node "a", graph node "b", graph node "c")
    }

    val relPredicates = Seq(
      VariablePredicate(varFor("r"), hasLabels(TraversalEndpoint(varFor("temp"), Endpoint.To), "TO"))
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("t")
      .bfsPruningVarExpandExpr("(s)-[r*]-(t)", relationshipPredicates = relPredicates)
      .nodeByIdSeek("s", Set.empty, a.getId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("t").withRows(inAnyOrder(Seq(Array(b), Array(c))))
  }

  test(
    "TraversalEndpoint(From) should resolve as the previous node of a relationship traversal during predicate evaluation"
  ) {
    val (a, b, c) = givenGraph {
      val graph = fromTemplate("""
        (a:FROM)<--(b:FROM)-->(c)-->()
       """)
      (graph node "a", graph node "b", graph node "c")
    }

    val relPredicates = Seq(
      VariablePredicate(varFor("r"), hasLabels(TraversalEndpoint(varFor("temp"), Endpoint.From), "FROM"))
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("s", "t")
      .bfsPruningVarExpandExpr("(s)-[r*]-(t)", relationshipPredicates = relPredicates)
      .nodeByIdSeek("s", Set.empty, a.getId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = inAnyOrder(Seq(
      Array(a, b),
      Array(a, c)
    ))

    runtimeResult should beColumns("s", "t").withRows(expected)
  }

  // HELPERS
  private def closestMultipleOf(sizeHint: Int, div: Int) = (sizeHint / div) * div
}
