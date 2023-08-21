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
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType

abstract class PruningVarLengthExpandTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("var-length-expand with no relationships") {
    // given
    given { nodeGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*1..2]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withNoRows()
  }

  test("var-length-expand with max length") {
    // given
    val (Seq(n1, n2, _), _) = given { lollipopGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*..1]->(y)")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array(Array(n2))

    runtimeResult should beColumns("y").withRows(expected)
  }

  test("var-length-expand with max length including start node") {
    // given
    val (Seq(n1, n2, _), _) = given { lollipopGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*0..1]->(y)")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      Array(
        Array(n1),
        Array(n2)
      )

    runtimeResult should beColumns("y").withRows(expected)
  }

  test("var-length-expand with min and max length") {
    // given
    val paths = given { chainGraphs(3, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*2..4]->(y)")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        path <- paths
        length <- 2 to 4
      } yield {
        val pathPrefix = path.take(length)
        Array(pathPrefix.endNode())
      }

    runtimeResult should beColumns("y").withRows(expected)
  }

  test("var-length-expand with length 0") {
    // given
    val (Seq(n1, _, _), _) = given { lollipopGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*0]->(y)")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array(Array(n1))
    runtimeResult should beColumns("y").withRows(expected)
  }

  test("var-length-expand with length 0..1") {
    // given
    val (Seq(n1, n2, _), _) = given { lollipopGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*0..1]->(y)")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array(
      Array(n1),
      Array(n2)
    )
    runtimeResult should beColumns("y").withRows(expected)
  }

  test("var-length-expand with length 0..2") {
    // given
    val (Seq(n1, n2, n3), _) = given { lollipopGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*0..2]->(y)")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array(
      Array(n1),
      Array(n2),
      Array(n3)
    )
    runtimeResult should beColumns("y").withRows(expected)
  }

  test("var-length-expand with self-loop") {
    // given
    val n2 = given {
      val n1 = tx.createNode(Label.label("START"))
      val n2 = tx.createNode()
      val relType = RelationshipType.withName("R")
      n1.createRelationshipTo(n2, relType)
      n1.createRelationshipTo(n1, relType)
      n2
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*2..2]-(y)")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array(
      Array(n2)
    )
    runtimeResult should beColumns("y").withRows(expected)
  }

  test("fixed length with shortcut") {
    /*
    n1 - ---- - n2 - n3 - n4
       - n1_2 -

    Even though n1-n2 is traversed first, the length 4 path over n1_2 should be found
     */

    // given
    val n4 = given {
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

      n4
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*4..4]-(y)")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array(
      Array(n4)
    )
    runtimeResult should beColumns("y").withRows(expected)
  }

  test("fixed length with longer shortcut") {
    /*
    n1 - ----- - ----- - n2 - n3 - n4
       - n1_2a - n1_2b -

    Even though n1-n2 is traversed first, the length 4 path over n1_2 should be found
     */

    // given
    val n4 = given {
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

      n4
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*5..5]-(y)")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array(
      Array(n4)
    )
    runtimeResult should beColumns("y").withRows(expected)
  }

  test("two ways to get to the same node - one inside and one outside the max") {
    /*
    (x)-[1]->()-[2]->()-[3]->()-[4]->(y)-[4]->(z)
      \                              ^
       \----------------------------/

     */

    // given
    val (n1, n2, n3, y, z) = given {
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
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*1..4]->(y)")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array(
      Array(n1),
      Array(n2),
      Array(n3),
      Array(y),
      Array(z)
    )
    runtimeResult should beColumns("y").withRows(expected)
  }

  // NULL INPUT

  test("should handle null from-node") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*..2]-(y)")
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
    val g = given { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*1..2]->(y)")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(Array(
      Array(g.sb1),
      Array(g.sa1),
      Array(g.middle),
      Array(g.sb2),
      Array(g.sc3),
      Array(g.ea1),
      Array(g.eb1),
      Array(g.ec1)
    ))
  }

  test("should filter on incoming direction") {
    // given
    val g = given { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)<-[*1..2]-(y)")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(Array(
      Array(g.sc1),
      Array(g.sc2)
    ))
  }

  test("should expand on BOTH direction") {
    // given
    val g = given { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*1..2]-(y)")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(Array(
      Array(g.sb1), // outgoing only
      Array(g.sa1),
      Array(g.middle),
      Array(g.sb2),
      Array(g.sc3),
      Array(g.ea1),
      Array(g.eb1),
      Array(g.ec1),
      Array(g.sc1), // incoming only
      Array(g.sc2),
      Array(g.end)
    ))
  }

  // EXPANSION FILTERING, RELATIONSHIP TYPE

  test("should filter on relationship type A") {
    // given
    val g = given { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[:A*1..2]->(y)")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(Array(
      Array(g.sa1),
      Array(g.middle),
      Array(g.sc3),
      Array(g.ea1),
      Array(g.ec1)
    ))
  }

  test("should filter on relationship type B") {
    // given
    val g = given { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[:B*1..2]->(y)")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(Array(
      Array(g.sb1),
      Array(g.sb2)
    ))
  }

  // EXPANSION FILTERING, NODE AND RELATIONSHIP PREDICATE

  test("should filter on node predicate") {
    // given
    val g = given { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*1..2]-(y)", nodePredicates = Seq(Predicate("n", "id(n) <> " + g.middle.getId)))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(Array(
      Array(g.sa1),
      Array(g.sb1),
      Array(g.sb2),
      Array(g.sc1),
      Array(g.sc2)
    ))
  }

  test("should filter on two node predicates") {
    // given
    val g = given { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand(
        "(x)-[*1..2]-(y)",
        nodePredicates = Seq(
          Predicate("n", "id(n) <> " + g.middle.getId),
          Predicate("n2", "id(n2) <> " + g.sc3.getId)
        )
      )
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(Array(
      Array(g.sa1),
      Array(g.sb1),
      Array(g.sb2),
      Array(g.sc1),
      Array(g.sc2)
    ))
  }

  test("should filter on node predicate on first node") {
    // given
    val g = given { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*1..2]-(y)", nodePredicates = Seq(Predicate("n", "id(n) <> " + g.start.getId)))
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withNoRows()
  }

  test("should filter on node predicate on first node from reference") {
    // given
    val g = given { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(X)-[*1..2]-(y)", nodePredicates = Seq(Predicate("n", "id(n) <> " + g.start.getId)))
      .projection("x AS X")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withNoRows()
  }

  test("should filter on relationship predicate") {
    // given
    val g = given { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand(
        "(x)-[*1..2]->(y)",
        relationshipPredicates = Seq(Predicate("r", "id(r) <> " + g.startMiddle.getId))
      )
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(Array(
      Array(g.sa1),
      Array(g.middle),
      Array(g.sb1),
      Array(g.sb2)
    ))
  }

  test("should filter on two relationship predicates") {
    // given
    val g = given { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand(
        "(x)-[*1..3]-(y)",
        relationshipPredicates = Seq(
          Predicate("r", "id(r) <> " + g.startMiddle.getId),
          Predicate("r2", "id(r2) <> " + g.endMiddle.getId)
        )
      )
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(Array(
      Array(g.sa1),
      Array(g.sb1),
      Array(g.sc1),
      Array(g.middle),
      Array(g.sb2),
      Array(g.sc2),
      Array(g.ea1),
      Array(g.eb1),
      Array(g.ec1),
      Array(g.sc3)
    ))
  }

  test("should filter on node and relationship predicate") {
    // given
    val g = given { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand(
        "(x)-[*2..2]-(y)",
        nodePredicates = Seq(Predicate("n", "id(n) <> " + g.sa1.getId)),
        relationshipPredicates = Seq(Predicate("r", "id(r) <> " + g.startMiddle.getId))
      )
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(Array(
      Array(g.sc2),
      Array(g.sb2)
    ))
  }

  test("should handle predicate accessing start node") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = given { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*..5]->(y)", nodePredicates = Seq(Predicate("n", "'START' IN labels(x)")))
      .input(nodes = Seq("x"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 1 to 5
      } yield Array(path.take(length).endNode())
    runtimeResult should beColumns("y").withRows(expected)
  }

  test("should handle predicate accessing start node including start node") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = given { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*0..5]->(y)", nodePredicates = Seq(Predicate("n", "'START' IN labels(x)")))
      .input(nodes = Seq("x"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 0 to 5
      } yield Array(path.take(length).endNode())
    runtimeResult should beColumns("y").withRows(expected)
  }

  test("should handle predicate accessing reference in context") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = given { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*..5]->(y)", nodePredicates = Seq(Predicate("n", "id(n) >= zero")))
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
      } yield Array(path.take(length).endNode())
    runtimeResult should beColumns("y").withRows(expected)
  }

  test("should handle predicate accessing reference in context and including start node") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = given { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*0..5]->(y)", nodePredicates = Seq(Predicate("n", "id(n) >= zero")))
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
      } yield Array(path.take(length).endNode())
    runtimeResult should beColumns("y").withRows(expected)
  }

  test("should handle predicate accessing node in context") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = given { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*..5]->(y)", nodePredicates = Seq(Predicate("n", "id(other) >= 0")))
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
      } yield Array(path.take(length).endNode())
    runtimeResult should beColumns("y").withRows(expected)
  }

  test("should handle predicate accessing node in context and including start node") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = given { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .distinct("y AS y")
      .pruningVarExpand("(x)-[*0..5]->(y)", nodePredicates = Seq(Predicate("n", "id(other) >= 0")))
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
      } yield Array(path.take(length).endNode())
    runtimeResult should beColumns("y").withRows(expected)
  }

  // HELPERS

  private def closestMultipleOf(sizeHint: Int, div: Int) = (sizeHint / div) * div
}
