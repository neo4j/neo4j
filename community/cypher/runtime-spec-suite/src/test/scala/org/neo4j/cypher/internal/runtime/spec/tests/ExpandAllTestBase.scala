/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}
import org.neo4j.graphdb.{Label, RelationshipType}

abstract class ExpandAllTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should expand and provide variables for relationship and end node - outgoing") {
    // given
    val n = sizeHint
    val nodes = nodeGraph(n, "Honey")
    val relTuples = (for(i <- 0 until n) yield {
      Seq(
        (i, (2 * i) % n, "OTHER"),
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)

    val rels = connect(nodes, relTuples)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expand("(x)-[r]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relTuples.zip(rels).map {
      case ((f, t, _), rel) => Array(nodes(f), nodes(t), rel)
    }

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should expand and provide variables for relationship and end node - outgoing, one type") {
    // given
    val n = sizeHint
    val nodes = nodeGraph(n, "Honey")
    val relTuples = (for(i <- 0 until n) yield {
      Seq(
        (i, (2 * i) % n, "OTHER"),
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)

    val rels = connect(nodes, relTuples)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expand("(x)-[r:OTHER]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relTuples.zip(rels).collect {
      case ((f, t, typ), rel) if typ == "OTHER" => Array(nodes(f), nodes(t), rel)
    }

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should expand and provide variables for relationship and end node - outgoing, two types") {
    // given
    val n = sizeHint
    val nodes = nodeGraph(n, "Honey")
    val relTuples = (for(i <- 0 until n) yield {
      Seq(
        (i, (2 * i) % n, "OTHER"),
        (i, (i + 1) % n, "NEXT"),
        (i, (3 * i + 5) % n, "BLACKHOLE")
      )
    }).reduce(_ ++ _)

    val rels = connect(nodes, relTuples)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expand("(x)-[r:OTHER|NEXT]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relTuples.zip(rels).collect {
      case ((f, t, typ), rel) if typ == "OTHER" || typ == "NEXT" => Array(nodes(f), nodes(t), rel)
    }

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should expand and handle self loops") {
    // given
    val n = sizeHint
    val nodes = nodeGraph(n, "Honey")
    val relTuples = (for(i <- 0 until n) yield {
      Seq(
        (i, i, "ME")
      )
    }).reduce(_ ++ _)

    val rels = connect(nodes, relTuples)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expand("(x)-[r:ME]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relTuples.zip(rels).map {
      case ((f, t, _), rel) => Array(nodes(f), nodes(t), rel)
    }

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should expand given an empty input") {
    // given
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expand("(x)-[r]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "r").withNoRows()
  }

  test("should handle expand outgoing") {
    // given
    val (_, rels) = circleGraph(sizeHint)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        r <- rels
        row <- List(Array(r.getStartNode, r.getEndNode))
      } yield row
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle expand incoming") {
    // given
    val (_, rels) = circleGraph(sizeHint)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandAll("(x)<--(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        r <- rels
        row <- List(Array(r.getEndNode, r.getStartNode))
      } yield row
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle existing types") {
    // given
    val (r1, r2, r3) = inTx {
      val node = graphDb.createNode(Label.label("L"))
      val other = graphDb.createNode(Label.label("L"))
      (node.createRelationshipTo(other, RelationshipType.withName("R")),
        node.createRelationshipTo(other, RelationshipType.withName("S")),
        node.createRelationshipTo(other, RelationshipType.withName("T")))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandAll("(x)-[:R|S|T]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = List(
      Array(r1.getStartNode, r1.getEndNode),
      Array(r2.getStartNode, r2.getEndNode),
      Array(r3.getStartNode, r3.getEndNode)
      )

    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle types missing on compile") {
    // given
    inTx(
      1 to sizeHint foreach { _ =>
        graphDb.createNode().createRelationshipTo(graphDb.createNode(), RelationshipType.withName("BASE"))
      })

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandAll("(x)-[:R|S|T]->(y)")
      .allNodeScan("x")
      .build()

    execute(logicalQuery, runtime) should beColumns("x", "y").withRows(List.empty)

    //CREATE S
    inTx(
      graphDb.createNode().createRelationshipTo(graphDb.createNode(), RelationshipType.withName("S"))
      )
    execute(logicalQuery, runtime) should beColumns("x", "y").withRows(RowCount(1))

    //CREATE R
    inTx(
      graphDb.createNode().createRelationshipTo(graphDb.createNode(), RelationshipType.withName("R"))
      )
    execute(logicalQuery, runtime) should beColumns("x", "y").withRows(RowCount(2))

    //CREATE T
    inTx(
      graphDb.createNode().createRelationshipTo(graphDb.createNode(), RelationshipType.withName("T"))
      )
    execute(logicalQuery, runtime) should beColumns("x", "y").withRows(RowCount(3))
  }

  test("cached plan should adapt to new relationship types") {
    // given
    inTx(
      1 to sizeHint foreach { _ =>
        graphDb.createNode().createRelationshipTo(graphDb.createNode(), RelationshipType.withName("BASE"))
      })

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandAll("(x)-[:R|S|T]->(y)")
      .allNodeScan("x")
      .build()

    val executablePlan = buildPlan(logicalQuery, runtime)

    execute(executablePlan) should beColumns("x", "y").withRows(List.empty)

    //CREATE S
    inTx(
      graphDb.createNode().createRelationshipTo(graphDb.createNode(), RelationshipType.withName("S"))
      )
    execute(logicalQuery, runtime) should beColumns("x", "y").withRows(RowCount(1))

    //CREATE R
    inTx(
      graphDb.createNode().createRelationshipTo(graphDb.createNode(), RelationshipType.withName("R"))
      )
    execute(executablePlan) should beColumns("x", "y").withRows(RowCount(2))

    //CREATE T
    inTx(
      graphDb.createNode().createRelationshipTo(graphDb.createNode(), RelationshipType.withName("T"))
      )
    execute(executablePlan) should beColumns("x", "y").withRows(RowCount(3))
  }
}

// Supported by interpreted, slotted
trait ExpandAllWithOptionalTestBase[CONTEXT <: RuntimeContext] {
  self: ExpandAllTestBase[CONTEXT] =>

  test("given a null start point, returns an empty iterator") {
    // given
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expand("(x)-[r]->(y)")
      .optional()
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x", "y", "r").withNoRows()
  }
}
