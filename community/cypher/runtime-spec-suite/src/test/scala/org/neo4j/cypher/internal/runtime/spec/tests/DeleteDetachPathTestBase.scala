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
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SingleRelationshipPathStep
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.internal.helpers.collection.Iterables

abstract class DeleteDetachPathTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("detach delete paths") {
    val chainCount = 3
    given {
      chainGraphs(chainCount, "SMELLS", "SMELLS", "STINKS")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .detachDeletePath("p")
      .eager()
      .projection(Map("p" -> multiOutgoingRelationshipPath("n", "r", "m")))
      .filter("m:END")
      .expandAll("(n)-[r*]-(m)")
      .nodeByLabelScan("n", "START")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p")
      .withStatistics(nodesDeleted = chainCount * 4, relationshipsDeleted = chainCount * 3)
    Iterables.count(tx.getAllNodes) shouldBe 0
    Iterables.count(tx.getAllRelationships) shouldBe 0
  }

  test("detach delete path that is part of connected graph") {
    val nodeCount = 4
    given {
      val allNodes = nodeGraph(nodeCount)
      allNodes.head.addLabel(Label.label("A"))
      allNodes(1).addLabel(Label.label("B"))

      for {
        nodeA <- allNodes
        nodeB <- allNodes
      } yield {
        nodeA.createRelationshipTo(nodeB, RelationshipType.withName("KNOWS"))
        nodeA.createRelationshipTo(nodeB, RelationshipType.withName("LIKES"))
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .detachDeletePath("p")
      .eager()
      .projection(Map("p" -> singleOutgoingRelationshipPath("a", "r", "b")))
      .filter("b:B")
      .expandAll("(a)-[r:LIKES]->(b)")
      .nodeByLabelScan("a", "A")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p")
      .withStatistics(nodesDeleted = 2, relationshipsDeleted = 8 + (8 * (nodeCount - 2)))
    Iterables.count(tx.getAllNodes) shouldBe 2
    Iterables.count(tx.getAllRelationships) shouldBe 8
  }

  test("multiple detach delete") {
    val chainCount = 3
    given {
      chainGraphs(chainCount, "SMELLS", "SMELLS", "STINKS")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .detachDeletePath("p")
      .detachDeletePath("p")
      .detachDeletePath("p")
      .eager()
      .projection(Map("p" -> multiOutgoingRelationshipPath("n", "r", "m")))
      .filter("m:END")
      .expandAll("(n)-[r*]-(m)")
      .nodeByLabelScan("n", "START")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p")
      .withStatistics(nodesDeleted = chainCount * 4, relationshipsDeleted = chainCount * 3)
    Iterables.count(tx.getAllNodes) shouldBe 0
    Iterables.count(tx.getAllRelationships) shouldBe 0
  }

  test("detach delete on rhs of apply") {
    val chainCount = 3
    given {
      chainGraphs(chainCount, "SMELLS", "SMELLS", "STINKS")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .apply()
      .|.detachDeletePath("p")
      .|.eager()
      .|.projection(Map("p" -> multiOutgoingRelationshipPath("n", "r", "m")))
      .|.expandInto("(n)-[r*]-(m)")
      .|.nodeByLabelScan("m", "END")
      .eager()
      .nodeByLabelScan("n", "START")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p")
      .withStatistics(nodesDeleted = chainCount * 4, relationshipsDeleted = chainCount * 3)
    Iterables.count(tx.getAllNodes) shouldBe 0
    Iterables.count(tx.getAllRelationships) shouldBe 0
  }

  test("detach delete paths with single node") {
    val nodeCount = 5
    given {
      val nodes = nodeGraph(nodeCount)
      nodes.last.addLabel(Label.label("PleaseKillMe"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .detachDeletePath("p")
      .projection(Map("p" -> PathExpression(NodePathStep(varFor("n"), NilPathStep()(pos))(pos))(InputPosition.NONE)))
      .nodeByLabelScan("n", "PleaseKillMe")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p")
      .withStatistics(nodesDeleted = 1)
    Iterables.count(tx.getAllNodes) shouldBe (nodeCount - 1)
  }

  def singleOutgoingRelationshipPath(fromNode: String, relationship: String, toNode: String) = {
    PathExpression(
      NodePathStep(
        node = varFor(fromNode),
        SingleRelationshipPathStep(
          rel = varFor(relationship),
          direction = OUTGOING,
          toNode = Some(varFor(toNode)),
          next = NilPathStep()(pos)
        )(pos)
      )(pos)
    )(InputPosition.NONE)
  }

  def multiOutgoingRelationshipPath(fromNode: String, relationships: String, toNode: String) = {
    PathExpression(
      NodePathStep(
        node = varFor(fromNode),
        MultiRelationshipPathStep(
          rel = varFor(relationships),
          direction = OUTGOING,
          toNode = Some(varFor(toNode)),
          next = NilPathStep()(pos)
        )(pos)
      )(pos)
    )(InputPosition.NONE)
  }
}
