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
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.internal.helpers.collection.Iterables

import scala.jdk.CollectionConverters.IteratorHasAsScala

abstract class DeleteDetachNodeTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("detach delete all nodes without relationships") {
    val nodeCount = 10

    given {
      nodeGraph(nodeCount)
    }

    detachDeleteAllTest(nodeCount, relationshipCount = 0)
  }

  test("detach delete all nodes on empty database") {
    detachDeleteAllTest(nodeCount = 0, relationshipCount = 0)
  }

  test("detach delete all nodes with relationsships") {
    given {
      chainGraphs(1, "LIKES", "LIKES", "KIND_OF_LIKES")
    }

    detachDeleteAllTest(nodeCount = 4, relationshipCount = 3)
  }

  test("detach delete node with loop relationship") {
    given {
      val narcissisticNode = runtimeTestSupport.tx.createNode()
      narcissisticNode.createRelationshipTo(narcissisticNode, RelationshipType.withName("ADORES"))
      narcissisticNode.createRelationshipTo(narcissisticNode, RelationshipType.withName("LOVES"))
    }

    detachDeleteAllTest(1, 2)
  }

  test("detach delete single connected node") {
    val nodeCount = 10
    given {
      val deleteNode = runtimeTestSupport.tx.createNode(Label.label("DELETE_ME"))
      val saveNodes = nodeGraph(nodeCount - 1, "SAVE_ME")
      val allNodes = saveNodes :+ deleteNode

      // Connect all nodes in both directions
      for {
        nodeA <- allNodes
        nodeB <- allNodes
      } yield {
        nodeA.createRelationshipTo(nodeB, RelationshipType.withName("FORWARDS"))
        nodeB.createRelationshipTo(nodeA, RelationshipType.withName("BACKWARDS"))
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .detachDeleteNode("n")
      .nodeByLabelScan("n", "DELETE_ME")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n")
      .withStatistics(nodesDeleted = 1, relationshipsDeleted = nodeCount * 2 + (nodeCount - 1) * 2)
    Iterables.count(tx.getAllNodes) shouldBe (nodeCount - 1)
    Iterables.count(tx.getAllRelationships) shouldBe ((nodeCount - 1) * (nodeCount - 1) * 2)
  }

  test("duplicate detach delete") {
    val nodeCount = 10
    given {
      nodeGraph(10)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .detachDeleteNode("n")
      .detachDeleteNode("n")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n").withStatistics(nodesDeleted = nodeCount)
    Iterables.count(tx.getAllNodes) shouldBe 0
  }

  test("detach delete on rhs of apply") {
    given {
      linkedChainGraph(3, 4)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .apply()
      .|.detachDeleteNode("n")
      .|.argument("n")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n").withStatistics(nodesDeleted = 14, relationshipsDeleted = 33)
    Iterables.count(tx.getAllNodes) shouldBe 0
    Iterables.count(tx.getAllRelationships) shouldBe 0
  }

  private def detachDeleteAllTest(nodeCount: Int, relationshipCount: Int): Unit = {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .detachDeleteNode("n")
      .allNodeScan("n")
      .build(readOnly = false)

    val allNodes = tx.getAllNodes
    try {
      val nodeArray = allNodes.iterator().asScala.toIndexedSeq.map(n => Array(n))
      // then
      val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
      runtimeResult should beColumns("n")
        .withRows(nodeArray, listInAnyOrder = true)
        .withStatistics(nodesDeleted = nodeCount, relationshipsDeleted = relationshipCount)
    } finally {
      allNodes.close()
    }

    Iterables.count(tx.getAllNodes) shouldBe 0
    Iterables.count(tx.getAllRelationships) shouldBe 0
  }
}
