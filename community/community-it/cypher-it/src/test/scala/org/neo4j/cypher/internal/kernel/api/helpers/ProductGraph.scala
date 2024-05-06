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

package org.neo4j.cypher.internal.kernel.api.helpers

import org.eclipse.collections.impl.stack.mutable.ArrayStack
import org.neo4j.cypher.internal.kernel.api.helpers.ProductGraph.PGNode
import org.neo4j.cypher.internal.kernel.api.helpers.ProductGraph.PGRelationship
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TraversalDirection
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.ProductGraphTraversalCursor
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.State
import org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

import java.util.Collections

import scala.collection.mutable

class ProductGraph(val adjacencyLists: mutable.HashMap[PGNode, mutable.Set[PGRelationship]] = mutable.HashMap.empty) {

  def addNode(nodeId: Long, stateId: Int): ProductGraph = {
    adjacencyLists.put(PGNode(nodeId, stateId), new mutable.HashSet[PGRelationship])
    this
  }

  def addRelationship(
    sourceNodeId: Long,
    sourceNodeStateId: Int,
    relId: Long,
    targetNodeId: Long,
    targetNodeStateId: Int
  ): ProductGraph = {
    adjacencyLists(PGNode(sourceNodeId, sourceNodeStateId))
      .add(PGRelationship(relId, PGNode(targetNodeId, targetNodeStateId)))
    this
  }

  def addJuxtaposition(nodeId: Long, sourceNodeStateId: Int, targetNodeStateId: Int): ProductGraph = {
    adjacencyLists(PGNode(nodeId, sourceNodeStateId))
      .add(PGRelationship(NO_SUCH_RELATIONSHIP, PGNode(nodeId, targetNodeStateId)))
    this
  }

  private def diff(other: ProductGraph) = {
    val missing = new mutable.HashMap[PGNode, mutable.Set[PGRelationship]]
    for ((node, rels) <- this.adjacencyLists) {
      val otherSet = other.adjacencyLists.get(node)
      if (!otherSet.contains(rels)) {
        val diff = new mutable.HashSet[PGRelationship]().addAll(rels)
        if (otherSet.isEmpty) {
          missing.put(node, diff)
        } else {
          otherSet.foreach { diff --= _ }
          if (diff.nonEmpty) {
            missing.put(node, diff)
          }
        }
      }
    }
    missing
  }

  private def diffMessage(other: ProductGraph): String = {
    val present = this.diff(other)
    val absent = other.diff(this)
    val message = new StringBuilder("Product graphs different.")
    if (present.nonEmpty) {
      message.append("\nPresent in the first but not the second: ")
      for ((node, rels) <- present) {
        message.append("\n").append(node.ids)
        for (rel <- rels) { message.append("\n - ").append(rel) }
      }
    }
    if (absent.nonEmpty) {
      message.append("\nPresent in the second but not the first: ")
      for ((node, rels) <- absent) {
        message.append("\n").append(node.ids)
        for (rel <- rels) { message.append("\n - ").append(rel) }
      }
    }
    if (present.isEmpty && absent.isEmpty) {
      message.append(" But could not find the difference!")
    }
    message.toString()
  }
}

object ProductGraph {

  def equalProductGraph(right: ProductGraph): Matcher[ProductGraph] =
    (left: ProductGraph) => {
      val matches = left.adjacencyLists.equals(right.adjacencyLists)
      MatchResult(matches, left.diffMessage(right), "product graphs were equal")
    }

  final case class PGNode(id: Long, stateId: Int) {
    def ids: String = "(node:" + id + ",state:" + stateId + ")"
  }

  final case class PGRelationship(id: Long, targetNode: PGNode)

  final private case class ToExpand(node: PGNode, state: State)

  /**
   * Creates a ProductGraph given a start node/state and cursors by exhausting the connected component containing
   * the source using depth first traversal and recording all traversed relationships/nodes+states in adjacency
   * lists. The expansions in this constructor will always initialize the pgCursor with one (node, state) pair
   * at a time.
   */
  def fromCursor(sourceNodeId: Long, startState: State, pgCursor: ProductGraphTraversalCursor): ProductGraph = {
    val queue = new ArrayStack[ToExpand]
    val seen = new mutable.HashSet[PGNode]
    val adjacencyLists = new mutable.HashMap[PGNode, mutable.Set[PGRelationship]]

    def expandNode(toExpand: ToExpand): Unit = {
      val relationships = new mutable.HashSet[PGRelationship]

      // Expand node juxtapositions
      for (nodeJuxtaposition <- toExpand.state.getNodeJuxtapositions) {
        if (nodeJuxtaposition.state(TraversalDirection.Forward).test(toExpand.node.id)) {
          val newNode = PGNode(toExpand.node.id, nodeJuxtaposition.targetState.id)
          if (seen.add(newNode)) {
            queue.push(ToExpand(newNode, nodeJuxtaposition.targetState))
          }
          relationships.add(PGRelationship(NO_SUCH_RELATIONSHIP, newNode))
        }
      }

      // Expand relationship expansions
      pgCursor.setNodeAndStates(toExpand.node.id, Collections.singletonList(toExpand.state), TraversalDirection.Forward)
      while (pgCursor.next) {
        val newNode = PGNode(pgCursor.otherNodeReference, pgCursor.targetState.id)
        if (seen.add(newNode)) {
          queue.push(ToExpand(newNode, pgCursor.targetState))
        }
        relationships.add(PGRelationship(pgCursor.relationshipReference, newNode))
      }
      adjacencyLists.put(toExpand.node, relationships)
    }

    val sourceNode = PGNode(sourceNodeId, startState.id)
    seen.add(sourceNode)
    queue.push(ToExpand(sourceNode, startState))

    while (queue.notEmpty) {
      val node = queue.pop
      expandNode(node)
    }
    new ProductGraph(adjacencyLists)
  }
}
