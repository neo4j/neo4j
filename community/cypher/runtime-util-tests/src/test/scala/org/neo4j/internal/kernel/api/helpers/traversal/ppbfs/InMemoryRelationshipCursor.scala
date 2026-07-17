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
package org.neo4j.internal.kernel.api.helpers.traversal.ppbfs

import org.neo4j.cypher.internal.util.test_helpers.InMemoryGraph
import org.neo4j.cypher.internal.util.test_helpers.InMemoryGraph.Rel
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.RelationshipTraversalEntities
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.InMemoryRelationshipCursor.TraversedRel
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.ProductGraphTraversalCursor.DataGraphRelationshipCursor
import org.neo4j.storageengine.api.RelationshipSelection

class InMemoryRelationshipCursor(graph: InMemoryGraph, hooks: PPBFSHooks) extends DataGraphRelationshipCursor {
  private var rels = Iterator.empty[TraversedRel]
  private var currentNode: Long = -1L
  private var current: TraversedRel = _

  def nextRelationship(): Boolean = {
    hooks.cursorNextRelationship(currentNode)
    if (rels.hasNext) {
      current = rels.next()
      true
    } else {
      current = null
      false
    }
  }

  def setNode(node: Long, selection: RelationshipSelection): Unit = {
    hooks.cursorSetNode(node)
    currentNode = node
    rels = graph.nodeRels(node)
      .collect { case (rel, dir) if selection.test(rel.relType, dir) => TraversedRel(rel, node) }
  }

  def relationshipReference(): Long = this.current.relationshipReference()

  def originNodeReference(): Long = this.currentNode

  def otherNodeReference(): Long = this.current.otherNodeReference()

  def sourceNodeReference(): Long = this.current.sourceNodeReference()

  def targetNodeReference(): Long = this.current.targetNodeReference()

  def `type`(): Int = this.current.`type`()

  def setTracer(tracer: KernelReadTracer): Unit = ()
}

object InMemoryRelationshipCursor {

  case class TraversedRel(rel: Rel, from: Long) extends RelationshipTraversalEntities {
    def relationshipReference(): Long = rel.id
    def `type`(): Int = rel.relType
    def sourceNodeReference(): Long = rel.source
    def targetNodeReference(): Long = rel.target

    def otherNodeReference(): Long =
      from match {
        case rel.target => rel.source
        case rel.source => rel.target
        case _          => throw new IllegalArgumentException()
      }
    def originNodeReference(): Long = from
  }
}
