/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.spi

import org.neo4j.cypher.QueryStatistics
import java.util.concurrent.atomic.AtomicInteger
import org.neo4j.graphdb.{PropertyContainer, Relationship, Node}


class UpdateCountingQueryContext(inner: QueryContext) extends DelegatingQueryContext(inner) {

  private val createdNodes = new Counter
  private val createdRelationships = new Counter
  private val propertySet = new Counter
  private val deletedNodes = new Counter
  private val deletedRelationships = new Counter
  private val addedLabels = new Counter
  private val removedLabels = new Counter

  def getStatistics: QueryStatistics = QueryStatistics(
    nodesCreated = createdNodes.count,
    relationshipsCreated = createdRelationships.count,
    propertiesSet = propertySet.count,
    deletedNodes = deletedNodes.count,
    addedLabels = addedLabels.count,
    removedLabels = removedLabels.count,
    deletedRelationships = deletedRelationships.count)

  override def createNode() = {
    createdNodes.increase()
    inner.createNode()
  }

  override def nodeOps: Operations[Node] = new CountingOps[Node](inner.nodeOps, deletedNodes)

  override def relationshipOps: Operations[Relationship] = new CountingOps[Relationship](inner.relationshipOps, deletedRelationships)

  override def setLabelsOnNode(node: Long, labelIds: Iterable[Long]): Int = {
    val added = inner.setLabelsOnNode(node, labelIds)
    addedLabels.increase(added)
    added
  }

  override def createRelationship(start: Node, end: Node, relType: String) = {
    createdRelationships.increase()
    inner.createRelationship(start, end, relType)
  }

  override def removeLabelsFromNode(node: Long, labelIds: Iterable[Long]): Int = {
    val removed = inner.removeLabelsFromNode(node, labelIds)
    removedLabels.increase(removed)
    removed
  }

  class Counter {
    val counter: AtomicInteger = new AtomicInteger()

    def count: Int = counter.get()

    def increase(amount: Int = 1) {
      counter.addAndGet(amount)
    }
  }

  private class CountingOps[T <: PropertyContainer](inner: Operations[T],
                                                    deletes: Counter) extends DelegatingOperations[T](inner) {
    override def delete(obj: T) {
      deletes.increase()
      inner.delete(obj)
    }

    override def removeProperty(obj: T, propertyKey: String) {
      propertySet.increase()
      inner.removeProperty(obj, propertyKey)
    }

    override def setProperty(obj: T, propertyKey: String, value: Any) {
      propertySet.increase()
      inner.setProperty(obj, propertyKey, value)
    }
  }
}