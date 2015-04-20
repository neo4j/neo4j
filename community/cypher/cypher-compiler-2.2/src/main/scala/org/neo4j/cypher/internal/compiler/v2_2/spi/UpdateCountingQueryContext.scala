/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.spi

import java.util.concurrent.atomic.AtomicInteger
import org.neo4j.cypher.internal.compiler.v2_2.InternalQueryStatistics
import org.neo4j.graphdb.{Direction, PropertyContainer, Relationship, Node}

class UpdateCountingQueryContext(inner: QueryContext) extends DelegatingQueryContext(inner) {

  private val nodesCreated = new Counter
  private val relationshipsCreated = new Counter
  private val propertiesSet = new Counter
  private val nodesDeleted = new Counter
  private val relationshipsDeleted = new Counter
  private val labelsAdded = new Counter
  private val labelsRemoved = new Counter
  private val indexesAdded = new Counter
  private val indexesRemoved = new Counter
  private val constraintsAdded = new Counter
  private val constraintsRemoved = new Counter

  def getStatistics = InternalQueryStatistics(
    nodesCreated = nodesCreated.count,
    relationshipsCreated = relationshipsCreated.count,
    propertiesSet = propertiesSet.count,
    nodesDeleted = nodesDeleted.count,
    labelsAdded = labelsAdded.count,
    labelsRemoved = labelsRemoved.count,
    relationshipsDeleted = relationshipsDeleted.count,
    indexesAdded = indexesAdded.count,
    indexesRemoved = indexesRemoved.count,
    constraintsAdded = constraintsAdded.count,
    constraintsRemoved = constraintsRemoved.count)

  override def getOptStatistics = Some(getStatistics)

  override def createNode() = {
    nodesCreated.increase()
    inner.createNode()
  }

  override def nodeOps: Operations[Node] = new CountingOps[Node](inner.nodeOps, nodesDeleted)

  override def relationshipOps: Operations[Relationship] = new CountingOps[Relationship](inner.relationshipOps,
    relationshipsDeleted)

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int = {
    val added = inner.setLabelsOnNode(node, labelIds)
    labelsAdded.increase(added)
    added
  }

  override def createRelationship(start: Node, end: Node, relType: String) = {
    relationshipsCreated.increase()
    inner.createRelationship(start, end, relType)
  }

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int = {
    val removed = inner.removeLabelsFromNode(node, labelIds)
    labelsRemoved.increase(removed)
    removed
  }

  override def addIndexRule(labelId: Int, propertyKeyId: Int) = {
    val result = inner.addIndexRule(labelId, propertyKeyId)
    result.ifCreated { indexesAdded.increase() }
    result
  }

  override def dropIndexRule(labelId: Int, propertyKeyId: Int) {
    inner.dropIndexRule(labelId, propertyKeyId)
    indexesRemoved.increase()
  }

  override def createUniqueConstraint(labelId: Int, propertyKeyId: Int) = {
    val result = inner.createUniqueConstraint(labelId, propertyKeyId)
    result.ifCreated { constraintsAdded.increase() }
    result
  }

  override def dropUniqueConstraint(labelId: Int, propertyKeyId: Int) {
    inner.dropUniqueConstraint(labelId, propertyKeyId)
    constraintsRemoved.increase()
  }

  override def nodeGetDegree(node: Long, dir: Direction): Int = super.nodeGetDegree(node, dir)

  class Counter {
    val counter: AtomicInteger = new AtomicInteger()

    def count: Int = counter.get()

    def increase(amount: Int = 1) {
      counter.addAndGet(amount)
    }
  }

  private class CountingOps[T <: PropertyContainer](inner: Operations[T],
                                                    deletes: Counter)
    extends DelegatingOperations[T](inner) {

    override def delete(obj: T) {
      deletes.increase()
      inner.delete(obj)
    }

    override def removeProperty(id: Long, propertyKeyId: Int) {
      propertiesSet.increase()
      inner.removeProperty(id, propertyKeyId)
    }

    override def setProperty(id: Long, propertyKeyId: Int, value: Any) {
      propertiesSet.increase()
      inner.setProperty(id, propertyKeyId, value)
    }
  }
}
