/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.spi

import java.util.concurrent.atomic.AtomicInteger
import org.neo4j.cypher.internal.compiler.v2_3.InternalQueryStatistics
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.graphdb.{PropertyContainer, Relationship, Node}

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
  private val uniqueConstraintsAdded = new Counter
  private val uniqueConstraintsRemoved = new Counter
  private val propertyExistenceConstraintsAdded = new Counter
  private val propertyExistenceConstraintsRemoved = new Counter

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
    uniqueConstraintsAdded = uniqueConstraintsAdded.count,
    uniqueConstraintsRemoved = uniqueConstraintsRemoved.count,
    existenceConstraintsAdded = propertyExistenceConstraintsAdded.count,
    existenceConstraintsRemoved = propertyExistenceConstraintsRemoved.count)

  override def getOptStatistics = Some(getStatistics)

  override def createNode() = {
    nodesCreated.increase()
    inner.createNode()
  }

  override def nodeOps: Operations[Node] =
    new CountingOps[Node](inner.nodeOps, nodesDeleted)

  override def relationshipOps: Operations[Relationship] =
    new CountingOps[Relationship](inner.relationshipOps, relationshipsDeleted)

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int = {
    val added = inner.setLabelsOnNode(node, labelIds)
    labelsAdded.increase(added)
    added
  }

  override def createRelationship(start: Node, end: Node, relType: String) = {
    relationshipsCreated.increase()
    inner.createRelationship(start, end, relType)
  }

  override def createRelationship(start: Long, end: Long, relType: Int) = {
    relationshipsCreated.increase()
    inner.createRelationship(start, end, relType)
  }

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int = {
    val removed = inner.removeLabelsFromNode(node, labelIds)
    labelsRemoved.increase(removed)
    removed
  }

  override def detachDeleteNode(obj: Node): Int = {
    nodesDeleted.increase()
    val count = inner.detachDeleteNode(obj)
    relationshipsDeleted.increase(count)
    count
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
    result.ifCreated { uniqueConstraintsAdded.increase() }
    result
  }

  override def dropUniqueConstraint(labelId: Int, propertyKeyId: Int) {
    inner.dropUniqueConstraint(labelId, propertyKeyId)
    uniqueConstraintsRemoved.increase()
  }

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int) = {
    val result = inner.createNodePropertyExistenceConstraint(labelId, propertyKeyId)
    result.ifCreated { propertyExistenceConstraintsAdded.increase() }
    result
  }

  override def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int) {
    inner.dropNodePropertyExistenceConstraint(labelId, propertyKeyId)
    propertyExistenceConstraintsRemoved.increase()
  }

  override def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) = {
    val result = inner.createRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId)
    result.ifCreated { propertyExistenceConstraintsAdded.increase() }
    result
  }

  override def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) {
    inner.dropRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId)
    propertyExistenceConstraintsRemoved.increase()
  }

  override def nodeGetDegree(node: Long, dir: SemanticDirection): Int = super.nodeGetDegree(node, dir)

  class Counter {
    val counter: AtomicInteger = new AtomicInteger()

    def count: Int = counter.get()

    def increase(amount: Int = 1) {
      counter.addAndGet(amount)
    }
  }

  private class CountingOps[T <: PropertyContainer](inner: Operations[T], deletes: Counter)
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
