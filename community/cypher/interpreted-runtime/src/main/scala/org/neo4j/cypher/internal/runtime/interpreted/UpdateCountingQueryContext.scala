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
package org.neo4j.cypher.internal.runtime.interpreted

import java.util.concurrent.atomic.AtomicInteger

import org.neo4j.cypher.internal.planner.v3_4.spi.IndexDescriptor
import org.neo4j.cypher.internal.runtime.{Operations, QueryContext, QueryStatistics}
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.{RelationshipValue, NodeValue}

class UpdateCountingQueryContext(inner: QueryContext) extends DelegatingQueryContext(inner) {

  override def createNewQueryContext(): QueryContext = inner.createNewQueryContext()

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
  private val nodekeyConstraintsAdded = new Counter
  private val nodekeyConstraintsRemoved = new Counter

  def getStatistics = QueryStatistics(
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
    existenceConstraintsRemoved = propertyExistenceConstraintsRemoved.count,
    nodekeyConstraintsAdded = nodekeyConstraintsAdded.count,
    nodekeyConstraintsRemoved = nodekeyConstraintsRemoved.count)

  override def getOptStatistics = Some(getStatistics)

  override def createNode() = {
    nodesCreated.increase()
    inner.createNode()
  }

  override def createNodeId() = {
    nodesCreated.increase()
    inner.createNodeId()
  }

  override def nodeOps: Operations[NodeValue] =
    new CountingOps[NodeValue](inner.nodeOps, nodesDeleted)

  override def relationshipOps: Operations[RelationshipValue] =
    new CountingOps[RelationshipValue](inner.relationshipOps, relationshipsDeleted)

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int = {
    val added = inner.setLabelsOnNode(node, labelIds)
    labelsAdded.increase(added)
    added
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

  override def addIndexRule(descriptor: IndexDescriptor) = {
    val result = inner.addIndexRule(descriptor)
    result.ifCreated { indexesAdded.increase() }
    result
  }

  override def dropIndexRule(descriptor: IndexDescriptor) {
    inner.dropIndexRule(descriptor)
    indexesRemoved.increase()
  }

  override def createNodeKeyConstraint(descriptor: IndexDescriptor): Boolean = {
    val result = inner.createNodeKeyConstraint(descriptor)
    if ( result ) nodekeyConstraintsAdded.increase()
    result
  }

  override def dropNodeKeyConstraint(descriptor: IndexDescriptor): Unit = {
    inner.dropNodeKeyConstraint(descriptor)
    nodekeyConstraintsRemoved.increase()
  }

  override def createUniqueConstraint(descriptor: IndexDescriptor): Boolean = {
    val result = inner.createUniqueConstraint(descriptor)
    if ( result ) uniqueConstraintsAdded.increase()
    result
  }

  override def dropUniqueConstraint(descriptor: IndexDescriptor) {
    inner.dropUniqueConstraint(descriptor)
    uniqueConstraintsRemoved.increase()
  }

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): Boolean = {
    val result = inner.createNodePropertyExistenceConstraint(labelId, propertyKeyId)
    if ( result ) propertyExistenceConstraintsAdded.increase()
    result
  }

  override def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int) {
    inner.dropNodePropertyExistenceConstraint(labelId, propertyKeyId)
    propertyExistenceConstraintsRemoved.increase()
  }

  override def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): Boolean = {
    val result = inner.createRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId)
    if ( result ) propertyExistenceConstraintsAdded.increase()
    result
  }

  override def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) {
    inner.dropRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId)
    propertyExistenceConstraintsRemoved.increase()
  }

  override def nodeGetDegree(node: Long, dir: SemanticDirection): Int = super.nodeGetDegree(node, dir)

  override def detachDeleteNode(node: Long): Int = {
    nodesDeleted.increase()
    val count = inner.detachDeleteNode(node)
    relationshipsDeleted.increase(count)
    count
  }

  class Counter {
    val counter: AtomicInteger = new AtomicInteger()

    def count: Int = counter.get()

    def increase(amount: Int = 1) {
      counter.addAndGet(amount)
    }
  }

  private class CountingOps[T](inner: Operations[T], deletes: Counter)
    extends DelegatingOperations[T](inner) {

    override def delete(id: Long) {
      deletes.increase()
      inner.delete(id)
    }

    override def removeProperty(id: Long, propertyKeyId: Int) {
      propertiesSet.increase()
      inner.removeProperty(id, propertyKeyId)
    }

    override def setProperty(id: Long, propertyKeyId: Int, value: Value) {
      propertiesSet.increase()
      inner.setProperty(id, propertyKeyId, value)
    }
  }
}
