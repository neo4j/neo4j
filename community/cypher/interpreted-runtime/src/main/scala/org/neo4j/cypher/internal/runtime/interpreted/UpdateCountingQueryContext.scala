/*
 * Copyright (c) "Neo4j"
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


import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.ConstraintInfo
import org.neo4j.cypher.internal.runtime.IndexInfo
import org.neo4j.cypher.internal.runtime.NodeOperations
import org.neo4j.cypher.internal.runtime.Operations
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.runtime.RelationshipOperations
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.RelationshipValue

import java.util.concurrent.atomic.AtomicInteger

class UpdateCountingQueryContext(inner: QueryContext) extends DelegatingQueryContext(inner) with CountingQueryContext {

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
  private val namedConstraintsRemoved = new Counter

  def getStatistics: QueryStatistics = QueryStatistics(
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
    nodekeyConstraintsRemoved = nodekeyConstraintsRemoved.count,
    namedConstraintsRemoved = namedConstraintsRemoved.count)

  override def getOptStatistics: Option[QueryStatistics] = Some(getStatistics)

  override def addStatistics(statistics: QueryStatistics): Unit = {
    nodesCreated.increase(statistics.nodesCreated)
    relationshipsCreated.increase(statistics.relationshipsCreated)
    propertiesSet.increase(statistics.propertiesSet)
    nodesDeleted.increase(statistics.nodesDeleted)
    labelsAdded.increase(statistics.labelsAdded)
    labelsRemoved.increase(statistics.labelsRemoved)
    relationshipsDeleted.increase(statistics.relationshipsDeleted)
    indexesAdded.increase(statistics.indexesAdded)
    indexesRemoved.increase(statistics.indexesRemoved)
    uniqueConstraintsAdded.increase(statistics.uniqueConstraintsAdded)
    uniqueConstraintsRemoved.increase(statistics.uniqueConstraintsRemoved)
    propertyExistenceConstraintsAdded.increase(statistics.existenceConstraintsAdded)
    propertyExistenceConstraintsRemoved.increase(statistics.existenceConstraintsRemoved)
    nodekeyConstraintsAdded.increase(statistics.nodekeyConstraintsAdded)
    nodekeyConstraintsRemoved.increase(statistics.nodekeyConstraintsRemoved)
    super.addStatistics(statistics)
  }

  override def createNode(labels: Array[Int]): NodeValue = {
    nodesCreated.increase()
    labelsAdded.increase(labels.length)
    inner.createNode(labels)
  }

  override def createNodeId(labels: Array[Int]): Long = {
    nodesCreated.increase()
    labelsAdded.increase(labels.length)
    inner.createNodeId(labels)
  }

  override val nodeOps: NodeOperations =
    new CountingOps[NodeValue, NodeCursor](inner.nodeOps, nodesDeleted) with NodeOperations

  override val relationshipOps: RelationshipOperations =
    new CountingOps[RelationshipValue, RelationshipScanCursor](inner.relationshipOps, relationshipsDeleted) with RelationshipOperations

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int = {
    val added = inner.setLabelsOnNode(node, labelIds)
    labelsAdded.increase(added)
    added
  }

  override def createRelationship(start: Long, end: Long, relType: Int): RelationshipValue = {
    relationshipsCreated.increase()
    inner.createRelationship(start, end, relType)
  }

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int = {
    val removed = inner.removeLabelsFromNode(node, labelIds)
    labelsRemoved.increase(removed)
    removed
  }

  override def addBtreeIndexRule(entityId: Int, entityType: EntityType, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[String], indexConfig: IndexConfig): IndexDescriptor = {
    val result = inner.addBtreeIndexRule(entityId, entityType, propertyKeyIds, name, provider, indexConfig)
    indexesAdded.increase()
    result
  }

  override def addRangeIndexRule(entityId: Int, entityType: EntityType, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[IndexProviderDescriptor]): IndexDescriptor = {
    val result = inner.addRangeIndexRule(entityId, entityType, propertyKeyIds, name, provider)
    indexesAdded.increase()
    result
  }

  override def addLookupIndexRule(entityType: EntityType, name: Option[String], provider: Option[IndexProviderDescriptor]): IndexDescriptor = {
    val result = inner.addLookupIndexRule(entityType, name, provider)
    indexesAdded.increase()
    result
  }

  override def addFulltextIndexRule(entityIds: List[Int], entityType: EntityType, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[IndexProviderDescriptor], indexConfig: IndexConfig): IndexDescriptor = {
    val result = inner.addFulltextIndexRule(entityIds, entityType, propertyKeyIds, name, provider, indexConfig)
    indexesAdded.increase()
    result
  }

  override def addTextIndexRule(entityId: Int, entityType: EntityType, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[IndexProviderDescriptor]): IndexDescriptor = {
    val result = inner.addTextIndexRule(entityId, entityType, propertyKeyIds, name, provider)
    indexesAdded.increase()
    result
  }

  override def dropIndexRule(labelId: Int, propertyKeyIds: Seq[Int]): Unit = {
    inner.dropIndexRule(labelId, propertyKeyIds)
    indexesRemoved.increase()
  }

  override def dropIndexRule(name: String): Unit = {
    inner.dropIndexRule(name)
    indexesRemoved.increase()
  }

  override def getAllIndexes(): Map[IndexDescriptor, IndexInfo] = {
    inner.getAllIndexes()
  }

  override def indexExists(name: String): Boolean = {
    inner.indexExists(name)
  }

  override def constraintExists(name: String): Boolean = {
    inner.constraintExists(name)
  }

  override def constraintExists(matchFn: ConstraintDescriptor => Boolean, entityId: Int, properties: Int*): Boolean = {
    inner.constraintExists(matchFn, entityId, properties: _*)
  }

  override def createNodeKeyConstraint(labelId: Int, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[String], indexConfig: IndexConfig): Unit = {
    inner.createNodeKeyConstraint(labelId, propertyKeyIds, name, provider, indexConfig)
    nodekeyConstraintsAdded.increase()
  }

  override def dropNodeKeyConstraint(labelId: Int, propertyKeyIds: Seq[Int]): Unit = {
    inner.dropNodeKeyConstraint(labelId, propertyKeyIds)
    nodekeyConstraintsRemoved.increase()
  }

  override def createUniqueConstraint(labelId: Int, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[String], indexConfig: IndexConfig): Unit = {
    inner.createUniqueConstraint(labelId, propertyKeyIds, name, provider, indexConfig)
    uniqueConstraintsAdded.increase()
  }

  override def dropUniqueConstraint(labelId: Int, propertyKeyIds: Seq[Int]): Unit = {
    inner.dropUniqueConstraint(labelId, propertyKeyIds)
    uniqueConstraintsRemoved.increase()
  }

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int, name: Option[String]): Unit = {
    inner.createNodePropertyExistenceConstraint(labelId, propertyKeyId, name)
    propertyExistenceConstraintsAdded.increase()
  }

  override def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): Unit = {
    inner.dropNodePropertyExistenceConstraint(labelId, propertyKeyId)
    propertyExistenceConstraintsRemoved.increase()
  }

  override def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int, name: Option[String]): Unit = {
    inner.createRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId, name)
    propertyExistenceConstraintsAdded.increase()
  }

  override def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): Unit = {
    inner.dropRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId)
    propertyExistenceConstraintsRemoved.increase()
  }

  override def dropNamedConstraint(name: String): Unit = {
    inner.dropNamedConstraint(name)
    namedConstraintsRemoved.increase()
  }

  override def getAllConstraints(): Map[ConstraintDescriptor, ConstraintInfo] = inner.getAllConstraints()

  override def nodeGetDegree(node: Long, dir: SemanticDirection, nodeCursor: NodeCursor): Int = super.nodeGetDegree(node, dir, nodeCursor)

  override def detachDeleteNode(node: Long): Int = {
    nodesDeleted.increase() // This relies on the assumption that the node was not already deleted
    val count = inner.detachDeleteNode(node)
    relationshipsDeleted.increase(count)
    count
  }

  override def contextWithNewTransaction(): UpdateCountingQueryContext = new UpdateCountingQueryContext(inner.contextWithNewTransaction())

  class Counter {
    val counter: AtomicInteger = new AtomicInteger()

    def count: Int = counter.get()

    def increase(amount: Int = 1): Unit = {
      counter.addAndGet(amount)
    }
  }

  private class CountingOps[T, CURSOR](inner: Operations[T, CURSOR], deletes: Counter)
    extends DelegatingOperations[T, CURSOR](inner) {

    override def delete(id: Long): Boolean = {
      if (inner.delete(id)) {
        deletes.increase()
        true
      } else {
        false
      }
    }

    override def removeProperty(id: Long, propertyKeyId: Int): Boolean = {
      val wasRemoved = inner.removeProperty(id, propertyKeyId)
      if (wasRemoved) {
        propertiesSet.increase()
      }
      wasRemoved
    }

    override def setProperty(id: Long, propertyKeyId: Int, value: Value): Unit = {
      propertiesSet.increase()
      inner.setProperty(id, propertyKeyId, value)
    }
  }
}
