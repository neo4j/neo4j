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
package org.neo4j.cypher.internal.runtime.interpreted

import org.eclipse.collections.api.map.primitive.IntObjectMap
import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.ConstraintInfo
import org.neo4j.cypher.internal.runtime.ConstraintInformation
import org.neo4j.cypher.internal.runtime.IndexInfo
import org.neo4j.cypher.internal.runtime.IndexInformation
import org.neo4j.cypher.internal.runtime.NodeOperations
import org.neo4j.cypher.internal.runtime.Operations
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.runtime.RelationshipOperations
import org.neo4j.cypher.internal.runtime.interpreted.CountingQueryContext.Counter
import org.neo4j.cypher.internal.runtime.interpreted.CountingQueryContext.LongCounter
import org.neo4j.internal.kernel.api.MutatingEntityCursor
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.EndpointType
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand
import org.neo4j.internal.schema.SchemaDescriptor
import org.neo4j.internal.schema.constraints.PropertyTypeSet
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

import scala.collection.immutable.ArraySeq

class UpdateCountingQueryContext(inner: QueryContext) extends DelegatingQueryContext(inner) with CountingQueryContext {
  private val nodesCreated = new LongCounter
  private val relationshipsCreated = new LongCounter
  private val propertiesSet = new LongCounter
  private val nodesDeleted = new LongCounter
  private val relationshipsDeleted = new LongCounter
  private val labelsAdded = new LongCounter
  private val labelsRemoved = new LongCounter
  private val indexesAdded = new Counter
  private val indexesRemoved = new Counter
  private val nodePropUniquenessConstraintsAdded = new Counter
  private val relPropUniquenessConstraintsAdded = new Counter
  private val nodePropertyExistenceConstraintsAdded = new Counter
  private val relPropertyExistenceConstraintsAdded = new Counter
  private val nodePropertyTypeConstraintsAdded = new Counter
  private val relPropertyTypeConstraintsAdded = new Counter
  private val nodeKeyConstraintsAdded = new Counter
  private val relKeyConstraintsAdded = new Counter
  private val nodeLabelExistenceConstraintsAdded = new Counter
  private val relSourceLabelConstraintsAdded = new Counter
  private val relTargetLabelConstraintsAdded = new Counter
  private val constraintsRemoved = new Counter

  def getTrackedStatistics: QueryStatistics = QueryStatistics(
    nodesCreated = nodesCreated.count,
    relationshipsCreated = relationshipsCreated.count,
    propertiesSet = propertiesSet.count,
    nodesDeleted = nodesDeleted.count,
    labelsAdded = labelsAdded.count,
    labelsRemoved = labelsRemoved.count,
    relationshipsDeleted = relationshipsDeleted.count,
    indexesAdded = indexesAdded.count,
    indexesRemoved = indexesRemoved.count,
    nodePropUniquenessConstraintsAdded = nodePropUniquenessConstraintsAdded.count,
    relPropUniquenessConstraintsAdded = relPropUniquenessConstraintsAdded.count,
    nodePropExistenceConstraintsAdded = nodePropertyExistenceConstraintsAdded.count,
    relPropExistenceConstraintsAdded = relPropertyExistenceConstraintsAdded.count,
    nodePropTypeConstraintsAdded = nodePropertyTypeConstraintsAdded.count,
    relPropTypeConstraintsAdded = relPropertyTypeConstraintsAdded.count,
    nodekeyConstraintsAdded = nodeKeyConstraintsAdded.count,
    relkeyConstraintsAdded = relKeyConstraintsAdded.count,
    nodeLabelExistenceConstraintsAdded = nodeLabelExistenceConstraintsAdded.count,
    relSourceLabelConstraintsAdded = relSourceLabelConstraintsAdded.count,
    relTargetLabelConstraintsAdded = relTargetLabelConstraintsAdded.count,
    constraintsRemoved = constraintsRemoved.count
  )

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
    nodePropUniquenessConstraintsAdded.increase(statistics.nodePropUniquenessConstraintsAdded)
    relPropUniquenessConstraintsAdded.increase(statistics.relPropUniquenessConstraintsAdded)
    nodePropertyExistenceConstraintsAdded.increase(statistics.nodePropExistenceConstraintsAdded)
    relPropertyExistenceConstraintsAdded.increase(statistics.relPropExistenceConstraintsAdded)
    nodePropertyTypeConstraintsAdded.increase(statistics.nodePropTypeConstraintsAdded)
    relPropertyTypeConstraintsAdded.increase(statistics.relPropTypeConstraintsAdded)
    nodeKeyConstraintsAdded.increase(statistics.nodekeyConstraintsAdded)
    relKeyConstraintsAdded.increase(statistics.relkeyConstraintsAdded)
    nodeLabelExistenceConstraintsAdded.increase(statistics.nodeLabelExistenceConstraintsAdded)
    relSourceLabelConstraintsAdded.increase(statistics.relSourceLabelConstraintsAdded)
    relTargetLabelConstraintsAdded.increase(statistics.relTargetLabelConstraintsAdded)
    constraintsRemoved.increase(statistics.constraintsRemoved)
    inner.addStatistics(statistics)
  }

  override def onMutation(nodes: Int, relationships: Int, labels: Int, properties: Int): Unit = {
    nodesCreated.increase(nodes)
    relationshipsCreated.increase(relationships)
    labelsAdded.increase(labels)
    propertiesSet.increase(properties)
  }

  override def createNodeId(labels: Array[Int]): Long = {
    nodesCreated.increase()
    labelsAdded.increase(labels.length)
    inner.createNodeId(labels)
  }

  override val nodeWriteOps: NodeOperations =
    new CountingOps[VirtualNodeValue, NodeCursor](inner.nodeWriteOps, nodesDeleted) with NodeOperations

  override val relationshipWriteOps: RelationshipOperations =
    new CountingOps[VirtualRelationshipValue, RelationshipScanCursor](inner.relationshipWriteOps, relationshipsDeleted)
      with RelationshipOperations

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int = {
    val added = inner.setLabelsOnNode(node, labelIds)
    labelsAdded.increase(added)
    added
  }

  override def createRelationshipId(start: Long, end: Long, relType: Int): Long = {
    relationshipsCreated.increase()
    inner.createRelationshipId(start, end, relType)
  }

  override def mergeInto(
    nodeCursor: NodeCursor,
    traversalCursor: RelationshipTraversalCursor,
    propertyCursor: PropertyCursor,
    source: Long,
    relType: Int,
    direction: SemanticDirection,
    target: Long,
    onMatch: IntObjectMap[Value],
    onCreate: IntObjectMap[Value]
  ): MutatingEntityCursor = {
    inner.mergeInto(
      nodeCursor,
      traversalCursor,
      propertyCursor,
      source,
      relType,
      direction,
      target,
      onMatch,
      onCreate
    )
  }

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int = {
    val removed = inner.removeLabelsFromNode(node, labelIds)
    labelsRemoved.increase(removed)
    removed
  }

  override def addRangeIndexRule(
    entityId: Int,
    entityType: EntityType,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): IndexDescriptor = {
    val result = inner.addRangeIndexRule(entityId, entityType, propertyKeyIds, name, provider)
    indexesAdded.increase()
    result
  }

  override def addLookupIndexRule(
    entityType: EntityType,
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): IndexDescriptor = {
    val result = inner.addLookupIndexRule(entityType, name, provider)
    indexesAdded.increase()
    result
  }

  override def addFulltextIndexRule(
    entityIds: List[Int],
    entityType: EntityType,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor],
    indexConfig: IndexConfig
  ): IndexDescriptor = {
    val result = inner.addFulltextIndexRule(entityIds, entityType, propertyKeyIds, name, provider, indexConfig)
    indexesAdded.increase()
    result
  }

  override def addTextIndexRule(
    entityId: Int,
    entityType: EntityType,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): IndexDescriptor = {
    val result = inner.addTextIndexRule(entityId, entityType, propertyKeyIds, name, provider)
    indexesAdded.increase()
    result
  }

  override def addPointIndexRule(
    entityId: Int,
    entityType: EntityType,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor],
    indexConfig: IndexConfig
  ): IndexDescriptor = {
    val result = inner.addPointIndexRule(entityId, entityType, propertyKeyIds, name, provider, indexConfig)
    indexesAdded.increase()
    result
  }

  override def addVectorIndexRule(
    entityIds: List[Int],
    entityType: EntityType,
    propertyKeyIds: Seq[Int],
    additionalPropertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor],
    indexConfig: IndexConfig
  ): IndexDescriptor = {
    val result = inner.addVectorIndexRule(
      entityIds,
      entityType,
      propertyKeyIds,
      additionalPropertyKeyIds,
      name,
      provider,
      indexConfig
    )
    indexesAdded.increase()
    result
  }

  override def dropIndexRule(name: String): Unit = {
    inner.dropIndexRule(name)
    indexesRemoved.increase()
  }

  override def getAllIndexes(): Map[IndexDescriptor, IndexInfo] = {
    inner.getAllIndexes()
  }

  override def getIndexInformation(name: String): IndexInformation =
    inner.getIndexInformation(name)

  override def getIndexInformation(index: IndexDescriptor): IndexInformation =
    inner.getIndexInformation(index)

  override def indexExists(name: String): Boolean = {
    inner.indexExists(name)
  }

  override def constraintExists(name: String): Boolean = {
    inner.constraintExists(name)
  }

  override def constraintExists(matchFn: ConstraintDescriptor => Boolean, entityId: Int, properties: Int*): Boolean = {
    inner.constraintExists(matchFn, entityId, properties: _*)
  }

  override def createConstraint(constraint: ConstraintCommand.Create): Unit = {
    inner.createConstraint(constraint)
    constraint match {
      case _: ConstraintCommand.Create.NodeKey                  => nodeKeyConstraintsAdded.increase()
      case _: ConstraintCommand.Create.RelationshipKey          => relKeyConstraintsAdded.increase()
      case _: ConstraintCommand.Create.NodeUniqueness           => nodePropUniquenessConstraintsAdded.increase()
      case _: ConstraintCommand.Create.RelationshipUniqueness   => relPropUniquenessConstraintsAdded.increase()
      case _: ConstraintCommand.Create.NodeExistence            => nodePropertyExistenceConstraintsAdded.increase()
      case _: ConstraintCommand.Create.RelationshipExistence    => relPropertyExistenceConstraintsAdded.increase()
      case _: ConstraintCommand.Create.NodePropertyType         => nodePropertyTypeConstraintsAdded.increase()
      case _: ConstraintCommand.Create.RelationshipPropertyType => relPropertyTypeConstraintsAdded.increase()
      case _: ConstraintCommand.Create.NodeLabelExistence       => nodeLabelExistenceConstraintsAdded.increase()
      case c: ConstraintCommand.Create.RelationshipEndpointLabel =>
        if (c.endpointType() == EndpointType.START) relSourceLabelConstraintsAdded.increase()
        if (c.endpointType() == EndpointType.END) relTargetLabelConstraintsAdded.increase()
    }
  }

  override def createNodeKeyConstraint(
    labelId: Int,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): Unit = {
    inner.createNodeKeyConstraint(labelId, propertyKeyIds, name, provider)
    nodeKeyConstraintsAdded.increase()
  }

  override def createRelationshipKeyConstraint(
    relTypeId: Int,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): Unit = {
    inner.createRelationshipKeyConstraint(relTypeId, propertyKeyIds, name, provider)
    relKeyConstraintsAdded.increase()
  }

  override def createNodeUniqueConstraint(
    labelId: Int,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): Unit = {
    inner.createNodeUniqueConstraint(labelId, propertyKeyIds, name, provider)
    nodePropUniquenessConstraintsAdded.increase()
  }

  override def createRelationshipUniqueConstraint(
    relTypeId: Int,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): Unit = {
    inner.createRelationshipUniqueConstraint(relTypeId, propertyKeyIds, name, provider)
    relPropUniquenessConstraintsAdded.increase()
  }

  override def createNodePropertyExistenceConstraint(
    labelId: Int,
    propertyKeyId: Int,
    name: Option[String],
    dependent: Boolean
  ): Unit = {
    inner.createNodePropertyExistenceConstraint(labelId, propertyKeyId, name, dependent)
    nodePropertyExistenceConstraintsAdded.increase()
  }

  override def createRelationshipPropertyExistenceConstraint(
    relTypeId: Int,
    propertyKeyId: Int,
    name: Option[String],
    dependent: Boolean
  ): Unit = {
    inner.createRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId, name, dependent)
    relPropertyExistenceConstraintsAdded.increase()
  }

  override def createNodePropertyTypeConstraint(
    labelId: Int,
    propertyKeyId: Int,
    propertyTypes: PropertyTypeSet,
    name: Option[String],
    dependent: Boolean
  ): Unit = {
    inner.createNodePropertyTypeConstraint(labelId, propertyKeyId, propertyTypes, name, dependent)
    nodePropertyTypeConstraintsAdded.increase()
  }

  override def createRelationshipPropertyTypeConstraint(
    relTypeId: Int,
    propertyKeyId: Int,
    propertyTypes: PropertyTypeSet,
    name: Option[String],
    dependent: Boolean
  ): Unit = {
    inner.createRelationshipPropertyTypeConstraint(relTypeId, propertyKeyId, propertyTypes, name, dependent)
    relPropertyTypeConstraintsAdded.increase()
  }

  override def createLabelExistenceConstraint(labelId: Int, impliedLabelId: Int): Unit = {
    inner.createLabelExistenceConstraint(labelId, impliedLabelId)
    nodeLabelExistenceConstraintsAdded.increase()
  }

  override def createRelationshipSourceLabelConstraint(relTypeId: Int, labelId: Int): Unit = {
    inner.createRelationshipSourceLabelConstraint(relTypeId, labelId)
    relSourceLabelConstraintsAdded.increase()
  }

  override def createRelationshipTargetLabelConstraint(relTypeId: Int, labelId: Int): Unit = {
    inner.createRelationshipTargetLabelConstraint(relTypeId, labelId)
    relTargetLabelConstraintsAdded.increase()
  }

  override def dropNamedConstraint(name: String, allowDependent: Boolean): Unit = {
    inner.dropNamedConstraint(name, allowDependent)
    constraintsRemoved.increase()
  }

  override def getConstraintInformation(name: String): ConstraintInformation = inner.getConstraintInformation(name)

  override def getConstraintInformation(
    matchFn: ConstraintDescriptor => Boolean,
    entityId: Int,
    properties: Int*
  ): ConstraintInformation =
    inner.getConstraintInformation(matchFn, entityId, properties: _*)

  override def getAllConstraints(): Map[ConstraintDescriptor, ConstraintInfo] = inner.getAllConstraints()

  override def getGeneratedNameForConstraint(
    forNode: Boolean,
    entityId: Int,
    propertyIds: ArraySeq[Int],
    descriptor: SchemaDescriptor => ConstraintDescriptor
  ): String = inner.getGeneratedNameForConstraint(forNode, entityId, propertyIds, descriptor)

  override def nodeGetDegree(node: Long, dir: SemanticDirection, nodeCursor: NodeCursor): Long =
    super.nodeGetDegree(node, dir, nodeCursor)

  override def detachDeleteNode(node: Long): Int = {
    nodesDeleted.increase() // This relies on the assumption that the node was not already deleted
    val count = inner.detachDeleteNode(node)
    relationshipsDeleted.increase(count)
    count
  }

  override def contextWithNewTransaction(): UpdateCountingQueryContext =
    new UpdateCountingQueryContext(inner.contextWithNewTransaction())

  private class CountingOps[T, CURSOR](inner: Operations[T, CURSOR], deletes: LongCounter)
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

    override def setProperties(obj: Long, properties: IntObjectMap[Value]): Unit = {

      propertiesSet.increase(properties.size())
      inner.setProperties(obj, properties)
    }
  }
}
