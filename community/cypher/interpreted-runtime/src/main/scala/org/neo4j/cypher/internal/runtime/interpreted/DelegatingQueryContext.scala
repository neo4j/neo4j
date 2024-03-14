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
import org.eclipse.collections.api.set.primitive.IntSet
import org.neo4j.common.EntityType
import org.neo4j.configuration.Config
import org.neo4j.csv.reader.CharReadable
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.ConstraintInfo
import org.neo4j.cypher.internal.runtime.ConstraintInformation
import org.neo4j.cypher.internal.runtime.EntityTransformer
import org.neo4j.cypher.internal.runtime.IndexInfo
import org.neo4j.cypher.internal.runtime.IndexInformation
import org.neo4j.cypher.internal.runtime.NodeOperations
import org.neo4j.cypher.internal.runtime.NodeReadOperations
import org.neo4j.cypher.internal.runtime.Operations
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.runtime.QueryTransactionalContext
import org.neo4j.cypher.internal.runtime.ReadOperations
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.RelationshipOperations
import org.neo4j.cypher.internal.runtime.RelationshipReadOperations
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.graphdb.Entity
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.internal.kernel.api
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.Locks
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.Procedures
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.PropertyIndexQuery
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.internal.kernel.api.SchemaRead
import org.neo4j.internal.kernel.api.SchemaWrite
import org.neo4j.internal.kernel.api.Token
import org.neo4j.internal.kernel.api.TokenRead
import org.neo4j.internal.kernel.api.TokenReadSession
import org.neo4j.internal.kernel.api.TokenWrite
import org.neo4j.internal.kernel.api.Write
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer
import org.neo4j.internal.kernel.api.security.AccessMode
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.internal.schema.constraints.PropertyTypeSet
import org.neo4j.io.pagecache.context.CursorContext
import org.neo4j.kernel.api.ExecutionContext
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.index.IndexUsageStats
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.impl.factory.DbmsInfo
import org.neo4j.kernel.impl.query.ConstituentTransactionFactory
import org.neo4j.kernel.impl.query.FunctionInformation
import org.neo4j.kernel.impl.query.statistic.StatisticProvider
import org.neo4j.logging.InternalLogProvider
import org.neo4j.memory.MemoryTracker
import org.neo4j.scheduler.JobScheduler
import org.neo4j.values.AnyValue
import org.neo4j.values.ElementIdMapper
import org.neo4j.values.ValueMapper
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.VirtualRelationshipValue

import java.net.URI

abstract class DelegatingQueryContext(val inner: QueryContext) extends QueryContext {

  protected def singleDbHit[A](value: A): A = value
  protected def unknownDbHits[A](value: A): A = value
  protected def manyDbHits[A](value: ClosingIterator[A]): ClosingIterator[A] = value

  protected def manyDbHits(value: ClosingLongIterator): ClosingLongIterator = value
  protected def manyDbHits(value: RelationshipIterator): RelationshipIterator = value

  protected def manyDbHitsCliRi(value: ClosingLongIterator with RelationshipIterator)
    : ClosingLongIterator with RelationshipIterator = value
  protected def manyDbHits(value: RelationshipTraversalCursor): RelationshipTraversalCursor = value
  protected def manyDbHits(value: NodeValueIndexCursor): NodeValueIndexCursor = value
  protected def manyDbHits(value: RelationshipValueIndexCursor): RelationshipValueIndexCursor = value
  protected def manyDbHits(value: NodeCursor): NodeCursor = value
  protected def manyDbHits(value: NodeLabelIndexCursor): NodeLabelIndexCursor = value
  protected def manyDbHits(value: RelationshipTypeIndexCursor): RelationshipTypeIndexCursor = value
  protected def manyDbHits(value: RelationshipScanCursor): RelationshipScanCursor = value
  protected def manyDbHits(value: PropertyCursor): PropertyCursor = value
  protected def manyDbHits(count: Int): Int = count

  override def resources: ResourceManager = inner.resources

  override def transactionalContext: QueryTransactionalContext = inner.transactionalContext

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int =
    singleDbHit(inner.setLabelsOnNode(node, labelIds))

  override def createNodeId(labels: Array[Int]): Long = singleDbHit(inner.createNodeId(labels))

  override def createRelationshipId(start: Long, end: Long, relType: Int): Long =
    singleDbHit(inner.createRelationshipId(start, end, relType))

  override def getOrCreateRelTypeId(relTypeName: String): Int = singleDbHit(inner.getOrCreateRelTypeId(relTypeName))

  override def getLabelsForNode(node: Long, nodeCursor: NodeCursor): ListValue =
    singleDbHit(inner.getLabelsForNode(node, nodeCursor))

  override def getTypeForRelationship(id: Long, cursor: RelationshipScanCursor): AnyValue =
    singleDbHit(inner.getTypeForRelationship(id, cursor))

  override def getLabelName(id: Int): String = singleDbHit(inner.getLabelName(id))

  override def getOptLabelId(labelName: String): Option[Int] = singleDbHit(inner.getOptLabelId(labelName))

  override def getLabelId(labelName: String): Int = singleDbHit(inner.getLabelId(labelName))

  override def getOrCreateLabelId(labelName: String): Int = singleDbHit(inner.getOrCreateLabelId(labelName))

  override def getOrCreateTypeId(relTypeName: String): Int = singleDbHit(inner.getOrCreateTypeId(relTypeName))

  override def getRelationshipsForIds(
    node: Long,
    dir: SemanticDirection,
    types: Array[Int]
  ): ClosingLongIterator with RelationshipIterator =
    manyDbHitsCliRi(inner.getRelationshipsForIds(node, dir, types))

  override def getRelationshipsByType(
    tokenReadSession: TokenReadSession,
    relType: Int,
    indexOrder: IndexOrder
  ): ClosingLongIterator with RelationshipIterator =
    manyDbHitsCliRi(inner.getRelationshipsByType(tokenReadSession, relType, indexOrder))

  override def nodeCursor(): NodeCursor = manyDbHits(inner.nodeCursor())

  override def nodeLabelIndexCursor(): NodeLabelIndexCursor = manyDbHits(inner.nodeLabelIndexCursor())

  override def relationshipTypeIndexCursor(): RelationshipTypeIndexCursor =
    manyDbHits(inner.relationshipTypeIndexCursor())

  override def traversalCursor(): RelationshipTraversalCursor = manyDbHits(inner.traversalCursor())

  override def scanCursor(): RelationshipScanCursor = manyDbHits(inner.scanCursor())

  override def singleNode(id: Long, cursor: NodeCursor): Unit = inner.singleNode(id, cursor)

  override def singleRelationship(id: Long, cursor: RelationshipScanCursor): Unit = inner.singleRelationship(id, cursor)

  override def relationshipById(
    relationshipId: Long,
    startNodeId: Long,
    endNodeId: Long,
    typeId: Int
  ): VirtualRelationshipValue =
    inner.relationshipById(relationshipId, startNodeId, endNodeId, typeId)

  override def nodeReadOps: NodeReadOperations = inner.nodeReadOps

  override def relationshipReadOps: RelationshipReadOperations = inner.relationshipReadOps

  override def nodeWriteOps: NodeOperations = inner.nodeWriteOps

  override def relationshipWriteOps: RelationshipOperations = inner.relationshipWriteOps

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int =
    singleDbHit(inner.removeLabelsFromNode(node, labelIds))

  override def getPropertyKeyName(propertyKeyId: Int): String = singleDbHit(inner.getPropertyKeyName(propertyKeyId))

  override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] =
    singleDbHit(inner.getOptPropertyKeyId(propertyKeyName))

  override def getPropertyKeyId(propertyKey: String): Int = singleDbHit(inner.getPropertyKeyId(propertyKey))

  override def getOrCreatePropertyKeyId(propertyKey: String): Int =
    singleDbHit(inner.getOrCreatePropertyKeyId(propertyKey))

  override def getOrCreatePropertyKeyIds(propertyKeys: Array[String]): Array[Int] = {
    manyDbHits(propertyKeys.length)
    inner.getOrCreatePropertyKeyIds(propertyKeys)
  }

  override def validateIndexProvider(
    schemaDescription: String,
    providerString: String,
    indexType: IndexType
  ): IndexProviderDescriptor =
    singleDbHit(inner.validateIndexProvider(schemaDescription, providerString, indexType))

  override def addRangeIndexRule(
    entityId: Int,
    entityType: EntityType,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): IndexDescriptor =
    singleDbHit(inner.addRangeIndexRule(entityId, entityType, propertyKeyIds, name, provider))

  override def addLookupIndexRule(
    entityType: EntityType,
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): IndexDescriptor =
    singleDbHit(inner.addLookupIndexRule(entityType, name, provider))

  override def addFulltextIndexRule(
    entityIds: List[Int],
    entityType: EntityType,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor],
    indexConfig: IndexConfig
  ): IndexDescriptor =
    singleDbHit(inner.addFulltextIndexRule(entityIds, entityType, propertyKeyIds, name, provider, indexConfig))

  override def addTextIndexRule(
    entityId: Int,
    entityType: EntityType,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): IndexDescriptor =
    singleDbHit(inner.addTextIndexRule(entityId, entityType, propertyKeyIds, name, provider))

  override def addPointIndexRule(
    entityId: Int,
    entityType: EntityType,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor],
    indexConfig: IndexConfig
  ): IndexDescriptor =
    singleDbHit(inner.addPointIndexRule(entityId, entityType, propertyKeyIds, name, provider, indexConfig))

  override def addVectorIndexRule(
    entityId: Int,
    entityType: EntityType,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor],
    indexConfig: IndexConfig
  ): IndexDescriptor =
    singleDbHit(inner.addVectorIndexRule(entityId, entityType, propertyKeyIds, name, provider, indexConfig))

  override def dropIndexRule(name: String): Unit = singleDbHit(inner.dropIndexRule(name))

  override def getAllIndexes(): Map[IndexDescriptor, IndexInfo] = singleDbHit(inner.getAllIndexes())

  override def getIndexUsageStatistics(index: IndexDescriptor): IndexUsageStats =
    singleDbHit(inner.getIndexUsageStatistics(index))

  override def getIndexInformation(name: String): IndexInformation =
    singleDbHit(inner.getIndexInformation(name))

  override def getIndexInformation(index: IndexDescriptor): IndexInformation =
    singleDbHit(inner.getIndexInformation(index))

  override def indexExists(name: String): Boolean = singleDbHit(inner.indexExists(name))

  override def constraintExists(name: String): Boolean = singleDbHit(inner.constraintExists(name))

  override def constraintExists(matchFn: ConstraintDescriptor => Boolean, entityId: Int, properties: Int*): Boolean =
    singleDbHit(inner.constraintExists(matchFn, entityId, properties: _*))

  override def indexReference(
    indexType: IndexType,
    entityId: Int,
    entityType: EntityType,
    properties: Int*
  ): IndexDescriptor = singleDbHit(inner.indexReference(indexType, entityId, entityType, properties: _*))

  override def lookupIndexReference(entityType: EntityType): IndexDescriptor =
    singleDbHit(inner.lookupIndexReference(entityType))

  override def fulltextIndexReference(entityIds: List[Int], entityType: EntityType, properties: Int*): IndexDescriptor =
    singleDbHit(inner.fulltextIndexReference(entityIds, entityType, properties: _*))

  override def nodeIndexSeek(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder,
    queries: Seq[PropertyIndexQuery]
  ): NodeValueIndexCursor =
    manyDbHits(inner.nodeIndexSeek(index, needsValues, indexOrder, queries))

  override def nodeIndexScan(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder
  ): NodeValueIndexCursor =
    manyDbHits(inner.nodeIndexScan(index, needsValues, indexOrder))

  override def nodeIndexSeekByContains(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder,
    value: TextValue
  ): NodeValueIndexCursor =
    manyDbHits(inner.nodeIndexSeekByContains(index, needsValues, indexOrder, value))

  override def nodeIndexSeekByEndsWith(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder,
    value: TextValue
  ): NodeValueIndexCursor =
    manyDbHits(inner.nodeIndexSeekByEndsWith(index, needsValues, indexOrder, value))

  override def relationshipIndexSeek(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder,
    queries: Seq[PropertyIndexQuery]
  ): RelationshipValueIndexCursor =
    manyDbHits(inner.relationshipIndexSeek(index, needsValues, indexOrder, queries))

  override def relationshipLockingUniqueIndexSeek(
    index: IndexDescriptor,
    queries: Seq[PropertyIndexQuery.ExactPredicate]
  ): RelationshipValueIndexCursor =
    singleDbHit(inner.relationshipLockingUniqueIndexSeek(index, queries))

  override def relationshipIndexSeekByContains(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder,
    value: TextValue
  ): RelationshipValueIndexCursor =
    manyDbHits(inner.relationshipIndexSeekByContains(index, needsValues, indexOrder, value))

  override def relationshipIndexSeekByEndsWith(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder,
    value: TextValue
  ): RelationshipValueIndexCursor =
    manyDbHits(inner.relationshipIndexSeekByEndsWith(index, needsValues, indexOrder, value))

  override def relationshipIndexScan(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder
  ): RelationshipValueIndexCursor =
    manyDbHits(inner.relationshipIndexScan(index, needsValues, indexOrder))

  override def getNodesByLabel(
    tokenReadSession: TokenReadSession,
    id: Int,
    indexOrder: IndexOrder
  ): ClosingLongIterator =
    manyDbHits(inner.getNodesByLabel(tokenReadSession, id, indexOrder))

  override def nodeAsMap(
    id: Long,
    nodeCursor: NodeCursor,
    propertyCursor: PropertyCursor,
    builder: MapValueBuilder,
    seenTokens: IntSet
  ): MapValue = {
    val map = inner.nodeAsMap(id, nodeCursor, propertyCursor, builder, seenTokens)
    // one hit finding the node, then finding the properties
    manyDbHits(1 + map.size() - seenTokens.size())
    map
  }

  override def relationshipAsMap(
    id: Long,
    relationshipCursor: RelationshipScanCursor,
    propertyCursor: PropertyCursor,
    builder: MapValueBuilder,
    seenTokens: IntSet
  ): MapValue = {
    val map = inner.relationshipAsMap(id, relationshipCursor, propertyCursor, builder, seenTokens)
    manyDbHits(1 + map.size())
    map
  }

  override def relationshipAsMap(
    relationship: VirtualRelationshipValue,
    relationshipCursor: RelationshipScanCursor,
    propertyCursor: PropertyCursor,
    builder: MapValueBuilder,
    seenTokens: IntSet
  ): MapValue = {
    val map = inner.relationshipAsMap(relationship, relationshipCursor, propertyCursor, builder, seenTokens)
    manyDbHits(1 + map.size())
    map
  }

  override def createNodeKeyConstraint(
    labelId: Int,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): Unit =
    singleDbHit(inner.createNodeKeyConstraint(labelId, propertyKeyIds, name, provider))

  override def createRelationshipKeyConstraint(
    relTypeId: Int,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): Unit =
    singleDbHit(inner.createRelationshipKeyConstraint(relTypeId, propertyKeyIds, name, provider))

  override def createNodeUniqueConstraint(
    labelId: Int,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): Unit =
    singleDbHit(inner.createNodeUniqueConstraint(labelId, propertyKeyIds, name, provider))

  override def createRelationshipUniqueConstraint(
    relTypeId: Int,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): Unit =
    singleDbHit(inner.createRelationshipUniqueConstraint(relTypeId, propertyKeyIds, name, provider))

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int, name: Option[String]): Unit =
    singleDbHit(inner.createNodePropertyExistenceConstraint(labelId, propertyKeyId, name))

  override def createRelationshipPropertyExistenceConstraint(
    relTypeId: Int,
    propertyKeyId: Int,
    name: Option[String]
  ): Unit =
    singleDbHit(inner.createRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId, name))

  override def createNodePropertyTypeConstraint(
    labelId: Int,
    propertyKeyId: Int,
    propertyTypes: PropertyTypeSet,
    name: Option[String]
  ): Unit =
    singleDbHit(inner.createNodePropertyTypeConstraint(labelId, propertyKeyId, propertyTypes, name))

  override def createRelationshipPropertyTypeConstraint(
    relTypeId: Int,
    propertyKeyId: Int,
    propertyTypes: PropertyTypeSet,
    name: Option[String]
  ): Unit =
    singleDbHit(inner.createRelationshipPropertyTypeConstraint(relTypeId, propertyKeyId, propertyTypes, name))

  override def dropNamedConstraint(name: String): Unit =
    singleDbHit(inner.dropNamedConstraint(name))

  override def getConstraintInformation(name: String): ConstraintInformation =
    singleDbHit(inner.getConstraintInformation(name))

  override def getConstraintInformation(
    matchFn: ConstraintDescriptor => Boolean,
    entityId: Int,
    properties: Int*
  ): ConstraintInformation =
    singleDbHit(inner.getConstraintInformation(matchFn, entityId, properties: _*))

  override def getAllConstraints(): Map[ConstraintDescriptor, ConstraintInfo] = singleDbHit(inner.getAllConstraints())

  override def nodeLockingUniqueIndexSeek(
    index: IndexDescriptor,
    queries: Seq[PropertyIndexQuery.ExactPredicate]
  ): NodeValueIndexCursor =
    singleDbHit(inner.nodeLockingUniqueIndexSeek(index, queries))

  override def getRelTypeId(relType: String): Int = singleDbHit(inner.getRelTypeId(relType))

  override def getOptRelTypeId(relType: String): Option[Int] = singleDbHit(inner.getOptRelTypeId(relType))

  override def getRelTypeName(id: Int): String = singleDbHit(inner.getRelTypeName(id))

  override def getImportDataConnection(uri: URI): CharReadable = inner.getImportDataConnection(uri)

  override def nodeGetOutgoingDegreeWithMax(maxDegree: Int, node: Long, nodeCursor: NodeCursor): Int =
    singleDbHit(inner.nodeGetOutgoingDegreeWithMax(maxDegree, node, nodeCursor))

  override def nodeGetOutgoingDegreeWithMax(
    maxDegree: Int,
    node: Long,
    relationship: Int,
    nodeCursor: NodeCursor
  ): Int = singleDbHit(inner.nodeGetOutgoingDegreeWithMax(maxDegree, node, relationship, nodeCursor))

  override def nodeGetIncomingDegreeWithMax(maxDegree: Int, node: Long, nodeCursor: NodeCursor): Int =
    singleDbHit(inner.nodeGetIncomingDegreeWithMax(maxDegree, node, nodeCursor))

  override def nodeGetIncomingDegreeWithMax(
    maxDegree: Int,
    node: Long,
    relationship: Int,
    nodeCursor: NodeCursor
  ): Int = singleDbHit(inner.nodeGetIncomingDegreeWithMax(maxDegree, node, relationship, nodeCursor))

  override def nodeGetTotalDegreeWithMax(maxDegree: Int, node: Long, nodeCursor: NodeCursor): Int =
    singleDbHit(inner.nodeGetTotalDegreeWithMax(maxDegree, node, nodeCursor))

  override def nodeGetTotalDegreeWithMax(maxDegree: Int, node: Long, relationship: Int, nodeCursor: NodeCursor): Int =
    singleDbHit(inner.nodeGetTotalDegreeWithMax(maxDegree, node, relationship, nodeCursor))

  override def nodeGetOutgoingDegree(node: Long, nodeCursor: NodeCursor): Int =
    singleDbHit(inner.nodeGetOutgoingDegree(node, nodeCursor))

  override def nodeGetOutgoingDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int =
    singleDbHit(inner.nodeGetOutgoingDegree(node, relationship, nodeCursor))

  override def nodeGetIncomingDegree(node: Long, nodeCursor: NodeCursor): Int =
    singleDbHit(inner.nodeGetIncomingDegree(node, nodeCursor))

  override def nodeGetIncomingDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int =
    singleDbHit(inner.nodeGetIncomingDegree(node, relationship, nodeCursor))

  override def nodeGetTotalDegree(node: Long, nodeCursor: NodeCursor): Int =
    singleDbHit(inner.nodeGetTotalDegree(node, nodeCursor))

  override def nodeGetTotalDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int =
    singleDbHit(inner.nodeGetTotalDegree(node, relationship, nodeCursor))

  override def nodeHasCheapDegrees(node: Long, nodeCursor: NodeCursor): Boolean =
    singleDbHit(inner.nodeHasCheapDegrees(node, nodeCursor))

  override def isLabelSetOnNode(label: Int, node: Long, nodeCursor: NodeCursor): Boolean =
    singleDbHit(inner.isLabelSetOnNode(label, node, nodeCursor))

  override def areLabelsSetOnNode(labels: Array[Int], id: Long, nodeCursor: NodeCursor): Boolean =
    singleDbHit(inner.areLabelsSetOnNode(labels, id, nodeCursor))

  override def isAnyLabelSetOnNode(labels: Array[Int], node: Long, nodeCursor: NodeCursor): Boolean =
    singleDbHit(inner.isAnyLabelSetOnNode(labels, node, nodeCursor))

  override def isALabelSetOnNode(node: Long, nodeCursor: NodeCursor): Boolean =
    singleDbHit(inner.isALabelSetOnNode(node, nodeCursor))

  override def isTypeSetOnRelationship(typ: Int, id: Long, relationshipCursor: RelationshipScanCursor): Boolean =
    singleDbHit(inner.isTypeSetOnRelationship(typ, id, relationshipCursor))

  override def areTypesSetOnRelationship(
    types: Array[Int],
    id: Long,
    relationshipCursor: RelationshipScanCursor
  ): Boolean =
    singleDbHit(inner.areTypesSetOnRelationship(types, id, relationshipCursor))

  override def areTypesSetOnRelationship(
    types: Array[Int],
    obj: VirtualRelationshipValue,
    relationshipCursor: RelationshipScanCursor
  ): Boolean =
    singleDbHit(inner.areTypesSetOnRelationship(types, obj, relationshipCursor))

  override def nodeCountByCountStore(labelId: Int): Long = singleDbHit(inner.nodeCountByCountStore(labelId))

  override def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long =
    singleDbHit(inner.relationshipCountByCountStore(startLabelId, typeId, endLabelId))

  override def lockNodes(nodeIds: Long*): Unit = inner.lockNodes(nodeIds: _*)

  override def lockRelationships(relIds: Long*): Unit = inner.lockRelationships(relIds: _*)

  override def callReadOnlyProcedure(
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): Iterator[Array[AnyValue]] =
    unknownDbHits(inner.callReadOnlyProcedure(id, args, context))

  override def callReadWriteProcedure(
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): Iterator[Array[AnyValue]] =
    unknownDbHits(inner.callReadWriteProcedure(id, args, context))

  override def callSchemaWriteProcedure(
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): Iterator[Array[AnyValue]] =
    unknownDbHits(inner.callSchemaWriteProcedure(id, args, context))

  override def callDbmsProcedure(
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): Iterator[Array[AnyValue]] =
    unknownDbHits(inner.callDbmsProcedure(id, args, context))

  override def callFunction(id: Int, args: Array[AnyValue], context: ProcedureCallContext): AnyValue =
    singleDbHit(inner.callFunction(id, args, context))

  override def callBuiltInFunction(id: Int, args: Array[AnyValue], context: ProcedureCallContext): AnyValue =
    singleDbHit(inner.callBuiltInFunction(id, args, context))

  override def aggregateFunction(id: Int, context: ProcedureCallContext): UserAggregationReducer =
    singleDbHit(inner.aggregateFunction(id, context))

  override def builtInAggregateFunction(id: Int, context: ProcedureCallContext): UserAggregationReducer =
    singleDbHit(inner.builtInAggregateFunction(id, context))

  override def detachDeleteNode(node: Long): Int = {
    val deletedRelationships = inner.detachDeleteNode(node)
    manyDbHits(1 + deletedRelationships) // This relies on the assumption that the node was not already deleted
    deletedRelationships
  }

  override def assertSchemaWritesAllowed(): Unit = inner.assertSchemaWritesAllowed()

  override def nodeApplyChanges(
    node: Long,
    addedLabels: IntSet,
    removedLabels: IntSet,
    properties: IntObjectMap[Value]
  ): Unit =
    singleDbHit(inner.nodeApplyChanges(node, addedLabels, removedLabels, properties))

  override def relationshipApplyChanges(relationship: Long, properties: IntObjectMap[Value]): Unit =
    singleDbHit(inner.relationshipApplyChanges(relationship, properties))

  override def assertShowIndexAllowed(): Unit = inner.assertShowIndexAllowed()

  override def assertShowConstraintAllowed(): Unit = inner.assertShowConstraintAllowed()

  override def asObject(value: AnyValue): AnyRef = inner.asObject(value)

  override def getTxStateNodePropertyOrNull(nodeId: Long, propertyKey: Int): Value =
    inner.getTxStateNodePropertyOrNull(nodeId, propertyKey)

  override def getTxStateRelationshipPropertyOrNull(relId: Long, propertyKey: Int): Value =
    inner.getTxStateRelationshipPropertyOrNull(relId, propertyKey)

  override def getTransactionType: KernelTransaction.Type = inner.getTransactionType

  override def contextWithNewTransaction(): QueryContext = inner.contextWithNewTransaction()

  override def close(): Unit = inner.close()

  override def addStatistics(statistics: QueryStatistics): Unit = inner.addStatistics(statistics)

  override def systemGraph: GraphDatabaseService = inner.systemGraph

  override def jobScheduler: JobScheduler = inner.jobScheduler

  override def logProvider: InternalLogProvider = inner.logProvider

  override def providedLanguageFunctions: Seq[FunctionInformation] = inner.providedLanguageFunctions

  override def getDatabaseContextProvider: DatabaseContextProvider[DatabaseContext] = inner.getDatabaseContextProvider

  override def getConfig: Config = inner.getConfig

  override def entityTransformer: EntityTransformer = inner.entityTransformer
}

class DelegatingReadOperations[T, CURSOR](protected val inner: ReadOperations[T, CURSOR])
    extends ReadOperations[T, CURSOR] {

  protected def singleDbHit[A](value: A): A = value
  protected def manyDbHits[A](value: ClosingIterator[A]): ClosingIterator[A] = value

  protected def manyDbHits[A](value: ClosingLongIterator): ClosingLongIterator = value

  override def getById(id: Long): T = inner.getById(id)

  override def getProperty(
    obj: Long,
    propertyKeyId: Int,
    cursor: CURSOR,
    propertyCursor: PropertyCursor,
    throwOnDeleted: Boolean
  ): Value =
    singleDbHit(inner.getProperty(obj, propertyKeyId, cursor, propertyCursor, throwOnDeleted))

  override def getProperty(
    obj: T,
    propertyKeyId: Int,
    cursor: CURSOR,
    propertyCursor: PropertyCursor,
    throwOnDeleted: Boolean
  ): Value =
    singleDbHit(inner.getProperty(obj, propertyKeyId, cursor, propertyCursor, throwOnDeleted))

  override def getProperties(
    obj: Long,
    properties: Array[Int],
    cursor: CURSOR,
    propertyCursor: PropertyCursor
  ): Array[Value] =
    singleDbHit(inner.getProperties(obj, properties, cursor, propertyCursor))

  override def getProperties(
    obj: T,
    properties: Array[Int],
    cursor: CURSOR,
    propertyCursor: PropertyCursor
  ): Array[Value] =
    singleDbHit(inner.getProperties(obj, properties, cursor, propertyCursor))

  override def getTxStateProperty(obj: Long, propertyKeyId: Int): Value = inner.getTxStateProperty(obj, propertyKeyId)

  override def hasProperty(obj: Long, propertyKeyId: Int, cursor: CURSOR, propertyCursor: PropertyCursor): Boolean =
    singleDbHit(inner.hasProperty(obj, propertyKeyId, cursor, propertyCursor))

  override def hasProperty(obj: T, propertyKeyId: Int, cursor: CURSOR, propertyCursor: PropertyCursor): Boolean =
    singleDbHit(inner.hasProperty(obj, propertyKeyId, cursor, propertyCursor))

  override def hasTxStatePropertyForCachedProperty(nodeId: Long, propertyKeyId: Int): Option[Boolean] =
    inner.hasTxStatePropertyForCachedProperty(nodeId, propertyKeyId)

  override def propertyKeyIds(obj: Long, cursor: CURSOR, propertyCursor: PropertyCursor): Array[Int] =
    singleDbHit(inner.propertyKeyIds(obj, cursor, propertyCursor))

  override def propertyKeyIds(obj: T, cursor: CURSOR, propertyCursor: PropertyCursor): Array[Int] =
    singleDbHit(inner.propertyKeyIds(obj, cursor, propertyCursor))

  override def all: ClosingLongIterator = manyDbHits(inner.all)

  override def isDeletedInThisTx(id: Long): Boolean = inner.isDeletedInThisTx(id)

  override def entityExists(id: Long): Boolean = singleDbHit(inner.entityExists(id))

  override def acquireExclusiveLock(obj: Long): Unit = inner.acquireExclusiveLock(obj)

  override def releaseExclusiveLock(obj: Long): Unit = inner.releaseExclusiveLock(obj)
}

class DelegatingOperations[T, CURSOR](override protected val inner: Operations[T, CURSOR])
    extends DelegatingReadOperations(inner) with Operations[T, CURSOR] {

  override def delete(id: Long): Boolean = singleDbHit(inner.delete(id))

  override def setProperty(obj: Long, propertyKey: Int, value: Value): Unit =
    singleDbHit(inner.setProperty(obj, propertyKey, value))

  override def setProperties(obj: Long, properties: IntObjectMap[Value]): Unit =
    singleDbHit(inner.setProperties(obj, properties))

  override def removeProperty(obj: Long, propertyKeyId: Int): Boolean =
    singleDbHit(inner.removeProperty(obj, propertyKeyId))

  override def entityExists(id: Long): Boolean = singleDbHit(inner.entityExists(id))
}

class DelegatingQueryTransactionalContext(val inner: QueryTransactionalContext) extends QueryTransactionalContext {

  override def transactionHeapHighWaterMark: Long = inner.transactionHeapHighWaterMark

  override def isTransactionOpen: Boolean = inner.isTransactionOpen

  override def assertTransactionOpen(): Unit = inner.assertTransactionOpen()

  override def close(): Unit = inner.close()

  override def kernelStatisticProvider: StatisticProvider = inner.kernelStatisticProvider

  override def dbmsInfo: DbmsInfo = inner.dbmsInfo

  override def databaseId: NamedDatabaseId = inner.databaseId

  override def commitTransaction(): Unit = inner.commitTransaction()

  override def cursors: CursorFactory = inner.cursors

  override def cursorContext: CursorContext = inner.cursorContext

  override def locks: Locks = inner.locks

  override def dataRead: Read = inner.dataRead

  override def dataWrite: Write = inner.dataWrite

  override def tokenRead: TokenRead = inner.tokenRead

  override def tokenWrite: TokenWrite = inner.tokenWrite

  override def token: Token = inner.token

  override def schemaRead: SchemaRead = inner.schemaRead

  override def schemaWrite: SchemaWrite = inner.schemaWrite

  override def rollback(): Unit = inner.rollback()

  override def markForTermination(reason: Status): Unit = inner.markForTermination(reason)

  override def kernelQueryContext: api.QueryContext = inner.kernelQueryContext

  override def procedures: Procedures = inner.procedures

  override def securityContext: SecurityContext = inner.securityContext

  override def securityAuthorizationHandler: SecurityAuthorizationHandler = inner.securityAuthorizationHandler

  override def accessMode: AccessMode = inner.accessMode

  override def memoryTracker: MemoryTracker = inner.memoryTracker

  override def validateSameDB[E <: Entity](entity: E): Unit = inner.validateSameDB(entity)

  override def elementIdMapper(): ElementIdMapper = inner.elementIdMapper()

  override def userTransactionId: String = inner.userTransactionId

  override def config: Config = inner.config

  override def kernelExecutingQuery: org.neo4j.kernel.api.query.ExecutingQuery = inner.kernelExecutingQuery

  override def kernelExecutionContext: ExecutionContext = inner.kernelExecutionContext

  override def createValueMapper: ValueMapper[AnyRef] = inner.createValueMapper

  override def constituentTransactionFactory: ConstituentTransactionFactory = inner.constituentTransactionFactory
}
