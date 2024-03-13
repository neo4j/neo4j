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
package org.neo4j.cypher.internal.planning

import org.eclipse.collections.api.map.primitive.IntObjectMap
import org.eclipse.collections.api.set.primitive.IntSet
import org.neo4j.common.EntityType
import org.neo4j.configuration.Config
import org.neo4j.csv.reader.CharReadable
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.macros.TranslateExceptionMacros.translateException
import org.neo4j.cypher.internal.macros.TranslateExceptionMacros.translateIterator
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
import org.neo4j.cypher.internal.runtime.QueryTransactionalContext
import org.neo4j.cypher.internal.runtime.ReadOperations
import org.neo4j.cypher.internal.runtime.ReadQueryContext
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.RelationshipOperations
import org.neo4j.cypher.internal.runtime.RelationshipReadOperations
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.DelegatingQueryTransactionalContext
import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.PropertyIndexQuery
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.internal.kernel.api.TokenReadSession
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.internal.schema.constraints.PropertyTypeSet
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.index.IndexUsageStats
import org.neo4j.kernel.impl.query.FunctionInformation
import org.neo4j.logging.InternalLogProvider
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

import java.net.URI

class ExceptionTranslatingReadQueryContext(val inner: ReadQueryContext) extends ReadQueryContext
    with ExceptionTranslationSupport {

  override def resources: ResourceManager = inner.resources

  override def transactionalContext: QueryTransactionalContext = ExceptionTranslatingTransactionalContext

  override def getLabelsForNode(node: Long, nodeCursor: NodeCursor): ListValue =
    translateException(tokenNameLookup, inner.getLabelsForNode(node, nodeCursor))

  override def getTypeForRelationship(id: Long, cursor: RelationshipScanCursor): AnyValue =
    translateException(tokenNameLookup, inner.getTypeForRelationship(id, cursor))

  override def getLabelName(id: Int): String =
    translateException(tokenNameLookup, inner.getLabelName(id))

  override def getOptLabelId(labelName: String): Option[Int] =
    translateException(tokenNameLookup, inner.getOptLabelId(labelName))

  override def getLabelId(labelName: String): Int =
    translateException(tokenNameLookup, inner.getLabelId(labelName))

  override val nodeReadOps: NodeReadOperations =
    new ExceptionTranslatingReadOperations[VirtualNodeValue, NodeCursor](inner.nodeReadOps) with NodeReadOperations

  override val relationshipReadOps: RelationshipReadOperations =
    new ExceptionTranslatingReadOperations[VirtualRelationshipValue, RelationshipScanCursor](inner.relationshipReadOps)
      with RelationshipReadOperations

  override def getPropertyKeyName(propertyKeyId: Int): String =
    translateException(tokenNameLookup, inner.getPropertyKeyName(propertyKeyId))

  override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] =
    translateException(tokenNameLookup, inner.getOptPropertyKeyId(propertyKeyName))

  override def getPropertyKeyId(propertyKey: String): Int =
    translateException(tokenNameLookup, inner.getPropertyKeyId(propertyKey))

  override def getAllIndexes(): Map[IndexDescriptor, IndexInfo] =
    translateException(tokenNameLookup, inner.getAllIndexes())

  override def getIndexUsageStatistics(index: IndexDescriptor): IndexUsageStats =
    translateException(tokenNameLookup, inner.getIndexUsageStatistics(index))

  override def getIndexInformation(name: String): IndexInformation =
    translateException(tokenNameLookup, inner.getIndexInformation(name))

  override def getIndexInformation(index: IndexDescriptor): IndexInformation =
    translateException(tokenNameLookup, inner.getIndexInformation(index))

  override def indexExists(name: String): Boolean =
    translateException(tokenNameLookup, inner.indexExists(name))

  override def constraintExists(name: String): Boolean =
    translateException(tokenNameLookup, inner.constraintExists(name))

  override def constraintExists(matchFn: ConstraintDescriptor => Boolean, entityId: Int, properties: Int*): Boolean =
    translateException(tokenNameLookup, inner.constraintExists(matchFn, entityId, properties: _*))

  override def indexReference(
    indexType: IndexType,
    entityId: Int,
    entityType: EntityType,
    properties: Int*
  ): IndexDescriptor =
    translateException(tokenNameLookup, inner.indexReference(indexType, entityId, entityType, properties: _*))

  override def lookupIndexReference(entityType: EntityType): IndexDescriptor =
    translateException(tokenNameLookup, inner.lookupIndexReference(entityType))

  override def fulltextIndexReference(entityIds: List[Int], entityType: EntityType, properties: Int*): IndexDescriptor =
    translateException(tokenNameLookup, inner.fulltextIndexReference(entityIds, entityType, properties: _*))

  override def nodeIndexSeek(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder,
    values: Seq[PropertyIndexQuery]
  ): NodeValueIndexCursor =
    translateException(tokenNameLookup, inner.nodeIndexSeek(index, needsValues, indexOrder, values))

  override def relationshipIndexSeek(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder,
    values: Seq[PropertyIndexQuery]
  ): RelationshipValueIndexCursor =
    translateException(tokenNameLookup, inner.relationshipIndexSeek(index, needsValues, indexOrder, values))

  override def relationshipLockingUniqueIndexSeek(
    index: IndexDescriptor,
    queries: Seq[PropertyIndexQuery.ExactPredicate]
  ): RelationshipValueIndexCursor =
    translateException(tokenNameLookup, inner.relationshipLockingUniqueIndexSeek(index, queries))

  override def relationshipIndexSeekByContains(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder,
    value: TextValue
  ): RelationshipValueIndexCursor =
    translateException(tokenNameLookup, inner.relationshipIndexSeekByContains(index, needsValues, indexOrder, value))

  override def relationshipIndexSeekByEndsWith(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder,
    value: TextValue
  ): RelationshipValueIndexCursor =
    translateException(tokenNameLookup, inner.relationshipIndexSeekByEndsWith(index, needsValues, indexOrder, value))

  override def relationshipIndexScan(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder
  ): RelationshipValueIndexCursor =
    translateException(tokenNameLookup, inner.relationshipIndexScan(index, needsValues, indexOrder))

  override def getNodesByLabel(
    tokenReadSession: TokenReadSession,
    id: Int,
    indexOrder: IndexOrder
  ): ClosingLongIterator =
    translateException(tokenNameLookup, inner.getNodesByLabel(tokenReadSession, id, indexOrder))

  override def nodeAsMap(id: Long, nodeCursor: NodeCursor, propertyCursor: PropertyCursor): MapValue =
    translateException(tokenNameLookup, inner.nodeAsMap(id, nodeCursor, propertyCursor))

  override def relationshipAsMap(
    id: Long,
    relationshipCursor: RelationshipScanCursor,
    propertyCursor: PropertyCursor
  ): MapValue =
    translateException(tokenNameLookup, inner.relationshipAsMap(id, relationshipCursor, propertyCursor))

  override def relationshipAsMap(
    rel: VirtualRelationshipValue,
    relationshipCursor: RelationshipScanCursor,
    propertyCursor: PropertyCursor
  ): MapValue =
    translateException(tokenNameLookup, inner.relationshipAsMap(rel, relationshipCursor, propertyCursor))

  override def nodeGetOutgoingDegreeWithMax(maxDegree: Int, node: Long, nodeCursor: NodeCursor): Int =
    translateException(tokenNameLookup, inner.nodeGetOutgoingDegreeWithMax(maxDegree, node, nodeCursor))

  override def nodeGetOutgoingDegreeWithMax(
    maxDegree: Int,
    node: Long,
    relationship: Int,
    nodeCursor: NodeCursor
  ): Int =
    translateException(tokenNameLookup, inner.nodeGetOutgoingDegreeWithMax(maxDegree, node, relationship, nodeCursor))

  override def nodeGetIncomingDegreeWithMax(maxDegree: Int, node: Long, nodeCursor: NodeCursor): Int =
    translateException(tokenNameLookup, inner.nodeGetIncomingDegreeWithMax(maxDegree, node, nodeCursor))

  override def nodeGetIncomingDegreeWithMax(
    maxDegree: Int,
    node: Long,
    relationship: Int,
    nodeCursor: NodeCursor
  ): Int =
    translateException(tokenNameLookup, inner.nodeGetIncomingDegreeWithMax(maxDegree, node, relationship, nodeCursor))

  override def nodeGetTotalDegreeWithMax(maxDegree: Int, node: Long, nodeCursor: NodeCursor): Int =
    translateException(tokenNameLookup, inner.nodeGetTotalDegreeWithMax(maxDegree, node, nodeCursor))

  override def nodeGetTotalDegreeWithMax(maxDegree: Int, node: Long, relationship: Int, nodeCursor: NodeCursor): Int =
    translateException(tokenNameLookup, inner.nodeGetTotalDegreeWithMax(maxDegree, node, relationship, nodeCursor))

  override def nodeGetOutgoingDegree(node: Long, nodeCursor: NodeCursor): Int =
    translateException(tokenNameLookup, inner.nodeGetOutgoingDegree(node, nodeCursor))

  override def nodeGetOutgoingDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int =
    translateException(tokenNameLookup, inner.nodeGetOutgoingDegree(node, relationship, nodeCursor))

  override def nodeGetIncomingDegree(node: Long, nodeCursor: NodeCursor): Int =
    translateException(tokenNameLookup, inner.nodeGetIncomingDegree(node, nodeCursor))

  override def nodeGetIncomingDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int =
    translateException(tokenNameLookup, inner.nodeGetIncomingDegree(node, relationship, nodeCursor))

  override def nodeGetTotalDegree(node: Long, nodeCursor: NodeCursor): Int =
    translateException(tokenNameLookup, inner.nodeGetTotalDegree(node, nodeCursor))

  override def singleNode(id: Long, cursor: NodeCursor): Unit =
    translateException(tokenNameLookup, inner.singleNode(id, cursor))

  override def singleRelationship(id: Long, cursor: RelationshipScanCursor): Unit =
    translateException(tokenNameLookup, inner.singleRelationship(id, cursor))

  override def nodeGetTotalDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int =
    translateException(tokenNameLookup, inner.nodeGetTotalDegree(node, relationship, nodeCursor))

  override def getConstraintInformation(name: String): ConstraintInformation =
    translateException(tokenNameLookup, inner.getConstraintInformation(name))

  override def getConstraintInformation(
    matchFn: ConstraintDescriptor => Boolean,
    entityId: Int,
    properties: Int*
  ): ConstraintInformation =
    translateException(tokenNameLookup, inner.getConstraintInformation(matchFn, entityId, properties: _*))

  override def getAllConstraints(): Map[ConstraintDescriptor, ConstraintInfo] =
    translateException(tokenNameLookup, inner.getAllConstraints())

  override def callReadOnlyProcedure(
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): Iterator[Array[AnyValue]] =
    translateIterator(tokenNameLookup, inner.callReadOnlyProcedure(id, args, context))

  override def callReadWriteProcedure(
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): Iterator[Array[AnyValue]] =
    translateIterator(tokenNameLookup, inner.callReadWriteProcedure(id, args, context))

  override def callSchemaWriteProcedure(
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): Iterator[Array[AnyValue]] =
    translateIterator(tokenNameLookup, inner.callSchemaWriteProcedure(id, args, context))

  override def callDbmsProcedure(
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): Iterator[Array[AnyValue]] =
    translateIterator(tokenNameLookup, inner.callDbmsProcedure(id, args, context))

  override def callFunction(id: Int, args: Array[AnyValue], context: ProcedureCallContext): AnyValue =
    translateException(tokenNameLookup, inner.callFunction(id, args, context))

  override def callBuiltInFunction(id: Int, args: Array[AnyValue], context: ProcedureCallContext): AnyValue =
    translateException(tokenNameLookup, inner.callBuiltInFunction(id, args, context))

  override def aggregateFunction(id: Int, context: ProcedureCallContext): UserAggregationReducer =
    translateException(tokenNameLookup, inner.aggregateFunction(id, context))

  override def builtInAggregateFunction(id: Int, context: ProcedureCallContext): UserAggregationReducer =
    translateException(tokenNameLookup, inner.builtInAggregateFunction(id, context))

  override def isLabelSetOnNode(label: Int, node: Long, nodeCursor: NodeCursor): Boolean =
    translateException(tokenNameLookup, inner.isLabelSetOnNode(label, node, nodeCursor))

  override def areLabelsSetOnNode(labels: Array[Int], id: Long, nodeCursor: NodeCursor): Boolean =
    translateException(tokenNameLookup, inner.areLabelsSetOnNode(labels, id, nodeCursor))

  override def isAnyLabelSetOnNode(labels: Array[Int], node: Long, nodeCursor: NodeCursor): Boolean =
    translateException(tokenNameLookup, inner.isAnyLabelSetOnNode(labels, node, nodeCursor))

  override def isALabelSetOnNode(node: Long, nodeCursor: NodeCursor): Boolean =
    translateException(tokenNameLookup, inner.isALabelSetOnNode(node, nodeCursor))

  override def isTypeSetOnRelationship(
    typ: Int,
    relationship: Long,
    relationshipCursor: RelationshipScanCursor
  ): Boolean =
    translateException(tokenNameLookup, inner.isTypeSetOnRelationship(typ, relationship, relationshipCursor))

  override def areTypesSetOnRelationship(
    types: Array[Int],
    relationship: Long,
    relationshipCursor: RelationshipScanCursor
  ): Boolean =
    translateException(tokenNameLookup, inner.areTypesSetOnRelationship(types, relationship, relationshipCursor))

  override def areTypesSetOnRelationship(
    types: Array[Int],
    relationship: VirtualRelationshipValue,
    relationshipCursor: RelationshipScanCursor
  ): Boolean =
    translateException(tokenNameLookup, inner.areTypesSetOnRelationship(types, relationship, relationshipCursor))

  override def getRelTypeId(relType: String): Int =
    translateException(tokenNameLookup, inner.getRelTypeId(relType))

  override def getRelTypeName(id: Int): String =
    translateException(tokenNameLookup, inner.getRelTypeName(id))

  override def nodeLockingUniqueIndexSeek(
    index: IndexDescriptor,
    values: Seq[PropertyIndexQuery.ExactPredicate]
  ): NodeValueIndexCursor =
    translateException(tokenNameLookup, inner.nodeLockingUniqueIndexSeek(index, values))

  override def getImportDataConnection(uri: URI): CharReadable =
    translateException(tokenNameLookup, inner.getImportDataConnection(uri))

  override def getRelationshipsForIds(
    node: Long,
    dir: SemanticDirection,
    types: Array[Int]
  ): ClosingLongIterator with RelationshipIterator =
    translateException(tokenNameLookup, inner.getRelationshipsForIds(node, dir, types))

  override def getRelationshipsByType(
    tokenReadSession: TokenReadSession,
    relType: Int,
    indexOrder: IndexOrder
  ): ClosingLongIterator with RelationshipIterator =
    translateException(tokenNameLookup, inner.getRelationshipsByType(tokenReadSession, relType, indexOrder))

  override def nodeCursor(): NodeCursor = translateException(tokenNameLookup, inner.nodeCursor())

  override def nodeLabelIndexCursor(): NodeLabelIndexCursor =
    translateException(tokenNameLookup, inner.nodeLabelIndexCursor())

  override def relationshipTypeIndexCursor(): RelationshipTypeIndexCursor =
    translateException(tokenNameLookup, inner.relationshipTypeIndexCursor())

  override def traversalCursor(): RelationshipTraversalCursor =
    translateException(tokenNameLookup, inner.traversalCursor())

  override def scanCursor(): RelationshipScanCursor = translateException(tokenNameLookup, inner.scanCursor())

  override def relationshipById(
    relationshipId: Long,
    startNodeId: Long,
    endNodeId: Long,
    typeId: Int
  ): VirtualRelationshipValue =
    translateException(tokenNameLookup, inner.relationshipById(relationshipId, startNodeId, endNodeId, typeId))

  override def nodeIndexSeekByContains(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder,
    value: TextValue
  ): NodeValueIndexCursor =
    translateException(tokenNameLookup, inner.nodeIndexSeekByContains(index, needsValues, indexOrder, value))

  override def nodeIndexSeekByEndsWith(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder,
    value: TextValue
  ): NodeValueIndexCursor =
    translateException(tokenNameLookup, inner.nodeIndexSeekByEndsWith(index, needsValues, indexOrder, value))

  override def nodeIndexScan(
    index: IndexReadSession,
    needsValues: Boolean,
    indexOrder: IndexOrder
  ): NodeValueIndexCursor =
    translateException(tokenNameLookup, inner.nodeIndexScan(index, needsValues, indexOrder))

  override def nodeHasCheapDegrees(node: Long, nodeCursor: NodeCursor): Boolean =
    translateException(tokenNameLookup, inner.nodeHasCheapDegrees(node, nodeCursor))

  override def asObject(value: AnyValue): AnyRef =
    translateException(tokenNameLookup, inner.asObject(value))

  override def getTxStateNodePropertyOrNull(nodeId: Long, propertyKey: Int): Value =
    translateException(tokenNameLookup, inner.getTxStateNodePropertyOrNull(nodeId, propertyKey))

  override def getTxStateRelationshipPropertyOrNull(relId: Long, propertyKey: Int): Value =
    translateException(tokenNameLookup, inner.getTxStateRelationshipPropertyOrNull(relId, propertyKey))

  override def nodeCountByCountStore(labelId: Int): Long =
    translateException(tokenNameLookup, inner.nodeCountByCountStore(labelId))

  override def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long =
    translateException(tokenNameLookup, inner.relationshipCountByCountStore(startLabelId, typeId, endLabelId))

  override def lockNodes(nodeIds: Long*): Unit =
    translateException(tokenNameLookup, inner.lockNodes(nodeIds: _*))

  override def lockRelationships(relIds: Long*): Unit =
    translateException(tokenNameLookup, inner.lockRelationships(relIds: _*))

  override def getOptRelTypeId(relType: String): Option[Int] =
    translateException(tokenNameLookup, inner.getOptRelTypeId(relType))

  override def assertShowIndexAllowed(): Unit = translateException(tokenNameLookup, inner.assertShowIndexAllowed())

  override def assertShowConstraintAllowed(): Unit =
    translateException(tokenNameLookup, inner.assertShowConstraintAllowed())

  override def getTransactionType: KernelTransaction.Type = inner.getTransactionType

  override def contextWithNewTransaction(): QueryContext =
    new ExceptionTranslatingQueryContext(inner.contextWithNewTransaction())

  override def systemGraph: GraphDatabaseService = translateException(tokenNameLookup, inner.systemGraph)

  override def logProvider: InternalLogProvider = translateException(tokenNameLookup, inner.logProvider)

  override def providedLanguageFunctions: Seq[FunctionInformation] =
    translateException(tokenNameLookup, inner.providedLanguageFunctions)

  override def getConfig: Config = translateException(tokenNameLookup, inner.getConfig)

  override def entityTransformer: EntityTransformer = translateException(tokenNameLookup, inner.entityTransformer)

  override def close(): Unit = inner.close()

  class ExceptionTranslatingReadOperations[T, CURSOR](inner: ReadOperations[T, CURSOR])
      extends ReadOperations[T, CURSOR] {

    override def getById(id: Long): T =
      translateException(tokenNameLookup, inner.getById(id))

    override def getProperty(
      id: Long,
      propertyKeyId: Int,
      cursor: CURSOR,
      propertyCursor: PropertyCursor,
      throwOnDeleted: Boolean
    ): Value =
      translateException(tokenNameLookup, inner.getProperty(id, propertyKeyId, cursor, propertyCursor, throwOnDeleted))

    override def getProperty(
      obj: T,
      propertyKeyId: Int,
      cursor: CURSOR,
      propertyCursor: PropertyCursor,
      throwOnDeleted: Boolean
    ): Value =
      translateException(tokenNameLookup, inner.getProperty(obj, propertyKeyId, cursor, propertyCursor, throwOnDeleted))

    override def getProperties(
      obj: T,
      properties: Array[Int],
      cursor: CURSOR,
      propertyCursor: PropertyCursor
    ): Array[Value] =
      translateException(tokenNameLookup, inner.getProperties(obj, properties, cursor, propertyCursor))

    override def getProperties(
      obj: Long,
      properties: Array[Int],
      cursor: CURSOR,
      propertyCursor: PropertyCursor
    ): Array[Value] =
      translateException(tokenNameLookup, inner.getProperties(obj, properties, cursor, propertyCursor))

    override def hasProperty(id: Long, propertyKeyId: Int, cursor: CURSOR, propertyCursor: PropertyCursor): Boolean =
      translateException(tokenNameLookup, inner.hasProperty(id, propertyKeyId, cursor, propertyCursor))

    override def hasProperty(obj: T, propertyKeyId: Int, cursor: CURSOR, propertyCursor: PropertyCursor): Boolean =
      translateException(tokenNameLookup, inner.hasProperty(obj, propertyKeyId, cursor, propertyCursor))

    override def propertyKeyIds(id: Long, cursor: CURSOR, propertyCursor: PropertyCursor): Array[Int] =
      translateException(tokenNameLookup, inner.propertyKeyIds(id, cursor, propertyCursor))

    override def propertyKeyIds(obj: T, cursor: CURSOR, propertyCursor: PropertyCursor): Array[Int] =
      translateException(tokenNameLookup, inner.propertyKeyIds(obj, cursor, propertyCursor))

    override def all: ClosingLongIterator =
      translateException(tokenNameLookup, inner.all)

    override def isDeletedInThisTx(id: Long): Boolean =
      translateException(tokenNameLookup, inner.isDeletedInThisTx(id))

    override def entityExists(id: Long): Boolean =
      translateException(tokenNameLookup, inner.entityExists(id))

    override def getTxStateProperty(obj: Long, propertyKeyId: Int): Value =
      translateException(tokenNameLookup, inner.getTxStateProperty(obj, propertyKeyId))

    override def hasTxStatePropertyForCachedProperty(entityId: Long, propertyKeyId: Int): Option[Boolean] =
      translateException(tokenNameLookup, inner.hasTxStatePropertyForCachedProperty(entityId, propertyKeyId))

    override def acquireExclusiveLock(obj: Long): Unit =
      translateException(tokenNameLookup, inner.acquireExclusiveLock(obj))

    override def releaseExclusiveLock(obj: Long): Unit =
      translateException(tokenNameLookup, inner.releaseExclusiveLock(obj))
  }

  private object ExceptionTranslatingTransactionalContext
      extends DelegatingQueryTransactionalContext(inner.transactionalContext) {
    override def close(): Unit = translateException(tokenNameLookup, super.close())

    override def rollback(): Unit = translateException(tokenNameLookup, super.rollback())

    override def markForTermination(reason: Status): Unit =
      translateException(tokenNameLookup, super.markForTermination(reason))
  }
}

class ExceptionTranslatingQueryContext(override val inner: QueryContext)
    extends ExceptionTranslatingReadQueryContext(inner)
    with QueryContext with ExceptionTranslationSupport {

  override def createParallelQueryContext(initialHeapMemory: Long): QueryContext = {
    new ExceptionTranslatingQueryContext(inner.createParallelQueryContext(initialHeapMemory))
  }

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int =
    translateException(tokenNameLookup, inner.setLabelsOnNode(node, labelIds))

  override def createNodeId(labels: Array[Int]): Long =
    translateException(tokenNameLookup, inner.createNodeId(labels))

  override val nodeWriteOps: NodeOperations =
    new ExceptionTranslatingOperations[VirtualNodeValue, NodeCursor](inner.nodeWriteOps) with NodeOperations

  override val relationshipWriteOps: RelationshipOperations =
    new ExceptionTranslatingOperations[VirtualRelationshipValue, RelationshipScanCursor](inner.relationshipWriteOps)
      with RelationshipOperations

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int =
    translateException(tokenNameLookup, inner.removeLabelsFromNode(node, labelIds))

  override def getOrCreateLabelId(labelName: String): Int =
    translateException(tokenNameLookup, inner.getOrCreateLabelId(labelName))

  override def getOrCreateTypeId(relTypeName: String): Int =
    translateException(tokenNameLookup, inner.getOrCreateTypeId(relTypeName))

  override def getOrCreatePropertyKeyId(propertyKey: String): Int =
    translateException(tokenNameLookup, inner.getOrCreatePropertyKeyId(propertyKey))

  override def getOrCreatePropertyKeyIds(propertyKeys: Array[String]): Array[Int] =
    translateException(tokenNameLookup, inner.getOrCreatePropertyKeyIds(propertyKeys))

  override def validateIndexProvider(
    schemaDescription: String,
    providerString: String,
    indexType: IndexType
  ): IndexProviderDescriptor =
    translateException(tokenNameLookup, inner.validateIndexProvider(schemaDescription, providerString, indexType))

  override def addRangeIndexRule(
    entityId: Int,
    entityType: EntityType,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): IndexDescriptor =
    translateException(tokenNameLookup, inner.addRangeIndexRule(entityId, entityType, propertyKeyIds, name, provider))

  override def addLookupIndexRule(
    entityType: EntityType,
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): IndexDescriptor =
    translateException(tokenNameLookup, inner.addLookupIndexRule(entityType, name, provider))

  override def addFulltextIndexRule(
    entityIds: List[Int],
    entityType: EntityType,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor],
    indexConfig: IndexConfig
  ): IndexDescriptor =
    translateException(
      tokenNameLookup,
      inner.addFulltextIndexRule(entityIds, entityType, propertyKeyIds, name, provider, indexConfig)
    )

  override def addTextIndexRule(
    entityId: Int,
    entityType: EntityType,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): IndexDescriptor =
    translateException(tokenNameLookup, inner.addTextIndexRule(entityId, entityType, propertyKeyIds, name, provider))

  override def addPointIndexRule(
    entityId: Int,
    entityType: EntityType,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor],
    indexConfig: IndexConfig
  ): IndexDescriptor =
    translateException(
      tokenNameLookup,
      inner.addPointIndexRule(entityId, entityType, propertyKeyIds, name, provider, indexConfig)
    )

  override def addVectorIndexRule(
    entityId: Int,
    entityType: EntityType,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor],
    indexConfig: IndexConfig
  ): IndexDescriptor =
    translateException(
      tokenNameLookup,
      inner.addVectorIndexRule(entityId, entityType, propertyKeyIds, name, provider, indexConfig)
    )

  override def dropIndexRule(name: String): Unit =
    translateException(tokenNameLookup, inner.dropIndexRule(name))

  override def createNodeKeyConstraint(
    labelId: Int,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): Unit =
    translateException(
      tokenNameLookup,
      inner.createNodeKeyConstraint(labelId, propertyKeyIds, name, provider)
    )

  override def createRelationshipKeyConstraint(
    relTypeId: Int,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): Unit =
    translateException(
      tokenNameLookup,
      inner.createRelationshipKeyConstraint(relTypeId, propertyKeyIds, name, provider)
    )

  override def createNodeUniqueConstraint(
    labelId: Int,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): Unit =
    translateException(
      tokenNameLookup,
      inner.createNodeUniqueConstraint(labelId, propertyKeyIds, name, provider)
    )

  override def createRelationshipUniqueConstraint(
    relTypeId: Int,
    propertyKeyIds: Seq[Int],
    name: Option[String],
    provider: Option[IndexProviderDescriptor]
  ): Unit =
    translateException(
      tokenNameLookup,
      inner.createRelationshipUniqueConstraint(relTypeId, propertyKeyIds, name, provider)
    )

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int, name: Option[String]): Unit =
    translateException(tokenNameLookup, inner.createNodePropertyExistenceConstraint(labelId, propertyKeyId, name))

  override def createRelationshipPropertyExistenceConstraint(
    relTypeId: Int,
    propertyKeyId: Int,
    name: Option[String]
  ): Unit =
    translateException(
      tokenNameLookup,
      inner.createRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId, name)
    )

  override def createNodePropertyTypeConstraint(
    labelId: Int,
    propertyKeyId: Int,
    propertyTypes: PropertyTypeSet,
    name: Option[String]
  ): Unit = translateException(
    tokenNameLookup,
    inner.createNodePropertyTypeConstraint(labelId, propertyKeyId, propertyTypes, name)
  )

  override def createRelationshipPropertyTypeConstraint(
    relTypeId: Int,
    propertyKeyId: Int,
    propertyTypes: PropertyTypeSet,
    name: Option[String]
  ): Unit = translateException(
    tokenNameLookup,
    inner.createRelationshipPropertyTypeConstraint(relTypeId, propertyKeyId, propertyTypes, name)
  )

  override def dropNamedConstraint(name: String): Unit =
    translateException(tokenNameLookup, inner.dropNamedConstraint(name))

  override def createRelationshipId(start: Long, end: Long, relType: Int): Long =
    translateException(tokenNameLookup, inner.createRelationshipId(start, end, relType))

  override def getOrCreateRelTypeId(relTypeName: String): Int =
    translateException(tokenNameLookup, inner.getOrCreateRelTypeId(relTypeName))

  override def detachDeleteNode(node: Long): Int =
    translateException(tokenNameLookup, inner.detachDeleteNode(node))

  override def assertSchemaWritesAllowed(): Unit =
    translateException(tokenNameLookup, inner.assertSchemaWritesAllowed())

  override def getDatabaseContextProvider: DatabaseContextProvider[DatabaseContext] =
    translateException(tokenNameLookup, inner.getDatabaseContextProvider)

  override def nodeApplyChanges(
    id: Long,
    addedLabels: IntSet,
    removedLabels: IntSet,
    properties: IntObjectMap[Value]
  ): Unit =
    translateException(tokenNameLookup, inner.nodeApplyChanges(id, addedLabels, removedLabels, properties))

  override def relationshipApplyChanges(relationship: Long, properties: IntObjectMap[Value]): Unit =
    translateException(tokenNameLookup, inner.relationshipApplyChanges(relationship, properties))

  class ExceptionTranslatingOperations[T, CURSOR](inner: Operations[T, CURSOR])
      extends ExceptionTranslatingReadOperations[T, CURSOR](inner) with Operations[T, CURSOR] {

    override def delete(id: Long): Boolean =
      translateException(tokenNameLookup, inner.delete(id))

    override def setProperty(id: Long, propertyKey: Int, value: Value): Unit =
      translateException(tokenNameLookup, inner.setProperty(id, propertyKey, value))

    override def setProperties(obj: Long, properties: IntObjectMap[Value]): Unit =
      translateException(tokenNameLookup, inner.setProperties(obj, properties))

    override def removeProperty(id: Long, propertyKeyId: Int): Boolean =
      translateException(tokenNameLookup, inner.removeProperty(id, propertyKeyId))
  }
}
