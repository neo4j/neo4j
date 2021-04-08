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

import java.net.URL

import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.profiling.KernelStatisticProvider
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.ConstraintInfo
import org.neo4j.cypher.internal.runtime.Expander
import org.neo4j.cypher.internal.runtime.IndexInfo
import org.neo4j.cypher.internal.runtime.KernelPredicate
import org.neo4j.cypher.internal.runtime.NodeOperations
import org.neo4j.cypher.internal.runtime.Operations
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryTransactionalContext
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.RelationshipOperations
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.UserDefinedAggregator
import org.neo4j.graphdb.Entity
import org.neo4j.graphdb.Path
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.PropertyIndexQuery
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.internal.kernel.api.SchemaRead
import org.neo4j.internal.kernel.api.TokenRead
import org.neo4j.internal.kernel.api.TokenReadSession
import org.neo4j.internal.kernel.api.Write
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.impl.core.TransactionalEntityFactory
import org.neo4j.kernel.impl.factory.DbmsInfo
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.RelationshipValue

import scala.collection.Iterator

abstract class DelegatingQueryContext(val inner: QueryContext) extends QueryContext {

  protected def singleDbHit[A](value: A): A = value
  protected def unknownDbHits[A](value: A): A = value
  protected def manyDbHits[A](value: ClosingIterator[A]): ClosingIterator[A] = value

  protected def manyDbHits(value: ClosingLongIterator): ClosingLongIterator = value
  protected def manyDbHits(value: RelationshipIterator): RelationshipIterator = value
  protected def manyDbHitsCliRi(value: ClosingLongIterator with RelationshipIterator): ClosingLongIterator with RelationshipIterator = value
  protected def manyDbHits(value: RelationshipTraversalCursor): RelationshipTraversalCursor = value
  protected def manyDbHits(value: NodeValueIndexCursor): NodeValueIndexCursor = value
  protected def manyDbHits(value: RelationshipValueIndexCursor): RelationshipValueIndexCursor = value
  protected def manyDbHits(value: NodeCursor): NodeCursor = value
  protected def manyDbHits(value: RelationshipScanCursor): RelationshipScanCursor = value
  protected def manyDbHits(value: PropertyCursor): PropertyCursor = value
  protected def manyDbHits(count: Int): Int = count

  override def resources: ResourceManager = inner.resources

  override def transactionalContext: QueryTransactionalContext = inner.transactionalContext

  override def entityAccessor: TransactionalEntityFactory = inner.entityAccessor

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int =
    singleDbHit(inner.setLabelsOnNode(node, labelIds))

  override def createNode(labels: Array[Int]): NodeValue = singleDbHit(inner.createNode(labels))

  override def createNodeId(labels: Array[Int]): Long = singleDbHit(inner.createNodeId(labels))

  override def createRelationship(start: Long, end: Long, relType: Int): RelationshipValue =
    singleDbHit(inner.createRelationship(start, end, relType))

  override def getOrCreateRelTypeId(relTypeName: String): Int = singleDbHit(inner.getOrCreateRelTypeId(relTypeName))

  override def getLabelsForNode(node: Long, nodeCursor: NodeCursor): ListValue = singleDbHit(inner.getLabelsForNode(node, nodeCursor))

  override def getTypeForRelationship(id: Long, cursor: RelationshipScanCursor): TextValue = singleDbHit(inner.getTypeForRelationship(id, cursor))

  override def getLabelName(id: Int): String = singleDbHit(inner.getLabelName(id))

  override def getOptLabelId(labelName: String): Option[Int] = singleDbHit(inner.getOptLabelId(labelName))

  override def getLabelId(labelName: String): Int = singleDbHit(inner.getLabelId(labelName))

  override def getOrCreateLabelId(labelName: String): Int = singleDbHit(inner.getOrCreateLabelId(labelName))

  override def getOrCreateTypeId(relTypeName: String): Int = singleDbHit(inner.getOrCreateTypeId(relTypeName))

  override def getRelationshipsForIds(node: Long, dir: SemanticDirection, types: Array[Int]): ClosingIterator[RelationshipValue] =
  manyDbHits(inner.getRelationshipsForIds(node, dir, types))

  override def getRelationshipsForIdsPrimitive(node: Long, dir: SemanticDirection, types: Array[Int]): ClosingLongIterator with RelationshipIterator =
    manyDbHitsCliRi(inner.getRelationshipsForIdsPrimitive(node, dir, types))

  override def getRelationshipsByType(tokenReadSession: TokenReadSession, relType: Int): ClosingLongIterator =
    manyDbHits(inner.getRelationshipsByType(tokenReadSession, relType))

  override def nodeCursor(): NodeCursor = manyDbHits(inner.nodeCursor())

  override def relationshipScanCursor(): RelationshipScanCursor = manyDbHits(inner.relationshipScanCursor())

  override def propertyCursor(): PropertyCursor = manyDbHits(inner.propertyCursor())

  override def traversalCursor(): RelationshipTraversalCursor = manyDbHits(inner.traversalCursor())

  override def singleRelationship(id: Long, cursor: RelationshipScanCursor): Unit =  singleDbHit(inner.singleRelationship(id, cursor))

  override def relationshipById(relationshipId: Long, startNodeId: Long, endNodeId: Long, typeId: Int): RelationshipValue =
    inner.relationshipById(relationshipId, startNodeId, endNodeId, typeId)

  override def nodeOps: NodeOperations = inner.nodeOps

  override def relationshipOps: RelationshipOperations = inner.relationshipOps

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int =
    singleDbHit(inner.removeLabelsFromNode(node, labelIds))

  override def getPropertyKeyName(propertyKeyId: Int): String = singleDbHit(inner.getPropertyKeyName(propertyKeyId))

  override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] =
    singleDbHit(inner.getOptPropertyKeyId(propertyKeyName))

  override def getPropertyKeyId(propertyKey: String): Int = singleDbHit(inner.getPropertyKeyId(propertyKey))

  override def getOrCreatePropertyKeyId(propertyKey: String): Int = singleDbHit(inner.getOrCreatePropertyKeyId(propertyKey))

  override def getOrCreatePropertyKeyIds(propertyKeys: Array[String]): Array[Int] = {
    manyDbHits(propertyKeys.length)
    inner.getOrCreatePropertyKeyIds(propertyKeys)
  }

  override def addBtreeIndexRule(entityId: Int, isNodeIndex: Boolean, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[String], indexConfig: IndexConfig): IndexDescriptor =
    singleDbHit(inner.addBtreeIndexRule(entityId, isNodeIndex, propertyKeyIds, name, provider, indexConfig))

  override def addLookupIndexRule(isNodeIndex: Boolean, name: Option[String]): IndexDescriptor =
    singleDbHit(inner.addLookupIndexRule(isNodeIndex, name))

  override def dropIndexRule(labelId: Int, propertyKeyIds: Seq[Int]): Unit = singleDbHit(inner.dropIndexRule(labelId, propertyKeyIds))

  override def dropIndexRule(name: String): Unit = singleDbHit(inner.dropIndexRule(name))

  override def getAllIndexes(): Map[IndexDescriptor, IndexInfo] = singleDbHit(inner.getAllIndexes())

  override def indexExists(name: String): Boolean = singleDbHit(inner.indexExists(name))

  override def constraintExists(name: String): Boolean = singleDbHit(inner.constraintExists(name))

  override def constraintExists(matchFn: ConstraintDescriptor => Boolean, entityId: Int, properties: Int*): Boolean =
    singleDbHit(inner.constraintExists(matchFn, entityId, properties: _*))

  override def btreeIndexReference(entityId: Int, isNodeIndex: Boolean, properties: Int*): IndexDescriptor = singleDbHit(inner.btreeIndexReference(entityId, isNodeIndex, properties:_*))

  override def lookupIndexReference(isNodeIndex: Boolean): IndexDescriptor = singleDbHit(inner.lookupIndexReference(isNodeIndex))

  override def nodeIndexSeek(index: IndexReadSession,
                             needsValues: Boolean,
                             indexOrder: IndexOrder,
                             queries: Seq[PropertyIndexQuery]): NodeValueIndexCursor =
    manyDbHits(inner.nodeIndexSeek(index, needsValues, indexOrder, queries))

  override def nodeIndexScan(index: IndexReadSession,
                             needsValues: Boolean,
                             indexOrder: IndexOrder): NodeValueIndexCursor =
    manyDbHits(inner.nodeIndexScan(index, needsValues, indexOrder))

  override def nodeIndexSeekByContains(index: IndexReadSession,
                                       needsValues: Boolean,
                                       indexOrder: IndexOrder,
                                       value: TextValue): NodeValueIndexCursor =
    manyDbHits(inner.nodeIndexSeekByContains(index, needsValues, indexOrder, value))

  override def nodeIndexSeekByEndsWith(index: IndexReadSession,
                                       needsValues: Boolean,
                                       indexOrder: IndexOrder,
                                       value: TextValue): NodeValueIndexCursor =
    manyDbHits(inner.nodeIndexSeekByEndsWith(index, needsValues, indexOrder, value))

  override def relationshipIndexSeek(index: IndexReadSession,
                                     needsValues: Boolean,
                                     indexOrder: IndexOrder,
                                     queries: Seq[PropertyIndexQuery]): RelationshipValueIndexCursor =
    manyDbHits(inner.relationshipIndexSeek(index, needsValues, indexOrder, queries))

  override def relationshipIndexSeekByContains(index: IndexReadSession,
                                               needsValues: Boolean,
                                               indexOrder: IndexOrder,
                                               value: TextValue): RelationshipValueIndexCursor =
    manyDbHits(inner.relationshipIndexSeekByContains(index, needsValues, indexOrder, value))

  override def relationshipIndexSeekByEndsWith(index: IndexReadSession,
                                               needsValues: Boolean,
                                               indexOrder: IndexOrder,
                                               value: TextValue): RelationshipValueIndexCursor =
    manyDbHits(inner.relationshipIndexSeekByEndsWith(index, needsValues, indexOrder, value))

  override def relationshipIndexScan(index: IndexReadSession,
                                     needsValues: Boolean,
                                     indexOrder: IndexOrder): RelationshipValueIndexCursor =
    manyDbHits(inner.relationshipIndexScan(index, needsValues, indexOrder))

  override def getNodesByLabel(id: Int, indexOrder: IndexOrder): ClosingIterator[NodeValue] =
    manyDbHits(inner.getNodesByLabel(id, indexOrder))

  override def getNodesByLabelPrimitive(id: Int, indexOrder: IndexOrder): ClosingLongIterator =
    manyDbHits(inner.getNodesByLabelPrimitive(id, indexOrder))

  override def getNodesByLabel(tokenReadSession: TokenReadSession,
                               id: Int,
                               indexOrder: IndexOrder): ClosingIterator[NodeValue] =
    manyDbHits(inner.getNodesByLabel(tokenReadSession, id, indexOrder))

  override def getNodesByLabelPrimitive(tokenReadSession: TokenReadSession, id: Int, indexOrder: IndexOrder): ClosingLongIterator =
    manyDbHits(inner.getNodesByLabelPrimitive(tokenReadSession, id, indexOrder))

  override def nodeAsMap(id: Long, nodeCursor: NodeCursor, propertyCursor: PropertyCursor): MapValue = {
    val map = inner.nodeAsMap(id, nodeCursor, propertyCursor)
    //one hit finding the node, then finding the properies
    manyDbHits(1 + map.size())
    map
  }

  override def relationshipAsMap(id: Long, relationshipCursor: RelationshipScanCursor, propertyCursor: PropertyCursor): MapValue = {
    val map = inner.relationshipAsMap(id, relationshipCursor, propertyCursor)
    manyDbHits(1 + map.size())
    map
  }

  override def createNodeKeyConstraint(labelId: Int, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[String], indexConfig: IndexConfig): Unit =
    singleDbHit(inner.createNodeKeyConstraint(labelId, propertyKeyIds, name, provider, indexConfig))

  override def dropNodeKeyConstraint(labelId: Int, propertyKeyIds: Seq[Int]): Unit =
    singleDbHit(inner.dropNodeKeyConstraint(labelId, propertyKeyIds))

  override def createUniqueConstraint(labelId: Int, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[String], indexConfig: IndexConfig): Unit =
    singleDbHit(inner.createUniqueConstraint(labelId, propertyKeyIds, name, provider, indexConfig))

  override def dropUniqueConstraint(labelId: Int, propertyKeyIds: Seq[Int]): Unit =
    singleDbHit(inner.dropUniqueConstraint(labelId, propertyKeyIds))

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int, name: Option[String]): Unit =
    singleDbHit(inner.createNodePropertyExistenceConstraint(labelId, propertyKeyId, name))

  override def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): Unit =
    singleDbHit(inner.dropNodePropertyExistenceConstraint(labelId, propertyKeyId))

  override def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int, name: Option[String]): Unit =
    singleDbHit(inner.createRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId, name))

  override def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): Unit =
    singleDbHit(inner.dropRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId))

  override def dropNamedConstraint(name: String): Unit =
    singleDbHit(inner.dropNamedConstraint(name))

  override def getAllConstraints(): Map[ConstraintDescriptor, ConstraintInfo] = singleDbHit(inner.getAllConstraints())

  override def nodeLockingUniqueIndexSeek(index: IndexDescriptor,
                                          queries: Seq[PropertyIndexQuery.ExactPredicate]): NodeValueIndexCursor =
    singleDbHit(inner.nodeLockingUniqueIndexSeek(index, queries))

  override def getRelTypeId(relType: String): Int = singleDbHit(inner.getRelTypeId(relType))

  override def getOptRelTypeId(relType: String): Option[Int] = singleDbHit(inner.getOptRelTypeId(relType))

  override def getRelTypeName(id: Int): String = singleDbHit(inner.getRelTypeName(id))

  override def getImportURL(url: URL): Either[String,URL] = inner.getImportURL(url)

  override def nodeGetOutgoingDegreeWithMax(maxDegree: Int, node: Long, nodeCursor: NodeCursor): Int = singleDbHit(inner.nodeGetOutgoingDegreeWithMax(maxDegree, node, nodeCursor))

  override def nodeGetOutgoingDegreeWithMax(maxDegree: Int, node: Long, relationship: Int, nodeCursor: NodeCursor): Int = singleDbHit(inner.nodeGetOutgoingDegreeWithMax(maxDegree, node, relationship, nodeCursor))

  override def nodeGetIncomingDegreeWithMax(maxDegree: Int, node: Long, nodeCursor: NodeCursor): Int = singleDbHit(inner.nodeGetIncomingDegreeWithMax(maxDegree, node, nodeCursor))

  override def nodeGetIncomingDegreeWithMax(maxDegree: Int, node: Long, relationship: Int, nodeCursor: NodeCursor): Int = singleDbHit(inner.nodeGetIncomingDegreeWithMax(maxDegree, node, relationship, nodeCursor))

  override def nodeGetTotalDegreeWithMax(maxDegree: Int, node: Long, nodeCursor: NodeCursor): Int = singleDbHit(inner.nodeGetTotalDegreeWithMax(maxDegree, node, nodeCursor))

  override def nodeGetTotalDegreeWithMax(maxDegree: Int, node: Long, relationship: Int, nodeCursor: NodeCursor): Int = singleDbHit(inner.nodeGetTotalDegreeWithMax(maxDegree, node, relationship, nodeCursor))

  override def nodeGetOutgoingDegree(node: Long, nodeCursor: NodeCursor): Int = singleDbHit(inner.nodeGetOutgoingDegree(node, nodeCursor))

  override def nodeGetOutgoingDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int = singleDbHit(inner.nodeGetOutgoingDegree(node, relationship, nodeCursor))

  override def nodeGetIncomingDegree(node: Long, nodeCursor: NodeCursor): Int = singleDbHit(inner.nodeGetIncomingDegree(node, nodeCursor))

  override def nodeGetIncomingDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int = singleDbHit(inner.nodeGetIncomingDegree(node, relationship, nodeCursor))

  override def nodeGetTotalDegree(node: Long, nodeCursor: NodeCursor): Int = singleDbHit(inner.nodeGetTotalDegree(node, nodeCursor))

  override def nodeGetTotalDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int = singleDbHit(inner.nodeGetTotalDegree(node, relationship, nodeCursor))

  override def nodeHasCheapDegrees(node: Long, nodeCursor: NodeCursor): Boolean = singleDbHit(inner.nodeHasCheapDegrees(node, nodeCursor))

  override def isLabelSetOnNode(label: Int, node: Long, nodeCursor: NodeCursor): Boolean = singleDbHit(inner.isLabelSetOnNode(label, node, nodeCursor))

  override def isTypeSetOnRelationship(typ: Int, id: Long, relationshipCursor: RelationshipScanCursor): Boolean =
    singleDbHit(inner.isTypeSetOnRelationship(typ, id, relationshipCursor))

  override def nodeCountByCountStore(labelId: Int): Long = singleDbHit(inner.nodeCountByCountStore(labelId))

  override def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long =
    singleDbHit(inner.relationshipCountByCountStore(startLabelId, typeId, endLabelId))

  override def lockNodes(nodeIds: Long*): Unit = inner.lockNodes(nodeIds:_*)

  override def lockRelationships(relIds: Long*): Unit = inner.lockRelationships(relIds:_*)

  override def singleShortestPath(left: Long, right: Long, depth: Int, expander: Expander,
                                  pathPredicate: KernelPredicate[Path],
                                  filters: Seq[KernelPredicate[Entity]],
                                  memoryTracker: MemoryTracker): Option[Path] =
    singleDbHit(inner.singleShortestPath(left, right, depth, expander, pathPredicate, filters, memoryTracker))

  override def allShortestPath(left: Long, right: Long, depth: Int, expander: Expander,
                               pathPredicate: KernelPredicate[Path],
                               filters: Seq[KernelPredicate[Entity]],
                               memoryTracker: MemoryTracker): ClosingIterator[Path] =
    manyDbHits(inner.allShortestPath(left, right, depth, expander, pathPredicate, filters, memoryTracker))

  override def callReadOnlyProcedure(id: Int, args: Array[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    unknownDbHits(inner.callReadOnlyProcedure(id, args, allowed, context))

  override def callReadWriteProcedure(id: Int, args: Array[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    unknownDbHits(inner.callReadWriteProcedure(id, args, allowed, context))

  override def callSchemaWriteProcedure(id: Int, args: Array[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    unknownDbHits(inner.callSchemaWriteProcedure(id, args, allowed, context))

  override def callDbmsProcedure(id: Int, args: Array[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    unknownDbHits(inner.callDbmsProcedure(id, args, allowed, context))

  override def callFunction(id: Int, args: Array[AnyValue], allowed: Array[String]): AnyValue =
    singleDbHit(inner.callFunction(id, args, allowed))

  override def aggregateFunction(id: Int,
                                 allowed: Array[String]): UserDefinedAggregator =
    singleDbHit(inner.aggregateFunction(id, allowed))

  override def detachDeleteNode(node: Long): Int = {
    val deletedRelationships = inner.detachDeleteNode(node)
    manyDbHits(1 + deletedRelationships) // This relies on the assumption that the node was not already deleted
    deletedRelationships
  }

  override def assertSchemaWritesAllowed(): Unit = inner.assertSchemaWritesAllowed()

  override def assertShowIndexAllowed(): Unit = inner.assertShowIndexAllowed()

  override def assertShowConstraintAllowed(): Unit = inner.assertShowConstraintAllowed()

  override def asObject(value: AnyValue): AnyRef = inner.asObject(value)

  override def getTxStateNodePropertyOrNull(nodeId: Long,
                                            propertyKey: Int): Value =
    inner.getTxStateNodePropertyOrNull(nodeId, propertyKey)

  override def getTxStateRelationshipPropertyOrNull(relId: Long, propertyKey: Int): Value =
    inner.getTxStateRelationshipPropertyOrNull(relId, propertyKey)

}

class DelegatingOperations[T, CURSOR](protected val inner: Operations[T, CURSOR]) extends Operations[T, CURSOR] {

  protected def singleDbHit[A](value: A): A = value
  protected def manyDbHits[A](value: ClosingIterator[A]): ClosingIterator[A] = value

  protected def manyDbHits[A](value: ClosingLongIterator): ClosingLongIterator = value

  override def delete(id: Long): Boolean = singleDbHit(inner.delete(id))

  override def setProperty(obj: Long, propertyKey: Int, value: Value): Unit =
    singleDbHit(inner.setProperty(obj, propertyKey, value))

  override def getById(id: Long): T = inner.getById(id)

  override def getProperty(obj: Long, propertyKeyId: Int, cursor: CURSOR, propertyCursor: PropertyCursor, throwOnDeleted: Boolean): Value =
    singleDbHit(inner.getProperty(obj, propertyKeyId, cursor, propertyCursor, throwOnDeleted))

  override def getTxStateProperty(obj: Long, propertyKeyId: Int): Value = inner.getTxStateProperty(obj, propertyKeyId)

  override def hasProperty(obj: Long, propertyKeyId: Int, cursor: CURSOR, propertyCursor: PropertyCursor): Boolean =
    singleDbHit(inner.hasProperty(obj, propertyKeyId, cursor, propertyCursor))

  override def hasTxStatePropertyForCachedProperty(nodeId: Long, propertyKeyId: Int): Option[Boolean] =
    inner.hasTxStatePropertyForCachedProperty(nodeId, propertyKeyId)

  override def propertyKeyIds(obj: Long, cursor: CURSOR, propertyCursor: PropertyCursor): Array[Int] =
    singleDbHit(inner.propertyKeyIds(obj, cursor, propertyCursor))

  override def removeProperty(obj: Long, propertyKeyId: Int): Boolean = singleDbHit(inner.removeProperty(obj, propertyKeyId))

  override def all: ClosingIterator[T] = manyDbHits(inner.all)

  override def allPrimitive: ClosingLongIterator = manyDbHits(inner.allPrimitive)

  override def isDeletedInThisTx(id: Long): Boolean = inner.isDeletedInThisTx(id)

  override def acquireExclusiveLock(obj: Long): Unit = inner.acquireExclusiveLock(obj)

  override def releaseExclusiveLock(obj: Long): Unit = inner.releaseExclusiveLock(obj)

  override def getByIdIfExists(id: Long): Option[T] = singleDbHit(inner.getByIdIfExists(id))
}

class DelegatingQueryTransactionalContext(val inner: QueryTransactionalContext) extends QueryTransactionalContext {

  override def commitAndRestartTx() { inner.commitAndRestartTx() }

  override def isTopLevelTx: Boolean = inner.isTopLevelTx

  override def close() { inner.close() }

  override def kernelStatisticProvider: KernelStatisticProvider = inner.kernelStatisticProvider

  override def dbmsInfo: DbmsInfo = inner.dbmsInfo

  override def databaseId: NamedDatabaseId = inner.databaseId

  override def transaction: KernelTransaction = inner.transaction

  override def cursors: CursorFactory = inner.cursors

  override def dataRead: Read = inner.dataRead

  override def tokenRead: TokenRead = inner.tokenRead

  override def schemaRead: SchemaRead = inner.schemaRead

  override def dataWrite: Write = inner.dataWrite

  override def rollback(): Unit = inner.rollback()
}
