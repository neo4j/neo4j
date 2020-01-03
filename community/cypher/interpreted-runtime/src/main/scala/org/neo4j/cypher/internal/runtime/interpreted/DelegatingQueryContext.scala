/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.eclipse.collections.api.iterator.LongIterator
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.profiling.KernelStatisticProvider
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.graphdb.{Entity, Path}
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
import org.neo4j.internal.kernel.api.{QueryContext => _, _}
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.dbms.DbmsOperations
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.impl.core.TransactionalEntityFactory
import org.neo4j.kernel.impl.factory.DatabaseInfo
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{TextValue, Value}
import org.neo4j.values.virtual.{ListValue, MapValue, NodeValue, RelationshipValue}

import scala.collection.Iterator

abstract class DelegatingQueryContext(val inner: QueryContext) extends QueryContext {

  protected def singleDbHit[A](value: A): A = value
  protected def unknownDbHits[A](value: A): A = value
  protected def manyDbHits[A](value: Iterator[A]): Iterator[A] = value

  protected def manyDbHits(value: LongIterator): LongIterator = value
  protected def manyDbHits(value: RelationshipIterator): RelationshipIterator = value
  protected def manyDbHits(value: RelationshipSelectionCursor): RelationshipSelectionCursor = value
  protected def manyDbHits(value: NodeValueIndexCursor): NodeValueIndexCursor = value
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

  override def getRelationshipsForIds(node: Long, dir: SemanticDirection, types: Array[Int]): Iterator[RelationshipValue] =
  manyDbHits(inner.getRelationshipsForIds(node, dir, types))

  override def getRelationshipsForIdsPrimitive(node: Long, dir: SemanticDirection, types: Array[Int]): RelationshipIterator =
  manyDbHits(inner.getRelationshipsForIdsPrimitive(node, dir, types))

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

  override def addIndexRule(labelId: Int, propertyKeyIds: Seq[Int], name: Option[String]): IndexDescriptor = singleDbHit(inner.addIndexRule(labelId, propertyKeyIds, name))

  override def dropIndexRule(labelId: Int, propertyKeyIds: Seq[Int]): Unit = singleDbHit(inner.dropIndexRule(labelId, propertyKeyIds))

  override def dropIndexRule(name: String): Unit = singleDbHit(inner.dropIndexRule(name))

  override def indexReference(label: Int, properties: Int*): IndexDescriptor = singleDbHit(inner.indexReference(label, properties:_*))

  override def indexSeek[RESULT <: AnyRef](index: IndexReadSession,
                                           needsValues: Boolean,
                                           indexOrder: IndexOrder,
                                           queries: Seq[IndexQuery]): NodeValueIndexCursor =
    manyDbHits(inner.indexSeek(index, needsValues, indexOrder, queries))

  override def indexScan[RESULT <: AnyRef](index: IndexReadSession,
                                           needsValues: Boolean,
                                           indexOrder: IndexOrder): NodeValueIndexCursor =
    manyDbHits(inner.indexScan(index, needsValues, indexOrder))

  override def indexSeekByContains[RESULT <: AnyRef](index: IndexReadSession,
                                                     needsValues: Boolean,
                                                     indexOrder: IndexOrder,
                                                     value: TextValue): NodeValueIndexCursor =
    manyDbHits(inner.indexSeekByContains(index, needsValues, indexOrder, value))

  override def indexSeekByEndsWith[RESULT <: AnyRef](index: IndexReadSession,
                                                     needsValues: Boolean,
                                                     indexOrder: IndexOrder,
                                                     value: TextValue): NodeValueIndexCursor =
    manyDbHits(inner.indexSeekByEndsWith(index, needsValues, indexOrder, value))

  override def getNodesByLabel(id: Int): Iterator[NodeValue] = manyDbHits(inner.getNodesByLabel(id))

  override def getNodesByLabelPrimitive(id: Int): LongIterator = manyDbHits(inner.getNodesByLabelPrimitive(id))

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

  override def createNodeKeyConstraint(labelId: Int, propertyKeyIds: Seq[Int], name: Option[String]): Unit =
    singleDbHit(inner.createNodeKeyConstraint(labelId, propertyKeyIds, name))

  override def dropNodeKeyConstraint(labelId: Int, propertyKeyIds: Seq[Int]): Unit =
    singleDbHit(inner.dropNodeKeyConstraint(labelId, propertyKeyIds))

  override def createUniqueConstraint(labelId: Int, propertyKeyIds: Seq[Int], name: Option[String]): Unit =
    singleDbHit(inner.createUniqueConstraint(labelId, propertyKeyIds, name))

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

  override def lockingUniqueIndexSeek[RESULT](index: IndexDescriptor,
                                              queries: Seq[IndexQuery.ExactPredicate]): NodeValueIndexCursor =
    singleDbHit(inner.lockingUniqueIndexSeek(index, queries))

  override def getRelTypeId(relType: String): Int = singleDbHit(inner.getRelTypeId(relType))

  override def getOptRelTypeId(relType: String): Option[Int] = singleDbHit(inner.getOptRelTypeId(relType))

  override def getRelTypeName(id: Int): String = singleDbHit(inner.getRelTypeName(id))

  override def getImportURL(url: URL): Either[String,URL] = inner.getImportURL(url)

  override def nodeGetOutgoingDegree(node: Long, nodeCursor: NodeCursor): Int = singleDbHit(inner.nodeGetOutgoingDegree(node, nodeCursor))

  override def nodeGetOutgoingDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int = singleDbHit(inner.nodeGetOutgoingDegree(node, relationship, nodeCursor))

  override def nodeGetIncomingDegree(node: Long, nodeCursor: NodeCursor): Int = singleDbHit(inner.nodeGetIncomingDegree(node, nodeCursor))

  override def nodeGetIncomingDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int = singleDbHit(inner.nodeGetIncomingDegree(node, relationship, nodeCursor))

  override def nodeGetTotalDegree(node: Long, nodeCursor: NodeCursor): Int = singleDbHit(inner.nodeGetTotalDegree(node, nodeCursor))

  override def nodeGetTotalDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int = singleDbHit(inner.nodeGetTotalDegree(node, relationship, nodeCursor))

  override def nodeIsDense(node: Long, nodeCursor: NodeCursor): Boolean = singleDbHit(inner.nodeIsDense(node, nodeCursor))

  override def variableLengthPathExpand(realNode: Long,
                                        minHops: Option[Int],
                                        maxHops: Option[Int],
                                        direction: SemanticDirection,
                                        relTypes: Seq[String]): Iterator[Path] =
    manyDbHits(inner.variableLengthPathExpand(realNode, minHops, maxHops, direction, relTypes))

  override def isLabelSetOnNode(label: Int, node: Long, nodeCursor: NodeCursor): Boolean = singleDbHit(inner.isLabelSetOnNode(label, node, nodeCursor))

  override def nodeCountByCountStore(labelId: Int): Long = singleDbHit(inner.nodeCountByCountStore(labelId))

  override def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long =
    singleDbHit(inner.relationshipCountByCountStore(startLabelId, typeId, endLabelId))

  override def lockNodes(nodeIds: Long*): Unit = inner.lockNodes(nodeIds:_*)

  override def lockRelationships(relIds: Long*): Unit = inner.lockRelationships(relIds:_*)

  override def singleShortestPath(left: Long, right: Long, depth: Int, expander: Expander,
                                  pathPredicate: KernelPredicate[Path],
                                  filters: Seq[KernelPredicate[Entity]]): Option[Path] =
    singleDbHit(inner.singleShortestPath(left, right, depth, expander, pathPredicate, filters))

  override def allShortestPath(left: Long, right: Long, depth: Int, expander: Expander,
                               pathPredicate: KernelPredicate[Path],
                               filters: Seq[KernelPredicate[Entity]]): Iterator[Path] =
    manyDbHits(inner.allShortestPath(left, right, depth, expander, pathPredicate, filters))

  override def callReadOnlyProcedure(id: Int, args: Seq[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    unknownDbHits(inner.callReadOnlyProcedure(id, args, allowed, context))

  override def callReadWriteProcedure(id: Int, args: Seq[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    unknownDbHits(inner.callReadWriteProcedure(id, args, allowed, context))

  override def callSchemaWriteProcedure(id: Int, args: Seq[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    unknownDbHits(inner.callSchemaWriteProcedure(id, args, allowed, context))

  override def callDbmsProcedure(id: Int, args: Seq[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    unknownDbHits(inner.callDbmsProcedure(id, args, allowed, context))

  override def callFunction(id: Int, args: Array[AnyValue], allowed: Array[String]): AnyValue =
    singleDbHit(inner.callFunction(id, args, allowed))

  override def aggregateFunction(id: Int,
                                 allowed: Array[String]): UserDefinedAggregator =
    singleDbHit(inner.aggregateFunction(id, allowed))

  override def detachDeleteNode(node: Long): Int = manyDbHits(inner.detachDeleteNode(node))

  override def assertSchemaWritesAllowed(): Unit = inner.assertSchemaWritesAllowed()

  override def asObject(value: AnyValue): AnyRef = inner.asObject(value)

  override def getTxStateNodePropertyOrNull(nodeId: Long,
                                            propertyKey: Int): Value =
    inner.getTxStateNodePropertyOrNull(nodeId, propertyKey)

  override def getTxStateRelationshipPropertyOrNull(relId: Long, propertyKey: Int): Value =
    inner.getTxStateRelationshipPropertyOrNull(relId, propertyKey)

}

class DelegatingOperations[T, CURSOR](protected val inner: Operations[T, CURSOR]) extends Operations[T, CURSOR] {

  protected def singleDbHit[A](value: A): A = value
  protected def manyDbHits[A](value: Iterator[A]): Iterator[A] = value

  protected def manyDbHits[A](value: LongIterator): LongIterator = value

  override def delete(id: Long): Unit = singleDbHit(inner.delete(id))

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

  override def removeProperty(obj: Long, propertyKeyId: Int): Unit = singleDbHit(inner.removeProperty(obj, propertyKeyId))

  override def all: Iterator[T] = manyDbHits(inner.all)

  override def allPrimitive: LongIterator = manyDbHits(inner.allPrimitive)

  override def isDeletedInThisTx(id: Long): Boolean = inner.isDeletedInThisTx(id)

  override def acquireExclusiveLock(obj: Long): Unit = inner.acquireExclusiveLock(obj)

  override def releaseExclusiveLock(obj: Long): Unit = inner.releaseExclusiveLock(obj)

  override def getByIdIfExists(id: Long): Option[T] = singleDbHit(inner.getByIdIfExists(id))
}

class DelegatingQueryTransactionalContext(val inner: QueryTransactionalContext) extends QueryTransactionalContext {

  override def dbmsOperations: DbmsOperations = inner.dbmsOperations

  override def commitAndRestartTx() { inner.commitAndRestartTx() }

  override def isTopLevelTx: Boolean = inner.isTopLevelTx

  override def close() { inner.close() }

  override def kernelStatisticProvider: KernelStatisticProvider = inner.kernelStatisticProvider

  override def databaseInfo: DatabaseInfo = inner.databaseInfo

  override def databaseId: NamedDatabaseId = inner.databaseId

  override def transaction: KernelTransaction = inner.transaction

  override def cursors: CursorFactory = inner.cursors

  override def dataRead: Read = inner.dataRead

  override def tokenRead: TokenRead = inner.tokenRead

  override def schemaRead: SchemaRead = inner.schemaRead

  override def dataWrite: Write = inner.dataWrite

  override def rollback(): Unit = inner.rollback()
}
