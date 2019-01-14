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

import java.net.URL

import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.cypher.internal.planner.v3_4.spi.{IndexDescriptor, KernelStatisticProvider}
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.cypher.internal.v3_4.logical.plans.QualifiedName
import org.neo4j.graphdb.{Node, Path, PropertyContainer}
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor
import org.neo4j.internal.kernel.api.{CursorFactory, IndexReference, Read, Write, _}
import org.neo4j.kernel.api.dbms.DbmsOperations
import org.neo4j.kernel.impl.api.store.RelationshipIterator
import org.neo4j.kernel.impl.core.EmbeddedProxySPI
import org.neo4j.kernel.impl.factory.DatabaseInfo
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.{ListValue, NodeValue, RelationshipValue}

import scala.collection.Iterator

abstract class DelegatingQueryContext(val inner: QueryContext) extends QueryContext {

  protected def singleDbHit[A](value: A): A = value
  protected def manyDbHits[A](value: Iterator[A]): Iterator[A] = value
  protected def manyDbHits[A](value: PrimitiveLongIterator): PrimitiveLongIterator = value
  protected def manyDbHits[A](value: RelationshipIterator): RelationshipIterator = value
  protected def manyDbHits[A](value: RelationshipSelectionCursor): RelationshipSelectionCursor = value
  protected def manyDbHits(count: Int): Int = count

  override def resources: CloseableResource = inner.resources

  override def transactionalContext: QueryTransactionalContext = inner.transactionalContext

  override def entityAccessor: EmbeddedProxySPI = inner.entityAccessor

  override def withActiveRead: QueryContext = inner.withActiveRead

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int =
    singleDbHit(inner.setLabelsOnNode(node, labelIds))

  override def createNode(): Node = singleDbHit(inner.createNode())

  override def createNodeId(): Long = singleDbHit(inner.createNodeId())

  override def createRelationship(start: Long, end: Long, relType: Int): RelationshipValue =
    singleDbHit(inner.createRelationship(start, end, relType))

  override def getOrCreateRelTypeId(relTypeName: String): Int = singleDbHit(inner.getOrCreateRelTypeId(relTypeName))

  override def getLabelsForNode(node: Long): ListValue = singleDbHit(inner.getLabelsForNode(node))

  override def getLabelName(id: Int): String = singleDbHit(inner.getLabelName(id))

  override def getOptLabelId(labelName: String): Option[Int] = singleDbHit(inner.getOptLabelId(labelName))

  override def getLabelId(labelName: String): Int = singleDbHit(inner.getLabelId(labelName))

  override def getOrCreateLabelId(labelName: String): Int = singleDbHit(inner.getOrCreateLabelId(labelName))

  override def getRelationshipsForIds(node: Long, dir: SemanticDirection, types: Option[Array[Int]]): Iterator[RelationshipValue] =
  manyDbHits(inner.getRelationshipsForIds(node, dir, types))

  override def getRelationshipsForIdsPrimitive(node: Long, dir: SemanticDirection, types: Option[Array[Int]]): RelationshipIterator =
  manyDbHits(inner.getRelationshipsForIdsPrimitive(node, dir, types))

  override def getRelationshipsCursor(node: Long, dir: SemanticDirection, types: Option[Array[Int]]): RelationshipSelectionCursor =
    manyDbHits(inner.getRelationshipsCursor(node, dir, types))

  override def getRelationshipFor(relationshipId: Long, typeId: Int, startNodeId: Long, endNodeId: Long): RelationshipValue =
    inner.getRelationshipFor(relationshipId, typeId, startNodeId, endNodeId)

  override def nodeOps = inner.nodeOps

  override def relationshipOps = inner.relationshipOps

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int =
    singleDbHit(inner.removeLabelsFromNode(node, labelIds))

  override def getPropertyKeyName(propertyKeyId: Int): String = singleDbHit(inner.getPropertyKeyName(propertyKeyId))

  override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] =
    singleDbHit(inner.getOptPropertyKeyId(propertyKeyName))

  override def getPropertyKeyId(propertyKey: String) = singleDbHit(inner.getPropertyKeyId(propertyKey))

  override def getOrCreatePropertyKeyId(propertyKey: String) = singleDbHit(inner.getOrCreatePropertyKeyId(propertyKey))

  override def addIndexRule(descriptor: IndexDescriptor) = singleDbHit(inner.addIndexRule(descriptor))

  override def dropIndexRule(descriptor: IndexDescriptor) = singleDbHit(inner.dropIndexRule(descriptor))


  override def indexReference(label: Int, properties: Int*): IndexReference = singleDbHit(inner.indexReference(label, properties:_*))

  override def indexSeek(index: IndexReference, values: Seq[IndexQuery]): Iterator[NodeValue] =
    manyDbHits(inner.indexSeek(index, values))

  override def indexScan(index: IndexReference): Iterator[NodeValue] = manyDbHits(inner.indexScan(index))

  override def indexScanPrimitive(index: IndexReference): PrimitiveLongIterator = manyDbHits(inner.indexScanPrimitive(index))

  override def indexScanByContains(index: IndexReference, value: String): scala.Iterator[NodeValue] =
    manyDbHits(inner.indexScanByContains(index, value))

  override def indexScanByEndsWith(index: IndexReference, value: String): scala.Iterator[NodeValue] =
    manyDbHits(inner.indexScanByEndsWith(index, value))

  override def getNodesByLabel(id: Int): Iterator[NodeValue] = manyDbHits(inner.getNodesByLabel(id))

  override def getNodesByLabelPrimitive(id: Int): PrimitiveLongIterator = manyDbHits(inner.getNodesByLabelPrimitive(id))

  override def getOrCreateFromSchemaState[K, V](key: K, creator: => V): V =
    singleDbHit(inner.getOrCreateFromSchemaState(key, creator))

  override def createNodeKeyConstraint(descriptor: IndexDescriptor): Boolean =
    singleDbHit(inner.createNodeKeyConstraint(descriptor))

  override def dropNodeKeyConstraint(descriptor: IndexDescriptor) =
    singleDbHit(inner.dropNodeKeyConstraint(descriptor))

  override def createUniqueConstraint(descriptor: IndexDescriptor): Boolean =
    singleDbHit(inner.createUniqueConstraint(descriptor))

  override def dropUniqueConstraint(descriptor: IndexDescriptor) =
    singleDbHit(inner.dropUniqueConstraint(descriptor))

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): Boolean =
    singleDbHit(inner.createNodePropertyExistenceConstraint(labelId, propertyKeyId))

  override def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int) =
    singleDbHit(inner.dropNodePropertyExistenceConstraint(labelId, propertyKeyId))

  override def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): Boolean =
    singleDbHit(inner.createRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId))

  override def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) =
    singleDbHit(inner.dropRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId))

  override def withAnyOpenQueryContext[T](work: (QueryContext) => T): T = inner.withAnyOpenQueryContext(work)

  override def lockingUniqueIndexSeek(index: IndexReference, values: Seq[IndexQuery.ExactPredicate]): Option[NodeValue] =
    singleDbHit(inner.lockingUniqueIndexSeek(index, values))

  override def getRelTypeId(relType: String): Int = singleDbHit(inner.getRelTypeId(relType))

  override def getOptRelTypeId(relType: String): Option[Int] = singleDbHit(inner.getOptRelTypeId(relType))

  override def getRelTypeName(id: Int): String = singleDbHit(inner.getRelTypeName(id))

  override def getImportURL(url: URL): Either[String,URL] = inner.getImportURL(url)

  override def edgeGetStartNode(edge: RelationshipValue) = inner.edgeGetStartNode(edge)

  override def edgeGetEndNode(edge: RelationshipValue) = inner.edgeGetEndNode(edge)

  override def nodeGetDegree(node: Long, dir: SemanticDirection): Int = singleDbHit(inner.nodeGetDegree(node, dir))

  override def nodeGetDegree(node: Long, dir: SemanticDirection, relTypeId: Int): Int =
    singleDbHit(inner.nodeGetDegree(node, dir, relTypeId))

  override def nodeIsDense(node: Long): Boolean = singleDbHit(inner.nodeIsDense(node))

  override def variableLengthPathExpand(realNode: Long,
                                        minHops: Option[Int],
                                        maxHops: Option[Int],
                                        direction: SemanticDirection,
                                        relTypes: Seq[String]): Iterator[Path] =
    manyDbHits(inner.variableLengthPathExpand(realNode, minHops, maxHops, direction, relTypes))

  override def isLabelSetOnNode(label: Int, node: Long): Boolean = singleDbHit(inner.isLabelSetOnNode(label, node))

  override def nodeCountByCountStore(labelId: Int): Long = singleDbHit(inner.nodeCountByCountStore(labelId))

  override def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long =
    singleDbHit(inner.relationshipCountByCountStore(startLabelId, typeId, endLabelId))

  override def lockNodes(nodeIds: Long*): Unit = inner.lockNodes(nodeIds:_*)

  override def lockRelationships(relIds: Long*): Unit = inner.lockRelationships(relIds:_*)

  override def singleShortestPath(left: Long, right: Long, depth: Int, expander: Expander,
                                  pathPredicate: KernelPredicate[Path],
                                  filters: Seq[KernelPredicate[PropertyContainer]]): Option[Path] =
    singleDbHit(inner.singleShortestPath(left, right, depth, expander, pathPredicate, filters))

  override def allShortestPath(left: Long, right: Long, depth: Int, expander: Expander,
                               pathPredicate: KernelPredicate[Path],
                               filters: Seq[KernelPredicate[PropertyContainer]]): Iterator[Path] =
    manyDbHits(inner.allShortestPath(left, right, depth, expander, pathPredicate, filters))

  override def callReadOnlyProcedure(id: Int, args: Seq[Any], allowed: Array[String]) =
    singleDbHit(inner.callReadOnlyProcedure(id, args, allowed))

  override def callReadWriteProcedure(id: Int, args: Seq[Any], allowed: Array[String]) =
    singleDbHit(inner.callReadWriteProcedure(id, args, allowed))

  override def callSchemaWriteProcedure(id: Int, args: Seq[Any], allowed: Array[String]) =
    singleDbHit(inner.callSchemaWriteProcedure(id, args, allowed))

  override def callDbmsProcedure(id: Int, args: Seq[Any], allowed: Array[String]) =
    inner.callDbmsProcedure(id, args, allowed)

  override def callReadOnlyProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) =
    singleDbHit(inner.callReadOnlyProcedure(name, args, allowed))

  override def callReadWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) =
    singleDbHit(inner.callReadWriteProcedure(name, args, allowed))

  override def callSchemaWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) =
    singleDbHit(inner.callSchemaWriteProcedure(name, args, allowed))

  override def callDbmsProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) =
    inner.callDbmsProcedure(name, args, allowed)

  override def callFunction(id: Int, args: Seq[AnyValue], allowed: Array[String]) =
    singleDbHit(inner.callFunction(id, args, allowed))

  override def callFunction(name: QualifiedName, args: Seq[AnyValue], allowed: Array[String]) =
    singleDbHit(inner.callFunction(name, args, allowed))

  override def aggregateFunction(id: Int,
                                 allowed: Array[String]): UserDefinedAggregator =
    singleDbHit(inner.aggregateFunction(id, allowed))

  override def aggregateFunction(name: QualifiedName,
                                 allowed: Array[String]): UserDefinedAggregator =
    singleDbHit(inner.aggregateFunction(name, allowed))

  override def isGraphKernelResultValue(v: Any): Boolean =
    inner.isGraphKernelResultValue(v)

  override def detachDeleteNode(node: Long): Int = manyDbHits(inner.detachDeleteNode(node))

  override def assertSchemaWritesAllowed(): Unit = inner.assertSchemaWritesAllowed()

  override def asObject(value: AnyValue): Any = inner.asObject(value)
}

class DelegatingOperations[T](protected val inner: Operations[T]) extends Operations[T] {

  protected def singleDbHit[A](value: A): A = value
  protected def manyDbHits[A](value: Iterator[A]): Iterator[A] = value
  protected def manyDbHits[A](value: PrimitiveLongIterator): PrimitiveLongIterator = value

  override def delete(id: Long): Unit = singleDbHit(inner.delete(id))

  override def setProperty(obj: Long, propertyKey: Int, value: Value): Unit =
    singleDbHit(inner.setProperty(obj, propertyKey, value))

  override def getById(id: Long): T = inner.getById(id)

  override def getProperty(obj: Long, propertyKeyId: Int): Value = singleDbHit(inner.getProperty(obj, propertyKeyId))

  override def hasProperty(obj: Long, propertyKeyId: Int): Boolean = singleDbHit(inner.hasProperty(obj, propertyKeyId))

  override def propertyKeyIds(obj: Long): Iterator[Int] = singleDbHit(inner.propertyKeyIds(obj))

  override def removeProperty(obj: Long, propertyKeyId: Int): Unit = singleDbHit(inner.removeProperty(obj, propertyKeyId))

  override def all: Iterator[T] = manyDbHits(inner.all)

  override def allPrimitive: PrimitiveLongIterator = manyDbHits(inner.allPrimitive)

  override def isDeletedInThisTx(id: Long): Boolean = inner.isDeletedInThisTx(id)

  override def acquireExclusiveLock(obj: Long): Unit = inner.acquireExclusiveLock(obj)

  override def releaseExclusiveLock(obj: Long): Unit = inner.releaseExclusiveLock(obj)

  override def getByIdIfExists(id: Long): Option[T] = singleDbHit(inner.getByIdIfExists(id))
}

class DelegatingQueryTransactionalContext(val inner: QueryTransactionalContext) extends QueryTransactionalContext {

  override def dbmsOperations: DbmsOperations = inner.dbmsOperations

  override def commitAndRestartTx() { inner.commitAndRestartTx() }

  override def isTopLevelTx: Boolean = inner.isTopLevelTx

  override def close(success: Boolean) { inner.close(success) }

  override def kernelStatisticProvider: KernelStatisticProvider = inner.kernelStatisticProvider

  override def databaseInfo: DatabaseInfo = inner.databaseInfo

  override def cursors: CursorFactory = inner.cursors

  override def dataRead: Read = inner.dataRead

  override def stableDataRead: Read = inner.stableDataRead

  override def markAsStable(): Unit = inner.markAsStable()

  override def tokenRead: TokenRead = inner.tokenRead

  override def schemaRead: SchemaRead = inner.schemaRead

  override def dataWrite: Write = inner.dataWrite
}
