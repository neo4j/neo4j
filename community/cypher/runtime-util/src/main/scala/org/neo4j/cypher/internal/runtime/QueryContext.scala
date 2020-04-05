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
package org.neo4j.cypher.internal.runtime

import java.net.URL
import java.util.Optional

import org.eclipse.collections.api.iterator.LongIterator
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.profiling.KernelStatisticProvider
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.exceptions.EntityNotFoundException
import org.neo4j.graphdb.{Entity, Path}
import org.neo4j.internal.kernel.api._
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.dbms.DbmsOperations
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.impl.core.TransactionalEntityFactory
import org.neo4j.kernel.impl.factory.DatabaseInfo
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{TextValue, Value}
import org.neo4j.values.virtual.{NodeValue, RelationshipValue}

import scala.collection.Iterator

/*
 * Developer note: This is an attempt at an internal graph database API, which defines a clean cut between
 * two layers, the query engine layer and, for lack of a better name, the core database layer.
 *
 * Building the query engine layer on top of an internal layer means we can move much faster, not
 * having to worry about deprecations and so on. It is also acceptable if this layer is a bit clunkier, in this
 * case we are, for instance, not exposing any node or relationship objects, but provide direct methods for manipulating
 * them by ids instead.
 *
 * The driver for this was clarifying who is responsible for ensuring query isolation. By exposing a query concept in
 * the core layer, we can move that responsibility outside of the scope of cypher.
 */
trait QueryContext extends TokenContext with DbAccess {

  // See QueryContextAdaptation if you need a dummy that overrides all methods as ??? for writing a test

  def entityAccessor: TransactionalEntityFactory

  def transactionalContext: QueryTransactionalContext

  def resources: ResourceManager

  def nodeOps: NodeOperations

  def relationshipOps: RelationshipOperations

  def createNode(labels: Array[Int]): NodeValue

  def createNodeId(labels: Array[Int]): Long

  def createRelationship(start: Long, end: Long, relType: Int): RelationshipValue

  def getOrCreateRelTypeId(relTypeName: String): Int

  def getRelationshipsForIds(node: Long, dir: SemanticDirection, types: Array[Int]): Iterator[RelationshipValue]

  def getRelationshipsForIdsPrimitive(node: Long, dir: SemanticDirection, types: Array[Int]): RelationshipIterator

  def getOrCreateLabelId(labelName: String): Int

  def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int

  def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int

  def getOrCreatePropertyKeyId(propertyKey: String): Int

  def getOrCreatePropertyKeyIds(propertyKeys: Array[String]): Array[Int]

  def addIndexRule(labelId: Int, propertyKeyIds: Seq[Int], name: Option[String]): IndexDescriptor

  def dropIndexRule(labelId: Int, propertyKeyIds: Seq[Int]): Unit

  def dropIndexRule(name: String): Unit

  def indexReference(label: Int, properties: Int*): IndexDescriptor

  def indexSeek[RESULT <: AnyRef](index: IndexReadSession,
                                  needsValues: Boolean,
                                  indexOrder: IndexOrder,
                                  queries: Seq[IndexQuery]): NodeValueIndexCursor

  def indexSeekByContains[RESULT <: AnyRef](index: IndexReadSession,
                                            needsValues: Boolean,
                                            indexOrder: IndexOrder,
                                            value: TextValue): NodeValueIndexCursor

  def indexSeekByEndsWith[RESULT <: AnyRef](index: IndexReadSession,
                                            needsValues: Boolean,
                                            indexOrder: IndexOrder,
                                            value: TextValue): NodeValueIndexCursor

  def indexScan[RESULT <: AnyRef](index: IndexReadSession,
                                  needsValues: Boolean,
                                  indexOrder: IndexOrder): NodeValueIndexCursor

  def lockingUniqueIndexSeek[RESULT](index: IndexDescriptor, queries: Seq[IndexQuery.ExactPredicate]): NodeValueIndexCursor

  def getNodesByLabel(id: Int): Iterator[NodeValue]

  def getNodesByLabelPrimitive(id: Int): LongIterator

  /* return true if the constraint was created, false if preexisting, throws if failed */
  def createNodeKeyConstraint(labelId: Int, propertyKeyIds: Seq[Int], name: Option[String]): Unit

  def dropNodeKeyConstraint(labelId: Int, propertyKeyIds: Seq[Int]): Unit

  /* return true if the constraint was created, false if preexisting, throws if failed */
  def createUniqueConstraint(labelId: Int, propertyKeyIds: Seq[Int], name: Option[String]): Unit

  def dropUniqueConstraint(labelId: Int, propertyKeyIds: Seq[Int]): Unit

  /* return true if the constraint was created, false if preexisting, throws if failed */
  def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int, name: Option[String]): Unit

  def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int)

  /* return true if the constraint was created, false if preexisting, throws if failed */
  def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int, name: Option[String]): Unit

  def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int)

  def dropNamedConstraint(name: String)

  def getOptStatistics: Option[QueryStatistics] = None

  def getImportURL(url: URL): Either[String,URL]

  def nodeGetDegree(node: Long, dir: SemanticDirection, nodeCursor: NodeCursor): Int = dir match {
    case SemanticDirection.OUTGOING => nodeGetOutgoingDegree(node, nodeCursor)
    case SemanticDirection.INCOMING => nodeGetIncomingDegree(node, nodeCursor)
    case SemanticDirection.BOTH => nodeGetTotalDegree(node, nodeCursor)
  }

  def nodeGetDegree(node: Long, dir: SemanticDirection, relTypeId: Int, nodeCursor: NodeCursor): Int = dir match {
    case SemanticDirection.OUTGOING => nodeGetOutgoingDegree(node, relTypeId, nodeCursor)
    case SemanticDirection.INCOMING => nodeGetIncomingDegree(node, relTypeId, nodeCursor)
    case SemanticDirection.BOTH => nodeGetTotalDegree(node, relTypeId, nodeCursor)
  }

  def nodeIsDense(node: Long, nodeCursor: NodeCursor): Boolean

  def asObject(value: AnyValue): AnyRef

  // Legacy dependency between kernel and compiler
  def variableLengthPathExpand(realNode: Long, minHops: Option[Int], maxHops: Option[Int], direction: SemanticDirection, relTypes: Seq[String]): Iterator[Path]

  def singleShortestPath(left: Long, right: Long, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path],
                         filters: Seq[KernelPredicate[Entity]]): Option[Path]

  def allShortestPath(left: Long, right: Long, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path],
                      filters: Seq[KernelPredicate[Entity]]): Iterator[Path]

  def lockNodes(nodeIds: Long*)

  def lockRelationships(relIds: Long*)

  def callReadOnlyProcedure(id: Int, args: Seq[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]]

  def callReadWriteProcedure(id: Int, args: Seq[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]]

  def callSchemaWriteProcedure(id: Int, args: Seq[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]]

  def callDbmsProcedure(id: Int, args: Seq[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]]

  def aggregateFunction(id: Int, allowed: Array[String]): UserDefinedAggregator

  def detachDeleteNode(id: Long): Int

  def assertSchemaWritesAllowed(): Unit

  override def nodeById(id: Long): NodeValue = nodeOps.getById(id)

  override def relationshipById(id: Long): RelationshipValue = relationshipOps.getById(id)

  override def propertyKey(name: String): Int = transactionalContext.tokenRead.propertyKey(name)

  override def nodeLabel(name: String): Int = transactionalContext.tokenRead.nodeLabel(name)

  override def relationshipType(name: String): Int = transactionalContext.tokenRead.relationshipType(name)

  override def nodeProperty(node: Long,
                            property: Int,
                            nodeCursor: NodeCursor,
                            propertyCursor: PropertyCursor,
                            throwOnDeleted: Boolean): Value =
    nodeOps.getProperty(node, property, nodeCursor, propertyCursor, throwOnDeleted)

  override def nodePropertyIds(node: Long,
                               nodeCursor: NodeCursor,
                               propertyCursor: PropertyCursor): Array[Int] =
    nodeOps.propertyKeyIds(node, nodeCursor, propertyCursor)

  override def nodeHasProperty(node: Long,
                               property: Int,
                               nodeCursor: NodeCursor,
                               propertyCursor: PropertyCursor): Boolean =
    nodeOps.hasProperty(node, property, nodeCursor, propertyCursor)

  override def relationshipProperty(relationship: Long,
                                    property: Int,
                                    relationshipScanCursor: RelationshipScanCursor,
                                    propertyCursor: PropertyCursor,
                                    throwOnDeleted: Boolean): Value =
    relationshipOps.getProperty(relationship, property, relationshipScanCursor, propertyCursor, throwOnDeleted)

  override def relationshipPropertyIds(relationship: Long,
                                       relationshipScanCursor: RelationshipScanCursor,
                                       propertyCursor: PropertyCursor): Array[Int] =
    relationshipOps.propertyKeyIds(relationship, relationshipScanCursor, propertyCursor)

  override def relationshipHasProperty(relationship: Long,
                                       property: Int,
                                       relationshipScanCursor: RelationshipScanCursor,
                                       propertyCursor: PropertyCursor): Boolean =
    relationshipOps.hasProperty(relationship, property, relationshipScanCursor, propertyCursor)

  override def hasTxStatePropertyForCachedNodeProperty(nodeId: Long, propertyKeyId: Int): Optional[java.lang.Boolean] = {
    nodeOps.hasTxStatePropertyForCachedProperty(nodeId, propertyKeyId) match {
      case None => Optional.empty()
      case Some(bool) => Optional.of(bool)
    }
  }

  override def hasTxStatePropertyForCachedRelationshipProperty(relId: Long, propertyKeyId: Int): Optional[java.lang.Boolean] = {
    relationshipOps.hasTxStatePropertyForCachedProperty(relId, propertyKeyId)match {
      case None => Optional.empty()
      case Some(bool) => Optional.of(bool)
    }
  }
}

trait Operations[T, CURSOR] {
  def delete(id: Long)

  def setProperty(obj: Long, propertyKeyId: Int, value: Value)

  def removeProperty(obj: Long, propertyKeyId: Int)

  /**
    * @param throwOnDeleted if this is `true` an Exception will be thrown when the entity with id `obj` has been deleted in this transaction.
    *                       If this is `false`, it will return `Values.NO_VALUE` in that case.
    */
  def getProperty(obj: Long, propertyKeyId: Int, cursor: CURSOR, propertyCursor: PropertyCursor, throwOnDeleted: Boolean): Value

  def hasProperty(obj: Long, propertyKeyId: Int, cursor: CURSOR, propertyCursor: PropertyCursor): Boolean

  /**
    * @return `null` if there are no changes.
    *         `NO_VALUE` if the property was deleted.
    *         `v` if the property was set to v
    * @throws EntityNotFoundException if the node was deleted
    */
  def getTxStateProperty(obj: Long, propertyKeyId: Int): Value

  /**
    * @return `None` if TxState has no changes.
    *         `Some(true)` if the property was changed.
    *         `Some(false)` if the property or the entity were deleted in TxState.
    */
  def hasTxStatePropertyForCachedProperty(entityId: Long, propertyKeyId: Int): Option[Boolean]

  def propertyKeyIds(obj: Long, cursor: CURSOR, propertyCursor: PropertyCursor): Array[Int]

  def getById(id: Long): T

  def isDeletedInThisTx(id: Long): Boolean

  def all: Iterator[T]

  def allPrimitive: LongIterator

  def acquireExclusiveLock(obj: Long): Unit

  def releaseExclusiveLock(obj: Long): Unit

  def getByIdIfExists(id: Long): Option[T]
}

trait NodeOperations extends Operations[NodeValue, NodeCursor]

trait RelationshipOperations extends Operations[RelationshipValue, RelationshipScanCursor]

trait QueryTransactionalContext extends CloseableResource {

  def transaction : KernelTransaction

  def cursors : CursorFactory

  def dataRead: Read

  def tokenRead: TokenRead

  def schemaRead: SchemaRead

  def dataWrite: Write

  def dbmsOperations: DbmsOperations

  def isTopLevelTx: Boolean

  def close()

  def rollback()

  def commitAndRestartTx()

  def kernelStatisticProvider: KernelStatisticProvider

  def databaseInfo: DatabaseInfo

  def databaseId: NamedDatabaseId
}

trait KernelPredicate[T] {
  def test(obj: T): Boolean
}

trait Expander {
  def addRelationshipFilter(newFilter: KernelPredicate[Entity]): Expander
  def addNodeFilter(newFilter: KernelPredicate[Entity]): Expander
  def nodeFilters: Seq[KernelPredicate[Entity]]
  def relFilters: Seq[KernelPredicate[Entity]]
}

trait UserDefinedAggregator {
  def update(args: IndexedSeq[AnyValue]): Unit
  def result: AnyValue
}

trait CloseableResource extends AutoCloseable {
  def close(): Unit
}

object NodeValueHit {
  val EMPTY = new NodeValueHit(-1L, null)
}

class NodeValueHit(val nodeId: Long, val values: Array[Value]) extends DefaultCloseListenable with NodeValueIndexCursor {

  private var _next = nodeId != -1L

  override def numberOfProperties(): Int = values.length

  override def propertyKey(offset: Int): Int = throw new UnsupportedOperationException("not implemented")

  override def hasValue: Boolean = true

  override def propertyValue(offset: Int): Value = values(offset)

  override def node(cursor: NodeCursor): Unit = throw new UnsupportedOperationException("not implemented")

  override def nodeReference(): Long = nodeId

  override def next(): Boolean = {
    val temp = _next
    _next = false
    temp
  }

  override def close(): Unit = {
    closeInternal()
    val listener = closeListener
    if (listener != null) listener.onClosed(this)
  }

  override def closeInternal(): Unit = _next = false

  override def isClosed: Boolean = _next

  override def score(): Float = Float.NaN

  override def setTracer(tracer: KernelReadTracer): Unit = throw new UnsupportedOperationException("not implemented")

  override def removeTracer(): Unit = throw new UnsupportedOperationException("not implemented")
}
