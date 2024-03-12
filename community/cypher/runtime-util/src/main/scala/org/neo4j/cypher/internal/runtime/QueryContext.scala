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
package org.neo4j.cypher.internal.runtime

import org.eclipse.collections.api.map.primitive.IntObjectMap
import org.eclipse.collections.api.set.primitive.IntSet
import org.neo4j.common.EntityType
import org.neo4j.configuration.Config
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.profiling.KernelStatisticProvider
import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.dbms.database.DatabaseManager
import org.neo4j.graphdb.Entity
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Path
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.DefaultCloseListenable
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.Locks
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.Procedures
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.PropertyIndexQuery
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.internal.kernel.api.SchemaRead
import org.neo4j.internal.kernel.api.SchemaWrite
import org.neo4j.internal.kernel.api.Token
import org.neo4j.internal.kernel.api.TokenRead
import org.neo4j.internal.kernel.api.TokenReadSession
import org.neo4j.internal.kernel.api.TokenWrite
import org.neo4j.internal.kernel.api.Write
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.io.pagecache.context.CursorContext
import org.neo4j.kernel.api.KernelTransaction.ExecutionContext
import org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.impl.factory.DbmsInfo
import org.neo4j.kernel.impl.query.FunctionInformation
import org.neo4j.logging.LogProvider
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.MemoryTracker
import org.neo4j.util.VisibleForTesting
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

import java.net.URL
import java.util.Optional
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
trait QueryContext extends ReadQueryContext with WriteQueryContext {
  def createParallelQueryContext(): QueryContext = throw new UnsupportedOperationException("Not supported with parallel runtime.")
}

trait ReadQueryContext extends ReadTokenContext with DbAccess with AutoCloseable {

  // See QueryContextAdaptation if you need a dummy that overrides all methods as ??? for writing a test

  def transactionalContext: QueryTransactionalContext

  def resources: ResourceManager

  def nodeReadOps: NodeReadOperations

  def relationshipReadOps: RelationshipReadOperations

  def getRelationshipsForIds(node: Long, dir: SemanticDirection, types: Array[Int]): ClosingLongIterator with RelationshipIterator

  def getRelationshipsByType(tokenReadSession: TokenReadSession, relType: Int, indexOrder: IndexOrder): ClosingLongIterator with RelationshipIterator

  def nodeCursor(): NodeCursor

  def traversalCursor(): RelationshipTraversalCursor

  def scanCursor(): RelationshipScanCursor

  def getAllIndexes(): Map[IndexDescriptor, IndexInfo]

  def indexReference(indexType: IndexType, entityId: Int, entityType: EntityType, properties: Int*): IndexDescriptor

  def lookupIndexReference(entityType: EntityType): IndexDescriptor

  def fulltextIndexReference(entityIds: List[Int], entityType: EntityType, properties: Int*): IndexDescriptor

  def indexExists(name: String): Boolean

  def constraintExists(name: String): Boolean

  def constraintExists(matchFn: ConstraintDescriptor => Boolean, entityId: Int, properties: Int*): Boolean

  def nodeIndexSeek(index: IndexReadSession,
                    needsValues: Boolean,
                    indexOrder: IndexOrder,
                    queries: Seq[PropertyIndexQuery]): NodeValueIndexCursor

  def nodeIndexSeekByContains(index: IndexReadSession,
                              needsValues: Boolean,
                              indexOrder: IndexOrder,
                              value: TextValue): NodeValueIndexCursor

  def nodeIndexSeekByEndsWith(index: IndexReadSession,
                              needsValues: Boolean,
                              indexOrder: IndexOrder,
                              value: TextValue): NodeValueIndexCursor

  def nodeIndexScan(index: IndexReadSession,
                    needsValues: Boolean,
                    indexOrder: IndexOrder): NodeValueIndexCursor

  def nodeLockingUniqueIndexSeek(index: IndexDescriptor, queries: Seq[PropertyIndexQuery.ExactPredicate]): NodeValueIndexCursor

  def relationshipIndexSeek(index: IndexReadSession,
                            needsValues: Boolean,
                            indexOrder: IndexOrder,
                            queries: Seq[PropertyIndexQuery]): RelationshipValueIndexCursor

  def relationshipIndexSeekByContains(index: IndexReadSession,
                                      needsValues: Boolean,
                                      indexOrder: IndexOrder,
                                      value: TextValue): RelationshipValueIndexCursor

  def relationshipIndexSeekByEndsWith(index: IndexReadSession,
                                      needsValues: Boolean,
                                      indexOrder: IndexOrder,
                                      value: TextValue): RelationshipValueIndexCursor

  def relationshipIndexScan(index: IndexReadSession,
                            needsValues: Boolean,
                            indexOrder: IndexOrder): RelationshipValueIndexCursor

  def getNodesByLabel(tokenReadSession: TokenReadSession, id: Int, indexOrder: IndexOrder): ClosingLongIterator

  def getAllConstraints(): Map[ConstraintDescriptor, ConstraintInfo]

  def getOptStatistics: Option[QueryStatistics] = None

  def addStatistics(statistics: QueryStatistics): Unit = {}

  def getImportURL(url: URL): Either[String, URL]

  def nodeGetDegreeWithMax(maxDegree: Int, node: Long, dir: SemanticDirection, nodeCursor: NodeCursor): Int = dir match {
    case SemanticDirection.OUTGOING => nodeGetOutgoingDegreeWithMax(maxDegree, node, nodeCursor)
    case SemanticDirection.INCOMING => nodeGetIncomingDegreeWithMax(maxDegree, node, nodeCursor)
    case SemanticDirection.BOTH => nodeGetTotalDegreeWithMax(maxDegree, node, nodeCursor)
  }

  def nodeGetDegreeWithMax(maxDegree: Int, node: Long, dir: SemanticDirection, relTypeId: Int, nodeCursor: NodeCursor): Int = dir match {
    case SemanticDirection.OUTGOING => nodeGetOutgoingDegreeWithMax(maxDegree, node, relTypeId, nodeCursor)
    case SemanticDirection.INCOMING => nodeGetIncomingDegreeWithMax(maxDegree, node, relTypeId, nodeCursor)
    case SemanticDirection.BOTH => nodeGetTotalDegreeWithMax(maxDegree, node, relTypeId, nodeCursor)
  }

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

  def nodeHasCheapDegrees(node: Long, nodeCursor: NodeCursor): Boolean

  def asObject(value: AnyValue): AnyRef

  def singleShortestPath(left: Long, right: Long, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path],
                         filters: Seq[KernelPredicate[Entity]], memoryTracker: MemoryTracker = EmptyMemoryTracker.INSTANCE): Option[Path]

  def allShortestPath(left: Long, right: Long, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path],
                      filters: Seq[KernelPredicate[Entity]], memoryTracker: MemoryTracker = EmptyMemoryTracker.INSTANCE): ClosingIterator[Path]

  def lockNodes(nodeIds: Long*): Unit

  def lockRelationships(relIds: Long*): Unit

  def callReadOnlyProcedure(id: Int, args: Array[AnyValue], context: ProcedureCallContext): Iterator[Array[AnyValue]]

  // Even though the procedure itself could perform writes the call is in the kernel Read API
  def callReadWriteProcedure(id: Int, args: Array[AnyValue], context: ProcedureCallContext): Iterator[Array[AnyValue]]

  // Even though the procedure itself could perform writes the call is in the kernel Read API
  def callSchemaWriteProcedure(id: Int, args: Array[AnyValue], context: ProcedureCallContext): Iterator[Array[AnyValue]]

  def callDbmsProcedure(id: Int, args: Array[AnyValue], context: ProcedureCallContext): Iterator[Array[AnyValue]]

  def aggregateFunction(id: Int): UserDefinedAggregator

  def builtInAggregateFunction(id: Int): UserDefinedAggregator

  def assertShowIndexAllowed(): Unit

  def assertShowConstraintAllowed(): Unit

  def systemGraph: GraphDatabaseService

  def logProvider: LogProvider

  def providedLanguageFunctions: Seq[FunctionInformation]

  def entityTransformer: EntityTransformer

  override def nodeById(id: Long): VirtualNodeValue = nodeReadOps.getById(id)

  override def relationshipById(id: Long): VirtualRelationshipValue = relationshipReadOps.getById(id)

  override def propertyKey(name: String): Int = transactionalContext.tokenRead.propertyKey(name)

  override def propertyKeyName(token : Int): String = transactionalContext.tokenRead.propertyKeyName(token)

  override def nodeLabel(name: String): Int = transactionalContext.tokenRead.nodeLabel(name)

  override def nodeLabelName(token: Int): String = transactionalContext.tokenRead.nodeLabelName(token)

  override def relationshipType(name: String): Int = transactionalContext.tokenRead.relationshipType(name)

  override def relationshipTypeName(token: Int): String = transactionalContext.tokenRead.relationshipTypeName(token)

  override def nodeProperty(node: Long,
                            property: Int,
                            nodeCursor: NodeCursor,
                            propertyCursor: PropertyCursor,
                            throwOnDeleted: Boolean): Value =
    nodeReadOps.getProperty(node, property, nodeCursor, propertyCursor, throwOnDeleted)

  override def nodePropertyIds(node: Long,
                               nodeCursor: NodeCursor,
                               propertyCursor: PropertyCursor): Array[Int] =
    nodeReadOps.propertyKeyIds(node, nodeCursor, propertyCursor)

  override def nodeHasProperty(node: Long,
                               property: Int,
                               nodeCursor: NodeCursor,
                               propertyCursor: PropertyCursor): Boolean =
    nodeReadOps.hasProperty(node, property, nodeCursor, propertyCursor)

  override def nodeDeletedInThisTransactionI(id: Long): Boolean = nodeReadOps.isDeletedInThisTx(id)


  override def relationshipProperty(relationship: Long,
                                    property: Int,
                                    relationshipScanCursor: RelationshipScanCursor,
                                    propertyCursor: PropertyCursor,
                                    throwOnDeleted: Boolean): Value =
    relationshipReadOps.getProperty(relationship, property, relationshipScanCursor, propertyCursor, throwOnDeleted)

  override def relationshipPropertyIds(relationship: Long,
                                       relationshipScanCursor: RelationshipScanCursor,
                                       propertyCursor: PropertyCursor): Array[Int] =
    relationshipReadOps.propertyKeyIds(relationship, relationshipScanCursor, propertyCursor)

  override def relationshipHasProperty(relationship: Long,
                                       property: Int,
                                       relationshipScanCursor: RelationshipScanCursor,
                                       propertyCursor: PropertyCursor): Boolean =
    relationshipReadOps.hasProperty(relationship, property, relationshipScanCursor, propertyCursor)


  override def relationshipDeletedInThisTransactionI(id: Long): Boolean = relationshipReadOps.isDeletedInThisTx(id)

  override def hasTxStatePropertyForCachedNodeProperty(nodeId: Long, propertyKeyId: Int): Optional[java.lang.Boolean] = {
    nodeReadOps.hasTxStatePropertyForCachedProperty(nodeId, propertyKeyId) match {
      case None => Optional.empty()
      case Some(bool) => Optional.of(bool)
    }
  }

  override def hasTxStatePropertyForCachedRelationshipProperty(relId: Long, propertyKeyId: Int): Optional[java.lang.Boolean] = {
    relationshipReadOps.hasTxStatePropertyForCachedProperty(relId, propertyKeyId) match {
      case None => Optional.empty()
      case Some(bool) => Optional.of(bool)
    }
  }

  /**
   * Opens a new transaction and create a new `QueryContext` bound to this new transaction.
   * The new transaction is called an inner transaction that is connected to the transaction of this context, which we will call the outer transaction.
   * The connection is as follows:
   *
   *   - An outer transaction cannot commit if it is connected to an open inner transaction.
   *   - A termination or rollback of an outer transaction propagates to any open inner transactions.
   *   - The outer transaction and all connected inner transactions are connected to the same `ExecutingQuery`.
   *
   * This context is still open and can continue to be used.
   *
   * @return the new context.
   * @see org.neo4j.kernel.impl.query.TransactionalContext#contextWithNewTransaction()
   */
  def contextWithNewTransaction(): QueryContext

  def close(): Unit

  def createExpressionCursors(): ExpressionCursors = {
    val transactionMemoryTracker = transactionalContext.memoryTracker
    val cursors = new ExpressionCursors(transactionalContext.cursors, transactionalContext.cursorContext, transactionMemoryTracker)
    resources.trace(cursors)
    cursors
  }
}

trait WriteQueryContext {
  def nodeWriteOps: NodeOperations

  def relationshipWriteOps: RelationshipOperations

  def createNodeId(labels: Array[Int]): Long

  def createRelationshipId(start: Long, end: Long, relType: Int): Long

  def getOrCreateRelTypeId(relTypeName: String): Int

  def getOrCreateLabelId(labelName: String): Int

  def getOrCreateTypeId(relTypeName: String): Int

  def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int

  def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int

  def getOrCreatePropertyKeyId(propertyKey: String): Int

  def getOrCreatePropertyKeyIds(propertyKeys: Array[String]): Array[Int]

  def addBtreeIndexRule(entityId: Int,
                        entityType: EntityType,
                        propertyKeyIds: Seq[Int],
                        name: Option[String],
                        provider: Option[String],
                        indexConfig: IndexConfig): IndexDescriptor

  def addRangeIndexRule(entityId: Int, entityType: EntityType, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[IndexProviderDescriptor]): IndexDescriptor

  def addLookupIndexRule(entityType: EntityType, name: Option[String], provider: Option[IndexProviderDescriptor]): IndexDescriptor

  def addFulltextIndexRule(entityIds: List[Int],
                           entityType: EntityType,
                           propertyKeyIds: Seq[Int],
                           name: Option[String],
                           provider: Option[IndexProviderDescriptor],
                           indexConfig: IndexConfig): IndexDescriptor

  def addTextIndexRule(entityId: Int, entityType: EntityType, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[IndexProviderDescriptor]): IndexDescriptor

  def addPointIndexRule(entityId: Int, entityType: EntityType, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[IndexProviderDescriptor], indexConfig: IndexConfig): IndexDescriptor

  def dropIndexRule(labelId: Int, propertyKeyIds: Seq[Int]): Unit

  def dropIndexRule(name: String): Unit

  /* throws if failed or pre-existing */
  def createNodeKeyConstraint(labelId: Int, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[String], indexConfig: IndexConfig): Unit

  def dropNodeKeyConstraint(labelId: Int, propertyKeyIds: Seq[Int]): Unit

  /* throws if failed or pre-existing */
  def createUniqueConstraint(labelId: Int, propertyKeyIds: Seq[Int], name: Option[String], provider: Option[String], indexConfig: IndexConfig): Unit

  def dropUniqueConstraint(labelId: Int, propertyKeyIds: Seq[Int]): Unit

  /* throws if failed or pre-existing */
  def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int, name: Option[String]): Unit

  def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): Unit

  /* throws if failed or pre-existing */
  def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int, name: Option[String]): Unit

  def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): Unit

  def dropNamedConstraint(name: String): Unit

  /**
   * Delete the node with the specified id and all of its relationships and return the number of deleted relationships.
   *
   * Note, caller needs to make sure the node is not already deleted or else the count will be incorrect.
   *
   * @return number of deleted relationships
   */
  def detachDeleteNode(id: Long): Int

  def assertSchemaWritesAllowed(): Unit

  def getDatabaseManager: DatabaseManager[DatabaseContext]

  def getConfig: Config

  def nodeApplyChanges(node: Long, addedLabels: IntSet, removedLabels: IntSet, properties: IntObjectMap[Value]): Unit

  def relationshipApplyChanges(relationship: Long, properties: IntObjectMap[Value]): Unit
}

trait ReadOperations[T, CURSOR] {

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
   * @throws org.neo4j.exceptions.EntityNotFoundException if the node was deleted
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

  def all: ClosingLongIterator

  def acquireExclusiveLock(obj: Long): Unit

  def releaseExclusiveLock(obj: Long): Unit

  def entityExists(id: Long): Boolean
}

trait WriteOperations[T, CURSOR] {
  /**
   * Delete entity
   *
   * @return true if something was deleted, false if no entity was found for this id
   */
  def delete(id: Long): Boolean

  def setProperty(obj: Long, propertyKeyId: Int, value: Value): Unit

  def setProperties(obj: Long, properties: IntObjectMap[Value]): Unit

  def removeProperty(obj: Long, propertyKeyId: Int): Boolean
}

trait Operations[T, CURSOR] extends ReadOperations[T, CURSOR] with WriteOperations[T, CURSOR]

trait NodeReadOperations extends ReadOperations[VirtualNodeValue, NodeCursor]
trait NodeWriteOperations extends WriteOperations[VirtualNodeValue, NodeCursor]
trait NodeOperations extends Operations[VirtualNodeValue, NodeCursor] with NodeReadOperations with NodeWriteOperations

trait RelationshipReadOperations extends ReadOperations[VirtualRelationshipValue, RelationshipScanCursor]
trait RelationshipWriteOperations extends WriteOperations[VirtualRelationshipValue, RelationshipScanCursor]
trait RelationshipOperations extends Operations[VirtualRelationshipValue, RelationshipScanCursor] with RelationshipReadOperations with RelationshipWriteOperations


trait QueryTransactionalContext extends CloseableResource {

  def createKernelExecutionContext(): ExecutionContext // TODO: Manage this internally in a parallel transactional context instead

  def commitTransaction(): Unit

  def kernelQueryContext: org.neo4j.internal.kernel.api.QueryContext

  def cursors : CursorFactory

  def cursorContext: CursorContext

  def memoryTracker: MemoryTracker

  def locks: Locks

  def dataRead: Read

  def dataWrite: Write

  def tokenRead: TokenRead

  def tokenWrite: TokenWrite

  def token: Token

  def schemaRead: SchemaRead

  def schemaWrite: SchemaWrite

  def procedures: Procedures

  def securityContext: SecurityContext

  def securityAuthorizationHandler: SecurityAuthorizationHandler

  def assertTransactionOpen(): Unit

  def isTopLevelTx: Boolean

  def close(): Unit

  def rollback(): Unit

  def commitAndRestartTx(): Unit

  def kernelStatisticProvider: KernelStatisticProvider

  def dbmsInfo: DbmsInfo

  def databaseId: NamedDatabaseId

  def freezeLocks(): Unit

  def thawLocks(): Unit

  @VisibleForTesting
  def validateSameDB[E <: Entity](entity: E): E
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
  val EMPTY = new NodeValueHit(NO_SUCH_NODE, null, null)
}

class NodeValueHit(val nodeId: Long, val values: Array[Value], read: Read) extends DefaultCloseListenable with NodeValueIndexCursor {

  private var _next = nodeId != -1L

  override def numberOfProperties(): Int = values.length

  override def hasValue: Boolean = true

  override def propertyValue(offset: Int): Value = values(offset)

  override def node(cursor: NodeCursor): Unit = {
    read.singleNode(nodeId, cursor)
  }

  override def nodeReference(): Long = nodeId

  override def next(): Boolean = {
    val temp = _next
    _next = false
    temp
  }

  override def closeInternal(): Unit = _next = false

  override def isClosed: Boolean = _next

  override def score(): Float = Float.NaN

  //this cursor doesn't need tracing since all values has already been read.
  override def setTracer(tracer: KernelReadTracer): Unit = {}
  override def removeTracer(): Unit = {}
}

trait EntityTransformer {
  def rebindEntityWrappingValue(value: AnyValue): AnyValue
}

class NoopEntityTransformer extends EntityTransformer {
  override def rebindEntityWrappingValue(value: AnyValue): AnyValue = value
}
