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
import org.neo4j.collection.PrimitiveLongResourceIterator
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.logical.plans.{IndexOrder, IndexOrderAscending, IndexOrderDescending, IndexOrderNone}
import org.neo4j.cypher.internal.runtime.KernelAPISupport.RANGE_SEEKABLE_VALUE_GROUPS
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.DirectionConverter.toGraphDb
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{OnlyDirectionExpander, TypeAndDirectionExpander}
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.operations.CursorUtils
import org.neo4j.exceptions.{EntityNotFoundException, FailedIndexException}
import org.neo4j.graphalgo.BasicEvaluationContext
import org.neo4j.graphalgo.impl.path.ShortestPath
import org.neo4j.graphalgo.impl.path.ShortestPath.ShortestPathPredicate
import org.neo4j.graphdb._
import org.neo4j.graphdb.security.URLAccessValidationError
import org.neo4j.graphdb.traversal.{Evaluators, TraversalDescription, Uniqueness}
import org.neo4j.internal
import org.neo4j.internal.helpers.collection.Iterators
import org.neo4j.internal.kernel.api
import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.{allCursor, incomingCursor, outgoingCursor}
import org.neo4j.internal.kernel.api.helpers._
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
import org.neo4j.internal.kernel.api.{QueryContext => _, _}
import org.neo4j.internal.schema.{ConstraintType, IndexDescriptor, IndexPrototype, SchemaDescriptor, IndexOrder => KernelIndexOrder}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.exceptions.schema._
import org.neo4j.kernel.api.{ResourceTracker => _, _}
import org.neo4j.kernel.impl.core.TransactionalEntityFactory
import org.neo4j.kernel.impl.util.ValueUtils.{fromNodeEntity, fromRelationshipEntity}
import org.neo4j.kernel.impl.util.{DefaultValueMapper, ValueUtils}
import org.neo4j.storageengine.api.RelationshipVisitor
import org.neo4j.values.storable.{FloatingPointValue, TextValue, Value, Values}
import org.neo4j.values.virtual._
import org.neo4j.values.{AnyValue, ValueMapper}

import scala.collection.Iterator
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

sealed class TransactionBoundQueryContext(val transactionalContext: TransactionalContextWrapper,
                                          val resources: ResourceManager = new ResourceManager,
                                          trackResourcesInTransaction: Boolean = true)
                                        (implicit indexSearchMonitor: IndexSearchMonitor)
  extends TransactionBoundTokenContext(transactionalContext.kernelTransaction) with QueryContext {
  override val nodeOps: NodeOperations = new NodeOperations
  override val relationshipOps: RelationshipOperations = new RelationshipOperations
  override lazy val entityAccessor: TransactionalEntityFactory = transactionalContext.tc.transaction()
  private lazy val valueMapper: ValueMapper[java.lang.Object] = new DefaultValueMapper(transactionalContext.tc.transaction())

  // We don't need to unregister this anywhere since the TransactionBoundQueryContext will be closed together with the Statement
  if (trackResourcesInTransaction)
    transactionalContext.tc.statement().registerCloseableResource(resources)

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) => if (writes().nodeAddLabel(node, labelId)) count + 1 else count
  }

  //We cannot assign to value because of periodic commit
  protected def reads(): Read = transactionalContext.dataRead

  private def writes() = transactionalContext.dataWrite

  private def allocateNodeCursor() = transactionalContext.cursors.allocateNodeCursor()
  private def allocateRelationshipScanCursor() = transactionalContext.cursors.allocateRelationshipScanCursor()

  private def tokenRead = transactionalContext.kernelTransaction.tokenRead()

  private def tokenWrite = transactionalContext.kernelTransaction.tokenWrite()

  override def createNode(labels: Array[Int]): NodeValue = ValueUtils.fromNodeEntity(entityAccessor.newNodeEntity(writes().nodeCreateWithLabels(labels)))

  override def createNodeId(labels: Array[Int]): Long = writes().nodeCreateWithLabels(labels)

  override def createRelationship(start: Long, end: Long, relType: Int): RelationshipValue = {
    val relId = transactionalContext.kernelTransaction.dataWrite().relationshipCreate(start, relType, end)
    fromRelationshipEntity(entityAccessor.newRelationshipEntity(relId, start, relType, end))
  }

  override def singleRelationship(id: Long, cursor: RelationshipScanCursor): Unit = {
    reads().singleRelationship(id, cursor)
  }

  override def getOrCreateRelTypeId(relTypeName: String): Int =
    transactionalContext.kernelTransaction.tokenWrite().relationshipTypeGetOrCreateForName(relTypeName)

  override def getLabelsForNode(node: Long, nodeCursor: NodeCursor): ListValue = {
    reads().singleNode(node, nodeCursor)
    if (!nodeCursor.next()) {
      if (nodeOps.isDeletedInThisTx(node))
        throw new EntityNotFoundException(s"Node with id $node has been deleted in this transaction")
      else
        VirtualValues.EMPTY_LIST
    }
    val labelSet = nodeCursor.labels()
    val labelArray = new Array[TextValue](labelSet.numberOfLabels())
    var i = 0
    while (i < labelSet.numberOfLabels()) {
      labelArray(i) = Values.stringValue(tokenRead.nodeLabelName(labelSet.label(i)))
      i += 1
    }
    VirtualValues.list(labelArray: _*)
  }

  override def getTypeForRelationship(id: Long, cursor: RelationshipScanCursor): TextValue = {
    reads().singleRelationship(id, cursor)
    if (!cursor.next() && !relationshipOps.isDeletedInThisTx(id)) {
      // we are allowed to read the type of relationships we have deleted, but
      // if we have a concurrent delete by another tx we resort to NO_VALUE
      Values.NO_VALUE
    }
    Values.stringValue(tokenRead.relationshipTypeName(cursor.`type`()))
  }

  override def isLabelSetOnNode(label: Int, node: Long, nodeCursor: NodeCursor): Boolean = {
    CursorUtils.nodeHasLabel(reads(), nodeCursor, node, label)
  }

  override def getOrCreateLabelId(labelName: String): Int = {
    val id = tokenRead.nodeLabel(labelName)
    if (id != TokenRead.NO_TOKEN) id
    else tokenWrite.labelGetOrCreateForName(labelName)
  }

  def getRelationshipsForIds(node: Long, dir: SemanticDirection, types: Array[Int]): Iterator[RelationshipValue] = {

    val cursor = allocateNodeCursor()
    val cursors = transactionalContext.cursors

    try {
      val read = reads()
      read.singleNode(node, cursor)
      if (!cursor.next()) Iterator.empty
      else {
        val selectionCursor = dir match {
          case OUTGOING => outgoingCursor(cursors, cursor, types)
          case INCOMING => incomingCursor(cursors, cursor, types)
          case BOTH => allCursor(cursors, cursor, types)
        }
        resources.trace(selectionCursor)
        new CursorIterator[RelationshipValue] {
          override protected def close(): Unit = {}

          override protected def fetchNext(): RelationshipValue =
            if (selectionCursor.next())
              fromRelationshipEntity(entityAccessor.newRelationshipEntity(selectionCursor.relationshipReference(),
                                                                        selectionCursor.sourceNodeReference(),
                                                                        selectionCursor.`type`(),
                                                                        selectionCursor.targetNodeReference()))
            else null
        }
      }
    } finally {
      cursor.close()
    }
  }

  override def getRelationshipsForIdsPrimitive(node: Long, dir: SemanticDirection, types: Array[Int]): RelationshipIterator = {

    val cursor = allocateNodeCursor()
    try {
      val read = reads()
      val cursors = transactionalContext.cursors
      read.singleNode(node, cursor)
      if (!cursor.next()) RelationshipIterator.EMPTY
      else {
        val selectionCursor = dir match {
          case OUTGOING => outgoingCursor(cursors, cursor, types)
          case INCOMING => incomingCursor(cursors, cursor, types)
          case BOTH => allCursor(cursors, cursor, types)
        }
        new RelationshipCursorIterator(selectionCursor)
      }
    } finally {
      cursor.close()
    }
  }

  override def relationshipById(relationshipId: Long,
                                startNodeId: Long,
                                endNodeId: Long,
                                typeId: Int): RelationshipValue =
    try {
      fromRelationshipEntity(entityAccessor.newRelationshipEntity(relationshipId, startNodeId, typeId, endNodeId))
    } catch {
      case e: NotFoundException => throw new EntityNotFoundException(s"Relationship with id $relationshipId", e)
    }

  override def indexSeek[RESULT <: AnyRef](index: IndexReadSession,
                                           needsValues: Boolean,
                                           indexOrder: IndexOrder,
                                           predicates: Seq[IndexQuery]): NodeValueIndexCursor = {

    val impossiblePredicate =
      predicates.exists {
        case p: IndexQuery.ExactPredicate => (p.value() eq Values.NO_VALUE) || (p.value().isInstanceOf[FloatingPointValue] && p.value().asInstanceOf[FloatingPointValue].isNaN)
        case _: IndexQuery.ExistsPredicate => predicates.length <= 1
        case p: IndexQuery.RangePredicate[_] =>
          !RANGE_SEEKABLE_VALUE_GROUPS.contains(p.valueGroup())
        case _ => false
      }

    if (impossiblePredicate) NodeValueIndexCursor.EMPTY
    else seek(index, needsValues, indexOrder, predicates: _*)
  }

  override def indexReference(label: Int,
                              properties: Int*): IndexDescriptor =
    Iterators.single(transactionalContext.kernelTransaction.schemaRead().index(SchemaDescriptor.forLabel(label, properties: _*)))

  private def seek[RESULT <: AnyRef](index: IndexReadSession,
                                     needsValues: Boolean,
                                     indexOrder: IndexOrder,
                                     queries: IndexQuery*): NodeValueIndexCursor = {

    val nodeCursor: NodeValueIndexCursor = allocateAndTraceNodeValueIndexCursor()
    val actualValues =
      if (needsValues && queries.forall(_.isInstanceOf[ExactPredicate]))
        // We don't need property values from the index for an exact seek
        queries.map(_.asInstanceOf[ExactPredicate].value()).toArray
      else
        null

    val needsValuesFromIndexSeek = actualValues == null && needsValues
    reads().nodeIndexSeek(index, nodeCursor, asKernelIndexOrder(indexOrder), needsValuesFromIndexSeek, queries: _*)
    if (needsValues && actualValues != null)
      new ValuedNodeIndexCursor(nodeCursor, actualValues)
    else
      nodeCursor
  }

  override def indexScan[RESULT <: AnyRef](index: IndexReadSession,
                                           needsValues: Boolean,
                                           indexOrder: IndexOrder): NodeValueIndexCursor = {
    val nodeCursor = allocateAndTraceNodeValueIndexCursor()
    reads().nodeIndexScan(index, nodeCursor, asKernelIndexOrder(indexOrder), needsValues)
    nodeCursor
  }

  override def indexSeekByContains[RESULT <: AnyRef](index: IndexReadSession,
                                                     needsValues: Boolean,
                                                     indexOrder: IndexOrder,
                                                     value: TextValue): NodeValueIndexCursor =
    seek(index, needsValues, indexOrder, IndexQuery.stringContains(index.reference().schema().getPropertyIds()(0), value))

  override def indexSeekByEndsWith[RESULT <: AnyRef](index: IndexReadSession,
                                                     needsValues: Boolean,
                                                     indexOrder: IndexOrder,
                                                     value: TextValue): NodeValueIndexCursor =
    seek(index, needsValues, indexOrder, IndexQuery.stringSuffix(index.reference().schema().getPropertyIds()(0), value))

  override def lockingUniqueIndexSeek[RESULT](index: IndexDescriptor,
                                              queries: Seq[IndexQuery.ExactPredicate]): NodeValueIndexCursor = {

    val cursor = transactionalContext.cursors.allocateNodeValueIndexCursor()
    try {
      indexSearchMonitor.lockingUniqueIndexSeek(index, queries)
      if (queries.exists(q => q.value() eq Values.NO_VALUE))
        NodeValueHit.EMPTY
      else {
        val resultNodeId = reads().lockingNodeUniqueIndexSeek(index, cursor, queries: _*)
        if (StatementConstants.NO_SUCH_NODE == resultNodeId) {
          NodeValueHit.EMPTY
        } else {
          val values = queries.map(_.value()).toArray
          new NodeValueHit(resultNodeId, values)
        }
      }
    } finally {
      cursor.close()
    }
  }

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) =>
      if (transactionalContext.kernelTransaction.dataWrite().nodeRemoveLabel(node, labelId)) count + 1 else count
  }

  override def getNodesByLabel(id: Int): Iterator[NodeValue] = {
    val cursor = allocateAndTraceNodeLabelIndexCursor()
    reads().nodeLabelScan(id, cursor)
    new CursorIterator[NodeValue] {
      override protected def fetchNext(): NodeValue = {
        if (cursor.next()) fromNodeEntity(entityAccessor.newNodeEntity(cursor.nodeReference()))
        else null
      }

      override protected def close(): Unit = {}
    }
  }

  override def nodeAsMap(id: Long, nodeCursor: NodeCursor, propertyCursor: PropertyCursor): MapValue = {
      reads().singleNode(id, nodeCursor)
      if (!nodeCursor.next()) VirtualValues.EMPTY_MAP
      else {
        val tokens = tokenRead
        nodeCursor.properties(propertyCursor)
        val builder = new MapValueBuilder()
        while (propertyCursor.next()) {
          builder.add(tokens.propertyKeyName(propertyCursor.propertyKey()), propertyCursor.propertyValue())
        }
        builder.build()
    }
  }

  override def relationshipAsMap(id: Long, relationshipCursor: RelationshipScanCursor, propertyCursor: PropertyCursor): MapValue = {
    reads().singleRelationship(id, relationshipCursor)
    if (!relationshipCursor.next()) VirtualValues.EMPTY_MAP
    else {
      val tokens = tokenRead
      relationshipCursor.properties(propertyCursor)
      val builder = new MapValueBuilder()
      while (propertyCursor.next()) {
        builder.add(tokens.propertyKeyName(propertyCursor.propertyKey()), propertyCursor.propertyValue())
      }
      builder.build()
    }
  }

  override def getNodesByLabelPrimitive(id: Int): LongIterator = {
    val cursor = allocateAndTraceNodeLabelIndexCursor()
    reads().nodeLabelScan(id, cursor)
    new PrimitiveCursorIterator {
      override protected def fetchNext(): Long = if (cursor.next()) cursor.nodeReference() else -1L

      override protected def close(): Unit = {}
    }
  }

  override def nodeGetOutgoingDegree(node: Long, nodeCursor: NodeCursor): Int = {
      reads().singleNode(node, nodeCursor)
      if (!nodeCursor.next()) 0
      else Nodes.countOutgoing(nodeCursor, transactionalContext.cursors)
  }

  override def nodeGetIncomingDegree(node: Long, nodeCursor: NodeCursor): Int = {
      reads().singleNode(node, nodeCursor)
      if (!nodeCursor.next()) 0
      else Nodes.countIncoming(nodeCursor, transactionalContext.cursors)
  }

  override def nodeGetTotalDegree(node: Long, nodeCursor: NodeCursor): Int = {
      reads().singleNode(node, nodeCursor)
      if (!nodeCursor.next()) 0
      else Nodes.countAll(nodeCursor, transactionalContext.cursors)
  }

  override def nodeGetOutgoingDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int = {
    reads().singleNode(node, nodeCursor)
    if (!nodeCursor.next()) 0
    else Nodes.countOutgoing(nodeCursor, transactionalContext.cursors, relationship)
  }

  override def nodeGetIncomingDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int = {
    reads().singleNode(node, nodeCursor)
    if (!nodeCursor.next()) 0
    else Nodes.countIncoming(nodeCursor, transactionalContext.cursors, relationship)
  }

  override def nodeGetTotalDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int = {
    reads().singleNode(node, nodeCursor)
    if (!nodeCursor.next()) 0
    else Nodes.countAll(nodeCursor, transactionalContext.cursors, relationship)
  }

  override def nodeIsDense(node: Long, nodeCursor: NodeCursor): Boolean = {
    reads().singleNode(node, nodeCursor)
    if (!nodeCursor.next()) false
    else nodeCursor.isDense
  }

  override def asObject(value: AnyValue): AnyRef = value.map(valueMapper)

  override def getTxStateNodePropertyOrNull(nodeId: Long,
                                            propertyKey: Int): Value = {
    val ops = reads()
    if (ops.nodeDeletedInTransaction(nodeId)) {
      throw new EntityNotFoundException(
        s"Node with id $nodeId has been deleted in this transaction")
    }

   ops.nodePropertyChangeInTransactionOrNull(nodeId, propertyKey)
  }

  override def getTxStateRelationshipPropertyOrNull(relId: Long,
                                                    propertyKey: Int): Value = {
    val ops = reads()
    if (ops.relationshipDeletedInTransaction(relId)) {
      throw new EntityNotFoundException(
        s"Relationship with id $relId has been deleted in this transaction")
    }

    ops.relationshipPropertyChangeInTransactionOrNull(relId, propertyKey)
  }

  class NodeOperations extends BaseOperations[NodeValue, NodeCursor] with org.neo4j.cypher.internal.runtime.NodeOperations {

    override def delete(id: Long) {
      writes().nodeDelete(id)
    }

    override def propertyKeyIds(id: Long, nodeCursor: NodeCursor, propertyCursor: PropertyCursor): Array[Int] = {
      reads().singleNode(id, nodeCursor)
      if (!nodeCursor.next()) Array.empty
      else {
        val buffer = ArrayBuffer[Int]()
        nodeCursor.properties(propertyCursor)
        while (propertyCursor.next()) {
          buffer.append(propertyCursor.propertyKey())
        }
        buffer.toArray
      }
    }

    override def getProperty(id: Long, propertyKeyId: Int, nodeCursor: NodeCursor, propertyCursor: PropertyCursor, throwOnDeleted: Boolean): Value = {
     CursorUtils.nodeGetProperty(reads(), nodeCursor, id, propertyCursor, propertyKeyId, throwOnDeleted)
    }

    override def getTxStateProperty(nodeId: Long, propertyKeyId: Int): Value =
      getTxStateNodePropertyOrNull(nodeId, propertyKeyId)

    override def hasProperty(id: Long, propertyKey: Int, nodeCursor: NodeCursor, propertyCursor: PropertyCursor): Boolean = {
      reads().singleNode(id, nodeCursor)
      if (!nodeCursor.next()) false
      else {
        nodeCursor.properties(propertyCursor)
        while (propertyCursor.next()) {
          if (propertyCursor.propertyKey() == propertyKey) return true
        }
        false
      }
    }

    override def hasTxStatePropertyForCachedProperty(nodeId: Long, propertyKeyId: Int): Option[Boolean] = {
      if (isDeletedInThisTx(nodeId)) {
        // Node deleted in TxState
        Some(false)
      } else {
        val nodePropertyInTx = reads().nodePropertyChangeInTransactionOrNull(nodeId, propertyKeyId)
        nodePropertyInTx match {
          case null => None // no changes in TxState.
          case IsNoValue() => Some(false) // property removed in TxState
          case _ => Some(true) // property changed in TxState
        }
      }
    }

    override def removeProperty(id: Long, propertyKeyId: Int): Unit = {
      try {
        writes().nodeRemoveProperty(id, propertyKeyId)
      } catch {
        case _: api.exceptions.EntityNotFoundException => //ignore
      }
    }

    override def setProperty(id: Long, propertyKeyId: Int, value: Value): Unit = {
      try {
        writes().nodeSetProperty(id, propertyKeyId, value)
      } catch {
        case _: api.exceptions.EntityNotFoundException => //ignore
      }
    }

    override def getById(id: Long): NodeValue = try {
      fromNodeEntity(entityAccessor.newNodeEntity(id))
    } catch {
      case e: NotFoundException => throw new EntityNotFoundException(s"Node with id $id", e)
    }

    override def all: Iterator[NodeValue] = {
      val nodeCursor = allocateAndTraceNodeCursor()
      reads().allNodesScan(nodeCursor)
      new CursorIterator[NodeValue] {
        override protected def fetchNext(): NodeValue = {
          if (nodeCursor.next()) fromNodeEntity(entityAccessor.newNodeEntity(nodeCursor.nodeReference()))
          else null
        }

        override protected def close(): Unit = nodeCursor.close()
      }
    }

    override def allPrimitive: LongIterator = {
      val nodeCursor = allocateAndTraceNodeCursor()
      reads().allNodesScan(nodeCursor)
      new PrimitiveCursorIterator {
        override protected def fetchNext(): Long = if (nodeCursor.next()) nodeCursor.nodeReference() else -1L

        override protected def close(): Unit = nodeCursor.close()
      }
    }

    override def isDeletedInThisTx(id: Long): Boolean = reads().nodeDeletedInTransaction(id)

    override def acquireExclusiveLock(obj: Long): Unit =
      transactionalContext.kernelTransaction.locks().acquireExclusiveNodeLock(obj)

    override def releaseExclusiveLock(obj: Long): Unit =
      transactionalContext.kernelTransaction.locks().releaseExclusiveNodeLock(obj)

    override def getByIdIfExists(id: Long): Option[NodeValue] =
      if (id >= 0 && reads().nodeExists(id))
        Some(fromNodeEntity(entityAccessor.newNodeEntity(id)))
      else
        None
  }

  class RelationshipOperations extends BaseOperations[RelationshipValue, RelationshipScanCursor] with org.neo4j.cypher.internal.runtime.RelationshipOperations {

    override def delete(id: Long) {
      writes().relationshipDelete(id)
    }

    override def propertyKeyIds(id: Long, relationshipScanCursor: RelationshipScanCursor, propertyCursor: PropertyCursor): Array[Int] = {
      reads().singleRelationship(id, relationshipScanCursor)
      if (!relationshipScanCursor.next()) Array.empty
      else {
        val buffer = ArrayBuffer[Int]()
        relationshipScanCursor.properties(propertyCursor)
        while (propertyCursor.next()) {
          buffer.append(propertyCursor.propertyKey())
        }
        buffer.toArray
      }
    }

    override def getProperty(id: Long, propertyKeyId: Int, relationshipCursor: RelationshipScanCursor, propertyCursor: PropertyCursor, throwOnDeleted: Boolean): Value = {
     CursorUtils.relationshipGetProperty(reads(), relationshipCursor, id, propertyCursor, propertyKeyId, throwOnDeleted)
    }

    override def hasProperty(id: Long, propertyKey: Int, relationshipCursor: RelationshipScanCursor, propertyCursor: PropertyCursor): Boolean = {
      reads().singleRelationship(id, relationshipCursor)
      if (!relationshipCursor.next()) false
      else {
        relationshipCursor.properties(propertyCursor)
        while (propertyCursor.next()) {
          if (propertyCursor.propertyKey() == propertyKey) return true
        }
        false
      }
    }

    override def removeProperty(id: Long, propertyKeyId: Int): Unit = {
      try {
        writes().relationshipRemoveProperty(id, propertyKeyId)
      } catch {
        case _: api.exceptions.EntityNotFoundException => //ignore
      }
    }

    override def setProperty(id: Long, propertyKeyId: Int, value: Value): Unit = {
      try {
        writes().relationshipSetProperty(id, propertyKeyId, value)
      } catch {
        case _: api.exceptions.EntityNotFoundException => //ignore
      }
    }

    override def getById(id: Long): RelationshipValue = try {
      fromRelationshipEntity(entityAccessor.newRelationshipEntity(id))
    } catch {
      case e: NotFoundException => throw new EntityNotFoundException(s"Relationship with id $id", e)
    }

    override def getByIdIfExists(id: Long): Option[RelationshipValue] = {
      if (id < 0)
        None
      else {
        val cursor = allocateRelationshipScanCursor()
        try {
          reads().singleRelationship(id, cursor)
          if (cursor.next()) {
            val src = cursor.sourceNodeReference()
            val dst = cursor.targetNodeReference()
            val relProxy = entityAccessor.newRelationshipEntity(id, src, cursor.`type`(), dst)
            Some(fromRelationshipEntity(relProxy))
          }
          else
            None
        } finally {
          cursor.close()
        }
      }
    }

    override def all: Iterator[RelationshipValue] = {
      val relCursor = allocateAndTraceRelationshipScanCursor()
      reads().allRelationshipsScan(relCursor)
      new CursorIterator[RelationshipValue] {
        override protected def fetchNext(): RelationshipValue = {
          if (relCursor.next())
            fromRelationshipEntity(entityAccessor.newRelationshipEntity(relCursor.relationshipReference(),
                                                                      relCursor.sourceNodeReference(), relCursor.`type`(),
                                                                      relCursor.targetNodeReference()))
          else null
        }

        override protected def close(): Unit = relCursor.close()
      }
    }

    override def allPrimitive: LongIterator = {
      val relCursor = allocateAndTraceRelationshipScanCursor()
      reads().allRelationshipsScan(relCursor)
      new PrimitiveCursorIterator {
        override protected def fetchNext(): Long = if (relCursor.next()) relCursor.relationshipReference() else -1L

        override protected def close(): Unit = relCursor.close()
      }
    }

    override def isDeletedInThisTx(id: Long): Boolean =
      reads().relationshipDeletedInTransaction(id)

    override def acquireExclusiveLock(obj: Long): Unit =
      transactionalContext.kernelTransaction.locks().acquireExclusiveRelationshipLock(obj)

    override def releaseExclusiveLock(obj: Long): Unit =
      transactionalContext.kernelTransaction.locks().releaseExclusiveRelationshipLock(obj)

    override def getTxStateProperty(relId: Long, propertyKeyId: Int): Value =
      getTxStateRelationshipPropertyOrNull(relId, propertyKeyId)

    override def hasTxStatePropertyForCachedProperty(relId: Long, propertyKeyId: Int): Option[Boolean] = {
      if (isDeletedInThisTx(relId)) {
        // Relationship deleted in TxState
        Some(false)
      } else {
        val relPropertyInTx = reads().relationshipPropertyChangeInTransactionOrNull(relId, propertyKeyId)
        relPropertyInTx match {
          case null => None // no changes in TxState.
          case IsNoValue() => Some(false) // property removed in TxState
          case _ => Some(true) // property changed in TxState
        }
      }
    }
  }

  override def getOrCreatePropertyKeyId(propertyKey: String): Int =
    tokenWrite.propertyKeyGetOrCreateForName(propertyKey)

  override def getOrCreatePropertyKeyIds(propertyKeys: Array[String]): Array[Int] = {
    val ids = new Array[Int](propertyKeys.length)
    tokenWrite.propertyKeyGetOrCreateForNames(propertyKeys, ids)
    ids
  }

  abstract class BaseOperations[T, CURSOR] extends Operations[T, CURSOR] {

    def primitiveLongIteratorToScalaIterator(primitiveIterator: LongIterator): Iterator[Long] =
      new Iterator[Long] {
        override def hasNext: Boolean = primitiveIterator.hasNext

        override def next(): Long = primitiveIterator.next
      }
  }

  override def addIndexRule(labelId: Int, propertyKeyIds: Seq[Int], name: Option[String]): IndexDescriptor = {
    val ktx = transactionalContext.kernelTransaction
    try {
      ktx.schemaWrite().indexCreate(SchemaDescriptor.forLabel(labelId, propertyKeyIds:_*), name.orNull)
    } catch {
      case e: EquivalentSchemaRuleAlreadyExistsException =>
        val indexReference = ktx.schemaRead().index(SchemaDescriptor.forLabel(labelId, propertyKeyIds:_*)).next()
        if (ktx.schemaRead().indexGetState(indexReference) == InternalIndexState.FAILED) {
          val message = ktx.schemaRead().indexGetFailure(indexReference)
          throw new FailedIndexException(indexReference.userDescription(tokenNameLookup), message)
        }
        throw e
    }
  }

  override def dropIndexRule(labelId: Int, propertyKeyIds: Seq[Int]): Unit =
    transactionalContext.kernelTransaction.schemaWrite().indexDrop(SchemaDescriptor.forLabel(labelId, propertyKeyIds:_*))

  override def dropIndexRule(name: String): Unit =
    transactionalContext.kernelTransaction.schemaWrite().indexDrop(name)

  override def createNodeKeyConstraint(labelId: Int, propertyKeyIds: Seq[Int], name: Option[String]): Unit =
    transactionalContext.kernelTransaction.schemaWrite().nodeKeyConstraintCreate(
      IndexPrototype.uniqueForSchema(SchemaDescriptor.forLabel(labelId, propertyKeyIds:_*)).withName(name.orNull))

  override def dropNodeKeyConstraint(labelId: Int, propertyKeyIds: Seq[Int]): Unit =
    transactionalContext.kernelTransaction.schemaWrite()
      .constraintDrop(SchemaDescriptor.forLabel(labelId, propertyKeyIds:_*), ConstraintType.UNIQUE_EXISTS)

  override def createUniqueConstraint(labelId: Int, propertyKeyIds: Seq[Int], name: Option[String]): Unit =
    transactionalContext.kernelTransaction.schemaWrite().uniquePropertyConstraintCreate(
      IndexPrototype.uniqueForSchema(SchemaDescriptor.forLabel(labelId, propertyKeyIds:_*)).withName(name.orNull))

  override def dropUniqueConstraint(labelId: Int, propertyKeyIds: Seq[Int]): Unit =
    transactionalContext.kernelTransaction.schemaWrite()
      .constraintDrop(SchemaDescriptor.forLabel(labelId, propertyKeyIds:_*), ConstraintType.UNIQUE)

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int, name: Option[String]): Unit =
      transactionalContext.kernelTransaction.schemaWrite().nodePropertyExistenceConstraintCreate(
        SchemaDescriptor.forLabel(labelId, propertyKeyId), name.orNull)

  override def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): Unit =
    transactionalContext.kernelTransaction.schemaWrite()
      .constraintDrop(SchemaDescriptor.forLabel(labelId, propertyKeyId), ConstraintType.EXISTS)

  override def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int, name: Option[String]): Unit =
      transactionalContext.kernelTransaction.schemaWrite().relationshipPropertyExistenceConstraintCreate(
        SchemaDescriptor.forRelType(relTypeId, propertyKeyId), name.orNull)

  override def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): Unit =
    transactionalContext.kernelTransaction.schemaWrite()
      .constraintDrop(SchemaDescriptor.forRelType(relTypeId, propertyKeyId), ConstraintType.EXISTS)

  override def dropNamedConstraint(name: String): Unit =
   transactionalContext.kernelTransaction.schemaWrite().constraintDrop(name)

  override def getImportURL(url: URL): Either[String, URL] = transactionalContext.graph match {
    case db: GraphDatabaseQueryService =>
      try {
        Right(db.validateURLAccess(url))
      } catch {
        case error: URLAccessValidationError => Left(error.getMessage)
      }
  }

  private lazy val tokenNameLookup = new SilentTokenNameLookup(transactionalContext.kernelTransaction.tokenRead())

  // Legacy dependency between kernel and compiler
  override def variableLengthPathExpand(realNode: Long,
                                        minHops: Option[Int],
                                        maxHops: Option[Int],
                                        direction: SemanticDirection,
                                        relTypes: Seq[String]): Iterator[Path] = {
    val depthEval = (minHops, maxHops) match {
      case (None, None) => Evaluators.fromDepth(1)
      case (Some(min), None) => Evaluators.fromDepth(min)
      case (None, Some(max)) => Evaluators.includingDepths(1, max)
      case (Some(min), Some(max)) => Evaluators.includingDepths(min, max)
    }

    // The RULE compiler makes use of older kernel API capabilities for variable length expanding
    // TODO: Consider re-writing this using similar code to the COST var-length expand
    val baseTraversalDescription: TraversalDescription = transactionalContext.tc.transaction()
      .traversalDescription()
      .evaluator(depthEval)
      .uniqueness(Uniqueness.RELATIONSHIP_PATH)

    val traversalDescription = if (relTypes.isEmpty) {
      baseTraversalDescription.expand(PathExpanderBuilder.allTypes(toGraphDb(direction)).build())
    } else {
      val emptyExpander = PathExpanderBuilder.empty()
      val expander = relTypes.foldLeft(emptyExpander) {
        case (e, t) => e.add(RelationshipType.withName(t), toGraphDb(direction))
      }
      baseTraversalDescription.expand(expander.build())
    }
    traversalDescription.traverse(entityAccessor.newNodeEntity(realNode)).iterator().asScala
  }

  private def getDatabaseService = {
    transactionalContext.graph
      .asInstanceOf[GraphDatabaseCypherService]
      .getGraphDatabaseService
  }

  override def nodeCountByCountStore(labelId: Int): Long = {
    reads().countsForNode(labelId)
  }

  override def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long = {
    reads().countsForRelationship(startLabelId, typeId, endLabelId)
  }

  override def lockNodes(nodeIds: Long*): Unit =
    nodeIds.sorted.foreach(transactionalContext.kernelTransaction.locks().acquireExclusiveNodeLock(_))

  override def lockRelationships(relIds: Long*): Unit =
    relIds.sorted
      .foreach(transactionalContext.kernelTransaction.locks().acquireExclusiveRelationshipLock(_))

  override def singleShortestPath(left: Long, right: Long, depth: Int, expander: Expander,
                                  pathPredicate: KernelPredicate[Path],
                                  filters: Seq[KernelPredicate[Entity]]): Option[Path] = {
    val pathFinder = buildPathFinder(depth, expander, pathPredicate, filters)

    //could probably do without node proxies here
    Option(pathFinder.findSinglePath(entityAccessor.newNodeEntity(left), entityAccessor.newNodeEntity(right)))
  }

  override def allShortestPath(left: Long, right: Long, depth: Int, expander: Expander,
                               pathPredicate: KernelPredicate[Path],
                               filters: Seq[KernelPredicate[Entity]]): scala.Iterator[Path] = {
    val pathFinder = buildPathFinder(depth, expander, pathPredicate, filters)

    pathFinder.findAllPaths(entityAccessor.newNodeEntity(left), entityAccessor.newNodeEntity(right)).iterator()
      .asScala
  }

  override def callReadOnlyProcedure(id: Int, args: Seq[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    CallSupport.callReadOnlyProcedure(transactionalContext.tc, id, args, allowed, context)

  override def callReadWriteProcedure(id: Int, args: Seq[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    CallSupport.callReadWriteProcedure(transactionalContext.tc, id, args, allowed, context)

  override def callSchemaWriteProcedure(id: Int, args: Seq[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] =
  CallSupport.callSchemaWriteProcedure(transactionalContext.tc, id, args, allowed, context)

  override def callDbmsProcedure(id: Int, args: Seq[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    CallSupport.callDbmsProcedure(transactionalContext.tc, id, args, allowed, context)

  override def callFunction(id: Int, args: Array[AnyValue], allowed: Array[String]): AnyValue =
    CallSupport.callFunction(transactionalContext.tc, id, args, allowed)

  override def aggregateFunction(id: Int, allowed: Array[String]): UserDefinedAggregator =
    CallSupport.aggregateFunction(transactionalContext.tc, id, allowed)

  private def buildPathFinder(depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path],
                              filters: Seq[KernelPredicate[Entity]]): ShortestPath = {
    val startExpander = expander match {
      case OnlyDirectionExpander(_, _, dir) =>
        PathExpanderBuilder.allTypes(toGraphDb(dir))
      case TypeAndDirectionExpander(_, _, typDirs) =>
        typDirs.foldLeft(PathExpanderBuilder.empty()) {
          case (acc, (typ, dir)) => acc.add(RelationshipType.withName(typ), toGraphDb(dir))
        }
    }

    val expanderWithNodeFilters = expander.nodeFilters.foldLeft(startExpander) {
      case (acc, filter) => acc.addNodeFilter((t: Entity) => filter.test(t))
    }
    val expanderWithAllPredicates = expander.relFilters.foldLeft(expanderWithNodeFilters) {
      case (acc, filter) => acc.addRelationshipFilter((t: Entity) => filter.test(t))
    }
    val shortestPathPredicate = new ShortestPathPredicate {
      override def test(path: Path): Boolean = pathPredicate.test(path)
    }

    new ShortestPath(new BasicEvaluationContext(transactionalContext.tc.transaction(), getDatabaseService), depth, expanderWithAllPredicates.build(), shortestPathPredicate) {
      override protected def filterNextLevelNodes(nextNode: Node): Node =
        if (filters.isEmpty) nextNode
        else if (filters.forall(filter => filter test nextNode)) nextNode
        else null
    }
  }

  override def detachDeleteNode(node: Long): Int = transactionalContext.dataWrite.nodeDetachDelete(node)

  override def assertSchemaWritesAllowed(): Unit =
    transactionalContext.kernelTransaction.schemaWrite()

  private def allocateAndTraceNodeCursor() = {
    val cursor = transactionalContext.cursors.allocateNodeCursor()
    resources.trace(cursor)
    cursor
  }

  private def allocateAndTraceRelationshipScanCursor() = {
    val cursor = transactionalContext.cursors.allocateRelationshipScanCursor()
    resources.trace(cursor)
    cursor
  }

  private def allocateAndTraceNodeValueIndexCursor() = {
    val cursor = transactionalContext.cursors.allocateNodeValueIndexCursor()
    resources.trace(cursor)
    cursor
  }

  private def allocateAndTraceNodeLabelIndexCursor() = {
    val cursor = transactionalContext.cursors.allocateNodeLabelIndexCursor()
    resources.trace(cursor)
    cursor
  }

  private def asKernelIndexOrder(indexOrder: IndexOrder): internal.schema.IndexOrder = indexOrder match {
    case IndexOrderAscending => KernelIndexOrder.ASCENDING
    case IndexOrderDescending => KernelIndexOrder.DESCENDING
    case IndexOrderNone => KernelIndexOrder.NONE
  }

  abstract class CursorIterator[T] extends Iterator[T] with AutoCloseable {
    private var _next: T = fetchNext()

    protected def fetchNext(): T

    protected def close(): Unit

    override def hasNext: Boolean = _next != null

    override def next(): T = {
      if (!hasNext) {
        close()
        Iterator.empty.next()
      }

      val current = _next
      _next = fetchNext()
      if (!hasNext) {
        close()
      }
      current
    }
  }

  class RelationshipCursorIterator(selectionCursor: RelationshipSelectionCursor) extends RelationshipIterator with AutoCloseable {

    import RelationshipCursorIterator.{NOT_INITIALIZED, NO_ID}

    private var _next = NOT_INITIALIZED
    private var typeId: Int = NO_ID
    private var source: Long = NO_ID
    private var target: Long = NO_ID
    resources.trace(selectionCursor)

    override def relationshipVisit[EXCEPTION <: Exception](relationshipId: Long,
                                                           visitor: RelationshipVisitor[EXCEPTION]): Boolean = {
      visitor.visit(relationshipId, typeId, source, target)
      true
    }

    private def fetchNext(): Long = if (selectionCursor.next()) selectionCursor.relationshipReference() else -1L

    override def hasNext: Boolean = {
      if (_next == NOT_INITIALIZED) {
        _next = fetchNext()
      }

      _next >= 0
    }

    //We store the current state in case the underlying cursor is
    //closed when calling next.
    private def storeState(): Unit = {
      typeId = selectionCursor.`type`()
      source = selectionCursor.sourceNodeReference()
      target = selectionCursor.targetNodeReference()
    }

    override def next(): Long = {
      if (!hasNext) {
        selectionCursor.close()
        Iterator.empty.next()
      }

      val current = _next
      storeState()
      //Note that if no more elements are found the selection cursor
      //will be closed so no need to do a extra check after fetching.
      _next = fetchNext()

      current
    }

    override def close(): Unit = selectionCursor.close()
  }

  object RelationshipCursorIterator {
    private val NOT_INITIALIZED = -2L
    private val NO_ID = -1
  }

  abstract class PrimitiveCursorIterator extends PrimitiveLongResourceIterator {
    private var _next: Long = fetchNext()

    protected def fetchNext(): Long

    override def hasNext: Boolean = _next >= 0

    override def next(): Long = {
      if (!hasNext) {
        close()
        Iterator.empty.next()
      }

      val current = _next
      _next = fetchNext()
      if (!hasNext) close()

      current
    }
  }

  class ValuedNodeIndexCursor(inner: NodeValueIndexCursor, values: Array[Value]) extends DefaultCloseListenable with NodeValueIndexCursor {

    override def numberOfProperties(): Int = values.length

    override def propertyKey(offset: Int): Int = inner.propertyKey(offset)

    override def hasValue: Boolean = true

    override def propertyValue(offset: Int): Value = values(offset)

    override def node(cursor: NodeCursor): Unit = inner.node(cursor)

    override def nodeReference(): Long = inner.nodeReference()

    override def next(): Boolean = inner.next()

    override def closeInternal(): Unit = inner.close()

    // We do not call getCloseListener.onClosed(inner) here since
    // that will already happen in closeInternal.
    override def close(): Unit = closeInternal()

    override def isClosed: Boolean = inner.isClosed

    override def score(): Float = inner.score()

    override def setTracer(tracer: KernelReadTracer): Unit = inner.setTracer(tracer)

    override def removeTracer(): Unit = inner.removeTracer()
  }
}

object TransactionBoundQueryContext {

  trait IndexSearchMonitor {

    def indexSeek(index: IndexDescriptor, values: Seq[Any]): Unit

    def lockingUniqueIndexSeek(index: IndexDescriptor, values: Seq[Any]): Unit
  }

  object IndexSearchMonitor {
    val NOOP: IndexSearchMonitor = new IndexSearchMonitor {
      override def indexSeek(index: IndexDescriptor, values: Seq[Any]): Unit = {}

      override def lockingUniqueIndexSeek(index: IndexDescriptor,
                                          values: Seq[Any]): Unit = {}
    }
  }
}
