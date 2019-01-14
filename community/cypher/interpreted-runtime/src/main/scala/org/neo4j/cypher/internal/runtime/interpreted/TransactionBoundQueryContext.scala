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
import java.util.function.Predicate

import org.eclipse.collections.api.iterator.LongIterator
import org.neo4j.collection.PrimitiveLongResourceIterator
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.planner.v3_5.spi.{IdempotentResult, IndexDescriptor}
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.DirectionConverter.toGraphDb
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{OnlyDirectionExpander, TypeAndDirectionExpander}
import org.neo4j.cypher.internal.v3_5.logical.plans.{IndexOrder, _}
import org.neo4j.graphalgo.impl.path.ShortestPath
import org.neo4j.graphalgo.impl.path.ShortestPath.ShortestPathPredicate
import org.neo4j.graphdb._
import org.neo4j.graphdb.security.URLAccessValidationError
import org.neo4j.graphdb.traversal.{Evaluators, TraversalDescription, Uniqueness}
import org.neo4j.internal.kernel.api
import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate
import org.neo4j.internal.kernel.api.{IndexOrder => KernelIndexOrder, _}
import org.neo4j.internal.kernel.api._
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.{allCursor, incomingCursor, outgoingCursor}
import org.neo4j.internal.kernel.api.helpers._
import org.neo4j.io.IOUtils
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.{ResourceManager => NotTheTypeWeWant, _}
import org.neo4j.kernel.api.exceptions.schema.{AlreadyConstrainedException, AlreadyIndexedException}
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory
import org.neo4j.kernel.api.schema.constraints.ConstraintDescriptorFactory
import org.neo4j.kernel.impl.api.store.RelationshipIterator
import org.neo4j.kernel.impl.core.{EmbeddedProxySPI, ThreadToStatementContextBridge}
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker
import org.neo4j.kernel.impl.query.Neo4jTransactionalContext
import org.neo4j.kernel.impl.util.ValueUtils.{fromNodeProxy, fromRelationshipProxy}
import org.neo4j.kernel.impl.util.{DefaultValueMapper, ValueUtils}
import org.neo4j.storageengine.api.RelationshipVisitor
import org.neo4j.values.storable.{TextValue, Value, Values, _}
import org.neo4j.values.virtual._
import org.neo4j.values.{AnyValue, ValueMapper}
import org.neo4j.cypher.internal.v3_5.expressions.SemanticDirection
import org.neo4j.cypher.internal.v3_5.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.v3_5.util.{EntityNotFoundException, FailedIndexException}

import scala.collection.Iterator
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

sealed class TransactionBoundQueryContext(val transactionalContext: TransactionalContextWrapper,
                                          val resources: ResourceManager = new ResourceManager)
                                        (implicit indexSearchMonitor: IndexSearchMonitor)
  extends TransactionBoundTokenContext(transactionalContext.kernelTransaction) with QueryContext with
    IndexDescriptorCompatibility {
  override val nodeOps: NodeOperations = new NodeOperations
  override val relationshipOps: RelationshipOperations = new RelationshipOperations
  override lazy val entityAccessor: EmbeddedProxySPI =
    transactionalContext.graph.getDependencyResolver.resolveDependency(classOf[EmbeddedProxySPI])
  private lazy val valueMapper: ValueMapper[java.lang.Object] = new DefaultValueMapper(entityAccessor)

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) => if (writes().nodeAddLabel(node, labelId)) count + 1 else count
  }

  def createNewQueryContext(): QueryContext = {
    val statementProvider: ThreadToStatementContextBridge = transactionalContext.
      graph.
      getDependencyResolver.
      provideDependency(classOf[ThreadToStatementContextBridge]).
      get
    val locker = new PropertyContainerLocker
    val query = transactionalContext.tc.executingQuery()

    val context = transactionalContext.tc.asInstanceOf[Neo4jTransactionalContext]
    val newTx = transactionalContext.graph.beginTransaction(context.transactionType, context.securityContext)
    val neo4jTransactionalContext = context.copyFrom(context.graph, statementProvider, locker, newTx,
      statementProvider.get(), query)
    new TransactionBoundQueryContext(TransactionalContextWrapper(neo4jTransactionalContext))
  }

  //We cannot assign to value because of periodic commit
  protected def reads(): Read = transactionalContext.dataRead

  private def writes() = transactionalContext.dataWrite

  private def allocateNodeCursor() = transactionalContext.cursors.allocateNodeCursor()
  private def allocateRelationshipScanCursor() = transactionalContext.cursors.allocateRelationshipScanCursor()
  private def allocatePropertyCursor() = transactionalContext.cursors.allocatePropertyCursor()

  private def tokenRead = transactionalContext.kernelTransaction.tokenRead()

  private def tokenWrite = transactionalContext.kernelTransaction.tokenWrite()

  lazy val withActiveRead: TransactionBoundQueryContext =
    new TransactionBoundQueryContext(transactionalContext, resources)(indexSearchMonitor) {
      override def reads(): Read = transactionalContext.dataRead
    }

  override def withAnyOpenQueryContext[T](work: (QueryContext) => T): T = {
    if (transactionalContext.isOpen) {
      work(this)
    } else {
      val context = transactionalContext.getOrBeginNewIfClosed()
      var success = false
      try {
        val newContext = new TransactionBoundQueryContext(context, resources)
        val result = work(newContext)
        success = true
        result
      } finally {
        resources.close(success)
        context.close(success)
      }
    }
  }

  override def createNode(labels: Array[Int]): NodeValue = ValueUtils.fromNodeProxy(entityAccessor.newNodeProxy(writes().nodeCreateWithLabels(labels)))

  override def createNodeId(labels: Array[Int]): Long = writes().nodeCreateWithLabels(labels)

  override def createRelationship(start: Long, end: Long, relType: Int): RelationshipValue = {
    val relId = transactionalContext.kernelTransaction.dataWrite().relationshipCreate(start, relType, end)
    fromRelationshipProxy(entityAccessor.newRelationshipProxy(relId, start, relType, end))
  }

  override def getOrCreateRelTypeId(relTypeName: String): Int =
    transactionalContext.kernelTransaction.tokenWrite().relationshipTypeGetOrCreateForName(relTypeName)

  override def getLabelsForNode(node: Long): ListValue = {
    val cursor = allocateNodeCursor()
    try {
      reads().singleNode(node, cursor)
      if (!cursor.next()) {
        if (nodeOps.isDeletedInThisTx(node))
          throw new EntityNotFoundException(s"Node with id $node has been deleted in this transaction")
        else
          VirtualValues.EMPTY_LIST
      }
      val labelSet = cursor.labels()
      val labelArray = new Array[TextValue](labelSet.numberOfLabels())
      var i = 0
      while (i < labelSet.numberOfLabels()) {
        labelArray(i) = Values.stringValue(tokenRead.nodeLabelName(labelSet.label(i)))
        i += 1
      }
      VirtualValues.list(labelArray: _*)
    } finally {
      cursor.close()
    }
  }

  override def isLabelSetOnNode(label: Int, node: Long): Boolean = {
    if (label == StatementConstants.NO_SUCH_LABEL) false
    else {
      val cursor = allocateNodeCursor()
      try {
        reads().singleNode(node, cursor)
        if (!cursor.next()) false
        else cursor.hasLabel(label)
      } finally {
        cursor.close()
      }
    }
  }

  override def getOrCreateLabelId(labelName: String): Int = {
    val id = tokenRead.nodeLabel(labelName)
    if (id != TokenRead.NO_TOKEN) id
    else tokenWrite.labelGetOrCreateForName(labelName)
  }

  def getRelationshipsForIds(node: Long, dir: SemanticDirection,
                             types: Option[Array[Int]]): Iterator[RelationshipValue] = {

    val cursor = allocateNodeCursor()
    val cursors = transactionalContext.cursors

    try {
      val read = reads()
      read.singleNode(node, cursor)
      if (!cursor.next()) Iterator.empty
      else {
        val selectionCursor = dir match {
          case OUTGOING => outgoingCursor(cursors, cursor, types.orNull)
          case INCOMING => incomingCursor(cursors, cursor, types.orNull)
          case BOTH => allCursor(cursors, cursor, types.orNull)
        }
        new CursorIterator[RelationshipValue] {
          override protected def close(): Unit = selectionCursor.close()

          override protected def fetchNext(): RelationshipValue =
            if (selectionCursor.next())
              fromRelationshipProxy(entityAccessor.newRelationshipProxy(selectionCursor.relationshipReference(),
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

  override def getRelationshipsForIdsPrimitive(node: Long, dir: SemanticDirection,
                                               types: Option[Array[Int]]): RelationshipIterator = {

    val cursor = allocateNodeCursor()
    try {
      val read = reads()
      val cursors = transactionalContext.cursors
      read.singleNode(node, cursor)
      if (!cursor.next()) RelationshipIterator.EMPTY
      else {
        val selectionCursor = dir match {
          case OUTGOING => outgoingCursor(cursors, cursor, types.orNull)
          case INCOMING => incomingCursor(cursors, cursor, types.orNull)
          case BOTH => allCursor(cursors, cursor, types.orNull)
        }
        new RelationshipCursorIterator(selectionCursor)
      }
    } finally {
      cursor.close()
    }
  }

  override def getRelationshipsCursor(node: Long, dir: SemanticDirection,
                                      types: Option[Array[Int]]): RelationshipSelectionCursor = {

    val cursor = allocateNodeCursor()
    try {
      val read = reads()
      val cursors = transactionalContext.cursors
      read.singleNode(node, cursor)
      if (!cursor.next()) RelationshipSelectionCursor.EMPTY
      else {
        dir match {
          case OUTGOING => outgoingCursor(cursors, cursor, types.orNull)
          case INCOMING => incomingCursor(cursors, cursor, types.orNull)
          case BOTH => allCursor(cursors, cursor, types.orNull)
        }
      }
    } finally {
      cursor.close()
    }
  }

  override def getRelationshipFor(relationshipId: Long, typeId: Int, startNodeId: Long,
                                  endNodeId: Long): RelationshipValue = try {
    fromRelationshipProxy(entityAccessor.newRelationshipProxy(relationshipId, startNodeId, typeId, endNodeId))
  } catch {
    case e: NotFoundException => throw new EntityNotFoundException(s"Relationship with id $relationshipId", e)
  }

  val RANGE_SEEKABLE_VALUE_GROUPS = Array(ValueGroup.NUMBER,
    ValueGroup.TEXT,
    ValueGroup.GEOMETRY,
    ValueGroup.DATE,
    ValueGroup.LOCAL_DATE_TIME,
    ValueGroup.ZONED_DATE_TIME,
    ValueGroup.LOCAL_TIME,
    ValueGroup.ZONED_TIME,
    ValueGroup.DURATION)

  override def indexSeek[RESULT <: AnyRef](index: IndexReference,
                                           needsValues: Boolean,
                                           indexOrder: IndexOrder,
                                           predicates: Seq[IndexQuery]): NodeValueIndexCursor = {

    val impossiblePredicate =
      predicates.exists {
        case p: IndexQuery.ExactPredicate => p.value() == Values.NO_VALUE
        case p: IndexQuery =>
          !RANGE_SEEKABLE_VALUE_GROUPS.contains(p.valueGroup())
      }

    if (impossiblePredicate) NodeValueIndexCursor.EMPTY
    else seek(index, needsValues, indexOrder, predicates: _*)
  }

  override def indexReference(label: Int,
                              properties: Int*): IndexReference =
    transactionalContext.kernelTransaction.schemaRead().index(label, properties: _*)

  private def seek[RESULT <: AnyRef](index: IndexReference,
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

  override def indexScan[RESULT <: AnyRef](index: IndexReference,
                                           needsValues: Boolean,
                                           indexOrder: IndexOrder): NodeValueIndexCursor = {
    val nodeCursor = allocateAndTraceNodeValueIndexCursor()
    reads().nodeIndexScan(index, nodeCursor, asKernelIndexOrder(indexOrder), needsValues)
    nodeCursor
  }

  override def indexSeekByContains[RESULT <: AnyRef](index: IndexReference,
                                                     needsValues: Boolean,
                                                     indexOrder: IndexOrder,
                                                     value: TextValue): NodeValueIndexCursor =
    seek(index, needsValues, indexOrder, IndexQuery.stringContains(index.properties()(0), value))

  override def indexSeekByEndsWith[RESULT <: AnyRef](index: IndexReference,
                                                     needsValues: Boolean,
                                                     indexOrder: IndexOrder,
                                                     value: TextValue): NodeValueIndexCursor =
    seek(index, needsValues, indexOrder, IndexQuery.stringSuffix(index.properties()(0), value))

  override def lockingUniqueIndexSeek[RESULT](indexReference: IndexReference,
                                              queries: Seq[IndexQuery.ExactPredicate]): NodeValueIndexCursor = {

    indexSearchMonitor.lockingUniqueIndexSeek(indexReference, queries)
    if (queries.exists(q => q.value() == Values.NO_VALUE))
      NodeValueHit.EMPTY
    else {
      val index = transactionalContext.kernelTransaction.schemaRead().indexReferenceUnchecked(indexReference.schema())
      val resultNodeId = reads().lockingNodeUniqueIndexSeek(index, queries: _*)
      if (StatementConstants.NO_SUCH_NODE == resultNodeId) {
        NodeValueHit.EMPTY
      } else {
        val values = queries.map(_.value()).toArray
        new NodeValueHit(resultNodeId, values)
      }
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
        if (cursor.next()) fromNodeProxy(entityAccessor.newNodeProxy(cursor.nodeReference()))
        else null
      }

      override protected def close(): Unit = cursor.close()
    }
  }

  override def nodeAsMap(id: Long): MapValue = {
    val node = allocateNodeCursor()
    val property = allocatePropertyCursor()
    try {
      reads().singleNode(id, node)
      if (!node.next()) VirtualValues.EMPTY_MAP
      else {
        val tokens = tokenRead
        node.properties(property)
        val builder = new MapValueBuilder()
        while (property.next()) {
          builder.add(tokens.propertyKeyName(property.propertyKey()), property.propertyValue())
        }
        builder.build()
      }
    } finally {
      IOUtils.closeAll(node, property)
    }
  }

  override def relationshipAsMap(id: Long): MapValue = {
    val relationship = allocateRelationshipScanCursor()
    val property = allocatePropertyCursor()
    try {
      reads().singleRelationship(id, relationship)
      if (!relationship.next()) VirtualValues.EMPTY_MAP
      else {
        val tokens = tokenRead
        relationship.properties(property)
        val builder = new MapValueBuilder()
        while (property.next()) {
          builder.add(tokens.propertyKeyName(property.propertyKey()), property.propertyValue())
        }
        builder.build()
      }
    } finally {
      IOUtils.closeAll(relationship, property)
    }
  }

  override def getNodesByLabelPrimitive(id: Int): LongIterator = {
    val cursor = allocateAndTraceNodeLabelIndexCursor()
    reads().nodeLabelScan(id, cursor)
    new PrimitiveCursorIterator {
      override protected def fetchNext(): Long = if (cursor.next()) cursor.nodeReference() else -1L

      override protected def close(): Unit = cursor.close()
    }
  }

  override def nodeGetOutgoingDegree(node: Long): Int = {
    val cursor = allocateNodeCursor()
    try {
      reads().singleNode(node, cursor)
      if (!cursor.next()) 0
      else Nodes.countOutgoing(cursor, transactionalContext.cursors)
    } finally {
     cursor.close()
    }
  }

  override def nodeGetIncomingDegree(node: Long): Int = {
    val cursor = allocateNodeCursor()
    try {
      reads().singleNode(node, cursor)
      if (!cursor.next()) 0
      else Nodes.countIncoming(cursor, transactionalContext.cursors)
    } finally {
      cursor.close()
    }
  }

  override def nodeGetTotalDegree(node: Long): Int = {
    val cursor = allocateNodeCursor()
    try {
      reads().singleNode(node, cursor)
      if (!cursor.next()) 0
      else Nodes.countAll(cursor, transactionalContext.cursors)
    } finally {
      cursor.close()
    }
  }

  override def nodeGetOutgoingDegree(node: Long, relationship: Int): Int = {
    val cursor = allocateNodeCursor()
    try {
      reads().singleNode(node, cursor)
      if (!cursor.next()) 0
      else Nodes.countOutgoing(cursor, transactionalContext.cursors, relationship)
    } finally {
      cursor.close()
    }
  }

  override def nodeGetIncomingDegree(node: Long, relationship: Int): Int = {
    val cursor = allocateNodeCursor()
    try {
      reads().singleNode(node, cursor)
      if (!cursor.next()) 0
      else Nodes.countIncoming(cursor, transactionalContext.cursors, relationship)
    } finally {
      cursor.close()
    }
  }

  override def nodeGetTotalDegree(node: Long, relationship: Int): Int = {
    val cursor = allocateNodeCursor()
    try {
      reads().singleNode(node, cursor)
      if (!cursor.next()) 0
      else Nodes.countAll(cursor, transactionalContext.cursors, relationship)
    } finally {
      cursor.close()
    }
  }

  override def nodeIsDense(node: Long): Boolean = {
    val cursor = allocateNodeCursor()
    try {
      reads().singleNode(node, cursor)
      if (!cursor.next()) false
      else cursor.isDense
    } finally {
      cursor.close()
    }
  }

  override def asObject(value: AnyValue): AnyRef = value.map(valueMapper)

  class NodeOperations extends BaseOperations[NodeValue] {

    override def delete(id: Long) {
      writes().nodeDelete(id)
    }

    override def propertyKeyIds(id: Long): Array[Int] = {
      val node = allocateNodeCursor()
      val property = allocatePropertyCursor()
      try {
        reads().singleNode(id, node)
        if (!node.next()) Array.empty
        else {
          val buffer = ArrayBuffer[Int]()
          node.properties(property)
          while (property.next()) {
            buffer.append(property.propertyKey())
          }
          buffer.toArray
        }
      } finally {
        IOUtils.closeAll(node, property)
      }
    }

    override def getProperty(id: Long, propertyKeyId: Int): Value = {
      val node = allocateNodeCursor()
      val property = allocatePropertyCursor()
      try {
        reads().singleNode(id, node)
        if (!node.next()) {
          if (isDeletedInThisTx(id)) throw new EntityNotFoundException(
            s"Node with id $id has been deleted in this transaction")
          else Values.NO_VALUE
        } else {
          node.properties(property)
          while (property.next()) {
            if (property.propertyKey() == propertyKeyId) return property.propertyValue()
          }
          Values.NO_VALUE
        }
      } finally {
        IOUtils.closeAll(node, property)
      }
    }

    override def getTxStateProperty(nodeId: Long, propertyKeyId: Int): Option[Value] = {
      if (isDeletedInThisTx(nodeId)) throw new EntityNotFoundException(
        s"Node with id $nodeId has been deleted in this transaction")
      val nodePropertyInTx = reads().nodePropertyChangeInTransactionOrNull(nodeId, propertyKeyId)
      Option(nodePropertyInTx)
    }

    override def hasProperty(id: Long, propertyKey: Int): Boolean = {
      val node = allocateNodeCursor()
      val property = allocatePropertyCursor()
      try {
        reads().singleNode(id, node)
        if (!node.next()) false
        else {
          node.properties(property)
          while (property.next()) {
            if (property.propertyKey() == propertyKey) return true
          }
          false
        }
      } finally {
        IOUtils.closeAll(node, property)
      }
    }

    override def hasTxStatePropertyForCachedNodeProperty(nodeId: Long, propertyKeyId: Int): Boolean = {
      if (isDeletedInThisTx(nodeId)) {
        // Node deleted in TxState
        false
      } else {
        val nodePropertyInTx = reads().nodePropertyChangeInTransactionOrNull(nodeId, propertyKeyId)
        nodePropertyInTx match {
          case null => true // no changes in TxState. Property is cached, so it must exist.
          case Values.NO_VALUE => false // property removed in TxState
          case _ => true // property changed in TxState
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
      fromNodeProxy(entityAccessor.newNodeProxy(id))
    } catch {
      case e: NotFoundException => throw new EntityNotFoundException(s"Node with id $id", e)
    }

    override def all: Iterator[NodeValue] = {
      val nodeCursor = allocateAndTraceNodeCursor()
      reads().allNodesScan(nodeCursor)
      new CursorIterator[NodeValue] {
        override protected def fetchNext(): NodeValue = {
          if (nodeCursor.next()) fromNodeProxy(entityAccessor.newNodeProxy(nodeCursor.nodeReference()))
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
        Some(fromNodeProxy(entityAccessor.newNodeProxy(id)))
      else
        None
  }

  class RelationshipOperations extends BaseOperations[RelationshipValue] {

    override def delete(id: Long) {
      writes().relationshipDelete(id)
    }

    override def propertyKeyIds(id: Long): Array[Int] = {
      val relationship = allocateRelationshipScanCursor()
      val property = allocatePropertyCursor()
      try {
        reads().singleRelationship(id, relationship)
        if (!relationship.next()) Array.empty
        else {
          val buffer = ArrayBuffer[Int]()
          relationship.properties(property)
          while (property.next()) {
            buffer.append(property.propertyKey())
          }
          buffer.toArray
        }
      } finally {
        IOUtils.closeAll(relationship, property)
      }
    }

    override def getProperty(id: Long, propertyKeyId: Int): Value = {
      val relationship = allocateRelationshipScanCursor()
      val property = allocatePropertyCursor()
      try {
        reads().singleRelationship(id, relationship)
        if (!relationship.next()) {
          if (isDeletedInThisTx(id)) throw new EntityNotFoundException(
            s"Relationship with id $id has been deleted in this transaction")
          else Values.NO_VALUE
        } else {
          relationship.properties(property)
          while (property.next()) {
            if (property.propertyKey() == propertyKeyId) return property.propertyValue()
          }
          Values.NO_VALUE
        }
      } finally {
        IOUtils.closeAll(relationship, property)
      }
    }

    override def hasProperty(id: Long, propertyKey: Int): Boolean = {
      val relationship = allocateRelationshipScanCursor()
      val property = allocatePropertyCursor()
      try {
        reads().singleRelationship(id, relationship)
        if (!relationship.next()) false
        else {
          relationship.properties(property)
          while (property.next()) {
            if (property.propertyKey() == propertyKey) return true
          }
          false
        }
      } finally {
        IOUtils.closeAll(relationship, property)
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
      fromRelationshipProxy(entityAccessor.newRelationshipProxy(id))
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
            val relProxy = entityAccessor.newRelationshipProxy(id, src, cursor.`type`(), dst)
            Some(fromRelationshipProxy(relProxy))
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
            fromRelationshipProxy(entityAccessor.newRelationshipProxy(relCursor.relationshipReference(),
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

    override def getTxStateProperty(obj: Long, propertyKeyId: Int): Option[Value] =
      throw new UnsupportedOperationException("Not implemented: there was no user of this method as there are no relationship indexes.")

    override def hasTxStatePropertyForCachedNodeProperty(nodeId: Long, propertyKeyId: Int): Boolean =
      throw new UnsupportedOperationException("Not implemented: there was no user of this method as there are no relationship indexes.")
  }

  override def getOrCreatePropertyKeyId(propertyKey: String): Int =
    tokenWrite.propertyKeyGetOrCreateForName(propertyKey)

  override def getOrCreatePropertyKeyIds(propertyKeys: Array[String]): Array[Int] = {
    val ids = new Array[Int](propertyKeys.length)
    tokenWrite.propertyKeyGetOrCreateForNames(propertyKeys, ids)
    ids
  }

  abstract class BaseOperations[T] extends Operations[T] {

    def primitiveLongIteratorToScalaIterator(primitiveIterator: LongIterator): Iterator[Long] =
      new Iterator[Long] {
        override def hasNext: Boolean = primitiveIterator.hasNext

        override def next(): Long = primitiveIterator.next
      }
  }

  override def addIndexRule(descriptor: IndexDescriptor): IdempotentResult[IndexReference] = {
    val kernelDescriptor = cypherToKernelSchema(descriptor)
    try {
      IdempotentResult(transactionalContext.kernelTransaction.schemaWrite().indexCreate(kernelDescriptor))
    } catch {
      case _: AlreadyIndexedException =>
        val indexReference = transactionalContext.kernelTransaction.schemaRead().index(kernelDescriptor.getLabelId, kernelDescriptor.getPropertyIds: _*)
        if (transactionalContext.kernelTransaction.schemaRead().indexGetState(indexReference) == InternalIndexState.FAILED) {
          val message = transactionalContext.kernelTransaction.schemaRead().indexGetFailure(indexReference)
          throw new FailedIndexException(indexReference.userDescription(tokenNameLookup), message)
        }
        IdempotentResult(indexReference, wasCreated = false)
    }
  }

  override def dropIndexRule(descriptor: IndexDescriptor): Unit =
    transactionalContext.kernelTransaction.schemaWrite().indexDrop(
      transactionalContext.kernelTransaction.schemaRead().indexReferenceUnchecked(descriptor.label, descriptor.properties.map(_.id): _*)
    )

  override def createNodeKeyConstraint(descriptor: IndexDescriptor): Boolean = try {
    transactionalContext.kernelTransaction.schemaWrite().nodeKeyConstraintCreate(cypherToKernelSchema(descriptor))
    true
  } catch {
    case _: AlreadyConstrainedException => false
  }

  override def dropNodeKeyConstraint(descriptor: IndexDescriptor): Unit =
    transactionalContext.kernelTransaction.schemaWrite()
      .constraintDrop(ConstraintDescriptorFactory.nodeKeyForSchema(cypherToKernelSchema(descriptor)))

  override def createUniqueConstraint(descriptor: IndexDescriptor): Boolean = try {
    transactionalContext.kernelTransaction.schemaWrite().uniquePropertyConstraintCreate(cypherToKernelSchema(descriptor))
    true
  } catch {
    case _: AlreadyConstrainedException => false
  }

  override def dropUniqueConstraint(descriptor: IndexDescriptor): Unit =
    transactionalContext.kernelTransaction.schemaWrite()
      .constraintDrop(ConstraintDescriptorFactory.uniqueForSchema(cypherToKernelSchema(descriptor)))

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): Boolean =
    try {
      transactionalContext.kernelTransaction.schemaWrite().nodePropertyExistenceConstraintCreate(
        SchemaDescriptorFactory.forLabel(labelId, propertyKeyId))
      true
    } catch {
      case existing: AlreadyConstrainedException => false
    }

  override def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): Unit =
    transactionalContext.kernelTransaction.schemaWrite()
      .constraintDrop(ConstraintDescriptorFactory.existsForLabel(labelId, propertyKeyId))

  override def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): Boolean =
    try {
      transactionalContext.kernelTransaction.schemaWrite().relationshipPropertyExistenceConstraintCreate(
        SchemaDescriptorFactory.forRelType(relTypeId, propertyKeyId))
      true
    } catch {
      case existing: AlreadyConstrainedException => false
    }

  override def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) =
    transactionalContext.kernelTransaction.schemaWrite()
      .constraintDrop(ConstraintDescriptorFactory.existsForRelType(relTypeId, propertyKeyId))

  override def getImportURL(url: URL): Either[String, URL] = transactionalContext.graph match {
    case db: GraphDatabaseQueryService =>
      try {
        Right(db.validateURLAccess(url))
      } catch {
        case error: URLAccessValidationError => Left(error.getMessage)
      }
  }

  override def relationshipGetStartNode(relationship: RelationshipValue) = relationship.startNode()

  override def relationshipGetEndNode(relationship: RelationshipValue) = relationship.endNode()

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
    val baseTraversalDescription: TraversalDescription = transactionalContext.graph
      .asInstanceOf[GraphDatabaseCypherService]
      .getGraphDatabaseService
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
    traversalDescription.traverse(entityAccessor.newNodeProxy(realNode)).iterator().asScala
  }

  override def nodeCountByCountStore(labelId: Int): Long = {
    reads().countsForNode(labelId)
  }

  override def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long = {
    reads().countsForRelationship(startLabelId, typeId, endLabelId)
  }

  override def lockNodes(nodeIds: Long*) =
    nodeIds.sorted.foreach(transactionalContext.kernelTransaction.locks().acquireExclusiveNodeLock(_))

  override def lockRelationships(relIds: Long*) =
    relIds.sorted
      .foreach(transactionalContext.kernelTransaction.locks().acquireExclusiveRelationshipLock(_))

  override def singleShortestPath(left: Long, right: Long, depth: Int, expander: Expander,
                                  pathPredicate: KernelPredicate[Path],
                                  filters: Seq[KernelPredicate[PropertyContainer]]): Option[Path] = {
    val pathFinder = buildPathFinder(depth, expander, pathPredicate, filters)

    //could probably do without node proxies here
    Option(pathFinder.findSinglePath(entityAccessor.newNodeProxy(left), entityAccessor.newNodeProxy(right)))
  }

  override def allShortestPath(left: Long, right: Long, depth: Int, expander: Expander,
                               pathPredicate: KernelPredicate[Path],
                               filters: Seq[KernelPredicate[PropertyContainer]]): scala.Iterator[Path] = {
    val pathFinder = buildPathFinder(depth, expander, pathPredicate, filters)

    pathFinder.findAllPaths(entityAccessor.newNodeProxy(left), entityAccessor.newNodeProxy(right)).iterator()
      .asScala
  }

  override def callReadOnlyProcedure(id: Int, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] =
    CallSupport.callReadOnlyProcedure(transactionalContext.tc, id, args, allowed)

  override def callReadWriteProcedure(id: Int, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] =
    CallSupport.callReadWriteProcedure(transactionalContext.tc, id, args, allowed)

  override def callSchemaWriteProcedure(id: Int, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] =
  CallSupport.callSchemaWriteProcedure(transactionalContext.tc, id, args, allowed)

  override def callDbmsProcedure(id: Int, args: Seq[Any], allowed: Array[String]) : Iterator[Array[AnyRef]] =
    CallSupport.callDbmsProcedure(transactionalContext.tc, id, args, allowed)

  override def callReadOnlyProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] =
    CallSupport.callReadOnlyProcedure(transactionalContext.tc, name, args, allowed)

  override def callReadWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] =
    CallSupport.callReadWriteProcedure(transactionalContext.tc, name, args, allowed)

  override def callSchemaWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] =
    CallSupport.callSchemaWriteProcedure(transactionalContext.tc, name, args, allowed)

  override def callDbmsProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] =
    CallSupport.callDbmsProcedure(transactionalContext.tc, name, args, allowed)


  override def callFunction(id: Int, args: Seq[AnyValue], allowed: Array[String]): AnyValue =
    CallSupport.callFunction(transactionalContext.tc, id, args, allowed)

  override def callFunction(name: QualifiedName, args: Seq[AnyValue], allowed: Array[String]): AnyValue =
    CallSupport.callFunction(transactionalContext.tc, name, args, allowed)

  override def aggregateFunction(id: Int, allowed: Array[String]): UserDefinedAggregator =
    CallSupport.aggregateFunction(transactionalContext.tc, id, allowed)

  override def aggregateFunction(name: QualifiedName, allowed: Array[String]): UserDefinedAggregator =
    CallSupport.aggregateFunction(transactionalContext.tc, name, allowed)

  private def buildPathFinder(depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path],
                              filters: Seq[KernelPredicate[PropertyContainer]]): ShortestPath = {
    val startExpander = expander match {
      case OnlyDirectionExpander(_, _, dir) =>
        PathExpanderBuilder.allTypes(toGraphDb(dir))
      case TypeAndDirectionExpander(_, _, typDirs) =>
        typDirs.foldLeft(PathExpanderBuilder.empty()) {
          case (acc, (typ, dir)) => acc.add(RelationshipType.withName(typ), toGraphDb(dir))
        }
    }

    val expanderWithNodeFilters = expander.nodeFilters.foldLeft(startExpander) {
      case (acc, filter) => acc.addNodeFilter(new Predicate[PropertyContainer] {
        override def test(t: PropertyContainer): Boolean = filter.test(t)
      })
    }
    val expanderWithAllPredicates = expander.relFilters.foldLeft(expanderWithNodeFilters) {
      case (acc, filter) => acc.addRelationshipFilter(new Predicate[PropertyContainer] {
        override def test(t: PropertyContainer): Boolean = filter.test(t)
      })
    }
    val shortestPathPredicate = new ShortestPathPredicate {
      override def test(path: Path): Boolean = pathPredicate.test(path)
    }

    new ShortestPath(depth, expanderWithAllPredicates.build(), shortestPathPredicate) {
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

  private def allocateAndTracePropertyCursor() = {
    val cursor = transactionalContext.cursors.allocatePropertyCursor()
    resources.trace(cursor)
    cursor
  }

  private def asKernelIndexOrder(indexOrder: IndexOrder): KernelIndexOrder = indexOrder match {
    case IndexOrderAscending => KernelIndexOrder.ASCENDING
    case IndexOrderDescending => KernelIndexOrder.DESCENDING
    case IndexOrderNone => KernelIndexOrder.NONE
  }

  abstract class CursorIterator[T] extends Iterator[T] {
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

  class RelationshipCursorIterator(selectionCursor: RelationshipSelectionCursor) extends RelationshipIterator {

    import RelationshipCursorIterator.{NOT_INITIALIZED, NO_ID}

    private var _next = NOT_INITIALIZED
    private var typeId: Int = NO_ID
    private var source: Long = NO_ID
    private var target: Long = NO_ID

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

  class ValuedNodeIndexCursor(inner: NodeValueIndexCursor, values: Array[Value]) extends NodeValueIndexCursor {

    override def numberOfProperties(): Int = values.length

    override def propertyKey(offset: Int): Int = inner.propertyKey(offset)

    override def hasValue: Boolean = true

    override def propertyValue(offset: Int): Value = values(offset)

    override def node(cursor: NodeCursor): Unit = inner.node(cursor)

    override def nodeReference(): Long = inner.nodeReference()

    override def next(): Boolean = inner.next()

    override def close(): Unit = inner.close()

    override def isClosed: Boolean = inner.isClosed
  }
}

object TransactionBoundQueryContext {

  trait IndexSearchMonitor {

    def indexSeek(index: IndexReference, values: Seq[Any]): Unit

    def lockingUniqueIndexSeek(index: IndexReference, values: Seq[Any]): Unit
  }

}
