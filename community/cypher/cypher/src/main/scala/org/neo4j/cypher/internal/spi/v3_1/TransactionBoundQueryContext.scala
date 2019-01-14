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
package org.neo4j.cypher.internal.spi.v3_1

import java.net.URL
import java.util.function.Predicate

import org.neo4j.collection.RawIterator
import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v3_1.MinMaxOrdering.{BY_NUMBER, BY_STRING, BY_VALUE}
import org.neo4j.cypher.internal.compiler.v3_1._
import org.neo4j.cypher.internal.compiler.v3_1.ast.convert.commands.DirectionConverter.toGraphDb
import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions
import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions.{KernelPredicate, OnlyDirectionExpander, TypeAndDirectionExpander}
import org.neo4j.cypher.internal.compiler.v3_1.pipes.matching.PatternNode
import org.neo4j.cypher.internal.compiler.v3_1.spi.SchemaTypes.{IndexDescriptor, NodePropertyExistenceConstraint, RelationshipPropertyExistenceConstraint, UniquenessConstraint}
import org.neo4j.cypher.internal.compiler.v3_1.spi._
import org.neo4j.cypher.internal.frontend.v3_1.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.frontend.v3_1.{Bound, EntityNotFoundException, FailedIndexException, SemanticDirection}
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.runtime.interpreted.ResourceManager
import org.neo4j.cypher.internal.spi.v3_1.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.spi.{CursorIterator, PrimitiveCursorIterator}
import org.neo4j.graphalgo.impl.path.ShortestPath
import org.neo4j.graphalgo.impl.path.ShortestPath.ShortestPathPredicate
import org.neo4j.graphdb.RelationshipType._
import org.neo4j.graphdb._
import org.neo4j.graphdb.security.URLAccessValidationError
import org.neo4j.graphdb.traversal.{Evaluators, TraversalDescription, Uniqueness}
import org.neo4j.internal.kernel.api
import org.neo4j.internal.kernel.api._
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.helpers.Nodes
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.{allCursor, incomingCursor, outgoingCursor}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api._
import org.neo4j.kernel.api.dbms.DbmsOperations
import org.neo4j.kernel.api.exceptions.schema.{AlreadyConstrainedException, AlreadyIndexedException}
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory
import org.neo4j.kernel.impl.api.store.DefaultIndexReference
import org.neo4j.kernel.impl.core.EmbeddedProxySPI
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

import scala.collection.Iterator
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

final class TransactionBoundQueryContext(txContext: TransactionalContextWrapper, val resources: ResourceManager = new ResourceManager)(implicit indexSearchMonitor: IndexSearchMonitor)
  extends TransactionBoundTokenContext(txContext.kernelTransaction) with QueryContext with SchemaDescriptorTranslation {

  type EntityAccessor = EmbeddedProxySPI

  val nodeOps = new NodeOperations
  val relationshipOps = new RelationshipOperations

  override lazy val entityAccessor = txContext.graph.getDependencyResolver.resolveDependency(classOf[EmbeddedProxySPI])

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) => if (writes.nodeAddLabel(node, labelId)) count + 1 else count
  }

  //We cannot assign to value because of periodic commit
  protected def reads(): Read = txContext.kernelTransaction.stableDataRead
  private def writes() = txContext.kernelTransaction.dataWrite
  private lazy val nodeCursor = allocateAndTraceNodeCursor()
  private lazy val relationshipScanCursor = allocateAndTraceRelationshipScanCursor()
  private lazy val propertyCursor = allocateAndTracePropertyCursor()
  private def tokenWrite = txContext.tc.kernelTransaction.tokenWrite()

  override def withAnyOpenQueryContext[T](work: (QueryContext) => T): T = {
    if (txContext.isOpen) {
      work(this)
    } else {
      val context = txContext.getOrBeginNewIfClosed()
      var success = false
      try {
        val result = work(new TransactionBoundQueryContext(context, resources))
        success = true
        result
      } finally {
        resources.close(true)
        context.close(success)
      }
    }
  }

  override def createNode(): Node =
    entityAccessor.newNodeProxy(writes.nodeCreate())

  override def createRelationship(start: Node, end: Node, relType: String): Relationship = start match {
    case null => throw new IllegalArgumentException("Expected to find a node, but found instead: null")
    case _ => start.createRelationshipTo(end, withName(relType))
  }

  override def createRelationship(start: Long, end: Long, relType: Int): Relationship = {
    val relId = writes.relationshipCreate(start, relType, end)
    entityAccessor.newRelationshipProxy(relId, start, relType, end)
  }

  override def getOrCreateRelTypeId(relTypeName: String): Int =
    tokenWrite.relationshipTypeGetOrCreateForName(relTypeName)

  override def getLabelsForNode(node: Long) = {
    val cursor = nodeCursor
    reads().singleNode(node, cursor)
    if (!cursor.next()) {
      if (nodeOps.isDeletedInThisTx(node))
        throw new EntityNotFoundException(s"Node with id $node has been deleted in this transaction")
      else
        Iterator.empty
    }
    val labelSet = cursor.labels()
    new Iterator[Int] {
      private var pos = 0
      override def hasNext: Boolean = pos < labelSet.numberOfLabels()

      override def next(): Int = {
        val current = labelSet.label(pos)
        pos += 1
        current
      }
    }
  }

  override def getPropertiesForNode(node: Long) = {
    val nodes = nodeCursor
    reads().singleNode(node, nodes)
    if (!nodes.next()) Iterator.empty
    else {
      val property = propertyCursor
      val buffer = ArrayBuffer[Int]()
      nodes.properties(property)
      while (property.next()) {
        buffer.append(property.propertyKey())
      }
      buffer.iterator
    }
  }

  override def getPropertiesForRelationship(relId: Long) = {
    val rels = relationshipScanCursor
    reads().singleRelationship(relId, rels)
    if (!rels.next()) Iterator.empty
    else {
      val property = propertyCursor
      val buffer = ArrayBuffer[Int]()
      rels.properties(property)
      while (property.next()) {
        buffer.append(property.propertyKey())
      }
      buffer.iterator
    }
  }

  override def isLabelSetOnNode(label: Int, node: Long) = {
    val cursor = nodeCursor
    reads().singleNode(node, cursor)
    if (!cursor.next()) false
    else cursor.labels().contains(label)
  }


  override def getOrCreateLabelId(labelName: String) =
    tokenWrite.labelGetOrCreateForName(labelName)

  def getRelationshipsForIds(node: Node, dir: SemanticDirection, types: Option[Seq[Int]]): Iterator[Relationship] = {
    val read = reads()
    val cursor = nodeCursor
    read.singleNode(node.getId, cursor)
    if (!cursor.next())Iterator.empty
    else {
      val selectionCursor = dir match {
        case OUTGOING => outgoingCursor(txContext.kernelTransaction.cursors(), cursor, types.map(_.toArray).orNull)
        case INCOMING => incomingCursor(txContext.kernelTransaction.cursors(), cursor, types.map(_.toArray).orNull)
        case BOTH => allCursor(txContext.kernelTransaction.cursors(), cursor, types.map(_.toArray).orNull)
      }
      new CursorIterator[Relationship] {
        override protected def close(): Unit = selectionCursor.close()
        override protected def fetchNext(): Relationship =
          if (selectionCursor.next()) entityAccessor.newRelationshipProxy(selectionCursor.relationshipReference(),
                                                                    selectionCursor.sourceNodeReference(),
                                                                    selectionCursor.`type`(),
                                                                    selectionCursor.targetNodeReference())
          else null
      }
    }
  }

  override def indexSeek(index: IndexDescriptor, value: Any) =
    seek(DefaultIndexReference.general(index.labelId, index.propertyId),
         IndexQuery.exact(index.propertyId, value))

  override def indexSeekByRange(index: IndexDescriptor, value: Any) = value match {

    case PrefixRange(null) => Iterator.empty
    case PrefixRange(prefix: String) =>
      indexSeekByPrefixRange(index, prefix)
    case range: InequalitySeekRange[Any] =>
      indexSeekByPrefixRange(index, range)

    case range =>
      throw new InternalException(s"Unsupported index seek by range: $range")
  }

  private def seek(index: IndexReference, query: IndexQuery*) = {
    val nodeCursor = allocateAndTraceNodeValueIndexCursor()
    reads().nodeIndexSeek(index, nodeCursor, IndexOrder.NONE, query:_*)
    new CursorIterator[Node] {
      override protected def fetchNext(): Node = {
        if (nodeCursor.next()) entityAccessor.newNodeProxy(nodeCursor.nodeReference())
        else null
      }

      override protected def close(): Unit = nodeCursor.close()
    }
  }

  private def scan(index: IndexReference) = {
    val nodeCursor = allocateAndTraceNodeValueIndexCursor()
    reads().nodeIndexScan(index, nodeCursor, IndexOrder.NONE)
    new PrimitiveCursorIterator {
      override protected def fetchNext(): Long =
        if (nodeCursor.next()) nodeCursor.nodeReference() else -1L

      override protected def close(): Unit = nodeCursor.close()
    }
  }

  private def indexSeekByPrefixRange(index: IndexDescriptor, range: InequalitySeekRange[Any]): scala.Iterator[Node] = {
    val groupedRanges = range.groupBy { (bound: Bound[Any]) =>
      bound.endPoint match {
        case n: Number => classOf[Number]
        case s: String => classOf[String]
        case c: Character => classOf[String]
        case _ => classOf[Any]
      }
    }

      val optNumericRange = groupedRanges.get(classOf[Number]).map(_.asInstanceOf[InequalitySeekRange[Number]])
      val optStringRange = groupedRanges.get(classOf[String]).map(_.mapBounds(_.toString))
      val anyRange = groupedRanges.get(classOf[Any])

      if (anyRange.nonEmpty) {
        // If we get back an exclusion test, the range could return values otherwise it is empty
        anyRange.get.inclusionTest[Any](BY_VALUE).map { test =>
          throw new IllegalArgumentException("Cannot compare a property against values that are neither strings nor numbers.")
        }.getOrElse(Iterator.empty)
      } else {
        (optNumericRange, optStringRange) match {
          case (Some(numericRange), None) => indexSeekByNumericalRange(index, numericRange)
          case (None, Some(stringRange)) => indexSeekByStringRange(index, stringRange)

          case (Some(numericRange), Some(stringRange)) =>
            // Consider MATCH (n:Person) WHERE n.prop < 1 AND n.prop > "London":
            // The order of predicate evaluation is unspecified, i.e.
            // LabelScan fby Filter(n.prop < 1) fby Filter(n.prop > "London") is a valid plan
            // If the first filter returns no results, the plan returns no results.
            // If the first filter returns any result, the following filter will fail since
            // comparing string against numbers throws an exception. Same for the reverse case.
            //
            // Below we simulate this behaviour:
            //
            if (indexSeekByNumericalRange( index, numericRange ).isEmpty
                || indexSeekByStringRange(index, stringRange).isEmpty) {
              Iterator.empty
            } else {
              throw new IllegalArgumentException(s"Cannot compare a property against both numbers and strings. They are incomparable.")
            }

          case (None, None) =>
            // If we get here, the non-empty list of range bounds was partitioned into two empty ones
            throw new IllegalStateException("Failed to partition range bounds")
        }
      }
  }

  private def indexSeekByPrefixRange(index: IndexDescriptor, prefix: String): scala.Iterator[Node] =
    seek(DefaultIndexReference.general(index.labelId, index.propertyId), IndexQuery.stringPrefix(index.propertyId, prefix))

  private def indexSeekByNumericalRange(index: IndexDescriptor, range: InequalitySeekRange[Number]): scala.Iterator[Node] =(range match {
    case rangeLessThan: RangeLessThan[Number] =>
      rangeLessThan.limit(BY_NUMBER).map { limit =>
        val rangePredicate = IndexQuery.range(index.propertyId, null, false, limit.endPoint, limit.isInclusive)
        seek(DefaultIndexReference.general(index.labelId, index.propertyId), rangePredicate)
      }

    case rangeGreaterThan: RangeGreaterThan[Number] =>
      rangeGreaterThan.limit(BY_NUMBER).map { limit =>
        val rangePredicate = IndexQuery.range(index.propertyId, limit.endPoint, limit.isInclusive, null, false)
        seek(DefaultIndexReference.general(index.labelId, index.propertyId), rangePredicate)
      }

    case RangeBetween(rangeGreaterThan, rangeLessThan) =>
      rangeGreaterThan.limit(BY_NUMBER).flatMap { greaterThanLimit =>
        rangeLessThan.limit(BY_NUMBER).map { lessThanLimit =>
          val rangePredicate = IndexQuery
            .range(index.propertyId, greaterThanLimit.endPoint, greaterThanLimit.isInclusive,
                   lessThanLimit.endPoint,
                   lessThanLimit.isInclusive)
          seek(DefaultIndexReference.general(index.labelId, index.propertyId), rangePredicate)
        }
      }
  }).getOrElse(Iterator.empty)


  private def indexSeekByStringRange(index: IndexDescriptor, range: InequalitySeekRange[String]): scala.Iterator[Node] = range match {

    case rangeLessThan: RangeLessThan[String] =>
      rangeLessThan.limit(BY_STRING).map { limit =>
        val rangePredicate = IndexQuery
          .range(index.propertyId, null, false, limit.endPoint.asInstanceOf[String], limit.isInclusive)
        seek(DefaultIndexReference.general(index.labelId, index.propertyId), rangePredicate)
      }.getOrElse(Iterator.empty)

    case rangeGreaterThan: RangeGreaterThan[String] =>
      rangeGreaterThan.limit(BY_STRING).map { limit =>
        val rangePredicate = IndexQuery
          .range(index.propertyId, limit.endPoint.asInstanceOf[String], limit.isInclusive, null, false)
        seek(DefaultIndexReference.general(index.labelId, index.propertyId), rangePredicate)
      }.getOrElse(Iterator.empty)

    case RangeBetween(rangeGreaterThan, rangeLessThan) =>
      rangeGreaterThan.limit(BY_STRING).flatMap { greaterThanLimit =>
        rangeLessThan.limit(BY_STRING).map { lessThanLimit =>
          val rangePredicate = IndexQuery
            .range(index.propertyId, greaterThanLimit.endPoint.asInstanceOf[String], greaterThanLimit.isInclusive,
                   lessThanLimit.endPoint.asInstanceOf[String], lessThanLimit.isInclusive)
          seek(DefaultIndexReference.general(index.labelId, index.propertyId), rangePredicate)
        }
      }.getOrElse(Iterator.empty)
  }

  override def indexScan(index: IndexDescriptor) = {
    val cursor = allocateAndTraceNodeValueIndexCursor()
    reads().nodeIndexScan(DefaultIndexReference.general(index.labelId, index.propertyId), cursor, IndexOrder.NONE)
    new CursorIterator[Node] {
      override protected def fetchNext(): Node = {
        if (cursor.next()) entityAccessor.newNodeProxy(cursor.nodeReference())
        else null
      }
      override protected def close(): Unit = cursor.close()
    }
  }

  override def indexScanByContains(index: IndexDescriptor, value: String) =
    seek(DefaultIndexReference.general(index.labelId, index.propertyId), IndexQuery.stringContains(index.propertyId, value))

  override def indexScanByEndsWith(index: IndexDescriptor, value: String) =
    seek(DefaultIndexReference.general(index.labelId, index.propertyId), IndexQuery.stringSuffix(index.propertyId, value))

  override def lockingUniqueIndexSeek(index: IndexDescriptor, value: Any): Option[Node] = {
    indexSearchMonitor.lockingUniqueIndexSeek(index, value)
    val nodeId = reads().lockingNodeUniqueIndexSeek(DefaultIndexReference.general(index.labelId, index.propertyId), IndexQuery.exact(index.propertyId, value))
    if (StatementConstants.NO_SUCH_NODE == nodeId) None else Some(nodeOps.getById(nodeId))
  }

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) =>
      if (writes().nodeRemoveLabel(node, labelId)) count + 1 else count
  }

  override def getNodesByLabel(id: Int): Iterator[Node] ={
    val cursor = allocateAndTraceNodeLabelIndexCursor()
    reads().nodeLabelScan(id, cursor)
    new CursorIterator[Node] {
      override protected def fetchNext(): Node = {
        if (cursor.next()) entityAccessor.newNodeProxy(cursor.nodeReference())
        else null
      }
      override protected def close(): Unit = cursor.close()
    }
  }

  override def nodeGetDegree(node: Long, dir: SemanticDirection): Int = {
    val cursor = nodeCursor
    reads().singleNode(node, cursor)
    if (!cursor.next()) 0
    else {
      dir match {
        case OUTGOING => Nodes.countOutgoing(cursor, txContext.kernelTransaction.cursors)
        case INCOMING => Nodes.countIncoming(cursor, txContext.kernelTransaction.cursors)
        case BOTH => Nodes.countAll(cursor, txContext.kernelTransaction.cursors)
      }
    }
  }

  override def nodeGetDegree(node: Long, dir: SemanticDirection, relTypeId: Int): Int = {
    val cursor = nodeCursor
    reads().singleNode(node, cursor)
    if (!cursor.next()) 0
    else {
      dir match {
        case OUTGOING => Nodes.countOutgoing(cursor, txContext.kernelTransaction.cursors, relTypeId)
        case INCOMING => Nodes.countIncoming(cursor, txContext.kernelTransaction.cursors, relTypeId)
        case BOTH => Nodes.countAll(cursor, txContext.kernelTransaction.cursors, relTypeId)
      }
    }
  }

  override def nodeIsDense(node: Long): Boolean = {
    val cursor = nodeCursor
    reads().singleNode(node, cursor)
    if (!cursor.next()) false
    else cursor.isDense
  }

  class NodeOperations extends BaseOperations[Node] {
    override def delete(obj: Node) {
        writes().nodeDelete(obj.getId)
    }

    override def propertyKeyIds(id: Long): Iterator[Int] = {
      val node = nodeCursor
      reads().singleNode(id, node)
      if (!node.next()) Iterator.empty
      else {
        val property = propertyCursor
        val buffer = ArrayBuffer[Int]()
        node.properties(property)
        while (property.next()) {
          buffer.append(property.propertyKey())
        }
        buffer.iterator
      }
    }

    override def getProperty(id: Long, propertyKeyId: Int): Any = {
      val node = nodeCursor
      reads().singleNode(id, node)
      if (!node.next()) {
        if (isDeletedInThisTx(id)) throw new EntityNotFoundException(
          s"Node with id $id has been deleted in this transaction")
        else null
      } else {
        val property = propertyCursor
        node.properties(property)
        while (property.next()) {
          if (property.propertyKey() == propertyKeyId) return property.propertyValue().asObject()
        }
        null
      }
    }

    override def hasProperty(id: Long, propertyKey: Int): Boolean = {
      val node = nodeCursor
      reads().singleNode(id, node)
      if (!node.next()) false
      else {
        val property = propertyCursor
        node.properties(property)
        while (property.next()) {
          if (property.propertyKey() == propertyKey) return true
        }
        false
      }
    }

    override def removeProperty(id: Long, propertyKeyId: Int): Unit = {
      try {
        writes().nodeRemoveProperty(id, propertyKeyId)
      } catch {
        case _: api.exceptions.EntityNotFoundException => //ignore
      }
    }

    override def setProperty(id: Long, propertyKeyId: Int, value: Any): Unit = {
      try {
        writes().nodeSetProperty(id, propertyKeyId, Values.of(value) )
      } catch {
        case _: api.exceptions.EntityNotFoundException => //ignore
      }
    }

    override def getById(id: Long): Node =
      if (reads().nodeExists(id))
        entityAccessor.newNodeProxy(id)
      else
        throw new EntityNotFoundException(s"Node with id $id")

    def getByIdIfExists(id: Long): Option[Node] =
      if (reads().nodeExists(id))
        Some(entityAccessor.newNodeProxy(id))
      else
        None

    override def all: Iterator[Node] = {
      val nodeCursor = allocateAndTraceNodeCursor()
      reads().allNodesScan(nodeCursor)
      new CursorIterator[Node] {
        override protected def fetchNext(): Node = {
          if (nodeCursor.next()) entityAccessor.newNodeProxy(nodeCursor.nodeReference())
          else null
        }

        override protected def close(): Unit = nodeCursor.close()
      }
    }

    override def indexGet(name: String, key: String, value: Any): Iterator[Node] ={
      val cursor = allocateAndTraceNodeExplicitIndexCursor()
      txContext.kernelTransaction.indexRead().nodeExplicitIndexLookup(cursor, name, key, value )
      new CursorIterator[Node] {
        override protected def fetchNext(): Node = {
          while (cursor.next() ) {
            if (reads().nodeExists(cursor.nodeReference())) {
              return entityAccessor.newNodeProxy(cursor.nodeReference())
            }
          }
          null
        }
        override protected def close(): Unit = cursor.close()
      }
    }

    override def indexQuery(name: String, query: Any): Iterator[Node] ={
      val cursor = allocateAndTraceNodeExplicitIndexCursor()
      txContext.kernelTransaction.indexRead().nodeExplicitIndexQuery(cursor, name, query)
      new CursorIterator[Node] {
        override protected def fetchNext(): Node = {
          while (cursor.next() ) {
            if (reads().nodeExists(cursor.nodeReference())) {
              return entityAccessor.newNodeProxy(cursor.nodeReference())
            }
          }
          null
        }
        override protected def close(): Unit = cursor.close()
      }
    }

    override def isDeletedInThisTx(n: Node): Boolean = isDeletedInThisTx(n.getId)

    def isDeletedInThisTx(id: Long): Boolean =
      txContext.stateView.hasTxStateWithChanges && txContext.stateView.txState().nodeIsDeletedInThisTx(id)

    override def acquireExclusiveLock(obj: Long) =
      txContext.kernelTransaction.locks().acquireExclusiveNodeLock(obj)

    override def releaseExclusiveLock(obj: Long) =
      txContext.kernelTransaction.locks().releaseExclusiveNodeLock(obj)
  }

  class RelationshipOperations extends BaseOperations[Relationship] {

    override def delete(obj: Relationship) {
        writes().relationshipDelete(obj.getId)
    }

    override def propertyKeyIds(id: Long): Iterator[Int] = {
      val relationship = relationshipScanCursor
      reads().singleRelationship(id, relationship)
      if (!relationship.next()) Iterator.empty
      else {
        val buffer = ArrayBuffer[Int]()
        val property = propertyCursor
        relationship.properties(property)
        while (property.next()) {
          buffer.append(property.propertyKey())
        }
        buffer.iterator
      }
    }

    override def getProperty(id: Long, propertyKeyId: Int): Any = {
      val relationship = relationshipScanCursor
      reads().singleRelationship(id, relationship)
      if (!relationship.next()) {
        if (isDeletedInThisTx(id)) throw new EntityNotFoundException(
          s"Relationship with id $id has been deleted in this transaction")
        else null
      } else {
        val property = propertyCursor
        relationship.properties(property)
        while (property.next()) {
          if (property.propertyKey() == propertyKeyId) return property.propertyValue().asObject()
        }
        null
      }
    }

    override def hasProperty(id: Long, propertyKey: Int): Boolean =  {
      val relationship = relationshipScanCursor
      reads().singleRelationship(id, relationship)
      if (!relationship.next()) false
      else {
        val property = propertyCursor
        relationship.properties(property)
        while (property.next()) {
          if (property.propertyKey() == propertyKey) return true
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

    override def setProperty(id: Long, propertyKeyId: Int, value: Any): Unit = {
      try {
        writes().relationshipSetProperty(id, propertyKeyId, Values.of(value) )
      } catch {
        case _: api.exceptions.EntityNotFoundException => //ignore
      }
    }

    override def getById(id: Long): Relationship = try {
      entityAccessor.newRelationshipProxy(id)
    } catch {
      case e: api.exceptions.EntityNotFoundException =>
        throw new EntityNotFoundException(s"Relationship with id $id", e)
      case e: NotFoundException =>
        throw new EntityNotFoundException(s"Relationship with id $id", e)
    }

    def getByIdIfExists(id: Long): Option[Relationship] = {
      val cursor = relationshipScanCursor
      reads().singleRelationship(id, cursor)
      if (cursor.next())
        Some(entityAccessor.newRelationshipProxy(id, cursor.sourceNodeReference(), cursor.`type`(),
                                                 cursor.targetNodeReference()))
      else
        None
    }

    override def all: Iterator[Relationship] = {
      val relCursor = allocateAndTraceRelationshipScanCursor()
      reads().allRelationshipsScan(relCursor)
      new CursorIterator[Relationship] {
        override protected def fetchNext(): Relationship = {
          if (relCursor.next())
            entityAccessor.newRelationshipProxy(relCursor.relationshipReference(),
                                          relCursor.sourceNodeReference(), relCursor.`type`(),
                                          relCursor.targetNodeReference())
          else null
        }

        override protected def close(): Unit = relCursor.close()
      }
    }

    def indexGet(name: String, key: String, value: Any): Iterator[Relationship] = {
      val cursor = allocateAndTraceRelationshipExplicitIndexCursor()
      txContext.kernelTransaction.indexRead().relationshipExplicitIndexLookup(cursor, name, key, value, -1, -1)
      new CursorIterator[Relationship] {
        override protected def fetchNext(): Relationship = {
          while (cursor.next() ) {
            if (reads().relationshipExists(cursor.relationshipReference())) {
              return entityAccessor.newRelationshipProxy(cursor.relationshipReference(), cursor.sourceNodeReference(),
                                                  cursor.`type`(), cursor.targetNodeReference() )
            }
          }
          null
        }
        override protected def close(): Unit = cursor.close()
      }
    }

    def indexQuery(name: String, query: Any): Iterator[Relationship] = {
      val cursor = allocateAndTraceRelationshipExplicitIndexCursor()
      txContext.kernelTransaction.indexRead().relationshipExplicitIndexQuery(cursor, name, query, -1, -1)
      new CursorIterator[Relationship] {
        override protected def fetchNext(): Relationship = {
          while (cursor.next() ) {
            if (reads().relationshipExists(cursor.relationshipReference())) {
              return entityAccessor.newRelationshipProxy(cursor.relationshipReference(), cursor.sourceNodeReference(),
                                                         cursor.`type`(), cursor.targetNodeReference() )
            }
          }
          null
        }

        override protected def close(): Unit = cursor.close()
      }
    }

    override def isDeletedInThisTx(r: Relationship): Boolean = isDeletedInThisTx(r.getId)

    def isDeletedInThisTx(id: Long): Boolean =
      txContext.stateView.hasTxStateWithChanges && txContext.stateView.txState().relationshipIsDeletedInThisTx(id)

    override def acquireExclusiveLock(obj: Long) =
      txContext.kernelTransaction.locks().acquireExclusiveRelationshipLock(obj)

    override def releaseExclusiveLock(obj: Long) =
      txContext.kernelTransaction.locks().releaseExclusiveRelationshipLock(obj)
  }

  override def getOrCreatePropertyKeyId(propertyKey: String) =
    tokenWrite.propertyKeyGetOrCreateForName(propertyKey)

  abstract class BaseOperations[T <: PropertyContainer] extends Operations[T] {
    def primitiveLongIteratorToScalaIterator(primitiveIterator: PrimitiveLongIterator): Iterator[Long] =
      new Iterator[Long] {
        override def hasNext: Boolean = primitiveIterator.hasNext

        override def next(): Long = primitiveIterator.next
      }
  }

  override def getOrCreateFromSchemaState[K, V](key: K, creator: => V) = {
    val javaCreator = new java.util.function.Function[K, V]() {
      override def apply(key: K) = creator
    }
    txContext.kernelTransaction.schemaRead().schemaStateGetOrCreate(key, javaCreator)
  }

  override def addIndexRule(labelId: Int, propertyKeyId: Int): IdempotentResult[IndexDescriptor] = try {
    IdempotentResult(
      DefaultIndexReference.toDescriptor(
        txContext.kernelTransaction.schemaWrite().indexCreate(SchemaDescriptorFactory.forLabel(labelId, propertyKeyId)))
    )
  } catch {
    case _: AlreadyIndexedException =>

      val read = txContext.kernelTransaction.schemaRead
      val index = read.index(labelId, propertyKeyId)

      if (read.indexGetState(index) == InternalIndexState.FAILED)
        throw new FailedIndexException(index.userDescription(tokenNameLookup))
      IdempotentResult(DefaultIndexReference.toDescriptor(index), wasCreated = false)
  }

  override def dropIndexRule(labelId: Int, propertyKeyId: Int) =
    txContext.kernelTransaction.schemaWrite().indexDrop(DefaultIndexReference.general( labelId, propertyKeyId ))

  override def createUniqueConstraint(labelId: Int, propertyKeyId: Int): IdempotentResult[UniquenessConstraint] = try {
    txContext.kernelTransaction.schemaWrite().uniquePropertyConstraintCreate(
      SchemaDescriptorFactory.forLabel(labelId, propertyKeyId))
    IdempotentResult(
      SchemaTypes.UniquenessConstraint(labelId, propertyKeyId)
    )
  } catch {
    case _: AlreadyConstrainedException =>
      IdempotentResult(SchemaTypes.UniquenessConstraint(labelId, propertyKeyId), wasCreated = false)
  }

  override def dropUniqueConstraint(labelId: Int, propertyKeyId: Int) =
    txContext.kernelTransaction.schemaWrite().constraintDrop(ConstraintDescriptorFactory.uniqueForLabel(labelId, propertyKeyId))

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): IdempotentResult[NodePropertyExistenceConstraint] =
    try {
      txContext.kernelTransaction.schemaWrite().nodePropertyExistenceConstraintCreate(
        SchemaDescriptorFactory.forLabel(labelId, propertyKeyId)
      )
      IdempotentResult(SchemaTypes.NodePropertyExistenceConstraint(labelId, propertyKeyId))
    } catch {
      case existing: AlreadyConstrainedException =>
        IdempotentResult(SchemaTypes.NodePropertyExistenceConstraint(labelId, propertyKeyId), wasCreated = false)
    }

  override def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int) =
    txContext.kernelTransaction.schemaWrite().constraintDrop(ConstraintDescriptorFactory.existsForLabel(labelId, propertyKeyId))

  override def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): IdempotentResult[RelationshipPropertyExistenceConstraint] =
    try {
      txContext.kernelTransaction.schemaWrite().relationshipPropertyExistenceConstraintCreate(
        SchemaDescriptorFactory.forRelType(relTypeId, propertyKeyId)
      )
      IdempotentResult(SchemaTypes.RelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId))
    } catch {
      case _: AlreadyConstrainedException =>
        IdempotentResult(SchemaTypes.RelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId), wasCreated = false)
    }

  override def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) =
    txContext.kernelTransaction.schemaWrite().constraintDrop(ConstraintDescriptorFactory.existsForRelType(relTypeId,propertyKeyId))

  override def getImportURL(url: URL): Either[String,URL] = txContext.graph match {
    case db: GraphDatabaseQueryService =>
      try {
        Right(db.validateURLAccess(url))
      } catch {
        case error: URLAccessValidationError => Left(error.getMessage)
      }
  }

  override def relationshipStartNode(rel: Relationship) = rel.getStartNode

  override def relationshipEndNode(rel: Relationship) = rel.getEndNode

  private lazy val tokenNameLookup = new SilentTokenNameLookup(txContext.kernelTransaction.tokenRead())

  // Legacy dependency between kernel and compiler
  override def variableLengthPathExpand(node: PatternNode,
                                        realNode: Node,
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
    val baseTraversalDescription: TraversalDescription = txContext.graph.asInstanceOf[GraphDatabaseCypherService]
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
    traversalDescription.traverse(realNode).iterator().asScala
  }

  override def nodeCountByCountStore(labelId: Int): Long = {
    reads().countsForNode(labelId)
  }

  override def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long = {
    reads().countsForRelationship(startLabelId, typeId, endLabelId)
  }

  override def lockNodes(nodeIds: Long*) =
    nodeIds.sorted.foreach(txContext.kernelTransaction.locks().acquireExclusiveNodeLock(_))

  override def lockRelationships(relIds: Long*) =
    relIds.sorted.foreach(txContext.kernelTransaction.locks().acquireExclusiveRelationshipLock(_))

  override def singleShortestPath(left: Node, right: Node, depth: Int, expander: expressions.Expander,
                                  pathPredicate: KernelPredicate[Path],
                                  filters: Seq[KernelPredicate[PropertyContainer]]): Option[Path] = {
    val pathFinder = buildPathFinder(depth, expander, pathPredicate, filters)

    Option(pathFinder.findSinglePath(left, right))
  }

  override def allShortestPath(left: Node, right: Node, depth: Int, expander: expressions.Expander,
                               pathPredicate: KernelPredicate[Path],
                               filters: Seq[KernelPredicate[PropertyContainer]]): scala.Iterator[Path] = {
    val pathFinder = buildPathFinder(depth, expander, pathPredicate, filters)

    pathFinder.findAllPaths(left, right).iterator().asScala
  }

  type KernelProcedureCall = (procs.QualifiedName, Array[AnyRef]) => RawIterator[Array[AnyRef], ProcedureException]
  type KernelFunctionCall = (procs.QualifiedName, Array[AnyValue]) => AnyValue

  private def shouldElevate(allowed: Array[String]): Boolean = {
    // We have to be careful with elevation, since we cannot elevate permissions in a nested procedure call
    // above the original allowed procedure mode. We enforce this by checking if mode is already an overridden mode.
    val accessMode = txContext.securityContext.mode()
    allowed.nonEmpty && !accessMode.isOverridden && accessMode.allowsProcedureWith(allowed)
  }

  override def callReadOnlyProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) = {
    val call: KernelProcedureCall =
      if (shouldElevate(allowed))
        txContext.kernelTransaction.procedures().procedureCallReadOverride(_, _)
      else
        txContext.kernelTransaction.procedures().procedureCallRead(_, _)
    callProcedure(name, args, call)
  }

  override def callReadWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) = {
    val call: KernelProcedureCall =
      if (shouldElevate(allowed))
        txContext.kernelTransaction.procedures().procedureCallWriteOverride(_, _)
      else
        txContext.kernelTransaction.procedures().procedureCallWrite(_, _)
    callProcedure(name, args, call)
  }

  override def callSchemaWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) = {
    val call: KernelProcedureCall =
      if (shouldElevate(allowed))
        txContext.kernelTransaction.procedures().procedureCallSchemaOverride(_, _)
      else
        txContext.kernelTransaction.procedures().procedureCallSchema(_, _)
    callProcedure(name, args, call)
  }

  override def callDbmsProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) = {
    callProcedure(name, args,
                  txContext.dbmsOperations.procedureCallDbms(_,_,txContext.securityContext,
                                                                        txContext.resourceTracker))
  }

  private def callProcedure(name: QualifiedName, args: Seq[Any], call: KernelProcedureCall) = {
    val kn = new procs.QualifiedName(name.namespace.asJava, name.name)
    val toArray = args.map(_.asInstanceOf[AnyRef]).toArray
    val read = call(kn, toArray)
    new scala.Iterator[Array[AnyRef]] {
      override def hasNext: Boolean = read.hasNext
      override def next(): Array[AnyRef] = read.next
    }
  }

  override def callFunction(name: QualifiedName, args: Seq[Any], allowed: Array[String]) = {
    val call: KernelFunctionCall =
      if (shouldElevate(allowed))
        (name, args) => txContext.kernelTransaction.procedures().functionCallOverride(name, args)
      else
        (name, args) => txContext.kernelTransaction.procedures().functionCall(name, args)
    callFunction(name, args, call)
  }

  private def callFunction(name: QualifiedName, args: Seq[Any],
                           call: KernelFunctionCall) = {
    val kn = new procs.QualifiedName(name.namespace.asJava, name.name)
    val argArray = args.map(ValueUtils.of).toArray
    val result = call(kn, argArray)
    result.map(txContext.kernelTransaction.procedures().valueMapper)
  }

  override def isGraphKernelResultValue(v: Any): Boolean = v.isInstanceOf[PropertyContainer] || v.isInstanceOf[Path]

  private def buildPathFinder(depth: Int, expander: expressions.Expander, pathPredicate: KernelPredicate[Path],
                              filters: Seq[KernelPredicate[PropertyContainer]]): ShortestPath = {
    val startExpander = expander match {
      case OnlyDirectionExpander(_, _, dir) =>
        PathExpanderBuilder.allTypes(toGraphDb(dir))
      case TypeAndDirectionExpander(_,_,typDirs) =>
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

  override def detachDeleteNode(node: Node): Int = writes().nodeDetachDelete(node.getId)

  override def assertSchemaWritesAllowed(): Unit =
    txContext.kernelTransaction.schemaWrite()

  override def transactionalContext: QueryTransactionalContext = new QueryTransactionalContext {
    override type ReadOps = Nothing

    override type DbmsOps = DbmsOperations

    override def commitAndRestartTx(): Unit = txContext.commitAndRestartTx()

    override def readOperations: Nothing = ???

    override def isTopLevelTx: Boolean = txContext.isTopLevelTx

    override def dbmsOperations: DbmsOperations = txContext.tc.dbmsOperations()

    override def close(success: Boolean): Unit = txContext.tc.close(success)
  }

  private def allocateAndTraceNodeCursor() = {
    val cursor = txContext.kernelTransaction.cursors.allocateNodeCursor()
    resources.trace(cursor)
    cursor
  }

  private def allocateAndTraceRelationshipScanCursor() = {
    val cursor = txContext.kernelTransaction.cursors.allocateRelationshipScanCursor()
    resources.trace(cursor)
    cursor
  }

  private def allocateAndTraceNodeValueIndexCursor() = {
    val cursor = txContext.kernelTransaction.cursors.allocateNodeValueIndexCursor()
    resources.trace(cursor)
    cursor
  }

  private def allocateAndTraceNodeLabelIndexCursor() = {
    val cursor = txContext.kernelTransaction.cursors.allocateNodeLabelIndexCursor()
    resources.trace(cursor)
    cursor
  }

  private def allocateAndTracePropertyCursor() = {
    val cursor = txContext.kernelTransaction.cursors.allocatePropertyCursor()
    resources.trace(cursor)
    cursor
  }

  private def allocateAndTraceNodeExplicitIndexCursor() = {
    val cursor = txContext.kernelTransaction.cursors.allocateNodeExplicitIndexCursor()
    resources.trace(cursor)
    cursor
  }

  private def allocateAndTraceRelationshipExplicitIndexCursor() = {
    val cursor = txContext.kernelTransaction.cursors.allocateRelationshipExplicitIndexCursor()
    resources.trace(cursor)
    cursor
  }
}

object TransactionBoundQueryContext {
  trait IndexSearchMonitor {
    def indexSeek(index: IndexDescriptor, value: Any): Unit

    def lockingUniqueIndexSeek(index: IndexDescriptor, value: Any): Unit
  }
}
