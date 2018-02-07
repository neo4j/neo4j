/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.collection.RawIterator
import org.neo4j.collection.primitive.{PrimitiveLongIterator, PrimitiveLongResourceIterator}
import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.planner.v3_4.spi.{IdempotentResult, IndexDescriptor}
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.interpreted.CypherOrdering.{BY_NUMBER, BY_STRING, BY_VALUE}
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.DirectionConverter.toGraphDb
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{OnlyDirectionExpander, TypeAndDirectionExpander}
import org.neo4j.cypher.internal.util.v3_4.{EntityNotFoundException, FailedIndexException}
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.v3_4.logical.plans.{QualifiedName, _}
import org.neo4j.graphalgo.impl.path.ShortestPath
import org.neo4j.graphalgo.impl.path.ShortestPath.ShortestPathPredicate
import org.neo4j.graphdb._
import org.neo4j.graphdb.security.URLAccessValidationError
import org.neo4j.graphdb.traversal.{Evaluators, TraversalDescription, Uniqueness}
import org.neo4j.internal.kernel.api._
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections.{allCursor, incomingCursor, outgoingCursor}
import org.neo4j.internal.kernel.api.helpers._
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.exceptions.ProcedureException
import org.neo4j.kernel.api.exceptions.schema.{AlreadyConstrainedException, AlreadyIndexedException}
import org.neo4j.kernel.api.proc.CallableUserAggregationFunction.Aggregator
import org.neo4j.kernel.api.proc.{QualifiedName => KernelQualifiedName}
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory
import org.neo4j.kernel.api.{exceptions, _}
import org.neo4j.kernel.guard.TerminationGuard
import org.neo4j.kernel.impl.api.RelationshipVisitor
import org.neo4j.kernel.impl.api.operations.KeyReadOperations
import org.neo4j.kernel.impl.api.store.RelationshipIterator
import org.neo4j.kernel.impl.core.{EmbeddedProxySPI, RelationshipProxy, ThreadToStatementContextBridge}
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker
import org.neo4j.kernel.impl.locking.ResourceTypes
import org.neo4j.kernel.impl.query.Neo4jTransactionalContext
import org.neo4j.kernel.impl.util.ValueUtils.{fromNodeProxy, fromRelationshipProxy}
import org.neo4j.kernel.impl.util.{DefaultValueMapper, NodeProxyWrappingNodeValue, RelationshipProxyWrappingValue}
import org.neo4j.values.{AnyValue, ValueMapper}
import org.neo4j.values.storable.{TextValue, Value, Values}
import org.neo4j.values.virtual.{ListValue, NodeValue, RelationshipValue, VirtualValues}

import scala.collection.Iterator
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

final class TransactionBoundQueryContext(val transactionalContext: TransactionalContextWrapper)
                                        (implicit indexSearchMonitor: IndexSearchMonitor)
  extends TransactionBoundTokenContext(transactionalContext.statement) with QueryContext with
    IndexDescriptorCompatibility {
  override val resources: ResourceManager = new ResourceManager
  override val nodeOps: NodeOperations = new NodeOperations
  override val relationshipOps: RelationshipOperations = new RelationshipOperations
  override lazy val entityAccessor: EmbeddedProxySPI =
    transactionalContext.graph.getDependencyResolver.resolveDependency(classOf[EmbeddedProxySPI])
  private lazy val valueMapper: ValueMapper[java.lang.Object] = new DefaultValueMapper(entityAccessor)

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) => if (writes().nodeAddLabel(node, labelId)) count + 1 else count
  }

  def createNewQueryContext(): QueryContext = {
    val statementProvider : ThreadToStatementContextBridge = transactionalContext.
      graph.
      getDependencyResolver.
      provideDependency(classOf[ThreadToStatementContextBridge]).
      get
    transactionalContext.tc.asInstanceOf[Neo4jTransactionalContext]
    val guard = new TerminationGuard
    val locker = new PropertyContainerLocker
    val query = transactionalContext.tc.executingQuery()

    val context = transactionalContext.tc.asInstanceOf[Neo4jTransactionalContext]
    val newTx = transactionalContext.graph.beginTransaction(context.transactionType, context.securityContext)
    val neo4jTransactionalContext = new Neo4jTransactionalContext(context.graph, statementProvider, guard, statementProvider, locker, newTx, statementProvider.get(), query)
    new TransactionBoundQueryContext(TransactionalContextWrapper(neo4jTransactionalContext))
  }
  //We cannot assign to value because of periodic commit
  private def writes() = transactionalContext.dataWrite
  private def reads() = transactionalContext.dataRead
  private val nodeCursor = allocateAndTraceNodeCursor()
  private val propertyCursor = allocateAndTracePropertyCursor()
  private lazy val nodeValueIndexCursor = allocateAndTraceNodeValueIndexCursor()
  private def tokenRead = transactionalContext.kernelTransaction.tokenRead()
  private def tokenWrite = transactionalContext.kernelTransaction.tokenWrite()

  override def withAnyOpenQueryContext[T](work: (QueryContext) => T): T = {
    if (transactionalContext.isOpen) {
      work(this)
    } else {
      val context = transactionalContext.getOrBeginNewIfClosed()
      var success = false
      try {
        val result = work(new TransactionBoundQueryContext(context))
        success = true
        result
      } finally {
        context.close(success)
      }
    }
  }

  override def createNode(): Node = entityAccessor.newNodeProxy(writes().nodeCreate())

  override def createNodeId(): Long = writes().nodeCreate()

  override def createRelationship(start: Long, end: Long, relType: Int): RelationshipValue = {
    val relId = transactionalContext.statement.dataWriteOperations().relationshipCreate(relType, start, end)
    fromRelationshipProxy(entityAccessor.newRelationshipProxy(relId, start, relType, end))
  }

  override def getOrCreateRelTypeId(relTypeName: String): Int =
    transactionalContext.statement.tokenWriteOperations().relationshipTypeGetOrCreateForName(relTypeName)

  override def getLabelsForNode(node: Long): ListValue = {
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


  override def getPropertiesForRelationship(relId: Long): Iterator[Int] =
    JavaConversionSupport.asScala(transactionalContext.statement.readOperations().relationshipGetPropertyKeys(relId))

  override def isLabelSetOnNode(label: Int, node: Long): Boolean = {
    reads().singleNode(node, nodeCursor)
    if (!nodeCursor.next()) false
    else nodeCursor.labels().contains(label)
  }

  override def getOrCreateLabelId(labelName: String): Int = {
    val id = tokenRead.nodeLabel(labelName)
    if (id != KeyReadOperations.NO_SUCH_LABEL) id
    else tokenWrite.labelGetOrCreateForName(labelName)
  }

  def getRelationshipsForIds(node: Long, dir: SemanticDirection,
                             types: Option[Array[Int]]): Iterator[RelationshipValue] = {
    val read = reads()
    read.singleNode(node, nodeCursor)
    if (!nodeCursor.next())Iterator.empty
    else {
      val cursor = dir match {
        case OUTGOING => outgoingCursor(transactionalContext.kernelTransaction.cursors(), nodeCursor, types.orNull)
        case INCOMING => incomingCursor(transactionalContext.kernelTransaction.cursors(), nodeCursor, types.orNull)
        case BOTH => allCursor(transactionalContext.kernelTransaction.cursors(), nodeCursor, types.orNull)
      }
      new CursorIterator[RelationshipValue] {
        override protected def close(): Unit = cursor.close()
        override protected def fetchNext(): RelationshipValue =
          if (cursor.next())
            fromRelationshipProxy(entityAccessor.newRelationshipProxy(cursor.relationshipReference(),
                                                                      cursor.sourceNodeReference(),
                                                                      cursor.`type`(),
                                                                      cursor.targetNodeReference()))
          else null
      }
    }
  }

  override def getRelationshipsForIdsPrimitive(node: Long, dir: SemanticDirection,
                                               types: Option[Array[Int]]): RelationshipIterator = {
    val read = reads()
    read.singleNode(node, nodeCursor)
    if (!nodeCursor.next()) RelationshipIterator.EMPTY
    else {
      val cursor = dir match {
        case OUTGOING => outgoingCursor(transactionalContext.kernelTransaction.cursors(), nodeCursor, types.orNull)
        case INCOMING => incomingCursor(transactionalContext.kernelTransaction.cursors(), nodeCursor, types.orNull)
        case BOTH => allCursor(transactionalContext.kernelTransaction.cursors(), nodeCursor, types.orNull)
      }
      new RelationshipCursorIterator(cursor)
    }
  }

  override def getRelationshipFor(relationshipId: Long, typeId: Int, startNodeId: Long,
                                  endNodeId: Long): RelationshipValue = try {
    fromRelationshipProxy(entityAccessor.newRelationshipProxy(relationshipId, startNodeId, typeId, endNodeId))
  } catch {
    case e: NotFoundException => throw new EntityNotFoundException(s"Relationship with id $relationshipId", e)
  }

  override def indexSeek(index: IndexReference, values: Seq[Any]): Iterator[NodeValue] = {
    indexSearchMonitor.indexSeek(index, values)
    val predicates = index.properties.zip(values).map(p => IndexQuery.exact(p._1, p._2))
    seek(index, predicates:_*)
  }

  override def indexReference(label: Int,
                              properties: Int*): IndexReference =
    transactionalContext.kernelTransaction.schemaRead().index(label, properties:_*)

  override def indexSeekByRange(index: IndexReference, value: Any): Iterator[NodeValue] = value match {

    case PrefixRange(null) => Iterator.empty
    case PrefixRange(prefix: String) =>
      indexSeekByPrefixRange(index, prefix)
    case range: InequalitySeekRange[Any] =>
      indexSeekByPrefixRange(index, range)

    case range =>
      throw new InternalException(s"Unsupported index seek by range: $range")
  }

  private def indexSeekByPrefixRange(index: IndexReference, range: InequalitySeekRange[Any]): scala.Iterator[NodeValue] = {
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
        throw new IllegalArgumentException(
          "Cannot compare a property against values that are neither strings nor numbers.")
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
          if (indexSeekByNumericalRange(index, numericRange).isEmpty
            || indexSeekByStringRange(index, stringRange).isEmpty) {
            Iterator.empty
          } else {
            throw new IllegalArgumentException(
              s"Cannot compare a property against both numbers and strings. They are incomparable.")
          }

        case (None, None) =>
          // If we get here, the non-empty list of range bounds was partitioned into two empty ones
          throw new IllegalStateException("Failed to partition range bounds")
      }
    }
  }

  private def indexSeekByPrefixRange(index: IndexReference, prefix: String): scala.Iterator[NodeValue] =
    seek(index, IndexQuery.stringPrefix(index.properties()(0), prefix))

  private def seek(index: IndexReference, query: IndexQuery*) = {
    val nodeCursor = allocateAndTraceNodeValueIndexCursor()
    reads().nodeIndexSeek(index, nodeCursor, IndexOrder.NONE, query:_*)
    new CursorIterator[NodeValue] {
      override protected def fetchNext(): NodeValue = {
        if (nodeCursor.next()) fromNodeProxy(entityAccessor.newNodeProxy(nodeCursor.nodeReference()))
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

  private def indexSeekByNumericalRange(index: IndexReference,
                                        range: InequalitySeekRange[Number]): scala.Iterator[NodeValue] = (range match {

    case rangeLessThan: RangeLessThan[Number] =>
      rangeLessThan.limit(BY_NUMBER).map { limit =>
        val rangePredicate = IndexQuery.range(index.properties()(0), null, false, limit.endPoint, limit.isInclusive)
        seek(index, rangePredicate)
      }

    case rangeGreaterThan: RangeGreaterThan[Number] =>
      rangeGreaterThan.limit(BY_NUMBER).map { limit =>
        val rangePredicate = IndexQuery.range(index.properties()(0), limit.endPoint, limit.isInclusive, null, false)
        seek(index, rangePredicate)
      }

    case RangeBetween(rangeGreaterThan, rangeLessThan) =>
      rangeGreaterThan.limit(BY_NUMBER).flatMap { greaterThanLimit =>
        rangeLessThan.limit(BY_NUMBER).map { lessThanLimit =>
          val rangePredicate = IndexQuery
            .range(index.properties()(0), greaterThanLimit.endPoint, greaterThanLimit.isInclusive,
                   lessThanLimit.endPoint,
                   lessThanLimit.isInclusive)
          seek(index, rangePredicate)
        }
      }
  }).getOrElse(Iterator.empty)

  private def indexSeekByStringRange(index: IndexReference,
                                     range: InequalitySeekRange[String]): scala.Iterator[NodeValue] = range match {

    case rangeLessThan: RangeLessThan[String] =>
      rangeLessThan.limit(BY_STRING).map { limit =>
        val rangePredicate = IndexQuery
          .range(index.properties()(0), null, false, limit.endPoint.asInstanceOf[String], limit.isInclusive)
        seek(index, rangePredicate)
      }.getOrElse(Iterator.empty)

    case rangeGreaterThan: RangeGreaterThan[String] =>
      rangeGreaterThan.limit(BY_STRING).map { limit =>
        val rangePredicate = IndexQuery
          .range(index.properties()(0), limit.endPoint.asInstanceOf[String], limit.isInclusive, null, false)
        seek(index, rangePredicate)
      }.getOrElse(Iterator.empty)

    case RangeBetween(rangeGreaterThan, rangeLessThan) =>
      rangeGreaterThan.limit(BY_STRING).flatMap { greaterThanLimit =>
        rangeLessThan.limit(BY_STRING).map { lessThanLimit =>
          val rangePredicate = IndexQuery
            .range(index.properties()(0), greaterThanLimit.endPoint.asInstanceOf[String], greaterThanLimit.isInclusive,
                   lessThanLimit.endPoint.asInstanceOf[String], lessThanLimit.isInclusive)
          seek(index, rangePredicate)
        }
      }.getOrElse(Iterator.empty)
  }

  override def indexScan(index: IndexReference): Iterator[NodeValue] =
    JavaConversionSupport.mapToScalaENFXSafe(indexScanPrimitive(index))(nodeOps.getById)

  override def indexScanPrimitive(index: IndexReference): PrimitiveLongResourceIterator = scan(index)

  override def indexScanByContains(index: IndexReference, value: String): Iterator[NodeValue] =
    seek(index, IndexQuery.stringContains(index.properties()(0), value))

  override def indexScanByEndsWith(index: IndexReference, value: String): Iterator[NodeValue] =
    seek(index, IndexQuery.stringSuffix(index.properties()(0), value))

  override def lockingUniqueIndexSeek(indexReference: IndexReference, values: Seq[Any]): Option[NodeValue] = {
    indexSearchMonitor.lockingUniqueIndexSeek(indexReference, values)
    val index = IndexDescriptorFactory.uniqueForLabel(indexReference.label, indexReference.properties:_*)
    val predicates = indexReference.properties.zip(values).map(p => IndexQuery.exact(p._1, p._2))
    val nodeId = transactionalContext.statement.readOperations().nodeGetFromUniqueIndexSeek(index, predicates:_*)
    if (StatementConstants.NO_SUCH_NODE == nodeId) None else Some(nodeOps.getById(nodeId))
  }

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) =>
      if (transactionalContext.statement.dataWriteOperations().nodeRemoveLabel(node, labelId)) count + 1 else count
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

  override def getNodesByLabelPrimitive(id: Int): PrimitiveLongIterator = {
    val cursor = allocateAndTraceNodeLabelIndexCursor()
    reads().nodeLabelScan(id, cursor)
    new PrimitiveCursorIterator {
      override protected def fetchNext(): Long = if (cursor.next()) cursor.nodeReference() else -1L
      override protected def close(): Unit = cursor.close()
    }
  }

  override def nodeGetDegree(node: Long, dir: SemanticDirection): Int =
    transactionalContext.statement.readOperations().nodeGetDegree(node, toGraphDb(dir))

  override def nodeGetDegree(node: Long, dir: SemanticDirection, relTypeId: Int): Int =
    transactionalContext.statement.readOperations().nodeGetDegree(node, toGraphDb(dir), relTypeId)

  override def nodeIsDense(node: Long): Boolean = transactionalContext.statement.readOperations().nodeIsDense(node)

  override def asObject(value: AnyValue): Any = {
    value match {
      case node: NodeProxyWrappingNodeValue => node.nodeProxy
      case edge: RelationshipProxyWrappingValue => edge.relationshipProxy
      case _ => withAnyOpenQueryContext(_=>value.map(valueMapper))
    }
  }

  class NodeOperations extends BaseOperations[NodeValue] {

    override def delete(id: Long) {
      try {
        writes().nodeDelete(id)
      } catch {
        case _: exceptions.EntityNotFoundException => // node has been deleted by another transaction, oh well...
      }
    }

    override def propertyKeyIds(id: Long): Iterator[Int] = {
      reads().singleNode(id, nodeCursor)
      if (!nodeCursor.next()) Iterator.empty
      else {
        val buffer = ArrayBuffer[Int]()
        nodeCursor.properties(propertyCursor)
        while (propertyCursor.next()) {
          buffer.append(propertyCursor.propertyKey())
        }
        buffer.iterator
      }
    }

    override def getProperty(id: Long, propertyKeyId: Int): Value = {
      reads().singleNode(id, nodeCursor)
      if (!nodeCursor.next()) {
        if (isDeletedInThisTx(id)) throw new EntityNotFoundException(
          s"Node with id $id has been deleted in this transaction")
        else Values.NO_VALUE
      } else {
        nodeCursor.properties(propertyCursor)
        while (propertyCursor.next()) {
          if (propertyCursor.propertyKey() == propertyKeyId) return propertyCursor.propertyValue()
        }
        Values.NO_VALUE
      }
    }

    override def hasProperty(id: Long, propertyKey: Int): Boolean = {
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

    override def removeProperty(id: Long, propertyKeyId: Int): Unit = {
      try {
        writes().nodeRemoveProperty(id, propertyKeyId)
      } catch {
        case _: exceptions.EntityNotFoundException => //ignore
      }
    }

    override def setProperty(id: Long, propertyKeyId: Int, value: Value): Unit = {
      try {
        writes().nodeSetProperty(id, propertyKeyId, value)
      } catch {
        case _: exceptions.EntityNotFoundException => //ignore
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

    override def allPrimitive: PrimitiveLongIterator = {
      val nodeCursor = allocateAndTraceNodeCursor()
      reads().allNodesScan(nodeCursor)
      new PrimitiveCursorIterator {
        override protected def fetchNext(): Long = if (nodeCursor.next()) nodeCursor.nodeReference() else -1L
        override protected def close(): Unit = nodeCursor.close()
      }
    }

    override def indexGet(name: String, key: String, value: Any): Iterator[NodeValue] =
      JavaConversionSupport
        .mapToScalaENFXSafe(transactionalContext.statement.readOperations().nodeExplicitIndexGet(name, key, value))(
          getById)

    override def indexQuery(name: String, query: Any): Iterator[NodeValue] =
      JavaConversionSupport
        .mapToScalaENFXSafe(transactionalContext.statement.readOperations().nodeExplicitIndexQuery(name, query))(
          getById)

    override def isDeletedInThisTx(id: Long): Boolean = transactionalContext.stateView
      .hasTxStateWithChanges && transactionalContext.stateView.txState().nodeIsDeletedInThisTx(id)

    override def acquireExclusiveLock(obj: Long): Unit =
      transactionalContext.statement.readOperations().acquireExclusive(ResourceTypes.NODE, obj)

    override def releaseExclusiveLock(obj: Long): Unit =
      transactionalContext.statement.readOperations().releaseExclusive(ResourceTypes.NODE, obj)

    override def exists(id: Long): Boolean =
      transactionalContext.statement.readOperations().nodeExists(id)

    override def getByIdIfExists(id: Long): Option[NodeValue] =
      if (transactionalContext.statement.readOperations().nodeExists(id))
        Some(fromNodeProxy(entityAccessor.newNodeProxy(id)))
      else
        None
  }

  class RelationshipOperations extends BaseOperations[RelationshipValue] {

    override def delete(id: Long) {
      try {
        transactionalContext.statement.dataWriteOperations().relationshipDelete(id)
      } catch {
        case _: exceptions.EntityNotFoundException => // node has been deleted by another transaction, oh well...
      }
    }

    override def propertyKeyIds(id: Long): Iterator[Int] = try {
      JavaConversionSupport
        .asScalaENFXSafe(transactionalContext.statement.readOperations().relationshipGetPropertyKeys(id))
    } catch {
      case _: exceptions.EntityNotFoundException => Iterator.empty
    }

    override def getProperty(id: Long, propertyKeyId: Int): Value = try {
      transactionalContext.statement.readOperations().relationshipGetProperty(id, propertyKeyId)
    } catch {
      case e: exceptions.EntityNotFoundException =>
        if (isDeletedInThisTx(id))
          throw new EntityNotFoundException(s"Relationship with id $id has been deleted in this transaction", e)
        else
          Values.NO_VALUE
    }

    override def hasProperty(id: Long, propertyKey: Int): Boolean = try {
      transactionalContext.statement.readOperations().relationshipHasProperty(id, propertyKey)
    } catch {
      case _: exceptions.EntityNotFoundException => false
    }

    override def removeProperty(id: Long, propertyKeyId: Int): Unit = {
      try {
        transactionalContext.statement.dataWriteOperations().relationshipRemoveProperty(id, propertyKeyId)
      } catch {
        case _: exceptions.EntityNotFoundException => //ignore
      }
    }

    override def setProperty(id: Long, propertyKeyId: Int, value: Value): Unit = {
      try {
        transactionalContext.statement.dataWriteOperations().relationshipSetProperty(id, propertyKeyId, value)
      } catch {
        case _: exceptions.EntityNotFoundException => //ignore
      }
    }

    override def getById(id: Long): RelationshipValue = try {
      fromRelationshipProxy(entityAccessor.newRelationshipProxy(id))
    } catch {
      case e: NotFoundException => throw new EntityNotFoundException(s"Relationship with id $id", e)
    }

    override def getByIdIfExists(id: Long): Option[RelationshipValue] = try {
      var relationship: RelationshipProxy = null
      transactionalContext.statement.readOperations().relationshipVisit(id, new RelationshipVisitor[Exception] {
        override def visit(relationshipId: Long, typeId: Int, startNodeId: Long, endNodeId: Long): Unit = {
          relationship = entityAccessor.newRelationshipProxy(relationshipId, startNodeId, typeId, endNodeId)
        }
      })
      Option(fromRelationshipProxy(relationship))
    } catch {
      case _: exceptions.EntityNotFoundException => None
    }

    override def all: Iterator[RelationshipValue] = {
      JavaConversionSupport
        .mapToScalaENFXSafe(transactionalContext.statement.readOperations().relationshipsGetAll())(getById)
    }

    override def allPrimitive: PrimitiveLongIterator =
      transactionalContext.statement.readOperations().relationshipsGetAll()

    override def indexGet(name: String, key: String, value: Any): Iterator[RelationshipValue] =
      JavaConversionSupport.mapToScalaENFXSafe(
        transactionalContext.statement.readOperations().relationshipExplicitIndexGet(name, key, value, -1, -1))(getById)

    override def indexQuery(name: String, query: Any): Iterator[RelationshipValue] =
      JavaConversionSupport.mapToScalaENFXSafe(
        transactionalContext.statement.readOperations().relationshipExplicitIndexQuery(name, query, -1, -1))(getById)

    override def isDeletedInThisTx(id: Long): Boolean =
      transactionalContext.stateView.hasTxStateWithChanges && transactionalContext.stateView.txState()
        .relationshipIsDeletedInThisTx(id)

    override def acquireExclusiveLock(obj: Long) =
      transactionalContext.statement.readOperations().acquireExclusive(ResourceTypes.RELATIONSHIP, obj)

    override def releaseExclusiveLock(obj: Long) =
      transactionalContext.statement.readOperations().acquireExclusive(ResourceTypes.RELATIONSHIP, obj)

    override def exists(id: Long): Boolean = {
      try {
        transactionalContext.statement.readOperations().relationshipVisit(id, NoopVisitor)
        true
      } catch {
        case e: exceptions.EntityNotFoundException => false
      }
    }
  }

  override def getOrCreatePropertyKeyId(propertyKey: String) =
    transactionalContext.statement.tokenWriteOperations().propertyKeyGetOrCreateForName(propertyKey)

  abstract class BaseOperations[T] extends Operations[T] {

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
    transactionalContext.statement.readOperations().schemaStateGetOrCreate(key, javaCreator)
  }

  override def addIndexRule(descriptor: IndexDescriptor): IdempotentResult[IndexDescriptor] = {
    val kernelDescriptor = cypherToKernelSchema(descriptor)
    try {
      IdempotentResult(
        kernelToCypher(transactionalContext.statement.schemaWriteOperations().indexCreate(kernelDescriptor)))
    } catch {
      case _: AlreadyIndexedException =>
        val indexDescriptor = transactionalContext.statement.readOperations().indexGetForSchema(
          SchemaDescriptorFactory.forLabel(kernelDescriptor.getLabelId, kernelDescriptor.getPropertyIds: _*))
        if (transactionalContext.statement.readOperations().indexGetState(indexDescriptor) == InternalIndexState.FAILED)
          throw new FailedIndexException(indexDescriptor.userDescription(tokenNameLookup))
       IdempotentResult(kernelToCypher(indexDescriptor), wasCreated = false)
    }
  }

  override def dropIndexRule(descriptor: IndexDescriptor) =
    transactionalContext.statement.schemaWriteOperations().indexDrop(cypherToKernel(descriptor))

  override def createNodeKeyConstraint(descriptor: IndexDescriptor): Boolean = try {
    transactionalContext.statement.schemaWriteOperations().nodeKeyConstraintCreate(cypherToKernelSchema(descriptor))
    true
  } catch {
    case existing: AlreadyConstrainedException => false
  }

  override def dropNodeKeyConstraint(descriptor: IndexDescriptor) =
    transactionalContext.statement.schemaWriteOperations()
      .constraintDrop(ConstraintDescriptorFactory.nodeKeyForSchema(cypherToKernelSchema(descriptor)))

  override def createUniqueConstraint(descriptor: IndexDescriptor): Boolean = try {
    transactionalContext.statement.schemaWriteOperations().uniquePropertyConstraintCreate(cypherToKernelSchema(descriptor))
    true
  } catch {
    case existing: AlreadyConstrainedException => false
  }

  override def dropUniqueConstraint(descriptor: IndexDescriptor) =
    transactionalContext.statement.schemaWriteOperations()
      .constraintDrop(ConstraintDescriptorFactory.uniqueForSchema(cypherToKernelSchema(descriptor)))

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): Boolean =
    try {
      transactionalContext.statement.schemaWriteOperations().nodePropertyExistenceConstraintCreate(
        SchemaDescriptorFactory.forLabel(labelId, propertyKeyId))
      true
    } catch {
      case existing: AlreadyConstrainedException => false
    }

  override def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int) =
    transactionalContext.statement.schemaWriteOperations()
      .constraintDrop(ConstraintDescriptorFactory.existsForLabel(labelId, propertyKeyId))

  override def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): Boolean =
    try {
      transactionalContext.statement.schemaWriteOperations().relationshipPropertyExistenceConstraintCreate(
        SchemaDescriptorFactory.forRelType(relTypeId, propertyKeyId))
      true
    } catch {
      case existing: AlreadyConstrainedException => false
    }

  override def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) =
    transactionalContext.statement.schemaWriteOperations()
      .constraintDrop(ConstraintDescriptorFactory.existsForRelType(relTypeId, propertyKeyId))

  override def getImportURL(url: URL): Either[String, URL] = transactionalContext.graph match {
    case db: GraphDatabaseQueryService =>
      try {
        Right(db.validateURLAccess(url))
      } catch {
        case error: URLAccessValidationError => Left(error.getMessage)
      }
  }

  override def edgeGetStartNode(edge: RelationshipValue) = edge.startNode()

  override def edgeGetEndNode(edge: RelationshipValue) = edge.endNode()

  private lazy val tokenNameLookup = new StatementTokenNameLookup(transactionalContext.statement.readOperations())

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
    transactionalContext.statement.readOperations().countsForNode(labelId)
  }

  override def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long = {
    transactionalContext.statement.readOperations().countsForRelationship(startLabelId, typeId, endLabelId)
  }

  override def lockNodes(nodeIds: Long*) =
    nodeIds.sorted.foreach(transactionalContext.statement.readOperations().acquireExclusive(ResourceTypes.NODE, _))

  override def lockRelationships(relIds: Long*) =
    relIds.sorted
      .foreach(transactionalContext.statement.readOperations().acquireExclusive(ResourceTypes.RELATIONSHIP, _))

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

  type KernelProcedureCall = (KernelQualifiedName, Array[AnyRef]) => RawIterator[Array[AnyRef], ProcedureException]
  type KernelFunctionCall = (KernelQualifiedName, Array[AnyValue]) => AnyValue
  type KernelAggregationFunctionCall = (KernelQualifiedName) => Aggregator

  private def shouldElevate(allowed: Array[String]): Boolean = {
    // We have to be careful with elevation, since we cannot elevate permissions in a nested procedure call
    // above the original allowed procedure mode. We enforce this by checking if mode is already an overridden mode.
    val accessMode = transactionalContext.securityContext.mode()
    allowed.nonEmpty && !accessMode.isOverridden && accessMode.allowsProcedureWith(allowed)
  }

  override def callReadOnlyProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) = {
    val call: KernelProcedureCall =
      if (shouldElevate(allowed))
        transactionalContext.statement.procedureCallOperations.procedureCallReadOverride(_, _)
      else
        transactionalContext.statement.procedureCallOperations.procedureCallRead(_, _)
    callProcedure(name, args, call)
  }

  override def callReadWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) = {
    val call: KernelProcedureCall =
      if (shouldElevate(allowed))
        transactionalContext.statement.procedureCallOperations.procedureCallWriteOverride(_, _)
      else
        transactionalContext.statement.procedureCallOperations.procedureCallWrite(_, _)
    callProcedure(name, args, call)
  }

  override def callSchemaWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) = {
    val call: KernelProcedureCall =
      if (shouldElevate(allowed))
        transactionalContext.statement.procedureCallOperations.procedureCallSchemaOverride(_, _)
      else
        transactionalContext.statement.procedureCallOperations.procedureCallSchema(_, _)
    callProcedure(name, args, call)
  }

  override def callDbmsProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) = {
    callProcedure(name, args,
                  transactionalContext.dbmsOperations.procedureCallDbms(_, _, transactionalContext.securityContext))
  }

  private def callProcedure(name: QualifiedName, args: Seq[Any], call: KernelProcedureCall) = {
    val kn = new KernelQualifiedName(name.namespace.asJava, name.name)
    val toArray = args.map(_.asInstanceOf[AnyRef]).toArray
    val read = call(kn, toArray)
    new scala.Iterator[Array[AnyRef]] {
      override def hasNext: Boolean = read.hasNext

      override def next(): Array[AnyRef] = read.next
    }
  }

  override def callFunction(name: QualifiedName, args: Seq[AnyValue], allowed: Array[String]) = {
    val call: KernelFunctionCall =
      if (shouldElevate(allowed))
        transactionalContext.statement.procedureCallOperations.functionCallOverride(_, _)
      else
        transactionalContext.statement.procedureCallOperations.functionCall(_, _)
    callFunction(name, args, call)
  }

  override def aggregateFunction(name: QualifiedName, allowed: Array[String]) = {
    val call: KernelAggregationFunctionCall =
      if (shouldElevate(allowed))
        transactionalContext.statement.procedureCallOperations.aggregationFunctionOverride(_)
      else
        transactionalContext.statement.procedureCallOperations.aggregationFunction(_)
    callAggregationFunction(name, call)
  }

  private def callFunction(name: QualifiedName, args: Seq[AnyValue],
                           call: KernelFunctionCall) = {
    val kn = new KernelQualifiedName(name.namespace.asJava, name.name)
    call(kn, args.toArray)
  }

  private def callAggregationFunction(name: QualifiedName,
                                      call: KernelAggregationFunctionCall) = {
    val kn = new KernelQualifiedName(name.namespace.asJava, name.name)
    val aggregator = call(kn)
    new UserDefinedAggregator {
      override def result = aggregator.result()

      override def update(args: IndexedSeq[Any]) = {
        val toArray = args.map(_.asInstanceOf[AnyRef]).toArray
        aggregator.update(toArray)
      }
    }
  }

  override def isGraphKernelResultValue(v: Any): Boolean = v.isInstanceOf[PropertyContainer] || v.isInstanceOf[Path]

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

  override def detachDeleteNode(node: Long): Int = {
    try {
      transactionalContext.statement.dataWriteOperations().nodeDetachDelete(node)
    } catch {
      case _: exceptions.EntityNotFoundException => 0 // node has been deleted by another transaction, oh well...
    }
  }

  override def assertSchemaWritesAllowed(): Unit =
    transactionalContext.statement.schemaWriteOperations()

  private def allocateAndTraceNodeCursor() = {
    val cursor = transactionalContext.cursors.allocateNodeCursor()
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
}

object TransactionBoundQueryContext {

  trait IndexSearchMonitor {

    def indexSeek(index: IndexReference, values: Seq[Any]): Unit

    def lockingUniqueIndexSeek(index: IndexReference, values: Seq[Any]): Unit
  }

}

object NoopVisitor extends RelationshipVisitor[RuntimeException] {

  // should just throw if the relationship is missing
  override def visit(relationshipId: Long, typeId: Int, startNodeId: Long, endNodeId: Long): Unit = {}
}
