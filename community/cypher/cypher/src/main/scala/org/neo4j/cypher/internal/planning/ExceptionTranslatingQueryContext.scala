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
package org.neo4j.cypher.internal.planning

import java.net.URL

import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.macros.TranslateExceptionMacros.translateException
import org.neo4j.cypher.internal.macros.TranslateExceptionMacros.translateIterator
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.Expander
import org.neo4j.cypher.internal.runtime.KernelPredicate
import org.neo4j.cypher.internal.runtime.NodeOperations
import org.neo4j.cypher.internal.runtime.Operations
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryTransactionalContext
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.RelationshipOperations
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.UserDefinedAggregator
import org.neo4j.cypher.internal.runtime.interpreted.DelegatingQueryTransactionalContext
import org.neo4j.graphdb.Entity
import org.neo4j.graphdb.Path
import org.neo4j.internal.kernel.api.IndexQuery
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.kernel.impl.core.TransactionalEntityFactory
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.RelationshipValue

import scala.collection.Iterator

class ExceptionTranslatingQueryContext(val inner: QueryContext) extends QueryContext with ExceptionTranslationSupport {

  override def entityAccessor: TransactionalEntityFactory = inner.entityAccessor

  override def resources: ResourceManager = inner.resources

  override def transactionalContext =
    new ExceptionTranslatingTransactionalContext(inner.transactionalContext)

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int =
    translateException(tokenNameLookup, inner.setLabelsOnNode(node, labelIds))

  override def createNode(labels: Array[Int]): NodeValue =
    translateException(tokenNameLookup, inner.createNode(labels))

  override def createNodeId(labels: Array[Int]): Long =
    translateException(tokenNameLookup, inner.createNodeId(labels))

  override def getLabelsForNode(node: Long, nodeCursor: NodeCursor): ListValue =
    translateException(tokenNameLookup, inner.getLabelsForNode(node, nodeCursor))

  override def getTypeForRelationship(id: Long, cursor: RelationshipScanCursor): TextValue =
    translateException(tokenNameLookup, inner.getTypeForRelationship(id, cursor))

  override def getLabelName(id: Int): String =
    translateException(tokenNameLookup, inner.getLabelName(id))

  override def getOptLabelId(labelName: String): Option[Int] =
    translateException(tokenNameLookup, inner.getOptLabelId(labelName))

  override def getLabelId(labelName: String): Int =
    translateException(tokenNameLookup, inner.getLabelId(labelName))

  override def getOrCreateLabelId(labelName: String): Int =
    translateException(tokenNameLookup, inner.getOrCreateLabelId(labelName))

  override val nodeOps: NodeOperations =
    new ExceptionTranslatingOperations[NodeValue, NodeCursor](inner.nodeOps) with NodeOperations

  override val relationshipOps: RelationshipOperations =
    new ExceptionTranslatingOperations[RelationshipValue, RelationshipScanCursor](inner.relationshipOps) with RelationshipOperations

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int =
    translateException(tokenNameLookup, inner.removeLabelsFromNode(node, labelIds))

  override def getPropertyKeyName(propertyKeyId: Int): String =
    translateException(tokenNameLookup, inner.getPropertyKeyName(propertyKeyId))

  override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] =
    translateException(tokenNameLookup, inner.getOptPropertyKeyId(propertyKeyName))

  override def getPropertyKeyId(propertyKey: String): Int =
    translateException(tokenNameLookup, inner.getPropertyKeyId(propertyKey))

  override def getOrCreatePropertyKeyId(propertyKey: String): Int =
    translateException(tokenNameLookup, inner.getOrCreatePropertyKeyId(propertyKey))

  override def getOrCreatePropertyKeyIds(propertyKeys: Array[String]): Array[Int] =
    translateException(tokenNameLookup, inner.getOrCreatePropertyKeyIds(propertyKeys))

  override def addIndexRule(labelId: Int, propertyKeyIds: Seq[Int], name: Option[String]): IndexDescriptor =
    translateException(tokenNameLookup, inner.addIndexRule(labelId, propertyKeyIds, name))

  override def dropIndexRule(labelId: Int, propertyKeyIds: Seq[Int]): Unit =
    translateException(tokenNameLookup, inner.dropIndexRule(labelId, propertyKeyIds))

  override def dropIndexRule(name: String): Unit =
    translateException(tokenNameLookup, inner.dropIndexRule(name))

  override def indexExists(name: String): Boolean =
    translateException(tokenNameLookup, inner.indexExists(name))

  override def constraintExists(name: String): Boolean =
    translateException(tokenNameLookup, inner.constraintExists(name))

  override def constraintExists(matchFn: ConstraintDescriptor => Boolean, entityId: Int, properties: Int*): Boolean =
    translateException(tokenNameLookup, inner.constraintExists(matchFn, entityId, properties: _*))

  override def indexReference(label: Int, properties: Int*): IndexDescriptor =
    translateException(tokenNameLookup, inner.indexReference(label, properties:_*))

  override def indexSeek[RESULT <: AnyRef](index: IndexReadSession,
                                           needsValues: Boolean,
                                           indexOrder: IndexOrder,
                                           values: Seq[IndexQuery]): NodeValueIndexCursor =
    translateException(tokenNameLookup, inner.indexSeek(index, needsValues, indexOrder, values))

  override def getNodesByLabel(id: Int, indexOrder: IndexOrder): ClosingIterator[NodeValue] =
    translateException(tokenNameLookup, inner.getNodesByLabel(id, indexOrder))

  override def getNodesByLabelPrimitive(id: Int, indexOrder: IndexOrder): ClosingLongIterator =
    translateException(tokenNameLookup, inner.getNodesByLabelPrimitive(id, indexOrder))


  override def nodeAsMap(id: Long, nodeCursor: NodeCursor, propertyCursor: PropertyCursor): MapValue =
    translateException(tokenNameLookup, inner.nodeAsMap(id, nodeCursor, propertyCursor))

  override def relationshipAsMap(id: Long, relationshipCursor: RelationshipScanCursor, propertyCursor: PropertyCursor): MapValue =
    translateException(tokenNameLookup, inner.relationshipAsMap(id, relationshipCursor, propertyCursor))

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

  override def singleRelationship(id: Long, cursor: RelationshipScanCursor): Unit =
    translateException(tokenNameLookup, inner.singleRelationship(id,cursor))

  override def nodeGetTotalDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int =
    translateException(tokenNameLookup, inner.nodeGetTotalDegree(node, relationship, nodeCursor))

  override def createNodeKeyConstraint(labelId: Int, propertyKeyIds: Seq[Int], name: Option[String]): Unit =
    translateException(tokenNameLookup, inner.createNodeKeyConstraint(labelId, propertyKeyIds, name))

  override def dropNodeKeyConstraint(labelId: Int, propertyKeyIds: Seq[Int]): Unit =
    translateException(tokenNameLookup, inner.dropNodeKeyConstraint(labelId, propertyKeyIds))

  override def createUniqueConstraint(labelId: Int, propertyKeyIds: Seq[Int], name: Option[String]): Unit =
    translateException(tokenNameLookup, inner.createUniqueConstraint(labelId, propertyKeyIds, name))

  override def dropUniqueConstraint(labelId: Int, propertyKeyIds: Seq[Int]): Unit =
    translateException(tokenNameLookup, inner.dropUniqueConstraint(labelId, propertyKeyIds))

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int, name: Option[String]): Unit =
    translateException(tokenNameLookup, inner.createNodePropertyExistenceConstraint(labelId, propertyKeyId, name))

  override def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): Unit =
    translateException(tokenNameLookup, inner.dropNodePropertyExistenceConstraint(labelId, propertyKeyId))

  override def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int, name: Option[String]): Unit =
    translateException(tokenNameLookup, inner.createRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId, name))

  override def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): Unit =
    translateException(tokenNameLookup, inner.dropRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId))

  override def dropNamedConstraint(name: String): Unit =
    translateException(tokenNameLookup, inner.dropNamedConstraint(name))

  override def callReadOnlyProcedure(id: Int, args: Array[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    translateIterator(tokenNameLookup, inner.callReadOnlyProcedure(id, args, allowed, context))

  override def callReadWriteProcedure(id: Int, args: Array[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    translateIterator(tokenNameLookup, inner.callReadWriteProcedure(id, args, allowed, context))

  override def callSchemaWriteProcedure(id: Int, args: Array[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    translateIterator(tokenNameLookup, inner.callSchemaWriteProcedure(id, args, allowed, context))

  override def callDbmsProcedure(id: Int, args: Array[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] =
    translateIterator(tokenNameLookup, inner.callDbmsProcedure(id, args, allowed, context))

  override def callFunction(id: Int, args: Array[AnyValue], allowed: Array[String]): AnyValue =
    translateException(tokenNameLookup, inner.callFunction(id, args, allowed))

  override def aggregateFunction(id: Int,
                                 allowed: Array[String]): UserDefinedAggregator =
    translateException(tokenNameLookup, inner.aggregateFunction(id, allowed))

  override def isLabelSetOnNode(label: Int, node: Long, nodeCursor: NodeCursor): Boolean =
    translateException(tokenNameLookup, inner.isLabelSetOnNode(label, node, nodeCursor))

  override def getRelTypeId(relType: String): Int =
    translateException(tokenNameLookup, inner.getRelTypeId(relType))

  override def getRelTypeName(id: Int): String =
    translateException(tokenNameLookup, inner.getRelTypeName(id))

  override def lockingUniqueIndexSeek[RESULT](index: IndexDescriptor,
                                              values: Seq[IndexQuery.ExactPredicate]): NodeValueIndexCursor =
    translateException(tokenNameLookup, inner.lockingUniqueIndexSeek(index, values))

  override def getImportURL(url: URL): Either[String, URL] =
    translateException(tokenNameLookup, inner.getImportURL(url))

  override def createRelationship(start: Long, end: Long, relType: Int): RelationshipValue =
    translateException(tokenNameLookup, inner.createRelationship(start, end, relType))

  override def getOrCreateRelTypeId(relTypeName: String): Int =
    translateException(tokenNameLookup, inner.getOrCreateRelTypeId(relTypeName))

  override def getRelationshipsForIds(node: Long, dir: SemanticDirection, types: Array[Int]): ClosingIterator[RelationshipValue] =
    translateException(tokenNameLookup, inner.getRelationshipsForIds(node, dir, types))

  override def getRelationshipsForIdsPrimitive(node: Long, dir: SemanticDirection, types: Array[Int]): ClosingLongIterator with RelationshipIterator =
    translateException(tokenNameLookup, inner.getRelationshipsForIdsPrimitive(node, dir, types))

  override def nodeCursor(): NodeCursor = translateException(tokenNameLookup, inner.nodeCursor())

  override def traversalCursor(): RelationshipTraversalCursor = translateException(tokenNameLookup, inner.traversalCursor())

  override def relationshipById(relationshipId: Long, startNodeId: Long, endNodeId: Long, typeId: Int): RelationshipValue =
    translateException(tokenNameLookup, inner.relationshipById(relationshipId, startNodeId, endNodeId, typeId))

  override def indexSeekByContains[RESULT <: AnyRef](index: IndexReadSession,
                                                     needsValues: Boolean,
                                                     indexOrder: IndexOrder,
                                                     value: TextValue): NodeValueIndexCursor =
    translateException(tokenNameLookup, inner.indexSeekByContains(index, needsValues, indexOrder, value))

  override def indexSeekByEndsWith[RESULT <: AnyRef](index: IndexReadSession,
                                                     needsValues: Boolean,
                                                     indexOrder: IndexOrder,
                                                     value: TextValue): NodeValueIndexCursor =
    translateException(tokenNameLookup, inner.indexSeekByEndsWith(index, needsValues, indexOrder, value))

  override def indexScan[RESULT <: AnyRef](index: IndexReadSession,
                                           needsValues: Boolean,
                                           indexOrder: IndexOrder): NodeValueIndexCursor =
    translateException(tokenNameLookup, inner.indexScan(index, needsValues, indexOrder))

  override def nodeHasCheapDegrees(node: Long, nodeCursor: NodeCursor): Boolean =
    translateException(tokenNameLookup, inner.nodeHasCheapDegrees(node, nodeCursor))

  override def asObject(value: AnyValue): AnyRef =
    translateException(tokenNameLookup, inner.asObject(value))

  override def getTxStateNodePropertyOrNull(nodeId: Long,
                                            propertyKey: Int): Value =
    translateException(tokenNameLookup, inner.getTxStateNodePropertyOrNull(nodeId, propertyKey))

  override def getTxStateRelationshipPropertyOrNull(relId: Long, propertyKey: Int): Value =
    translateException(tokenNameLookup, inner.getTxStateRelationshipPropertyOrNull(relId, propertyKey))

  override def singleShortestPath(left: Long, right: Long, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path], filters: Seq[KernelPredicate[Entity]], memoryTracker: MemoryTracker): Option[Path] =
    translateException(tokenNameLookup, inner.singleShortestPath(left, right, depth, expander, pathPredicate, filters, memoryTracker))

  override def allShortestPath(left: Long, right: Long, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path], filters: Seq[KernelPredicate[Entity]], memoryTracker: MemoryTracker): ClosingIterator[Path] =
    translateException(tokenNameLookup, inner.allShortestPath(left, right, depth, expander, pathPredicate, filters, memoryTracker))

  override def nodeCountByCountStore(labelId: Int): Long =
    translateException(tokenNameLookup, inner.nodeCountByCountStore(labelId))

  override def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long =
    translateException(tokenNameLookup, inner.relationshipCountByCountStore(startLabelId, typeId, endLabelId))

  override def lockNodes(nodeIds: Long*): Unit =
    translateException(tokenNameLookup, inner.lockNodes(nodeIds:_*))

  override def lockRelationships(relIds: Long*): Unit =
    translateException(tokenNameLookup, inner.lockRelationships(relIds:_*))

  override def getOptRelTypeId(relType: String): Option[Int] =
    translateException(tokenNameLookup, inner.getOptRelTypeId(relType))

  override def detachDeleteNode(node: Long): Int =
    translateException(tokenNameLookup, inner.detachDeleteNode(node))

  override def assertSchemaWritesAllowed(): Unit = translateException(tokenNameLookup, inner.assertSchemaWritesAllowed())

  class ExceptionTranslatingOperations[T, CURSOR](inner: Operations[T, CURSOR])
    extends Operations[T, CURSOR] {
    override def delete(id: Long): Unit =
      translateException(tokenNameLookup, inner.delete(id))

    override def setProperty(id: Long, propertyKey: Int, value: Value): Unit =
      translateException(tokenNameLookup, inner.setProperty(id, propertyKey, value))

    override def getById(id: Long): T =
      translateException(tokenNameLookup, inner.getById(id))

    override def getProperty(id: Long, propertyKeyId: Int, cursor: CURSOR, propertyCursor: PropertyCursor, throwOnDeleted: Boolean): Value =
      translateException(tokenNameLookup, inner.getProperty(id, propertyKeyId, cursor, propertyCursor, throwOnDeleted))

    override def hasProperty(id: Long, propertyKeyId: Int, cursor: CURSOR, propertyCursor: PropertyCursor): Boolean =
      translateException(tokenNameLookup, inner.hasProperty(id, propertyKeyId, cursor, propertyCursor))

    override def propertyKeyIds(id: Long, cursor: CURSOR, propertyCursor: PropertyCursor): Array[Int] =
      translateException(tokenNameLookup, inner.propertyKeyIds(id, cursor, propertyCursor))

    override def removeProperty(id: Long, propertyKeyId: Int): Boolean =
      translateException(tokenNameLookup, inner.removeProperty(id, propertyKeyId))

    override def all: ClosingIterator[T] =
      translateException(tokenNameLookup, inner.all)

    override def allPrimitive: ClosingLongIterator =
      translateException(tokenNameLookup, inner.allPrimitive)

    override def isDeletedInThisTx(id: Long): Boolean =
      translateException(tokenNameLookup, inner.isDeletedInThisTx(id))

    override def getByIdIfExists(id: Long): Option[T] =
      translateException(tokenNameLookup, inner.getByIdIfExists(id))

    override def getTxStateProperty(obj: Long, propertyKeyId: Int): Value =
      translateException(tokenNameLookup, inner.getTxStateProperty(obj, propertyKeyId))

    override def hasTxStatePropertyForCachedProperty(entityId: Long, propertyKeyId: Int): Option[Boolean] =
      translateException(tokenNameLookup, inner.hasTxStatePropertyForCachedProperty(entityId, propertyKeyId))

    override def acquireExclusiveLock(obj: Long): Unit =
      translateException(tokenNameLookup, inner.acquireExclusiveLock(obj))

    override def releaseExclusiveLock(obj: Long): Unit =
      translateException(tokenNameLookup, inner.releaseExclusiveLock(obj))
  }

  class ExceptionTranslatingTransactionalContext(inner: QueryTransactionalContext) extends DelegatingQueryTransactionalContext(inner) {
    override def close() { translateException(tokenNameLookup, super.close()) }

    override def rollback() { translateException(tokenNameLookup, super.rollback()) }
  }

}
