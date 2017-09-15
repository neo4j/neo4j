/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_4

import java.net.URL

import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions.{Expander, KernelPredicate, UserDefinedAggregator}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.matching.PatternNode
import org.neo4j.cypher.internal.compiler.v3_4.IndexDescriptor
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticDirection
import org.neo4j.cypher.internal.spi.v3_4._
import org.neo4j.cypher.internal.v3_4.logical.plans.QualifiedName
import org.neo4j.graphdb.{Node, Path, PropertyContainer, Relationship}
import org.neo4j.kernel.impl.api.store.RelationshipIterator
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.EdgeValue

import scala.collection.Iterator

class ExceptionTranslatingQueryContext(val inner: QueryContext) extends QueryContext with ExceptionTranslationSupport {
  override type EntityAccessor = inner.EntityAccessor

  override def entityAccessor: EntityAccessor = inner.entityAccessor

  override def transactionalContext =
    new ExceptionTranslatingTransactionalContext(inner.transactionalContext)

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int =
    translateException(inner.setLabelsOnNode(node, labelIds))

  override def createNode(): Node =
    translateException(inner.createNode())

  override def createNodeId(): Long =
    translateException(inner.createNodeId())

  override def createRelationship(start: Node, end: Node, relType: String): Relationship =
    translateException(inner.createRelationship(start, end, relType))

  override def getLabelsForNode(node: Long): Iterator[Int] =
    translateException(inner.getLabelsForNode(node))

  override def getLabelName(id: Int): String =
    translateException(inner.getLabelName(id))

  override def getOptLabelId(labelName: String): Option[Int] =
    translateException(inner.getOptLabelId(labelName))

  override def getLabelId(labelName: String): Int =
    translateException(inner.getLabelId(labelName))

  override def getOrCreateLabelId(labelName: String): Int =
    translateException(inner.getOrCreateLabelId(labelName))

  override def nodeOps: Operations[Node] =
    new ExceptionTranslatingOperations[Node](inner.nodeOps)

  override def relationshipOps: Operations[Relationship] =
    new ExceptionTranslatingOperations[Relationship](inner.relationshipOps)

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int =
    translateException(inner.removeLabelsFromNode(node, labelIds))

  override def getPropertiesForNode(node: Long): Iterator[Int] =
    translateException(inner.getPropertiesForNode(node))

  override def getPropertiesForRelationship(relId: Long): Iterator[Int] =
    translateException(inner.getPropertiesForRelationship(relId))

  override def getPropertyKeyName(propertyKeyId: Int): String =
    translateException(inner.getPropertyKeyName(propertyKeyId))

  override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] =
    translateException(inner.getOptPropertyKeyId(propertyKeyName))

  override def getPropertyKeyId(propertyKey: String): Int =
    translateException(inner.getPropertyKeyId(propertyKey))

  override def getOrCreatePropertyKeyId(propertyKey: String): Int =
    translateException(inner.getOrCreatePropertyKeyId(propertyKey))

  override def addIndexRule(descriptor: IndexDescriptor) =
    translateException(inner.addIndexRule(descriptor))

  override def dropIndexRule(descriptor: IndexDescriptor) =
    translateException(inner.dropIndexRule(descriptor))

  override def indexSeek(index: IndexDescriptor, values: Seq[Any]): Iterator[Node] =
    translateException(inner.indexSeek(index, values))

  override def getNodesByLabel(id: Int): Iterator[Node] =
    translateException(inner.getNodesByLabel(id))

  override def getNodesByLabelPrimitive(id: Int): PrimitiveLongIterator =
    translateException(inner.getNodesByLabelPrimitive(id))

  override def nodeGetDegree(node: Long, dir: SemanticDirection): Int =
    translateException(inner.nodeGetDegree(node, dir))

  override def nodeGetDegree(node: Long, dir: SemanticDirection, relTypeId: Int): Int =
    translateException(inner.nodeGetDegree(node, dir, relTypeId))

  override def getOrCreateFromSchemaState[K, V](key: K, creator: => V): V =
    translateException(inner.getOrCreateFromSchemaState(key, creator))

  override def createNodeKeyConstraint(descriptor: IndexDescriptor): Boolean =
    translateException(inner.createNodeKeyConstraint(descriptor))

  override def dropNodeKeyConstraint(descriptor: IndexDescriptor) =
    translateException(inner.dropNodeKeyConstraint(descriptor))

  override def createUniqueConstraint(descriptor: IndexDescriptor): Boolean =
    translateException(inner.createUniqueConstraint(descriptor))

  override def dropUniqueConstraint(descriptor: IndexDescriptor) =
    translateException(inner.dropUniqueConstraint(descriptor))

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): Boolean =
    translateException(inner.createNodePropertyExistenceConstraint(labelId, propertyKeyId))

  override def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int) =
    translateException(inner.dropNodePropertyExistenceConstraint(labelId, propertyKeyId))

  override def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): Boolean =
    translateException(inner.createRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId))

  override def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) =
    translateException(inner.dropRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId))

  override def callReadOnlyProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] =
    translateIterator(inner.callReadOnlyProcedure(name, args, allowed))

  override def callReadWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] =
    translateIterator(inner.callReadWriteProcedure(name, args, allowed))

  override def callSchemaWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] =
    translateIterator(inner.callSchemaWriteProcedure(name, args, allowed))

  override def callDbmsProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]): Iterator[Array[AnyRef]] =
    translateIterator(inner.callDbmsProcedure(name, args, allowed))

  override def callFunction(name: QualifiedName, args: Seq[Any], allowed: Array[String]) =
    translateException(inner.callFunction(name, args, allowed))


  override def aggregateFunction(name: QualifiedName,
                                 allowed: Array[String]): UserDefinedAggregator =
    translateException(inner.aggregateFunction(name, allowed))

  override def isGraphKernelResultValue(v: Any): Boolean =
    translateException(inner.isGraphKernelResultValue(v))

  override def withAnyOpenQueryContext[T](work: (QueryContext) => T): T =
    inner.withAnyOpenQueryContext(qc =>
      translateException(
        work(new ExceptionTranslatingQueryContext(qc))
      ))

  override def isLabelSetOnNode(label: Int, node: Long): Boolean =
    translateException(inner.isLabelSetOnNode(label, node))

  override def getRelTypeId(relType: String) =
    translateException(inner.getRelTypeId(relType))

  override def getRelTypeName(id: Int) =
    translateException(inner.getRelTypeName(id))

  override def lockingUniqueIndexSeek(index: IndexDescriptor, values: Seq[Any]) =
    translateException(inner.lockingUniqueIndexSeek(index, values))

  override def getImportURL(url: URL) =
    translateException(inner.getImportURL(url))

  override def edgeGetStartNode(edge: EdgeValue) =
    translateException(inner.edgeGetStartNode(edge))

  override def edgeGetEndNode(edge: EdgeValue) =
    translateException(inner.edgeGetEndNode(edge))

  override def createRelationship(start: Long, end: Long, relType: Int) =
    translateException(inner.createRelationship(start, end, relType))

  override def getOrCreateRelTypeId(relTypeName: String) =
    translateException(inner.getOrCreateRelTypeId(relTypeName))

  override def getRelationshipsForIds(node: Long, dir: SemanticDirection, types: Option[Seq[Int]]) =
    translateException(inner.getRelationshipsForIds(node, dir, types))

  override def getRelationshipsForIdsPrimitive(node: Long, dir: SemanticDirection, types: Option[Seq[Int]]): RelationshipIterator =
    translateException(inner.getRelationshipsForIdsPrimitive(node, dir, types))

  override def getRelationshipFor(relationshipId: Long, typeId: Int, startNodeId: Long, endNodeId: Long): Relationship =
    translateException(inner.getRelationshipFor(relationshipId, typeId, startNodeId, endNodeId))

  override def indexSeekByRange(index: IndexDescriptor, value: Any) =
    translateException(inner.indexSeekByRange(index, value))

  override def indexScanByContains(index: IndexDescriptor, value: String) =
    translateException(inner.indexScanByContains(index, value))

  override def indexScanByEndsWith(index: IndexDescriptor, value: String) =
    translateException(inner.indexScanByEndsWith(index, value))

  override def indexScan(index: IndexDescriptor) =
    translateException(inner.indexScan(index))

  override def indexScanPrimitive(index: IndexDescriptor) =
    translateException(inner.indexScanPrimitive(index))

  override def nodeIsDense(node: Long) =
    translateException(inner.nodeIsDense(node))

  override def asObject(value: AnyValue) =
    translateException(inner.asObject(value))

  override def variableLengthPathExpand(node: PatternNode, realNode: Long, minHops: Option[Int], maxHops: Option[Int], direction: SemanticDirection, relTypes: Seq[String]) =
    translateException(inner.variableLengthPathExpand(node, realNode, minHops, maxHops, direction, relTypes))

  override def singleShortestPath(left: Long, right: Long, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path], filters: Seq[KernelPredicate[PropertyContainer]]) =
    translateException(inner.singleShortestPath(left, right, depth, expander, pathPredicate, filters))

  override def allShortestPath(left: Long, right: Long, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path], filters: Seq[KernelPredicate[PropertyContainer]]) =
    translateException(inner.allShortestPath(left, right, depth, expander, pathPredicate, filters))

  override def nodeCountByCountStore(labelId: Int) =
    translateException(inner.nodeCountByCountStore(labelId))

  override def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int) =
  translateException(inner.relationshipCountByCountStore(startLabelId, typeId, endLabelId))

  override def lockNodes(nodeIds: Long*) =
    translateException(inner.lockNodes(nodeIds:_*))

  override def lockRelationships(relIds: Long*) =
    translateException(inner.lockRelationships(relIds:_*))

  override def getOptRelTypeId(relType: String) =
    translateException(inner.getOptRelTypeId(relType))

  override def detachDeleteNode(node: Long): Int =
    translateException(inner.detachDeleteNode(node))

  override def assertSchemaWritesAllowed(): Unit = translateException(inner.assertSchemaWritesAllowed())

  class ExceptionTranslatingOperations[T <: PropertyContainer](inner: Operations[T])
    extends DelegatingOperations[T](inner) {
    override def delete(id: Long) =
      translateException(inner.delete(id))

    override def setProperty(id: Long, propertyKey: Int, value: Value) =
      translateException(inner.setProperty(id, propertyKey, value))

    override def getById(id: Long): T =
      translateException(inner.getById(id))

    override def getProperty(id: Long, propertyKeyId: Int): Value =
      translateException(inner.getProperty(id, propertyKeyId))

    override def hasProperty(id: Long, propertyKeyId: Int): Boolean =
      translateException(inner.hasProperty(id, propertyKeyId))

    override def propertyKeyIds(id: Long): Iterator[Int] =
      translateException(inner.propertyKeyIds(id))

    override def removeProperty(id: Long, propertyKeyId: Int) =
      translateException(inner.removeProperty(id, propertyKeyId))

    override def indexGet(name: String, key: String, value: Any): Iterator[T] =
      translateException(inner.indexGet(name, key, value))

    override def indexQuery(name: String, query: Any): Iterator[T] =
      translateException(inner.indexQuery(name, query))

    override def all: Iterator[T] =
      translateException(inner.all)

    override def allPrimitive: PrimitiveLongIterator =
      translateException(inner.allPrimitive)

    override def isDeletedInThisTx(id: Long): Boolean =
      translateException(inner.isDeletedInThisTx(id))

    override def getByIdIfExists(id: Long): Option[T] =
      translateException(inner.getByIdIfExists(id))
  }

  class ExceptionTranslatingTransactionalContext(inner: QueryTransactionalContext) extends DelegatingQueryTransactionalContext(inner) {
    override def close(success: Boolean) { translateException(super.close(success)) }
  }
}

