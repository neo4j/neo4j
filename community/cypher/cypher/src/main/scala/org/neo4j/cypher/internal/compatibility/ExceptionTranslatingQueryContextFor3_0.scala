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
package org.neo4j.cypher.internal.compatibility

import java.net.URL

import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.{Expander, KernelPredicate}
import org.neo4j.cypher.internal.compiler.v3_0.pipes.matching.PatternNode
import org.neo4j.cypher.internal.compiler.v3_0.spi._
import org.neo4j.cypher.internal.frontend.v3_0.SemanticDirection
import org.neo4j.cypher.internal.spi.v3_0.ExceptionTranslationSupport
import org.neo4j.graphdb.{Node, Path, PropertyContainer, Relationship}
import org.neo4j.kernel.api.index.IndexDescriptor

import scala.collection.Iterator

class ExceptionTranslatingQueryContextFor3_0(val inner: QueryContext) extends QueryContext with ExceptionTranslationSupport {
  override type EntityAccessor = inner.EntityAccessor

  override def entityAccessor = inner.entityAccessor

  override def transactionalContext =
    new ExceptionTranslatingTransactionalContext(inner.transactionalContext)

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int =
    translateException(inner.setLabelsOnNode(node, labelIds))

  override def createNode(): Node =
    translateException(inner.createNode())

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

  override def detachDeleteNode(node: Node): Int =
    translateException(inner.detachDeleteNode(node))

  override def getPropertyKeyName(propertyKeyId: Int): String =
    translateException(inner.getPropertyKeyName(propertyKeyId))

  override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] =
    translateException(inner.getOptPropertyKeyId(propertyKeyName))

  override def getPropertyKeyId(propertyKey: String): Int =
    translateException(inner.getPropertyKeyId(propertyKey))

  override def getOrCreatePropertyKeyId(propertyKey: String): Int =
    translateException(inner.getOrCreatePropertyKeyId(propertyKey))

  override def addIndexRule(labelId: Int, propertyKeyId: Int) =
    translateException(inner.addIndexRule(labelId, propertyKeyId))

  override def dropIndexRule(labelId: Int, propertyKeyId: Int) =
    translateException(inner.dropIndexRule(labelId, propertyKeyId))

  override def indexSeek(index: IndexDescriptor, value: Any): Iterator[Node] =
    translateException(inner.indexSeek(index, value))

  override def getNodesByLabel(id: Int): Iterator[Node] =
    translateException(inner.getNodesByLabel(id))

  override def nodeGetDegree(node: Long, dir: SemanticDirection): Int =
    translateException(inner.nodeGetDegree(node, dir))

  override def nodeGetDegree(node: Long, dir: SemanticDirection, relTypeId: Int): Int =
    translateException(inner.nodeGetDegree(node, dir, relTypeId))

  override def getOrCreateFromSchemaState[K, V](key: K, creator: => V): V =
    translateException(inner.getOrCreateFromSchemaState(key, creator))

  override def createUniqueConstraint(labelId: Int, propertyKeyId: Int) =
    translateException(inner.createUniqueConstraint(labelId, propertyKeyId))

  override def dropUniqueConstraint(labelId: Int, propertyKeyId: Int) =
    translateException(inner.dropUniqueConstraint(labelId, propertyKeyId))

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int) =
    translateException(inner.createNodePropertyExistenceConstraint(labelId, propertyKeyId))

  override def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int) =
    translateException(inner.dropNodePropertyExistenceConstraint(labelId, propertyKeyId))

  override def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) =
    translateException(inner.createRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId))

  override def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) =
    translateException(inner.dropRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId))

  override def callReadOnlyProcedure(name: QualifiedProcedureName, args: Seq[Any]): Iterator[Array[AnyRef]] =
    translateIterator(inner.callReadOnlyProcedure(name, args))

  override def callReadWriteProcedure(name: QualifiedProcedureName, args: Seq[Any]): Iterator[Array[AnyRef]] =
    translateIterator(inner.callReadWriteProcedure(name, args))

  override def callDbmsProcedure(name: QualifiedProcedureName, args: Seq[Any]): Iterator[Array[AnyRef]] =
    translateIterator(inner.callDbmsProcedure(name, args))

  override def isGraphKernelResultValue(v: Any): Boolean =
    translateException(inner.isGraphKernelResultValue(v))

  override def withAnyOpenQueryContext[T](work: (QueryContext) => T): T =
    inner.withAnyOpenQueryContext(qc =>
      translateException(
        work(new ExceptionTranslatingQueryContextFor3_0(qc))
      ))

  override def isLabelSetOnNode(label: Int, node: Long): Boolean =
    translateException(inner.isLabelSetOnNode(label, node))

  override def getRelTypeId(relType: String) =
    translateException(inner.getRelTypeId(relType))

  override def getRelTypeName(id: Int) =
    translateException(inner.getRelTypeName(id))

  override def lockingUniqueIndexSeek(index: IndexDescriptor, value: Any) =
    translateException(inner.lockingUniqueIndexSeek(index, value))

  override def getImportURL(url: URL) =
    translateException(inner.getImportURL(url))

  override def relationshipStartNode(rel: Relationship) =
    translateException(inner.relationshipStartNode(rel))

  override def relationshipEndNode(rel: Relationship) =
    translateException(inner.relationshipEndNode(rel))

  override def createRelationship(start: Long, end: Long, relType: Int) =
    translateException(inner.createRelationship(start, end, relType))

  override def getOrCreateRelTypeId(relTypeName: String) =
    translateException(inner.getOrCreateRelTypeId(relTypeName))

  override def getRelationshipsForIds(node: Node, dir: SemanticDirection, types: Option[Seq[Int]]) =
    translateException(inner.getRelationshipsForIds(node, dir, types))

  override def indexSeekByRange(index: IndexDescriptor, value: Any) =
    translateException(inner.indexSeekByRange(index, value))

  override def indexScanByContains(index: IndexDescriptor, value: String) =
    translateException(inner.indexScanByContains(index, value))

  override def indexScanByEndsWith(index: IndexDescriptor, value: String) =
    translateException(inner.indexScanByEndsWith(index, value))

  override def indexScan(index: IndexDescriptor) =
    translateException(inner.indexScan(index))

  override def nodeIsDense(node: Long) =
    translateException(inner.nodeIsDense(node))

  override def variableLengthPathExpand(node: PatternNode, realNode: Node, minHops: Option[Int], maxHops: Option[Int], direction: SemanticDirection, relTypes: Seq[String]) =
    translateException(inner.variableLengthPathExpand(node, realNode, minHops, maxHops, direction, relTypes))

  override def singleShortestPath(left: Node, right: Node, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path], filters: Seq[KernelPredicate[PropertyContainer]]) =
    translateException(inner.singleShortestPath(left, right, depth, expander, pathPredicate, filters))

  override def allShortestPath(left: Node, right: Node, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path], filters: Seq[KernelPredicate[PropertyContainer]]) =
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

  class ExceptionTranslatingOperations[T <: PropertyContainer](inner: Operations[T])
    extends DelegatingOperations[T](inner) {
    override def delete(obj: T) =
      translateException(inner.delete(obj))

    override def setProperty(id: Long, propertyKey: Int, value: Any) =
      translateException(inner.setProperty(id, propertyKey, value))

    override def getById(id: Long): T =
      translateException(inner.getById(id))

    override def getProperty(id: Long, propertyKeyId: Int): Any =
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

    override def isDeletedInThisTx(obj: T): Boolean =
      translateException(inner.isDeletedInThisTx(obj))
  }

  class ExceptionTranslatingTransactionalContext(inner: QueryTransactionalContext) extends DelegatingQueryTransactionalContext(inner) {
    override def close(success: Boolean) { translateException(super.close(success)) }
  }
}

