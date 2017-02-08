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
package org.neo4j.cypher.internal.compiler.v3_1.spi

import java.net.URL

import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions.{Expander, KernelPredicate}
import org.neo4j.cypher.internal.compiler.v3_1.pipes.matching.PatternNode
import org.neo4j.cypher.internal.compiler.v3_1.spi.SchemaTypes.IndexDescriptor
import org.neo4j.cypher.internal.frontend.v3_1.SemanticDirection
import org.neo4j.graphdb.{Node, Path, PropertyContainer, Relationship}

import scala.collection.Iterator

class DelegatingQueryContext(val inner: QueryContext) extends QueryContext {

  protected def singleDbHit[A](value: A): A = value
  protected def manyDbHits[A](value: Iterator[A]): Iterator[A] = value
  protected def manyDbHits(count: Int): Int = count

  type EntityAccessor = inner.EntityAccessor

  override def transactionalContext: QueryTransactionalContext = inner.transactionalContext

  override def entityAccessor: EntityAccessor = inner.entityAccessor

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int =
    singleDbHit(inner.setLabelsOnNode(node, labelIds))

  override def createNode(): Node = singleDbHit(inner.createNode())

  override def createRelationship(start: Node, end: Node, relType: String): Relationship = singleDbHit(inner
    .createRelationship(start, end, relType))

  override def createRelationship(start: Long, end: Long, relType: Int): Relationship =
    singleDbHit(inner.createRelationship(start, end, relType))

  override def getOrCreateRelTypeId(relTypeName: String): Int = singleDbHit(inner.getOrCreateRelTypeId(relTypeName))

  override def getLabelsForNode(node: Long): Iterator[Int] = singleDbHit(inner.getLabelsForNode(node))

  override def getLabelName(id: Int): String = singleDbHit(inner.getLabelName(id))

  override def getOptLabelId(labelName: String): Option[Int] = singleDbHit(inner.getOptLabelId(labelName))

  override def getLabelId(labelName: String): Int = singleDbHit(inner.getLabelId(labelName))

  override def getOrCreateLabelId(labelName: String): Int = singleDbHit(inner.getOrCreateLabelId(labelName))

  override def getRelationshipsForIds(node: Node, dir: SemanticDirection, types: Option[Seq[Int]]): Iterator[Relationship] =
  manyDbHits(inner.getRelationshipsForIds(node, dir, types))

  override def nodeOps = inner.nodeOps

  override def relationshipOps = inner.relationshipOps

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int =
    singleDbHit(inner.removeLabelsFromNode(node, labelIds))

  override def getPropertiesForNode(node: Long): Iterator[Int] = singleDbHit(inner.getPropertiesForNode(node))

  override def getPropertiesForRelationship(relId: Long): Iterator[Int] =
    singleDbHit(inner.getPropertiesForRelationship(relId))

  override def getPropertyKeyName(propertyKeyId: Int): String = singleDbHit(inner.getPropertyKeyName(propertyKeyId))

  override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] =
    singleDbHit(inner.getOptPropertyKeyId(propertyKeyName))

  override def getPropertyKeyId(propertyKey: String) = singleDbHit(inner.getPropertyKeyId(propertyKey))

  override def getOrCreatePropertyKeyId(propertyKey: String) = singleDbHit(inner.getOrCreatePropertyKeyId(propertyKey))

  override def addIndexRule(labelId: Int, propertyKeyId: Int) = singleDbHit(inner.addIndexRule(labelId, propertyKeyId))

  override def dropIndexRule(labelId: Int, propertyKeyId: Int) = singleDbHit(inner.dropIndexRule(labelId, propertyKeyId))

  override def indexSeek(index: IndexDescriptor, value: Any): Iterator[Node] = manyDbHits(inner.indexSeek(index, value))

  override def indexSeekByRange(index: IndexDescriptor, value: Any): Iterator[Node] =
    manyDbHits(inner.indexSeekByRange(index, value))

  override def indexScan(index: IndexDescriptor): Iterator[Node] = manyDbHits(inner.indexScan(index))

  override def indexScanByContains(index: IndexDescriptor, value: String): scala.Iterator[Node] =
    manyDbHits(inner.indexScanByContains(index, value))

  override def indexScanByEndsWith(index: IndexDescriptor, value: String): scala.Iterator[Node] =
    manyDbHits(inner.indexScanByEndsWith(index, value))

  override def getNodesByLabel(id: Int): Iterator[Node] = manyDbHits(inner.getNodesByLabel(id))

  override def getOrCreateFromSchemaState[K, V](key: K, creator: => V): V =
    singleDbHit(inner.getOrCreateFromSchemaState(key, creator))

  override def createUniqueConstraint(labelId: Int, propertyKeyId: Int) =
    singleDbHit(inner.createUniqueConstraint(labelId, propertyKeyId))

  override def dropUniqueConstraint(labelId: Int, propertyKeyId: Int) =
    singleDbHit(inner.dropUniqueConstraint(labelId, propertyKeyId))

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int) =
    singleDbHit(inner.createNodePropertyExistenceConstraint(labelId, propertyKeyId))

  override def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int) =
    singleDbHit(inner.dropNodePropertyExistenceConstraint(labelId, propertyKeyId))

  override def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) =
    singleDbHit(inner.createRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId))

  override def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) =
    singleDbHit(inner.dropRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId))

  override def withAnyOpenQueryContext[T](work: (QueryContext) => T): T = inner.withAnyOpenQueryContext(work)

  override def lockingUniqueIndexSeek(index: IndexDescriptor, value: Any): Option[Node] =
    singleDbHit(inner.lockingUniqueIndexSeek(index, value))

  override def getRelTypeId(relType: String): Int = singleDbHit(inner.getRelTypeId(relType))

  override def getOptRelTypeId(relType: String): Option[Int] = singleDbHit(inner.getOptRelTypeId(relType))

  override def getRelTypeName(id: Int): String = singleDbHit(inner.getRelTypeName(id))

  override def getImportURL(url: URL): Either[String,URL] = inner.getImportURL(url)

  override def relationshipStartNode(rel: Relationship) = inner.relationshipStartNode(rel)

  override def relationshipEndNode(rel: Relationship) = inner.relationshipEndNode(rel)

  override def nodeGetDegree(node: Long, dir: SemanticDirection): Int = singleDbHit(inner.nodeGetDegree(node, dir))

  override def nodeGetDegree(node: Long, dir: SemanticDirection, relTypeId: Int): Int =
    singleDbHit(inner.nodeGetDegree(node, dir, relTypeId))

  override def nodeIsDense(node: Long): Boolean = singleDbHit(inner.nodeIsDense(node))

  override def variableLengthPathExpand(node: PatternNode,
                                        realNode: Node,
                                        minHops: Option[Int],
                                        maxHops: Option[Int],
                                        direction: SemanticDirection,
                                        relTypes: Seq[String]): Iterator[Path] =
    manyDbHits(inner.variableLengthPathExpand(node, realNode, minHops, maxHops, direction, relTypes))

  override def isLabelSetOnNode(label: Int, node: Long): Boolean = getLabelsForNode(node).contains(label)

  override def nodeCountByCountStore(labelId: Int): Long = inner.nodeCountByCountStore(labelId)

  override def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long =
    inner.relationshipCountByCountStore(startLabelId, typeId, endLabelId)

  override def lockNodes(nodeIds: Long*): Unit = inner.lockNodes(nodeIds:_*)

  override def lockRelationships(relIds: Long*): Unit = inner.lockRelationships(relIds:_*)

  override def singleShortestPath(left: Node, right: Node, depth: Int, expander: Expander,
                                  pathPredicate: KernelPredicate[Path],
                                  filters: Seq[KernelPredicate[PropertyContainer]]): Option[Path] =
    singleDbHit(inner.singleShortestPath(left, right, depth, expander, pathPredicate, filters))

  override def allShortestPath(left: Node, right: Node, depth: Int, expander: Expander,
                               pathPredicate: KernelPredicate[Path],
                               filters: Seq[KernelPredicate[PropertyContainer]]): Iterator[Path] =
    manyDbHits(inner.allShortestPath(left, right, depth, expander, pathPredicate, filters))

  override def callReadOnlyProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) =
    singleDbHit(inner.callReadOnlyProcedure(name, args, allowed))

  override def callReadWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) =
    singleDbHit(inner.callReadWriteProcedure(name, args, allowed))

  override def callTokenWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) =
    singleDbHit(inner.callTokenWriteProcedure(name, args, allowed))

  override def callSchemaWriteProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) =
    singleDbHit(inner.callSchemaWriteProcedure(name, args, allowed))

  override def callDbmsProcedure(name: QualifiedName, args: Seq[Any], allowed: Array[String]) =
    inner.callDbmsProcedure(name, args, allowed)

  override def callFunction(name: QualifiedName, args: Seq[Any], allowed: Array[String]) =
    singleDbHit(inner.callFunction(name, args, allowed))

  override def isGraphKernelResultValue(v: Any): Boolean =
    inner.isGraphKernelResultValue(v)

  override def detachDeleteNode(node: Node): Int = manyDbHits(inner.detachDeleteNode(node))

  override def assertSchemaWritesAllowed(): Unit = inner.assertSchemaWritesAllowed()
}

class DelegatingOperations[T <: PropertyContainer](protected val inner: Operations[T]) extends Operations[T] {

  protected def singleDbHit[A](value: A): A = value
  protected def manyDbHits[A](value: Iterator[A]): Iterator[A] = value

  override def delete(obj: T): Unit = singleDbHit(inner.delete(obj))

  override def setProperty(obj: Long, propertyKey: Int, value: Any): Unit =
    singleDbHit(inner.setProperty(obj, propertyKey, value))

  override def getById(id: Long): T = singleDbHit(inner.getById(id))

  override def getProperty(obj: Long, propertyKeyId: Int): Any = singleDbHit(inner.getProperty(obj, propertyKeyId))

  override def hasProperty(obj: Long, propertyKeyId: Int): Boolean = singleDbHit(inner.hasProperty(obj, propertyKeyId))

  override def propertyKeyIds(obj: Long): Iterator[Int] = singleDbHit(inner.propertyKeyIds(obj))

  override def removeProperty(obj: Long, propertyKeyId: Int): Unit = singleDbHit(inner.removeProperty(obj, propertyKeyId))

  override def indexGet(name: String, key: String, value: Any): Iterator[T] = manyDbHits(inner.indexGet(name, key, value))

  override def indexQuery(name: String, query: Any): Iterator[T] = manyDbHits(inner.indexQuery(name, query))

  override def all: Iterator[T] = manyDbHits(inner.all)

  override def isDeletedInThisTx(obj: T): Boolean = inner.isDeletedInThisTx(obj)

  override def acquireExclusiveLock(obj: Long): Unit = inner.acquireExclusiveLock(obj)

  override def releaseExclusiveLock(obj: Long): Unit = inner.releaseExclusiveLock(obj)
}

class DelegatingQueryTransactionalContext(val inner: QueryTransactionalContext) extends QueryTransactionalContext {

  override type ReadOps = inner.ReadOps

  override type DbmsOps = inner.DbmsOps

  override def readOperations: ReadOps = inner.readOperations

  override def dbmsOperations: DbmsOps = inner.dbmsOperations

  override def commitAndRestartTx() { inner.commitAndRestartTx() }

  override def isTopLevelTx: Boolean = inner.isTopLevelTx

  override def close(success: Boolean) { inner.close(success) }
}
