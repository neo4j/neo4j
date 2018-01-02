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
package org.neo4j.cypher.internal.compiler.v2_3.spi

import java.net.URL

import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Expander, KernelPredicate}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.matching.PatternNode
import org.neo4j.cypher.internal.compiler.v2_3.spi.SchemaTypes.IndexDescriptor
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.graphdb.{Node, Path, PropertyContainer, Relationship}

class DelegatingQueryContext(inner: QueryContext) extends QueryContext {

  protected def singleDbHit[A](value: A): A = value
  protected def manyDbHits[A](value: Iterator[A]): Iterator[A] = value
  protected def manyDbHits(count: Int): Int = count

  def isOpen: Boolean = inner.isOpen

  def isTopLevelTx: Boolean = inner.isTopLevelTx

  def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int =
    singleDbHit(inner.setLabelsOnNode(node, labelIds))

  def close(success: Boolean) {
    inner.close(success)
  }

  def createNode(): Node = singleDbHit(inner.createNode())

  def createRelationship(start: Node, end: Node, relType: String): Relationship = singleDbHit(inner.createRelationship(start, end, relType))

  override def createRelationship(start: Long, end: Long, relType: Int): Relationship = singleDbHit(inner.createRelationship(start, end, relType))

  def getOrCreateRelTypeId(relTypeName: String): Int = singleDbHit(inner.getOrCreateRelTypeId(relTypeName))

  def getLabelsForNode(node: Long): Iterator[Int] = singleDbHit(inner.getLabelsForNode(node))

  def getLabelName(id: Int): String = singleDbHit(inner.getLabelName(id))

  def getOptLabelId(labelName: String): Option[Int] = singleDbHit(inner.getOptLabelId(labelName))

  def getLabelId(labelName: String): Int = singleDbHit(inner.getLabelId(labelName))

  def getOrCreateLabelId(labelName: String): Int = singleDbHit(inner.getOrCreateLabelId(labelName))

  def getRelationshipsForIds(node: Node, dir: SemanticDirection, types: Option[Seq[Int]]): Iterator[Relationship] = manyDbHits(inner.getRelationshipsForIds(node, dir, types))

  def nodeOps = inner.nodeOps

  def relationshipOps = inner.relationshipOps

  def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int =
    singleDbHit(inner.removeLabelsFromNode(node, labelIds))

  def getPropertiesForNode(node: Long): Iterator[Int] = singleDbHit(inner.getPropertiesForNode(node))

  def getPropertiesForRelationship(relId: Long): Iterator[Int] = singleDbHit(inner.getPropertiesForRelationship(relId))

  def detachDeleteNode(obj: Node): Int = manyDbHits(inner.detachDeleteNode(obj))

  def getPropertyKeyName(propertyKeyId: Int): String = singleDbHit(inner.getPropertyKeyName(propertyKeyId))

  def getOptPropertyKeyId(propertyKeyName: String): Option[Int] = singleDbHit(inner.getOptPropertyKeyId(propertyKeyName))

  def getPropertyKeyId(propertyKey: String) = singleDbHit(inner.getPropertyKeyId(propertyKey))

  def getOrCreatePropertyKeyId(propertyKey: String) = singleDbHit(inner.getOrCreatePropertyKeyId(propertyKey))

  def addIndexRule(labelId: Int, propertyKeyId: Int) = singleDbHit(inner.addIndexRule(labelId, propertyKeyId))

  def dropIndexRule(labelId: Int, propertyKeyId: Int) = singleDbHit(inner.dropIndexRule(labelId, propertyKeyId))

  def indexSeek(index: IndexDescriptor, value: Any): Iterator[Node] = manyDbHits(inner.indexSeek(index, value))

  def indexSeekByRange(index: IndexDescriptor, value: Any): Iterator[Node] = manyDbHits(inner.indexSeekByRange(index, value))

  def indexScan(index: IndexDescriptor): Iterator[Node] = manyDbHits(inner.indexScan(index))

  def getNodesByLabel(id: Int): Iterator[Node] = manyDbHits(inner.getNodesByLabel(id))

  def upgrade(context: QueryContext): LockingQueryContext = inner.upgrade(context)

  def getOrCreateFromSchemaState[K, V](key: K, creator: => V): V = singleDbHit(inner.getOrCreateFromSchemaState(key, creator))

  def createUniqueConstraint(labelId: Int, propertyKeyId: Int) = singleDbHit(inner.createUniqueConstraint(labelId, propertyKeyId))

  def dropUniqueConstraint(labelId: Int, propertyKeyId: Int) = singleDbHit(inner.dropUniqueConstraint(labelId, propertyKeyId))

  def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int) = singleDbHit(inner.createNodePropertyExistenceConstraint(labelId, propertyKeyId))

  def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int) = singleDbHit(inner.dropNodePropertyExistenceConstraint(labelId, propertyKeyId))

  def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) = singleDbHit(inner.createRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId))

  def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) = singleDbHit(inner.dropRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId))

  def withAnyOpenQueryContext[T](work: (QueryContext) => T): T = inner.withAnyOpenQueryContext(work)

  def lockingExactUniqueIndexSearch(index: IndexDescriptor, value: Any): Option[Node] = singleDbHit(inner.lockingExactUniqueIndexSearch(index, value))

  override def commitAndRestartTx() {
    inner.commitAndRestartTx()
  }

  def getRelTypeId(relType: String): Int = singleDbHit(inner.getRelTypeId(relType))

  def getOptRelTypeId(relType: String): Option[Int] = singleDbHit(inner.getOptRelTypeId(relType))

  def getRelTypeName(id: Int): String = singleDbHit(inner.getRelTypeName(id))

  def getImportURL(url: URL): Either[String,URL] = inner.getImportURL(url)

  def relationshipStartNode(rel: Relationship) = inner.relationshipStartNode(rel)

  def relationshipEndNode(rel: Relationship) = inner.relationshipEndNode(rel)

  def nodeGetDegree(node: Long, dir: SemanticDirection): Int = singleDbHit(inner.nodeGetDegree(node, dir))

  def nodeGetDegree(node: Long, dir: SemanticDirection, relTypeId: Int): Int = singleDbHit(inner.nodeGetDegree(node, dir, relTypeId))

  def nodeIsDense(node: Long): Boolean = singleDbHit(inner.nodeIsDense(node))

  override def variableLengthPathExpand(node: PatternNode,
                                        realNode: Node,
                                        minHops: Option[Int],
                                        maxHops: Option[Int],
                                        direction: SemanticDirection,
                                        relTypes: Seq[String]): Iterator[Path] =
    manyDbHits(inner.variableLengthPathExpand(node, realNode, minHops, maxHops, direction, relTypes))

  override def isLabelSetOnNode(label: Int, node: Long): Boolean = getLabelsForNode(node).contains(label)

  override def singleShortestPath(left: Node, right: Node, depth: Int, expander: Expander,
                                  pathPredicate: KernelPredicate[Path],
                                  filters: Seq[KernelPredicate[PropertyContainer]]): Option[Path] =
    singleDbHit(inner.singleShortestPath(left, right, depth, expander, pathPredicate, filters))

  override def allShortestPath(left: Node, right: Node, depth: Int, expander: Expander,
                               pathPredicate: KernelPredicate[Path],
                               filters: Seq[KernelPredicate[PropertyContainer]]): Iterator[Path] =
    manyDbHits(inner.allShortestPath(left, right, depth, expander, pathPredicate, filters))
}

class DelegatingOperations[T <: PropertyContainer](protected val inner: Operations[T]) extends Operations[T] {

  protected def singleDbHit[A](value: A): A = value
  protected def manyDbHits[A](value: Iterator[A]): Iterator[A] = value

  def delete(obj: T): Unit = singleDbHit(inner.delete(obj))

  def setProperty(obj: Long, propertyKey: Int, value: Any): Unit = singleDbHit(inner.setProperty(obj, propertyKey, value))

  def getById(id: Long): T = singleDbHit(inner.getById(id))

  def getProperty(obj: Long, propertyKeyId: Int): Any = singleDbHit(inner.getProperty(obj, propertyKeyId))

  def hasProperty(obj: Long, propertyKeyId: Int): Boolean = singleDbHit(inner.hasProperty(obj, propertyKeyId))

  def propertyKeyIds(obj: Long): Iterator[Int] = singleDbHit(inner.propertyKeyIds(obj))

  def removeProperty(obj: Long, propertyKeyId: Int): Unit = singleDbHit(inner.removeProperty(obj, propertyKeyId))

  def indexGet(name: String, key: String, value: Any): Iterator[T] = manyDbHits(inner.indexGet(name, key, value))

  def indexQuery(name: String, query: Any): Iterator[T] = manyDbHits(inner.indexQuery(name, query))

  def all: Iterator[T] = manyDbHits(inner.all)

  def isDeleted(obj: T): Boolean = inner.isDeleted(obj)
}
