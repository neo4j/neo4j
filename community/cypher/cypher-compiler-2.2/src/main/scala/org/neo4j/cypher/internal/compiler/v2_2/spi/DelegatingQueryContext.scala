/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.spi

import org.neo4j.graphdb.{Relationship, PropertyContainer, Direction, Node}
import org.neo4j.kernel.api.index.IndexDescriptor

class DelegatingQueryContext(inner: QueryContext) extends QueryContext {

  protected def singleDbHit[A](value: A): A = value
  protected def manyDbHits[A](value: Iterator[A]): Iterator[A] = value

  def isOpen: Boolean = inner.isOpen

  def isTopLevelTx: Boolean = inner.isTopLevelTx

  def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int =
    singleDbHit(inner.setLabelsOnNode(node, labelIds))

  def close(success: Boolean) {
    inner.close(success)
  }

  def createNode(): Node = singleDbHit(inner.createNode())

  def createRelationship(start: Node, end: Node, relType: String): Relationship = singleDbHit(inner.createRelationship(start, end, relType))

  def getOrCreateRelTypeId(relTypeName: String): Int = singleDbHit(inner.getOrCreateRelTypeId(relTypeName))

  def getLabelsForNode(node: Long): Iterator[Int] = singleDbHit(inner.getLabelsForNode(node))

  def getLabelName(id: Int): String = singleDbHit(inner.getLabelName(id))

  def getOptLabelId(labelName: String): Option[Int] = singleDbHit(inner.getOptLabelId(labelName))

  def getLabelId(labelName: String): Int = singleDbHit(inner.getLabelId(labelName))

  def getOrCreateLabelId(labelName: String): Int = singleDbHit(inner.getOrCreateLabelId(labelName))

  def getRelationshipsForIds(node: Node, dir: Direction, types: Option[Seq[Int]]): Iterator[Relationship] = manyDbHits(inner.getRelationshipsForIds(node, dir, types))

  def nodeOps = inner.nodeOps

  def relationshipOps = inner.relationshipOps

  def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int =
    singleDbHit(inner.removeLabelsFromNode(node, labelIds))

  def getPropertiesForNode(node: Long): Iterator[Long] = singleDbHit(inner.getPropertiesForNode(node))

  def getPropertiesForRelationship(relId: Long): Iterator[Long] = singleDbHit(inner.getPropertiesForRelationship(relId))

  def getPropertyKeyName(propertyKeyId: Int): String = singleDbHit(inner.getPropertyKeyName(propertyKeyId))

  def getOptPropertyKeyId(propertyKeyName: String): Option[Int] = singleDbHit(inner.getOptPropertyKeyId(propertyKeyName))

  def getPropertyKeyId(propertyKey: String) = singleDbHit(inner.getPropertyKeyId(propertyKey))

  def getOrCreatePropertyKeyId(propertyKey: String) = singleDbHit(inner.getOrCreatePropertyKeyId(propertyKey))

  def addIndexRule(labelId: Int, propertyKeyId: Int) = singleDbHit(inner.addIndexRule(labelId, propertyKeyId))

  def dropIndexRule(labelId: Int, propertyKeyId: Int) = singleDbHit(inner.dropIndexRule(labelId, propertyKeyId))

  def exactIndexSearch(index: IndexDescriptor, value: Any): Iterator[Node] = manyDbHits(inner.exactIndexSearch(index, value))

  def getNodesByLabel(id: Int): Iterator[Node] = manyDbHits(inner.getNodesByLabel(id))

  def upgrade(context: QueryContext): LockingQueryContext = inner.upgrade(context)

  def getOrCreateFromSchemaState[K, V](key: K, creator: => V): V = singleDbHit(inner.getOrCreateFromSchemaState(key, creator))

  def createUniqueConstraint(labelId: Int, propertyKeyId: Int) = singleDbHit(inner.createUniqueConstraint(labelId, propertyKeyId))

  def dropUniqueConstraint(labelId: Int, propertyKeyId: Int) = singleDbHit(inner.dropUniqueConstraint(labelId, propertyKeyId))

  def withAnyOpenQueryContext[T](work: (QueryContext) => T): T = inner.withAnyOpenQueryContext(work)

  def exactUniqueIndexSearch(index: IndexDescriptor, value: Any): Option[Node] = singleDbHit(inner.exactUniqueIndexSearch(index, value))

  override def commitAndRestartTx() {
    inner.commitAndRestartTx()
  }

  def getRelTypeId(relType: String): Int = singleDbHit(inner.getRelTypeId(relType))

  def getOptRelTypeId(relType: String): Option[Int] = singleDbHit(inner.getOptRelTypeId(relType))

  def getRelTypeName(id: Int): String = singleDbHit(inner.getRelTypeName(id))

  override def hasLocalFileAccess: Boolean = inner.hasLocalFileAccess

  def relationshipStartNode(rel: Relationship) = inner.relationshipStartNode(rel)

  def relationshipEndNode(rel: Relationship) = inner.relationshipEndNode(rel)

  def nodeGetDegree(node: Long, dir: Direction): Int = singleDbHit(inner.nodeGetDegree(node, dir))

  def nodeGetDegree(node: Long, dir: Direction, relTypeId: Int): Int = singleDbHit(inner.nodeGetDegree(node, dir, relTypeId))
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
