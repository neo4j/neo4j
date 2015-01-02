/**
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
package org.neo4j.cypher.internal.compiler.v2_0.spi

import org.neo4j.graphdb.{PropertyContainer, Direction, Node}
import org.neo4j.kernel.api.index.IndexDescriptor
import java.net.URL


class DelegatingQueryContext(inner: QueryContext) extends QueryContext {
  def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int = {
    inner.setLabelsOnNode(node, labelIds)
  }

  def close(success: Boolean) {
    inner.close(success)
  }

  def createNode() = inner.createNode()

  def createRelationship(start: Node, end: Node, relType: String) = inner.createRelationship(start, end, relType)

  def getLabelsForNode(node: Long) = inner.getLabelsForNode(node)

  def getLabelName(id: Int) = inner.getLabelName(id)

  def getOptLabelId(labelName: String): Option[Int] = inner.getOptLabelId(labelName)

  def getLabelId(labelName: String): Int = inner.getLabelId(labelName)

  def getOrCreateLabelId(labelName: String) = inner.getOrCreateLabelId(labelName)

  def getRelationshipsFor(node: Node, dir: Direction, types: Seq[String]) = inner.getRelationshipsFor(node, dir, types)

  def nodeOps = inner.nodeOps

  def relationshipOps = inner.relationshipOps

  def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int = {
    inner.removeLabelsFromNode(node, labelIds)
  }

  def getPropertyKeyName(propertyKeyId: Int): String = inner.getPropertyKeyName(propertyKeyId)

  def getOptPropertyKeyId(propertyKeyName: String): Option[Int] = inner.getOptPropertyKeyId(propertyKeyName)

  def getPropertyKeyId(propertyKey: String) = inner.getPropertyKeyId(propertyKey)

  def getOrCreatePropertyKeyId(propertyKey: String) = inner.getOrCreatePropertyKeyId(propertyKey)

  def addIndexRule(labelId: Int, propertyKeyId: Int) = inner.addIndexRule(labelId, propertyKeyId)

  def dropIndexRule(labelId: Int, propertyKeyId: Int) { inner.dropIndexRule(labelId, propertyKeyId) }

  def exactIndexSearch(index: IndexDescriptor, value: Any): Iterator[Node] = inner.exactIndexSearch(index, value)

  def getNodesByLabel(id: Int): Iterator[Node] = inner.getNodesByLabel(id)

  def upgrade(context: QueryContext): LockingQueryContext = inner.upgrade(context)

  def getOrCreateFromSchemaState[K, V](key: K, creator: => V): V = inner.getOrCreateFromSchemaState(key, creator)

  def createUniqueConstraint(labelId: Int, propertyKeyId: Int) = inner.createUniqueConstraint(labelId, propertyKeyId)

  def dropUniqueConstraint(labelId: Int, propertyKeyId: Int) {
    inner.dropUniqueConstraint(labelId, propertyKeyId)
  }

  def withAnyOpenQueryContext[T](work: (QueryContext) => T): T = inner.withAnyOpenQueryContext(work)

  def exactUniqueIndexSearch(index: IndexDescriptor, value: Any): Option[Node] = inner.exactUniqueIndexSearch(index, value)
}

class DelegatingOperations[T <: PropertyContainer](protected val inner: Operations[T]) extends Operations[T] {
  def delete(obj: T) {
    inner.delete(obj)
  }

  def setProperty(obj: Long, propertyKey: Int, value: Any) {
    inner.setProperty(obj, propertyKey, value)
  }

  def getById(id: Long) = inner.getById(id)

  def getProperty(obj: Long, propertyKeyId: Int) = inner.getProperty(obj, propertyKeyId)

  def hasProperty(obj: Long, propertyKeyId: Int) = inner.hasProperty(obj, propertyKeyId)

  def propertyKeyIds(obj: Long) = inner.propertyKeyIds(obj)

  def removeProperty(obj: Long, propertyKeyId: Int) {
    inner.removeProperty(obj, propertyKeyId)
  }

  def indexGet(name: String, key: String, value: Any): Iterator[T] = inner.indexGet(name, key, value)

  def indexQuery(name: String, query: Any): Iterator[T] = inner.indexQuery(name, query)

  def all: Iterator[T] = inner.all


}
