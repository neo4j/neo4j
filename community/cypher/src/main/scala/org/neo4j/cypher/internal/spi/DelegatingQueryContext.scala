/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.spi

import org.neo4j.graphdb.{PropertyContainer, Direction, Node}


class DelegatingQueryContext(inner: QueryContext) extends QueryContext {
  def setLabelsOnNode(node: Long, labelIds: Iterable[Long]): Int = {
    inner.setLabelsOnNode(node, labelIds)
  }

  def close(success: Boolean) {
    inner.close(success)
  }

  def createNode() = inner.createNode()

  def createRelationship(start: Node, end: Node, relType: String) = inner.createRelationship(start, end, relType)

  def getLabelName(id: Long) = inner.getLabelName(id)

  def getLabelsForNode(node: Long) = inner.getLabelsForNode(node)

  def getOrCreateLabelId(labelName: String) = inner.getOrCreateLabelId(labelName)

  def getRelationshipsFor(node: Node, dir: Direction, types: Seq[String]) = inner.getRelationshipsFor(node, dir, types)

  def nodeOps = inner.nodeOps

  def relationshipOps = inner.relationshipOps

  def removeLabelsFromNode(node: Long, labelIds: Iterable[Long]): Int = {
    inner.removeLabelsFromNode(node, labelIds)
  }

  def replaceLabelsOfNode(node: Long, labelIds: Iterable[Long]) {
    inner.replaceLabelsOfNode(node, labelIds)
  }

  def getTransaction = inner.getTransaction

  def getOrCreatePropertyKeyId(propertyKey: String) = inner.getOrCreatePropertyKeyId(propertyKey)

  def getPropertyKeyId(propertyKey: String) = inner.getPropertyKeyId(propertyKey)

  def addIndexRule(labelIds: Long, propertyKeyId: Long) = inner.addIndexRule(labelIds, propertyKeyId)

  def dropIndexRule(labelIds: Long, propertyKeyId: Long) = inner.dropIndexRule(labelIds, propertyKeyId)
}

class DelegatingOperations[T <: PropertyContainer](protected val inner: Operations[T]) extends Operations[T] {
  def delete(obj: T) {
    inner.delete(obj)
  }

  def setProperty(obj: T, propertyKey: String, value: Any) {
    inner.setProperty(obj, propertyKey, value)
  }

  def getById(id: Long) = inner.getById(id)

  def getProperty(obj: T, propertyKey: String) = inner.getProperty(obj, propertyKey)

  def hasProperty(obj: T, propertyKey: String) = inner.hasProperty(obj, propertyKey)

  def propertyKeys(obj: T) = inner.propertyKeys(obj)

  def removeProperty(obj: T, propertyKey: String) {
    inner.removeProperty(obj, propertyKey)
  }

  def indexGet(name: String, key: String, value: Any): Iterator[T] = inner.indexGet(name, key, value)

  def indexQuery(name: String, query: Any): Iterator[T] = inner.indexQuery(name, query)

  def all: Iterator[T] = inner.all
}