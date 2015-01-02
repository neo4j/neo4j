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
package org.neo4j.cypher.internal.compiler.v1_9.spi

import org.neo4j.graphdb.{PropertyContainer, Direction, Node}


class DelegatingQueryContext(inner: QueryContext) extends QueryContext {

  def close() {
    inner.close()
  }

  def createNode() = inner.createNode()

  def createRelationship(start: Node, end: Node, relType: String) = inner.createRelationship(start, end, relType)

  def getRelationshipsFor(node: Node, dir: Direction, types: Seq[String]) = inner.getRelationshipsFor(node, dir, types)

  def nodeOps = inner.nodeOps

  def relationshipOps = inner.relationshipOps
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

  def isDeleted(obj: T): Boolean = inner.isDeleted(obj)
}
