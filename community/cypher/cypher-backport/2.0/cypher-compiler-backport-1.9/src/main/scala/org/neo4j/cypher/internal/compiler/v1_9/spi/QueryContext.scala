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

import org.neo4j.graphdb._

trait QueryContext {
  def nodeOps: Operations[Node]

  def relationshipOps: Operations[Relationship]

  def createNode(): Node

  def createRelationship(start: Node, end: Node, relType: String): Relationship

  def getRelationshipsFor(node: Node, dir: Direction, types: Seq[String]): Iterable[Relationship]

  def close()
}

trait Operations[T <: PropertyContainer] {
  def delete(obj: T)

  def setProperty(obj: T, propertyKey: String, value: Any)

  def removeProperty(obj: T, propertyKey: String)

  def getProperty(obj: T, propertyKey: String): Any

  def hasProperty(obj: T, propertyKey: String): Boolean

  def propertyKeys(obj: T): Iterable[String]

  def getById(id: Long): T

  def indexGet(name: String, key: String, value: Any): Iterator[T]

  def indexQuery(name: String, query: Any): Iterator[T]

  def all : Iterator[T]

  def isDeleted(obj: T): Boolean
}
