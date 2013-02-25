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

import org.neo4j.graphdb._

/*
 * Developer note: This is an attempt at an internal graph database API, which defines a clean cut between
 * two layers, the query engine layer and, for lack of a better name, the core database layer.
 *
 * Building the query engine layer on top of an internal layer means we can move much faster, not
 * having to worry about deprecations and so on. It is also acceptable if this layer is a bit clunkier, in this
 * case we are, for instance, not exposing any node or relationship objects, but provide direct methods for manipulating
 * them by ids instead.
 *
 * The driver for this was clarifying who is responsible for ensuring query isolation. By exposing a query concept in
 * the core layer, we can move that responsibility outside of the scope of cypher.
 */
trait QueryContext {
  def nodeOps: Operations[Node]

  def relationshipOps: Operations[Relationship]

  def createNode(): Node

  def createRelationship(start: Node, end: Node, relType: String): Relationship

  def getRelationshipsFor(node: Node, dir: Direction, types: Seq[String]): Iterable[Relationship]

  def getOrCreateLabelId(labelName: String): Long

  def getLabelName(id: Long): String

  def getLabelsForNode(node: Long): Iterable[Long]

  def isLabelSetOnNode(label: Long, node: Long): Boolean = getLabelsForNode(node).toIterator.contains(label)

  def setLabelsOnNode(node: Long, labelIds: Iterable[Long]): Int

  def replaceLabelsOfNode(node: Long, labelIds: Iterable[Long])

  def removeLabelsFromNode(node: Long, labelIds: Iterable[Long]): Int

  def getOrCreatePropertyKeyId(propertyKey: String): Long

  def getPropertyKeyId(propertyKey: String): Long

  def addIndexRule(labelIds: Long, propertyKeyId: Long)

  def dropIndexRule(labelIds: Long, propertyKeyId: Long)

  def close(success: Boolean)

  /**
   * This should not exist. It's a transient stated before locking is done somewhere else
   * @return
   */
  @Deprecated
  def getTransaction: Transaction
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
}
