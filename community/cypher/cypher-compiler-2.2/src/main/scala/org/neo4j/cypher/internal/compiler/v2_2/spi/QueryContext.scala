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

import org.neo4j.cypher.internal.compiler.v2_2.InternalQueryStatistics
import org.neo4j.graphdb._
import org.neo4j.kernel.api.constraints.UniquenessConstraint
import org.neo4j.kernel.api.index.IndexDescriptor

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
trait QueryContext extends TokenContext {
  def nodeOps: Operations[Node]

  def relationshipOps: Operations[Relationship]

  def createNode(): Node

  def createRelationship(start: Node, end: Node, relType: String): Relationship

  def getOrCreateRelTypeId(relTypeName: String): Int

  def getRelationshipsForIds(node: Node, dir: Direction, types: Option[Seq[Int]]): Iterator[Relationship]

  def getOrCreateLabelId(labelName: String): Int

  def getLabelsForNode(node: Long): Iterator[Int]

  def isLabelSetOnNode(label: Int, node: Long): Boolean = getLabelsForNode(node).toIterator.contains(label)

  def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int

  def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int

  def getPropertiesForNode(node: Long): Iterator[Long]

  def getPropertiesForRelationship(relId: Long): Iterator[Long]

  def getOrCreatePropertyKeyId(propertyKey: String): Int

  def addIndexRule(labelId: Int, propertyKeyId: Int): IdempotentResult[IndexDescriptor]

  def dropIndexRule(labelId: Int, propertyKeyId: Int)

  def isOpen: Boolean

  def isTopLevelTx: Boolean

  def close(success: Boolean)

  def exactIndexSearch(index: IndexDescriptor, value: Any): Iterator[Node]

  def exactUniqueIndexSearch(index: IndexDescriptor, value: Any): Option[Node]

  def getNodesByLabel(id: Int): Iterator[Node]

  def upgradeToLockingQueryContext: LockingQueryContext = upgrade(this)

  def upgrade(context: QueryContext): LockingQueryContext

  def getOrCreateFromSchemaState[K, V](key: K, creator: => V): V

  def createUniqueConstraint(labelId: Int, propertyKeyId: Int): IdempotentResult[UniquenessConstraint]

  def dropUniqueConstraint(labelId: Int, propertyKeyId: Int)

  def getOptStatistics: Option[InternalQueryStatistics] = None

  def hasLocalFileAccess: Boolean = false

  /**
   * This should not be used. We'll remove sooner (or later). Don't do it.
   */
  def withAnyOpenQueryContext[T](work: (QueryContext) => T): T

  def commitAndRestartTx()

  def relationshipStartNode(rel: Relationship): Node

  def relationshipEndNode(rel: Relationship): Node

  def nodeGetDegree(node: Long, dir: Direction): Int

  def nodeGetDegree(node: Long, dir: Direction, relTypeId: Int): Int
}

trait LockingQueryContext extends QueryContext {
  def releaseLocks()
}

trait Operations[T <: PropertyContainer] {
  def delete(obj: T)

  def setProperty(obj: Long, propertyKeyId: Int, value: Any)

  def removeProperty(obj: Long, propertyKeyId: Int)

  def getProperty(obj: Long, propertyKeyId: Int): Any

  def hasProperty(obj: Long, propertyKeyId: Int): Boolean

  def propertyKeyIds(obj: Long): Iterator[Int]

  def getById(id: Long): T

  def indexGet(name: String, key: String, value: Any): Iterator[T]

  def indexQuery(name: String, query: Any): Iterator[T]

  def isDeleted(obj: T): Boolean

  def all: Iterator[T]
}
