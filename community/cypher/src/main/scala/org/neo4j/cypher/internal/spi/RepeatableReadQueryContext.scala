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

import org.neo4j.graphdb.{PropertyContainer, Relationship, Direction, Node}


trait Locker {
  def acquireLock(p: PropertyContainer)

  def releaseAllLocks()
}

class RepeatableReadQueryContext(inner: QueryContext, locker: Locker) extends DelegatingQueryContext(inner) with LockingQueryContext {

  override def getRelationshipsFor(node: Node, dir: Direction, types: Seq[String]): Iterable[Relationship] = {
    locker.acquireLock(node)
    val rels = inner.getRelationshipsFor(node, dir, types)

    new Iterable[Relationship] {
      def iterator: Iterator[Relationship] = lockAll(rels.iterator)
    }
  }

  override def getLabelsForNode(node: Long): Iterator[Long] = {
    lockNode(node)
    inner.getLabelsForNode(node)
  }

  override def isLabelSetOnNode(label: Long, node: Long): Boolean = {
    lockNode(node)
    inner.isLabelSetOnNode(label, node)
  }

  override def exactIndexSearch(id: Long, value: Any): Iterator[Node] = lockAll(inner.exactIndexSearch(id, value))

  override def getNodesByLabel(id: Long): Iterator[Node] = lockAll(inner.getNodesByLabel(id))

  val nodeOpsValue = new RepeatableReadOperations[Node](inner.nodeOps)
  val relationshipOpsValue = new RepeatableReadOperations[Relationship](inner.relationshipOps)

  override def nodeOps = nodeOpsValue

  override def relationshipOps = relationshipOpsValue

  def releaseLocks() {
    locker.releaseAllLocks()
  }

  class RepeatableReadOperations[T <: PropertyContainer](inner: Operations[T]) extends DelegatingOperations[T](inner) {
    override def getProperty(obj: T, propertyKey: String) = {
      locker.acquireLock(obj)
      inner.getProperty(obj, propertyKey)
    }

    override def hasProperty(obj: T, propertyKey: String) = {
      locker.acquireLock(obj)
      inner.hasProperty(obj, propertyKey)
    }

    override def propertyKeys(obj: T) = {
      locker.acquireLock(obj)
      inner.propertyKeys(obj)
    }

    override def getById(id: Long): T = {
      val result = inner.getById(id)
      locker.acquireLock(result)
      result
    }

    override def indexGet(name: String, key: String, value: Any): Iterator[T] = lockAll(inner.indexGet(name, key, value))

    override def indexQuery(name: String, query: Any): Iterator[T] = lockAll(inner.indexQuery(name, query))

    def getByInnerId(id: Long): T = inner.getById(id)
  }

  private def lockNode(id: Long) {
    locker.acquireLock(nodeOps.getByInnerId(id))
  }

  private def lockAll[T <: PropertyContainer](iter: Iterator[T]): Iterator[T] = iter.map {
    item =>
      locker.acquireLock(item)
      item
  }
}



