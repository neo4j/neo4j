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

import org.neo4j.graphdb.{PropertyContainer, Relationship, Direction, Node}


trait Locker {
  def readLock(p: PropertyContainer)

  def releaseAllReadLocks()
}

class RepeatableReadQueryContext(inner: QueryContext, locker: Locker) extends DelegatingQueryContext(inner) {

  override def getRelationshipsFor(node: Node, dir: Direction, types: Seq[String]): Iterable[Relationship] = {
    locker.readLock(node)
    val rels = inner.getRelationshipsFor(node, dir, types)

    new Iterable[Relationship] {
      def iterator: Iterator[Relationship] = rels.iterator.map {
        rel =>
          locker.readLock(rel)
          rel
      }
    }
  }

  val nodeOpsValue = new RepeatableReadOperations[Node](inner.nodeOps)
  val relationshipOpsValue = new RepeatableReadOperations[Relationship](inner.relationshipOps)

  override def nodeOps = nodeOpsValue

  override def relationshipOps = relationshipOpsValue

  override def close() {
    locker.releaseAllReadLocks()
    inner.close()
  }

  class RepeatableReadOperations[T <: PropertyContainer](inner: Operations[T]) extends DelegatingOperations[T](inner) {
    override def getProperty(obj: T, propertyKey: String) = {
      locker.readLock(obj)
      inner.getProperty(obj, propertyKey)
    }

    override def hasProperty(obj: T, propertyKey: String) = {
      locker.readLock(obj)
      inner.hasProperty(obj, propertyKey)
    }

    override def propertyKeys(obj: T) = {
      locker.readLock(obj)
      inner.propertyKeys(obj)
    }

    override def isDeleted(obj: T): Boolean = {
      locker.readLock(obj)
      inner.isDeleted(obj)
    }
  }

}



