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
package org.neo4j.cypher.internal.spi.gdsimpl

import org.neo4j.cypher.internal.spi.{Operations, DelegatingOperations, DelegatingQueryContext, QueryContext}
import org.neo4j.graphdb.{Relationship, Direction, Node}
import org.neo4j.kernel.impl.api.LockHolder

class RepeatableReadQueryContext(inner: QueryContext, locker: LockHolder) extends DelegatingQueryContext(inner) {
  override def getLabelsForNode(node: Long) = {
    locker.acquireNodeReadLock(node)
    inner.getLabelsForNode(node)
  }

  override def getRelationshipsFor(node: Node, dir: Direction, types: Seq[String]) = {
    locker.acquireNodeReadLock(node.getId)
    inner.getRelationshipsFor(node, dir, types)
    ??? // We should lock relationships here, before we return the seq
  }

  val nodeOpsValue = new RepeatableReadOperations[Node](inner.nodeOps)
  val relationshipOpsValue = new RepeatableReadOperations[Relationship](inner.relationshipOps)

  override def nodeOps = nodeOpsValue

  override def relationshipOps = relationshipOpsValue

  override def close(success: Boolean) {
    locker.releaseLocks()
    inner.close(success)
  }

  private def id(obj: Any): Long = obj match {
    case n: Node         => n.getId
    case r: Relationship => r.getId
  }

  class RepeatableReadOperations[T](inner: Operations[T]) extends DelegatingOperations[T](inner) {
    override def getProperty(obj: T, propertyKey: String) = {
      locker.acquireNodeReadLock(id(obj))
      inner.getProperty(obj, propertyKey)
    }

    override def hasProperty(obj: T, propertyKey: String) = {
      locker.acquireNodeReadLock(id(obj))
      inner.hasProperty(obj, propertyKey)
    }

    override def propertyKeys(obj: T) = {
      locker.acquireNodeReadLock(id(obj))
      inner.propertyKeys(obj)
    }
  }
}




