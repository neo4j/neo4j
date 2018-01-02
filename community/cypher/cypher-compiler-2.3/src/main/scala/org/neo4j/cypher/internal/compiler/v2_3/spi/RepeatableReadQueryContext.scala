/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.spi

import org.neo4j.cypher.internal.compiler.v2_3.spi.SchemaTypes.IndexDescriptor
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}


trait Locker {
  def acquireLock(p: PropertyContainer)

  def releaseAllLocks()
}

final class RepeatableReadQueryContext(inner: QueryContext, locker: Locker) extends DelegatingQueryContext(inner) with LockingQueryContext {

  override def getLabelsForNode(node: Long): Iterator[Int] = {
    lockNode(node)
    inner.getLabelsForNode(node)
  }

  override def nodeGetDegree(node: Long, dir: SemanticDirection): Int = {
    lockNode(node)
    inner.nodeGetDegree(node, dir)
  }

  override def nodeGetDegree(node: Long, dir: SemanticDirection, relTypeId: Int): Int = {
    lockNode(node)
    inner.nodeGetDegree(node, dir, relTypeId)
  }

  override def isLabelSetOnNode(label: Int, node: Long): Boolean = {
    lockNode(node)
    inner.isLabelSetOnNode(label, node)
  }

  override def indexSeek(index: IndexDescriptor, value: Any): Iterator[Node] =
    lockAll(inner.indexSeek(index, value))

  override def getNodesByLabel(id: Int): Iterator[Node] = lockAll(inner.getNodesByLabel(id))

  val nodeOpsValue = new RepeatableReadOperations[Node](inner.nodeOps)
  val relationshipOpsValue = new RepeatableReadOperations[Relationship](inner.relationshipOps)

  override def nodeOps = nodeOpsValue

  override def relationshipOps = relationshipOpsValue

  def releaseLocks() {
    locker.releaseAllLocks()
  }


  override def getPropertiesForNode(node: Long): Iterator[Int] = {
    lockNode(node)
    inner.getPropertiesForNode(node)
  }

  override def getPropertiesForRelationship(relId: Long): Iterator[Int] = {
    lockRelationship(relId)
    inner.getPropertiesForRelationship(relId)
  }


  class RepeatableReadOperations[T <: PropertyContainer](inner: Operations[T]) extends DelegatingOperations[T](inner) {
    override def getProperty(id: Long, propertyKeyId: Int) = {
      val obj = inner.getById(id)
      locker.acquireLock(obj)
      inner.getProperty(id, propertyKeyId)
    }

    override def hasProperty(id: Long, propertyKeyId: Int) = {
      val obj = inner.getById(id)
      locker.acquireLock(obj)
      inner.hasProperty(id, propertyKeyId)
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

  private def lockRelationship(id : Long) {
    locker.acquireLock(relationshipOps.getByInnerId(id))
  }

  private def lockAll[T <: PropertyContainer](iter: Iterator[T]): Iterator[T] = iter.map {
    item =>
      locker.acquireLock(item)
      item
  }
}



