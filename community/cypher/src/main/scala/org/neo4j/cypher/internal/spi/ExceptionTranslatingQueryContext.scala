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

import org.neo4j.graphdb.{PropertyContainer, Direction, Relationship, Node}
import org.neo4j.kernel.impl.api.index.IndexDescriptor
import org.neo4j.kernel.api.exceptions.KernelException
import org.neo4j.cypher.CypherExecutionException
import org.neo4j.kernel.api.operations.TokenNameLookup
import org.neo4j.cypher.internal.spi

class ExceptionTranslatingQueryContext(inner: QueryContext) extends DelegatingQueryContext(inner) {
  override def setLabelsOnNode(node: Long, labelIds: Iterator[Long]): Int =
    translateException(super.setLabelsOnNode(node, labelIds))

  override def close(success: Boolean) =
    translateException(super.close(success))

  override def createNode(): Node =
    translateException(super.createNode())


  override def createRelationship(start: Node, end: Node, relType: String): Relationship =
    translateException(super.createRelationship(start, end, relType))

  override def getLabelsForNode(node: Long): Iterator[Long] =
    translateException(super.getLabelsForNode(node))

  override def getLabelName(id: Long): String =
    translateException(super.getLabelName(id))

  override def getOptLabelId(labelName: String): Option[Long] =
    translateException(super.getOptLabelId(labelName))

  override def getLabelId(labelName: String): Long =
    translateException(super.getLabelId(labelName))

  override def getOrCreateLabelId(labelName: String): Long =
    translateException(super.getOrCreateLabelId(labelName))

  override def getRelationshipsFor(node: Node, dir: Direction, types: Seq[String]): Iterator[Relationship] =
    translateException(super.getRelationshipsFor(node, dir, types))

  override def nodeOps: Operations[Node] =
    new ExceptionTranslatingOperations[Node](super.nodeOps)

  override def relationshipOps: Operations[Relationship] =
    new ExceptionTranslatingOperations[Relationship](super.relationshipOps)

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Long]): Int =
    translateException(super.removeLabelsFromNode(node, labelIds))

  override def getPropertyKeyName(propertyKeyId: Long): String =
    translateException(super.getPropertyKeyName(propertyKeyId))

  override def getOptPropertyKeyId(propertyKeyName: String): Option[Long] =
    translateException(super.getOptPropertyKeyId(propertyKeyName))

  override def getPropertyKeyId(propertyKey: String): Long =
    translateException(super.getPropertyKeyId(propertyKey))

  override def getOrCreatePropertyKeyId(propertyKey: String): Long =
    translateException(super.getOrCreatePropertyKeyId(propertyKey))

  override def addIndexRule(labelIds: Long, propertyKeyId: Long) =
    translateException(super.addIndexRule(labelIds, propertyKeyId))

  override def dropIndexRule(labelIds: Long, propertyKeyId: Long) =
    translateException(super.dropIndexRule(labelIds, propertyKeyId))

  override def exactIndexSearch(index: IndexDescriptor, value: Any): Iterator[Node] =
    translateException(super.exactIndexSearch(index, value))

  override def getNodesByLabel(id: Long): Iterator[Node] =
    translateException(super.getNodesByLabel(id))

  override def getOrCreateFromSchemaState[K, V](key: K, creator: => V): V =
    translateException(super.getOrCreateFromSchemaState(key, creator))

  override def upgrade(context: spi.QueryContext): LockingQueryContext =
    translateException(super.upgrade(context))

  override def createUniqueConstraint(labelId: Long, propertyKeyId: Long) =
    translateException(super.createUniqueConstraint(labelId, propertyKeyId))

  override def dropUniqueConstraint(labelId: Long, propertyKeyId: Long) =
    translateException(super.dropUniqueConstraint(labelId, propertyKeyId))

  override def withAnyOpenQueryContext[T](work: (QueryContext) => T): T =
    super.withAnyOpenQueryContext(qc =>
      translateException(
        work(new ExceptionTranslatingQueryContext(qc))
      ))

  override def isLabelSetOnNode(label: Long, node: Long): Boolean =
    translateException(super.isLabelSetOnNode(label, node))

  class ExceptionTranslatingOperations[T <: PropertyContainer](inner: Operations[T])
    extends DelegatingOperations[T](inner) {
    override def delete(obj: T) =
      translateException(super.delete(obj))

    override def setProperty(obj: T, propertyKey: Long, value: Any) =
      translateException(super.setProperty(obj, propertyKey, value))

    override def getById(id: Long): T =
      translateException(super.getById(id))

    override def getProperty(obj: T, propertyKeyId: Long): Any =
      translateException(super.getProperty(obj, propertyKeyId))

    override def hasProperty(obj: T, propertyKeyId: Long): Boolean =
      translateException(super.hasProperty(obj, propertyKeyId))

    override def propertyKeyIds(obj: T): Iterator[Long] =
      translateException(super.propertyKeyIds(obj))

    override def removeProperty(obj: T, propertyKeyId: Long) =
      translateException(super.removeProperty(obj, propertyKeyId))

    override def indexGet(name: String, key: String, value: Any): Iterator[T] =
      translateException(super.indexGet(name, key, value))

    override def indexQuery(name: String, query: Any): Iterator[T] =
      translateException(super.indexQuery(name, query))

    override def all: Iterator[T] =
      translateException(super.all)
  }

  private def translateException[A](f: => A) = try {
    f
  } catch {
    case e: KernelException => throw new CypherExecutionException(e.getUserMessage(new TokenNameLookup {
      def propertyKeyGetName(propertyKeyId: Long): String = inner.getPropertyKeyName(propertyKeyId)

      def labelGetName(labelId: Long): String = inner.getLabelName(labelId)
    }), e)

  }
}

