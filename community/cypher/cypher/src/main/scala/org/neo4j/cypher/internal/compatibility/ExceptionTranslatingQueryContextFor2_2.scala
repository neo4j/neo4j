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
package org.neo4j.cypher.internal.compatibility

import java.net.URL

import org.neo4j.cypher.CypherExecutionException
import org.neo4j.cypher.internal.compiler.v2_2.spi
import org.neo4j.cypher.internal.compiler.v2_2.spi._
import org.neo4j.graphdb.{Direction, Node, PropertyContainer, Relationship}
import org.neo4j.kernel.api.TokenNameLookup
import org.neo4j.kernel.api.exceptions.KernelException
import org.neo4j.kernel.api.index.IndexDescriptor

class ExceptionTranslatingQueryContextFor2_2(inner: QueryContext) extends DelegatingQueryContext(inner) {
  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int =
    translateException(super.setLabelsOnNode(node, labelIds))

  override def close(success: Boolean) =
    translateException(super.close(success))

  override def createNode(): Node =
    translateException(super.createNode())

  override def createRelationship(start: Node, end: Node, relType: String): Relationship =
    translateException(super.createRelationship(start, end, relType))

  override def getLabelsForNode(node: Long): Iterator[Int] =
    translateException(super.getLabelsForNode(node))

  override def getLabelName(id: Int): String =
    translateException(super.getLabelName(id))

  override def getOptLabelId(labelName: String): Option[Int] =
    translateException(super.getOptLabelId(labelName))

  override def getLabelId(labelName: String): Int =
    translateException(super.getLabelId(labelName))

  override def getOrCreateLabelId(labelName: String): Int =
    translateException(super.getOrCreateLabelId(labelName))

  override def nodeOps: Operations[Node] =
    new ExceptionTranslatingOperations[Node](super.nodeOps)

  override def relationshipOps: Operations[Relationship] =
    new ExceptionTranslatingOperations[Relationship](super.relationshipOps)

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int =
    translateException(super.removeLabelsFromNode(node, labelIds))

  override def getPropertiesForNode(node: Long): Iterator[Long] =
    translateException(super.getPropertiesForNode(node))

  override def getPropertiesForRelationship(relId: Long): Iterator[Long] =
    translateException(super.getPropertiesForRelationship(relId))

  override def getPropertyKeyName(propertyKeyId: Int): String =
    translateException(super.getPropertyKeyName(propertyKeyId))

  override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] =
    translateException(super.getOptPropertyKeyId(propertyKeyName))

  override def getPropertyKeyId(propertyKey: String): Int =
    translateException(super.getPropertyKeyId(propertyKey))

  override def getOrCreatePropertyKeyId(propertyKey: String): Int =
    translateException(super.getOrCreatePropertyKeyId(propertyKey))

  override def addIndexRule(labelId: Int, propertyKeyId: Int) =
    translateException(super.addIndexRule(labelId, propertyKeyId))

  override def dropIndexRule(labelId: Int, propertyKeyId: Int) =
    translateException(super.dropIndexRule(labelId, propertyKeyId))

  override def exactIndexSearch(index: IndexDescriptor, value: Any): Iterator[Node] =
    translateException(super.exactIndexSearch(index, value))

  override def getNodesByLabel(id: Int): Iterator[Node] =
    translateException(super.getNodesByLabel(id))

  override def nodeGetDegree(node: Long, dir: Direction): Int =
    translateException(super.nodeGetDegree(node, dir))

  override def nodeGetDegree(node: Long, dir: Direction, relTypeId: Int): Int =
    translateException(super.nodeGetDegree(node, dir, relTypeId))

  override def getOrCreateFromSchemaState[K, V](key: K, creator: => V): V =
    translateException(super.getOrCreateFromSchemaState(key, creator))

  override def upgrade(context: spi.QueryContext): LockingQueryContext =
    translateException(super.upgrade(context))

  override def createUniqueConstraint(labelId: Int, propertyKeyId: Int) =
    translateException(super.createUniqueConstraint(labelId, propertyKeyId))

  override def dropUniqueConstraint(labelId: Int, propertyKeyId: Int) =
    translateException(super.dropUniqueConstraint(labelId, propertyKeyId))

  override def withAnyOpenQueryContext[T](work: (QueryContext) => T): T =
    super.withAnyOpenQueryContext(qc =>
      translateException(
        work(new ExceptionTranslatingQueryContextFor2_2(qc))
      ))

  override def isLabelSetOnNode(label: Int, node: Long): Boolean =
    translateException(super.isLabelSetOnNode(label, node))

  override def getRelTypeId(relType: String) =
    translateException(super.getRelTypeId(relType))

  override def getRelTypeName(id: Int) =
    translateException(super.getRelTypeName(id))

  override def lockingExactUniqueIndexSearch(index: IndexDescriptor, value: Any) =
    translateException(super.lockingExactUniqueIndexSearch(index, value))

  override def commitAndRestartTx() =
    translateException(super.commitAndRestartTx())

  override def getImportURL(url: URL): Either[String,URL] =
    translateException(super.getImportURL(url))

  override def relationshipStartNode(rel: Relationship) =
    translateException(super.relationshipStartNode(rel))

  override def relationshipEndNode(rel: Relationship) =
    translateException(super.relationshipEndNode(rel))

  class ExceptionTranslatingOperations[T <: PropertyContainer](inner: Operations[T])
    extends DelegatingOperations[T](inner) {
    override def delete(obj: T) =
      translateException(super.delete(obj))

    override def setProperty(id: Long, propertyKey: Int, value: Any) =
      translateException(super.setProperty(id, propertyKey, value))

    override def getById(id: Long): T =
      translateException(super.getById(id))

    override def getProperty(id: Long, propertyKeyId: Int): Any =
      translateException(super.getProperty(id, propertyKeyId))

    override def hasProperty(id: Long, propertyKeyId: Int): Boolean =
      translateException(super.hasProperty(id, propertyKeyId))

    override def propertyKeyIds(id: Long): Iterator[Int] =
      translateException(super.propertyKeyIds(id))

    override def removeProperty(id: Long, propertyKeyId: Int) =
      translateException(super.removeProperty(id, propertyKeyId))

    override def indexGet(name: String, key: String, value: Any): Iterator[T] =
      translateException(super.indexGet(name, key, value))

    override def indexQuery(name: String, query: Any): Iterator[T] =
      translateException(super.indexQuery(name, query))

    override def all: Iterator[T] =
      translateException(super.all)

    override def isDeleted(obj: T): Boolean =
      translateException(super.isDeleted(obj))
  }

  private def translateException[A](f: => A) = try {
    f
  } catch {
    case e: KernelException => throw new CypherExecutionException(e.getUserMessage(new TokenNameLookup {
      def propertyKeyGetName(propertyKeyId: Int): String = inner.getPropertyKeyName(propertyKeyId)

      def labelGetName(labelId: Int): String = inner.getLabelName(labelId)

      def relationshipTypeGetName(relTypeId: Int): String = inner.getRelTypeName(relTypeId)
    }), e)

  }
}

