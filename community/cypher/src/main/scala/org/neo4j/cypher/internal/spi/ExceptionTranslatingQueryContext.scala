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


class ExceptionTranslatingQueryContext(inner: QueryContext) extends DelegatingQueryContext(inner)
{
  private def translateKernelException(e: KernelException): CypherExecutionException =
  {
    new CypherExecutionException(e.getUserMessage(new TokenNameLookup
    {
      def propertyKeyGetName(propertyKeyId: Long): String = inner.getPropertyKeyName(propertyKeyId)

      def labelGetName(labelId: Long): String = inner.getLabelName(labelId)
    }), e)
  }

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Long]): Int =
    try
    {
      super.setLabelsOnNode(node, labelIds)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }

  override def close(success: Boolean) =
    try
    {
      super.close(success)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }

  override def createNode(): Node =
    try
    {
      super.createNode()
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }


  override def createRelationship(start: Node, end: Node, relType: String): Relationship =
    try
    {
      super.createRelationship(start, end, relType)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }

  override def getLabelsForNode(node: Long): Iterator[Long] =
    try
    {
      super.getLabelsForNode(node)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }

  override def getLabelName(id: Long): String =
    try
    {
      super.getLabelName(id)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }

  override def getOptLabelId(labelName: String): Option[Long] =
    try
    {
      super.getOptLabelId(labelName)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }

  override def getLabelId(labelName: String): Long =
    try
    {
      super.getLabelId(labelName)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }

  override def getOrCreateLabelId(labelName: String): Long =
    try
    {
      super.getOrCreateLabelId(labelName)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }

  override def getRelationshipsFor(node: Node, dir: Direction, types: Seq[String]): Iterator[Relationship] =
    try
    {
      super.getRelationshipsFor(node, dir, types)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }

  override def nodeOps: Operations[Node] =
    new ExceptionTranslatingOperations[Node](super.nodeOps, translateKernelException)

  override def relationshipOps: Operations[Relationship] =
    new ExceptionTranslatingOperations[Relationship](super.relationshipOps, translateKernelException)

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Long]): Int =
    try
    {
      super.removeLabelsFromNode(node, labelIds)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }

  override def getPropertyKeyName(propertyKeyId: Long): String =
    try
    {
      super.getPropertyKeyName(propertyKeyId)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }

  override def getOptPropertyKeyId(propertyKeyName: String): Option[Long] =
    try
    {
      super.getOptPropertyKeyId(propertyKeyName)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }

  override def getPropertyKeyId(propertyKey: String): Long =
    try
    {
      super.getPropertyKeyId(propertyKey)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }

  override def getOrCreatePropertyKeyId(propertyKey: String): Long =
    try
    {
      super.getOrCreatePropertyKeyId(propertyKey)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }

  override def addIndexRule(labelIds: Long, propertyKeyId: Long) =
    try
    {
      super.addIndexRule(labelIds, propertyKeyId)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }

  override def dropIndexRule(labelIds: Long, propertyKeyId: Long) =
    try
    {
      super.dropIndexRule(labelIds, propertyKeyId)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }

  override def exactIndexSearch(index: IndexDescriptor, value: Any): Iterator[Node] =
    try
    {
      super.exactIndexSearch(index, value)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }

  override def getNodesByLabel(id: Long): Iterator[Node] =
    try
    {
      super.getNodesByLabel(id)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }

  override def upgrade(context: QueryContext): LockingQueryContext =
    try
    {
      super.upgrade(context)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }

  override def getOrCreateFromSchemaState[K, V](key: K, creator: => V): V =
    try
    {
      super.getOrCreateFromSchemaState(key, creator)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }

  override def schemaStateContains(key: String): Boolean =
    try
    {
      super.schemaStateContains(key)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }

  override def createUniqueConstraint(labelId: Long, propertyKeyId: Long) =
    try
    {
      super.createUniqueConstraint(labelId, propertyKeyId)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }

  override def dropUniqueConstraint(labelId: Long, propertyKeyId: Long) =
    try
    {
      super.dropUniqueConstraint(labelId, propertyKeyId)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }

  override def withAnyOpenQueryContext[T](work: (QueryContext) => T): T =
    super.withAnyOpenQueryContext(qc =>
      try
      {
        work(new ExceptionTranslatingQueryContext(qc))
      }
      catch
        {
          case e: KernelException =>
            throw translateKernelException(e)
        })

  override def isLabelSetOnNode(label: Long, node: Long): Boolean =
    try
    {
      super.isLabelSetOnNode(label, node)
    }
    catch
      {
        case e: KernelException =>
          throw translateKernelException(e)
      }
}

class ExceptionTranslatingOperations[T <: PropertyContainer]
(inner: Operations[T], val translateKernelException: (KernelException) => CypherExecutionException)
  extends DelegatingOperations[T](inner)
{
  override def delete(obj: T) =
    try
    {
      super.delete(obj)
    }
    catch
      {
        case e: KernelException => throw translateKernelException(e)
      }

  override def setProperty(obj: T, propertyKey: Long, value: Any) =
    try
    {
      super.setProperty(obj, propertyKey, value)
    }
    catch
      {
        case e: KernelException => throw translateKernelException(e)
      }

  override def getById(id: Long): T =
    try
    {
      super.getById(id)
    }
    catch
      {
        case e: KernelException => throw translateKernelException(e)
      }

  override def getProperty(obj: T, propertyKeyId: Long): Any =
    try
    {
      super.getProperty(obj, propertyKeyId)
    }
    catch
      {
        case e: KernelException => throw translateKernelException(e)
      }

  override def hasProperty(obj: T, propertyKeyId: Long): Boolean =
    try
    {
      super.hasProperty(obj, propertyKeyId)
    }
    catch
      {
        case e: KernelException => throw translateKernelException(e)
      }

  override def propertyKeys(obj: T): Iterator[String] =
    try
    {
      super.propertyKeys(obj)
    }
    catch
      {
        case e: KernelException => throw translateKernelException(e)
      }

  override def propertyKeyIds(obj: T): Iterator[Long] =
    try
    {
      super.propertyKeyIds(obj)
    }
    catch
      {
        case e: KernelException => throw translateKernelException(e)
      }

  override def removeProperty(obj: T, propertyKeyId: Long) =
    try
    {
      super.removeProperty(obj, propertyKeyId)
    }
    catch
      {
        case e: KernelException => throw translateKernelException(e)
      }

  override def indexGet(name: String, key: String, value: Any): Iterator[T] =
    try
    {
      super.indexGet(name, key, value)
    }
    catch
      {
        case e: KernelException => throw translateKernelException(e)
      }

  override def indexQuery(name: String, query: Any): Iterator[T] =
    try
    {
      super.indexQuery(name, query)
    }
    catch
      {
        case e: KernelException => throw translateKernelException(e)
      }

  override def all: Iterator[T] =
    try
    {
      super.all
    }
    catch
      {
        case e: KernelException => throw translateKernelException(e)
      }
}