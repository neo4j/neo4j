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

import org.neo4j.cypher.internal.spi._
import org.neo4j.graphdb._
import org.neo4j.kernel.{ThreadToStatementContextBridge, GraphDatabaseAPI}
import org.neo4j.kernel.api._
import collection.JavaConverters._
import org.neo4j.graphdb.DynamicRelationshipType.withName
import org.neo4j.cypher._
import org.neo4j.tooling.GlobalGraphOperations
import collection.mutable
import org.neo4j.kernel.impl.api.index.IndexDescriptor
import org.neo4j.helpers.collection.IteratorUtil.singleOrNull
import org.neo4j.kernel.api.operations.KeyNameLookup
import org.neo4j.kernel.api.exceptions.KernelException
import org.neo4j.kernel.api.exceptions.schema.{SchemaKernelException, DropIndexFailureException}
import org.neo4j.kernel.api.operations.StatementState
import org.neo4j.kernel.impl.api.PrimitiveLongIterator
import scala.collection.Iterator
import org.neo4j.cypher.internal.helpers.JavaConversionSupport
import org.neo4j.cypher.internal.helpers.JavaConversionSupport.mapToScala

class TransactionBoundQueryContext(graph: GraphDatabaseAPI, tx: Transaction,
                                   ctx: StatementOperationParts, theState: StatementState)
  extends TransactionBoundTokenContext(ctx.keyReadOperations, theState) with QueryContext {

  private var open = true

  def setLabelsOnNode(node: Long, labelIds: Iterator[Long]): Int = labelIds.foldLeft(0) {
    case (count, labelId) => if (ctx.entityWriteOperations.nodeAddLabel(theState, node, labelId)) count + 1 else count
  }

  def close(success: Boolean) {
    try {
      theState.close()

      if (success)
        tx.success()
      else
        tx.failure()
      tx.finish()
    }
    finally {
      open = false
    }
  }

  def withAnyOpenQueryContext[T](work: (QueryContext) => T): T = {
    if (open) {
      work(this)
    }
    else {
      val tx = graph.beginTx()
      try {
        val bridge   = graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])
        val stmCtx   = bridge.getCtxForReading
        val state    = bridge.statementForReading
        val result   = try {
          work(new TransactionBoundQueryContext(graph, tx, stmCtx, state))
        }
        finally {
          state.close()
        }
        tx.success()
        result
      }
      finally {
        tx.finish()
      }
    }
  }

  def createNode(): Node =
    graph.createNode()

  def createRelationship(start: Node, end: Node, relType: String) =
    start.createRelationshipTo(end, withName(relType))

  def getLabelsForNode(node: Long) =
    JavaConversionSupport.asScala( ctx.entityReadOperations.nodeGetLabels(theState, node) )

  override def isLabelSetOnNode(label: Long, node: Long) =
    ctx.entityReadOperations.nodeHasLabel(theState, node, label)

  def getOrCreateLabelId(labelName: String) =
    ctx.keyWriteOperations.labelGetOrCreateForName(theState, labelName)


  def getRelationshipsFor(node: Node, dir: Direction, types: Seq[String]): Iterator[Relationship] = types match {
    case Seq() => node.getRelationships(dir).iterator().asScala
    case _     => node.getRelationships(dir, types.map(withName): _*).iterator().asScala
  }

  def getTransaction = tx

  def exactIndexSearch(index: IndexDescriptor, value: Any) =
    mapToScala( ctx.entityReadOperations.nodesGetFromIndexLookup(theState, index, value) )(nodeOps.getById(_))

  val nodeOps = new NodeOperations

  val relationshipOps = new RelationshipOperations

  def removeLabelsFromNode(node: Long, labelIds: Iterator[Long]): Int = labelIds.foldLeft(0) {
    case (count, labelId) =>
      if (ctx.entityWriteOperations.nodeRemoveLabel(theState, node, labelId)) count + 1 else count
  }

  def getNodesByLabel(id: Long): Iterator[Node] =
    mapToScala( ctx.entityReadOperations.nodesGetForLabel(theState, id) )(nodeOps.getById(_))

  class NodeOperations extends BaseOperations[Node] {
    def delete(obj: Node) {
      ctx.entityWriteOperations.nodeDelete(theState, obj.getId)
    }

    def propertyKeyIds(obj: Node): Iterator[Long] =
      primitiveLongIteratorToScalaIterator(
        ctx.entityReadOperations.nodeGetPropertyKeys(theState, obj.getId)).map(_.longValue())

    def getProperty(obj: Node, propertyKeyId: Long): Any = {
      ctx.entityReadOperations.nodeGetProperty(theState, obj.getId, propertyKeyId).value(null)
    }

    def hasProperty(obj: Node, propertyKey: Long) =
      ctx.entityReadOperations.nodeHasProperty(theState, obj.getId, propertyKey)

    def removeProperty(obj: Node, propertyKeyId: Long) {
      ctx.entityWriteOperations.nodeRemoveProperty(theState, obj.getId, propertyKeyId)
    }

    def setProperty(obj: Node, propertyKeyId: Long, value: Any) {
      ctx.entityWriteOperations
         .nodeSetProperty(theState, obj.getId, properties.Property.property(propertyKeyId, value) )
    }


    def getById(id: Long) = try {
      graph.getNodeById(id)
    } catch {
      case e: NotFoundException => throw new EntityNotFoundException(s"Node with id $id", e)
      case e: RuntimeException  => throw e
    }

    def all: Iterator[Node] = GlobalGraphOperations.at(graph).getAllNodes.iterator().asScala

    def indexGet(name: String, key: String, value: Any): Iterator[Node] =
      graph.index.forNodes(name).get(key, value).iterator().asScala

    def indexQuery(name: String, query: Any): Iterator[Node] =
      graph.index.forNodes(name).query(query).iterator().asScala
  }

  class RelationshipOperations extends BaseOperations[Relationship] {
    def delete(obj: Relationship) {
      ctx.entityWriteOperations.relationshipDelete(theState, obj.getId)
    }

    def propertyKeyIds(obj: Relationship): Iterator[Long] =
      primitiveLongIteratorToScalaIterator(
        ctx.entityReadOperations.relationshipGetPropertyKeys(theState, obj.getId)).map(_.longValue())

    def getProperty(obj: Relationship, propertyKeyId: Long): Any =
      ctx.entityReadOperations.relationshipGetProperty(theState, obj.getId, propertyKeyId).value(null)

    def hasProperty(obj: Relationship, propertyKey: Long) =
      ctx.entityReadOperations.relationshipHasProperty(theState, obj.getId, propertyKey)

    def removeProperty(obj: Relationship, propertyKeyId: Long) {
      ctx.entityWriteOperations.relationshipRemoveProperty(theState, obj.getId, propertyKeyId)
    }

    def setProperty(obj: Relationship, propertyKeyId: Long, value: Any) {
      ctx.entityWriteOperations
         .relationshipSetProperty(theState, obj.getId, properties.Property.property(propertyKeyId, value) )
    }

    def getById(id: Long) = graph.getRelationshipById(id)

    def all: Iterator[Relationship] =
      GlobalGraphOperations.at(graph).getAllRelationships.iterator().asScala

    def indexGet(name: String, key: String, value: Any): Iterator[Relationship] =
      graph.index.forRelationships(name).get(key, value).iterator().asScala

    def indexQuery(name: String, query: Any): Iterator[Relationship] =
      graph.index.forRelationships(name).query(query).iterator().asScala
  }

  def getOrCreatePropertyKeyId(propertyKey: String) =
    ctx.keyWriteOperations.propertyKeyGetOrCreateForName(theState, propertyKey)

  def addIndexRule(labelIds: Long, propertyKeyId: Long) {
    try {
      ctx.schemaWriteOperations.indexCreate(theState, labelIds, propertyKeyId)
    } catch {
      case e: SchemaKernelException =>
        val labelName = getLabelName(labelIds)
        val propName = ctx.keyReadOperations.propertyKeyGetName(theState, propertyKeyId)
        throw new IndexAlreadyDefinedException(labelName, propName, e)
    }
  }

  def dropIndexRule(labelId: Long, propertyKeyId: Long) {
    try {
      ctx.schemaWriteOperations.indexDrop(theState, new IndexDescriptor(labelId, propertyKeyId))
    } catch {
      case e: DropIndexFailureException =>
        throw new CouldNotDropIndexException(e.getUserMessage(new KeyNameLookup(theState, ctx.keyReadOperations)), e)
    }
  }

  def upgrade(context: QueryContext): LockingQueryContext = new RepeatableReadQueryContext(context, new Locker {
    private val locks = new mutable.ListBuffer[Lock]

    def releaseAllLocks() {
      locks.foreach(_.release())
    }

    def acquireLock(p: PropertyContainer) {
      locks += tx.acquireWriteLock(p)
    }
  })

  abstract class BaseOperations[T <: PropertyContainer] extends Operations[T] {
    def propertyKeys(obj: T) = obj.getPropertyKeys.iterator().asScala
    
    def primitiveLongIteratorToScalaIterator( primitiveIterator: PrimitiveLongIterator ): Iterator[Long] = {
      new Iterator[Long]
      {
        def hasNext: Boolean = primitiveIterator.hasNext
        
        def next(): Long = primitiveIterator.next
      }
    }
  }

  def getOrCreateFromSchemaState[K, V](key: K, creator: => V) = {
    val javaCreator = new org.neo4j.helpers.Function[K, V]() {
      def apply(key: K) = creator
    }
    ctx.schemaStateOperations.schemaStateGetOrCreate(theState, key, javaCreator)
  }

  def schemaStateContains(key: String) = ctx.schemaStateOperations.schemaStateContains(theState, key)

  def createUniqueConstraint(labelId: Long, propertyKeyId: Long) {
    try {
      ctx.schemaWriteOperations.uniquenessConstraintCreate(theState, labelId, propertyKeyId)
    } catch {
        case e: KernelException =>
          throw new CouldNotCreateConstraintException(
            e.getUserMessage(new KeyNameLookup(theState, ctx.keyReadOperations)), e)
    }
  }

  def dropUniqueConstraint(labelId: Long, propertyKeyId: Long) {
    val constraint = singleOrNull(ctx.schemaReadOperations
                                     .constraintsGetForLabelAndPropertyKey(theState, labelId, propertyKeyId))

    if (constraint == null) {
      throw new MissingConstraintException()
    }

    ctx.schemaWriteOperations.constraintDrop(theState, constraint)
  }
}