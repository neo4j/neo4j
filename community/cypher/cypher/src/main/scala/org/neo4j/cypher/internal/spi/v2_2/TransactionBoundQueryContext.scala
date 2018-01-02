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
package org.neo4j.cypher.internal.spi.v2_2

import java.net.URL

import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.cypher.internal.compiler.v2_2.spi._
import org.neo4j.cypher.internal.compiler.v2_2.{EntityNotFoundException, FailedIndexException}
import org.neo4j.cypher.internal.helpers.JavaConversionSupport._
import org.neo4j.graphdb.DynamicRelationshipType._
import org.neo4j.graphdb._
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.api.constraints.UniquenessConstraint
import org.neo4j.kernel.api.exceptions.schema.{AlreadyConstrainedException, AlreadyIndexedException}
import org.neo4j.kernel.api.index.{IndexDescriptor, InternalIndexState}
import org.neo4j.kernel.api.{exceptions, _}
import org.neo4j.kernel.impl.api.KernelStatement
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.security.URLAccessValidationError
import org.neo4j.tooling.GlobalGraphOperations

import scala.collection.JavaConverters._
import scala.collection.{Iterator, mutable}

final class TransactionBoundQueryContext(graph: GraphDatabaseAPI,
                                         private var tx: Transaction,
                                         val isTopLevelTx: Boolean,
                                         initialStatement: Statement)
  extends TransactionBoundTokenContext(initialStatement) with QueryContext {

  private var open = true
  private val txBridge = graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])

  val nodeOps = new NodeOperations

  val relationshipOps = new RelationshipOperations

  def isOpen = open

  def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) => if (statement.dataWriteOperations().nodeAddLabel(node, labelId)) count + 1 else count
  }

  def close(success: Boolean) {
    if (isOpen) {
      try {
        statement.close()

        if (success)
          tx.success()
        else
          tx.failure()
        tx.close()
      }
      finally {
        statement = null
        tx = null
        open = false
      }
    }
  }

  def withAnyOpenQueryContext[T](work: (QueryContext) => T): T = {
    if (open) {
      work(this)
    }
    else {
      val isTopLevelTx = !txBridge.hasTransaction
      val tx = graph.beginTx()
      try {
        val otherStatement = txBridge.get()
        val result = try {
          work(new TransactionBoundQueryContext(graph, tx, isTopLevelTx, otherStatement))
        }
        finally {
          otherStatement.close()
        }
        tx.success()
        result
      }
      finally {
        tx.close()
      }
    }
  }

  def createNode(): Node =
    graph.createNode()

  def createRelationship(start: Node, end: Node, relType: String) =
    start.createRelationshipTo(end, withName(relType))

  def getOrCreateRelTypeId(relTypeName: String): Int =
    statement.tokenWriteOperations().relationshipTypeGetOrCreateForName(relTypeName)

  def getLabelsForNode(node: Long) =
    asScala(statement.readOperations().nodeGetLabels(node))

  def getPropertiesForNode(node: Long) =
    mapToScala(statement.readOperations().nodeGetPropertyKeys(node))(_.toLong)

  def getPropertiesForRelationship(relId: Long) =
    mapToScala(statement.readOperations().relationshipGetPropertyKeys(relId))(_.toLong)

  override def isLabelSetOnNode(label: Int, node: Long) =
    statement.readOperations().nodeHasLabel(node, label)

  def getOrCreateLabelId(labelName: String) =
    statement.tokenWriteOperations().labelGetOrCreateForName(labelName)

  def getRelationshipsForIds(node: Node, dir: Direction, types: Option[Seq[Int]]): Iterator[Relationship] = types match {
    case None => asScala(statement.readOperations().nodeGetRelationships(node.getId, dir)).map(relationshipOps.getById)
    case Some(typeIds) => asScala(statement.readOperations().nodeGetRelationships(node.getId, dir, typeIds: _* )).map(relationshipOps.getById)
  }

  def exactIndexSearch(index: IndexDescriptor, value: Any) =
    mapToScala(statement.readOperations().nodesGetFromIndexSeek(index, value))(nodeOps.getById)

  def lockingExactUniqueIndexSearch(index: IndexDescriptor, value: Any): Option[Node] = {
    val nodeId: Long = statement.readOperations().nodeGetFromUniqueIndexSeek(index, value)
    if (StatementConstants.NO_SUCH_NODE == nodeId) None else Some(nodeOps.getById(nodeId))
  }

  def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) =>
      if (statement.dataWriteOperations().nodeRemoveLabel(node, labelId)) count + 1 else count
  }

  def getNodesByLabel(id: Int): Iterator[Node] =
    mapToScala(statement.readOperations().nodesGetForLabel(id))(nodeOps.getById)

  def nodeGetDegree(node: Long, dir: Direction): Int = statement.readOperations().nodeGetDegree(node, dir)

  def nodeGetDegree(node: Long, dir: Direction, relTypeId: Int): Int = statement.readOperations().nodeGetDegree(node, dir, relTypeId)

  private def kernelStatement: KernelStatement =
    txBridge
      .getKernelTransactionBoundToThisThread(true)
      .acquireStatement()
      .asInstanceOf[KernelStatement]

  class NodeOperations extends BaseOperations[Node] {
    def delete(obj: Node) {
      try {
        statement.dataWriteOperations().nodeDelete(obj.getId)
      } catch {
        case _: exceptions.EntityNotFoundException => // node has been deleted by another transaction, oh well...
      }
    }

    def propertyKeyIds(id: Long): Iterator[Int] =
      asScala(statement.readOperations().nodeGetPropertyKeys(id))

    def getProperty(id: Long, propertyKeyId: Int): Any = {
      statement.readOperations().nodeGetProperty(id, propertyKeyId)
    }

    def hasProperty(id: Long, propertyKey: Int) =
      statement.readOperations().nodeHasProperty(id, propertyKey)

    def removeProperty(id: Long, propertyKeyId: Int) {
      statement.dataWriteOperations().nodeRemoveProperty(id, propertyKeyId)
    }

    def setProperty(id: Long, propertyKeyId: Int, value: Any) {
      statement.dataWriteOperations().nodeSetProperty(id, properties.Property.property(propertyKeyId, value) )
    }

    def getById(id: Long) = try {
      graph.getNodeById(id)
    } catch {
      case e: NotFoundException => throw new EntityNotFoundException(s"Node with id $id", e)
    }

    def all: Iterator[Node] = GlobalGraphOperations.at(graph).getAllNodes.iterator().asScala

    def indexGet(name: String, key: String, value: Any): Iterator[Node] =
      graph.index.forNodes(name).get(key, value).iterator().asScala

    def indexQuery(name: String, query: Any): Iterator[Node] =
      graph.index.forNodes(name).query(query).iterator().asScala

    def isDeleted(n: Node): Boolean =
      kernelStatement.hasTxStateWithChanges && kernelStatement.txState().nodeIsDeletedInThisTx(n.getId)
  }

  class RelationshipOperations extends BaseOperations[Relationship] {
    def delete(obj: Relationship) {
      try {
        statement.dataWriteOperations().relationshipDelete(obj.getId)
      } catch {
        case _: exceptions.EntityNotFoundException => // relationship has been deleted by another transaction, oh well...
      }
    }

    def propertyKeyIds(id: Long): Iterator[Int] =
      asScala(statement.readOperations().relationshipGetPropertyKeys(id))

    def getProperty(id: Long, propertyKeyId: Int): Any =
      statement.readOperations().relationshipGetProperty(id, propertyKeyId)

    def hasProperty(id: Long, propertyKey: Int) =
      statement.readOperations().relationshipHasProperty(id, propertyKey)

    def removeProperty(id: Long, propertyKeyId: Int) {
      statement.dataWriteOperations().relationshipRemoveProperty(id, propertyKeyId)
    }

    def setProperty(id: Long, propertyKeyId: Int, value: Any) {
      statement.dataWriteOperations().relationshipSetProperty(id, properties.Property.property(propertyKeyId, value) )
    }

    def getById(id: Long) = try {
      graph.getRelationshipById(id)
    } catch {
      case e: NotFoundException => throw new EntityNotFoundException(s"Relationship with id $id", e)
    }

    def all: Iterator[Relationship] =
      GlobalGraphOperations.at(graph).getAllRelationships.iterator().asScala

    def indexGet(name: String, key: String, value: Any): Iterator[Relationship] =
      graph.index.forRelationships(name).get(key, value).iterator().asScala

    def indexQuery(name: String, query: Any): Iterator[Relationship] =
      graph.index.forRelationships(name).query(query).iterator().asScala

    def isDeleted(r: Relationship): Boolean =
      kernelStatement.hasTxStateWithChanges && kernelStatement.txState().relationshipIsDeletedInThisTx(r.getId)
  }

  def getOrCreatePropertyKeyId(propertyKey: String) =
    statement.tokenWriteOperations().propertyKeyGetOrCreateForName(propertyKey)

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
    def primitiveLongIteratorToScalaIterator(primitiveIterator: PrimitiveLongIterator): Iterator[Long] =
      new Iterator[Long] {
        def hasNext: Boolean = primitiveIterator.hasNext

        def next(): Long = primitiveIterator.next
      }
  }

  def getOrCreateFromSchemaState[K, V](key: K, creator: => V) = {
    val javaCreator = new org.neo4j.function.Function[K, V]() {
      def apply(key: K) = creator
    }
    statement.readOperations().schemaStateGetOrCreate(key, javaCreator)
  }

  def addIndexRule(labelId: Int, propertyKeyId: Int): IdempotentResult[IndexDescriptor] = try {
    IdempotentResult(statement.schemaWriteOperations().indexCreate(labelId, propertyKeyId))
  } catch {
    case _: AlreadyIndexedException =>
      val indexDescriptor = statement.readOperations().indexesGetForLabelAndPropertyKey(labelId, propertyKeyId)
      if(statement.readOperations().indexGetState(indexDescriptor) == InternalIndexState.FAILED)
        throw new FailedIndexException(indexDescriptor.userDescription(tokenNameLookup))
     IdempotentResult(indexDescriptor, wasCreated = false)
  }

  def dropIndexRule(labelId: Int, propertyKeyId: Int) =
    statement.schemaWriteOperations().indexDrop(new IndexDescriptor(labelId, propertyKeyId))

  def createUniqueConstraint(labelId: Int, propertyKeyId: Int): IdempotentResult[UniquenessConstraint] = try {
    IdempotentResult(statement.schemaWriteOperations().uniquePropertyConstraintCreate(labelId, propertyKeyId))
  } catch {
    case existing: AlreadyConstrainedException =>
      IdempotentResult(existing.constraint().asInstanceOf[UniquenessConstraint], wasCreated = false)
  }

  def dropUniqueConstraint(labelId: Int, propertyKeyId: Int) =
    statement.schemaWriteOperations().constraintDrop(new UniquenessConstraint(labelId, propertyKeyId))

  override def getImportURL(url: URL): Either[String,URL] = graph match {
    case db: GraphDatabaseAPI =>
      try {
        Right(db.validateURLAccess(url))
      } catch {
        case error: URLAccessValidationError => Left(error.getMessage)
      }
  }

  def relationshipStartNode(rel: Relationship) = rel.getStartNode

  def relationshipEndNode(rel: Relationship) = rel.getEndNode

  private val tokenNameLookup = new StatementTokenNameLookup(statement.readOperations())

  override def commitAndRestartTx() {
    tx.success()
    tx.close()

    tx = graph.beginTx()
    statement = txBridge.get()
  }
}
