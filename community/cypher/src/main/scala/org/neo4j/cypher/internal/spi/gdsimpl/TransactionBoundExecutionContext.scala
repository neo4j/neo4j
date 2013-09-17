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
import org.neo4j.kernel.impl.api.index.IndexDescriptor
import org.neo4j.kernel.{GraphDatabaseAPI, ThreadToStatementContextBridge}
import collection.JavaConverters._
import collection.mutable
import scala.collection.Iterator
import org.neo4j.graphdb.DynamicRelationshipType._
import org.neo4j.cypher.internal.helpers.JavaConversionSupport
import org.neo4j.cypher.internal.helpers.JavaConversionSupport._
import org.neo4j.kernel.api._
import org.neo4j.cypher.EntityNotFoundException
import org.neo4j.tooling.GlobalGraphOperations
import org.neo4j.kernel.impl.api.PrimitiveLongIterator
import org.neo4j.kernel.api.constraints.UniquenessConstraint

class TransactionBoundExecutionContext(graph: GraphDatabaseAPI, tx: Transaction, statement: Statement)
  extends TransactionBoundTokenContext(statement) with QueryContext {

  private var open = true

  def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) => if (statement.dataWriteOperations().nodeAddLabel(node, labelId)) count + 1 else count
  }

  def close(success: Boolean) {
    try {
      statement.close()

      if (success)
        tx.success()
      else
        tx.failure()
      tx.close()
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
        val bridge = graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])
        val otherStatement   = bridge.statement()
        val result   = try {
          work(new TransactionBoundExecutionContext(graph, tx, otherStatement))
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

  def getLabelsForNode(node: Long) =
    JavaConversionSupport.asScala( statement.readOperations().nodeGetLabels(node) )

  override def isLabelSetOnNode(label: Int, node: Long) =
    statement.readOperations().nodeHasLabel(node, label)

  def getOrCreateLabelId(labelName: String) =
    statement.tokenWriteOperations().labelGetOrCreateForName(labelName)

  def getRelationshipsFor(node: Node, dir: Direction, types: Seq[String]): Iterator[Relationship] = types match {
    case Seq() => node.getRelationships(dir).iterator().asScala
    case _     => node.getRelationships(dir, types.map(withName): _*).iterator().asScala
  }

  def exactIndexSearch(index: IndexDescriptor, value: Any) =
    mapToScala( statement.readOperations().nodesGetFromIndexLookup(index, value) )(nodeOps.getById)

  def exactUniqueIndexSearch(index: IndexDescriptor, value: Any): Option[Node] = {
    val nodeId: Long = statement.readOperations().nodeGetUniqueFromIndexLookup(index, value)
    if (StatementConstants.NO_SUCH_NODE == nodeId) None else Some(nodeOps.getById(nodeId))
  }

  val nodeOps = new NodeOperations

  val relationshipOps = new RelationshipOperations

  def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int = labelIds.foldLeft(0) {
    case (count, labelId) =>
      if (statement.dataWriteOperations().nodeRemoveLabel(node, labelId)) count + 1 else count
  }

  def getNodesByLabel(id: Int): Iterator[Node] =
    mapToScala( statement.readOperations().nodesGetForLabel(id) )(nodeOps.getById)

  class NodeOperations extends BaseOperations[Node] {
    def delete(obj: Node) {
      statement.dataWriteOperations().nodeDelete(obj.getId)
    }

    def propertyKeyIds(obj: Node): Iterator[Int] =
      statement.readOperations().nodeGetAllProperties(obj.getId).asScala.map(_.propertyKeyId())

    def getProperty(obj: Node, propertyKeyId: Int): Any = {
      statement.readOperations().nodeGetProperty(obj.getId, propertyKeyId).value(null)
    }

    def hasProperty(obj: Node, propertyKey: Int) =
      statement.readOperations().nodeGetProperty(obj.getId, propertyKey).isDefined

    def removeProperty(obj: Node, propertyKeyId: Int) {
      statement.dataWriteOperations().nodeRemoveProperty(obj.getId, propertyKeyId)
    }

    def setProperty(obj: Node, propertyKeyId: Int, value: Any) {
      statement.dataWriteOperations().nodeSetProperty(obj.getId, properties.Property.property(propertyKeyId, value) )
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
      statement.dataWriteOperations().relationshipDelete(obj.getId)
    }

    def propertyKeyIds(obj: Relationship): Iterator[Int] =
      statement.readOperations().relationshipGetAllProperties(obj.getId).asScala.map(_.propertyKeyId())

    def getProperty(obj: Relationship, propertyKeyId: Int): Any =
      statement.readOperations().relationshipGetProperty(obj.getId, propertyKeyId).value(null)

    def hasProperty(obj: Relationship, propertyKey: Int) =
      statement.readOperations().relationshipGetProperty(obj.getId, propertyKey).isDefined

    def removeProperty(obj: Relationship, propertyKeyId: Int) {
      statement.dataWriteOperations().relationshipRemoveProperty(obj.getId, propertyKeyId)
    }

    def setProperty(obj: Relationship, propertyKeyId: Int, value: Any) {
      statement.dataWriteOperations().relationshipSetProperty(obj.getId, properties.Property.property(propertyKeyId, value) )
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
    val javaCreator = new org.neo4j.helpers.Function[K, V]() {
      def apply(key: K) = creator
    }
    statement.readOperations().schemaStateGetOrCreate(key, javaCreator)
  }

  def addIndexRule(labelId: Int, propertyKeyId: Int) =
    statement.schemaWriteOperations().indexCreate(labelId, propertyKeyId)

  def dropIndexRule(labelId: Int, propertyKeyId: Int) =
    statement.schemaWriteOperations().indexDrop(new IndexDescriptor(labelId, propertyKeyId))

  def createUniqueConstraint(labelId: Int, propertyKeyId: Int) =
    statement.schemaWriteOperations().uniquenessConstraintCreate(labelId, propertyKeyId)

  def dropUniqueConstraint(labelId: Int, propertyKeyId: Int) =
    statement.schemaWriteOperations().constraintDrop(new UniquenessConstraint(labelId, propertyKeyId))
}
