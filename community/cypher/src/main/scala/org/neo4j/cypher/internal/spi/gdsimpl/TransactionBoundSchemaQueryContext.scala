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
import collection.mutable
import org.neo4j.kernel.impl.api.index.IndexDescriptor
import scala.collection.Iterator
import org.neo4j.kernel.api.constraints.UniquenessConstraint
import org.neo4j.kernel.api.SchemaStatement

class TransactionBoundSchemaQueryContext(graph: GraphDatabaseAPI, tx: Transaction, statement: SchemaStatement)
  extends TransactionBoundTokenContext(statement) with QueryContext {

  private var open = true

  def close(success: Boolean) {
    try {
      statement.close()

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
        val stmCtx   = bridge.schemaStatement()
        val result   = try {
          work(new TransactionBoundSchemaQueryContext(graph, tx, stmCtx))
        }
        finally {
          stmCtx.close()
        }
        tx.success()
        result
      }
      finally {
        tx.finish()
      }
    }
  }

  def addIndexRule(labelIds: Long, propertyKeyId: Long) =
    statement.indexCreate(labelIds, propertyKeyId)

  def dropIndexRule(labelId: Long, propertyKeyId: Long) =
    statement.indexDrop(new IndexDescriptor(labelId, propertyKeyId))

  def createUniqueConstraint(labelId: Long, propertyKeyId: Long) =
    statement.uniquenessConstraintCreate(labelId, propertyKeyId)

  def dropUniqueConstraint(labelId: Long, propertyKeyId: Long) =
    statement.constraintDrop(new UniquenessConstraint(labelId, propertyKeyId))

  def getOrCreateLabelId(labelName: String) =
    statement.labelGetOrCreateForName(labelName)

  def getOrCreatePropertyKeyId(propertyKey: String) =
    statement.propertyKeyGetOrCreateForName(propertyKey)

  def upgrade(context: QueryContext): LockingQueryContext = new RepeatableReadQueryContext(context, new Locker {
    private val locks = new mutable.ListBuffer[Lock]

    def releaseAllLocks() {
      locks.foreach(_.release())
    }

    def acquireLock(p: PropertyContainer) {
      locks += tx.acquireWriteLock(p)
    }
  })

  def getOrCreateFromSchemaState[K, V](key: K, creator: => V) = {
    val javaCreator = new org.neo4j.helpers.Function[K, V]() {
      def apply(key: K) = creator
    }
    statement.schemaStateGetOrCreate(key, javaCreator)
  }

  def getTransaction = tx

  val nodeOps = new NodeOperations

  val relationshipOps = new RelationshipOperations

  def illegalOperation() = new UnsupportedOperationException("Should not perform data operations in a schema query.")

  class NodeOperations extends Operations[Node] {
    def delete(obj: Node) = throw illegalOperation()

    def setProperty(obj: Node, propertyKeyId: Long, value: Any) = throw illegalOperation()

    def removeProperty(obj: Node, propertyKeyId: Long) = throw illegalOperation()

    def getProperty(obj: Node, propertyKeyId: Long): Any = throw illegalOperation()

    def hasProperty(obj: Node, propertyKeyId: Long): Boolean = throw illegalOperation()

    def propertyKeyIds(obj: Node): Iterator[Long] = throw illegalOperation()

    def getById(id: Long): Node = throw illegalOperation()

    def indexGet(name: String, key: String, value: Any): Iterator[Node] = throw illegalOperation()

    def indexQuery(name: String, query: Any): Iterator[Node] = throw illegalOperation()

    def all: Iterator[Node] = throw illegalOperation()

    def propertyKeys(obj: Node): Iterator[String] = throw illegalOperation()
  }

  class RelationshipOperations extends Operations[Relationship] {
    def delete(obj: Relationship) = throw illegalOperation()

    def setProperty(obj: Relationship, propertyKeyId: Long, value: Any) = throw illegalOperation()

    def removeProperty(obj: Relationship, propertyKeyId: Long) = throw illegalOperation()

    def getProperty(obj: Relationship, propertyKeyId: Long): Any = throw illegalOperation()

    def hasProperty(obj: Relationship, propertyKeyId: Long): Boolean = throw illegalOperation()

    def propertyKeyIds(obj: Relationship): Iterator[Long] = throw illegalOperation()

    def getById(id: Long): Relationship = throw illegalOperation()

    def indexGet(name: String, key: String, value: Any): Iterator[Relationship] = throw illegalOperation()

    def indexQuery(name: String, query: Any): Iterator[Relationship] = throw illegalOperation()

    def all: Iterator[Relationship] = throw illegalOperation()

    def propertyKeys(obj: Relationship): Iterator[String] = throw illegalOperation()
  }

  def createNode(): Node = throw illegalOperation()

  def createRelationship(start: Node, end: Node, relType: String): Relationship = throw illegalOperation()

  def getRelationshipsFor(node: Node, dir: Direction, types: Seq[String]): Iterator[Relationship] = throw illegalOperation()

  def getLabelsForNode(node: Long): Iterator[Long] = throw illegalOperation()

  def setLabelsOnNode(node: Long, labelIds: Iterator[Long]): Int = throw illegalOperation()

  def removeLabelsFromNode(node: Long, labelIds: Iterator[Long]): Int = throw illegalOperation()

  def exactIndexSearch(index: IndexDescriptor, value: Any): Iterator[Node] = throw illegalOperation()

  def getNodesByLabel(id: Long): Iterator[Node] = throw illegalOperation()
}