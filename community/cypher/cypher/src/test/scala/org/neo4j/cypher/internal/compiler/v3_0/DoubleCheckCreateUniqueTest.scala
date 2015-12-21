/*
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
package org.neo4j.cypher.internal.compiler.v3_0

import java.lang.Iterable
import java.net.URL
import java.util

import org.neo4j.cypher.internal.compiler.v3_0.mutation.{CreateUniqueAction, UniqueLink}
import org.neo4j.cypher.internal.compiler.v3_0.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v3_0.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Traverser.Order
import org.neo4j.graphdb._
import org.neo4j.graphdb.event.{KernelEventHandler, TransactionEventHandler}
import org.neo4j.graphdb.index.IndexManager
import org.neo4j.graphdb.schema.Schema
import org.neo4j.graphdb.traversal.{BidirectionalTraversalDescription, TraversalDescription}
import org.neo4j.kernel.impl.GraphDatabaseAPI
import org.neo4j.kernel.impl.store.StoreId
import org.neo4j.test.TestGraphDatabaseFactory

import scala.collection.JavaConverters._

/*
This test tries to set up a situation where CREATE UNIQUE would fail, unless we guard with locks to prevent creating
multiple relationships.

It does so by using a decorator around ImpermanentGraphDatabase, so directly after CREATE UNIQUE has done
getRelationships on a node, we'll create a new relationship.
*/

class DoubleCheckCreateUniqueTest extends CypherFunSuite {
  var done = false
  val db = new Wrapper(new TestGraphDatabaseFactory().newImpermanentDatabase().asInstanceOf[GraphDatabaseAPI])
  var tx:Transaction = null

  test("double_check_unique") {
    //GIVEN
    db.afterGetRelationship = createRel
    val a = createNode()

    withQueryState { state =>
      //WHEN we create a relationship just after seeing an empty iterable
      relateAction.exec(createExecutionContext(state, a), state)

      //THEN we double-check, and don't create a second rel
      a.getRelationships.asScala should have size 1
    }
  }

  val relateAction = CreateUniqueAction(UniqueLink("a", "b", "r", "X", SemanticDirection.OUTGOING))

  private def createExecutionContext(state:QueryState, a: Node): ExecutionContext = {
    ExecutionContext().newWith(Map("a" -> a))
  }

  private def withQueryState(f: QueryState => Unit) {
    val tx = db.beginTx()
    f(QueryStateHelper.queryStateFrom(db, tx))
    tx.close()
  }

  private def createNode(): Node = {
    val tx = db.beginTx()
    try {
      val n = db.createNode()
      tx.success()
      n
    } finally {
      tx.close()
    }
  }

  private def createRel(node: Node) {
    if (!done) {
      done = true
      val x = db.createNode()
      node.createRelationshipTo(x, RelationshipType.withName("X"))
    }
  }
}

class Wrapper(delegate: GraphDatabaseAPI) extends GraphDatabaseAPI
{
  var afterGetRelationship: Node => Unit = (n) => {}

  override def createNode(): Node = {
    val n = delegate.createNode()
    new PausingNode(n, afterGetRelationship)
  }

  override def createNode(labels: Label*): Node = delegate.createNode(labels:_*)

  override def shutdown(): Unit = delegate.shutdown()

  override def findNodesByLabelAndProperty(label: Label, key: String, value: scala.Any): ResourceIterable[Node] =
    delegate.findNodesByLabelAndProperty(label, key, value)

  override def registerKernelEventHandler(handler: KernelEventHandler): KernelEventHandler =
    delegate.registerKernelEventHandler(handler)

  override def unregisterTransactionEventHandler[T](handler: TransactionEventHandler[T]): TransactionEventHandler[T] =
    delegate.unregisterTransactionEventHandler(handler)

  override def getAllNodes: ResourceIterable[Node] = delegate.getAllNodes

  override def getRelationshipById(id: Long): Relationship = delegate.getRelationshipById(id)

  override def findNode(label: Label, key: String, value: scala.Any): Node = findNode(label, key, value)

  override def execute(query: String): Result = delegate.execute(query)

  override def execute(query: String, parameters: util.Map[String, AnyRef]): Result =
    delegate.execute(query, parameters)

  override def unregisterKernelEventHandler(handler: KernelEventHandler): KernelEventHandler =
    delegate.unregisterKernelEventHandler(handler)

  override def registerTransactionEventHandler[T](handler: TransactionEventHandler[T]): TransactionEventHandler[T] =
    delegate.registerTransactionEventHandler(handler)

  override def getAllRelationshipTypes: ResourceIterable[RelationshipType] = delegate.getAllRelationshipTypes

  override def getAllRelationships: ResourceIterable[Relationship] = delegate.getAllRelationships

  override def getAllPropertyKeys: ResourceIterable[String] = delegate.getAllPropertyKeys

  override def getAllLabels: ResourceIterable[Label] = delegate.getAllLabels

  override def traversalDescription(): TraversalDescription = delegate.traversalDescription()

  override def index(): IndexManager = delegate.index()

  override def getNodeById(id: Long): Node = delegate.getNodeById(id)

  override def isAvailable(timeout: Long): Boolean = delegate.isAvailable(timeout)

  override def schema(): Schema = delegate.schema()

  override def findNodes(label: Label, key: String, value: scala.Any): ResourceIterator[Node] =
    delegate.findNodes(label, key, value)

  override def findNodes(label: Label): ResourceIterator[Node] = delegate.findNodes(label)

  override def bidirectionalTraversalDescription(): BidirectionalTraversalDescription =
    delegate.bidirectionalTraversalDescription()

  override def beginTx(): Transaction = delegate.beginTx()

  override def getDependencyResolver: DependencyResolver = delegate.getDependencyResolver

  override def storeId(): StoreId = delegate.storeId()

  override def validateURLAccess(url: URL): URL = delegate.validateURLAccess(url)

  override def getStoreDir: String = delegate.getStoreDir
}

class PausingNode(n: Node, afterGetRelationship: Node => Unit) extends Node {
  def getId: Long = n.getId

  def delete() {
    ???
  }

  def getRelationships: Iterable[Relationship] = {
    val rels = n.getRelationships.asScala.toList
    afterGetRelationship(n)
    rels.toIterable.asJava
  }

  def hasRelationship: Boolean = ???

  def getRelationships(types: RelationshipType*): Iterable[Relationship] = ???

  def getRelationships(direction: Direction, types: RelationshipType*): Iterable[Relationship] = getRelationships

  def hasRelationship(types: RelationshipType*): Boolean = ???

  def hasRelationship(direction: Direction, types: RelationshipType*): Boolean = ???

  def getRelationships(dir: Direction): Iterable[Relationship] = ???

  def hasRelationship(dir: Direction): Boolean = ???

  def getRelationships(`type`: RelationshipType, dir: Direction): Iterable[Relationship] = {
    val rels = n.getRelationships(`type`, dir).asScala.toList
    afterGetRelationship(n)
    rels.toIterable.asJava
  }


  def hasRelationship(`type`: RelationshipType, dir: Direction): Boolean = ???

  def getSingleRelationship(`type`: RelationshipType, dir: Direction): Relationship = ???

  def createRelationshipTo(otherNode: Node, `type`: RelationshipType): Relationship = {
    n.createRelationshipTo(otherNode, `type`)
  }

  def traverse(traversalOrder: Order, stopEvaluator: StopEvaluator, returnableEvaluator: ReturnableEvaluator, relationshipType: RelationshipType, direction: Direction): Traverser = ???

  def traverse(traversalOrder: Order, stopEvaluator: StopEvaluator, returnableEvaluator: ReturnableEvaluator, firstRelationshipType: RelationshipType, firstDirection: Direction, secondRelationshipType: RelationshipType, secondDirection: Direction): Traverser = ???

  def traverse(traversalOrder: Order, stopEvaluator: StopEvaluator, returnableEvaluator: ReturnableEvaluator, relationshipTypesAndDirections: AnyRef*): Traverser = ???

  def getGraphDatabase: GraphDatabaseService = ???

  def hasProperty(key: String): Boolean = ???

  def getProperty(key: String): AnyRef = ???

  def getProperty(key: String, defaultValue: Any): AnyRef = ???

  def setProperty(key: String, value: Any) {
    ???
  }

  def removeProperty(key: String): AnyRef = ???

  def getPropertyKeys: Iterable[String] = ???

  def getProperties( keys: String* ): util.Map[String, AnyRef] = ???

  def getAllProperties: util.Map[String, AnyRef] = ???

  def addLabel(label: Label) {
    ???
  }

  def removeLabel(label: Label) {
    ???
  }

  def hasLabel(label: Label) = ???

  def getLabels = ???

  def getRelationshipTypes = ???

  def getDegree:Int = ???

  def getDegree( direction:Direction ):Int = ???

  def getDegree( relType:RelationshipType ):Int = ???

  def getDegree( relType:RelationshipType, direction:Direction ):Int = ???
}
