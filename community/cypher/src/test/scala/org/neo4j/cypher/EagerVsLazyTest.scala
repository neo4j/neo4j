/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher

import org.scalatest.Assertions
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.test.ImpermanentGraphDatabase
import org.neo4j.graphdb.event.{KernelEventHandler, TransactionEventHandler}
import org.junit.{Test, Before}
import org.junit.Assert._

class EagerVsLazyTest extends Assertions {
  var graph: SnitchGraphDatabaseService = _

  @Test
  def should_be_lazy_reading_data() {
    graph.getNodeById_Do(() => fail("This should be lazy"))

    execute("start n=node(0) return n")
  }

  @Test
  def should_be_eager_writing_data() {
    var created = false
    graph.createNode_Do(() => created = true)

    execute("create ({})")

    assertTrue("This should be eager", created)
  }

  private def execute(query: String): ExecutionResult = {
    val engine = new ExecutionEngine(graph)
    engine.execute(query)
  }

  @Before
  def init() {
    graph = new SnitchGraphDatabaseService(new ImpermanentGraphDatabase())
  }
}

class SnitchGraphDatabaseService(inner: GraphDatabaseService) extends GraphDatabaseService {

  def beginTx() = inner.beginTx()

  def createNode_Do(f: () => Unit) {
    createNodeListeners = f
  }
  var createNodeListeners: () => Unit = null

  def createNode() = {
    createNodeListeners()
    inner.createNode()
  }

  def getAllNodes = inner.getAllNodes

  def getNodeById_Do(f: () => Unit) {
    getNodeByIdListeners = f
  }

  var getNodeByIdListeners: () => Unit = null

  def getNodeById(id: Long) = {
    getNodeByIdListeners()
    inner.getNodeById(id)
  }

  def getReferenceNode = inner.getReferenceNode

  def getRelationshipById(id: Long) = inner.getRelationshipById(id)

  def getRelationshipTypes = inner.getRelationshipTypes

  def index() = inner.index()

  def registerKernelEventHandler(handler: KernelEventHandler) = inner.registerKernelEventHandler(handler)

  def registerTransactionEventHandler[T](handler: TransactionEventHandler[T]) = inner.registerTransactionEventHandler(handler)

  def shutdown() {
    inner.shutdown()
  }

  def unregisterKernelEventHandler(handler: KernelEventHandler) = inner.unregisterKernelEventHandler(handler)

  def unregisterTransactionEventHandler[T](handler: TransactionEventHandler[T]) = inner.unregisterTransactionEventHandler(handler)
}