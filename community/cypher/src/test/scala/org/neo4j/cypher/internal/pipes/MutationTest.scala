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
package org.neo4j.cypher.internal.pipes

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.cypher.internal.mutation.{CreateRelationship, CreateNode, DeleteEntityAction}
import org.neo4j.cypher.{CypherTypeException, ExecutionEngineHelper}
import collection.mutable.{Map => MutableMap}
import org.neo4j.graphdb.{Node, NotFoundException}
import org.neo4j.cypher.internal.symbols.{SymbolTable, CypherType, NodeType}
import org.neo4j.cypher.internal.commands.expressions.{Expression, Literal}
import org.neo4j.cypher.internal.spi.gdsimpl.GDSBackedQueryContext
import org.neo4j.cypher.internal.ExecutionContext

class MutationTest extends ExecutionEngineHelper with Assertions {

  def createQueryState = new QueryState(graph, new GDSBackedQueryContext(graph), Map.empty)

  @Test
  def create_node() {
    val start = new NullPipe
    val txBegin = new TransactionStartPipe(start, graph)
    val createNode = new ExecuteUpdateCommandsPipe(txBegin, graph, Seq(CreateNode("n", Map("name" -> Literal("Andres")))))

    val queryState = createQueryState
    createNode.createResults(queryState).toList


    val n = graph.getNodeById(1)
    assert(n.getProperty("name") === "Andres")
    assert(queryState.createdNodes.count === 1)
    assert(queryState.propertySet.count === 1)
  }

  @Test
  def join_existing_transaction_and_rollback() {
    val tx = graph.beginTx()
    val start = new NullPipe
    val txBegin = new TransactionStartPipe(start, graph)
    val createNode = new ExecuteUpdateCommandsPipe(txBegin, graph, Seq(CreateNode("n", Map("name" -> Literal("Andres")))))

    createNode.createResults(createQueryState).toList

    tx.failure()
    tx.finish()

    intercept[NotFoundException](graph.getNodeById(1))
  }

  @Test
  def join_existing_transaction_and_commit() {
    val tx = graph.beginTx()
    val start = new NullPipe
    val txBegin = new TransactionStartPipe(start, graph)
    val createNode = new ExecuteUpdateCommandsPipe(txBegin, graph, Seq(CreateNode("n", Map("name" -> Literal("Andres")))))

    createNode.createResults(createQueryState).toList

    tx.success()
    tx.finish()

    val n = graph.getNodeById(1)
    assert(n.getProperty("name") === "Andres")
  }

  private def getNode(key:String, n:Node) = InjectValue(n, NodeType())

  @Test
  def create_rel() {
    val a = createNode()
    val b = createNode()

    val createRel = CreateRelationship("r", (getNode("a", a),Map()), (getNode("b", b),Map()), "REL", Map("I" -> Literal("was here")))

    val startPipe = new NullPipe
    val txBeginPipe = new TransactionStartPipe(startPipe, graph)
    val createNodePipe = new ExecuteUpdateCommandsPipe(txBeginPipe, graph, Seq(createRel))

    val state = createQueryState
    val results: List[MutableMap[String, Any]] = createNodePipe.createResults(state).map(ctx => ctx.m).toList

    val r = graph.getRelationshipById(0)
    assert(r.getProperty("I") === "was here")
    assert(results === List(Map("r" -> r)))
    assert(state.createdRelationships.count === 1)
    assert(state.propertySet.count === 1)
  }

  @Test
  def throw_exception_if_wrong_stuff_to_delete() {
    val createRel = DeleteEntityAction(Literal("some text"))

    intercept[CypherTypeException](createRel.exec(ExecutionContext.empty, createQueryState))
  }

  @Test
  def delete_node() {
    val a: Node = createNode()
    val node_id: Long = a.getId
    val deleteCommand = DeleteEntityAction(getNode("a", a))

    val startPipe = new NullPipe
    val txBeginPipe = new TransactionStartPipe(startPipe, graph)
    val createNodePipe = new ExecuteUpdateCommandsPipe(txBeginPipe, graph, Seq(deleteCommand))

    val state = createQueryState
    createNodePipe.createResults(state).toList
    state.transaction.get.success()
    state.transaction.get.finish()

    intercept[NotFoundException](graph.getNodeById(node_id))
  }
}

case class InjectValue(value:Any, typ:CypherType) extends Expression {
  def apply(v1: ExecutionContext) = value

  def children = Seq()

  def rewrite(f: (Expression) => Expression) = this

  def calculateType(symbols: SymbolTable): CypherType = typ

  def symbolTableDependencies = Set()
}