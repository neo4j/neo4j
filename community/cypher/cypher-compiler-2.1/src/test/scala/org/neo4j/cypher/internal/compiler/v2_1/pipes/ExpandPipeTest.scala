/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.pipes

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.spi.QueryContext
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.mockito.stubbing.Answer
import org.neo4j.graphdb.{Node, Direction, Relationship}
import org.mockito.invocation.InvocationOnMock
import org.neo4j.cypher.internal.compiler.v2_1.symbols._
import org.neo4j.cypher.internal.compiler.v2_1.ExecutionContext

class ExpandPipeTest extends CypherFunSuite {

  implicit val monitor = mock[PipeMonitor]

  test("should support expand between two nodes with a relationship") {
    // given
    val startNode = newMockedNode(1)
    val endNode = newMockedNode(2)
    val relationship = newMockedRealtionship(1, startNode, endNode)
    val query = mock[QueryContext]
    when(query.getRelationshipsFor(any(), any(), any())).thenAnswer(new Answer[Iterator[Relationship]] {
      def answer(invocation: InvocationOnMock): Iterator[Relationship] = Iterator(relationship)
    })

    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode)))
    when(left.createResults(queryState)).thenAnswer(new Answer[Iterator[ExecutionContext]] {
      def answer(invocation: InvocationOnMock): Iterator[ExecutionContext] = Iterator(row("a" -> startNode))
    })

    // when
    val result = ExpandPipe(left, "a", "r", "b", Direction.OUTGOING, Seq.empty).createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single("a") should equal(startNode)
    single("r") should equal(relationship)
    single("b") should equal(endNode)
  }

  test("should support expand between two nodes with multiple relationships") {
    // given
    val startNode = newMockedNode(1)
    val endNode1 = newMockedNode(2)
    val endNode2 = newMockedNode(3)
    val relationship1 = newMockedRealtionship(1, startNode, endNode1)
    val relationship2 = newMockedRealtionship(2, startNode, endNode2)
    val query = mock[QueryContext]
    when(query.getRelationshipsFor(any(), any(), any())).thenAnswer(new Answer[Iterator[Relationship]] {
      def answer(invocation: InvocationOnMock): Iterator[Relationship] = Iterator(relationship1, relationship2)
    })

    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode)))
    when(left.createResults(queryState)).thenAnswer(new Answer[Iterator[ExecutionContext]] {
      def answer(invocation: InvocationOnMock): Iterator[ExecutionContext] = Iterator(row("a" -> startNode))
    })

    // when
    val result = ExpandPipe(left, "a", "r", "b", Direction.OUTGOING, Seq.empty).createResults(queryState).toList

    // then
    val (first :: second :: Nil) = result
    first("a") should equal(startNode)
    first("r") should equal(relationship1)
    first("b") should equal(endNode1)
    second("a") should equal(startNode)
    second("r") should equal(relationship2)
    second("b") should equal(endNode2)
  }

  test("should support expand between two nodes with multiple relationships and selfloops") {
    // given
    val startNode = newMockedNode(1)
    val endNode = newMockedNode(2)
    val relationship1 = newMockedRealtionship(1, startNode, endNode)
    val relationship2 = newMockedRealtionship(2, startNode, startNode)
    val query = mock[QueryContext]
    when(query.getRelationshipsFor(any(), any(), any())).thenAnswer(new Answer[Iterator[Relationship]] {
      def answer(invocation: InvocationOnMock): Iterator[Relationship] = Iterator(relationship1, relationship2)
    })

    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode)))
    when(left.createResults(queryState)).thenAnswer(new Answer[Iterator[ExecutionContext]] {
      def answer(invocation: InvocationOnMock): Iterator[ExecutionContext] = Iterator(row("a" -> startNode))
    })

    // when
    val result = ExpandPipe(left, "a", "r", "b", Direction.OUTGOING, Seq.empty).createResults(queryState).toList

    // then
    val (first :: second :: Nil) = result
    first("a") should equal(startNode)
    first("r") should equal(relationship1)
    first("b") should equal(endNode)
    second("a") should equal(startNode)
    second("r") should equal(relationship2)
    second("b") should equal(startNode)
  }

  private def row(values: (String, Any)*) = ExecutionContext.from(values: _*)

  private def newMockedNode(id: Int) = {
    val node = mock[Node]
    when(node.getId).thenReturn(id)
    node
  }

  private def newMockedRealtionship(id: Int, startNode: Node, endNode: Node): Relationship = {
    val relationship = mock[Relationship]
    when(relationship.getId).thenReturn(id)
    when(relationship.getStartNode).thenReturn(startNode)
    when(relationship.getEndNode).thenReturn(endNode)
    when(relationship.getOtherNode(startNode)).thenReturn(endNode)
    when(relationship.getOtherNode(endNode)).thenReturn(startNode)
    relationship
  }

  private def newMockedPipe(symbolTable: SymbolTable): Pipe = {
    val pipe = mock[Pipe]
    when(pipe.sources).thenReturn(Seq.empty)
    when(pipe.symbols).thenReturn(symbolTable)
    pipe
  }
}
