/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.neo4j.cypher.internal.compiler.v2_1.ExecutionContext
import org.neo4j.graphdb.{Relationship, Direction, Node}
import org.neo4j.cypher.internal.compiler.v2_1.symbols._
import org.neo4j.cypher.internal.compiler.v2_1.spi.QueryContext
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock

class VarLengthExpandPipeTest extends CypherFunSuite {

  implicit val monitor = mock[PipeMonitor]

  test("should support var length expand between two nodes") {
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
    val result = VarLengthExpandPipe(left, "a", "r", "b", Direction.OUTGOING, Seq.empty, 1, None).createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single("a") should equal(startNode)
    single("r") should equal(List(relationship))
    single("b") should equal(endNode)
  }

  test("should support var length expand between three nodes") {
    // given
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)
    val leftRelationship = newMockedRealtionship(1, startNode, middleNode)
    val rightRelationship = newMockedRealtionship(2, middleNode, endNode)

    val query = mock[QueryContext]
    replyWithMap(query, Map(
        (startNode, Direction.OUTGOING) -> Seq(leftRelationship),
        (middleNode, Direction.INCOMING) -> Seq(leftRelationship),
        (middleNode, Direction.OUTGOING) -> Seq(rightRelationship),
        (endNode, Direction.INCOMING) -> Seq(rightRelationship)
      ).withDefaultValue(Seq.empty)
    )
    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("a" -> startNode)))

    // when
    val result = VarLengthExpandPipe(left, "a", "r", "b", Direction.OUTGOING, Seq.empty, 1, None).createResults(queryState).toList

    // then
    val (first :: second :: Nil) = result
    first("a") should equal(startNode)
    first("r") should equal(List(leftRelationship))
    first("b") should equal(middleNode)
    second("a") should equal(startNode)
    second("r") should equal(List(leftRelationship, rightRelationship))
    second("b") should equal(endNode)
  }

  test("should support var length expand between three nodes with multiple relationships") {
    // given
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)
    val leftRelationship1 = newMockedRealtionship(1, startNode, middleNode)
    val leftRelationship2 = newMockedRealtionship(2, startNode, middleNode)
    val rightRelationship = newMockedRealtionship(3, middleNode, endNode)

    val query = mock[QueryContext]
    replyWithMap(query, Map(
        (startNode, Direction.OUTGOING) -> Seq(leftRelationship1, leftRelationship2),
        (middleNode, Direction.INCOMING) -> Seq(leftRelationship1, leftRelationship2),
        (middleNode, Direction.OUTGOING) -> Seq(rightRelationship),
        (endNode, Direction.INCOMING) -> Seq(rightRelationship)
      ).withDefaultValue(Seq.empty)
    )
    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("a" -> startNode)))

    // when
    val result = VarLengthExpandPipe(left, "a", "r", "b", Direction.OUTGOING, Seq.empty, 1, None).createResults(queryState).toList

    // then
    val (first :: second :: third :: fourth :: Nil) = result
    first("a") should equal(startNode)
    first("r") should equal(List(leftRelationship2))
    first("b") should equal(middleNode)
    second("a") should equal(startNode)
    second("r") should equal(List(leftRelationship2, rightRelationship))
    second("b") should equal(endNode)
    third("a") should equal(startNode)
    third("r") should equal(List(leftRelationship1))
    third("b") should equal(middleNode)
    fourth("a") should equal(startNode)
    fourth("r") should equal(List(leftRelationship1, rightRelationship))
    fourth("b") should equal(endNode)
  }

  test("should support var length expand between three nodes with multiple relationships and a fixed max length") {
    // given
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)
    val leftRelationship1 = newMockedRealtionship(1, startNode, middleNode)
    val leftRelationship2 = newMockedRealtionship(2, startNode, middleNode)
    val rightRelationship = newMockedRealtionship(3, middleNode, endNode)

    val query = mock[QueryContext]
    replyWithMap(query, Map(
        (startNode, Direction.OUTGOING) -> Seq(leftRelationship1, leftRelationship2),
        (middleNode, Direction.INCOMING) -> Seq(leftRelationship1, leftRelationship2),
        (middleNode, Direction.OUTGOING) -> Seq(rightRelationship),
        (endNode, Direction.INCOMING) -> Seq(rightRelationship)
      ).withDefaultValue(Seq.empty)
    )
    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("a" -> startNode)))

    // when
    val result = VarLengthExpandPipe(left, "a", "r", "b", Direction.OUTGOING, Seq.empty, 1, Some(1)).createResults(queryState).toList

    // then
    val (first :: second :: Nil) = result
    first("a") should equal(startNode)
    first("r") should equal(List(leftRelationship2))
    first("b") should equal(middleNode)
    second("a") should equal(startNode)
    second("r") should equal(List(leftRelationship1))
    second("b") should equal(middleNode)
  }

  test("should support var length expand between three nodes with multiple relationships and fixed min and max lengths") {
    // given
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)
    val leftRelationship1 = newMockedRealtionship(1, startNode, middleNode)
    val leftRelationship2 = newMockedRealtionship(2, startNode, middleNode)
    val rightRelationship = newMockedRealtionship(3, middleNode, endNode)

    val query = mock[QueryContext]
    replyWithMap(query, Map(
        (startNode, Direction.OUTGOING) -> Seq(leftRelationship1, leftRelationship2),
        (middleNode, Direction.INCOMING) -> Seq(leftRelationship1, leftRelationship2),
        (middleNode, Direction.OUTGOING) -> Seq(rightRelationship),
        (endNode, Direction.INCOMING) -> Seq(rightRelationship)
      ).withDefaultValue(Seq.empty)
    )
    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("a" -> startNode)))

    // when
    val result = VarLengthExpandPipe(left, "a", "r", "b", Direction.OUTGOING, Seq.empty, 2, Some(2)).createResults(queryState).toList

    // then
    val (first :: second :: Nil) = result
    first("a") should equal(startNode)
    first("r") should equal(List(leftRelationship2, rightRelationship))
    first("b") should equal(endNode)
    second("a") should equal(startNode)
    second("r") should equal(List(leftRelationship1, rightRelationship))
    second("b") should equal(endNode)
  }

  test("should support var length expand between three nodes with multiple relationships and fixed min and max lengths 2") {
    // given
    val firstNode = newMockedNode(4)
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)
    val initialRelationship = newMockedRealtionship(4, firstNode, startNode)
    val leftRelationship1 = newMockedRealtionship(1, startNode, middleNode)
    val leftRelationship2 = newMockedRealtionship(2, startNode, middleNode)
    val rightRelationship = newMockedRealtionship(3, middleNode, endNode)

    val query = mock[QueryContext]
    replyWithMap(query, Map(
        (firstNode, Direction.OUTGOING) -> Seq(initialRelationship),
        (startNode, Direction.INCOMING) -> Seq(initialRelationship),
        (startNode, Direction.OUTGOING) -> Seq(leftRelationship1, leftRelationship2),
        (middleNode, Direction.INCOMING) -> Seq(leftRelationship1, leftRelationship2),
        (middleNode, Direction.OUTGOING) -> Seq(rightRelationship),
        (endNode, Direction.INCOMING) -> Seq(rightRelationship)
      ).withDefaultValue(Seq.empty)
    )
    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("a" -> firstNode)))

    // when
    val result = VarLengthExpandPipe(left, "a", "r", "b", Direction.OUTGOING, Seq.empty, 2, Some(3)).createResults(queryState).toList

    // then
    val (first :: second :: third :: fourth :: Nil) = result
    first("a") should equal(firstNode)
    first("r") should equal(List(initialRelationship, leftRelationship2))
    first("b") should equal(middleNode)
    second("a") should equal(firstNode)
    second("r") should equal(List(initialRelationship, leftRelationship2, rightRelationship))
    second("b") should equal(endNode)
    third("a") should equal(firstNode)
    third("r") should equal(List(initialRelationship, leftRelationship1))
    third("b") should equal(middleNode)
    fourth("a") should equal(firstNode)
    fourth("r") should equal(List(initialRelationship, leftRelationship1, rightRelationship))
    fourth("b") should equal(endNode)
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

  private def replyWithMap(query: QueryContext, mapping: Map[(Node, Direction), Seq[Relationship]]) {
    when(query.getRelationshipsFor(any(), any(), any())).thenAnswer(new Answer[Iterator[Relationship]] {
      def answer(invocation: InvocationOnMock): Iterator[Relationship] = {
        val (startNode :: dir :: _ :: Nil) = invocation.getArguments.toList
        mapping((startNode.asInstanceOf[Node], dir.asInstanceOf[Direction])).iterator
      }
    })
  }

  private def newMockedPipe(symbolTable: SymbolTable): Pipe = {
    val pipe = mock[Pipe]
    when(pipe.sources).thenReturn(Seq.empty)
    when(pipe.symbols).thenReturn(symbolTable)
    pipe
  }
}
