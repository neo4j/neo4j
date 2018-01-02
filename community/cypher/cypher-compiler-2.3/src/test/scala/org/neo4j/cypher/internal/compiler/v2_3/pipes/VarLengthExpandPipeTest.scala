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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.{Node, Relationship}

class VarLengthExpandPipeTest extends CypherFunSuite {

  implicit val monitor = mock[PipeMonitor]

  test("should support var length expand between two nodes") {
    // given
    val startNode = newMockedNode(1)
    val endNode = newMockedNode(2)
    val relationship = newMockedRelationship(1, startNode, endNode)
    val query = mock[QueryContext]
    when(query.getRelationshipsForIds(any(), any(), any())).thenAnswer(new Answer[Iterator[Relationship]] {
      def answer(invocation: InvocationOnMock): Iterator[Relationship] = Iterator(relationship)
    })

    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode)))
    when(left.createResults(queryState)).thenAnswer(new Answer[Iterator[ExecutionContext]] {
      def answer(invocation: InvocationOnMock): Iterator[ExecutionContext] = Iterator(row("a" -> startNode))
    })

    // when
    val result =
      VarLengthExpandPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, LazyTypes.empty, 1, None, nodeInScope = false)()
      .createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single("a") should equal(startNode)
    single("r") should equal(List(relationship))
    single("b") should equal(endNode)
  }

  test("should support var length expand between two nodes when the end node is in scope") {
    // given
    val startNode = newMockedNode(1)
    val endNode = newMockedNode(2)
    val relationship = newMockedRelationship(1, startNode, endNode)
    val query = mock[QueryContext]
    when(query.getRelationshipsForIds(any(), any(), any())).thenAnswer(new Answer[Iterator[Relationship]] {
      def answer(invocation: InvocationOnMock): Iterator[Relationship] = Iterator(relationship)
    })

    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode, "b" -> CTNode)))
    when(left.createResults(queryState)).thenAnswer(new Answer[Iterator[ExecutionContext]] {
      def answer(invocation: InvocationOnMock): Iterator[ExecutionContext] = Iterator(row("a" -> startNode, "b" -> endNode))
    })

    // when
    val result =
      VarLengthExpandPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, LazyTypes.empty, 1, None, nodeInScope = true)()
      .createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single("a") should equal(startNode)
    single("r") should equal(List(relationship))
    single("b") should equal(endNode)
  }

  test("should support var length expand between two nodes when a mismatching end node is in scope") {
    // given
    val startNode = newMockedNode(1)
    val endNode = newMockedNode(2)
    val relationship = newMockedRelationship(1, startNode, endNode)
    val query = mock[QueryContext]
    when(query.getRelationshipsForIds(any(), any(), any())).thenAnswer(new Answer[Iterator[Relationship]] {
      def answer(invocation: InvocationOnMock): Iterator[Relationship] = Iterator(relationship)
    })

    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode, "b" -> CTNode)))
    when(left.createResults(queryState)).thenAnswer(new Answer[Iterator[ExecutionContext]] {
      def answer(invocation: InvocationOnMock): Iterator[ExecutionContext] = Iterator(row("a" -> startNode, "b" -> newMockedNode(42)))
    })

    // when
    val result =
      VarLengthExpandPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, LazyTypes.empty, 1, None, nodeInScope = true)()
      .createResults(queryState).toList

    // then
    result should be(empty)
  }

  test("should support var length expand between two nodes when something in scope is not a node") {
    // given
    val startNode = newMockedNode(1)
    val endNode = newMockedNode(2)
    val relationship = newMockedRelationship(1, startNode, endNode)
    val query = mock[QueryContext]
    when(query.getRelationshipsForIds(any(), any(), any())).thenAnswer(new Answer[Iterator[Relationship]] {
      def answer(invocation: InvocationOnMock): Iterator[Relationship] = Iterator(relationship)
    })

    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode, "b" -> CTInteger)))
    when(left.createResults(queryState)).thenAnswer(new Answer[Iterator[ExecutionContext]] {
      def answer(invocation: InvocationOnMock): Iterator[ExecutionContext] = Iterator(row("a" -> startNode, "b" -> 42))
    })

    // when
    val result =
      VarLengthExpandPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, LazyTypes.empty, 1, None, nodeInScope = true)()
      .createResults(queryState).toList

    // then
    result should be(empty)
  }

  test("should support var length expand between two nodes when row in input are mixed with nodes matching and not matching") {
    // given
    val startNode = newMockedNode(1)
    val endNode = newMockedNode(2)
    val relationship = newMockedRelationship(1, startNode, endNode)
    val query = mock[QueryContext]
    when(query.getRelationshipsForIds(any(), any(), any())).thenAnswer(new Answer[Iterator[Relationship]] {
      def answer(invocation: InvocationOnMock): Iterator[Relationship] = Iterator(relationship)
    })

    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode, "b" -> CTNode)))
    when(left.createResults(queryState)).thenAnswer(new Answer[Iterator[ExecutionContext]] {
      def answer(invocation: InvocationOnMock): Iterator[ExecutionContext] = Iterator(
        row("a" -> startNode, "b" -> newMockedNode(42)),
        row("a" -> startNode, "b" -> endNode)
      )
    })

    // when
    val result =
      VarLengthExpandPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, LazyTypes.empty, 1, None, nodeInScope = true)()
      .createResults(queryState).toList

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
    val leftRelationship = newMockedRelationship(1, startNode, middleNode)
    val rightRelationship = newMockedRelationship(2, middleNode, endNode)

    val query = mock[QueryContext]
    val nodeMapping: Map[(Node, SemanticDirection), Seq[Relationship]] = Map(
      (startNode, SemanticDirection.OUTGOING) -> Seq(leftRelationship),
      (middleNode, SemanticDirection.INCOMING) -> Seq(leftRelationship),
      (middleNode, SemanticDirection.OUTGOING) -> Seq(rightRelationship),
      (endNode, SemanticDirection.INCOMING) -> Seq(rightRelationship)
    )
    replyWithMap(query, nodeMapping.withDefaultValue(Seq.empty))
    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("a" -> startNode)))

    // when
    val result =
      VarLengthExpandPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, LazyTypes.empty, 1, None, nodeInScope = false)().
        createResults(queryState).toList

    // then
    val (first :: second :: Nil) = result
    first("a") should equal(startNode)
    first("r") should equal(List(leftRelationship))
    first("b") should equal(middleNode)
    second("a") should equal(startNode)
    second("r") should equal(List(leftRelationship, rightRelationship))
    second("b") should equal(endNode)
  }

  test("should support var length expand between three nodes and end node in scope") {
    // given
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)
    val leftRelationship = newMockedRelationship(1, startNode, middleNode)
    val rightRelationship = newMockedRelationship(2, middleNode, endNode)

    val query = mock[QueryContext]
    val nodeMapping: Map[(Node, SemanticDirection), Seq[Relationship]] = Map(
      (startNode, SemanticDirection.OUTGOING) -> Seq(leftRelationship),
      (middleNode, SemanticDirection.INCOMING) -> Seq(leftRelationship),
      (middleNode, SemanticDirection.OUTGOING) -> Seq(rightRelationship),
      (endNode, SemanticDirection.INCOMING) -> Seq(rightRelationship)
    )
    replyWithMap(query, nodeMapping.withDefaultValue(Seq.empty))
    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode, "b" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("a" -> startNode, "b" -> endNode)))

    // when
    val result =
      VarLengthExpandPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, LazyTypes.empty, 1, None, nodeInScope = false)().
        createResults(queryState).toList

    // then
    val (first :: second :: Nil) = result
    first("a") should equal(startNode)
    first("r") should equal(List(leftRelationship))
    first("b") should equal(middleNode)
    second("a") should equal(startNode)
    second("r") should equal(List(leftRelationship, rightRelationship))
    second("b") should equal(endNode)
  }

  test("should support var length expand between three nodes and mismatching end node in scope") {
    // given
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)
    val leftRelationship = newMockedRelationship(1, startNode, middleNode)
    val rightRelationship = newMockedRelationship(2, middleNode, endNode)

    val query = mock[QueryContext]
    val nodeMapping: Map[(Node, SemanticDirection), Seq[Relationship]] = Map(
      (startNode, SemanticDirection.OUTGOING) -> Seq(leftRelationship),
      (middleNode, SemanticDirection.INCOMING) -> Seq(leftRelationship),
      (middleNode, SemanticDirection.OUTGOING) -> Seq(rightRelationship),
      (endNode, SemanticDirection.INCOMING) -> Seq(rightRelationship)
    )
    replyWithMap(query, nodeMapping.withDefaultValue(Seq.empty))
    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode, "b" -> CTNode)))
    val badNode: Node = newMockedNode(42)
    when(left.createResults(queryState)).thenReturn(Iterator(row("a" -> startNode, "b" -> badNode)))

    // when
    val result =
      VarLengthExpandPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, LazyTypes.empty, 1, None, nodeInScope = true)().
        createResults(queryState).toList

    // then
    result should be(empty)
  }

  test("should support var length expand between three nodes with multiple relationships") {
    // given
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)
    val leftRelationship1 = newMockedRelationship(1, startNode, middleNode)
    val leftRelationship2 = newMockedRelationship(2, startNode, middleNode)
    val rightRelationship = newMockedRelationship(3, middleNode, endNode)

    val query = mock[QueryContext]
    val nodeMapping: Map[(Node, SemanticDirection), Seq[Relationship]] = Map(
      (startNode, SemanticDirection.OUTGOING) -> Seq(leftRelationship1, leftRelationship2),
      (middleNode, SemanticDirection.INCOMING) -> Seq(leftRelationship1, leftRelationship2),
      (middleNode, SemanticDirection.OUTGOING) -> Seq(rightRelationship),
      (endNode, SemanticDirection.INCOMING) -> Seq(rightRelationship)
    )
    replyWithMap(query, nodeMapping.withDefaultValue(Seq.empty))
    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("a" -> startNode)))

    // when
    val result = VarLengthExpandPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, LazyTypes.empty, 1, None, nodeInScope = false)().
      createResults(queryState).toList

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

  test("should support var length expand between three nodes with multiple relationships with end node in scope") {
    // given
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)
    val leftRelationship1 = newMockedRelationship(1, startNode, middleNode)
    val leftRelationship2 = newMockedRelationship(2, startNode, middleNode)
    val rightRelationship = newMockedRelationship(3, middleNode, endNode)

    val query = mock[QueryContext]
    val nodeMapping: Map[(Node, SemanticDirection), Seq[Relationship]] = Map(
      (startNode, SemanticDirection.OUTGOING) -> Seq(leftRelationship1, leftRelationship2),
      (middleNode, SemanticDirection.INCOMING) -> Seq(leftRelationship1, leftRelationship2),
      (middleNode, SemanticDirection.OUTGOING) -> Seq(rightRelationship),
      (endNode, SemanticDirection.INCOMING) -> Seq(rightRelationship)
    )
    replyWithMap(query, nodeMapping.withDefaultValue(Seq.empty))
    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode, "b" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("a" -> startNode, "b" -> endNode)))

    // when
    val result = VarLengthExpandPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, LazyTypes.empty, 1, None, nodeInScope = true)().
      createResults(queryState).toList

    // then
    val (first :: second :: Nil) = result
    first("a") should equal(startNode)
    first("r") should equal(List(leftRelationship2, rightRelationship))
    first("b") should equal(endNode)
    second("a") should equal(startNode)
    second("r") should equal(List(leftRelationship1, rightRelationship))
    second("b") should equal(endNode)
  }

  test("should support var length expand between three nodes with multiple relationships with mismatching end node in scope") {
    // given
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)
    val leftRelationship1 = newMockedRelationship(1, startNode, middleNode)
    val leftRelationship2 = newMockedRelationship(2, startNode, middleNode)
    val rightRelationship = newMockedRelationship(3, middleNode, endNode)

    val query = mock[QueryContext]
    val nodeMapping: Map[(Node, SemanticDirection), Seq[Relationship]] = Map(
      (startNode, SemanticDirection.OUTGOING) -> Seq(leftRelationship1, leftRelationship2),
      (middleNode, SemanticDirection.INCOMING) -> Seq(leftRelationship1, leftRelationship2),
      (middleNode, SemanticDirection.OUTGOING) -> Seq(rightRelationship),
      (endNode, SemanticDirection.INCOMING) -> Seq(rightRelationship)
    )
    replyWithMap(query, nodeMapping.withDefaultValue(Seq.empty))
    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode, "b" -> CTNode)))
    val badNode = newMockedNode(42)
    when(left.createResults(queryState)).thenReturn(Iterator(row("a" -> startNode, "b" -> badNode)))

    // when
    val result = VarLengthExpandPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, LazyTypes.empty, 1, None, nodeInScope = true)().
      createResults(queryState).toList

    // then
    result should be(empty)
  }

  test("should support var length expand between three nodes with multiple relationships and a fixed max length") {
    // given
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)
    val leftRelationship1 = newMockedRelationship(1, startNode, middleNode)
    val leftRelationship2 = newMockedRelationship(2, startNode, middleNode)
    val rightRelationship = newMockedRelationship(3, middleNode, endNode)

    val query = mock[QueryContext]
    val nodeMapping: Map[(Node, SemanticDirection), Seq[Relationship]] = Map(
      (startNode, SemanticDirection.OUTGOING) -> Seq(leftRelationship1, leftRelationship2),
      (middleNode, SemanticDirection.INCOMING) -> Seq(leftRelationship1, leftRelationship2),
      (middleNode, SemanticDirection.OUTGOING) -> Seq(rightRelationship),
      (endNode, SemanticDirection.INCOMING) -> Seq(rightRelationship)
    )
    replyWithMap(query, nodeMapping.withDefaultValue(Seq.empty))
    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("a" -> startNode)))

    // when
    val result = VarLengthExpandPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, LazyTypes.empty, 1, Some(1), nodeInScope = false)().
      createResults(queryState).toList

    // then
    val (first :: second :: Nil) = result
    first("a") should equal(startNode)
    first("r") should equal(List(leftRelationship2))
    first("b") should equal(middleNode)
    second("a") should equal(startNode)
    second("r") should equal(List(leftRelationship1))
    second("b") should equal(middleNode)
  }

  test("should support var length expand between three nodes with multiple relationships and a fixed max length and end node in scope") {
    // given
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)
    val leftRelationship1 = newMockedRelationship(1, startNode, middleNode)
    val leftRelationship2 = newMockedRelationship(2, startNode, middleNode)
    val leftRelationship3 = newMockedRelationship(4, startNode, endNode)
    val rightRelationship = newMockedRelationship(3, middleNode, endNode)

    val query = mock[QueryContext]
    val nodeMapping: Map[(Node, SemanticDirection), Seq[Relationship]] = Map(
      (startNode, SemanticDirection.OUTGOING) -> Seq(leftRelationship1, leftRelationship2, leftRelationship3),
      (middleNode, SemanticDirection.INCOMING) -> Seq(leftRelationship1, leftRelationship2),
      (middleNode, SemanticDirection.OUTGOING) -> Seq(rightRelationship),
      (endNode, SemanticDirection.INCOMING) -> Seq(rightRelationship, leftRelationship3)
    )
    replyWithMap(query, nodeMapping.withDefaultValue(Seq.empty))
    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode, "b" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("a" -> startNode, "b" -> endNode)))

    // when
    val result = VarLengthExpandPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, LazyTypes.empty, 1, Some(1), nodeInScope = true)().
      createResults(queryState).toList

    // then
    val (first :: Nil) = result
    first("a") should equal(startNode)
    first("r") should equal(List(leftRelationship3))
    first("b") should equal(endNode)
  }

  test("should support var length expand between three nodes with multiple relationships and fixed min and max lengths") {
    // given
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)
    val leftRelationship1 = newMockedRelationship(1, startNode, middleNode)
    val leftRelationship2 = newMockedRelationship(2, startNode, middleNode)
    val rightRelationship = newMockedRelationship(3, middleNode, endNode)

    val query = mock[QueryContext]
    val nodeMapping: Map[(Node, SemanticDirection), Seq[Relationship]] = Map(
      (startNode, SemanticDirection.OUTGOING) -> Seq(leftRelationship1, leftRelationship2),
      (middleNode, SemanticDirection.INCOMING) -> Seq(leftRelationship1, leftRelationship2),
      (middleNode, SemanticDirection.OUTGOING) -> Seq(rightRelationship),
      (endNode, SemanticDirection.INCOMING) -> Seq(rightRelationship)
    )
    replyWithMap(query, nodeMapping.withDefaultValue(Seq.empty))
    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("a" -> startNode)))

    // when
    val result = VarLengthExpandPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, LazyTypes.empty, 2, Some(2), nodeInScope = false)().
      createResults(queryState).toList

    // then
    val (first :: second :: Nil) = result
    first("a") should equal(startNode)
    first("r") should equal(List(leftRelationship2, rightRelationship))
    first("b") should equal(endNode)
    second("a") should equal(startNode)
    second("r") should equal(List(leftRelationship1, rightRelationship))
    second("b") should equal(endNode)
  }

  test("should support var length expand between three nodes with multiple relationships and fixed min and max lengths and end node in scope") {
    // given
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)
    val otherNode = newMockedNode(4)
    val leftRelationship1 = newMockedRelationship(1, startNode, middleNode)
    val leftRelationship2 = newMockedRelationship(2, startNode, middleNode)
    val rightRelationship = newMockedRelationship(3, middleNode, endNode)
    val otherRelationship = newMockedRelationship(4, middleNode, otherNode)

    val query = mock[QueryContext]
    val nodeMapping: Map[(Node, SemanticDirection), Seq[Relationship]] = Map(
      (startNode, SemanticDirection.OUTGOING) -> Seq(leftRelationship1, leftRelationship2),
      (middleNode, SemanticDirection.INCOMING) -> Seq(leftRelationship1, leftRelationship2),
      (middleNode, SemanticDirection.OUTGOING) -> Seq(rightRelationship, otherRelationship),
      (endNode, SemanticDirection.INCOMING) -> Seq(rightRelationship),
      (otherNode, SemanticDirection.INCOMING) -> Seq(otherRelationship)
    )
    replyWithMap(query, nodeMapping.withDefaultValue(Seq.empty)
    )
    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode, "b"-> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("a" -> startNode, "b" -> endNode)))

    // when
    val result = VarLengthExpandPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, LazyTypes.empty, 2, Some(2), nodeInScope = true)().
      createResults(queryState).toList

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
    val initialRelationship = newMockedRelationship(4, firstNode, startNode)
    val leftRelationship1 = newMockedRelationship(1, startNode, middleNode)
    val leftRelationship2 = newMockedRelationship(2, startNode, middleNode)
    val rightRelationship = newMockedRelationship(3, middleNode, endNode)

    val query = mock[QueryContext]
    val nodeMapping: Map[(Node, SemanticDirection), Seq[Relationship]] = Map(
      (firstNode, SemanticDirection.OUTGOING) -> Seq(initialRelationship),
      (startNode, SemanticDirection.INCOMING) -> Seq(initialRelationship),
      (startNode, SemanticDirection.OUTGOING) -> Seq(leftRelationship1, leftRelationship2),
      (middleNode, SemanticDirection.INCOMING) -> Seq(leftRelationship1, leftRelationship2),
      (middleNode, SemanticDirection.OUTGOING) -> Seq(rightRelationship),
      (endNode, SemanticDirection.INCOMING) -> Seq(rightRelationship)
    )
    replyWithMap(query, nodeMapping.withDefaultValue(Seq.empty)
    )
    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("a" -> firstNode)))

    // when
    val result = VarLengthExpandPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, LazyTypes.empty, 2, Some(3), nodeInScope = false)().
      createResults(queryState).toList

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

  test("should support var length expand between three nodes with multiple relationships and fixed min and max lengths 2 with end node in scope") {
    // given
    val firstNode = newMockedNode(4)
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)
    val otherNode = newMockedNode(5)
    val initialRelationship = newMockedRelationship(4, firstNode, startNode)
    val leftRelationship1 = newMockedRelationship(1, startNode, middleNode)
    val leftRelationship2 = newMockedRelationship(2, startNode, middleNode)
    val rightRelationship = newMockedRelationship(3, middleNode, endNode)
    val otherRelationship = newMockedRelationship(5, middleNode, otherNode)

    val query = mock[QueryContext]
    val nodeMapping: Map[(Node, SemanticDirection), Seq[Relationship]] = Map(
      (firstNode, SemanticDirection.OUTGOING) -> Seq(initialRelationship),
      (startNode, SemanticDirection.INCOMING) -> Seq(initialRelationship),
      (startNode, SemanticDirection.OUTGOING) -> Seq(leftRelationship1, leftRelationship2),
      (middleNode, SemanticDirection.INCOMING) -> Seq(leftRelationship1, leftRelationship2),
      (middleNode, SemanticDirection.OUTGOING) -> Seq(rightRelationship, otherRelationship),
      (endNode, SemanticDirection.INCOMING) -> Seq(rightRelationship),
      (otherNode, SemanticDirection.INCOMING) -> Seq(otherRelationship)
    )
    replyWithMap(query, nodeMapping.withDefaultValue(Seq.empty)
    )
    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode, "b" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("a" -> firstNode, "b" -> endNode)))

    // when
    val result = VarLengthExpandPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, LazyTypes.empty, 2, Some(3), nodeInScope = true)().
      createResults(queryState).toList

    // then
    val (first :: second :: Nil) = result
    first("a") should equal(firstNode)
    first("r") should equal(List(initialRelationship, leftRelationship2, rightRelationship))
    first("b") should equal(endNode)
    second("a") should equal(firstNode)
    second("r") should equal(List(initialRelationship, leftRelationship1, rightRelationship))
    second("b") should equal(endNode)
  }

  test("should project the relationship list in the right direction") {
    // given
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)
    val leftRelationship = newMockedRelationship(1, startNode, middleNode)
    val rightRelationship = newMockedRelationship(2, middleNode, endNode)

    val query = mock[QueryContext]
    val nodeMapping: Map[(Node, SemanticDirection), Seq[Relationship]] = Map(
      (startNode, SemanticDirection.OUTGOING) -> Seq(leftRelationship),
      (middleNode, SemanticDirection.INCOMING) -> Seq(leftRelationship),
      (middleNode, SemanticDirection.OUTGOING) -> Seq(rightRelationship),
      (endNode, SemanticDirection.INCOMING) -> Seq(rightRelationship)
    )
    replyWithMap(query, nodeMapping.withDefaultValue(Seq.empty))
    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("a" -> startNode)))

    // (b)-[r]->(a)

    // when
    val result = VarLengthExpandPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, SemanticDirection.INCOMING, LazyTypes.empty, 1, None, nodeInScope = false)().
      createResults(queryState).toList

    // then
    val (first :: second :: Nil) = result
    first("a") should equal(startNode)
    first("r") should equal(List(leftRelationship))
    first("b") should equal(middleNode)
    second("a") should equal(startNode)
    second("r") should equal(List(rightRelationship, leftRelationship))
    second("b") should equal(endNode)
  }

  test("should support var length expand with expansion-stage filtering") {
    // given
    val firstNode = newMockedNode(4)
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)
    val initialRelationship = newMockedRelationship(4, firstNode, startNode)
    val leftRelationship1 = newMockedRelationship(1, startNode, middleNode)
    val leftRelationship2 = newMockedRelationship(2, startNode, middleNode)
    val rightRelationship = newMockedRelationship(3, middleNode, endNode)
    val query = mock[QueryContext]
    val nodeMapping: Map[(Node, SemanticDirection), Seq[Relationship]] = Map(
      (firstNode, SemanticDirection.OUTGOING) -> Seq(initialRelationship),
      (startNode, SemanticDirection.INCOMING) -> Seq(initialRelationship),
      (startNode, SemanticDirection.OUTGOING) -> Seq(leftRelationship1, leftRelationship2),
      (middleNode, SemanticDirection.INCOMING) -> Seq(leftRelationship1, leftRelationship2),
      (middleNode, SemanticDirection.OUTGOING) -> Seq(rightRelationship),
      (endNode, SemanticDirection.INCOMING) -> Seq(rightRelationship)
    )
    replyWithMap(query, nodeMapping.withDefaultValue(Seq.empty)
    )
    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("a" -> firstNode)))

    def filteringStep(context: ExecutionContext, q: QueryState, rel: Relationship): Boolean = rel.getId != 2

    // when
    val result = VarLengthExpandPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, SemanticDirection.OUTGOING,
                                     LazyTypes.empty, 3, None, nodeInScope = false, filteringStep)().createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single("a") should equal(firstNode)
    single("r") should equal(List(initialRelationship, leftRelationship1, rightRelationship))
    single("b") should equal(endNode)
  }

  test("should project (a)-[r*]->(b) correctly when from = a, to = b") {
    // given
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)

    // a-[r1]->()-[r2]->b
    val relationship1 = newNamedMockedRelationship(1, "r1", startNode, middleNode)
    val relationship2 = newNamedMockedRelationship(2, "r2", middleNode, endNode)

    val query = mock[QueryContext]
    val nodeMapping: Map[(Node, SemanticDirection), Seq[Relationship]] = Map(
      (startNode, SemanticDirection.OUTGOING) -> Seq(relationship1),
      (middleNode, SemanticDirection.INCOMING) -> Seq(relationship1),
      (middleNode, SemanticDirection.OUTGOING) -> Seq(relationship2),
      (endNode, SemanticDirection.INCOMING) -> Seq(relationship2)
    )
    replyWithMap(query, nodeMapping.withDefaultValue(Seq.empty))

    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("a" -> startNode)))

    // when
    val result = VarLengthExpandPipe(left, "a", "r", "b", /* dir */ SemanticDirection.OUTGOING, /* projectedDir */ SemanticDirection.OUTGOING, LazyTypes.empty, 2, Some(2), nodeInScope = false)().createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single("a") should equal(startNode)
    single("r") should equal(Seq(relationship1, relationship2))
    single("b") should equal(endNode)
  }

  test("should project (a)-[r*]->(b) correctly when from = b, to = a") {
    // given
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)

    // a-[r1]->()-[r2]->b
    val relationship2 = newNamedMockedRelationship(2, "r2", middleNode, endNode)
    val relationship1 = newNamedMockedRelationship(1, "r1", startNode, middleNode)

    val query = mock[QueryContext]
    val nodeMapping: Map[(Node, SemanticDirection), Seq[Relationship]] = Map(
      (startNode, SemanticDirection.OUTGOING) -> Seq(relationship1),
      (middleNode, SemanticDirection.INCOMING) -> Seq(relationship1),
      (middleNode, SemanticDirection.OUTGOING) -> Seq(relationship2),
      (endNode, SemanticDirection.INCOMING) -> Seq(relationship2)
    )
    replyWithMap(query, nodeMapping.withDefaultValue(Seq.empty))

    val queryState = QueryStateHelper.emptyWith(query = query)

    val right = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(right.createResults(queryState)).thenReturn(Iterator(row("b" -> endNode)))

    // when
    val result = VarLengthExpandPipe(right, "b", "r", "a", /* dir */ SemanticDirection.INCOMING, /* projectedDir */ SemanticDirection.OUTGOING, LazyTypes.empty, 2, Some(2), nodeInScope = false)().createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single("a") should equal(startNode)
    single("r") should equal(Seq(relationship1, relationship2))
    single("b") should equal(endNode)
  }

  test("should project (a)<-[r*]-(b) correctly when from = a, to = b") {
    // given
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)

    // a<-[r1]-()<-[r2]-b
    val relationship1 = newNamedMockedRelationship(1, "r1", middleNode, startNode)
    val relationship2 = newNamedMockedRelationship(2, "r2", endNode, middleNode)

    val query = mock[QueryContext]
    val nodeMapping: Map[(Node, SemanticDirection), Seq[Relationship]] = Map(
      (startNode, SemanticDirection.INCOMING) -> Seq(relationship1),
      (middleNode, SemanticDirection.OUTGOING) -> Seq(relationship1),
      (middleNode, SemanticDirection.INCOMING) -> Seq(relationship2),
      (endNode, SemanticDirection.OUTGOING) -> Seq(relationship2)
    )
    replyWithMap(query, nodeMapping.withDefaultValue(Seq.empty))

    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("a" -> startNode)))

    // when
    val result = VarLengthExpandPipe(left, "a", "r", "b", /* dir */ SemanticDirection.INCOMING, /* projectedDir */ SemanticDirection.INCOMING, LazyTypes.empty, 2, Some(2), nodeInScope = false)().createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single("a") should equal(startNode)
    single("r") should equal(Seq(relationship1, relationship2))
    single("b") should equal(endNode)
  }

  test("should project (a)<-[r*]-(b) correctly when from = b, to = a") {
    // given
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)

    // a<-[r1]-()<-[r2]-b
    val relationship1 = newNamedMockedRelationship(1, "r1", middleNode, startNode)
    val relationship2 = newNamedMockedRelationship(2, "r2", endNode, middleNode)

    val query = mock[QueryContext]
    val nodeMapping: Map[(Node, SemanticDirection), Seq[Relationship]] = Map(
      (startNode, SemanticDirection.INCOMING) -> Seq(relationship1),
      (middleNode, SemanticDirection.OUTGOING) -> Seq(relationship1),
      (middleNode, SemanticDirection.INCOMING) -> Seq(relationship2),
      (endNode, SemanticDirection.OUTGOING) -> Seq(relationship2)
    )
    replyWithMap(query, nodeMapping.withDefaultValue(Seq.empty))

    val queryState = QueryStateHelper.emptyWith(query = query)

    val right = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(right.createResults(queryState)).thenReturn(Iterator(row("b" -> endNode)))

    // when
    val result = VarLengthExpandPipe(right, "b", "r", "a", /* dir */ SemanticDirection.OUTGOING, /* projectedDir */ SemanticDirection.INCOMING, LazyTypes.empty, 2, Some(2), nodeInScope = false)().createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single("a") should equal(startNode)
    single("r") should equal(Seq(relationship1, relationship2))
    single("b") should equal(endNode)
  }

  ///
  // UNDIRECTED CASES
  //

  test("should project (a)-[r*]-(b) correctly when from = a, to = b for a graph a-[r1]->()-[r2]->b") {
    // given
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)

    // a-[r1]->()-[r2]->b
    val relationship1 = newNamedMockedRelationship(1, "r1", startNode, middleNode)
    val relationship2 = newNamedMockedRelationship(2, "r2", middleNode, endNode)

    val query = mock[QueryContext]
    val nodeMapping: Map[(Node, SemanticDirection), Seq[Relationship]] = Map(
      (startNode, SemanticDirection.BOTH) -> Seq(relationship1),
      (middleNode, SemanticDirection.BOTH) -> Seq(relationship1, relationship2),
      (endNode, SemanticDirection.BOTH) -> Seq(relationship2)
    )
    replyWithMap(query, nodeMapping.withDefaultValue(Seq.empty))

    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("a" -> startNode)))

    // when
    val result = VarLengthExpandPipe(left, "a", "r", "b", /* dir */ SemanticDirection.BOTH, /* projectedDir */ SemanticDirection.OUTGOING, LazyTypes.empty, 2, Some(2), nodeInScope = false)().createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single("a") should equal(startNode)
    single("r") should equal(Seq(relationship1, relationship2))
    single("b") should equal(endNode)
  }

  test("should project (a)-[r*]-(b) correctly when from = b, to = a for a graph a-[r1]->()-[r2]->b") {
    // given
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)

    // a-[r1]->()-[r2]->b
    val relationship2 = newNamedMockedRelationship(2, "r2", middleNode, endNode)
    val relationship1 = newNamedMockedRelationship(1, "r1", startNode, middleNode)

    val query = mock[QueryContext]
    val nodeMapping: Map[(Node, SemanticDirection), Seq[Relationship]] = Map(
      (startNode, SemanticDirection.BOTH) -> Seq(relationship1),
      (middleNode, SemanticDirection.BOTH) -> Seq(relationship1, relationship2),
      (endNode, SemanticDirection.BOTH) -> Seq(relationship2)
    )
    replyWithMap(query, nodeMapping.withDefaultValue(Seq.empty))

    val queryState = QueryStateHelper.emptyWith(query = query)

    val right = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(right.createResults(queryState)).thenReturn(Iterator(row("b" -> endNode)))

    // when
    val result = VarLengthExpandPipe(right, "b", "r", "a", /* dir */ SemanticDirection.BOTH, /* projectedDir */ SemanticDirection.INCOMING, LazyTypes.empty, 2, Some(2), nodeInScope = false)().createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single("a") should equal(startNode)
    single("r") should equal(Seq(relationship1, relationship2))
    single("b") should equal(endNode)
  }

  test("should project (a)-[r*]-(b) correctly when from = a, to = b from a<-[r1]-()<-[r2]-b") {
    // given
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)

    // a<-[r1]-()<-[r2]-b
    val relationship1 = newNamedMockedRelationship(1, "r1", middleNode, startNode)
    val relationship2 = newNamedMockedRelationship(2, "r2", endNode, middleNode)

    val query = mock[QueryContext]
    val nodeMapping: Map[(Node, SemanticDirection), Seq[Relationship]] = Map(
      (startNode, SemanticDirection.BOTH) -> Seq(relationship1),
      (middleNode, SemanticDirection.BOTH) -> Seq(relationship1, relationship2),
      (endNode, SemanticDirection.BOTH) -> Seq(relationship2)
    )
    replyWithMap(query, nodeMapping.withDefaultValue(Seq.empty))

    val queryState = QueryStateHelper.emptyWith(query = query)

    val left = newMockedPipe(SymbolTable(Map("a" -> CTNode)))
    when(left.createResults(queryState)).thenReturn(Iterator(row("a" -> startNode)))

    // when
    val result = VarLengthExpandPipe(left, "a", "r", "b", /* dir */ SemanticDirection.BOTH, /* projectedDir */ SemanticDirection.OUTGOING, LazyTypes.empty, 2, Some(2), nodeInScope = false)().createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single("a") should equal(startNode)
    single("r") should equal(Seq(relationship1, relationship2))
    single("b") should equal(endNode)
  }

  test("should project (a)-[r*]-(b) correctly when from = b, to = a for a graph a<-[r1]-()<-[r2]-b") {
    // given
    val startNode = newMockedNode(1)
    val middleNode = newMockedNode(2)
    val endNode = newMockedNode(3)

    // a<-[r1]-()<-[r2]-b
    val relationship1 = newNamedMockedRelationship(1, "r1", middleNode, startNode)
    val relationship2 = newNamedMockedRelationship(2, "r2", endNode, middleNode)

    val query = mock[QueryContext]
    val nodeMapping: Map[(Node, SemanticDirection), Seq[Relationship]] = Map(
      (startNode, SemanticDirection.BOTH) -> Seq(relationship1),
      (middleNode, SemanticDirection.BOTH) -> Seq(relationship1, relationship2),
      (endNode, SemanticDirection.BOTH) -> Seq(relationship2)
    )
    replyWithMap(query, nodeMapping.withDefaultValue(Seq.empty))

    val queryState = QueryStateHelper.emptyWith(query = query)

    val right = newMockedPipe(SymbolTable(Map("b" -> CTNode)))
    when(right.createResults(queryState)).thenReturn(Iterator(row("b" -> endNode)))

    // when
    val result = VarLengthExpandPipe(right, "b", "r", "a", /* dir */ SemanticDirection.BOTH, /* projectedDir */ SemanticDirection.INCOMING, LazyTypes.empty, 2, Some(2), nodeInScope = false)().createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single("a") should equal(startNode)
    single("r") should equal(Seq(relationship1, relationship2))
    single("b") should equal(endNode)
  }

  test("should correctly handle nulls from source pipe") {
    // given
    val query = mock[QueryContext]
    val queryState = QueryStateHelper.emptyWith(query = query)

    val source = newMockedPipe(SymbolTable(Map("a" -> CTNode)))
    when(source.createResults(queryState)).thenReturn(Iterator(row("a" -> null)))

    // when
    val result = VarLengthExpandPipe(source, "a", "r", "b", SemanticDirection.BOTH, SemanticDirection.INCOMING, LazyTypes.empty, 1, None, nodeInScope = false)().createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single("a").asInstanceOf[AnyRef] should be(null)
    single("r").asInstanceOf[AnyRef] should be(null)
    single("b").asInstanceOf[AnyRef] should be(null)
  }

  private def row(values: (String, Any)*) = ExecutionContext.from(values: _*)

  private def newMockedNode(id: Int) = {
    val node = mock[Node]
    when(node.getId).thenReturn(id)
    node
  }

  private def newNamedMockedRelationship(id: Int, relName: String, startNode: Node, endNode: Node): Relationship = {
    val rel = newMockedRelationship(id, startNode, endNode)
    when(rel.toString).thenReturn(relName)
    rel
  }

  private def newMockedRelationship(id: Int, startNode: Node, endNode: Node): Relationship = {
    val relationship = mock[Relationship]
    when(relationship.getId).thenReturn(id)
    when(relationship.getStartNode).thenReturn(startNode)
    when(relationship.getEndNode).thenReturn(endNode)
    when(relationship.getOtherNode(startNode)).thenReturn(endNode)
    when(relationship.getOtherNode(endNode)).thenReturn(startNode)
    relationship
  }

  private def replyWithMap(query: QueryContext, mapping: Map[(Node, _ <: SemanticDirection), Seq[Relationship]]) {
    when(query.getRelationshipsForIds(any(), any(), any())).thenAnswer(new Answer[Iterator[Relationship]] {
      def answer(invocation: InvocationOnMock): Iterator[Relationship] = {
        val (startNode :: dir :: _ :: Nil) = invocation.getArguments.toList
        mapping((startNode.asInstanceOf[Node], dir.asInstanceOf[SemanticDirection])).iterator
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
