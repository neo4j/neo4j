/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ImplicitValueConversion._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates.{Not, Predicate, True}
import org.neo4j.cypher.internal.frontend.v3_4.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.helpers.ValueUtils.{fromNodeProxy, fromRelationshipProxy}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.NO_VALUE

class OptionalExpandIntoPipeTest extends CypherFunSuite {

  val startNode = newMockedNode(1)
  val endNode1 = newMockedNode(2)
  val endNode2 = newMockedNode(3)
  val endNode3 = newMockedNode(4)
  val relationship1 = newMockedRelationship(1, startNode, endNode1)
  val relationship2 = newMockedRelationship(2, startNode, endNode2)
  val relationship3 = newMockedRelationship(3, startNode, endNode3)
  val selfRelationship = newMockedRelationship(4, startNode, startNode)
  val query = mock[QueryContext]
  val queryState = QueryStateHelper.emptyWith(query = query)

  test("should support expand between two nodes with a relationship") {
    // given
    mockRelationships(relationship1)
    val left = newMockedPipe("a",
      row("a" -> startNode, "b" -> endNode1))

    // when
    val result = OptionalExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())().createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single.toMap should equal(Map("a" -> fromNodeProxy(startNode), "r" -> fromRelationshipProxy(relationship1), "b" -> fromNodeProxy(endNode1)))
  }

  test("should support optional expand from a node with no relationships") {
    // given
    mockRelationships()
    val left = newMockedPipe("a",
      row("a" -> startNode, "b" -> endNode1))

    // when
    val result = OptionalExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())().createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single.toMap should equal(Map("a" -> fromNodeProxy(startNode), "r" -> NO_VALUE, "b" -> fromNodeProxy(endNode1)))
  }

  test("should find null when two nodes have no shared relationships but do have some rels") {
    // given
    mockRelationships(relationship1)
    val left = newMockedPipe("a",
      row("a" -> startNode, "b" -> endNode2))

    // when
    val result = OptionalExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())().createResults(queryState).toList

    // then
    val (single :: Nil) = result

    single.toMap should equal(Map("a" -> fromNodeProxy(startNode), "r" -> NO_VALUE, "b" -> fromNodeProxy(endNode2)))
  }

  test("should filter out relationships not matching the end node") {
    // given
    mockRelationships(relationship1, relationship2)

    val left = newMockedPipe("a",
      row("a" -> startNode, "b" -> endNode2))

    // when
    val result = OptionalExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())().createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single.toMap should equal(Map("a" -> fromNodeProxy(startNode), "r" -> fromRelationshipProxy(relationship2), "b" -> fromNodeProxy(endNode2)))
  }

  test("should support optional expand from a node with relationships that do not match the predicates") {
    // given
    mockRelationships(relationship1)
    val left = newMockedPipe("a",
      row("a" -> startNode, "b" -> endNode1))

    val falsePredicate: Predicate = Not(True())
    // when
    val result = OptionalExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, falsePredicate)().createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single.toMap should equal(Map("a" -> fromNodeProxy(startNode), "r" -> NO_VALUE, "b" -> fromNodeProxy(endNode1)))
  }

  test("should support expand between two nodes with multiple relationships") {
    // given
    when(query.getRelationshipsForIds(any(), any(), any())).thenReturn(Iterator.empty)
    when(query.getRelationshipsForIds(startNode.getId, SemanticDirection.OUTGOING, None))
      .thenReturn(Iterator(relationship1, relationship2, relationship3, selfRelationship))
    when(query.getRelationshipsForIds(endNode1.getId, SemanticDirection.INCOMING, None))
      .thenReturn(Iterator(relationship1))
    when(query.getRelationshipsForIds(endNode2.getId, SemanticDirection.INCOMING, None))
      .thenReturn(Iterator(relationship2))
    when(query.getRelationshipsForIds(endNode3.getId, SemanticDirection.INCOMING, None))
      .thenReturn(Iterator(relationship3))

    val left = newMockedPipe("a",
      row("a" -> startNode, "b" -> endNode1),//(1) - (2)
      row("a" -> startNode, "b" -> endNode2))// (1) - (3)

    // when
    val result = OptionalExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())().createResults(queryState).toList

    // then
    val (first :: second :: Nil) = result
    first.toMap should equal(Map("a" -> fromNodeProxy(startNode), "r" -> fromRelationshipProxy(relationship1), "b" -> fromNodeProxy(endNode1)))
    second.toMap should equal(Map("a" -> fromNodeProxy(startNode), "r" -> fromRelationshipProxy(relationship2), "b" -> fromNodeProxy(endNode2)))
  }

  test("should support expand between two nodes with multiple relationships and self loops") {
    // given
    mockRelationships(relationship1, selfRelationship, relationship3)
    val left = newMockedPipe("a",
      row("a" -> startNode, "b" -> endNode1),
      row("a" -> startNode, "b" -> startNode)
    )

    // when
    val result = OptionalExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())().createResults(queryState).toList

    // then
    val (first :: second :: Nil) = result
    first.toMap should equal(Map("a" -> fromNodeProxy(startNode), "r" -> fromRelationshipProxy(relationship1), "b" -> fromNodeProxy(endNode1)))
    second.toMap should equal(Map("a" -> fromNodeProxy(startNode), "r" -> fromRelationshipProxy(selfRelationship), "b" -> fromNodeProxy(startNode)))
  }

  test("given empty input, should return empty output") {
    // given
    mockRelationships()
    val left = newMockedPipe("a")

    // when
    val result = OptionalExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())().createResults(queryState).toList

    // then
    result shouldBe 'empty
  }

  test("expand into null should return nulled row") {
    // given
    val node: Node = mock[Node]
    val input = new FakePipe(Iterator(Map("a" -> node, "b" -> null)))

    // when
    val result: List[ExecutionContext] = OptionalExpandIntoPipe(input, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())().createResults(queryState).toList

    // then
    result should equal(List(Map("a" -> fromNodeProxy(node), "r" -> NO_VALUE, "b" -> NO_VALUE)))
  }

  test("expand null into something should return nulled row") {
    // given
    val node: Node = mock[Node]
    val input = new FakePipe(Iterator(Map("a" -> null, "b" -> node)))

    // when
    val result: List[ExecutionContext] = OptionalExpandIntoPipe(input, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())().createResults(queryState).toList

    // then
    result should equal(List(Map("a" -> NO_VALUE, "r" -> NO_VALUE, "b" -> fromNodeProxy(node))))
  }

  test("expand null into null should return nulled row") {
    // given
    val input = new FakePipe(Iterator(Map("a" -> null, "b" -> null)))

    // when
    val result: List[ExecutionContext] = OptionalExpandIntoPipe(input, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())().createResults(queryState).toList

    // then
    result should equal(List(Map("a" -> NO_VALUE, "r" -> NO_VALUE, "b" -> NO_VALUE)))
  }

  test("expand into should handle multiple relationships between the same node") {
    // given
    val rel1 = newMockedRelationship(1, startNode, endNode1)
    val rel2 = newMockedRelationship(1, startNode, endNode1)
    mockRelationships(rel1, rel2)
    val left = newMockedPipe("a",
      row("a" -> startNode, "b" -> endNode1)
    )

    val predicate = mock[Predicate]
    when(predicate.isTrue(any[ExecutionContext], any[QueryState]))
      .thenReturn(true)
      .thenReturn(false)
    // when
    val result = OptionalExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, predicate)().createResults(queryState).toList

    // then
    result should equal(List(
      Map("a" -> fromNodeProxy(startNode), "b" -> fromNodeProxy(endNode1), "r" -> fromRelationshipProxy(rel1))))
  }

  private def mockRelationships(rels: Relationship*) {
    when(query.getRelationshipsForIds(any(), any(), any())).thenAnswer(new Answer[Iterator[Relationship]] {
      def answer(invocation: InvocationOnMock): Iterator[Relationship] = rels.iterator
    })
  }

  private def row(values: (String, AnyValue)*) = ExecutionContext.from(values: _*)

  private def newMockedNode(id: Int) = {
    val node = mock[Node]
    when(node.getId).thenReturn(id)
    node
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

  private def newMockedPipe(node: String, rows: ExecutionContext*): Pipe = {
    val pipe = mock[Pipe]
    when(pipe.createResults(any())).thenAnswer(new Answer[Iterator[ExecutionContext]] {
      def answer(invocation: InvocationOnMock): Iterator[ExecutionContext] = rows.iterator
    })

    pipe
  }
}
