/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, QueryStateHelper}
import org.neo4j.cypher.internal.runtime.ImplicitValueConversion._
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.{Not, Predicate, True}
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.expressions.SemanticDirection
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.kernel.impl.util.ValueUtils.{fromNodeProxy, fromRelationshipProxy}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.RelationshipValue

class OptionalExpandAllPipeTest extends CypherFunSuite {

  val startNode = newMockedNode(1)
  val endNode1 = newMockedNode(2)
  val endNode2 = newMockedNode(3)
  val relationship1 = newMockedRelationship(1, startNode, endNode1)
  val relationship2 = newMockedRelationship(2, startNode, endNode2)
  val selfRelationship = newMockedRelationship(3, startNode, startNode)
  val query = mock[QueryContext]
  val queryState = QueryStateHelper.emptyWith(query = query)

  test("should support expand between two nodes with a relationship") {
    // given
    mockRelationships(relationship1)
    val left = newMockedPipe("a",
      row("a" -> startNode))

    // when
    val result = OptionalExpandAllPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())().createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single.toMap should equal(Map("a" -> fromNodeProxy(startNode), "r" -> fromRelationshipProxy(relationship1),
                              "b" -> fromNodeProxy(endNode1)))
  }

  test("should support optional expand from a node with no relationships") {
    // given
    mockRelationships()
    val left = newMockedPipe("a",
      row("a" -> startNode))

    // when
    val result = OptionalExpandAllPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())().createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single.toMap should equal(Map("a" -> fromNodeProxy(startNode), "r" -> NO_VALUE, "b" -> NO_VALUE))
  }

  test("should support optional expand from a node with relationships that do not match the predicates") {
    // given
    mockRelationships(relationship1)
    val left = newMockedPipe("a",
      row("a" -> startNode))

    val falsePredicate: Predicate = Not(True())
    // when
    val result = OptionalExpandAllPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, falsePredicate)().createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single.toMap should equal(Map("a" -> fromNodeProxy(startNode), "r" -> NO_VALUE, "b" -> NO_VALUE))
  }

  test("should support expand between two nodes with multiple relationships") {
    // given
    mockRelationships(relationship1, relationship2)
    val left = newMockedPipe("a",
      row("a" -> startNode))

    // when
    val result = OptionalExpandAllPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())().createResults(queryState).toList

    // then
    val (first :: second :: Nil) = result
    first.toMap should equal(Map("a" -> fromNodeProxy(startNode), "r" -> fromRelationshipProxy(relationship1), "b" -> fromNodeProxy(endNode1)))
    second.toMap should equal(Map("a" -> fromNodeProxy(startNode), "r" -> fromRelationshipProxy(relationship2), "b" -> fromNodeProxy(endNode2)))
  }

  test("should support expand between two nodes with multiple relationships and self loops") {
    // given
    mockRelationships(relationship1, selfRelationship)
    val left = newMockedPipe("a",
      row("a" -> startNode))

    // when
    val result = OptionalExpandAllPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())().createResults(queryState).toList

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
    val result = OptionalExpandAllPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())().createResults(queryState).toList

    // then
    result shouldBe 'empty
  }

  private def mockRelationships(rels: Relationship*) {
    when(query.getRelationshipsForIds(any(), any(), any())).thenAnswer(new Answer[Iterator[RelationshipValue]] {
      def answer(invocation: InvocationOnMock): Iterator[RelationshipValue] = rels.iterator.map(fromRelationshipProxy)
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
    val startId = startNode.getId
    val endId = endNode.getId
    when(relationship.getId).thenReturn(id)
    when(relationship.getStartNode).thenReturn(startNode)
    when(relationship.getStartNodeId).thenReturn(startId)
    when(relationship.getEndNode).thenReturn(endNode)
    when(relationship.getEndNodeId).thenReturn(endId)
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
