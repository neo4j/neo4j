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
package org.neo4j.cypher.internal.compiler.v3_2.pipes

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compiler.v3_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_2.commands.predicates.{Not, Predicate, True}
import org.neo4j.cypher.internal.compiler.v3_2.spi.QueryContext
import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.graphdb.{Node, Relationship}

class OptionalExpandIntoPipeTest extends CypherFunSuite {

  implicit val monitor = mock[PipeMonitor]
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

    val pipe = OptionalExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())()
    // when
    val result = pipe.createResults(queryState).toList
    pipe.close(true)

    // then
    val (single :: Nil) = result
    single.m should equal(Map("a" -> startNode, "r" -> relationship1, "b" -> endNode1))
  }

  test("should support optional expand from a node with no relationships") {
    // given
    mockRelationships()
    val left = newMockedPipe("a",
      row("a" -> startNode, "b" -> endNode1))

    val pipe = OptionalExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())()
    // when
    val result = pipe.createResults(queryState).toList
    pipe.close(true)

    // then
    val (single :: Nil) = result
    single.m should equal(Map("a" -> startNode, "r" -> null, "b" -> endNode1))
  }

  test("should find null when two nodes have no shared relationships but do have some rels") {
    // given
    mockRelationships(relationship1)
    val left = newMockedPipe("a",
      row("a" -> startNode, "b" -> endNode2))

    val pipe = OptionalExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())()
    // when
    val result = pipe.createResults(queryState).toList
    pipe.close(true)

    // then
    val (single :: Nil) = result
    single.m should equal(Map("a" -> startNode, "r" -> null, "b" -> endNode2))
  }

  test("should filter out relationships not matching the end node") {
    // given
    mockRelationships(relationship1, relationship2)

    val left = newMockedPipe("a",
      row("a" -> startNode, "b" -> endNode2))

    val pipe = OptionalExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())()
    // when
    val result = pipe.createResults(queryState).toList
    pipe.close(true)

    // then
    val (single :: Nil) = result
    single.m should equal(Map("a" -> startNode, "r" -> relationship2, "b" -> endNode2))
  }

  test("should support optional expand from a node with relationships that do not match the predicates") {
    // given
    mockRelationships(relationship1)
    val left = newMockedPipe("a",
      row("a" -> startNode, "b" -> endNode1))

    val falsePredicate: Predicate = Not(True())
    val pipe = OptionalExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, falsePredicate)()
    // when
    val result = pipe.createResults(queryState).toList
    pipe.close(true)

    // then
    val (single :: Nil) = result
    single.m should equal(Map("a" -> startNode, "r" -> null, "b" -> endNode1))
  }

  test("should support expand between two nodes with multiple relationships") {
    // given
    mockRelationships(relationship1, relationship2, relationship3)
    val left = newMockedPipe("a",
      row("a" -> startNode, "b" -> endNode1),
      row("a" -> startNode, "b" -> endNode2))

    val pipe = OptionalExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())()
    // when
    val result = pipe.createResults(queryState).toList
    pipe.close(true)

    // then
    val (first :: second :: Nil) = result
    first.m should equal(Map("a" -> startNode, "r" -> relationship1, "b" -> endNode1))
    second.m should equal(Map("a" -> startNode, "r" -> relationship2, "b" -> endNode2))
  }

  test("should support expand between two nodes with multiple relationships and self loops") {
    // given
    mockRelationships(relationship1, selfRelationship, relationship3)
    val left = newMockedPipe("a",
      row("a" -> startNode, "b" -> endNode1),
      row("a" -> startNode, "b" -> startNode)
    )

    val pipe = OptionalExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())()
    // when
    val result = pipe.createResults(queryState).toList
    pipe.close(true)

    // then
    val (first :: second :: Nil) = result
    first.m should equal(Map("a" -> startNode, "r" -> relationship1, "b" -> endNode1))
    second.m should equal(Map("a" -> startNode, "r" -> selfRelationship, "b" -> startNode))
  }

  test("given empty input, should return empty output") {
    // given
    mockRelationships()
    val left = newMockedPipe("a")

    val pipe = OptionalExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())()
    // when
    val result = pipe.createResults(queryState).toList
    pipe.close(true)

    // then
    result shouldBe 'empty
  }

  test("expand into null should return nulled row") {
    // given
    val node: Node = mock[Node]
    val input = new FakePipe(Iterator(Map("a" -> node, "b" -> null)))

    val pipe = OptionalExpandIntoPipe(input, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())()
    // when
    val result: List[ExecutionContext] = pipe.createResults(queryState).toList
    pipe.close(true)

    // then
    result should equal(List(Map("a" -> node, "r" -> null, "b" -> null)))
  }

  test("expand null into something should return nulled row") {
    // given
    val node: Node = mock[Node]
    val input = new FakePipe(Iterator(Map("a" -> null, "b" -> node)))

    // when
    val result: List[ExecutionContext] = OptionalExpandIntoPipe(input, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())().createResults(queryState).toList

    // then
    result should equal(List(Map("a" -> null, "r" -> null, "b" -> node)))
  }

  test("expand null into null should return nulled row") {
    // given
    val input = new FakePipe(Iterator(Map("a" -> null, "b" -> null)))

    // when
    val result: List[ExecutionContext] = OptionalExpandIntoPipe(input, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, True())().createResults(queryState).toList

    // then
    result should equal(List(Map("a" -> null, "r" -> null, "b" -> null)))
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
    when(predicate.isTrue(any[ExecutionContext])(any[QueryState]))
      .thenReturn(true)
      .thenReturn(false)
    val pipe = OptionalExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty, predicate)()
    // when
    val result = pipe.createResults(queryState).toList
    pipe.close(true)

    // then
    result.toList should equal(List(
      Map("a" -> startNode, "b" -> endNode1, "r" -> rel1)))
  }

  private def mockRelationships(rels: Relationship*) {
    when(query.getRelationshipsForIds(any(), any(), any())).thenAnswer(new Answer[Iterator[Relationship] with AutoCloseable] {
      def answer(invocation: InvocationOnMock): Iterator[Relationship] with AutoCloseable =
        new Iterator[Relationship] with AutoCloseable {
          private val inner = rels.iterator
          override def hasNext: Boolean = inner.hasNext

          override def next(): Relationship = inner.next()

          override def close(): Unit = ()
        }
    })
  }

  private def row(values: (String, Any)*) = ExecutionContext.from(values: _*)

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
