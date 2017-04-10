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

import org.mockito.Matchers
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compiler.v3_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_2.spi.QueryContext
import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.graphdb.{Node, Relationship}

class ExpandAllPipeTest extends CypherFunSuite {

  implicit val monitor = mock[PipeMonitor]
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
    val left = newMockedPipe("a", row("a" -> startNode))

    val pipe = ExpandAllPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty)()
    // when
    val result = pipe.createResults(queryState).toList
    pipe.close(true)

    // then
    val (single :: Nil) = result
    single.m should equal(Map("a" -> startNode, "r" -> relationship1, "b" -> endNode1))
  }

  test("should return no relationships for types that have not been defined yet") {
    // given
    when(query.getRelationshipsForIds(any(), any(), Matchers.eq(Some(Seq.empty)))).thenAnswer(new Answer[Iterator[Relationship] with AutoCloseable]{
      override def answer(invocationOnMock: InvocationOnMock): Iterator[Relationship] with AutoCloseable =
      new Iterator[Relationship] with AutoCloseable {
        override def hasNext: Boolean = Iterator.empty.hasNext

        override def next(): Relationship = Iterator.empty.next()

        override def close(): Unit = ()
      }
    })
    when(query.getRelationshipsForIds(any(), any(), Matchers.eq(Some(Seq(1,2))))).thenAnswer(new Answer[Iterator[Relationship] with AutoCloseable]{
        override def answer(invocationOnMock: InvocationOnMock): Iterator[Relationship] with AutoCloseable =
          new Iterator[Relationship] with AutoCloseable {
            private val inner = Iterator(relationship1, relationship2)
            override def hasNext: Boolean = inner.hasNext

            override def next(): Relationship = inner.next()

            override def close(): Unit = ()
          }
    })

    val pipe = ExpandAllPipe(newMockedPipe("a", row("a"-> startNode)), "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes(Seq("FOO", "BAR")))()

    // when
    when(query.getOptRelTypeId("FOO")).thenReturn(None)
    when(query.getOptRelTypeId("BAR")).thenReturn(None)
    val result1 = pipe.createResults(queryState).toList
    pipe.close(true)

    // when
    when(query.getOptRelTypeId("FOO")).thenReturn(Some(1))
    when(query.getOptRelTypeId("BAR")).thenReturn(Some(2))
    val result2 = pipe.createResults(queryState).toList
    pipe.close(true)

    // then
    result1 should be(empty)
    result2 should not be empty
  }

  test("should support expand between two nodes with multiple relationships") {
    // given
    mockRelationships(relationship1, relationship2)
    val left = newMockedPipe("a",
      row("a" -> startNode))

    val pipe = ExpandAllPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty)()
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
    mockRelationships(relationship1, selfRelationship)
    val left = newMockedPipe("a",
      row("a" -> startNode))

    val pipe = ExpandAllPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty)()
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
    val left = newMockedPipe("a", row("a" -> null))

    val pipe = ExpandAllPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty)()
    // when
    val result = pipe.createResults(queryState).toList
    pipe.close(true)

    // then
    result should be (empty)
  }

  test("given a null start point, returns an empty iterator") {
    // given
    mockRelationships(relationship1)
    val left = newMockedPipe("a",
      row("a" -> startNode))

    val pipe = ExpandAllPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty)()
    // when
    val result = pipe.createResults(queryState).toList
    pipe.close(true)

    // then
    val (single :: Nil) = result
    single.m should equal(Map("a" -> startNode, "r" -> relationship1, "b" -> endNode1))
  }

  private def row(values: (String, Any)*) = ExecutionContext.from(values: _*)

  private def mockRelationships(rels: Relationship*) {
    when(query.getRelationshipsForIds(any(), any(), any())).thenAnswer(new Answer[Iterator[Relationship] with AutoCloseable] {
      def answer(invocation: InvocationOnMock): Iterator[Relationship] with AutoCloseable =
        new Iterator[Relationship] with AutoCloseable {
          private val iterator = rels.iterator

          override def hasNext: Boolean = iterator.hasNext

          override def next(): Relationship = iterator.next()

          override def close(): Unit = ()
        }
    })
  }

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
