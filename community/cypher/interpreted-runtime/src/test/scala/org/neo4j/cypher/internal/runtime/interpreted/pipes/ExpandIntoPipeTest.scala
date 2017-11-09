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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.mockito.ArgumentMatchers.{eq => mockEq, _}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.graphdb.Relationship
import org.neo4j.kernel.impl.util.ValueUtils.{fromNodeProxy, fromRelationshipProxy}

class ExpandIntoPipeTest extends CypherFunSuite with PipeTestSupport {

  val startNode = newMockedNode(1)
  val endNode1 = newMockedNode(2)
  val endNode2 = newMockedNode(3)
  val endNode3 = newMockedNode(4)
  val relationship1 = newMockedRelationship(1, startNode, endNode1)
  val relationship2 = newMockedRelationship(2, startNode, endNode2)
  val relationship3 = newMockedRelationship(3, startNode, endNode3)
  val selfRelationship = newMockedRelationship(4, startNode, startNode)

  val queryState = QueryStateHelper.emptyWith(query = query)

  test("should support expand between two nodes with a relationship") {
    // given
    setUpRelMockingInQueryContext(relationship1)
    val left = newMockedPipe("a",
      row("a" -> startNode, "b" -> endNode1))

    // when
    val result = ExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty)().createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single.toMap should equal(Map("a" -> fromNodeProxy(startNode), "r" -> fromRelationshipProxy(relationship1),
                              "b" -> fromNodeProxy(endNode1)))
  }

  test("should return no relationships for types that have not been defined yet") {
    // given
    when(query.getRelationshipsForIds(any(), any(), any())).thenAnswer(new Answer[Iterator[Relationship]] {
      override def answer(invocationOnMock: InvocationOnMock): Iterator[Relationship] = {
        val arg = invocationOnMock.getArgument[Option[Array[Int]]](2)
        arg match {
          case None => Iterator.empty
          case Some(array) if array.isEmpty => Iterator.empty
          case _ =>
            val nodeId = invocationOnMock.getArgument[Long](0)
            val dir = invocationOnMock.getArgument[SemanticDirection](1)
            (nodeId, dir) match {
              case (0, SemanticDirection.INCOMING) => Iterator.empty
              case (0, _) => Iterator(relationship1, relationship2, relationship3, selfRelationship)
              case (2, SemanticDirection.OUTGOING) => Iterator.empty
              case (2, _) => Iterator(relationship1)
              case (id, d)=> throw new AssertionError(s"I am only a mocked hack, not a real database. I have no clue about $id and $d")
            }
        }
      }
    })
    when(query.nodeGetDegree(any(), any(), any())).thenReturn(1)

    val pipe = ExpandIntoPipe(newMockedPipe("a", row("a"-> startNode, "b" -> endNode1)), "a", "r", "b", SemanticDirection.OUTGOING, new LazyTypes(Array("FOO", "BAR")))()

    // when
    when(query.getOptRelTypeId("FOO")).thenReturn(None)
    when(query.getOptRelTypeId("BAR")).thenReturn(None)
    val result1 = pipe.createResults(queryState).toList

    // when
    when(query.getOptRelTypeId("FOO")).thenReturn(Some(1))
    when(query.getOptRelTypeId("BAR")).thenReturn(Some(2))
    val result2 = pipe.createResults(queryState).toList

    // then
    result1 should be(empty)
    result2 should not be empty
  }

  test("should support expand between two nodes with multiple relationships") {
    // given
    setUpRelMockingInQueryContext(relationship1, relationship2, relationship3)
    val left = newMockedPipe("a",
      row("a" -> startNode, "b" -> endNode1),
      row("a" -> startNode, "b" -> endNode2)
    )

    // when
    val result = ExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty)().createResults(queryState).toList

    // then
    val (first :: second :: Nil) = result
    first.toMap should equal(Map("a" -> fromNodeProxy(startNode), "r" -> fromRelationshipProxy(relationship1), "b" -> fromNodeProxy(endNode1)))
    second.toMap should equal(Map("a" -> fromNodeProxy(startNode), "r" -> fromRelationshipProxy(relationship2), "b" -> fromNodeProxy(endNode2)))
  }

  test("should support expand between two nodes with multiple relationships and self loops") {
    // given
    setUpRelMockingInQueryContext(relationship1, selfRelationship, relationship3)
    val left = newMockedPipe("a",
      row("a" -> startNode, "b" -> endNode1),
      row("a" -> startNode, "b" -> startNode)
    )

    // when
    val result = ExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty)().createResults(queryState).toList

    // then
    val (first :: second :: Nil) = result
    first.toMap should equal(Map("a" -> fromNodeProxy(startNode), "r" -> fromRelationshipProxy(relationship1), "b" -> fromNodeProxy(endNode1)))
    second.toMap should equal(Map("a" -> fromNodeProxy(startNode), "r" -> fromRelationshipProxy(selfRelationship), "b" -> fromNodeProxy(startNode)))
  }

  test("given empty input, should return empty output") {
    // given
    setUpRelMockingInQueryContext()
    val left = newMockedPipe("a", row("a" -> null, "b" -> null))

    // when
    val result = ExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty)().createResults(queryState).toList

    // then
    result should be (empty)
  }

  test("given a null start point, returns an empty iterator") {
    // given
    setUpRelMockingInQueryContext(relationship1)
    val left = newMockedPipe("a",
      row("a" -> null, "b" -> endNode1))

    // when
    val result = ExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty)().createResults(queryState).toList

    // then
    result shouldBe empty
  }

  test("given a null end point, returns an empty iterator") {
    // given
    setUpRelMockingInQueryContext(relationship1)
    val left = newMockedPipe("a",
      row("a" -> startNode, "b" -> null))

    // when
    val result = ExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty)().createResults(queryState).toList

    // then
    result shouldBe empty
  }

  test("issue 4692 should respect relationship direction") {
    // Given
    val node0 = newMockedNode(0)
    val node1 = newMockedNode(1)
    val rel0 = newMockedRelationship(0, node0, node1)
    val rel1 = newMockedRelationship(1, node1, node0)

    setUpRelMockingInQueryContext(rel0, rel1)

    val source = newMockedPipe(
      Map("n" -> CTNode, "r2" -> CTRelationship, "k" -> CTNode),
      row("n" -> node1, "r2" -> rel1, "k" -> node0),
      row("n" -> node0, "r2" -> rel0, "k" -> node1))

    // When
    val results = ExpandIntoPipe(source, "n", "r1", "k", SemanticDirection.OUTGOING, LazyTypes.empty)().createResults(queryState).toList

    // Then
    results should contain theSameElementsAs List(
      Map("n" -> fromNodeProxy(node1), "k" -> fromNodeProxy(node0), "r1" -> fromRelationshipProxy(rel1), "r2" -> fromRelationshipProxy(rel1)),
      Map("n" -> fromNodeProxy(node0), "k" -> fromNodeProxy(node1), "r1" -> fromRelationshipProxy(rel0), "r2" -> fromRelationshipProxy(rel0)))
  }

  test("should work for bidirectional relationships") {
    // Given
    val node0 = newMockedNode(0)
    val node1 = newMockedNode(1)
    val rel0 = newMockedRelationship(0, node0, node1)
    val rel1 = newMockedRelationship(1, node1, node0)

    setUpRelMockingInQueryContext(rel0, rel1)

    val source = newMockedPipe(
      Map("n" -> CTNode, "r2" -> CTRelationship, "k" -> CTNode),
      row("n" -> node1, "r2" -> rel1, "k" -> node0),
      row("n" -> node0, "r2" -> rel0, "k" -> node1))

    // When
    val results = ExpandIntoPipe(source, "n", "r1", "k", SemanticDirection.BOTH, LazyTypes.empty)().createResults(queryState).toList

    // Then
    results should contain theSameElementsAs List(
      Map("n" -> fromNodeProxy(node1), "k" -> fromNodeProxy(node0), "r1" -> fromRelationshipProxy(rel0), "r2" -> fromRelationshipProxy(rel1)),
      Map("n" -> fromNodeProxy(node1), "k" -> fromNodeProxy(node0), "r1" -> fromRelationshipProxy(rel1), "r2" -> fromRelationshipProxy(rel1)),
      Map("n" -> fromNodeProxy(node0), "k" -> fromNodeProxy(node1), "r1" -> fromRelationshipProxy(rel1), "r2" -> fromRelationshipProxy(rel0)),
      Map("n" -> fromNodeProxy(node0), "k" -> fromNodeProxy(node1), "r1" -> fromRelationshipProxy(rel0), "r2" -> fromRelationshipProxy(rel0)))

    // relationships should be cached after the first call
    verify(query, times(1)).getRelationshipsForIds(any(), mockEq(SemanticDirection.BOTH), mockEq(None))
  }

}
