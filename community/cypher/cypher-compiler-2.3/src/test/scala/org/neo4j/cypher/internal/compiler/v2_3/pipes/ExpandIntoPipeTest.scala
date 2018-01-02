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

import org.mockito.Matchers.{eq => mockEq, _}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.{ Node, Relationship}

class ExpandIntoPipeTest extends CypherFunSuite {

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
    setUpRelMockingInQuertyContext(relationship1)
    val left = newMockedPipe("a",
      row("a" -> startNode, "b" -> endNode1))

    // when
    val result = ExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty)().createResults(queryState).toList

    // then
    val (single :: Nil) = result
    single.m should equal(Map("a" -> startNode, "r" -> relationship1, "b" -> endNode1))
  }

  test("should return no relationships for types that have not been defined yet") {
    // given
    when(query.getRelationshipsForIds(any(), any(), mockEq(Some(Seq.empty)))).thenAnswer(
      new Answer[Iterator[Relationship]] {
        override def answer(invocationOnMock: InvocationOnMock) = Iterator.empty
      })
    when(query.getRelationshipsForIds(any(), any(), mockEq(Some(Seq(1, 2))))).thenAnswer(
      new Answer[Iterator[Relationship]] {
        override def answer(invocationOnMock: InvocationOnMock) = Iterator(relationship1, relationship2)
      })
    when(query.nodeGetDegree(any(), any(), any())).thenReturn(1)

    val pipe = ExpandIntoPipe(newMockedPipe("a", row("a"-> startNode, "b" -> endNode1)), "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes(Seq("FOO", "BAR")))()

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
    setUpRelMockingInQuertyContext(relationship1, relationship2, relationship3)
    val left = newMockedPipe("a",
      row("a" -> startNode, "b" -> endNode1),
      row("a" -> startNode, "b" -> endNode2)
    )

    // when
    val result = ExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty)().createResults(queryState).toList

    // then
    val (first :: second :: Nil) = result
    first.m should equal(Map("a" -> startNode, "r" -> relationship1, "b" -> endNode1))
    second.m should equal(Map("a" -> startNode, "r" -> relationship2, "b" -> endNode2))
  }

  test("should support expand between two nodes with multiple relationships and self loops") {
    // given
    setUpRelMockingInQuertyContext(relationship1, selfRelationship, relationship3)
    val left = newMockedPipe("a",
      row("a" -> startNode, "b" -> endNode1),
      row("a" -> startNode, "b" -> startNode)
    )

    // when
    val result = ExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty)().createResults(queryState).toList

    // then
    val (first :: second :: Nil) = result
    first.m should equal(Map("a" -> startNode, "r" -> relationship1, "b" -> endNode1))
    second.m should equal(Map("a" -> startNode, "r" -> selfRelationship, "b" -> startNode))
  }

  test("given empty input, should return empty output") {
    // given
    setUpRelMockingInQuertyContext()
    val left = newMockedPipe("a", row("a" -> null, "b" -> null))

    // when
    val result = ExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty)().createResults(queryState).toList

    // then
    result should be (empty)
  }

  test("given a null start point, returns an empty iterator") {
    // given
    setUpRelMockingInQuertyContext(relationship1)
    val left = newMockedPipe("a",
      row("a" -> null, "b" -> endNode1))

    // when
    val result = ExpandIntoPipe(left, "a", "r", "b", SemanticDirection.OUTGOING, LazyTypes.empty)().createResults(queryState).toList

    // then
    result shouldBe empty
  }

  test("given a null end point, returns an empty iterator") {
    // given
    setUpRelMockingInQuertyContext(relationship1)
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

    setUpRelMockingInQuertyContext(rel0, rel1)

    val source = newMockedPipe(
      Map("n" -> CTNode, "r2" -> CTRelationship, "k" -> CTNode),
      row("n" -> node1, "r2" -> rel1, "k" -> node0),
      row("n" -> node0, "r2" -> rel0, "k" -> node1))

    // When
    val results = ExpandIntoPipe(source, "n", "r1", "k", SemanticDirection.OUTGOING, LazyTypes.empty)().createResults(queryState).toList

    // Then
    results should contain theSameElementsAs List(
      Map("n" -> node1, "k" -> node0, "r1" -> rel1, "r2" -> rel1),
      Map("n" -> node0, "k" -> node1, "r1" -> rel0, "r2" -> rel0))
  }

  test("should work for bidirectional relationships") {
    // Given
    val node0 = newMockedNode(0)
    val node1 = newMockedNode(1)
    val rel0 = newMockedRelationship(0, node0, node1)
    val rel1 = newMockedRelationship(1, node1, node0)

    setUpRelMockingInQuertyContext(rel0, rel1)

    val source = newMockedPipe(
      Map("n" -> CTNode, "r2" -> CTRelationship, "k" -> CTNode),
      row("n" -> node1, "r2" -> rel1, "k" -> node0),
      row("n" -> node0, "r2" -> rel0, "k" -> node1))

    // When
    val results = ExpandIntoPipe(source, "n", "r1", "k", SemanticDirection.BOTH, LazyTypes.empty)().createResults(queryState).toList

    // Then
    results should contain theSameElementsAs List(
      Map("n" -> node1, "k" -> node0, "r1" -> rel0, "r2" -> rel1),
      Map("n" -> node1, "k" -> node0, "r1" -> rel1, "r2" -> rel1),
      Map("n" -> node0, "k" -> node1, "r1" -> rel1, "r2" -> rel0),
      Map("n" -> node0, "k" -> node1, "r1" -> rel0, "r2" -> rel0))

    // relationships should be cached after the first call
    verify(query, times(1)).getRelationshipsForIds(any(), mockEq(SemanticDirection.BOTH), mockEq(None))
  }

  private def row(values: (String, Any)*) = ExecutionContext.from(values: _*)

  private def setUpRelMockingInQuertyContext(rels: Relationship*) {
    val relsByStartNode = rels.groupBy(_.getStartNode)
    val relsByEndNode = rels.groupBy(_.getEndNode)
    val relsByNode = (relsByStartNode.keySet ++ relsByEndNode.keySet).map {
      n => n -> (relsByStartNode.getOrElse(n, Seq.empty) ++ relsByEndNode.getOrElse(n, Seq.empty))
    }.toMap

    setUpRelLookupMocking(SemanticDirection.OUTGOING, relsByStartNode)
    setUpRelLookupMocking(SemanticDirection.INCOMING, relsByEndNode)
    setUpRelLookupMocking(SemanticDirection.BOTH, relsByNode)
  }

  private def setUpRelLookupMocking(direction: SemanticDirection, relsByNode: Map[Node, Seq[Relationship]]) {
    relsByNode.foreach {
      case (node, rels) =>
        when(query.getRelationshipsForIds(node, direction, None)).thenAnswer(
          new Answer[Iterator[Relationship]] {
            def answer(invocation: InvocationOnMock) = rels.iterator
          })

        when(query.nodeGetDegree(node.getId, direction)).thenReturn(rels.size)
    }
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
    newMockedPipe(Map(node -> CTNode), rows: _*)
  }

  private def newMockedPipe(symbols: Map[String, CypherType], rows: ExecutionContext*): Pipe = {
    val pipe = mock[Pipe]

    when(pipe.sources).thenReturn(Seq.empty)
    when(pipe.symbols).thenReturn(SymbolTable(symbols))
    when(pipe.createResults(any())).thenAnswer(new Answer[Iterator[ExecutionContext]] {
      def answer(invocation: InvocationOnMock) = rows.iterator
    })

    pipe
  }
}
