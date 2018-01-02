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
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.{Node, Relationship}

class ProjectEndpointsPipeTest extends CypherFunSuite {

  implicit val monitor = mock[PipeMonitor]
  val node1 = newMockedNode(1)
  val node2 = newMockedNode(2)
  val node3 = newMockedNode(3)
  val node4 = newMockedNode(4)

  val query = mock[QueryContext]
  val queryState = QueryStateHelper.emptyWith(query = query)

  test("projects endpoints of a directed, simple relationship") {
    // given

    val rel = newMockedRelationship(12, node1, node2)
    when(query.relationshipStartNode(rel)).thenReturn(node1)
    when(query.relationshipEndNode(rel)).thenReturn(node2)

    val left = newMockedPipe("r",
      row("r" -> rel)
    )

    // when
    val result =
      ProjectEndpointsPipe(left, "r", "a", startInScope = false, "b", endInScope = false, None, directed = true, simpleLength = true)().
        createResults(queryState).toList

    // then
    result should equal(List(Map("r" -> rel, "a" -> node1, "b" -> node2)))
  }

  test("projects endpoints of a directed, simple relationship with start in scope which doesn't match") {
    // given

    val rel = newMockedRelationship(12, node1, node2)
    when(query.relationshipStartNode(rel)).thenReturn(node1)
    when(query.relationshipEndNode(rel)).thenReturn(node2)

    val left = newMockedPipe("r",
      row("r" -> rel, "a" -> node2)
    )

    // when
    val result =
      ProjectEndpointsPipe(left, "r", "a", startInScope = true, "b", endInScope = false, None, directed = true, simpleLength = true)().
        createResults(queryState).toList

    // then
    result should be(empty)
  }

  test("should work if in scope there are non-node object with the same name as the start node") {
    // given

    val rel = newMockedRelationship(12, node1, node2)
    when(query.relationshipStartNode(rel)).thenReturn(node1)
    when(query.relationshipEndNode(rel)).thenReturn(node2)

    val left = newMockedPipe("r",
      row("r" -> rel, "a" -> 42)
    )

    // when
    val result =
      ProjectEndpointsPipe(left, "r", "a", startInScope = true, "b", endInScope = false, None, directed = true, simpleLength = true)().
        createResults(queryState).toList

    // then
    result should be(empty)
  }

  test("projects endpoints of a directed, simple relationship with type") {
    // given

    val rel1 = newMockedRelationship(12, node1, node2, Some("A"))
    val rel2 = newMockedRelationship(12, node3, node4, Some("B"))
    when(query.relationshipStartNode(rel1)).thenReturn(node1)
    when(query.relationshipEndNode(rel1)).thenReturn(node2)
    when(query.relationshipStartNode(rel2)).thenReturn(node3)
    when(query.relationshipEndNode(rel2)).thenReturn(node4)

    val left = newMockedPipe("r",
      row("r" -> rel1),
      row("r" -> rel2)
    )

    // when
    val result =
      ProjectEndpointsPipe(left, "r", "a", startInScope = false, "b", endInScope = false, Some(LazyTypes(Seq("A"))), directed = true, simpleLength = true)().
        createResults(queryState).toList

    // then
    result should equal(List(Map("r" -> rel1, "a" -> node1, "b" -> node2)))
  }

  test("projects endpoints of a directed, simple relationship with start in scope") {
    // given

    val rel = newMockedRelationship(12, node1, node2)
    when(query.relationshipStartNode(rel)).thenReturn(node1)
    when(query.relationshipEndNode(rel)).thenReturn(node2)

    val left = newMockedPipe("r",
      row("r" -> rel, "a" -> node1),
      row("r" -> rel, "a" -> node3)
    )

    // when
    val result =
      ProjectEndpointsPipe(left, "r", "a", startInScope = true, "b", endInScope = false, None, directed = true, simpleLength = true)().
        createResults(queryState).toList

    // then
    result should equal(List(Map("r" -> rel, "a" -> node1, "b" -> node2)))
  }

  test("projects endpoints of a directed, simple relationship with end in scope") {
    // given

    val rel = newMockedRelationship(12, node1, node2)
    when(query.relationshipStartNode(rel)).thenReturn(node1)
    when(query.relationshipEndNode(rel)).thenReturn(node2)

    val left = newMockedPipe("r",
      row("r" -> rel, "b" -> node2),
      row("r" -> rel, "b" -> node3)
    )

    // when
    val result =
      ProjectEndpointsPipe(left, "r", "a", startInScope = false, "b", endInScope = true, None, directed = true, simpleLength = true)().
        createResults(queryState).toList

    // then
    result should equal(List(Map("r" -> rel, "a" -> node1, "b" -> node2)))
  }

  test("projects endpoints of an undirected, simple relationship") {
    // given

    val rel = newMockedRelationship(12, node1, node2)
    when(query.relationshipStartNode(rel)).thenReturn(node1)
    when(query.relationshipEndNode(rel)).thenReturn(node2)

    val left = newMockedPipe("r",
      row("r" -> rel)
    )

    // when
    val result =
      ProjectEndpointsPipe(left, "r", "a", startInScope = false, "b", endInScope = false,  None, directed = false, simpleLength = true)().
        createResults(queryState).toList

    // then
    result should equal(List(
      Map("r" -> rel, "a" -> node1, "b" -> node2),
      Map("r" -> rel, "a" -> node2, "b" -> node1)
    ))
  }

  test("projects endpoints of an undirected, simple relationship with start in scope") {
    // given

    val rel = newMockedRelationship(12, node1, node2)
    when(query.relationshipStartNode(rel)).thenReturn(node1)
    when(query.relationshipEndNode(rel)).thenReturn(node2)

    val left = newMockedPipe("r",
      row("r" -> rel, "a" -> node1),
      row("r" -> rel, "a" -> node2)
    )

    // when
    val result =
      ProjectEndpointsPipe(left, "r", "a", startInScope = true, "b", endInScope = false, None, directed = false, simpleLength = true)().
        createResults(queryState).toList

    // then
    result should equal(List(
      Map("r" -> rel, "a" -> node1, "b" -> node2),
      Map("r" -> rel, "a" -> node2, "b" -> node1)
    ))
  }

  test("projects endpoints of an undirected, simple relationship with type") {
    // given

    val rel1 = newMockedRelationship(12, node1, node2, Some("A"))
    val rel2 = newMockedRelationship(12, node3, node4, Some("B"))
    when(query.relationshipStartNode(rel1)).thenReturn(node1)
    when(query.relationshipEndNode(rel1)).thenReturn(node2)
    when(query.relationshipStartNode(rel2)).thenReturn(node3)
    when(query.relationshipEndNode(rel2)).thenReturn(node4)

    val left = newMockedPipe("r",
      row("r" -> rel1),
      row("r" -> rel2)
    )

    // when
    val result =
      ProjectEndpointsPipe(left, "r", "a", startInScope = false, "b", endInScope = false, Some(LazyTypes(Seq("B"))), directed = false, simpleLength = true)().
        createResults(queryState).toList

    // then
    result should equal(List(
      Map("r" -> rel2, "a" -> node3, "b" -> node4),
      Map("r" -> rel2, "a" -> node4, "b" -> node3)
    ))
  }

  test("projects endpoints of a directed, var length relationship") {
    // given

    val rel1 = newMockedRelationship(12, node1, node2)
    val rel2 = newMockedRelationship(23, node2, node3)
    val rel3 = newMockedRelationship(34, node3, node4)

    when(query.relationshipStartNode(rel1)).thenReturn(node1)
    when(query.relationshipEndNode(rel1)).thenReturn(node2)
    when(query.relationshipStartNode(rel2)).thenReturn(node2)
    when(query.relationshipEndNode(rel2)).thenReturn(node3)
    when(query.relationshipStartNode(rel3)).thenReturn(node3)
    when(query.relationshipEndNode(rel3)).thenReturn(node4)

    val rels = Seq(rel1, rel2, rel3)
    val left = newMockedPipe("r",
      row("r" -> rels)
    )

    // when
    val result =
      ProjectEndpointsPipe(left, "r", "a", startInScope = false, "b", endInScope = false, None, directed = true, simpleLength = false)().
        createResults(queryState).toList

    // then
    result should equal(List(
      Map("r" -> rels, "a" -> node1, "b" -> node4)
    ))
  }

  test("projects endpoints of a directed, var length relationship with both start and end in scope") {
    // given

    val rel1 = newMockedRelationship(12, node1, node2)
    val rel2 = newMockedRelationship(23, node2, node3)
    val rel3 = newMockedRelationship(34, node3, node4)

    when(query.relationshipStartNode(rel1)).thenReturn(node1)
    when(query.relationshipEndNode(rel1)).thenReturn(node2)
    when(query.relationshipStartNode(rel2)).thenReturn(node2)
    when(query.relationshipEndNode(rel2)).thenReturn(node3)
    when(query.relationshipStartNode(rel3)).thenReturn(node3)
    when(query.relationshipEndNode(rel3)).thenReturn(node4)

    val rels = Seq(rel1, rel2, rel3)
    val left = newMockedPipe("r",
      row("r" -> rels, "a" -> node1, "b" -> node4)
    )

    // when
    val result =
      ProjectEndpointsPipe(left, "r", "a", startInScope = true, "b", endInScope = true, None, directed = true, simpleLength = false)().
        createResults(queryState).toList

    // then
    result should equal(List(
      Map("r" -> rels, "a" -> node1, "b" -> node4)
    ))
  }

  test("projects endpoints of a directed, var length relationship with types") {
    // given

    val rel1 = newMockedRelationship(12, node1, node2, Some("A"))
    val rel2 = newMockedRelationship(23, node2, node3, Some("A"))
    val rel3 = newMockedRelationship(34, node3, node4, Some("A"))

    when(query.relationshipStartNode(rel1)).thenReturn(node1)
    when(query.relationshipEndNode(rel1)).thenReturn(node2)
    when(query.relationshipStartNode(rel2)).thenReturn(node2)
    when(query.relationshipEndNode(rel2)).thenReturn(node3)
    when(query.relationshipStartNode(rel3)).thenReturn(node3)
    when(query.relationshipEndNode(rel3)).thenReturn(node4)

    val rels = Seq(rel1, rel2, rel3)
    val left = newMockedPipe("r",
      row("r" -> rels)
    )

    // when
    val result =
      ProjectEndpointsPipe(left, "r", "a", startInScope = false, "b", endInScope = false, Some(LazyTypes(Seq("A"))), directed = true, simpleLength = false)().
        createResults(queryState).toList

    // then
    result should equal(List(
      Map("r" -> rels, "a" -> node1, "b" -> node4)
    ))
  }

  test("projects endpoints of a directed, var length relationship with different types") {
    // given

    val rel1 = newMockedRelationship(12, node1, node2, Some("A"))
    val rel2 = newMockedRelationship(23, node2, node3, Some("B"))
    val rel3 = newMockedRelationship(34, node3, node4, Some("A"))

    when(query.relationshipStartNode(rel1)).thenReturn(node1)
    when(query.relationshipEndNode(rel1)).thenReturn(node2)
    when(query.relationshipStartNode(rel2)).thenReturn(node2)
    when(query.relationshipEndNode(rel2)).thenReturn(node3)
    when(query.relationshipStartNode(rel3)).thenReturn(node3)
    when(query.relationshipEndNode(rel3)).thenReturn(node4)

    val rels = Seq(rel1, rel2, rel3)
    val left = newMockedPipe("r",
      row("r" -> rels)
    )

    // when
    val result =
      ProjectEndpointsPipe(left, "r", "a", startInScope = false, "b", endInScope = false, Some(LazyTypes(Seq("A"))), directed = true, simpleLength = false)().
        createResults(queryState).toList

    // then
    result should be(empty)
  }

  test("projects endpoints of an undirected, var length relationship") {
    // given

    val rel1 = newMockedRelationship(12, node1, node2)
    val rel2 = newMockedRelationship(23, node2, node3)
    val rel3 = newMockedRelationship(34, node3, node4)

    when(query.relationshipStartNode(rel1)).thenReturn(node1)
    when(query.relationshipEndNode(rel1)).thenReturn(node2)
    when(query.relationshipStartNode(rel2)).thenReturn(node2)
    when(query.relationshipEndNode(rel2)).thenReturn(node3)
    when(query.relationshipStartNode(rel3)).thenReturn(node3)
    when(query.relationshipEndNode(rel3)).thenReturn(node4)

    val rels = Seq(rel1, rel2, rel3)
    val reversedRels = Seq(rel3, rel2, rel1)

    val left = newMockedPipe("r",
      row("r" -> rels)
    )

    // when
    val result =
      ProjectEndpointsPipe(left, "r", "a", startInScope = false, "b", endInScope = false, None, directed = false, simpleLength = false)().
      createResults(queryState).toList

    // then
    result should equal(List(
      Map("r" -> rels, "a" -> node1, "b" -> node4),
      Map("r" -> reversedRels, "a" -> node4, "b" -> node1)
    ))
  }

  test("projects endpoints of an undirected, var length relationship with end in scope which doesn't match") {
    // given

    val rel1 = newMockedRelationship(12, node1, node2)
    val rel2 = newMockedRelationship(23, node2, node3)
    val rel3 = newMockedRelationship(34, node3, node4)

    when(query.relationshipStartNode(rel1)).thenReturn(node1)
    when(query.relationshipEndNode(rel1)).thenReturn(node2)
    when(query.relationshipStartNode(rel2)).thenReturn(node2)
    when(query.relationshipEndNode(rel2)).thenReturn(node3)
    when(query.relationshipStartNode(rel3)).thenReturn(node3)
    when(query.relationshipEndNode(rel3)).thenReturn(node4)

    val rels = Seq(rel1, rel2, rel3)
    val reversedRels = Seq(rel3, rel2, rel1)

    val left = newMockedPipe("r",
      row("r" -> rels, "b" -> node3)
    )

    // when
    val result =
      ProjectEndpointsPipe(left, "r", "a", startInScope = false, "b", endInScope = true, None, directed = false, simpleLength = false)().
      createResults(queryState).toList

    // then
    result should be(empty)
  }

  test("projects endpoints of an undirected, var length relationship with end in scope") {
    // given

    val rel1 = newMockedRelationship(12, node1, node2)
    val rel2 = newMockedRelationship(23, node2, node3)
    val rel3 = newMockedRelationship(34, node3, node4)

    when(query.relationshipStartNode(rel1)).thenReturn(node1)
    when(query.relationshipEndNode(rel1)).thenReturn(node2)
    when(query.relationshipStartNode(rel2)).thenReturn(node2)
    when(query.relationshipEndNode(rel2)).thenReturn(node3)
    when(query.relationshipStartNode(rel3)).thenReturn(node3)
    when(query.relationshipEndNode(rel3)).thenReturn(node4)

    val rels = Seq(rel1, rel2, rel3)
    val reversedRels = Seq(rel3, rel2, rel1)

    val left = newMockedPipe("r",
      row("r" -> rels, "b" -> node1),
      row("r" -> rels, "b" -> node4)
    )

    // when
    val result =
      ProjectEndpointsPipe(left, "r", "a", startInScope = false, "b", endInScope = true, None, directed = false, simpleLength = false)().
        createResults(queryState).toList

    // then
    result should equal(List(
      Map("r" -> rels, "a" -> node1, "b" -> node4),
      Map("r" -> reversedRels, "a" -> node4, "b" -> node1)
    ))
  }

  test("projects no endpoints of an empty directed var length relationship") {
    // given
    val rels = Seq.empty

    val left = newMockedPipe("r",
      row("r" -> rels)
    )

    // when
    val result =
      ProjectEndpointsPipe(left, "r", "a", startInScope = false, "b", endInScope = false, None, directed = true, simpleLength = false)().
        createResults(queryState).toList

    // then
    result should be('isEmpty)
  }

  test("projects no endpoints of an empty undirected var length relationship") {
    // given
    val rels = Seq.empty

    val left = newMockedPipe("r",
      row("r" -> rels)
    )

    // when
    val result =
      ProjectEndpointsPipe(left, "r", "a", startInScope = false, "b", endInScope = false, None, directed = false, simpleLength = false)().
        createResults(queryState).toList

    // then
    result should be('isEmpty)
  }

  private def row(values: (String, Any)*) = ExecutionContext.from(values: _*)

  private def newMockedNode(id: Int) = {
    val node = mock[Node]
    when(node.getId).thenReturn(id)
    node
  }

  private def newMockedRelationship(id: Int, startNode: Node, endNode: Node, relType: Option[String] = None): Relationship = {
    val relationship = mock[Relationship]
    when(relationship.getId).thenReturn(id)
    when(relationship.getStartNode).thenReturn(startNode)
    when(relationship.getEndNode).thenReturn(endNode)
    when(relationship.getOtherNode(startNode)).thenReturn(endNode)
    when(relationship.getOtherNode(endNode)).thenReturn(startNode)
    val relationshipType = relType.map(newMockRelationshipType).orNull
    when(relationship.getType).thenReturn(relationshipType)
    relationship
  }

  private def newMockRelationshipType(relType: String) = {
    val relationshipType = mock[org.neo4j.graphdb.RelationshipType]
    when(relationshipType.name()).thenReturn(relType)
    relationshipType
  }

  private def newMockedPipe(rel: String, rows: ExecutionContext*): Pipe = {
    val pipe = mock[Pipe]
    when(pipe.sources).thenReturn(Seq.empty)
    when(pipe.symbols).thenReturn(SymbolTable(Map(rel -> CTRelationship)))
    when(pipe.createResults(any())).thenAnswer(new Answer[Iterator[ExecutionContext]] {
      def answer(invocation: InvocationOnMock): Iterator[ExecutionContext] = rows.iterator
    })

    pipe
  }
}
