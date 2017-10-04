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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes

import org.mockito.Mockito
import org.neo4j.cypher.ValueComparisonHelper._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions.{ListLiteral, Literal}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.spi.v3_4.{Operations, QueryContext}
import org.neo4j.graphdb.{Node, Relationship}

class DirectedDirectedRelationshipByIdSeekPipeTest extends CypherFunSuite {

  import Mockito.when

  test("should seek relationship by id") {
    // given
    val (startNode, rel, endNode) = getRelWithNodes
    val relOps= mock[Operations[Relationship]]
    when(relOps.getByIdIfExists(17)).thenReturn(Some(rel))

    val to = "to"
    val from = "from"
    val queryState = QueryStateHelper.emptyWith(
      query = when(mock[QueryContext].relationshipOps).thenReturn(relOps).getMock[QueryContext]
    )

    // when
    val result: Iterator[ExecutionContext] =
      DirectedRelationshipByIdSeekPipe("a", SingleSeekArg(Literal(17)), to, from)().createResults(queryState)

    // then
    result.toList should beEquivalentTo(List(Map("a" -> rel, "to" -> endNode, "from" -> startNode)))
  }

  test("should seek relationships by multiple ids") {
    // given
    val (s1, r1, e1) = getRelWithNodes
    val (s2, r2, e2) = getRelWithNodes
    val relationshipOps = mock[Operations[Relationship]]
    val to = "to"
    val from = "from"

    when(relationshipOps.getByIdIfExists(42)).thenReturn(Some(r1))
    when(relationshipOps.getByIdIfExists(21)).thenReturn(Some(r2))
    val queryState = QueryStateHelper.emptyWith(
      query = when(mock[QueryContext].relationshipOps).thenReturn(relationshipOps).getMock[QueryContext]
    )

    val relName = "a"
    // whens
    val result =
      DirectedRelationshipByIdSeekPipe(relName, ManySeekArgs(ListLiteral(Literal(42), Literal(21))), to, from)().createResults(queryState)

    // then
    result.toList should beEquivalentTo(List(
      Map(relName -> r1, to -> e1, from -> s1),
      Map(relName -> r2, to -> e2, from -> s2)
    ))
  }

  test("handle null") {
    // given
    val to = "to"
    val from = "from"
    val relationshipOps = mock[Operations[Relationship]]
    val queryState = QueryStateHelper.emptyWith(
      query = when(mock[QueryContext].relationshipOps).thenReturn(relationshipOps).getMock[QueryContext]
    )

    // when
    val result: Iterator[ExecutionContext] =
      DirectedRelationshipByIdSeekPipe("a", SingleSeekArg(Literal(null)), to, from)().createResults(queryState)

    // then
    result.toList should be(empty)
  }

  private def getRelWithNodes:(Node,Relationship,Node) = {
    val rel = mock[Relationship]
    val startNode = mock[Node]
    val endNode = mock[Node]
    when(rel.getStartNode).thenReturn(startNode)
    when(rel.getEndNode).thenReturn(endNode)
    (startNode, rel, endNode)
  }

}
