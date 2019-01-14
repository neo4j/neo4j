/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.mockito.Mockito
import org.neo4j.cypher.internal.runtime.interpreted.ValueComparisonHelper.beEquivalentTo
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{ListLiteral, Literal}
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, QueryStateHelper}
import org.neo4j.cypher.internal.runtime.{Operations, QueryContext}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.virtual.{RelationshipValue, NodeValue}

class UndirectedDirectedRelationshipByIdSeekPipeTest extends CypherFunSuite {

  import Mockito.when

  test("should seek relationship by id") {
    // given
    val (startNode, rel, endNode) = getRelWithNodes
    val relOps= mock[Operations[RelationshipValue]]
    when(relOps.getByIdIfExists(17)).thenReturn(Some(rel))

    val to = "to"
    val from = "from"
    val queryContext = mock[QueryContext]
    when(queryContext.relationshipOps).thenReturn(relOps)
    val queryState = QueryStateHelper.emptyWith(query = queryContext)

    // when
    val result: Iterator[ExecutionContext] =
      UndirectedRelationshipByIdSeekPipe("a", SingleSeekArg(Literal(17)), to, from)()
      .createResults(queryState)

    // then
    result.toList should beEquivalentTo(List(
      Map("a" -> rel, "to" -> endNode, "from" -> startNode),
      Map("a" -> rel, "to" -> startNode, "from" -> endNode)))
  }

  test("should seek relationships by multiple ids") {
    // given
    val (s1, r1, e1) = getRelWithNodes
    val (s2, r2, e2) = getRelWithNodes
    val relationshipOps = mock[Operations[RelationshipValue]]
    val to = "to"
    val from = "from"

    when(relationshipOps.getByIdIfExists(42)).thenReturn(Some(r1))
    when(relationshipOps.getByIdIfExists(21)).thenReturn(Some(r2))
    val queryContext = mock[QueryContext]
    when(queryContext.relationshipOps).thenReturn(relationshipOps)
    val queryState = QueryStateHelper.emptyWith(query = queryContext)

    val relName = "a"
    // whens
    val result =
      UndirectedRelationshipByIdSeekPipe(relName, ManySeekArgs(ListLiteral(Literal(42), Literal(21))), to, from)().
      createResults(queryState)

    // then
    result.toSet should equal(Set(
      Map(relName -> r1, to -> e1, from -> s1),
      Map(relName -> r2, to -> e2, from -> s2),
      Map(relName -> r1, to -> s1, from -> e1),
      Map(relName -> r2, to -> s2, from -> e2)
    ))
  }

  test("handle null") {
    // given
    val to = "to"
    val from = "from"
    val relationshipOps = mock[Operations[RelationshipValue]]
    val queryContext = mock[QueryContext]
    when(queryContext.relationshipOps).thenReturn(relationshipOps)
    val queryState = QueryStateHelper.emptyWith(query = queryContext)

    // when
    val result: Iterator[ExecutionContext] = UndirectedRelationshipByIdSeekPipe("a", SingleSeekArg(Literal(null)), to, from)().createResults(queryState)

    // then
    result.toList should be(empty)
  }

  private def getRelWithNodes:(NodeValue,RelationshipValue,NodeValue) = {
    val rel = mock[RelationshipValue]
    val startNode = mock[NodeValue]
    val endNode = mock[NodeValue]
    when(rel.startNode()).thenReturn(startNode)
    when(rel.endNode()).thenReturn(endNode)
    (startNode, rel, endNode)
  }

}
