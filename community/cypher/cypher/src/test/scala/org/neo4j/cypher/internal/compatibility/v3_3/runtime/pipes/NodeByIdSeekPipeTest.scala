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

import org.mockito.Mockito
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.{ListLiteral, Literal}
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.spi.v3_3.{Operations, QueryContext}
import org.neo4j.graphdb.Node

class NodeByIdSeekPipeTest extends CypherFunSuite {

  import Mockito.when

  test("should seek node by id") {
    // given
    val node = mock[Node]
    val nodeOps = mock[Operations[Node]]
    when(nodeOps.exists(17)).thenReturn(true)
    when(nodeOps.getById(17)).thenReturn(node)
    val queryState = QueryStateHelper.emptyWith(
      query = when(mock[QueryContext].nodeOps).thenReturn(nodeOps).getMock[QueryContext]
    )

    // when
    val result = NodeByIdSeekPipe("a", SingleSeekArg(Literal(17)))().createResults(queryState)

    // then
    result.map(_("a")).toList should equal(List(node))
  }

  test("should seek nodes by multiple ids") {
    // given
    val node1 = mock[Node]
    val node2 = mock[Node]
    val node3 = mock[Node]
    val nodeOps = mock[Operations[Node]]

    when(nodeOps.getById(42)).thenReturn(node1)
    when(nodeOps.exists(42)).thenReturn(true)
    when(nodeOps.getById(21)).thenReturn(node2)
    when(nodeOps.exists(21)).thenReturn(true)
    when(nodeOps.getById(11)).thenReturn(node3)
    when(nodeOps.exists(11)).thenReturn(true)

    val queryState = QueryStateHelper.emptyWith(
      query = when(mock[QueryContext].nodeOps).thenReturn(nodeOps).getMock[QueryContext]
    )

    // whens
    val result = NodeByIdSeekPipe("a", ManySeekArgs(ListLiteral(Literal(42), Literal(21), Literal(11))))().createResults(queryState)

    // then
    result.map(_("a")).toList should equal(List(node1, node2, node3))
  }
}
