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
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions.{ListLiteral, Literal}
import org.neo4j.cypher.internal.apa.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.spi.v3_4.{Operations, QueryContext}
import org.neo4j.graphdb.Node
import org.neo4j.helpers.ValueUtils.fromNodeProxy

class NodeByIdSeekPipeTest extends CypherFunSuite {

  import Mockito.when

  test("should seek node by id") {
    // given
    val id = 17
    val node = nodeProxy(17)
    val nodeOps = when(mock[Operations[Node]].getById(id)).thenReturn(node).getMock[Operations[Node]]
    when(nodeOps.getByIdIfExists(17)).thenReturn(Some(node))
    val queryState = QueryStateHelper.emptyWith(
      query = when(mock[QueryContext].nodeOps).thenReturn(nodeOps).getMock[QueryContext]
    )

    // when
    val result = NodeByIdSeekPipe("a", SingleSeekArg(Literal(id)))().createResults(queryState)

    // then
    result.map(_("a")).toList should equal(List(fromNodeProxy(node)))
  }

  test("should seek nodes by multiple ids") {
    // given
    val node1 = nodeProxy(42)
    val node2 = nodeProxy(21)
    val node3 = nodeProxy(11)
    val nodeOps = mock[Operations[Node]]

    when(nodeOps.getByIdIfExists(42)).thenReturn(Some(node1))
    when(nodeOps.getByIdIfExists(21)).thenReturn(Some(node2))
    when(nodeOps.getByIdIfExists(11)).thenReturn(Some(node3))

    val queryState = QueryStateHelper.emptyWith(
      query = when(mock[QueryContext].nodeOps).thenReturn(nodeOps).getMock[QueryContext]
    )

    // whens
    val result = NodeByIdSeekPipe("a", ManySeekArgs(ListLiteral(Literal(42), Literal(21), Literal(11))))().createResults(queryState)

    // then
    result.map(_("a")).toList should equal(List(fromNodeProxy(node1), fromNodeProxy(node2), fromNodeProxy(node3)))
  }

  private def nodeProxy(id: Long) = {
    val node = mock[Node]
    when(node.getId).thenReturn(id)
    node
  }
}
