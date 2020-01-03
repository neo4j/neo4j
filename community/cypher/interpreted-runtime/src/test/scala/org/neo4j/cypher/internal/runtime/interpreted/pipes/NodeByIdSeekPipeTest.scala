/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.{ArgumentMatchers, Mockito}
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{ListLiteral, Literal}
import org.neo4j.cypher.internal.runtime.{NodeOperations, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.kernel.impl.util.ValueUtils.fromNodeEntity
import org.neo4j.values.virtual.NodeValue

class NodeByIdSeekPipeTest extends CypherFunSuite {

  import Mockito.when

  test("should seek node by id") {
    // given
    val id = 17
    val node = nodeMock(17)
    val nodeOps = mock[NodeOperations]
    when(nodeOps.getByIdIfExists(17)).thenAnswer(new Answer[Option[NodeValue]] {
      override def answer(invocation: InvocationOnMock): Option[NodeValue] = Some(fromNodeEntity(node))
    })

    val queryContext = mock[QueryContext]
    when(queryContext.nodeOps).thenReturn(nodeOps)
    val queryState = QueryStateHelper.emptyWith(query = queryContext)

    // when
    val result = NodeByIdSeekPipe("a", SingleSeekArg(Literal(id)))().createResults(queryState)

    // then
    result.map(_.getByName("a")).toList should equal(List(fromNodeEntity(node)))
  }

  test("should seek nodes by multiple ids") {
    // given
    val node1 = nodeMock(42)
    val node2 = nodeMock(21)
    val node3 = nodeMock(11)
    val nodeOps = mock[NodeOperations]

    when(nodeOps.getByIdIfExists(ArgumentMatchers.anyLong())).thenAnswer(new Answer[Option[NodeValue]] {
      override def answer(invocation: InvocationOnMock): Option[NodeValue] = invocation.getArgument[Long](0) match {
        case 42 => Some(fromNodeEntity(node1))
        case 21 => Some(fromNodeEntity(node2))
        case 11 => Some(fromNodeEntity(node3))
        case _ => fail()
      }
    })


    val queryContext = mock[QueryContext]
    when(queryContext.nodeOps).thenReturn(nodeOps)
    val queryState = QueryStateHelper.emptyWith(query = queryContext)

    // whens
    val result = NodeByIdSeekPipe("a", ManySeekArgs(ListLiteral(Literal(42), Literal(21), Literal(11))))().createResults(queryState)

    // then
    result.map(_.getByName("a")).toList should equal(List(fromNodeEntity(node1), fromNodeEntity(node2), fromNodeEntity(node3)))
  }

  private def nodeMock(id: Long) = {
    val node = mock[Node]
    when(node.getId).thenReturn(id)
    node
  }
}
