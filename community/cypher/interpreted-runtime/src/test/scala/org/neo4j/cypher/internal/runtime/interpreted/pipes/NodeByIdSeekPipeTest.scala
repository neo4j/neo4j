/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.mockito.ArgumentMatchers
import org.neo4j.cypher.internal.runtime.NodeReadOperations
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ListLiteral
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.kernel.impl.util.ValueUtils.fromNodeEntity

class NodeByIdSeekPipeTest extends CypherFunSuite {

  import org.mockito.Mockito.when

  test("should seek node by id") {
    // given
    val id = 17
    val node = nodeMock(17)
    val nodeOps = mock[NodeReadOperations]
    when(nodeOps.entityExists(17)).thenReturn(true)

    val queryContext = mock[QueryContext]
    when(queryContext.nodeReadOps).thenReturn(nodeOps)
    val queryState = QueryStateHelper.emptyWith(query = queryContext)

    // when
    val result = NodeByIdSeekPipe("a", SingleSeekArg(literal(id)))().createResults(queryState)

    // then
    result.map(_.getByName("a")).toList should equal(List(fromNodeEntity(node)))
  }

  test("should seek nodes by multiple ids") {
    // given
    val node1 = nodeMock(42)
    val node2 = nodeMock(21)
    val node3 = nodeMock(11)
    val nodeOps = mock[NodeReadOperations]

    when(nodeOps.entityExists(ArgumentMatchers.anyLong())).thenReturn(true)

    val queryContext = mock[QueryContext]
    when(queryContext.nodeReadOps).thenReturn(nodeOps)
    val queryState = QueryStateHelper.emptyWith(query = queryContext)

    // whens
    val result = NodeByIdSeekPipe(
      "a",
      ManySeekArgs(ListLiteral(literal(42), literal(21), literal(11)))
    )().createResults(queryState)

    // then
    result.map(_.getByName("a")).toList should equal(List(
      fromNodeEntity(node1),
      fromNodeEntity(node2),
      fromNodeEntity(node3)
    ))
  }

  private def nodeMock(id: Long) = {
    val node = mock[Node]
    when(node.getId).thenReturn(id)
    when(node.getElementId).thenReturn(id.toString)
    node
  }
}
