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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.mockito.Mockito
import org.neo4j.cypher.internal.runtime.interpreted.ValueComparisonHelper._
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, QueryStateHelper}
import org.neo4j.cypher.internal.runtime.{Operations, QueryContext}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.virtual.NodeValue

class AllNodesScanPipeTest extends CypherFunSuite {

  import Mockito.when

  test("should scan all nodes") {
    val node1 = node(1)
    val node2 = node(2)
    // given
    val nodes = List(node1, node2)
    val nodeOps = when(mock[Operations[NodeValue]].all).thenReturn(nodes.iterator).getMock[Operations[NodeValue]]
    val queryState = QueryStateHelper.emptyWith(
      query = when(mock[QueryContext].nodeOps).thenReturn(nodeOps).getMock[QueryContext]
    )

    // when
    val result: Iterator[ExecutionContext] = AllNodesScanPipe("a")().createResults(queryState)

    // then
    result.toList should beEquivalentTo(List(Map("a" -> node1), Map("a" -> node2)))
  }

  private def node(id: Long) = {
    val n = mock[NodeValue]
    when(n.id()).thenReturn(id)
    n
  }
}
