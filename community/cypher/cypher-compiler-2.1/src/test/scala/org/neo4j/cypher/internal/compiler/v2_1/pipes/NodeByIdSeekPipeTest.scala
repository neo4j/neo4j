/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.pipes

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.spi.{Operations, QueryContext}
import org.neo4j.graphdb.Node
import org.mockito.Mockito
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.Literal

class NodeByIdSeekPipeTest extends CypherFunSuite {

  import Mockito.when

  test("should seek node by id") {
    // given
    val node = mock[Node]
    val nodeOps = when(mock[Operations[Node]].getById(17)).thenReturn(node).getMock[Operations[Node]]
    val queryState = QueryStateHelper.emptyWith(
      query = when(mock[QueryContext].nodeOps).thenReturn(nodeOps).getMock[QueryContext]
    )

    // when
    val result = NodeByIdSeekPipe("a", Literal(17)).createResults(queryState)

    // then
    result.map(_("a")).toList should equal(List(node))
  }
}
