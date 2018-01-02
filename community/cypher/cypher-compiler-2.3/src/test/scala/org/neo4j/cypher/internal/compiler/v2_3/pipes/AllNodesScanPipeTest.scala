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

import org.mockito.Mockito
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.cypher.internal.compiler.v2_3.spi.{Operations, QueryContext}

class AllNodesScanPipeTest extends CypherFunSuite {

  private implicit val monitor = mock[PipeMonitor]
  import Mockito.when

  test("should scan all nodes") {
    // given
    val nodes = List(mock[Node], mock[Node])
    val nodeOps = when(mock[Operations[Node]].all).thenReturn(nodes.iterator).getMock[Operations[Node]]
    val queryState = QueryStateHelper.emptyWith(
      query = when(mock[QueryContext].nodeOps).thenReturn(nodeOps).getMock[QueryContext]
    )

    // when
    val result = AllNodesScanPipe("a")().createResults(queryState)

    // then
    result.map(_("a")).toList should equal(nodes)
  }
}
