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

import org.neo4j.cypher.internal.frontend.v2_3.ast.LabelName
import org.neo4j.cypher.internal.compiler.v2_3.spi.QueryContext
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticTable, LabelId}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node

class NodeByLabelScanPipeTest extends CypherFunSuite {

  implicit val monitor = mock[PipeMonitor]
  import org.mockito.Mockito.when

  test("should scan labeled nodes") {
    // given
    val nodes = List(mock[Node], mock[Node])
    val queryState = QueryStateHelper.emptyWith(
      query = when(mock[QueryContext].getNodesByLabel(12)).thenReturn(nodes.iterator).getMock[QueryContext]
    )

    implicit val table = new SemanticTable()
    table.resolvedLabelIds.put("Foo", LabelId(12))

    // when
    val result = NodeByLabelScanPipe("a", LazyLabel(LabelName("Foo")(null)))().createResults(queryState)

    // then
    result.map(_("a")).toList should equal(nodes)
  }
}
