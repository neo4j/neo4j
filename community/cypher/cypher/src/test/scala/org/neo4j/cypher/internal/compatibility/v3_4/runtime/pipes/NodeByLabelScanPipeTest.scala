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

import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_4.LabelId
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.spi.v3_4.QueryContext
import org.neo4j.cypher.internal.v3_4.expressions.LabelName
import org.neo4j.graphdb.Node
import org.neo4j.kernel.impl.util.ValueUtils.fromNodeProxy

class NodeByLabelScanPipeTest extends CypherFunSuite {

  import org.mockito.Mockito.when

  test("should scan labeled nodes") {
    // given
    val nodes = List(nodeProxy(1), nodeProxy(2))
    val queryState = QueryStateHelper.emptyWith(
      query = when(mock[QueryContext].getNodesByLabel(12)).thenReturn(nodes.iterator).getMock[QueryContext]
    )

    implicit val table = new SemanticTable()
    table.resolvedLabelNames.put("Foo", LabelId(12))

    // when
    val result = NodeByLabelScanPipe("a", LazyLabel(LabelName("Foo")(null)))().createResults(queryState)

    // then
    result.map(_("a")).toList should equal(List(fromNodeProxy(nodeProxy(1)), fromNodeProxy(nodeProxy(2))))
  }

  private def nodeProxy(id: Long) = {
    val node = mock[Node]
    when(node.getId).thenReturn(id)
    node
  }
}
