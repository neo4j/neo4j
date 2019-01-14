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

import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.util.v3_4.LabelId
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions.LabelName
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

class NodeByLabelScanPipeTest extends CypherFunSuite {

  import org.mockito.Mockito.when

  test("should scan labeled nodes") {
    // given
    val nodes = List(nodeValue(1), nodeValue(2))
    val queryContext = mock[QueryContext]
    when(queryContext.getNodesByLabel(12)).thenReturn(nodes.iterator)
    val queryState = QueryStateHelper.emptyWith(query = queryContext)

    implicit val table = new SemanticTable()
    table.resolvedLabelNames.put("Foo", LabelId(12))

    // when
    val result = NodeByLabelScanPipe("a", LazyLabel(LabelName("Foo")(null)))().createResults(queryState)

    // then
    result.map(_("a")).toList should equal(List(nodeValue(1), nodeValue(2)))
  }

  private def nodeValue(id: Long) = VirtualValues.nodeValue(id, Values.EMPTY_TEXT_ARRAY, VirtualValues.EMPTY_MAP)
}
