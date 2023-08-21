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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ImplicitValueConversion.toNodeValue
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.VirtualValues.list

import scala.collection.mutable

class LabelsFunctionTest extends CypherFunSuite {

  test("testIdLookup") {
    // GIVEN
    val node = mock[Node]
    when(node.getId).thenReturn(1337L)
    when(node.getElementId).thenReturn("1337")
    val queryContext = mock[QueryContext]
    val state = QueryStateHelper.emptyWith(query = queryContext)
    when(queryContext.getLabelsForNode(1337L, state.cursors.nodeCursor)).thenReturn(list(stringValue("bambi")))

    val ctx = CypherRow(mutable.Map("n" -> node))

    // WHEN
    val result = LabelsFunction(Variable("n"))(ctx, state)

    // THEN
    result should equal(list(stringValue("bambi")))
  }
}
