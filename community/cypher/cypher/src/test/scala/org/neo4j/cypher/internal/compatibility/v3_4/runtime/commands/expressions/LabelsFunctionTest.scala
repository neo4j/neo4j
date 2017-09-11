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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ImplicitValueConversion._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.QueryStateHelper
import org.neo4j.cypher.internal.apa.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.graphdb.Node
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.VirtualValues.list

class LabelsFunctionTest extends CypherFunSuite {

  test("testIdLookup") {
    // GIVEN
    val node = mock[Node]
    when(node.getId).thenReturn(1337L)
    val queryContext = mock[QueryContext]
    when(queryContext.getLabelsForNode(1337L)).thenReturn(Iterator(42))
    when(queryContext.getLabelName(42)).thenReturn("bambi")

    val state = QueryStateHelper.emptyWith(query = queryContext)
    val ctx = ExecutionContext() += ("n" -> node)

    // WHEN
    val result = LabelsFunction(Variable("n"))(ctx, state)

    // THEN
    result should equal(list(stringValue("bambi")))
  }
}
