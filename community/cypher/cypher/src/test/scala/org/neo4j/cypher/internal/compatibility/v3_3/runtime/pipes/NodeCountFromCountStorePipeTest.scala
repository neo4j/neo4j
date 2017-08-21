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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import org.mockito.Mockito._
import org.neo4j.cypher.internal.frontend.v3_3.ast.{AstConstructionTestSupport, LabelName}
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_3.{LabelId, NameId, SemanticTable}
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.values.storable.Values.longValue

class NodeCountFromCountStorePipeTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should return a count for nodes with a label") {
    implicit val table = new SemanticTable()
    table.resolvedLabelIds.put("A", LabelId(12))

    val pipe = NodeCountFromCountStorePipe("count(n)", List(Some(LazyLabel(LabelName("A") _))))()

    val queryState = QueryStateHelper.emptyWith(
      query = when(mock[QueryContext].nodeCountByCountStore(12)).thenReturn(42L).getMock[QueryContext]
    )
    pipe.createResults(queryState).map(_("count(n)")).toSet should equal(Set(longValue(42L)))
  }

  test("should return zero if label is missing") {
    implicit val table = new SemanticTable()

    val pipe = NodeCountFromCountStorePipe("count(n)", List(Some(LazyLabel(LabelName("A") _))))()

    val mockedContext: QueryContext = mock[QueryContext]
    when(mockedContext.nodeCountByCountStore(12)).thenReturn(42L)
    when(mockedContext.getOptLabelId("A")).thenReturn(None)
    val queryState = QueryStateHelper.emptyWith(query = mockedContext)

    pipe.createResults(queryState).map(_("count(n)")).toSet should equal(Set(longValue(0L)))
  }

  test("should return a count for nodes without a label") {
    val pipe = NodeCountFromCountStorePipe("count(n)", List(None))()

    val queryState = QueryStateHelper.emptyWith(
      query = when(mock[QueryContext].nodeCountByCountStore(NameId.WILDCARD)).thenReturn(42L).getMock[QueryContext]
    )
    pipe.createResults(queryState).map(_("count(n)")).toSet should equal(Set(longValue(42L)))
  }

}
