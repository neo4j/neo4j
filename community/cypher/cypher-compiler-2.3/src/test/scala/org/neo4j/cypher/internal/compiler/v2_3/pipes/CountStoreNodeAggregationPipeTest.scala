/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_3.spi.QueryContext
import org.neo4j.cypher.internal.frontend.v2_3.ast.{AstConstructionTestSupport, LabelName}
import org.neo4j.cypher.internal.frontend.v2_3.{NameId, LabelId, SemanticTable}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class CountStoreNodeAggregationPipeTest extends CypherFunSuite with AstConstructionTestSupport {

  implicit val monitor = mock[PipeMonitor]

  test("should return a count for nodes with a label") {
    implicit val table = new SemanticTable()
    table.resolvedLabelIds.put("A", LabelId(12))

    val pipe = CountStoreNodeAggregationPipe("count(n)", Some(LazyLabel(LabelName("A") _)))()

    val queryState = QueryStateHelper.emptyWith(
      query = when(mock[QueryContext].nodeCountByCountStore(12)).thenReturn(42L).getMock[QueryContext]
    )
    pipe.createResults(queryState).map(_("count(n)")).toSet should equal(Set(42L))
  }

  test("should throw an exception when the label id can not be resolved") {
    implicit val table = new SemanticTable()

    val pipe = CountStoreNodeAggregationPipe("count(n)", Some(LazyLabel(LabelName("A") _)))()

    val mockedContext: QueryContext = mock[QueryContext]
    when(mockedContext.nodeCountByCountStore(12)).thenReturn(42L)
    when(mockedContext.getOptLabelId("A")).thenReturn(None)
    val queryState = QueryStateHelper.emptyWith(query = mockedContext)

    a [IllegalArgumentException] should be thrownBy pipe.createResults(queryState)
  }

  test("should return a count for nodes without a label") {
    val pipe = CountStoreNodeAggregationPipe("count(n)", None)()

    val queryState = QueryStateHelper.emptyWith(
      query = when(mock[QueryContext].nodeCountByCountStore(NameId.WILDCARD)).thenReturn(42L).getMock[QueryContext]
    )
    pipe.createResults(queryState).map(_("count(n)")).toSet should equal(Set(42L))
  }

}
