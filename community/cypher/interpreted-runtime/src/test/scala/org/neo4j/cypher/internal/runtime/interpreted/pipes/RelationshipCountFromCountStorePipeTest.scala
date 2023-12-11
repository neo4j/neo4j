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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.ImplicitDummyPos
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.NameId.WILDCARD
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values.longValue

class RelationshipCountFromCountStorePipeTest extends CypherFunSuite with ImplicitDummyPos {

  test("should return a count for relationships without a type or any labels") {
    val pipe = RelationshipCountFromCountStorePipe("count(r)", None, RelationshipTypes.empty, None)()

    val queryContext = mock[QueryContext]
    when(queryContext.relationshipCountByCountStore(WILDCARD, WILDCARD, WILDCARD)).thenReturn(42L)
    val queryState = QueryStateHelper.emptyWith(query = queryContext)
    pipe.createResults(queryState).map(_.getByName("count(r)")).toSet should equal(Set(longValue(42L)))
  }

  test("should return a count for relationships with a type but no labels") {
    implicit val table = new SemanticTable(
      resolvedRelTypeNames = Map("X" -> RelTypeId(22))
    )

    val pipe =
      RelationshipCountFromCountStorePipe("count(r)", None, RelationshipTypes(Array(RelTypeName("X")(pos))), None)()

    val queryContext = mock[QueryContext]
    when(queryContext.relationshipCountByCountStore(WILDCARD, 22, WILDCARD)).thenReturn(42L)
    val queryState = QueryStateHelper.emptyWith(query = queryContext)
    pipe.createResults(queryState).map(_.getByName("count(r)")).toSet should equal(Set(longValue(42L)))
  }

  test("should return a count for relationships with a type and start label") {
    implicit val table = new SemanticTable(
      resolvedRelTypeNames = Map("X" -> RelTypeId(22)),
      resolvedLabelNames = Map("A" -> LabelId(12))
    )

    val pipe = RelationshipCountFromCountStorePipe(
      "count(r)",
      Some(LazyLabel(LabelName("A") _)),
      RelationshipTypes(Array(RelTypeName("X")(pos))),
      None
    )()

    val queryContext = mock[QueryContext]
    when(queryContext.relationshipCountByCountStore(12, 22, WILDCARD)).thenReturn(42L)
    val queryState = QueryStateHelper.emptyWith(query = queryContext)
    pipe.createResults(queryState).map(_.getByName("count(r)")).toSet should equal(Set(longValue(42L)))
  }

  test("should return zero if rel-type is missing") {
    implicit val table = new SemanticTable()

    val pipe = RelationshipCountFromCountStorePipe(
      "count(r)",
      None,
      RelationshipTypes(Array("X")),
      Some(LazyLabel(LabelName("A") _))
    )()

    val mockedContext: QueryContext = mock[QueryContext]
    // try to guarantee that the mock won't be the reason for the exception
    when(mockedContext.relationshipCountByCountStore(WILDCARD, 22, 12)).thenReturn(38L)
    when(mockedContext.getOptLabelId("A")).thenReturn(None)
    val queryState = QueryStateHelper.emptyWith(query = mockedContext)

    pipe.createResults(queryState).map(_.getByName("count(r)")).toSet should equal(Set(longValue(0L)))
  }

}
