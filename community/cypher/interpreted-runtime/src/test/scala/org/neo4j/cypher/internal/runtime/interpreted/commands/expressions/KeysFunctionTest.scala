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
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.values.AnyValues
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualValues.EMPTY_LIST
import org.neo4j.values.virtual.VirtualValues.list

import scala.collection.mutable

class KeysFunctionTest extends CypherFunSuite {

  test("test Property Keys") {
    // GIVEN

    val node = mock[Node]
    when(node.getElementId).thenReturn("dummy")

    val queryContext = mock[QueryContext]
    val state = QueryStateHelper.emptyWith(query = queryContext)

    when(queryContext.nodePropertyIds(node.getId, state.cursors.nodeCursor, state.cursors.propertyCursor)).thenReturn(
      Array(11, 12, 13)
    )

    when(queryContext.getPropertyKeyName(11)).thenReturn("theProp1")
    when(queryContext.getPropertyKeyName(12)).thenReturn("OtherProp")
    when(queryContext.getPropertyKeyName(13)).thenReturn("MoreProp")

    val ctx = CypherRow(mutable.Map("n" -> node))

    // WHEN
    val result = KeysFunction(Variable("n"))(ctx, state)

    // THEN
    result should equal(list(stringValue("theProp1"), stringValue("OtherProp"), stringValue("MoreProp")))
  }

  test("test without Property Keys ") {
    // GIVEN
    val node = mock[Node]
    when(node.getElementId).thenReturn("dummy")

    val queryContext = mock[QueryContext]
    val state = QueryStateHelper.emptyWith(query = queryContext)
    when(queryContext.nodePropertyIds(node.getId, state.cursors.nodeCursor, state.cursors.propertyCursor)).thenReturn(
      Array.empty[Int]
    )

    val ctx = CypherRow(mutable.Map("n" -> node))

    // WHEN
    val result = KeysFunction(Variable("n"))(ctx, state)

    // THEN
    result should equal(EMPTY_LIST)
  }

  test("test using a literal map") {
    // GIVEN
    val queryContext = mock[QueryContext]
    val state = QueryStateHelper.emptyWith(query = queryContext)
    val ctx = CypherRow.empty

    val function = KeysFunction(LiteralMap(Map("foo" -> literal(1), "bar" -> literal(2), "baz" -> literal(3))))
    // WHEN
    val result =
      function(ctx, state).asInstanceOf[ListValue].asArray().sortWith((a, b) => AnyValues.COMPARATOR.compare(a, b) >= 0)

    result should equal(Array(stringValue("foo"), stringValue("baz"), stringValue("bar")))
  }
}
