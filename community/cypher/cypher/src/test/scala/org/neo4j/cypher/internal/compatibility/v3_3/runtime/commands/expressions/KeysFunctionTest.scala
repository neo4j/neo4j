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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ImplicitValueConversion._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.spi.v3_3.{Operations, QueryContext}
import org.neo4j.graphdb.Node
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.VirtualValues.{EMPTY_LIST, list}

class KeysFunctionTest extends CypherFunSuite {

  test("test Property Keys") {
    // GIVEN

    val node = mock[Node]

    val queryContext = mock[QueryContext]

    val ops = mock[Operations[Node]]
    when(queryContext.nodeOps).thenReturn(ops)
    when(ops.propertyKeyIds(node.getId)).thenReturn(Iterator(11, 12, 13))

    when(queryContext.getPropertyKeyName(11)).thenReturn("theProp1")
    when(queryContext.getPropertyKeyName(12)).thenReturn("OtherProp")
    when(queryContext.getPropertyKeyName(13)).thenReturn("MoreProp")

    val state = QueryStateHelper.emptyWith(query = queryContext)
    val ctx = ExecutionContext() += ("n" -> node)

    // WHEN
    val result = KeysFunction(Variable("n"))(ctx, state)

    // THEN
    result should equal(list(stringValue("theProp1"), stringValue("OtherProp"), stringValue("MoreProp")))
  }

  test("test without Property Keys ") {
    // GIVEN
    val node = mock[Node]
    val queryContext = mock[QueryContext]
    val ops = mock[Operations[Node]]
    when(queryContext.nodeOps).thenReturn(ops)
    when(ops.propertyKeyIds(node.getId)).thenReturn(Iterator.empty)


    val state = QueryStateHelper.emptyWith(query = queryContext)
    val ctx = ExecutionContext() += ("n" -> node)

    // WHEN
    val result = KeysFunction(Variable("n"))(ctx, state)

    // THEN
    result should equal(EMPTY_LIST)
  }

  test("test using a literal map") {
    // GIVEN
    val queryContext = mock[QueryContext]
    val state = QueryStateHelper.emptyWith(query = queryContext)
    val ctx = ExecutionContext.empty

    val function = KeysFunction(LiteralMap(Map("foo" -> Literal(1), "bar" -> Literal(2), "baz" -> Literal(3))))
    // WHEN
    val result = function(ctx, state)

    result should equal(list(stringValue("foo"), stringValue("bar"), stringValue("baz")))
  }
}
