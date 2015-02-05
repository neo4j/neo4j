/**
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
package org.neo4j.cypher.internal.compiler.v2_2.commands.expressions

import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.pipes.QueryStateHelper
import org.neo4j.cypher.internal.compiler.v2_2.spi.QueryContext
import org.neo4j.graphdb.Node

class KeysFunctionTest extends CypherFunSuite {

  test("test Property Keys") {
    // GIVEN
    val node = mock[Node]
    val queryContext = mock[QueryContext]

    val ls = List(11,12,13)
    val lsValues = List("theProp1","OtherProp","MoreProp")
    when(queryContext.getPropertiesForNode(node.getId)).then(new Answer[Iterator[Long]]() {
      def answer(invocation: InvocationOnMock): Iterator[Long] = ls.toIterator.map(_.toLong)
    })

    when(queryContext.getPropertyKeyName(11)).thenReturn(lsValues(0))
    when(queryContext.getPropertyKeyName(12)).thenReturn(lsValues(1))
    when(queryContext.getPropertyKeyName(13)).thenReturn(lsValues(2))

    val state = QueryStateHelper.emptyWith(query = queryContext)
    val ctx = ExecutionContext() += ("n" -> node)

    // WHEN
    val result = KeysFunction(Identifier("n"))(ctx)(state)

    // THEN
    result should equal(lsValues)
  }

  test("test without Property Keys ") {

    // GIVEN
    val expectedNull: Any = null
    val node = mock[Node]
    val queryContext = mock[QueryContext]

    val ls = List()

    when(queryContext.getPropertiesForNode(node.getId)).then(new Answer[Iterator[Long]]() {
      def answer(invocation: InvocationOnMock): Iterator[Long] = ls.toIterator
    })

    val state = QueryStateHelper.emptyWith(query = queryContext)
    val ctx = ExecutionContext() += ("n" -> node)

    // WHEN
    val result = KeysFunction(Identifier("n"))(ctx)(state)


    // THEN
    //result should equal(expectedNull)
    result should equal(List())
  }
}
