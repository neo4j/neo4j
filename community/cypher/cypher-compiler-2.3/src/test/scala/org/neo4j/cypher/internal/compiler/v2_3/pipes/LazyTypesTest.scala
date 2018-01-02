/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class LazyTypesTest extends CypherFunSuite {

  test("should not initialize state when state is complete") {
    // given
    val types = LazyTypes(Seq("a", "b", "c"))

    val context = mock[QueryContext]
    when(context.getOptRelTypeId("a")).thenReturn(Some(1))
    when(context.getOptRelTypeId("b")).thenReturn(Some(2))
    when(context.getOptRelTypeId("c")).thenReturn(Some(3))
    types.types(context)

    // when
    val newContext = mock[QueryContext]
    types.types(newContext)

    // then
    verifyZeroInteractions(newContext)
  }

  test("should re-initialize if not fully initialized") {
    // given
    val types = LazyTypes(Seq("a", "b", "c"))

    val context = mock[QueryContext]
    when(context.getOptRelTypeId("a")).thenReturn(Some(1))
    when(context.getOptRelTypeId("b")).thenReturn(None)
    when(context.getOptRelTypeId("c")).thenReturn(Some(3))
    types.types(context)

    // when
    val newContext = mock[QueryContext]
    when(newContext.getOptRelTypeId("a")).thenReturn(Some(1))
    when(newContext.getOptRelTypeId("b")).thenReturn(None)
    when(newContext.getOptRelTypeId("c")).thenReturn(Some(3))
    types.types(newContext)

    // then
    verify(newContext).getOptRelTypeId("b")
  }

}
