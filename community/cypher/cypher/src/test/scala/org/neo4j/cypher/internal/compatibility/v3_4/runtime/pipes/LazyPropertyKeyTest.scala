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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes

import org.mockito.Mockito._
import org.neo4j.cypher.internal.util.v3_4.DummyPosition
import org.neo4j.cypher.internal.compiler.v3_4.spi.TokenContext
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_4.PropertyKeyId
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.v3_4.expressions.PropertyKeyName

import scala.collection.mutable

class LazyPropertyKeyTest extends CypherFunSuite {
  private val pos = DummyPosition(0)
  private val PROPERTY_KEY_NAME = PropertyKeyName("foo")(pos)
  private val PROPERTY_KEY_ID = PropertyKeyId(42)

  test("if key is resolved, don't do any lookups") {
    // GIVEN
    implicit val table = mock[SemanticTable]
    val context = mock[TokenContext]
    when(table.id(PROPERTY_KEY_NAME)).thenReturn(Some(PROPERTY_KEY_ID))

    //WHEN
    val id = LazyPropertyKey(PROPERTY_KEY_NAME).id(context)

    // THEN
    id should equal(Some(PROPERTY_KEY_ID))
    verifyZeroInteractions(context)
  }

  test("if key is not resolved, do a lookup") {
    // GIVEN
    implicit val table = mock[SemanticTable]
    val context = mock[TokenContext]
    when(context.getOptPropertyKeyId(PROPERTY_KEY_NAME.name)).thenReturn(Some(PROPERTY_KEY_ID.id))
    when(table.id(PROPERTY_KEY_NAME)).thenReturn(None)

    // WHEN
    val id = LazyPropertyKey(PROPERTY_KEY_NAME).id(context)

    // THEN
    id should equal(Some(PROPERTY_KEY_ID))
    verify(context, times(1)).getOptPropertyKeyId("foo")
    verifyNoMoreInteractions(context)
  }

  test("multiple calls to id should result in only one lookup") {
    // GIVEN
    implicit val table = mock[SemanticTable]
    val context = mock[TokenContext]
    when(context.getOptPropertyKeyId(PROPERTY_KEY_NAME.name)).thenReturn(Some(PROPERTY_KEY_ID.id))
    when(table.id(PROPERTY_KEY_NAME)).thenReturn(None)

    // WHEN
    val lazyPropertyKey = LazyPropertyKey(PROPERTY_KEY_NAME)
    for (i <- 1 to 100) lazyPropertyKey.id(context)

    // THEN
    verify(context, times(1)).getOptPropertyKeyId("foo")
    verifyNoMoreInteractions(context)
  }
}
