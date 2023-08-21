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

import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoInteractions
import org.mockito.Mockito.verifyNoMoreInteractions
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class LazyPropertyKeyTest extends CypherFunSuite {
  private val pos = DummyPosition(0)
  private val PROPERTY_KEY_NAME = PropertyKeyName("foo")(pos)
  private val PROPERTY_KEY_ID = PropertyKeyId(42)

  test("if key is resolved, don't do any lookups") {
    // GIVEN
    implicit val table = mock[SemanticTable]
    val context = mock[ReadTokenContext]
    when(table.id(PROPERTY_KEY_NAME)).thenReturn(Some(PROPERTY_KEY_ID))

    // WHEN
    val id = LazyPropertyKey(PROPERTY_KEY_NAME).id(context)

    // THEN
    id should equal(PROPERTY_KEY_ID.id)
    verifyNoInteractions(context)
  }

  test("if key is not resolved, do a lookup") {
    // GIVEN
    implicit val table = mock[SemanticTable]
    val context = mock[ReadTokenContext]
    when(context.getOptPropertyKeyId(PROPERTY_KEY_NAME.name)).thenReturn(Some(PROPERTY_KEY_ID.id))
    when(table.id(PROPERTY_KEY_NAME)).thenReturn(None)

    // WHEN
    val id = LazyPropertyKey(PROPERTY_KEY_NAME).id(context)

    // THEN
    id should equal(PROPERTY_KEY_ID.id)
    verify(context).getOptPropertyKeyId("foo")
    verifyNoMoreInteractions(context)
  }

  test("multiple calls to id should result in only one lookup") {
    // GIVEN
    implicit val table = mock[SemanticTable]
    val context = mock[ReadTokenContext]
    when(context.getOptPropertyKeyId(PROPERTY_KEY_NAME.name)).thenReturn(Some(PROPERTY_KEY_ID.id))
    when(table.id(PROPERTY_KEY_NAME)).thenReturn(None)

    // WHEN
    val lazyPropertyKey = LazyPropertyKey(PROPERTY_KEY_NAME)
    for (_ <- 1 to 100) lazyPropertyKey.id(context)

    // THEN
    verify(context).getOptPropertyKeyId("foo")
    verifyNoMoreInteractions(context)
  }
}
