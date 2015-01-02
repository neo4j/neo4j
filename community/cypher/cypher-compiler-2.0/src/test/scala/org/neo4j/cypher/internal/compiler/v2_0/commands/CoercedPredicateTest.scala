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
package org.neo4j.cypher.internal.compiler.v2_0.commands

import expressions.{Literal, Collection}
import org.neo4j.cypher.internal.compiler.v2_0._
import pipes.{QueryStateHelper, QueryState}
import org.junit.Test
import org.junit.Assert._

class CoercedPredicateTest {

  val ctx: ExecutionContext = null
  implicit val state: QueryState = QueryStateHelper.empty

  @Test def should_coerce_non_empty_collection_to_true() {
    // Given
    val collection = Collection(Literal(1))

    // When
    val result = CoercedPredicate(collection).isTrue(ctx)

    // Then
    assertTrue(s"$collection should return true", result)
  }

  @Test def should_coerce_empty_collection_to_false() {
    // Given
    val collection = Collection()

    // When
    val result = CoercedPredicate(collection).isTrue(ctx)

    // Then
    assertFalse(s"$collection should return false", result)
  }
  
  @Test def should_pass_through_false() {
    // Given
    val inner = Not(True())

    // When
    val result = CoercedPredicate(inner).isTrue(ctx)

    // Then
    assertFalse(s"$inner should return false", result)
  }

  @Test def should_pass_through_true() {
    // Given
    val inner = True()

    // When
    val result = CoercedPredicate(inner).isTrue(ctx)

    // Then
    assertTrue(s"$inner should return true", result)
  }

  @Test def should_treat_null_as_false() {
    // Given
    val inner = Literal(null)

    // When
    val result = CoercedPredicate(inner).isTrue(ctx)

    // Then
    assertFalse(s"$inner should return false", result)
  }
}
