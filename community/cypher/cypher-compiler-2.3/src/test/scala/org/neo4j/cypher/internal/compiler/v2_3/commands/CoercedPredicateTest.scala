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
package org.neo4j.cypher.internal.compiler.v2_3.commands

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Collection, Literal}
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.{CoercedPredicate, True, Not}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{QueryState, QueryStateHelper}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class CoercedPredicateTest extends CypherFunSuite {

  val ctx: ExecutionContext = null
  implicit val state: QueryState = QueryStateHelper.empty

  test("should_coerce_non_empty_collection_to_true") {
    // Given
    val collection = Collection(Literal(1))

    // When
    val result = CoercedPredicate(collection).isTrue(ctx)

    // Then
    result should equal(true)
  }

  test("should_coerce_empty_collection_to_false") {
    // Given
    val collection = Collection()

    // When
    val result = CoercedPredicate(collection).isTrue(ctx)

    // Then
    result should equal(false)
  }

  test("should_pass_through_false") {
    // Given
    val inner = Not(True())

    // When
    val result = CoercedPredicate(inner).isTrue(ctx)

    // Then
    result should equal(false)
  }

  test("should_pass_through_true") {
    // Given
    val inner = True()

    // When
    val result = CoercedPredicate(inner).isTrue(ctx)

    // Then
    result should equal(true)
  }

  test("should_treat_null_as_false") {
    // Given
    val inner = Literal(null)

    // When
    val result = CoercedPredicate(inner).isTrue(ctx)

    // Then
    result should equal(false)
  }
}
