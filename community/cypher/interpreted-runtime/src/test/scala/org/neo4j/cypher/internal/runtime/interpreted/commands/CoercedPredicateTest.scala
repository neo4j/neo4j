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
package org.neo4j.cypher.internal.runtime.interpreted.commands

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ListLiteral
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.CoercedPredicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Not
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.True
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values.NO_VALUE

class CoercedPredicateTest extends CypherFunSuite {

  val ctx: CypherRow = null
  val state: QueryState = QueryStateHelper.empty

  test("should coerce non empty collection to true") {
    // Given
    val collection = ListLiteral(literal(1))

    // When
    val result = CoercedPredicate(collection).isTrue(ctx, state)

    // Then
    result should equal(true)
  }

  test("should coerce empty collection to false") {
    // Given
    val collection = ListLiteral()

    // When
    val result = CoercedPredicate(collection).isTrue(ctx, state)

    // Then
    result should equal(false)
  }

  test("should pass through false") {
    // Given
    val inner = Not(True())

    // When
    val result = CoercedPredicate(inner).isTrue(ctx, state)

    // Then
    result should equal(false)
  }

  test("should pass through true") {
    // Given
    val inner = True()

    // When
    val result = CoercedPredicate(inner).isTrue(ctx, state)

    // Then
    result should equal(true)
  }

  test("should treat null as false") {
    // Given
    val inner = Literal(NO_VALUE)

    // When
    val result = CoercedPredicate(inner).isTrue(ctx, state)

    // Then
    result should equal(false)
  }
}
