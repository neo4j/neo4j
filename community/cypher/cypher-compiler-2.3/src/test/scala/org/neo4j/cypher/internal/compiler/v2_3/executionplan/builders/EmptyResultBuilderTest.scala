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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Identifier, Literal, Property, RangeFunction}
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.compiler.v2_3.commands.{ReturnItem, Slice, SortItem}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{ExecutionPlanInProgress, PartiallySolvedQuery}
import org.neo4j.cypher.internal.compiler.v2_3.mutation.{CreateNode, ForeachAction}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{EmptyResultPipe, SingleRowPipe}

class EmptyResultBuilderTest extends BuilderTest {

  def builder = new EmptyResultBuilder

  test("should add empty result pipe") {
    // Given
    val query = PartiallySolvedQuery().copy(
      updates = Seq(Unsolved(ForeachAction(RangeFunction(Literal(0), Literal(1), Literal(1)), "n", List(CreateNode("p", Map(), List())))))
    )

    // When
    val plan = assertAccepts(query)

    // Then
    plan.pipe shouldBe an[EmptyResultPipe]
  }

  test("should reject when empty result pipe already planned") {
    // Given
    val query = PartiallySolvedQuery().copy(
      updates = Seq(Unsolved(ForeachAction(RangeFunction(Literal(0), Literal(1), Literal(1)), "n", List(CreateNode("p", Map(), List())))))
    )
    val pipe = EmptyResultPipe(SingleRowPipe())
    val planInProgress = ExecutionPlanInProgress(query, pipe, isUpdating = true)

    // When / Then
    assertRejects(planInProgress)
  }

  test("should reject when sorting should be done") {
    // Given
    val query = PartiallySolvedQuery().copy(
      sort = Seq(Unsolved(SortItem(Property(Identifier("x"), PropertyKey("y")), ascending = true))),
      extracted = true
    )

    // When / Then
    assertRejects(query)
  }

  test("should reject when skip should be done") {
    // Given
    val query = PartiallySolvedQuery().copy(
      slice = Some(Unsolved(Slice(Some(Literal(10)), None)))
    )

    // When / Then
    assertRejects(query)
  }

  test("should reject when limit should be done") {
    // Given
    val query = PartiallySolvedQuery().copy(
      slice = Some(Unsolved(Slice(None, Some(Literal(10)))))
    )

    // When / Then
    assertRejects(query)
  }

  test("should reject when both skip and limit should be done") {
    // Given
    val query = PartiallySolvedQuery().copy(
      slice = Some(Unsolved(Slice(Some(Literal(42)), Some(Literal(42)))))
    )

    // When / Then
    assertRejects(query)
  }

  test("should reject when query has something to return") {
    // Given
    val query = PartiallySolvedQuery().copy(
      returns = Seq(Unsolved(ReturnItem(Literal("foo"), "foo")))
    )

    // When / Then
    assertRejects(query)
  }

  test("should reject when query has tail") {
    // Given
    val query = PartiallySolvedQuery().copy(
      tail = Some(PartiallySolvedQuery())
    )

    // When / Then
    assertRejects(query)
  }
}
