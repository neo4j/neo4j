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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates.{Ands, Not, Predicate, True}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v3_4.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite

class AndsTest extends CypherFunSuite {
  private val state = QueryStateHelper.empty
  private val ctx = ExecutionContext.empty

  private val nullPredicate = mock[Predicate]
  when(nullPredicate.isMatch(ctx, state)).thenReturn(None)

  private val explodingPredicate = mock[Predicate]
  when(explodingPredicate.isMatch(any(), any())).thenThrow(new IllegalStateException("there is something wrong"))

  test("should return null if there are no false values and one or more nulls") {
    ands(T, nullPredicate).isMatch(ctx, state) should equal(None)
  }

  test("should quit early when finding a false value") {
    ands(F, explodingPredicate).isMatch(ctx, state) should equal(Some(false))
  }

  test("should return true if all predicates evaluate to true") {
    ands(T, T).isMatch(ctx, state) should equal(Some(true))
  }

  test("should return false instead of null") {
    ands(nullPredicate, F).isMatch(ctx, state) should equal(Some(false))
  }

  private def ands(predicate: Predicate, predicates: Predicate*) = Ands(NonEmptyList(predicate, predicates: _*))
  private def T = True()
  private def F = Not(True())
}
