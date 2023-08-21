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

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Ands
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.IsFalse
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.IsTrue
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.IsUnknown
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Not
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.True
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class AndsTest extends CypherFunSuite {
  private val state = QueryStateHelper.empty
  private val ctx = CypherRow.empty

  private val nullPredicate = mock[Predicate]
  when(nullPredicate.isMatch(ctx, state)).thenReturn(IsUnknown)
  when(nullPredicate.children).thenReturn(Seq.empty)

  private val explodingPredicate = mock[Predicate]
  when(explodingPredicate.isMatch(any(), any())).thenThrow(new IllegalStateException("there is something wrong"))
  when(explodingPredicate.children).thenReturn(Seq.empty)

  test("should return null if there are no false values and one or more nulls") {
    ands(T, nullPredicate).isMatch(ctx, state) should equal(IsUnknown)
  }

  test("should quit early when finding a false value") {
    ands(F, explodingPredicate).isMatch(ctx, state) should equal(IsFalse)
  }

  test("should return true if all predicates evaluate to true") {
    ands(T, T).isMatch(ctx, state) should equal(IsTrue)
  }

  test("should return false instead of null") {
    ands(nullPredicate, F).isMatch(ctx, state) should equal(IsFalse)
  }

  private def ands(predicate: Predicate, predicates: Predicate*) = Ands(NonEmptyList(predicate, predicates: _*))
  private def T = True()
  private def F = Not(True())
}
