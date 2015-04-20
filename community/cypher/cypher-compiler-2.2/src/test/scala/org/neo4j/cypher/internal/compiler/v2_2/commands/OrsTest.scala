/*
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
package org.neo4j.cypher.internal.compiler.v2_2.commands

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.neo4j.cypher.internal.compiler.v2_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_2.pipes.QueryStateHelper

class OrsTest extends CypherFunSuite {
  private implicit val state = QueryStateHelper.empty
  private val ctx = ExecutionContext.empty

  private val nullPredicate = mock[Predicate]
  when(nullPredicate.isMatch(ctx)).thenReturn(None)

  private val explodingPredicate = mock[Predicate]
  when(explodingPredicate.isMatch(any())(any())).thenThrow(new IllegalStateException("there is something wrong"))

  test("should return null if there are no true values and one or more nulls") {
    ors(F, nullPredicate).isMatch(ctx) should equal(None)
  }

  test("should quit early when finding a true value") {
    ors(T, explodingPredicate).isMatch(ctx) should equal(Some(true))
  }

  test("should return false if all predicates evaluate to false") {
    ors(F, F).isMatch(ctx) should equal(Some(false))
  }

  test("should return true instead of null") {
    ors(nullPredicate, T).isMatch(ctx) should equal(Some(true))
  }

  private def ors(predicates: Predicate*) = Ors(predicates.toList)
  private def T = True()
  private def F = Not(True())
}
