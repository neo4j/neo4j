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
package org.neo4j.cypher.internal.compiler.v1_9.executionplan.builders

import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.internal.compiler.v1_9.commands.expressions.{Identifier, Literal, Property}
import org.neo4j.cypher.internal.compiler.v1_9.executionplan.PartiallySolvedQuery
import org.neo4j.cypher.internal.compiler.v1_9.commands.Equals

class FilterBuilderTest extends BuilderTest {

  val builder = new FilterBuilder

  @Test
  def does_not_offer_to_solve_queries_without_start_items() {
    val q = PartiallySolvedQuery().
      copy(where = Seq(Unsolved(Equals(Property(Identifier("s"), "foo"), Literal("bar")))))

    assertFalse("Should be able to build on this", builder.canWorkWith(plan(q)))
  }

  @Test
  def should_offer_to_filter_the_necessary_pipe_is_there() {
    val q = PartiallySolvedQuery().
      copy(where = Seq(Unsolved(Equals(Property(Identifier("s"), "foo"), Literal("bar")))))

    val pipe = createPipe(nodes = Seq("s"))

    assertTrue("Should be able to build on this", builder.canWorkWith(plan(pipe, q)))
  }

  @Test
  def should_solve_the_predicates_that_are_possible_to_solve() {
    val q = PartiallySolvedQuery().
      copy(where = Seq(
      Unsolved(Equals(Property(Identifier("s"), "foo"), Literal("bar"))),
      Unsolved(Equals(Property(Identifier("x"), "foo"), Literal("bar"))))
    )

    val pipe = createPipe(nodes = Seq("s"))

    val resultPlan = builder(plan(pipe, q))

    assert(resultPlan.query.where.toSet === Set(
      Solved(Equals(Property(Identifier("s"), "foo"), Literal("bar"))),
      Unsolved(Equals(Property(Identifier("x"), "foo"), Literal("bar")))))
  }
}
