/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan.builders

import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.internal.commands.{Property, Equals, Literal}
import org.neo4j.cypher.internal.pipes.NullPipe
import org.neo4j.cypher.internal.executionplan.{Solved, Unsolved, PartiallySolvedQuery}
import org.scalatest.Assertions

class FilterBuilderTest extends Assertions with PipeBuilder {

  val builder = new FilterBuilder

  @Test
  def does_not_offer_to_solve_queries_without_start_items() {
    val q = PartiallySolvedQuery().
      copy(where = Seq(Unsolved(Equals(Property("s", "foo"), Literal("bar")))))

    assertFalse("Should be able to build on this", builder.isDefinedAt(new NullPipe(), q))
  }

  @Test
  def should_offer_to_filter_the_necessary_pipe_is_there() {
    val q = PartiallySolvedQuery().
      copy(where = Seq(Unsolved(Equals(Property("s", "foo"), Literal("bar")))))

    val pipe = createPipe(nodes = Seq("s"))

    assertTrue("Should be able to build on this", builder.isDefinedAt(pipe, q))
  }

  @Test
  def should_solve_the_predicates_that_are_possible_to_solve() {
    val q = PartiallySolvedQuery().
      copy(where = Seq(
      Unsolved(Equals(Property("s", "foo"), Literal("bar"))),
      Unsolved(Equals(Property("x", "foo"), Literal("bar"))))
    )

    val pipe = createPipe(nodes = Seq("s"))

    val (_, result) = builder(pipe, q)

    assert(result.where.toSet === Set(
      Solved(Equals(Property("s", "foo"), Literal("bar"))),
      Unsolved(Equals(Property("x", "foo"), Literal("bar")))))
  }
}