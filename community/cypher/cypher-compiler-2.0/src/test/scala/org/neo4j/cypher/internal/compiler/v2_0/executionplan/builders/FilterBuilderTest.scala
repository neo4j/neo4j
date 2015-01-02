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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders

import org.junit.Test
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions._
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.PartiallySolvedQuery
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.TokenType._
import org.neo4j.cypher.internal.compiler.v2_0.commands.{SingleNode, Predicate, Equals}
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.mock.MockitoSugar

class FilterBuilderTest extends BuilderTest with MockitoSugar {

  val builder = new FilterBuilder

  @Test
  def does_not_offer_to_solve_queries_without_start_items() {
    val q = PartiallySolvedQuery().
      copy(where = Seq(Unsolved(Equals(Property(Identifier("s"), PropertyKey("foo")), Literal("bar")))))

    assertRejects(q)
  }

  @Test
  def should_offer_to_filter_the_necessary_pipe_is_there() {
    val q = PartiallySolvedQuery().
      copy(where = Seq(Unsolved(Equals(Property(Identifier("s"), PropertyKey("foo")), Literal("bar")))))

    val pipe = createPipe(nodes = Seq("s"))

    assertAccepts(pipe, q)
  }

  @Test
  def should_solve_the_predicates_that_are_possible_to_solve() {
    val q = PartiallySolvedQuery().
      copy(where = Seq(
      Unsolved(Equals(Property(Identifier("s"), PropertyKey("foo")), Literal("bar"))),
      Unsolved(Equals(Property(Identifier("x"), PropertyKey("foo")), Literal("bar"))))
    )

    val pipe = createPipe(nodes = Seq("s"))

    val resultPlan = assertAccepts(pipe, q)

    assert(resultPlan.query.where.toSet === Set(
      Solved(Equals(Property(Identifier("s"), PropertyKey("foo")), Literal("bar"))),
      Unsolved(Equals(Property(Identifier("x"), PropertyKey("foo")), Literal("bar")))))
  }

  @Test
  def does_not_take_on_non_deterministic_predicates_until_the_whole_pattern_is_solved() {
    val nonDeterministicPredicate = mock[Predicate]
    when(nonDeterministicPredicate.isDeterministic).thenReturn(false)
    when(nonDeterministicPredicate.symbolDependenciesMet(any())).thenReturn(true)

    val q = PartiallySolvedQuery().copy(
      where = Seq(Unsolved(nonDeterministicPredicate)),
      patterns = Seq(Unsolved(SingleNode("a"))))

    val pipe = createPipe(nodes = Seq("s"))

    assertRejects(pipe, q)
  }

  @Test
  def takes_on_predicates_that_are_deterministic_as_soon_as_possible() {
    val nonDeterministicPredicate = mock[Predicate]
    when(nonDeterministicPredicate.isDeterministic).thenReturn(true)
    when(nonDeterministicPredicate.symbolDependenciesMet(any())).thenReturn(true)

    val q = PartiallySolvedQuery().copy(
      where = Seq(Unsolved(nonDeterministicPredicate)),
      patterns = Seq(Unsolved(SingleNode("a"))))

    val pipe = createPipe(nodes = Seq("s"))

    assertAccepts(pipe, q)
  }
}
