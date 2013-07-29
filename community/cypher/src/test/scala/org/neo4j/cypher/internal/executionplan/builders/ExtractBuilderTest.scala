/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.internal.executionplan.PartiallySolvedQuery
import org.neo4j.cypher.internal.commands.expressions._
import org.neo4j.cypher.internal.pipes.ExtractPipe
import org.neo4j.cypher.internal.commands.ReturnItem
import org.neo4j.cypher.internal.commands.expressions.AbsFunction
import org.neo4j.cypher.internal.commands.expressions.RandFunction
import org.neo4j.cypher.internal.executionplan.ExecutionPlanInProgress
import org.neo4j.cypher.internal.commands.expressions.Literal
import org.neo4j.cypher.internal.symbols.{DoubleType, NumberType}

class ExtractBuilderTest extends BuilderTest {

  val builder = new ExtractBuilder

  @Test
  def should_solve_the_predicates_that_are_possible_to_solve() {
    val q = PartiallySolvedQuery().
      copy(returns = Seq(Unsolved(ReturnItem(Literal("foo"), "foo")))
    )

    val p = createPipe(nodes = Seq("s"))

    assertTrue("This query should be accepted", builder.canWorkWith(plan(p, q)))

    val result = builder(plan(p, q)).query

    assertTrue("the builder did not mark the query as extracted", result.extracted)
  }

  @Test
  def should_not_accept_stuff_when_aggregation_is_not_done() {
    val q = PartiallySolvedQuery().
      copy(
      returns = Seq(Unsolved(ReturnItem(Literal("foo"), "foo"))),
      aggregateToDo = true
    )

    val p = createPipe(nodes = Seq("s"))

    assertFalse("This query should not be accepted", builder.canWorkWith(plan(p, q)))
  }

  @Test
  def should_not_accept_a_query_that_is_already_extracted() {
    val q = PartiallySolvedQuery().
      copy(
      returns = Seq(Unsolved(ReturnItem(Literal("foo"), "foo"))),
      extracted = true
    )

    val p = createPipe(nodes = Seq("s"))

    assertTrue("This query should be accepted", builder.canWorkWith(plan(p, q)))
  }

  @Test
  def does_not_introduce_extract_pipe_unless_necessary() {
    //GIVEN
    val q = PartiallySolvedQuery().
      copy(returns = Seq(Unsolved(ReturnItem(Identifier("foo"), "foo")))
    )

    val p = createPipe(nodes = Seq("foo"))

    val planInProgress = ExecutionPlanInProgress(q, p, isUpdating = false)

    //WHEN
    val resultPlan = builder(planInProgress)

    //THEN
    assert(!resultPlan.pipe.isInstanceOf[ExtractPipe], "No need to extract here")
  }

  @Test
  def should_materialize_non_deterministic_expressions() {
    val q = PartiallySolvedQuery().
      copy(returns = Seq(
        Unsolved(ReturnItem(AbsFunction(RandFunction()), "bar")),
        Unsolved(ReturnItem(AbsFunction(Literal(1)), "foo")))
    )

    val p = createPipe(nodes = Seq("s"))

    assertTrue("This query should be accepted", builder.canWorkWith(plan(p, q)))

    val result = builder(plan(p, q))

    assertTrue("the builder did not mark the query as extracted", result.query.extracted)

    val returnItems = result.query.returns.toSet
    assertEquals( Set(
      Solved(ReturnItem(CachedExpression("bar", DoubleType()), "bar")),
      Solved(ReturnItem(CachedExpression("foo", NumberType()), "foo"))
    ), returnItems )
  }

  @Test
  def should_not_cache_calls_to_rand() {
    val q = PartiallySolvedQuery().
      copy(returns = Seq(
      Unsolved(ReturnItem(AbsFunction(RandFunction()), "bar")),
      Unsolved(ReturnItem(AbsFunction(Literal(1)), "foo")))
    )

    val p = createPipe(nodes = Seq("s"))
    val planInProgress = plan(p, q)
    val expressions = Map("bar" -> AbsFunction(RandFunction()), "foo" -> AbsFunction(Literal(1)))
    val result: ExecutionPlanInProgress = ExtractBuilder.extractIfNecessary(planInProgress, expressions)
//    fail(result.query.toString)

    val returnItems = result.query.returns.toSet

    assertEquals(Set(
      Unsolved(ReturnItem(AbsFunction(RandFunction()), "bar")),
      Unsolved(ReturnItem(CachedExpression("foo", NumberType()), "foo"))
    ), returnItems)


    //    assertTrue("This query should be accepted", builder.canWorkWith(plan))
    //
    //    val result = builder(plan(p, q))
    //
    //    assertTrue("the builder did not mark the query as extracted", result.query.extracted)
    //
    //    val returnItems = result.query.returns.toSet
    //    assertEquals( Set(
    //      Unsolved(ReturnItem(AbsFunction(RandFunction()), "bar")),
    //      Unsolved(ReturnItem(CachedExpression("foo", NumberType()), "foo"))
    //    ), returnItems )
  }
}