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

/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v1_9.executionplan.{ExecutionPlanInProgress, PartiallySolvedQuery}
import org.neo4j.cypher.internal.compiler.v1_9.commands.ReturnItem
import org.neo4j.cypher.internal.compiler.v1_9.commands.expressions.{Identifier, Literal}
import org.neo4j.cypher.internal.compiler.v1_9.pipes.ExtractPipe

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
      aggregateQuery = Unsolved(true)
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

    val planInProgress = ExecutionPlanInProgress(q, p, false)

    //WHEN
    val resultPlan = builder(planInProgress)

    //THEN
    assert(!resultPlan.pipe.isInstanceOf[ExtractPipe], "No need to extract here")
  }
}
