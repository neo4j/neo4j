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

import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{CountStar, Identifier}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{ExecutionPlanInProgress, PartiallySolvedQuery}

class AggregationBuilderTest extends BuilderTest {

  val builder = new AggregationBuilder

  test("should_accept_if_stuff_looks_ok") {
    val q = PartiallySolvedQuery().
      copy(
      aggregation = Seq(Unsolved(CountStar())),
      returns = Seq(Unsolved(ReturnItem(Identifier("n"), "n"))),
      aggregateToDo = true
    )

    val p = createPipe(nodes = Seq("n"))

    val plan = ExecutionPlanInProgress(q, p)

    val resultPlan = assertAccepts(plan)

    val expectedQuery = q.copy(
      aggregation = q.aggregation.map(_.solve),
      aggregateToDo = false,
      returns = Seq(Solved(ReturnItem(Identifier("n"), "n"))),
      extracted = true
    )

    resultPlan.query should equal(expectedQuery)
  }

  test("should_not_accept_if_there_are_still_other_things_to_do_in_the_query") {
    val q = PartiallySolvedQuery().
      copy(
      start = Seq(Unsolved(NodeById("n", 0))),
      aggregation = Seq(Unsolved(CountStar())),
      returns = Seq(Unsolved(ReturnItem(Identifier("n"), "n")))
    )

    val p = createPipe(nodes = Seq())
    val plan = ExecutionPlanInProgress(q, p)

    assertRejects(plan)
  }
}
