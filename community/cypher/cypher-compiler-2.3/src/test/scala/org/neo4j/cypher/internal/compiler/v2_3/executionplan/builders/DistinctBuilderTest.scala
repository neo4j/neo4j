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

import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions._
import org.neo4j.cypher.internal.compiler.v2_3.commands.{ReturnItem, SortItem}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{ExecutionPlanInProgress, PartiallySolvedQuery}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.FakePipe
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

class DistinctBuilderTest extends BuilderTest {

  val builder = new DistinctBuilder

  test("does_not_offer_to_solve_queries_with_aggregations") {
    val q = PartiallySolvedQuery().
      copy(returns = Seq(Unsolved(ReturnItem(Literal(42), "42"))),
      aggregation = Seq(Unsolved(CountStar())) ,
      aggregateToDo = true)

    assertRejects(q)
  }

  test("does_not_offer_to_solve_solved_queries") {
    val q = PartiallySolvedQuery().
      copy(returns = Seq(Unsolved(ReturnItem(Literal(42), "42"))),
      aggregation = Seq.empty ,
      aggregateToDo = false)

    assertRejects(q)
  }

  test("does_offer_to_solve_queries_without_empty_aggregations") {
    val q = PartiallySolvedQuery().
      copy(returns = Seq(Unsolved(ReturnItem(Literal(42), "42"))),
      aggregation = Seq.empty,
      aggregateToDo = true)

    assertAccepts(q)
  }

  test("should_rewrite_expressions_coming_after_return") {
    val query = PartiallySolvedQuery().
      copy(
      returns = Seq(Unsolved(ReturnItem(IdFunction(Identifier("n")), "42"))),
      aggregation = Seq.empty,
      aggregateToDo = true,
      sort = Seq(Unsolved(SortItem(IdFunction(Identifier("n")), ascending = false))))

    val pipe = new FakePipe(Iterator.empty, ("n", CTNode))
    val planInProgress: ExecutionPlanInProgress = plan(pipe, query)

    val resultPlan: ExecutionPlanInProgress = assertAccepts(planInProgress)
    resultPlan.query.sort should have size 1
    withClue("didn't rewrite the expression to a cached one")(resultPlan.query.sort.head.token.expression.isInstanceOf[CachedExpression]) should equal(true)
  }
}
