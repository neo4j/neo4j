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
import org.neo4j.cypher.internal.compiler.v1_9.commands.expressions._
import org.neo4j.cypher.internal.compiler.v1_9.executionplan.{ExecutionPlanInProgress, PartiallySolvedQuery}
import org.neo4j.cypher.internal.compiler.v1_9.commands.{SortItem, ReturnItem}
import org.neo4j.cypher.internal.compiler.v1_9.commands.expressions.IdFunction
import org.neo4j.cypher.internal.compiler.v1_9.pipes.FakePipe
import org.neo4j.cypher.internal.compiler.v1_9.symbols.NodeType

class DistinctBuilderTest extends BuilderTest {

  val builder = new DistinctBuilder

  @Test
  def does_not_offer_to_solve_queries_with_aggregations() {
    val q = PartiallySolvedQuery().
      copy(returns = Seq(Unsolved(ReturnItem(Literal(42), "42"))),
      aggregation = Seq(Unsolved(CountStar())) ,
      aggregateQuery = Unsolved(true))

    assertFalse("Should not be able to build on this", builder.canWorkWith(plan(q)))
  }

  @Test
  def does_not_offer_to_solve_solved_queries() {
    val q = PartiallySolvedQuery().
      copy(returns = Seq(Unsolved(ReturnItem(Literal(42), "42"))),
      aggregation = Seq.empty ,
      aggregateQuery = Solved(true))

    assertFalse("Should not be able to build on this", builder.canWorkWith(plan(q)))
  }

  @Test
  def does_offer_to_solve_queries_without_empty_aggregations() {
    val q = PartiallySolvedQuery().
      copy(returns = Seq(Unsolved(ReturnItem(Literal(42), "42"))),
      aggregation = Seq.empty,
      aggregateQuery = Unsolved(true))


    assertTrue("Should be able to build on this", builder.canWorkWith(plan(q)))
  }

  @Test
  def should_rewrite_expressions_coming_after_return() {
    val query = PartiallySolvedQuery().
      copy(
      returns = Seq(Unsolved(ReturnItem(IdFunction(Identifier("n")), "42"))),
      aggregation = Seq.empty,
      aggregateQuery = Unsolved(true),
      sort = Seq(Unsolved(SortItem(IdFunction(Identifier("n")), ascending = false))))

    val pipe = new FakePipe(Iterator.empty, ("n", NodeType()))
    val planInProgress: ExecutionPlanInProgress = plan(pipe, query)

    assertTrue("Should be able to build on this", builder.canWorkWith(planInProgress))

    val resultPlan: ExecutionPlanInProgress = builder(planInProgress)
    assertTrue("Expected to have a single sort item", resultPlan.query.sort.size == 1)
    assertTrue("didn't rewrite the expression to a cached one", resultPlan.query.sort.head.token.expression.isInstanceOf[CachedExpression] )
  }
}
