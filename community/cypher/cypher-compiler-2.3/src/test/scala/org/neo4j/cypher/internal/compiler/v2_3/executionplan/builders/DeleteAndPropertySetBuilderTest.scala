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

import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Identifier
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.PartiallySolvedQuery
import org.neo4j.cypher.internal.compiler.v2_3.mutation.DeleteEntityAction
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext

class DeleteAndPropertySetBuilderTest extends BuilderTest {

  val builder = new UpdateActionBuilder
  val planContext = mock[PlanContext]

  test("does_not_offer_to_solve_done_queries") {
    val q = PartiallySolvedQuery().
      copy(updates = Seq(Solved(DeleteEntityAction(Identifier("x"), forced = false))))

    withClue("Should not be able to build on this")(builder.canWorkWith(plan(q), planContext)) should equal(false)
  }

  test("offers_to_solve_queries") {
    val q = PartiallySolvedQuery().
      copy(updates = Seq(Unsolved(DeleteEntityAction(Identifier("x"), forced = false))))

    val pipe = createPipe(nodes = Seq("x"))

    val executionPlan = plan(pipe, q)
    withClue("Should accept this")(builder.canWorkWith(executionPlan, planContext)) should equal(true)

    val resultPlan = builder(executionPlan, planContext)
    val resultQ = resultPlan.query

    resultQ should equal(q.copy(updates = q.updates.map(_.solve)))
    withClue("Execution plan should contain transaction")(resultPlan.isUpdating) should equal(true)
  }

  test("does_not_offer_to_delete_something_not_yet_there") {
    val q = PartiallySolvedQuery().
      copy(updates = Seq(Unsolved(DeleteEntityAction(Identifier("x"), forced = false))))

    val executionPlan = plan(q)
    withClue("Should not accept this")(builder.canWorkWith(executionPlan, planContext)) should equal(false)
  }
}
