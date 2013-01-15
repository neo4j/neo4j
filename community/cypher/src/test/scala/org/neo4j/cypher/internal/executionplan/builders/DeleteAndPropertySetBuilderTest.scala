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

import org.neo4j.cypher.internal.executionplan.PartiallySolvedQuery
import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.internal.mutation.DeleteEntityAction
import org.neo4j.cypher.internal.commands.expressions.Identifier

class DeleteAndPropertySetBuilderTest extends BuilderTest {
  val builder = new UpdateActionBuilder(null)

  @Test
  def does_not_offer_to_solve_done_queries() {
    val q = PartiallySolvedQuery().
      copy(updates = Seq(Solved(DeleteEntityAction(Identifier("x")))))

    assertFalse("Should not be able to build on this", builder.canWorkWith(plan(q)))
  }

  @Test
  def offers_to_solve_queries() {
    val q = PartiallySolvedQuery().
      copy(updates = Seq(Unsolved(DeleteEntityAction(Identifier("x")))))

    val pipe = createPipe(nodes = Seq("x"))

    val executionPlan = plan(pipe, q)
    assertTrue("Should accept this", builder.canWorkWith(executionPlan))

    val resultPlan = builder(executionPlan)
    val resultQ = resultPlan.query

    assert(resultQ === q.copy(updates = q.updates.map(_.solve)))
    assertTrue("Execution plan should contain transaction", resultPlan.isUpdating)
  }

  @Test
  def does_not_offer_to_delete_something_not_yet_there() {
    val q = PartiallySolvedQuery().
      copy(updates = Seq(Unsolved(DeleteEntityAction(Identifier("x")))))

    val executionPlan = plan(q)
    assertFalse("Should not accept this", builder.canWorkWith(executionPlan))
  }
}