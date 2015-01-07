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

import org.neo4j.cypher.internal.compiler.v1_9.executionplan.PartiallySolvedQuery
import org.junit.Test
import org.junit.Assert._
import org.hamcrest.CoreMatchers.instanceOf
import org.hamcrest.CoreMatchers.not
import org.neo4j.cypher.internal.compiler.v1_9.pipes.{TransactionStartPipe, ExecuteUpdateCommandsPipe}
import org.neo4j.cypher.internal.compiler.v1_9.commands._
import expressions.{HeadFunction, Identifier}
import org.neo4j.cypher.internal.compiler.v1_9.mutation.{CreateRelationship, CreateNode}

class CreateNodesAndRelationshipsBuilderTest extends BuilderTest {

  val builder = new CreateNodesAndRelationshipsBuilder(null)

  @Test
  def does_not_offer_to_solve_queries_without_start_items() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(NodeById("s", 0))))

    assertFalse("Should be able to build on this", builder.canWorkWith(plan(q)))
  }

  @Test
  def does_offer_to_solve_queries_without_start_items() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(CreateNodeStartItem(CreateNode("r", Map())))))

    assertTrue("Should be able to build on this", builder.canWorkWith(plan(q)))
  }

  @Test
  def full_path() {
    val q = PartiallySolvedQuery().copy(start = Seq(
      Unsolved(CreateRelationshipStartItem(CreateRelationship("r1", (Identifier("a"), Map()), (Identifier("  UNNAMED1"), Map()), "KNOWS", Map()))),
      Unsolved(CreateRelationshipStartItem(CreateRelationship("r2", (Identifier("b"), Map()), (Identifier("  UNNAMED1"), Map()), "LOVES", Map())))))


    val startPipe = createPipe(Seq("a", "b"))

    assertTrue("Should be able to build on this", builder.canWorkWith(plan(startPipe, q)))
  }

  @Test
  def single_relationship_missing_nodes() {
    val q = PartiallySolvedQuery().copy(start = Seq(
      Unsolved(CreateRelationshipStartItem(CreateRelationship("r", (Identifier("a"), Map()), (Identifier("b"), Map()), "LOVES", Map())))))

    assertTrue("Should be able to build on this", builder.canWorkWith(plan(q)))
  }

  @Test
  def single_relationship_missing_nodes_with_expression() {
    val q = PartiallySolvedQuery().copy(updates = Seq(
      Unsolved(CreateRelationship("r", (HeadFunction(Identifier("p")), Map()), (Identifier("b"), Map()), "LOVES", Map()))))

    assertFalse("Should not be able to build on this", builder.canWorkWith(plan(q)))
  }

  @Test
  def inserts_tx_start_pipe() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(CreateNodeStartItem(CreateNode("r", Map())))))

    val resultPlan = builder(plan(q))

    assertTrue("The execution plan should be markes as containing a transaction", resultPlan.containsTransaction)

    val p = resultPlan.pipe.asInstanceOf[ExecuteUpdateCommandsPipe]

    val inner = p.source
    assertTrue("Expected a transaction pipe, got: " + inner.getClass(), inner.getClass() == classOf[TransactionStartPipe])
  }

  @Test
  def does_not_start_transaction() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(CreateNodeStartItem(CreateNode("r", Map())))))

    val inputPlan = plan(q).copy(containsTransaction = true)
    val resultPlan = builder(inputPlan)

    assertTrue(resultPlan.containsTransaction)

    val inner = resultPlan.pipe.asInstanceOf[ExecuteUpdateCommandsPipe].source

    assertFalse("Expected not a transaction pipe, got: " + inner.getClass(), inner.getClass() == classOf[TransactionStartPipe])
  }
}
