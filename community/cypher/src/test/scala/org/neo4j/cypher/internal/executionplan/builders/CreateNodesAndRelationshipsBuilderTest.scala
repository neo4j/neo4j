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

import org.neo4j.cypher.internal.executionplan.PartiallySolvedQuery
import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.internal.commands.{NodeById, CreateNodeStartItem}
import org.hamcrest.core.IsInstanceOf.instanceOf
import org.hamcrest.core.IsNot.not
import org.neo4j.cypher.internal.pipes.{TransactionStartPipe, ExecuteUpdateCommandsPipe}

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
      copy(start = Seq(Unsolved(CreateNodeStartItem("r", Map()))))

    assertTrue("Should be able to build on this", builder.canWorkWith(plan(q)))
  }
  
  @Test
  def inserts_tx_start_pipe() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(CreateNodeStartItem("r", Map()))))

    val resultPlan = builder(plan(q))
    
    assertTrue("The execution plan should be markes as containing a transaction",  resultPlan.containsTransaction)
    
    val p = resultPlan.pipe.asInstanceOf[ExecuteUpdateCommandsPipe]
    
    val inner = p.source
    
    assertThat(inner, instanceOf(classOf[TransactionStartPipe]))
  }
  
  @Test
  def does_not_start_transaction() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(CreateNodeStartItem("r", Map()))))

    val inputPlan = plan(q).copy(containsTransaction = true)
    val resultPlan = builder(inputPlan)
    
    assertTrue(resultPlan.containsTransaction)
    
    val inner = resultPlan.pipe.asInstanceOf[ExecuteUpdateCommandsPipe].source
    
    assertThat(inner, not(instanceOf(classOf[TransactionStartPipe])))
  }
}