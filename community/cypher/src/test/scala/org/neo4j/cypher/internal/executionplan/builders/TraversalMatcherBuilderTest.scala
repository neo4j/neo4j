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

import org.junit.{Before, Test}
import org.neo4j.cypher.internal.commands._
import org.scalatest.Assertions
import org.neo4j.cypher.GraphDatabaseTestBase
import org.neo4j.cypher.internal.executionplan.{ExecutionPlanInProgress, PartiallySolvedQuery}
import org.junit.Assert._
import org.neo4j.cypher.internal.commands.expressions.Literal
import org.neo4j.cypher.internal.pipes.ParameterPipe
import org.neo4j.cypher.internal.parser.v1_9.CypherParserImpl

class TraversalMatcherBuilderTest extends GraphDatabaseTestBase with Assertions with BuilderTest {
  var builder: TraversalMatcherBuilder = null

  @Before def init() {
    builder = new TraversalMatcherBuilder(graph)
  }

  @Test def should_not_accept_queries_without_patterns() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(NodeByIndex("n", "index", Literal("key"), Literal("expression"))))
    )

    assertFalse("This query should not be accepted", builder.canWorkWith(plan(new ParameterPipe(), q)))
  }

  @Test def should_accept_variable_length_paths() {
    val q = query("START me=node:node_auto_index(name = 'Jane') " +
                  "MATCH me-[:jane_knows*]->friend-[:has]->status " +
                  "RETURN me")

    assertTrue("This query should not be accepted", builder.canWorkWith(plan(new ParameterPipe(), q)))
  }

  @Test def should_not_accept_queries_with_varlength_paths() {
    val q = query("START me=node:node_auto_index(name = 'Tarzan'), you=node:node_auto_index(name = 'Jane') " +
                  "MATCH me-[:LOVES*]->banana-[:LIKES*]->you " +
                  "RETURN me")

    assertTrue("This query should be accepted", builder.canWorkWith(plan(new ParameterPipe(), q)))
  }

  @Test def should_handle_loops() {
    val q = query("START me=node:node_auto_index(name = 'Tarzan'), you=node:node_auto_index(name = 'Jane') " +
                  "MATCH me-[:LIKES]->(u1)<-[:LIKES]->you, me-[:HATES]->(u2)<-[:HATES]->you " +
                  "RETURN me")

    assertTrue("This query should be accepted", builder.canWorkWith(plan(new ParameterPipe(), q)))
  }

  @Test def should_not_take_on_path_expression_predicates() {
    val q = query("START a=node({self}) MATCH a-->b WHERE b-->() RETURN b")

    val testPlan = plan(new ParameterPipe(), q)
    assertTrue("This query should be accepted", builder.canWorkWith(testPlan))

    val newPlan = builder.apply(testPlan)

    assertQueryHasNotSolvedPathExpressions(newPlan)
  }


  def assertQueryHasNotSolvedPathExpressions(newPlan: ExecutionPlanInProgress) {
    newPlan.query.where.foreach {
      case Solved(pred) if pred.exists(_.isInstanceOf[PatternPredicate]) => fail("Didn't expect the predicate to be solved")
      case _                                                           =>
    }
  }

  val parser = new CypherParserImpl

  private def query(text: String): PartiallySolvedQuery = PartiallySolvedQuery(parser.parse(text).asInstanceOf[Query])
}