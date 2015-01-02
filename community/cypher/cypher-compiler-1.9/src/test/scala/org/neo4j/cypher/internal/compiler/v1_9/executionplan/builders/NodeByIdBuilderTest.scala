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

import org.junit.Assert._
import org.junit.{Ignore, Test}
import org.neo4j.cypher.internal.compiler.v1_9.commands.{RelationshipById, NodeById}
import org.neo4j.cypher.internal.compiler.v1_9.executionplan.{PlanBuilder, PartiallySolvedQuery}
import org.neo4j.cypher.internal.compiler.v1_9.commands.expressions.ParameterExpression

class NodeByIdBuilderTest extends BuilderTest {

  val builder: PlanBuilder = new NodeByIdBuilder(null)

  @Test
  def says_yes_to_node_by_id_queries() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(NodeById("s", 0))))

    assertTrue("Should be able to build on this", builder.canWorkWith(plan(q)))
  }

  @Test
  def only_takes_one_start_item_at_the_time() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(NodeById("s", 0)), Unsolved(NodeById("x", 1))))

    val remaining = builder(plan(q)).query

    assertEquals("No more than 1 startitem should be solved", 1, remaining.start.filter(_.solved).length)
    assertEquals("Stuff should remain", 1, remaining.start.filterNot(_.solved).length)
  }

  @Test
  def fixes_node_by_id_and_keeps_the_rest_around() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(NodeById("s", 0)), Unsolved(RelationshipById("x", 1))))


    val result = builder(plan(q)).query

    val expected = Set(Solved(NodeById("s", 0)), Unsolved(RelationshipById("x", 1)))

    assert(result.start.toSet === expected)
  }

  @Test
  def says_no_to_already_solved_node_by_id_queries() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeById("s", 0))))

    assertFalse("Should not build on this", builder.canWorkWith(plan(q)))
  }

  @Test
  @Ignore("revisit when we consider this")
  def say_no_id_param_is_needed() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(NodeById("s", ParameterExpression("x")))))

    assertFalse("Should not build on this", builder.canWorkWith(plan(q)))
  }

  @Test
  def builds_a_nice_start_pipe() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(NodeById("s", 0))))

    val remainingQ = builder(plan(q)).query

    assert(remainingQ.start === Seq(Solved(NodeById("s", 0))))
  }
}
