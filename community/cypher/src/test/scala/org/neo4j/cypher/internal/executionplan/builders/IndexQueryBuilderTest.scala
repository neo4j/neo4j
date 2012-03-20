package org.neo4j.cypher.internal.executionplan.builders

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

import org.scalatest.Assertions
import org.junit.Assert._
import org.neo4j.cypher.internal.pipes.NullPipe
import org.neo4j.cypher.internal.executionplan.{Solved, Unsolved, PartiallySolvedQuery}
import org.junit.Test
import org.neo4j.cypher.internal.commands._

class IndexQueryBuilderTest extends Assertions {

  val builder = new IndexQueryBuilder(null)
  //case class NodeByIndexQuery(varName:String, idxName: String, query: Expression) extends NodeStartItem(varName)
  @Test
  def says_yes_to_node_by_id_queries() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(NodeByIndexQuery("s", "idx", Literal("foo")))))

    assertTrue("Should be able to build on this", builder.isDefinedAt(new NullPipe(), q))
  }

  @Test
  def only_takes_one_start_item_at_the_time() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(
        Unsolved(NodeByIndexQuery("s", "idx", Literal("foo"))),
        Unsolved(NodeByIndexQuery("x", "idx", Literal("foo")))))

    val (_, remaining) = builder(new NullPipe(), q)

    assertEquals("No more than 1 startitem should be solved", 1, remaining.start.filter(_.solved).length)
    assertEquals("Stuff should remain", 1, remaining.start.filterNot(_.solved).length)
  }

  @Test
  def fixes_node_by_id_and_keeps_the_rest_around() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(NodeByIndexQuery("s", "idx", Literal("foo"))), Unsolved(RelationshipById("x", 1))))


    val (_, result) = builder(new NullPipe(), q)

    val expected = Set(Solved(NodeByIndexQuery("s", "idx", Literal("foo"))), Unsolved(RelationshipById("x", 1)))

    assert(result.start.toSet === expected)
  }

  @Test
  def says_no_to_already_solved_node_by_id_queries() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeByIndexQuery("s", "idx", Literal("foo")))))

    assertFalse("Should not build on this", builder.isDefinedAt(new NullPipe(), q))
  }

  @Test
  def builds_a_nice_start_pipe() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(NodeByIndexQuery("s", "idx", Literal("foo")))))

    val (_, remainingQ) = builder(new NullPipe(), q)

    assert(remainingQ.start === Seq(Solved(NodeByIndexQuery("s", "idx", Literal("foo")))))
  }
}