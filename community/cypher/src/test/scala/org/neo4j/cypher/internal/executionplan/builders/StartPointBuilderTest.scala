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

import org.junit.Assert._
import org.neo4j.cypher.internal.executionplan.PartiallySolvedQuery
import org.junit.Test
import org.neo4j.cypher.internal.commands._
import expressions._
import expressions.Literal
import expressions.Property
import org.neo4j.cypher.internal.pipes.NodeStartPipe
import org.scalatest.mock.MockitoSugar
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.spi.PlanContext
import org.neo4j.cypher.IndexHintException
import org.neo4j.cypher.internal.commands.SchemaIndex
import org.neo4j.cypher.internal.commands.AllNodes
import org.neo4j.cypher.internal.commands.Equals
import org.neo4j.cypher.internal.commands.NodeByIndexQuery
import org.neo4j.kernel.impl.api.index.IndexDescriptor

class StartPointBuilderTest extends BuilderTest with MockitoSugar {

  override val context = mock[PlanContext]
  val builder = new StartPointBuilder()

  @Test
  def says_yes_to_node_by_id_queries() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(NodeByIndexQuery("s", "idx", Literal("foo")))))

    assertAccepts(q)
  }

  @Test
  def only_takes_one_start_item_at_the_time() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(
        Unsolved(NodeByIndexQuery("s", "idx", Literal("foo"))),
        Unsolved(NodeByIndexQuery("x", "idx", Literal("foo")))))

    val remaining = assertAccepts(q).query

    assertEquals("No more than 1 startitem should be solved", 1, remaining.start.filter(_.solved).length)
    assertEquals("Stuff should remain", 1, remaining.start.filterNot(_.solved).length)
  }

  @Test
  def fixes_node_by_id_and_keeps_the_rest_around() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(NodeByIndexQuery("s", "idx", Literal("foo"))), Unsolved(RelationshipById("x", 1))))


    val result = assertAccepts(q).query

    val expected = Set(Solved(NodeByIndexQuery("s", "idx", Literal("foo"))), Unsolved(RelationshipById("x", 1)))

    assert(result.start.toSet === expected)
  }

  @Test
  def says_no_to_already_solved_node_by_id_queries() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeByIndexQuery("s", "idx", Literal("foo")))))

    assertRejects(q)
  }

  @Test
  def builds_a_nice_start_pipe() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(NodeByIndexQuery("s", "idx", Literal("foo")))))

    val remainingQ = assertAccepts(q).query

    assert(remainingQ.start === Seq(Solved(NodeByIndexQuery("s", "idx", Literal("foo")))))
  }

  @Test
  def does_not_offer_to_solve_empty_queries() {
    //GIVEN WHEN THEN
    assertRejects(PartiallySolvedQuery())
  }

  @Test
  def offers_to_solve_query_with_index_hints() {
    val propertyKey: String = "name"
    val labelName: String = "Person"
    //GIVEN
    val q = PartiallySolvedQuery().copy(
      where = Seq(Unsolved(Equals(Property(Identifier("n"), propertyKey), Literal("Stefan")))),
      start = Seq(Unsolved(SchemaIndex("n", labelName, propertyKey, Some(Literal("a"))))))

    when(context.getIndexRule(labelName, propertyKey)).thenReturn(Some(new IndexDescriptor(123,456)))

    //THEN
    val producedPlan = assertAccepts(q)

    assert(producedPlan.pipe.isInstanceOf[NodeStartPipe])
  }

  @Test
  def throws_exception_if_no_index_is_found() {
    //GIVEN
    val q = PartiallySolvedQuery().copy(
      where = Seq(Unsolved(Equals(Property(Identifier("n"), "name"), Literal("Stefan")))),
      start = Seq(Unsolved(SchemaIndex("n", "Person", "name", None))))

    when(context.getIndexRule(any(), any())).thenReturn(None)

    //THEN
    intercept[IndexHintException](assertAccepts(q))
  }

  @Test
  def says_yes_to_global_queries() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(AllNodes("s"))))

    assertAccepts(q)
  }

  @Test
  def says_yes_to_global_rel_queries() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(AllRelationships("s"))))

    assertAccepts(q)
  }

  @Test
  def only_takes_one_global_start_item_at_the_time() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(AllNodes("s")), Unsolved(AllNodes("x"))))

    val remaining = assertAccepts(q).query

    assertEquals("No more than 1 startitem should be solved", 1, remaining.start.filter(_.solved).length)
    assertEquals("Stuff should remain", 1, remaining.start.filterNot(_.solved).length)
  }
}