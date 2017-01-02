/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.executionplan.builders

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_2.IndexHintException
import org.neo4j.cypher.internal.compiler.v2_2.commands._
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions._
import org.neo4j.cypher.internal.compiler.v2_2.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.PartiallySolvedQuery
import org.neo4j.cypher.internal.compiler.v2_2.pipes.NodeStartPipe
import org.neo4j.cypher.internal.compiler.v2_2.spi.PlanContext
import org.neo4j.kernel.api.index.IndexDescriptor

class StartPointBuilderTest extends BuilderTest {

  context = mock[PlanContext]
  val builder = new StartPointBuilder()

  test("says_yes_to_node_by_id_queries") {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(NodeByIndexQuery("s", "idx", Literal("foo")))))

    assertAccepts(q)
  }

  test("only_takes_one_start_item_at_the_time") {
    val q = PartiallySolvedQuery().
      copy(start = Seq(
        Unsolved(NodeByIndexQuery("s", "idx", Literal("foo"))),
        Unsolved(NodeByIndexQuery("x", "idx", Literal("foo")))))

    val remaining = assertAccepts(q).query

    remaining.start.filter(_.solved) should have length 1
    remaining.start.filterNot(_.solved) should have length 1
  }

  test("fixes_node_by_id_and_keeps_the_rest_around") {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(NodeByIndexQuery("s", "idx", Literal("foo"))), Unsolved(RelationshipById("x", 1))))


    val result = assertAccepts(q).query

    val expected = Set(Solved(NodeByIndexQuery("s", "idx", Literal("foo"))), Unsolved(RelationshipById("x", 1)))

    result.start.toSet should equal(expected)
  }

  test("says_no_to_already_solved_node_by_id_queries") {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeByIndexQuery("s", "idx", Literal("foo")))))

    assertRejects(q)
  }

  test("builds_a_nice_start_pipe") {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(NodeByIndexQuery("s", "idx", Literal("foo")))))

    val remainingQ = assertAccepts(q).query

    remainingQ.start should equal(Seq(Solved(NodeByIndexQuery("s", "idx", Literal("foo")))))
  }

  test("does_not_offer_to_solve_empty_queries") {
    //GIVEN WHEN THEN
    assertRejects(PartiallySolvedQuery())
  }

  test("offers_to_solve_query_with_index_hints") {
    val propertyKey= PropertyKey("name")
    val labelName: String = "Person"
    //GIVEN
    val q = PartiallySolvedQuery().copy(
      where = Seq(Unsolved(Equals(Property(Identifier("n"), propertyKey), Literal("Stefan")))),
      start = Seq(Unsolved(SchemaIndex("n", labelName, propertyKey.name, AnyIndex, Some(SingleQueryExpression(Literal("a")))))))

    when(context.getIndexRule(labelName, propertyKey.name)).thenReturn(Some(new IndexDescriptor(123,456)))

    //THEN
    val producedPlan = assertAccepts(q)

    producedPlan.pipe shouldBe a [NodeStartPipe]
  }

  test("throws_exception_if_no_index_is_found") {
    //GIVEN
    val propertyKey= PropertyKey("name")
    val q = PartiallySolvedQuery().copy(
      where = Seq(Unsolved(Equals(Property(Identifier("n"), propertyKey), Literal("Stefan")))),
      start = Seq(Unsolved(SchemaIndex("n", "Person", "name", AnyIndex, None))))

    when(context.getIndexRule(any(), any())).thenReturn(None)

    //THEN
    intercept[IndexHintException](assertAccepts(q))
  }

  test("says_yes_to_global_queries") {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(AllNodes("s"))))

    assertAccepts(q)
  }

  test("says_yes_to_global_rel_queries") {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(AllRelationships("s"))))

    assertAccepts(q)
  }

  test("only_takes_one_global_start_item_at_the_time") {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(AllNodes("s")), Unsolved(AllNodes("x"))))

    val remaining = assertAccepts(q).query

    remaining.start.filter(_.solved) should have length 1
    remaining.start.filterNot(_.solved) should have length 1
  }
}
