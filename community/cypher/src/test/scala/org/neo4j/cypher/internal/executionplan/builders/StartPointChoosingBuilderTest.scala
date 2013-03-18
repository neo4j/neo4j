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

import org.junit.Test
import org.neo4j.cypher.internal.executionplan.PartiallySolvedQuery
import org.neo4j.cypher.internal.commands._
import expressions.{Property, Literal, Identifier}
import values.LabelName
import org.neo4j.cypher.internal.commands.HasLabel
import org.neo4j.cypher.internal.spi.PlanContext
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.neo4j.cypher.UnableToPickIndexException


class StartPointChoosingBuilderTest extends BuilderTest with MockitoSugar {
  def builder = new StartPointChoosingBuilder

  override val context = mock[PlanContext]

  @Test
  def should_not_accept_queries_with_start_items() {
    assertRejects(
      q(start = Seq(Unsolved(NodeById("n", 0))))
    )

    assertRejects(
      q(start = Seq(Solved(NodeById("n", 0))))
    )
  }

  @Test
  def should_pick_an_index_if_only_one_possible_exists() {
    // Given
    val identifier = "n"
    val label = "Person"
    val property = "prop"
    val expression = Literal(42)
    val query = q(where = Seq(
      Unsolved(HasLabel(Identifier(identifier), Seq(LabelName(label)))),
      Unsolved(Equals(Property(Identifier(identifier), property), expression))
    ))

    when( context.getIndexRuleId( "Person", "prop" ) ).thenReturn( Some(1337l) )

    // When
    val plan = assertAccepts(query)

    // Then
    assert(plan.query.start.toList === Seq(Unsolved(SchemaIndex(identifier, label, property, None))))
  }

  @Test
  def should_pick_an_index_if_only_one_possible_exists_other_side() {
    // Given
    val identifier = "n"
    val label = "Person"
    val property = "prop"
    val expression = Literal(42)
    val query = q(where = Seq(
      Unsolved(HasLabel(Identifier(identifier), Seq(LabelName(label)))),
      Unsolved(Equals(expression, Property(Identifier(identifier), property)))
    ))

    when( context.getIndexRuleId( "Person", "prop" ) ).thenReturn( Some(1337l) )

    // When
    val plan = assertAccepts(query)

    // Then
    assert(plan.query.start.toList === List(Unsolved(SchemaIndex(identifier, label, property, None))))
  }

  @Test
  def should_throw_if_multiple_possible_indexes_are_available() {
    // Given
    val identifier = "n"
    val label = "Person"
    val property1 = "prop1"
    val property2 = "prop2"
    val expression = Literal(42)
    val query = q(where = Seq(
      Unsolved(HasLabel(Identifier(identifier), Seq(LabelName(label)))),
      Unsolved(Equals(Property(Identifier(identifier), property1), expression)),
      Unsolved(Equals(Property(Identifier(identifier), property2), expression))
    ))

    when( context.getIndexRuleId( "Person", "prop1" ) ).thenReturn( Some(1337l) )
    when( context.getIndexRuleId( "Person", "prop2" ) ).thenReturn( Some(1338l) )

    // When
    intercept[UnableToPickIndexException]( assertAccepts(query) )
  }

  @Test
  def should_produce_label_start_points_when_no_property_predicate_is_used() {
    // Given MATCH n:Person
    val identifier = "n"
    val label = "Person"
    val query = q(where = Seq(
      Unsolved(HasLabel(Identifier(identifier), Seq(LabelName(label))))
    ))

    // When
    val plan = assertAccepts(query)

    assert(plan.query.start.toList === List(Unsolved(NodeByLabel("n", "Person"))))
  }

  @Test
  def should_produce_label_start_points_when_no_matching_index_exist() {
    // Given
    val identifier = "n"
    val label = "Person"
    val property = "prop"
    val expression = Literal(42)
    val query = q(where = Seq(
      Unsolved(HasLabel(Identifier(identifier), Seq(LabelName(label)))),
      Unsolved(Equals(Property(Identifier(identifier), property), expression))
    ))

    when( context.getIndexRuleId( "Person", "prop" ) ).thenReturn( None )

    // When
    val plan = assertAccepts(query)

    // Then
    assert(plan.query.start.toList === Seq(Unsolved(NodeByLabel("n", "Person"))))
  }

  private def q(start: Seq[QueryToken[StartItem]] = Seq(),
                where: Seq[QueryToken[Predicate]] = Seq()) =
    PartiallySolvedQuery().copy(
      start = start,
      where = where
    )
}