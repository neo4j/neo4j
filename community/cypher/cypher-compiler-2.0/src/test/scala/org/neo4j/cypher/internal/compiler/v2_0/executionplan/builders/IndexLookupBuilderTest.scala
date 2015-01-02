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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders

import org.junit.Test
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.PartiallySolvedQuery
import org.neo4j.cypher.internal.compiler.v2_0.commands._
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Identifier
import org.neo4j.cypher.IndexHintException
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.{KeyToken, TokenType}
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.TokenType._
import org.neo4j.cypher.internal.compiler.v2_0.commands.SchemaIndex
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Literal
import org.neo4j.cypher.internal.compiler.v2_0.commands.Equals
import scala.Some
import org.neo4j.cypher.internal.compiler.v2_0.commands.HasLabel
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Property

class IndexLookupBuilderTest extends BuilderTest {

  def builder = new IndexLookupBuilder()

  @Test def should_not_accept_empty_query() {
    assertRejects(PartiallySolvedQuery())
  }

  @Test def should_accept_a_query_with_index_hints() {
    //GIVEN
    val identifier = "id"
    val label = "label"
    val property = "prop"
    val valueExpression = Literal(42)
    val predicate = Equals(Property(Identifier(identifier), PropertyKey(property)), valueExpression)


    test(identifier, label, property, predicate, valueExpression)
  }

  @Test def should_accept_a_query_with_index_hints2() {
    //GIVEN
    val identifier = "id"
    val label = "label"
    val property = "prop"
    val valueExpression = Literal(42)
    val predicate = Equals(valueExpression, Property(Identifier(identifier), PropertyKey(property)))


    test(identifier, label, property, predicate, valueExpression)
  }

  @Test def should_throw_if_no_matching_index_is_found() {
    //GIVEN
    val identifier = "id"
    val label = "label"
    val property = "prop"

    val q = PartiallySolvedQuery().copy(
      start = Seq(Unsolved(SchemaIndex(identifier, label, property, AnyIndex, None)))
    )

    //WHEN
    intercept[IndexHintException](assertAccepts(q))
  }

  @Test
  def should_pick_out_correct_label_predicate() {
    //GIVEN
    val identifier = "id"
    val label1 = "label1"
    val label2 = "label2"
    val property = "prop"
    val valueExpression = Literal(42)

    val label1Predicate = HasLabel(Identifier(identifier), KeyToken.Unresolved(label1, TokenType.Label))
    val label2Predicate = HasLabel(Identifier(identifier), KeyToken.Unresolved(label2, TokenType.Label))
    val propertyPredicate = Equals(valueExpression, Property(Identifier(identifier), PropertyKey(property)))

    val predicates: Seq[Unsolved[Predicate]] = Seq(
      Unsolved(label1Predicate),
      Unsolved(label2Predicate),
      Unsolved(propertyPredicate))

    val q = PartiallySolvedQuery().copy(
      start = Seq(Unsolved(SchemaIndex(identifier, label1, property, AnyIndex, None))),
      where = predicates
    )

    //WHEN
    val plan = assertAccepts(q)

    //THEN
    assert(plan.query.start === Seq(Unsolved(SchemaIndex(identifier, label1, property, AnyIndex, Some(valueExpression)))))
    val a = plan.query.where.toSet
    val b = Set(Solved(label1Predicate), Unsolved(label2Predicate), Solved(propertyPredicate))
    assert(a === b)
  }

  private def test(identifier: String, label: String, property: String, predicate: Equals, valueExpression: Literal) {
    val labelPredicate = HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label))

    val q = PartiallySolvedQuery().copy(
      start = Seq(Unsolved(SchemaIndex(identifier, label, property, AnyIndex, None))),
      where = Seq(Unsolved(predicate), Unsolved(labelPredicate))
    )

    //WHEN
    val plan = assertAccepts(q)

    //THEN
    assert(plan.query.start === Seq(Unsolved(SchemaIndex(identifier, label, property, AnyIndex, Some(valueExpression)))))
    assert(plan.query.where.toSet === Set(Solved(predicate), Solved(labelPredicate)))
  }
}
