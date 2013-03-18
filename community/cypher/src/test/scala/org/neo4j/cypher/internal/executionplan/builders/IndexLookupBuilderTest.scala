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
import org.neo4j.cypher.internal.commands.{HasLabel, Equals, SchemaIndex}
import org.neo4j.cypher.internal.commands.expressions.{Literal, Property, Identifier}
import org.neo4j.cypher.IndexHintException
import org.neo4j.cypher.internal.commands.values.LabelName

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
    val predicate = Equals(Property(Identifier(identifier), property), valueExpression)


    test(identifier, label, property, predicate, valueExpression)
  }

  @Test def should_accept_a_query_with_index_hints2() {
    //GIVEN
    val identifier = "id"
    val label = "label"
    val property = "prop"
    val valueExpression = Literal(42)
    val predicate = Equals(valueExpression, Property(Identifier(identifier), property))


    test(identifier, label, property, predicate, valueExpression)
  }

  @Test def should_throw_if_no_matching_index_is_found() {
    //GIVEN
    val identifier = "id"
    val label = "label"
    val property = "prop"

    val q = PartiallySolvedQuery().copy(
      start = Seq(Unsolved(SchemaIndex(identifier, label, property, None)))
    )

    //WHEN
    intercept[IndexHintException](assertAccepts(q))
  }

  @Test
  def should_pick_out_correct_label_predicate() {
    //GIVEN
    val identifier = "id"
    val label1 = "label"
    val label2 = "apa"
    val property = "prop"
    val valueExpression = Literal(42)
    val predicate = Equals(valueExpression, Property(Identifier(identifier), property))

    val labelPredicate = HasLabel(Identifier(identifier), Seq(LabelName(label1), LabelName(label2)))
    val expectedLabelPredicates = Seq(
      Solved(HasLabel(Identifier(identifier), Seq(LabelName(label1)))),
      Unsolved(HasLabel(Identifier(identifier), Seq(LabelName(label2)))))

    val q = PartiallySolvedQuery().copy(
      start = Seq(Unsolved(SchemaIndex(identifier, label1, property, None))),
      where = Seq(Unsolved(predicate), Unsolved(labelPredicate))
    )

    //WHEN
    val plan = assertAccepts(q)

    //THEN
    assert(plan.query.start === Seq(Unsolved(SchemaIndex(identifier, label1, property, Some(valueExpression)))))
    assert(plan.query.where.toSet === (expectedLabelPredicates :+ Solved(predicate)).toSet)
  }

  private def test(identifier: String, label: String, property: String, predicate: Equals, valueExpression: Literal) {
    val labelPredicate = HasLabel(Identifier(identifier), Seq(LabelName(label)))

    val q = PartiallySolvedQuery().copy(
      start = Seq(Unsolved(SchemaIndex(identifier, label, property, None))),
      where = Seq(Unsolved(predicate), Unsolved(labelPredicate))
    )

    //WHEN
    val plan = assertAccepts(q)

    //THEN
    assert(plan.query.start === Seq(Unsolved(SchemaIndex(identifier, label, property, Some(valueExpression)))))
    assert(plan.query.where === Seq(Solved(predicate), Solved(labelPredicate)))
  }
}
