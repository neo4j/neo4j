/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions._
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.{Equals, HasLabel, Predicate}
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.TokenType._
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.{KeyToken, TokenType}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.PartiallySolvedQuery
import org.neo4j.cypher.internal.frontend.v2_3.ast
import org.neo4j.cypher.internal.frontend.v2_3.ast.{StartsWith, AstConstructionTestSupport}
import org.neo4j.cypher.internal.frontend.v2_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v2_3._

class IndexLookupBuilderTest extends BuilderTest {

  def builder = new IndexLookupBuilder()

  test("should_not_accept_empty_query") {
    assertRejects(PartiallySolvedQuery())
  }

  test("should_accept_a_query_with_equality_index_hints") {
    //GIVEN
    val identifier = "id"
    val label = "label"
    val property = "prop"
    val valueExpression = Literal(42)
    val predicate = Equals(Property(Identifier(identifier), PropertyKey(property)), valueExpression)

    check(identifier, label, property, predicate, valueExpression)
  }

  test("should_accept_a_query_with_equality_index_hints2") {
    //GIVEN
    val identifier = "id"
    val label = "label"
    val property = "prop"
    val valueExpression = Literal(42)
    val predicate = Equals(valueExpression, Property(Identifier(identifier), PropertyKey(property)))

    check(identifier, label, property, predicate, valueExpression)
  }

  test("should_accept_a_prefix_seek_query") {
    object inner extends AstConstructionTestSupport {

      def run() = {
        //GIVEN
        val startsWith: ast.StartsWith = ast.StartsWith(ast.Property(ident("n"), ast.PropertyKeyName("prop")_)_, ast.StringLiteral("prefix")_)_
        val predicate = toCommandPredicate(startsWith)

        // WHAM
        check("n", "label", "prop", predicate, RangeQueryExpression(PrefixSeekRangeExpression(PrefixRange(Literal("prefix")))))
      }
    }

    inner.run()
  }

  test("should accept a textual range seek query") {
    object inner extends AstConstructionTestSupport {

      def run() = {
        //GIVEN
        val property: ast.Property = ast.Property(ident("n"), ast.PropertyKeyName("prop")_)_
        val inequality = ast.AndedPropertyInequalities(ident("n"), property, NonEmptyList(ast.LessThanOrEqual(property, ast.StringLiteral("xxx")_)_))
        val predicate = toCommandPredicate(inequality)

        // WHAM
        check("n", "label", "prop", predicate, RangeQueryExpression(InequalitySeekRangeExpression(RangeLessThan(NonEmptyList(InclusiveBound(Literal("xxx")))))))
      }
    }

    inner.run()
  }

  test("should accept a parameterised range seek query") {
    object inner extends AstConstructionTestSupport {

      def run() = {
        //GIVEN
        val property: ast.Property = ast.Property(ident("n"), ast.PropertyKeyName("prop")_)_
        val inequality: StartsWith = ast.StartsWith(property, ast.Parameter("paramName")_)_
        val predicate = toCommandPredicate(inequality)

        // WHAM
        check("n", "label", "prop", predicate, RangeQueryExpression(PrefixSeekRangeExpression(PrefixRange(ParameterExpression("paramName")))))
      }
    }

    inner.run()
  }

  test("should accept a numerical range seek query") {
    object inner extends AstConstructionTestSupport {

      def run() = {
        //GIVEN
        val property: ast.Property = ast.Property(ident("n"), ast.PropertyKeyName("prop") _) _
        val inequality = ast.AndedPropertyInequalities(ident("n"), property, NonEmptyList(ast.GreaterThan(property, ast.SignedDecimalIntegerLiteral("42") _) _))
        val predicate = toCommandPredicate(inequality)

        // WHAM
        check("n", "label", "prop", predicate, RangeQueryExpression(
          InequalitySeekRangeExpression(RangeGreaterThan(NonEmptyList(ExclusiveBound(Literal(42)))))))
      }
    }

    inner.run()
  }

  test("should accept a numerical range seek query with many ranges") {
    object inner extends AstConstructionTestSupport {
      def run() = {
        //GIVEN
        val property: ast.Property = ast.Property(ident("n"), ast.PropertyKeyName("prop") _) _
        val inequality = ast.AndedPropertyInequalities(ident("n"), property, NonEmptyList(ast.GreaterThan(property, ast.SignedDecimalIntegerLiteral("10") _) _,
                                                                                          ast.LessThan(property, ast.SignedDecimalIntegerLiteral("100") _) _,
                                                                                          ast.GreaterThanOrEqual(property, ast.DecimalDoubleLiteral("15.5") _) _))
        val predicate = toCommandPredicate(inequality)

        // WHAM
        check("n", "label", "prop", predicate, RangeQueryExpression(InequalitySeekRangeExpression(RangeBetween(
          RangeGreaterThan(NonEmptyList(ExclusiveBound(Literal(10)), InclusiveBound(Literal(15.5)))),
          RangeLessThan(NonEmptyList(ExclusiveBound(Literal(100))))))))
      }
    }

    inner.run()
  }

  test("should_throw_if_no_matching_index_is_found") {
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

  test("should_pick_out_correct_label_predicate") {
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
    plan.query.start should equal(Seq(Unsolved(SchemaIndex(identifier, label1, property, AnyIndex, Some(SingleQueryExpression(valueExpression))))))
    plan.query.where.toSet should equal(Set(Solved(label1Predicate), Unsolved(label2Predicate), Solved(propertyPredicate)))
  }

  test("should_accept_a_query_with_index_hints_on_in") {
    //GIVEN
    val identifier = "id"
    val label = "label"
    val property = "prop"
    val collectionExpression: Expression = Collection(Literal(42),Literal(43))
    val predicate = AnyInCollection(collectionExpression,"_identifier_",Equals(Property(Identifier(identifier), PropertyKey(property)),Identifier("_identifier_")))

    check(identifier, label, property, predicate, ManyQueryExpression(collectionExpression))
  }

  test("should_accept_a_query_with_index_hints_on_in_with_collection_containing_duplicates") {
    //GIVEN
    val identifier = "id"
    val label = "label"
    val property = "prop"
    val collectionExpression: Expression = Collection(Literal(42),Literal(42))
    val predicate = AnyInCollection(collectionExpression,"_identifier_",Equals(Property(Identifier(identifier), PropertyKey(property)),Identifier("_identifier_")))

    check(identifier, label, property, predicate, ManyQueryExpression(collectionExpression))
  }

  test("should_accept_a_query_with_index_hints_on_in_with_null") {
    //GIVEN
    val identifier = "id"
    val label = "label"
    val property = "prop"
    val collectionExpression: Expression = Null()
    val predicate = AnyInCollection(collectionExpression,"_identifier_",Equals(Property(Identifier(identifier), PropertyKey(property)),Identifier("_identifier_")))

    check(identifier, label, property, predicate, ManyQueryExpression(collectionExpression))
  }

  test("should_accept_a_query_with_index_hints_on_in_with_empty_collection") {
    //GIVEN
    val identifier = "id"
    val label = "label"
    val property = "prop"
    val collectionExpression: Expression = Collection()
    val predicate = AnyInCollection(collectionExpression,"_identifier_",Equals(Property(Identifier(identifier), PropertyKey(property)),Identifier("_identifier_")))

    check(identifier, label, property, predicate, ManyQueryExpression(collectionExpression))
  }

  private def check(identifier: String, label: String, property: String, predicate: Equals, expression: Expression) {
    check(identifier, label, property, predicate, SingleQueryExpression(expression))
  }

  private def check(identifier: String, label: String, property: String, predicate: Predicate, queryExpression: QueryExpression[Expression]) {
    val labelPredicate = HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label))

    val q = PartiallySolvedQuery().copy(
      start = Seq(Unsolved(SchemaIndex(identifier, label, property, AnyIndex, None))),
      where = Seq(Unsolved(predicate), Unsolved(labelPredicate))
    )

    //WHEN
    val plan = assertAccepts(q)

    plan.query.start should equal(
      Seq(Unsolved(SchemaIndex(identifier, label, property, AnyIndex, Some(queryExpression)))))
    plan.query.where.toSet should equal(Set(Solved(predicate), Solved(labelPredicate)))
  }
}
