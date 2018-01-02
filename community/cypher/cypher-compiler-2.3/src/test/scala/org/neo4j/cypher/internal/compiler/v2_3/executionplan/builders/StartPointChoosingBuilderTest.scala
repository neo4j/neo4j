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

import org.mockito.Matchers
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions._
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.{Equals, HasLabel}
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.TokenType._
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.{KeyToken, TokenType}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.PartiallySolvedQuery
import org.neo4j.cypher.internal.compiler.v2_3.pipes.FakePipe
import org.neo4j.cypher.internal.compiler.v2_3.spi.SchemaTypes.{IndexDescriptor, UniquenessConstraint}
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v2_3.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.frontend.v2_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticDirection, ast}

class StartPointChoosingBuilderTest extends BuilderTest {
  def builder = new StartPointChoosingBuilder

  context = mock[PlanContext]
  val identifier = "n"
  val otherIdentifier = "p"
  val label = "Person"
  val property = "prop"
  val propertyKey = PropertyKey(property)
  val otherProperty = "prop2"
  val otherPropertyKey = PropertyKey(otherProperty)
  val expression = Literal(42)

  test("should_create_multiple_start_points_for_disjoint_graphs") {
    // Given
    val query = newQuery(
      patterns = Seq(SingleNode(identifier), SingleNode(otherIdentifier))
    )

    // When
    val plan = assertAccepts(query)

    // Then
    plan.query.start.toList should equal(Seq(Unsolved(AllNodes(identifier)),
      Unsolved(AllNodes(otherIdentifier))))
  }

  test("should_not_create_start_points_tail_query") {
    // Given
    val query = newQuery(
      patterns = Seq(SingleNode(identifier)),
      tail = Some(newQuery(
        patterns = Seq(SingleNode(otherIdentifier))
      ))
    )

    // When
    val plan = assertAccepts(query)

    // Then
    plan.query.start.toList should equal(Seq(Unsolved(AllNodes(identifier))))
    plan.query.tail.get.start.toList should equal(Seq())
  }

  test("should_create_multiple_index_start_points_when_available_for_disjoint_graphs") {
    // Given
    val query = newQuery(
      patterns = Seq(SingleNode(identifier), SingleNode(otherIdentifier)),
      where = Seq(HasLabel(Identifier(identifier), KeyToken.Unresolved("Person", TokenType.Label)),
        Equals(Property(Identifier(identifier), PropertyKey("prop1")), Literal("banana")))
    )

    when(context.getIndexRule("Person", "prop1")).thenReturn(Some(IndexDescriptor(123,456)))
    when(context.getUniquenessConstraint( Matchers.any(), Matchers.any() )).thenReturn(None)

    // When
    val plan = assertAccepts(query)

    // Then
    plan.query.start.toList should equal(Seq(
      Unsolved(SchemaIndex(identifier, "Person", "prop1", AnyIndex, None)),
      Unsolved(AllNodes(otherIdentifier))))
  }

  test("should_not_accept_queries_with_start_items_on_all_levels") {
    assertRejects(
      newQuery(start = Seq(NodeById("n", 0)))
    )

    assertRejects(
      newQuery(start = Seq(NodeById("n", 0)))
    )
  }

  test("should_pick_an_index_if_only_one_possible_exists") {
    // Given
    val query = newQuery(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
      Equals(Property(Identifier(identifier), propertyKey), expression)
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    when(context.getIndexRule("Person", "prop")).thenReturn(Some(IndexDescriptor(123,456)))
    when(context.getUniquenessConstraint( Matchers.any(), Matchers.any() )).thenReturn(None)

    // When
    val plan = assertAccepts(query)

    // Then
    plan.query.start.toList should equal(Seq(Unsolved(SchemaIndex(identifier, label, property, AnyIndex, None))))
  }

  test("should_pick_an_uniqueness_constraint_index_if_only_one_possible_exists") {
    // Given
    val query = newQuery(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
      Equals(Property(Identifier(identifier), propertyKey), expression)
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    when(context.getIndexRule("Person", "prop")).thenReturn(Some(IndexDescriptor(123,456)))
    when(context.getUniquenessConstraint( "Person", "prop" )).thenReturn(Some(UniquenessConstraint(123,456)))

    // When
    val plan = assertAccepts(query)

    // Then
    plan.query.start.toList should equal(Seq(Unsolved(SchemaIndex(identifier, label, property, UniqueIndex, None))))
  }

  test("should_pick_an_index_if_only_one_possible_exists_other_side") {
    // Given
    val query = newQuery(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
      Equals(expression, Property(Identifier(identifier), propertyKey))
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    when(context.getIndexRule("Person", "prop")).thenReturn(Some(IndexDescriptor(123,456)))
    when(context.getUniquenessConstraint( Matchers.any(), Matchers.any() )).thenReturn(None)

    // When
    val plan = assertAccepts(query)

    // Then
    plan.query.start.toList should equal(List(Unsolved(SchemaIndex(identifier, label, property, AnyIndex, None))))
  }

  test("should_pick_an_uniqueness_constraint_index_if_only_one_possible_exists_other_side") {
    // Given
    val query = newQuery(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
      Equals(expression, Property(Identifier(identifier), propertyKey))
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    when(context.getIndexRule("Person", "prop")).thenReturn(Some(IndexDescriptor(123,456)))
    when(context.getUniquenessConstraint( "Person", "prop" )).thenReturn(Some(UniquenessConstraint(123,456)))

    // When
    val plan = assertAccepts(query)

    // Then
    plan.query.start.toList should equal(Seq(Unsolved(SchemaIndex(identifier, label, property, UniqueIndex, None))))
  }

  test("should_pick_an_index_if_only_one_possible_nullable_property_exists") {
    // Given
    val query = newQuery(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
      Equals(Property(Identifier(identifier), propertyKey), expression)
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    when(context.getIndexRule("Person", "prop")).thenReturn(Some(IndexDescriptor(123,456)))
    when(context.getUniquenessConstraint( Matchers.any(), Matchers.any() )).thenReturn(None)

    // When
    val plan = assertAccepts(query)

    // Then
    plan.query.start.toList should equal(Seq(Unsolved(SchemaIndex(identifier, label, property, AnyIndex, None))))
  }

  test("should_pick_an_index_if_only_one_possible_nullable_property_exists_other_side") {
    // Given
    val query = newQuery(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
      Equals(expression, Property(Identifier(identifier), propertyKey))
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    when(context.getIndexRule("Person", "prop")).thenReturn(Some(IndexDescriptor(123,456)))
    when(context.getUniquenessConstraint( Matchers.any(), Matchers.any() )).thenReturn(None)

    // When
    val plan = assertAccepts(query)

    // Then
    plan.query.start.toList should equal(Seq(Unsolved(SchemaIndex(identifier, label, property, AnyIndex, None))))
  }

  test("should_pick_an_uniqueness_constraint_index_if_only_one_possible_nullable_property_exists") {
    // Given
    val query = newQuery(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
      Equals(Property(Identifier(identifier), propertyKey), expression)
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    when(context.getIndexRule("Person", "prop")).thenReturn(Some(IndexDescriptor(123,456)))
    when(context.getUniquenessConstraint( "Person", "prop" )).thenReturn(Some(UniquenessConstraint(123,456)))

    // When
    val plan = assertAccepts(query)

    // Then
    plan.query.start.toList should equal(Seq(Unsolved(SchemaIndex(identifier, label, property, UniqueIndex, None))))
  }

  test("should_pick_any_index_available") {
    // Given
    val query = newQuery(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
      Equals(Property(Identifier(identifier), propertyKey), expression),
      Equals(Property(Identifier(identifier), otherPropertyKey), expression)
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    when(context.getIndexRule(label, property)).thenReturn(Some(IndexDescriptor(123,456)))
    when(context.getIndexRule(label, otherProperty)).thenReturn(Some(IndexDescriptor(2468,3579)))
    when(context.getUniquenessConstraint( Matchers.any(), Matchers.any() )).thenReturn(None)

    // When
    val result = assertAccepts(query).query

    // Then
    result.start.exists(_.token.isInstanceOf[SchemaIndex]) should equal(true)
  }

  test("should_pick_any_index_available_for_prefix_search") {
    object inner extends AstConstructionTestSupport {

      def run() = {
        // Given
        val labelPredicate = HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label))
        val startsWith: ast.StartsWith = ast.StartsWith(ast.Property(ident("n"), ast.PropertyKeyName(property)_)_, ast.StringLiteral("prefix")_)_
        val startsWithPredicate = toCommandPredicate(startsWith)

        val query = newQuery(
          where = Seq(labelPredicate, startsWithPredicate),
          patterns = Seq(SingleNode(identifier))
        )

        when(context.getIndexRule(label, property)).thenReturn(Some(IndexDescriptor(123, 456)))
        when(context.getUniquenessConstraint( Matchers.any(), Matchers.any() )).thenReturn(None)

        // When
        val result = assertAccepts(query).query

        // Then
        result.start.exists(_.token.isInstanceOf[SchemaIndex]) should equal(true)
      }
    }

    inner.run()
  }

  test("should pick any index available for range queries") {
    object inner extends AstConstructionTestSupport {

      def run() = {
        // Given
        val labelPredicate = HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label))
        val prop: ast.Property = ast.Property(ident("n"), ast.PropertyKeyName("prop") _) _
        val inequality = ast.AndedPropertyInequalities(ident("n"), prop, NonEmptyList(ast.GreaterThan(prop, ast.SignedDecimalIntegerLiteral("42") _) _))
        val inequalityPredicate = toCommandPredicate(inequality)

        val query = newQuery(
          where = Seq(labelPredicate, inequalityPredicate),
          patterns = Seq(SingleNode(identifier))
        )

        when(context.getIndexRule(label, property)).thenReturn(Some(IndexDescriptor(123, 456)))
        when(context.getUniquenessConstraint( Matchers.any(), Matchers.any() )).thenReturn(None)

        // When
        val result = assertAccepts(query).query

        // Then
        result.start.exists(_.token.isInstanceOf[SchemaIndex]) should equal(true)
      }
    }

    inner.run()
  }

  test("should_prefer_uniqueness_constraint_indexes_over_other_indexes") {
    // Given
    val query = newQuery(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
      Equals(Property(Identifier(identifier), propertyKey), expression),
      Equals(Property(Identifier(identifier), otherPropertyKey), expression)
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    when(context.getIndexRule(label, property)).thenReturn(Some(IndexDescriptor(123,456)))
    when(context.getIndexRule(label, otherProperty)).thenReturn(Some(IndexDescriptor(2468,3579)))
    when(context.getUniquenessConstraint( label, property )).thenReturn(None)
    when(context.getUniquenessConstraint( label, otherProperty )).thenReturn(Some(UniquenessConstraint(2468,3579)))

    // When
    val result = assertAccepts(query).query

    // Then
    result.start.find(_.token.isInstanceOf[SchemaIndex]) should equal(Some(Unsolved(SchemaIndex(identifier, label, otherProperty, UniqueIndex, None))))
  }

  test("should_prefer_uniqueness_constraint_indexes_over_other_indexes_other_side") {
    // Given
    val query = newQuery(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
      Equals(Property(Identifier(identifier), propertyKey), expression),
      Equals(Property(Identifier(identifier), otherPropertyKey), expression)
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    when(context.getIndexRule(label, property)).thenReturn(Some(IndexDescriptor(123,456)))
    when(context.getIndexRule(label, otherProperty)).thenReturn(Some(IndexDescriptor(2468,3579)))
    when(context.getUniquenessConstraint( label, property )).thenReturn(Some(UniquenessConstraint(123,456)))
    when(context.getUniquenessConstraint( label, otherProperty )).thenReturn(None)

    // When
    val result = assertAccepts(query).query

    // Then
    result.start.find(_.token.isInstanceOf[SchemaIndex]) should equal(Some(Unsolved(SchemaIndex(identifier, label, property, UniqueIndex, None))))
  }

  test("should_produce_label_start_points_when_no_property_predicate_is_used") {
    // Given MATCH n:Person
    val query = newQuery(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label))
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    // When
    val plan = assertAccepts(query)

    plan.query.start.toList should equal(List(Unsolved(NodeByLabel("n", "Person"))))
  }

  test("should_identify_start_points_with_id_from_where") {
    // Given MATCH n WHERE id(n) == 0
    val nodeId = 0
    val query = newQuery(where = Seq(
      Equals(IdFunction(Identifier(identifier)), Literal(nodeId))
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    // When
    val plan = assertAccepts(query)

    plan.query.start.toList should equal(List(Unsolved(NodeByIdOrEmpty(identifier, Literal(nodeId)))))
  }

  test("should_identify_start_points_with_id_from_expression_over_bound_identifier") {
    // Given ... WITH n MATCH p WHERE id(p) = n.otherId

    val propertyLookup: Property = Property(Identifier(identifier), PropertyKey("otherId"))
    val equalityPredicate: Equals = Equals(IdFunction(Identifier(otherIdentifier)), propertyLookup)
    val query = newQuery(
      where = Seq(equalityPredicate),
      patterns = Seq(SingleNode(otherIdentifier))
    )

    val pipe = new FakePipe(Seq.empty, identifier -> CTNode)
    val plan = assertAccepts(pipe, query)

    plan.query.start.toList should equal(List(Unsolved(NodeByIdOrEmpty(otherIdentifier, propertyLookup))))
    plan.query.where should equal(Seq(Solved(equalityPredicate)))
  }

  test("should_identify_start_points_with_id_from_collection_expression_over_bound_identifier") {
    // Given ... WITH n MATCH p WHERE id(p) IN n.collection

    val propertyLookup: Property = Property(Identifier(identifier), PropertyKey("collection"))
    val equalityPredicate: Equals = Equals(IdFunction(Identifier(otherIdentifier)), propertyLookup)
    val collectionPredicate: AnyInCollection = AnyInCollection(propertyLookup, "-_-INNER-_-", equalityPredicate)
    val query = newQuery(
      where = Seq(collectionPredicate),
      patterns = Seq(SingleNode(otherIdentifier))
    )

    val pipe = new FakePipe(Seq.empty, identifier -> CTNode)
    val plan = assertAccepts(pipe, query)

    plan.query.start.toList should equal(List(Unsolved(NodeByIdOrEmpty(otherIdentifier, propertyLookup))))
    plan.query.where should equal(Seq(Solved(collectionPredicate)))
  }

  test("should_produce_label_start_points_when_no_matching_index_exist") {
    // Given
    val query = newQuery(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
      Equals(Property(Identifier(identifier), propertyKey), expression)
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    when(context.getIndexRule("Person", "prop")).thenReturn(None)
    when(context.getUniquenessConstraint( Matchers.any(), Matchers.any() )).thenReturn(None)

    // When
    val plan = assertAccepts(query)

    // Then
    plan.query.start.toList should equal(Seq(Unsolved(NodeByLabel("n", "Person"))))
  }

  test("should_pick_a_global_start_point_if_nothing_else_is_possible") {
    // Given
    val query = PartiallySolvedQuery().copy(
      patterns = Seq(Unsolved(SingleNode(identifier)))
    )
    // When
    val plan = assertAccepts(query)

    // Then
    plan.query.start.toList should equal(Seq(Unsolved(AllNodes(identifier))))
  }

  test("should_be_able_to_figure_out_shortest_path_patterns") {
    // Given
    val expression1 = Literal(42)
    val expression2 = Literal(666)

    // MATCH p=shortestPath( (a:Person{prop:42}) -[*]-> (b{prop:666}) )
    val query = newQuery(
      where = Seq(
        HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
        Equals(Property(Identifier(identifier), propertyKey), expression1),
        Equals(Property(Identifier(otherIdentifier), propertyKey), expression2)),

      patterns = Seq(
        ShortestPath("p", SingleNode(identifier), SingleNode(otherIdentifier), Nil, SemanticDirection.OUTGOING, allowZeroLength = false, None, single = true, None))
    )

    when(context.getIndexRule(label, property)).thenReturn(None)
    when(context.getUniquenessConstraint( Matchers.any(), Matchers.any() )).thenReturn(None)

    // When
    val plan = assertAccepts(query)

    // Then
    plan.query.start.toList should equal(Seq(
      Unsolved(NodeByLabel(identifier, label)),
      Unsolved(AllNodes(otherIdentifier))))
  }

  test("should_find_start_items_for_all_patterns") {
    // Given

    // START a=node(0) MATCH a-[x]->b, c-[x]->d
    val query = newQuery(
      start = Seq(NodeById("a", 0)),
      patterns = Seq(
        RelatedTo(SingleNode("a"),SingleNode("b"), "x", Seq.empty, SemanticDirection.OUTGOING, Map.empty),
        RelatedTo(SingleNode("c"), SingleNode("d"), "x", Seq.empty, SemanticDirection.OUTGOING, Map.empty)
      )
    )

    // When
    val plan = assertAccepts(query)

    // Then
    plan.query.start.contains(Unsolved(AllNodes("c"))) should equal(true)
  }

  test("should_not_introduce_start_points_if_provided_by_last_pipe") {
    // Given
    val pipe = new FakePipe(Iterator.empty, identifier -> CTNode)
    val query = newQuery(
      patterns = Seq(SingleNode(identifier), SingleNode(otherIdentifier))
    )

    // When
    val plan = assertAccepts(pipe, query)

    // Then
    plan.query.start.toList should equal(Seq(Unsolved(AllNodes(otherIdentifier))))
  }

  test("should_not_introduce_start_points_if_relationship_is_bound") {
    // Given
    val query = newQuery(
      start = Seq(RelationshipById("r", 123)),
      patterns = Seq(RelatedTo(identifier, otherIdentifier, "r", "KNOWS", SemanticDirection.BOTH))
    )

    // When + Then
    assertRejects(query)
  }
}
