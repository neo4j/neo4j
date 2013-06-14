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
import org.junit.Assert.assertEquals
import org.mockito.Mockito._
import org.neo4j.cypher.internal.commands._
import org.neo4j.cypher.internal.commands.expressions._
import org.neo4j.cypher.internal.commands.values.{KeyToken, TokenType}
import org.neo4j.cypher.internal.executionplan.PartiallySolvedQuery
import org.neo4j.cypher.internal.mutation.{UpdateAction, MergeNodeAction}
import org.neo4j.cypher.internal.parser.v2_0.DefaultFalse
import org.neo4j.cypher.internal.pipes.FakePipe
import org.neo4j.cypher.internal.spi.PlanContext
import org.neo4j.cypher.internal.symbols.NodeType
import org.neo4j.graphdb.Direction
import org.neo4j.kernel.impl.api.index.IndexDescriptor
import org.scalatest.mock.MockitoSugar
import org.mockito.Matchers
import org.neo4j.kernel.api.constraints.UniquenessConstraint


class StartPointChoosingBuilderTest extends BuilderTest with MockitoSugar {
  def builder = new StartPointChoosingBuilder

  override val context = mock[PlanContext]
  val identifier = "n"
  val otherIdentifier = "p"
  val label = "Person"
  val property = "prop"
  val otherProperty = "prop2"
  val expression = Literal(42)

  @Test
  def should_create_multiple_start_points_for_disjoint_graphs() {
    // Given
    val query = q(
      patterns = Seq(SingleNode(identifier), SingleNode(otherIdentifier))
    )

    // When
    val plan = assertAccepts(query)

    // Then
    assert(plan.query.start.toList === Seq(Unsolved(AllNodes(identifier)),
      Unsolved(AllNodes(otherIdentifier))))
  }

  @Test
  def should_not_create_start_points_tail_query() {
    // Given
    val query = q(
      patterns = Seq(SingleNode(identifier)),
      tail = Some(q(
        patterns = Seq(SingleNode(otherIdentifier))
      ))
    )

    // When
    val plan = assertAccepts(query)

    // Then
    assert(plan.query.start.toList === Seq(Unsolved(AllNodes(identifier))))
    assert(plan.query.tail.get.start.toList === Seq())
  }

  @Test
  def should_create_multiple_index_start_points_when_available_for_disjoint_graphs() {
    // Given
    val query = q(
      patterns = Seq(SingleNode(identifier), SingleNode(otherIdentifier)),
      where = Seq(HasLabel(Identifier(identifier), KeyToken.Unresolved("Person", TokenType.Label)),
        Equals(Property(Identifier(identifier), "prop1"), Literal("banana")))
    )

    when(context.getIndexRule("Person", "prop1")).thenReturn(Some(new IndexDescriptor(123,456)))
    when(context.getUniquenessConstraint( Matchers.any(), Matchers.any() )).thenReturn(None)

    // When
    val plan = assertAccepts(query)

    // Then
    assert(plan.query.start.toList === Seq(
      Unsolved(SchemaIndex(identifier, "Person", "prop1", None)),
      Unsolved(AllNodes(otherIdentifier))))
  }

  @Test
  def should_not_accept_queries_with_start_items_on_all_levels() {
    assertRejects(
      q(start = Seq(NodeById("n", 0)))
    )

    assertRejects(
      q(start = Seq(NodeById("n", 0)))
    )
  }

  @Test
  def should_pick_an_index_if_only_one_possible_exists() {
    // Given
    val query = q(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
      Equals(Property(Identifier(identifier), property), expression)
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    when(context.getIndexRule("Person", "prop")).thenReturn(Some(new IndexDescriptor(123,456)))
    when(context.getUniquenessConstraint( Matchers.any(), Matchers.any() )).thenReturn(None)

    // When
    val plan = assertAccepts(query)

    // Then
    assert(plan.query.start.toList === Seq(Unsolved(SchemaIndex(identifier, label, property, None))))
  }

  @Test
  def should_pick_an_uniqueness_constraint_index_if_only_one_possible_exists() {
    // Given
    val query = q(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
      Equals(Property(Identifier(identifier), property), expression)
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    when(context.getIndexRule("Person", "prop")).thenReturn(Some(new IndexDescriptor(123,456)))
    when(context.getUniquenessConstraint( "Person", "prop" )).thenReturn(Some(new UniquenessConstraint(123,456)))

    // When
    val plan = assertAccepts(query)

    // Then
    assert(plan.query.start.toList === Seq(Unsolved(SchemaIndex(identifier, label, property, None))))
  }


  @Test
  def should_pick_an_index_if_only_one_possible_exists_other_side() {
    // Given
    val query = q(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
      Equals(expression, Property(Identifier(identifier), property))
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    when(context.getIndexRule("Person", "prop")).thenReturn(Some(new IndexDescriptor(123,456)))
    when(context.getUniquenessConstraint( Matchers.any(), Matchers.any() )).thenReturn(None)

    // When
    val plan = assertAccepts(query)

    // Then
    assert(plan.query.start.toList === List(Unsolved(SchemaIndex(identifier, label, property, None))))
  }

  @Test
  def should_pick_an_uniqueness_constraint_index_if_only_one_possible_exists_other_side() {
    // Given
    val query = q(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
      Equals(expression, Property(Identifier(identifier), property))
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    when(context.getIndexRule("Person", "prop")).thenReturn(Some(new IndexDescriptor(123,456)))
    when(context.getUniquenessConstraint( "Person", "prop" )).thenReturn(Some(new UniquenessConstraint(123,456)))

    // When
    val plan = assertAccepts(query)

    // Then
    assert(plan.query.start.toList === Seq(Unsolved(SchemaIndex(identifier, label, property, None))))
  }

  @Test
  def should_pick_an_index_if_only_one_possible_nullable_property_exists() {
    // Given
    val query = q(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
      Equals(new Nullable(Property(Identifier(identifier), property)) with DefaultFalse, expression)
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    when(context.getIndexRule("Person", "prop")).thenReturn(Some(new IndexDescriptor(123,456)))
    when(context.getUniquenessConstraint( Matchers.any(), Matchers.any() )).thenReturn(None)

    // When
    val plan = assertAccepts(query)

    // Then
    assert(plan.query.start.toList === Seq(Unsolved(SchemaIndex(identifier, label, property, None))))
  }

  @Test
  def should_pick_an_index_if_only_one_possible_nullable_property_exists_other_side() {
    // Given
    val query = q(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
      Equals(expression, new Nullable(Property(Identifier(identifier), property)) with DefaultFalse)
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    when(context.getIndexRule("Person", "prop")).thenReturn(Some(new IndexDescriptor(123,456)))
    when(context.getUniquenessConstraint( Matchers.any(), Matchers.any() )).thenReturn(None)

    // When
    val plan = assertAccepts(query)

    // Then
    assert(plan.query.start.toList === Seq(Unsolved(SchemaIndex(identifier, label, property, None))))
  }

  @Test
  def should_pick_an_uniqueness_constraint_index_if_only_one_possible_nullable_property_exists() {
    // Given
    val query = q(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
      Equals(new Nullable(Property(Identifier(identifier), property)) with DefaultFalse, expression)
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    when(context.getIndexRule("Person", "prop")).thenReturn(Some(new IndexDescriptor(123,456)))
    when(context.getUniquenessConstraint( "Person", "prop" )).thenReturn(Some(new UniquenessConstraint(123,456)))

    // When
    val plan = assertAccepts(query)

    // Then
    assert(plan.query.start.toList === Seq(Unsolved(SchemaIndex(identifier, label, property, None))))
  }

  @Test
  def should_pick_any_index_available() {
    // Given
    val query = q(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
      Equals(Property(Identifier(identifier), property), expression),
      Equals(Property(Identifier(identifier), otherProperty), expression)
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    when(context.getIndexRule(label, property)).thenReturn(Some(new IndexDescriptor(123,456)))
    when(context.getIndexRule(label, otherProperty)).thenReturn(Some(new IndexDescriptor(2468,3579)))
    when(context.getUniquenessConstraint( Matchers.any(), Matchers.any() )).thenReturn(None)

    // When
    val result = assertAccepts(query).query

    // Then
    assert(result.start.exists(_.token.isInstanceOf[SchemaIndex]))
  }

  @Test
  def should_prefer_uniqueness_constraint_indexes_over_other_indexes() {
    // Given
    val query = q(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
      Equals(Property(Identifier(identifier), property), expression),
      Equals(Property(Identifier(identifier), otherProperty), expression)
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    when(context.getIndexRule(label, property)).thenReturn(Some(new IndexDescriptor(123,456)))
    when(context.getIndexRule(label, otherProperty)).thenReturn(Some(new IndexDescriptor(2468,3579)))
    when(context.getUniquenessConstraint( label, property )).thenReturn(None)
    when(context.getUniquenessConstraint( label, otherProperty )).thenReturn(Some(new UniquenessConstraint(2468,3579)))

    // When
    val result = assertAccepts(query).query

    // Then
    assertEquals(Some(Unsolved(SchemaIndex(identifier, label, otherProperty, None))), result.start.find(_.token.isInstanceOf[SchemaIndex]))
  }

  @Test
  def should_prefer_uniqueness_constraint_indexes_over_other_indexes_other_side() {
    // Given
    val query = q(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
      Equals(Property(Identifier(identifier), property), expression),
      Equals(Property(Identifier(identifier), otherProperty), expression)
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    when(context.getIndexRule(label, property)).thenReturn(Some(new IndexDescriptor(123,456)))
    when(context.getIndexRule(label, otherProperty)).thenReturn(Some(new IndexDescriptor(2468,3579)))
    when(context.getUniquenessConstraint( label, property )).thenReturn(Some(new UniquenessConstraint(123,456)))
    when(context.getUniquenessConstraint( label, otherProperty )).thenReturn(None)

    // When
    val result = assertAccepts(query).query

    // Then
    assertEquals(Some(Unsolved(SchemaIndex(identifier, label, property, None))), result.start.find(_.token.isInstanceOf[SchemaIndex]))
  }

  @Test
  def should_produce_label_start_points_when_no_property_predicate_is_used() {
    // Given MATCH n:Person
    val query = q(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label))
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    // When
    val plan = assertAccepts(query)

    assert(plan.query.start.toList === List(Unsolved(NodeByLabel("n", "Person"))))
  }

  @Test
  def should_identify_start_points_with_id_from_where() {
    // Given MATCH n WHERE id(n) == 0
    val nodeId = 0
    val query = q(where = Seq(
      Equals(IdFunction(Identifier(identifier)), Literal(nodeId))
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    // When
    val plan = assertAccepts(query)

    assert(plan.query.start.toList === List(Unsolved(NodeById(identifier, nodeId))))
  }

  @Test
  def should_produce_label_start_points_when_no_matching_index_exist() {
    // Given
    val query = q(where = Seq(
      HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
      Equals(Property(Identifier(identifier), property), expression)
    ), patterns = Seq(
      SingleNode(identifier)
    ))

    when(context.getIndexRule("Person", "prop")).thenReturn(None)
    when(context.getUniquenessConstraint( Matchers.any(), Matchers.any() )).thenReturn(None)

    // When
    val plan = assertAccepts(query)

    // Then
    assert(plan.query.start.toList === Seq(Unsolved(NodeByLabel("n", "Person"))))
  }

  @Test
  def should_pick_a_global_start_point_if_nothing_else_is_possible() {
    // Given
    val query = PartiallySolvedQuery().copy(
      patterns = Seq(Unsolved(SingleNode(identifier)))
    )
    // When
    val plan = assertAccepts(query)

    // Then
    assert(plan.query.start.toList === Seq(Unsolved(AllNodes(identifier))))
  }

  @Test
  def should_be_able_to_figure_out_shortest_path_patterns() {
    // Given
    val expression1 = Literal(42)
    val expression2 = Literal(666)

    // MATCH p=shortestPath( (a:Person{prop:42}) -[*]-> (b{prop:666}) )
    val query = q(
      where = Seq(
        HasLabel(Identifier(identifier), KeyToken.Unresolved(label, TokenType.Label)),
        Equals(Property(Identifier(identifier), property), expression1),
        Equals(Property(Identifier(otherIdentifier), property), expression2)),

      patterns = Seq(
        ShortestPath("p", identifier, otherIdentifier, Nil, Direction.OUTGOING, None, optional = false, single = true, None))
    )

    when(context.getIndexRule(label, property)).thenReturn(None)
    when(context.getUniquenessConstraint( Matchers.any(), Matchers.any() )).thenReturn(None)

    // When
    val plan = assertAccepts(query)

    // Then
    assert(plan.query.start.toList === Seq(
      Unsolved(NodeByLabel(identifier, label)),
      Unsolved(AllNodes(otherIdentifier))))
  }

  @Test
  def should_find_start_items_for_all_patterns() {
    // Given

    // START a=node(0) MATCH a-[x]->b, c-[x]->d
    val query = q(
      start = Seq(NodeById("a", 0)),
      patterns = Seq(
        RelatedTo("a", "b", "x", Seq.empty, Direction.OUTGOING, optional = false),
        RelatedTo("c", "d", "x", Seq.empty, Direction.OUTGOING, optional = false)
      )
    )

    // When
    val plan = assertAccepts(query)

    // Then
    assert(plan.query.start.contains(Unsolved(AllNodes("c"))))
  }


  @Test
  def should_not_introduce_start_points_if_provided_by_last_pipe() {
    // Given
    val pipe = new FakePipe(Iterator.empty, identifier -> NodeType())
    val query = q(
      patterns = Seq(SingleNode(identifier), SingleNode(otherIdentifier))
    )

    // When
    val plan = assertAccepts(pipe, query)

    // Then
    assert(plan.query.start.toList === Seq(Unsolved(AllNodes(otherIdentifier))))
  }

  @Test
  def should_solved_merge_node_start_points() {
    // Given MERGE (x:Label)
    val pipe = new FakePipe(Iterator.empty, identifier -> NodeType())
    val query = q(
      updates = Seq(MergeNodeAction("x", Seq(HasLabel(Identifier("x"), KeyToken.Unresolved("Label", TokenType.Label))), Seq.empty, Seq.empty, None))
    )
    when(context.getLabelId("Label")).thenReturn(Some(42L))

    // When
    val plan = assertAccepts(pipe, query)

    // Then
    plan.query.updates match {
      case Seq(Unsolved(MergeNodeAction("x", Seq(), Seq(), Seq(), Some(_)))) =>
      case _                                                                 =>
        fail("Expected something else, but got this: " + plan.query.start)
    }
  }

  private def q(start: Seq[StartItem] = Seq(),
                where: Seq[Predicate] = Seq(),
                updates: Seq[UpdateAction] = Seq(),
                patterns: Seq[Pattern] = Seq(),
                returns: Seq[ReturnColumn] = Seq(),
                tail: Option[PartiallySolvedQuery] = None) =
    PartiallySolvedQuery().copy(
      start = start.map(Unsolved(_)),
      where = where.map(Unsolved(_)),
      patterns = patterns.map(Unsolved(_)),
      returns = returns.map(Unsolved(_)),
      updates = updates.map(Unsolved(_)),
      tail = tail
    )
}