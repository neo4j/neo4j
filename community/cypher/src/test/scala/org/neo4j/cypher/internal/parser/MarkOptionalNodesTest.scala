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
package org.neo4j.cypher.internal.parser

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.cypher.internal.commands._
import org.neo4j.cypher.internal.commands.expressions.Identifier
import org.neo4j.cypher.internal.commands.ReturnItem
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.mutation.CreateNode

class MarkOptionalNodesTest extends Assertions {
  val optRelation: RelatedTo = RelatedTo.optional("a", "b", "r", Seq.empty, Direction.OUTGOING)
  val optVarLengthRel = VarLengthRelatedTo("p", "a", "b", None, None, "FOO", Direction.OUTGOING, None, optional = true)

  @Test def should_pass_through_queries_with_no_optional_parts() {
    // Given
    // START a=node(1) MATCH a-[r]->b RETURN a
    val q = Query.
      start(NodeById("a", 1)).
      matches(RelatedTo("a", "b", "r", Seq.empty, Direction.OUTGOING)).
      returns(ReturnItem(Identifier("a"), "a"))

    val result: AbstractQuery = MarkOptionalNodes(q)

    assert(result === q)
  }

  @Test def should_pass_through_queries_with_no_start_items() {
    // Given
    // MATCH a-[r]->b RETURN a
    val q = Query.
      matches(RelatedTo("a", "b", "r", Seq.empty, Direction.OUTGOING)).
      returns(ReturnItem(Identifier("a"), "a"))

    val result: AbstractQuery = MarkOptionalNodes(q)

    assert(result === q)
  }

  @Test def should_find_single_optional_node() {
    // Given
    // START a=node(1) MATCH a-[r?]->b RETURN a
    val q = Query.
      start(NodeById("a", 1)).
      matches(optRelation).
      returns(ReturnItem(Identifier("a"), "a"))

    // When
    val result: AbstractQuery = MarkOptionalNodes(q)

    // Then
    val newRelationship = optRelation.copy(right = optRelation.right.copy(optional = true))
    val expectedQuery = q.copy(matching = Seq(newRelationship))

    assert(result === expectedQuery)
  }

  @Test def should_find_another_single_optional_node() {
    // Given
    // START b=node(1) MATCH a-[r?]->b RETURN a
    val q = Query.
      start(NodeById("b", 1)).
      matches(optRelation).
      returns(ReturnItem(Identifier("a"), "a"))

    // When
    val result: AbstractQuery = MarkOptionalNodes(q)

    // Then
    val newRelationship = optRelation.copy(left = optRelation.left.copy(optional = true))
    val expectedQuery = q.copy(matching = Seq(newRelationship))

    assert(result === expectedQuery)
  }

  @Test def should_figure_out_patterns_bound_on_both_sides() {
    // Given
    // START a = node(0), b=node(1) MATCH a-[r?]->b RETURN a
    val q = Query.
      start(NodeById("b", 1), NodeById("a", 0)).
      matches(optRelation).
      returns(ReturnItem(Identifier("a"), "a"))

    // When
    val result: AbstractQuery = MarkOptionalNodes(q)

    // Then
    assert(result === q)
  }

  @Test def should_handle_longer_patterns() {
    // Given
    // START a = node(0) MATCH a-[r?]->b-[r2]->c RETURN a
    val secondRel: RelatedTo = RelatedTo("b", "c", "r2", Seq.empty, Direction.OUTGOING)
    val q = Query.
      start(NodeById("a", 0)).
      matches(optRelation, secondRel).
      returns(ReturnItem(Identifier("a"), "a"))

    // When
    val result: AbstractQuery = MarkOptionalNodes(q)

    // Then
    val newFirstRelationship = optRelation.copy(right = optRelation.right.copy(optional = true))
    val newSecondRelationship = secondRel.copy(
      left = secondRel.left.copy(optional = true),
      right = secondRel.right.copy(optional = true),
      optional = true
    )
    val expectedQuery = q.copy(matching = Seq(newFirstRelationship, newSecondRelationship))

    assert(result === expectedQuery)
  }

  @Test def should_handle_varlength_patterns() {
    // Given
    // START a = node(0) MATCH p = a-[*?]->b RETURN a
    val q = Query.
      start(NodeById("a", 0)).
      matches(optVarLengthRel).
      returns(ReturnItem(Identifier("a"), "a"))

    // When
    val result: AbstractQuery = MarkOptionalNodes(q)

    // Then
    val newFirstRelationship = optVarLengthRel.copy(right = optVarLengthRel.right.copy(optional = true))
    val expectedQuery = q.copy(matching = Seq(newFirstRelationship))

    assert(result === expectedQuery)
  }

  @Test def should_handle_varlength_patterns_the_other_side() {
    // Given
    // START b = node(0) MATCH p = a-[*?]->b RETURN a
    val q = Query.
      start(NodeById("b", 0)).
      matches(optVarLengthRel).
      returns(ReturnItem(Identifier("a"), "a"))

    // When
    val result: AbstractQuery = MarkOptionalNodes(q)

    // Then
    val newFirstRelationship = optVarLengthRel.copy(left = optVarLengthRel.left.copy(optional = true))
    val expectedQuery = q.copy(matching = Seq(newFirstRelationship))

    assert(result === expectedQuery)
  }

  @Test def should_handle_longer_patterns_with_varlength_relationships() {
    // Given
    // START a = node(0) MATCH p=a-[r?]->b-[:KNOWS*]->c RETURN a
    val secondRel = VarLengthRelatedTo("p", "b", "c", None, None, "KNOWS", Direction.OUTGOING)

    val q = Query.
      start(NodeById("a", 0)).
      matches(optRelation, secondRel).
      returns(ReturnItem(Identifier("a"), "a"))

    // When
    val result: AbstractQuery = MarkOptionalNodes(q)

    // Then
    val newFirstRelationship = optRelation.copy(right = optRelation.right.copy(optional = true))
    val newSecondRelationship = secondRel.copy(
      left = secondRel.left.copy(optional = true),
      right = secondRel.right.copy(optional = true),
      optional = true
    )
    val expectedQuery = q.copy(matching = Seq(newFirstRelationship, newSecondRelationship))

    assert(result === expectedQuery)
  }

  @Test def should_handle_longer_patterns_with_two_bound_ends() {
    // Given
    // START a = node(0), c = node(1) MATCH a-[r?]->b-[r2]->c RETURN a
    val secondRel: RelatedTo = RelatedTo("b", "c", "r2", Seq.empty, Direction.OUTGOING)
    val q = Query.
      start(NodeById("a", 0), NodeById("c", 1)).
      matches(optRelation, secondRel).
      returns(ReturnItem(Identifier("a"), "a"))

    // When
    val result: AbstractQuery = MarkOptionalNodes(q)

    // Then
    assert(result === q)
  }

  @Test def should_handle_longer_patterns_with_bound_relationships() {
    // Given
    // START r = relationship(1) MATCH a-[r]->b-[r2?]->c RETURN a
    val firstRel = optRelation.copy(optional = false)
    val secondRel: RelatedTo = RelatedTo.optional("b", "c", "r2", Seq.empty, Direction.OUTGOING)
    val q = Query.
      start(RelationshipById("r", 1)).
      matches(firstRel, secondRel).
      returns(ReturnItem(Identifier("a"), "a"))

    // When
    val result: AbstractQuery = MarkOptionalNodes(q)

    // Then
    val newSecondRel = secondRel.copy(right = secondRel.right.copy(optional = true))
    assert(result === q.copy(matching = Seq(firstRel, newSecondRel)))
  }

  @Test def should_handle_queries_with_multiple_query_parts() {
    val secondRel: RelatedTo = RelatedTo.optional("a", "b", "r2", Seq.empty, Direction.OUTGOING)
    // Given
    // START a = node(0) MATCH a-->b WITH b MATCH a-[?]->b RETURN b
    val tailQ = Query.
      matches(secondRel).
      returns(ReturnItem(Identifier("b"), "b"))

    val q = Query.
      start(NodeById("a", 0)).
      matches(RelatedTo("a", "b", "r", Seq.empty, Direction.OUTGOING)).
      tail(tailQ).
      returns(ReturnItem(Identifier("b"), "b"))

    // When
    val result: AbstractQuery = MarkOptionalNodes(q)

    // Then
    val updatedSecondRel = secondRel.copy(left = secondRel.left.copy(optional = true))
    val expectedQ = q.copy(tail = q.tail.map(_.copy(matching = Seq(updatedSecondRel))))
    assert(result === expectedQ)
  }

  @Test def should_handle_queries_with_multiple_query_parts_with_shadowed_identifiers() {
    val firstRel = RelatedTo("a", "b", "r2", Seq.empty, Direction.OUTGOING)
    // Given
    // START a = node(0) MATCH a-[r2]->b WITH b as a MATCH a-[r?]->b RETURN a
    val tailQ = Query.
      matches(optRelation).
      returns(ReturnItem(Identifier("a"), "a"))

    val q = Query.
      start(NodeById("a", 0)).
      matches(firstRel).
      tail(tailQ).
      returns(ReturnItem(Identifier("b"), "b"))

    // When
    val result: AbstractQuery = MarkOptionalNodes(q)

    // Then
    val updatedSecondRel = optRelation.copy(left = optRelation.left.copy(optional = true))
    val expectedQ = q.copy(tail = q.tail.map(_.copy(matching = Seq(updatedSecondRel))))
    assert(result === expectedQ)
  }

  @Test def should_handle_disconnected_varlength_relationships() {
    // Given
    // START a = node(0) MATCH a-[r?]->b-[?*]->c RETURN a
    val secondRel = VarLengthRelatedTo("p", "b", "c", None, None, "KNOWS", Direction.OUTGOING, optional = true)
    val q = Query.
      start(NodeById("a", 0)).
      matches(optRelation, secondRel).
      returns(ReturnItem(Identifier("a"), "a"))

    // When
    val result: AbstractQuery = MarkOptionalNodes(q)

    // Then
    val newFirstRel = optRelation.copy(right = optRelation.right.copy(optional = true))
    val newSecondRel = secondRel.copy(
      right = secondRel.right.copy(optional = true),
      left = secondRel.left.copy(optional = true),
      optional = true)

    assert(result === q.copy(matching = Seq(newFirstRel, newSecondRel)))
  }

  @Test def should_handle_query_parts_returning_all_identifiers() {
    // Given
    // CREATE a MATCH a-[r?]->b RETURN a
    val tailQ = Query.
      matches(optRelation).
      returns(ReturnItem(Identifier("a"), "a"))

    val q = Query.
      start(CreateNodeStartItem(CreateNode("a", Map.empty, Seq.empty))).
      tail(tailQ).
      returns(AllIdentifiers())

    // When
    val result: AbstractQuery = MarkOptionalNodes(q)

    // Then
    val updatedSecondRel = optRelation.copy(right = optRelation.right.copy(optional = true))
    val expectedQ = q.copy(tail = q.tail.map(_.copy(matching = Seq(updatedSecondRel))))
    assert(result === expectedQ)
  }

  @Test def should_find_optional_through_shortest_path() {
    val pattern = ShortestPath("p", SingleNode("a"), SingleNode("b"), Seq.empty, Direction.OUTGOING, None, optional = true, false, None)
    // Given
    // START a MATCH p = shortestPath(a-[r?]->b) RETURN a
    val q = Query.
      start(NodeById("a", 0)).
      matches(pattern).
      returns(ReturnItem(Identifier("a"), "a"))

    // When
    val result: AbstractQuery = MarkOptionalNodes(q)

    // Then
    val newPattern = pattern.copy(right = pattern.right.copy(optional = true))
    val expectedQ = q.copy(matching = Seq(newPattern))
    assert(result === expectedQ)
  }

  @Test def should_pass_through_shortest_path_patterns_without_optional_elements() {
    val pattern = ShortestPath("p", SingleNode("a"), SingleNode("b"), Seq.empty, Direction.OUTGOING, None, false, false, None)
    // Given
    // START a MATCH p = shortestPath(a-[r?]->b) RETURN a
    val q = Query.
      start(NodeById("a", 0)).
      matches(pattern).
      returns(ReturnItem(Identifier("a"), "a"))

    // When
    val result: AbstractQuery = MarkOptionalNodes(q)

    // Then
    assert(result === q)
  }

  @Test def should_find_another_optional_through_shortest_path() {
    val pattern = ShortestPath("p", SingleNode("a"), SingleNode("b"), Seq.empty, Direction.OUTGOING, None, optional = true, false, None)
    // Given
    // START b MATCH p = shortestPath(a-[*r?]->b) RETURN a
    val q = Query.
      start(NodeById("b", 0)).
      matches(pattern).
      returns(ReturnItem(Identifier("a"), "a"))

    // When
    val result: AbstractQuery = MarkOptionalNodes(q)

    // Then
    val newPattern = pattern.copy(left = pattern.left.copy(optional = true))
    val expectedQ = q.copy(matching = Seq(newPattern))
    assert(result === expectedQ)
  }

  @Test def should_handle_longer_patterns_containing_shortest_path() {
    val pattern = ShortestPath("p", SingleNode("b"), SingleNode("c"), Seq.empty, Direction.OUTGOING, None, optional = false, false, None)
    // Given
    // START a MATCH a-[?]->b, p = shortestPath(b-[*]->c) RETURN a
    val q = Query.
      start(NodeById("a", 0)).
      matches(optRelation, pattern).
      returns(ReturnItem(Identifier("a"), "a"))

    // When
    val result: AbstractQuery = MarkOptionalNodes(q)

    // Then
    val newPattern = pattern.copy(
      left = pattern.left.copy(optional = true),
      right = pattern.right.copy(optional = true),
      optional = true)
    val updatedFirstRel = optRelation.copy(right = optRelation.right.copy(optional = true))

    val expectedQ = q.copy(matching = Seq(updatedFirstRel, newPattern))
    assert(result === expectedQ)
  }

  @Test def should_handle_disconnected_shortest_path_relationships() {
    val pattern = ShortestPath("p", SingleNode("b"), SingleNode("c"), Seq.empty, Direction.OUTGOING, None, optional = true, false, None)
    // Given
    // START a MATCH a-[?]->b, p = shortestPath(b-[?*]->c) RETURN a
    val q = Query.
      start(NodeById("a", 0)).
      matches(optRelation, pattern).
      returns(ReturnItem(Identifier("a"), "a"))

    // When
    val result: AbstractQuery = MarkOptionalNodes(q)

    // Then
    val newPattern = pattern.copy(
      left = pattern.left.copy(optional = true),
      right = pattern.right.copy(optional = true),
      optional = true)
    val updatedFirstRel = optRelation.copy(right = optRelation.right.copy(optional = true))

    val expectedQ = q.copy(matching = Seq(updatedFirstRel, newPattern))
    assert(result === expectedQ)
  }

  @Test def should_handle_union() {
    // Given
    // START a=node(1) MATCH a-[r?]->b RETURN a
    // UNION
    // START a=node(*) RETURN a
    val q1 = Query.
      start(NodeById("a", 1)).
      matches(optRelation).
      returns(ReturnItem(Identifier("a"), "a"))

    val q2 = Query.
      start(AllNodes("a")).
      returns(ReturnItem(Identifier("a"), "a"))

    val q = Union(Seq(q1,q2), distinct = true)

    // When
    val result: AbstractQuery = MarkOptionalNodes(q)

    // Then
    val newRelationship = optRelation.copy(right = optRelation.right.copy(optional = true))
    val newQ1 = q1.copy(matching = Seq(newRelationship))
    val expectedQuery = q.copy(queries = Seq(newQ1, q2))
    assert(result === expectedQuery)
  }

  @Test def should_handle_optionals_without_start() {
    // Given
    // MATCH a WITH a MATCH a-[r?]->b RETURN a
    val q1 = Query.
      start(NodeById("a", 1)).
      matches(optRelation).
      returns(ReturnItem(Identifier("a"), "a"))

    val q = Query.
      matches(SingleNode("a")).
      tail(q1).
      returns(ReturnItem(Identifier("a"), "a"))


    // When
    val result: AbstractQuery = MarkOptionalNodes(q)

    // Then
    val newRelationship = optRelation.copy(right = optRelation.right.copy(optional = true))
    val newQ1 = q1.copy(matching = Seq(newRelationship))
    assert(result === q.copy(tail = Some(newQ1)))
  }

  @Test def should_handle_two_disconnected_patterns_without_marking_optionals() {
    // Given
    // MATCH a-->b WITH a,b MATCH c-->d RETURN *

    val secondPart = Query.
      matches(RelatedTo("c", "d", "r2", Seq.empty, Direction.OUTGOING)).
      returns(AllIdentifiers())

    val q = Query.
      matches(RelatedTo("a", "b", "r1", Seq.empty, Direction.OUTGOING)).
      tail(secondPart).
      returns(ReturnItem(Identifier("a"), "a"), ReturnItem(Identifier("b"), "b"))

    // When
    val result: AbstractQuery = MarkOptionalNodes(q)

    // Then
    assert(result === q)
  }
}