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
package org.neo4j.cypher.internal.compiler.v2_0

import commands.{SingleOptionalNode, SingleNode, RelatedTo}
import executionplan.builders.PatternGraphBuilder
import symbols._
import org.neo4j.cypher.ExecutionEngineHelper
import org.neo4j.graphdb.Direction
import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.internal.compiler.v2_0.pipes.matching.PatternMatchingBuilder

class ScalaPatternMatchingTest extends ExecutionEngineHelper with PatternGraphBuilder {
  val symbols = new SymbolTable(Map("a" -> NodeType()))
  val patternRelationship: RelatedTo = RelatedTo("a", "b", "r", Seq.empty, Direction.OUTGOING)
  val rightNode = patternRelationship.right

  def queryState = QueryStateHelper.queryStateFrom(graph)

  @Test def should_handle_a_single_relationship_with_no_matches() {

    // Given
    val patternGraph = buildPatternGraph(symbols, Seq(patternRelationship))
    val matcher = new PatternMatchingBuilder(patternGraph, Seq.empty, Set("a", "r", "b"))
    val aNode = createNode()

    // When
    val result = matcher.getMatches(ExecutionContext.empty.newWith("a" -> aNode), queryState).toList

    // Then
    assert(result === List.empty)
  }

  @Test def should_handle_a_single_relationship_with_1_match() {
    // Given
    val patternGraph = buildPatternGraph(symbols, Seq(patternRelationship))
    val matcher = new PatternMatchingBuilder(patternGraph, Seq.empty, Set("a", "r", "b"))
    val aNode = createNode()
    val bNode = createNode()
    val relationship = relate(aNode, bNode)

    // When
    val result = matcher.getMatches(ExecutionContext.empty.newWith("a" -> aNode), queryState).toList

    // Then
    assert(result === List(Map("a" -> aNode, "b" -> bNode, "r" -> relationship)))
  }

  @Test def should_handle_a_single_optional_relationship_with_no_match() {
    // Given
    val patternGraph = buildPatternGraph(symbols, Seq(patternRelationship.copy(optional = true)))
    val matcher = new PatternMatchingBuilder(patternGraph, Seq.empty, Set("a", "r", "b"))
    val aNode = createNode()

    // When
    val result = matcher.getMatches(ExecutionContext.empty.newWith("a" -> aNode), queryState).toList

    // Then
    assert(result === List(Map("a" -> aNode, "b" -> null, "r" -> null)))
  }

  @Test def should_only_return_matches_that_fulfill_the_uniqueness_constraint() {
    // Given MATCH (a)--(b)--(c)

    val r1 = RelatedTo("a", "b", "r1", Seq.empty, Direction.BOTH)
    val r2 = RelatedTo("b", "c", "r2", Seq.empty, Direction.BOTH)

    val patternGraph = buildPatternGraph(symbols, Seq(r1, r2))
    val matcher = new PatternMatchingBuilder(patternGraph, Seq(), Set("a", "r1", "b", "r2", "c"))
    val n0 = createNode()
    val n1 = createNode()
    relate(n0, n1)

    // When
    val result = matcher.getMatches(ExecutionContext.empty.newWith("a" -> n0), queryState).toList

    // Then
    assert(result === List.empty)
  }

  @Test def should_exclude_matches_overlapping_previously_found_relationships() {
    // Given MATCH (a)--(b)--(c)
    // This matcher is responsible for (b)--(c), and should exclude matches from the previous step
    val r2 = RelatedTo("b", "c", "r2", Seq.empty, Direction.BOTH)

    val symbolTable = symbols.add("b", NodeType()).add("r1", RelationshipType())
    val patternGraph = buildPatternGraph(symbolTable, Seq(r2))
    val matcher = new PatternMatchingBuilder(patternGraph, Seq(), Set("a", "r1", "b", "r2", "c"))

    val n0 = createNode()
    val n1 = createNode()
    val rel = relate(n0, n1)

    val startingState = ExecutionContext.empty.newWith(Map("a" -> n0, "b" -> n1, "r1" -> rel))

    // When
    val result = matcher.getMatches(startingState, queryState).toList

    // Then
    assert(result === List.empty)
  }

  @Test def should_not_exclude_matches_with_relationships_in_scope_but_outside_clause() {
    // Given MATCH (b)-[r2]-(c), in scope is (a), [r1]
    val r2 = RelatedTo("b", "c", "r2", Seq.empty, Direction.BOTH)

    val symbolTable = symbols.add("b", NodeType()).add("r1", RelationshipType())
    val patternGraph = buildPatternGraph(symbolTable, Seq(r2))
    val matcher = new PatternMatchingBuilder(patternGraph, Seq(), Set("b", "r2", "c"))

    val n0 = createNode()
    val n1 = createNode()
    val rel = relate(n0, n1)

    val startingState = ExecutionContext.empty.newWith(Map("a" -> n0, "b" -> n1, "r1" -> rel))

    // When
    val result = matcher.getMatches(startingState, queryState).toList

    // Then
    assert(List(Map("a" -> n0, "b" -> n1, "c" -> n0, "r1" -> rel, "r2" -> rel)) ===  result)
  }

  @Test def shouldReturnTwoSubgraphsWithBoundUndirectedRelationshipAndOptionalRelationship() {
    // Given
    // MATCH a-[pr1]-b-[pr2?]->c
    // n1-[r0]->n2, n2-[r1]->n3
    val n1 = createNode("a")
    val n2 = createNode("b")
    val n3 = createNode("c")
    val r0 = relate(n1, n2)
    val r1 = relate(n2, n3)

    val pr1 = RelatedTo("a", "b", "pr1", Seq.empty, Direction.BOTH)
    val pr2 = RelatedTo(SingleNode("b"), SingleOptionalNode("c"), "pr2", Seq.empty, Direction.OUTGOING, optional = true)

    val startingState = ExecutionContext.empty.newWith(Map("pr1" -> r0))
    val symbolTable = symbols.add("pr1", RelationshipType())
    val patternGraph = buildPatternGraph(symbolTable, Seq(pr1, pr2))
    val matcher = new PatternMatchingBuilder(patternGraph, Seq.empty, Set("a", "pr1", "b", "pr2", "c"))

    val result = matcher.getMatches(startingState, queryState).toList

    assertEquals(Set(Map("a" -> n1, "b" -> n2, "c" -> n3, "pr1" -> r0, "pr2" -> r1),
      Map("a" -> n2, "b" -> n1, "c" -> null, "pr1" -> r0, "pr2" -> null)), result.toSet)
  }
}
