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
package org.neo4j.cypher.internal.compiler.v2_0.pipes.matching

import org.neo4j.cypher.internal.compiler.v2_0._
import commands.{HasLabel, RelatedTo}
import commands.expressions.Identifier
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.UnresolvedLabel
import executionplan.builders.PatternGraphBuilder
import pipes.QueryStateHelper
import symbols.{NodeType, SymbolTable}
import org.neo4j.cypher.ExecutionEngineHelper
import org.neo4j.graphdb.Direction
import org.junit.Test

class PatternMatchingTest extends ExecutionEngineHelper with PatternGraphBuilder {
  val symbols = new SymbolTable(Map("a" -> NodeType()))
  val patternRelationship: RelatedTo = RelatedTo("a", "b", "r", Seq.empty, Direction.OUTGOING)
  val rightNode = patternRelationship.right

  val label = UnresolvedLabel("Person")


  @Test def should_handle_a_single_relationship_with_no_matches() {

    // Given
    val patternGraph = buildPatternGraph(symbols, Seq(patternRelationship))
    val matcher = new PatternMatchingBuilder(patternGraph, Seq.empty, Set("a", "r", "b"))
    val aNode = createNode()

    // When
    val result = matcher.getMatches(ExecutionContext.empty.newWith("a" -> aNode), QueryStateHelper.queryStateFrom(graph)).toList

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
    val result = matcher.getMatches(ExecutionContext.empty.newWith("a" -> aNode), QueryStateHelper.queryStateFrom(graph)).toList

    // Then
    assert(result === List(Map("a" -> aNode, "b" -> bNode, "r" -> relationship)))
  }

  @Test def should_handle_a_single_optional_relationship_with_no_match() {
    // Given
    val patternGraph = buildPatternGraph(symbols, Seq(patternRelationship.copy(optional = true)))
    val matcher = new PatternMatchingBuilder(patternGraph, Seq.empty, Set("a", "r", "b"))
    val aNode = createNode()

    // When
    val result = matcher.getMatches(ExecutionContext.empty.newWith("a" -> aNode), QueryStateHelper.queryStateFrom(graph)).toList

    // Then
    assert(result === List(Map("a" -> aNode, "b" -> null, "r" -> null)))
  }

  @Test def should_handle_a_mandatory_labeled_node_with_no_matches() {
    // Given
    val patternGraph = buildPatternGraph(symbols, Seq(patternRelationship))
    val matcher = new PatternMatchingBuilder(patternGraph, Seq(HasLabel(Identifier("b"), label)), Set("a", "r", "b"))
    val aNode = createNode()
    val bNode = createNode()
    relate(aNode, bNode)

    // When
    val result = matcher.getMatches(ExecutionContext.empty.newWith("a" -> aNode), QueryStateHelper.queryStateFrom(graph)).toList

    // Then
    assert(result === List())
  }

  @Test def should_handle_a_mandatory_labeled_node_with_matches() {
    // Given
    val patternGraph = buildPatternGraph(symbols, Seq(patternRelationship))
    val matcher = new PatternMatchingBuilder(patternGraph, Seq(HasLabel(Identifier("b"), label)), Set("a", "r", "b"))
    val aNode = createNode()
    val bNode = createLabeledNode("Person")
    val relationship = relate(aNode, bNode)

    // When
    val result = matcher.getMatches(ExecutionContext.empty.newWith("a" -> aNode), QueryStateHelper.queryStateFrom(graph)).toList

    // Then
    assert(result === List(Map("a" -> aNode, "b" -> bNode, "r" -> relationship)))
  }

  @Test def should_handle_a_optional_labeled_node_with_matches() {
    // Given
    val patternGraph = buildPatternGraph(symbols, Seq(patternRelationship.copy(optional = true, right = rightNode.copy(optional = true))))
    val matcher = new PatternMatchingBuilder(patternGraph, Seq(HasLabel(Identifier("b"), label)), Set("a", "r", "b"))
    val aNode = createNode()
    val bNode = createLabeledNode("Person")
    val relationship = relate(aNode, bNode)

    // When
    val result = matcher.getMatches(ExecutionContext.empty.newWith("a" -> aNode), QueryStateHelper.queryStateFrom(graph)).toList

    // Then
    assert(result === List(Map("a" -> aNode, "b" -> bNode, "r" -> relationship)))
  }

  @Test def should_handle_an_optional_relationship_to_an_optional_labeled_node_with_no_matches() {
    // Given
    val patternGraph = buildPatternGraph(symbols, Seq(patternRelationship.copy(optional = true, right = rightNode.copy(optional = true, labels = Seq(label)))))
    val matcher = new PatternMatchingBuilder(patternGraph, Seq.empty, Set("a", "r", "b"))
    val aNode = createNode()
    val bNode = createNode()
    relate(aNode, bNode)

    // When
    val result = matcher.getMatches(ExecutionContext.empty.newWith("a" -> aNode), QueryStateHelper.queryStateFrom(graph)).toList

    // Then
    assert(result === List(Map("a" -> aNode, "b" -> null, "r" -> null)))
  }
}
