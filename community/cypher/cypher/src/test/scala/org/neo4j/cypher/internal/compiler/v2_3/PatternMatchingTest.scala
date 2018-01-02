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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.compiler.v2_3.commands.RelatedTo
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Identifier
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.HasLabel
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.UnresolvedLabel
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders.PatternGraphBuilder
import org.neo4j.cypher.internal.compiler.v2_3.pipes.matching.PatternMatchingBuilder
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

class PatternMatchingTest extends ExecutionEngineFunSuite with PatternGraphBuilder with QueryStateTestSupport {
  val symbols = new SymbolTable(Map("a" -> CTNode))
  val patternRelationship: RelatedTo = RelatedTo("a", "b", "r", Seq.empty, SemanticDirection.OUTGOING)
  val rightNode = patternRelationship.right
  val label = UnresolvedLabel("Person")

  test("should_handle_a_single_relationship_with_no_matches") {
    // Given
    val patternGraph = buildPatternGraph(symbols, Seq(patternRelationship))
    val matcher = new PatternMatchingBuilder(patternGraph, Seq.empty, Set("a", "r", "b"))
    val aNode = createNode()

    // When
    val result = withQueryState { queryState =>
      matcher.getMatches(ExecutionContext.empty.newWith("a" -> aNode), queryState).toList
    }

    // Then
    result shouldBe empty
  }

  test("should_handle_a_single_relationship_with_1_match") {
    // Given
    val patternGraph = buildPatternGraph(symbols, Seq(patternRelationship))
    val matcher = new PatternMatchingBuilder(patternGraph, Seq.empty, Set("a", "r", "b"))
    val aNode = createNode()
    val bNode = createNode()
    val relationship = relate(aNode, bNode)

    // When
    val result = withQueryState { queryState =>
      matcher.getMatches(ExecutionContext.empty.newWith("a" -> aNode), queryState).toList
    }

    // Then
    result should equal(List(Map("a" -> aNode, "b" -> bNode, "r" -> relationship)))
  }

  test("should_handle_a_mandatory_labeled_node_with_no_matches") {
    // Given
    val patternGraph = buildPatternGraph(symbols, Seq(patternRelationship))
    val matcher = new PatternMatchingBuilder(patternGraph, Seq(HasLabel(Identifier("b"), label)), Set("a", "r", "b"))
    val aNode = createNode()
    val bNode = createNode()
    relate(aNode, bNode)

    // When
    val result = withQueryState { queryState =>
      matcher.getMatches(ExecutionContext.empty.newWith("a" -> aNode), queryState).toList
    }

    // Then
    result shouldBe empty
  }

  test("should_handle_a_mandatory_labeled_node_with_matches") {
    // Given
    val patternGraph = buildPatternGraph(symbols, Seq(patternRelationship))
    val matcher = new PatternMatchingBuilder(patternGraph, Seq(HasLabel(Identifier("b"), label)), Set("a", "r", "b"))
    val aNode = createNode()
    val bNode = createLabeledNode("Person")
    val relationship = relate(aNode, bNode)

    // When
    val result = withQueryState { queryState =>
      matcher.getMatches(ExecutionContext.empty.newWith("a" -> aNode), queryState).toList
    }

    // Then
    result should equal(List(Map("a" -> aNode, "b" -> bNode, "r" -> relationship)))
  }
}
