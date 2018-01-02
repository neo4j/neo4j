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
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders.PatternGraphBuilder
import org.neo4j.cypher.internal.compiler.v2_3.pipes.matching.SimplePatternMatcherBuilder
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

class SimplePatternMatchingTest extends ExecutionEngineFunSuite with PatternGraphBuilder with QueryStateTestSupport {
  val symbols = new SymbolTable(Map("a" -> CTNode))

  test("should_only_return_matches_that_fulfill_the_uniqueness_constraint") {
    // Given MATCH (a)--(b)--(c)
    val r1 = RelatedTo("a", "b", "r1", Seq.empty, SemanticDirection.BOTH)
    val r2 = RelatedTo("b", "c", "r2", Seq.empty, SemanticDirection.BOTH)

    val patternGraph = buildPatternGraph(symbols, Seq(r1, r2))
    val matcher = new SimplePatternMatcherBuilder(patternGraph, Seq(), symbols, Set("a", "r1", "b", "r2", "c"))
    val n0 = createNode()
    val n1 = createNode()
    relate(n0, n1)

    // When
    val result = withQueryState { queryState =>
      matcher.getMatches(ExecutionContext.empty.newWith("a" -> n0), queryState).toList
    }

    // Then
    result shouldBe empty
  }

  test("should_exclude_matches_overlapping_previously_found_relationships") {
    // Given MATCH (a)-[r1]-(b)-[r2]-(c)
    // This matcher is responsible for (b)-[r2]-(c), and needs to check the result from previous steps
    val r2 = RelatedTo("b", "c", "r2", Seq.empty, SemanticDirection.BOTH)

    val symbolTable = symbols.add("b", CTNode).add("r", CTRelationship)
    val patternGraph = buildPatternGraph(symbolTable, Seq(r2))
    val matcher = new SimplePatternMatcherBuilder(patternGraph, Seq(), symbolTable, Set("a", "r", "b", "r2", "c"))
    val n0 = createNode()
    val n1 = createNode()
    val rel = relate(n0, n1)

    val startingState = ExecutionContext.empty.newWith(Map("a" -> n0, "b" -> n1, "r" -> rel))

    // When
    val result = withQueryState { queryState =>
      matcher.getMatches(startingState, queryState).toList
    }

    // Then
    result shouldBe empty
  }

  test("should_ignore_earlier_matches_overlapping_previously_found_relationships") {
    // Given MATCH (b)-[r2]-(c), with a and r1 already in scope
    // This matcher is responsible for (b)-[r2]-(c), and needs to check the result from previous steps
    val r2 = RelatedTo("b", "c", "r2", Seq.empty, SemanticDirection.BOTH)

    val symbolTable = symbols.add("b", CTNode).add("r", CTRelationship)
    val patternGraph = buildPatternGraph(symbolTable, Seq(r2))
    val matcher = new SimplePatternMatcherBuilder(patternGraph, Seq(), symbolTable, Set("b", "r2", "c"))
    val n0 = createNode()
    val n1 = createNode()
    val rel = relate(n0, n1)

    val startingState = ExecutionContext.empty.newWith(Map("a" -> n0, "b" -> n1, "r" -> rel))

    // When
    val result = withQueryState { queryState =>
      matcher.getMatches(startingState, queryState).toList
    }

    // Then
    result should equal(List(Map("a" -> n0, "b" -> n1, "r" -> rel, "r2" -> rel, "c" -> n0)))
  }
}
