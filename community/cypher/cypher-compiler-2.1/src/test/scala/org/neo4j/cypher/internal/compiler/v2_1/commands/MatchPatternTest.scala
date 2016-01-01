/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.commands

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.{MatchPattern, MatchRelationship}

class MatchPatternTest extends CypherFunSuite {

  test("should_find_disjoint_graph_with_single_nodes") {
    // Given
    val pattern = new MatchPattern(Seq("A", "B"), Seq())

    // When and then
    pattern.disconnectedPatterns should equal(Seq(
      new MatchPattern(Seq("A"), Seq()),
      new MatchPattern(Seq("B"), Seq()))
    )
  }

  test("should_be_non_empty_if_it_contains_a_node") {
    // When and then
    new MatchPattern(Seq("A"), Seq()).nonEmpty should equal(true)
  }

  test("should_find_single_graph_for_simple_rel") {
    // Given
    val pattern = new MatchPattern(Seq("A", "B"), Seq(MatchRelationship(None, "A", "B")))

    // When and then
    pattern.disconnectedPatterns should equal(Seq(pattern))
  }

  test("should_find_deeply_nested_disjoint_graphs") {
    // Given
    val pattern = new MatchPattern(Seq("A", "B", "C", "D"),
        Seq(MatchRelationship(None, "A", "B"), MatchRelationship(None, "B", "D")))

    // When and then
    pattern.disconnectedPatterns should equal(Seq(
      new MatchPattern(Seq("A", "B", "D"), Seq(MatchRelationship(None, "A", "B"), MatchRelationship(None, "B", "D"))),
      new MatchPattern(Seq("C"), Seq()))
    )
  }

  test("should_list_subgraphs_without_specified_points") {
    // Given
    val pattern = new MatchPattern(Seq("A", "B", "C"), Seq(MatchRelationship(None, "A", "B")))

    // When and then
    pattern.disconnectedPatternsWithout(Seq("B")) should equal(Seq(
      new MatchPattern(Seq("C"), Seq()))
    )
  }
}
