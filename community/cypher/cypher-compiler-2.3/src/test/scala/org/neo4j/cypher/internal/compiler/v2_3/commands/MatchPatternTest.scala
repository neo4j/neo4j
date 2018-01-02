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
package org.neo4j.cypher.internal.compiler.v2_3.commands

import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{MatchPattern, MatchRelationship}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class MatchPatternTest extends CypherFunSuite {

  test("should find disjoint graph with single nodes") {
    // Given
    val pattern = new MatchPattern(Seq("A", "B"), Seq())

    // When and then
    pattern.disconnectedPatterns should equal(Seq(
      new MatchPattern(Seq("A"), Seq()),
      new MatchPattern(Seq("B"), Seq()))
    )
  }

  test("should be non empty if it contains a node") {
    // When and then
    new MatchPattern(Seq("A"), Seq()).nonEmpty should equal(true)
  }

  test("should find single graph for simple rel") {
    // Given
    val pattern = new MatchPattern(Seq("A", "B"), Seq(MatchRelationship(None, "A", "B")))

    // When and then
    pattern.disconnectedPatterns should equal(Seq(pattern))
  }

  test("should find deeply nested disjoint graphs") {
    // Given
    val pattern = new MatchPattern(Seq("A", "B", "C", "D"),
        Seq(MatchRelationship(None, "A", "B"), MatchRelationship(None, "B", "D")))

    // When and then
    pattern.disconnectedPatterns should equal(Seq(
      new MatchPattern(Seq("A", "B", "D"), Seq(MatchRelationship(None, "A", "B"), MatchRelationship(None, "B", "D"))),
      new MatchPattern(Seq("C"), Seq()))
    )
  }

  test("should list subgraphs without specified points") {
    // Given
    val pattern = new MatchPattern(Seq("A", "B", "C"), Seq(MatchRelationship(None, "A", "B")))

    // When and then
    pattern.disconnectedPatternsWithout(Seq("B")) should equal(Seq(
      new MatchPattern(Seq("C"), Seq()))
    )
  }

  test("should not consider patterns bound by relationships as unbounded") {
    // Given
    val pattern = new MatchPattern(Seq("A", "B"), Seq(MatchRelationship(Some("r"), "A", "B")))

    // When and then
    pattern.disconnectedPatternsWithout(Seq("r")) should equal(Seq.empty)
  }
}
