/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.commands

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.{MatchRelationship, MatchPattern}

class MatchPatternTest extends Assertions {

  @Test
  def should_find_disjoint_graph_with_single_nodes() {
    // Given
    val pattern = new MatchPattern(Seq("A", "B"), Seq())

    // When and then
    assert( pattern.disconnectedPatterns === Seq(
      new MatchPattern(Seq("A"), Seq()),
      new MatchPattern(Seq("B"), Seq())))

  }

  @Test
  def should_be_non_empty_if_it_contains_a_node() {
    // When and then
    assert( new MatchPattern(Seq("A"), Seq()).nonEmpty === true)
  }

  @Test
  def should_find_single_graph_for_simple_rel() {
    // Given
    val pattern = new MatchPattern(Seq("A", "B"), Seq(MatchRelationship(None, "A", "B")))

    // When and then
    assert( pattern.disconnectedPatterns === Seq(pattern))

  }

  @Test
  def should_find_deeply_nested_disjoint_graphs() {
    // Given
    val pattern = new MatchPattern(Seq("A", "B", "C", "D"),
        Seq(MatchRelationship(None, "A", "B"), MatchRelationship(None, "B", "D")))

    // When and then
    assert( pattern.disconnectedPatterns === Seq(
      new MatchPattern(Seq("A", "B", "D"), Seq(MatchRelationship(None, "A", "B"), MatchRelationship(None, "B", "D"))),
      new MatchPattern(Seq("C"), Seq())))

  }

  @Test
  def should_list_subgraphs_without_specified_points() {
    // Given
    val pattern = new MatchPattern(Seq("A", "B", "C"), Seq(MatchRelationship(None, "A", "B")))

    // When and then
    assert( pattern.disconnectedPatternsWithout(Seq("B")) === Seq(
      new MatchPattern(Seq("C"), Seq())))

  }

}
