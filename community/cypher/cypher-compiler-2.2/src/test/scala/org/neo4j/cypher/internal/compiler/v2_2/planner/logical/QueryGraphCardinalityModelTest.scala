/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.LogicalPlanningTestSupport

class QueryGraphCardinalityModelTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("all nodes is gotten from stats") {
    givenPattern("MATCH (n)").
      withAllNodes(425).
      shouldHaveCardinality(425)
  }

  test("all nodes of given label") {
    givenPattern("MATCH (n:A)").
      withAllNodes(425).
      withLabel('A -> 42).
      shouldHaveCardinality(42)
  }

  test("cross product of all nodes") {
    givenPattern("MATCH a, b").
      withAllNodes(425).
      shouldHaveCardinality(425 * 425)
  }

  test("empty pattern yields single result") {
    givenPattern("").
      shouldHaveCardinality(1)
  }

  test("cross product of all nodes and a label scan") {
    givenPattern("MATCH a, (b:B)").
      withAllNodes(40).
      withLabel('B -> 30).
      shouldHaveCardinality(40 * 30)
  }

  test("node cardinality given multiple labels") {
    givenPattern("MATCH (a:A:B)").
      withAllNodes(40).
      withLabel('A -> 20).
      withLabel('B -> 30).
      shouldHaveCardinality(Math.min(20, 30))
  }

  test("node cardinality given multiple labels 2") {
    givenPattern("MATCH (a:A:B)").
      withAllNodes(40).
      withLabel('A -> 30).
      withLabel('B -> 20).
      shouldHaveCardinality(Math.min(20, 30))
  }

  test("node cardinality when label is missing from store") {
    givenPattern("MATCH (a:A)").
      withAllNodes(40).
      shouldHaveCardinality(0)
  }

  test("node cardinality when label is missing from store 2") {
    givenPattern("MATCH (a:A:B)").
      withAllNodes(40).
      withLabel('B -> 30).
      shouldHaveCardinality(0)
  }

  private def givenPattern(pattern: String) = CardinalityTestHelper(pattern)
}
