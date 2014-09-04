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
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics

class StatisticsBackedSelectivityModelTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("cardinality for label and property equality when index is present") {
    givenPattern("MATCH (a:A) WHERE a.prop = 42").
      withAllNodes(40).
      withLabel('A -> 30).
      withIndexSelectivity(('A, 'prop) -> .5).
      shouldHaveCardinality(30 * .5)
  }

  test("cardinality for label and property NOT-equality when index is present") {
    givenPattern("MATCH (a:A) WHERE NOT a.prop = 42").
      withAllNodes(40).
      withLabel('A -> 30).
      withIndexSelectivity(('A, 'prop) -> .3).
      shouldHaveCardinality(30 * (1 - .3))
  }

  test("cardinality for multiple OR:ed equality predicates on a single index") {
    givenPattern("MATCH (a:A) WHERE a.prop = 42 OR a.prop = 43").
      withAllNodes(40).
      withLabel('A -> 30).
      withIndexSelectivity(('A, 'prop) -> .3).
      shouldHaveCardinality(30 * (1 - (1 - .3) * (1 - .3)))
  }

  test("cardinality for multiple OR:ed equality predicates on two indexes") {
    givenPattern("MATCH (a:A) WHERE a.prop = 42 OR a.bar = 43").
      withAllNodes(40).
      withLabel('A -> 30).
      withIndexSelectivity(('A, 'prop) -> .3).
      withIndexSelectivity(('A, 'bar) -> .4).
      shouldHaveCardinality(30 * (1 - (1 - .3) * (1 - .4)))
  }

  test("cardinality for multiple OR:ed equality predicates where one is backed by index and one is not") {
    givenPattern("MATCH (a:A) WHERE a.prop = 42 OR a.bar = 43").
      withAllNodes(40).
      withLabel('A -> 30).
      withIndexSelectivity(('A, 'prop) -> .3).
      withKnownProperty('bar).
      shouldHaveCardinality(30 * (1 - (1 - .3) * (1 - DEFAULT_PREDICATE_SELECTIVITY)))
  }

  test("cardinality for multiple AND:ed equality predicates on two indexes") {
    givenPattern("MATCH (a:A) WHERE a.prop = 42 AND a.bar = 43").
      withAllNodes(40).
      withLabel('A -> 30).
      withIndexSelectivity(('A, 'prop) -> .3).
      withIndexSelectivity(('A, 'bar) -> .4).
      shouldHaveCardinality(30 * .3 * .4)
  }

  test("cardinality for multiple AND:ed equality predicates where one is backed by index and one is not") {
    givenPattern("MATCH (a:A) WHERE a.prop = 42 AND a.bar = 43").
      withAllNodes(40).
      withLabel('A -> 30).
      withIndexSelectivity(('A, 'prop) -> .3).
      withKnownProperty('bar).
      shouldHaveCardinality(30 * .3 * DEFAULT_PREDICATE_SELECTIVITY)
  }

  test("cardinality for label and property equality when no index is present") {
    givenPattern("MATCH (a:A) WHERE a.prop = 42").
      withAllNodes(40).
      withLabel('A -> 30).
      withKnownProperty('prop).
      shouldHaveCardinality(30 * DEFAULT_PREDICATE_SELECTIVITY)
  }

  val DEFAULT_PREDICATE_SELECTIVITY = GraphStatistics.DEFAULT_PREDICATE_SELECTIVITY.coefficient

  private def givenPattern(pattern: String): CardinalityTestHelper = CardinalityTestHelper(pattern)
}
