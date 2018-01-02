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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, QueryStatisticsTestSupport}
import org.neo4j.graphdb.Node

import scala.collection.mutable

/*
The tests here are all designed to resemble permutations of the triadic case,
whether or not the current version of Cypher is able to actually use the
TriadicSelection to solve them. The idea being that we should have a relatively
exhaustive test suite that verifies that Cypher always returns the correct
results with or without TriadicSelection, and with both RULE and COST planners,
and this behaviour should not change as we expand support for TriadicSelection
to a wider set of queries. For this reason we do not test for the
TriadicSelection here. For that take a look at TriadicIntegrationTest and
TriadicSelectionFinderTest.
 */
class TriadicSelectionAcceptanceTest extends ExecutionEngineFunSuite
                                     with QueryStatisticsTestSupport with NewPlannerTestSupport {

  private def makeTriadicModel(relTypeAB1: String = "KNOWS",
                               relTypeAB2: String = "FOLLOWS", relTypeBC: String = "FRIEND",
                               aLabel: String = "A", bLabel: String = "X",
                               cLabel1: String = "X", cLabel2: String = "X"): Map[String, Node] = {
    val nodes = mutable.Map[String, Node]()
    def addNamedNode(name: String, label: String) {
      nodes(name) = createLabeledNode(Map("name" -> name), label)
    }
    addNamedNode("a", aLabel)
    addNamedNode("b1", bLabel)
    addNamedNode("b2", bLabel)
    addNamedNode("b3", bLabel)
    addNamedNode("b4", bLabel)
    addNamedNode("c11", cLabel1)
    addNamedNode("c12", cLabel2)
    addNamedNode("c21", cLabel1)
    addNamedNode("c22", cLabel2)
    addNamedNode("c31", cLabel1)
    addNamedNode("c32", cLabel2)
    addNamedNode("c41", cLabel1)
    addNamedNode("c42", cLabel2)

    // create relationships to match (a)-->(b)
    relate(nodes("a"), nodes("b1"), relTypeAB1)
    relate(nodes("a"), nodes("b2"), relTypeAB1)
    relate(nodes("a"), nodes("b3"), relTypeAB2)
    relate(nodes("a"), nodes("b4"), relTypeAB2)

    // create relationships to match (b)-->(c)
    relate(nodes("b1"), nodes("c11"), relTypeBC)
    relate(nodes("b1"), nodes("c12"), relTypeBC)
    relate(nodes("b2"), nodes("c21"), relTypeBC)
    relate(nodes("b2"), nodes("c22"), relTypeBC)
    relate(nodes("b3"), nodes("c31"), relTypeBC)
    relate(nodes("b3"), nodes("c32"), relTypeBC)
    relate(nodes("b4"), nodes("c41"), relTypeBC)
    relate(nodes("b4"), nodes("c42"), relTypeBC)

    // create relationships to match (b)-->(c) where (c) is in the set of (b)
    relate(nodes("b1"), nodes("b2"), relTypeBC)
    relate(nodes("b2"), nodes("b3"), relTypeBC)
    relate(nodes("b3"), nodes("b4"), relTypeBC)
    relate(nodes("b4"), nodes("b1"), relTypeBC)

    nodes.toMap
  }

  // No predicate

  test("should handle triadic friend of a friend") {
    // Given
    val nodes = makeTriadicModel()
    // When
    val result = executeWithAllPlanners("MATCH (a:A)-[:KNOWS]->(b)-->(c) RETURN c.name")
    // Then
    result.columnAs("c.name").toSet[Node] should equal(Set("c11", "c12", "c21", "c22", "b2", "b3"))
  }

  test("should handle triadic friend of a friend with no re-type") {
    // Given
    val nodes = makeTriadicModel()
    // When
    val result = executeWithAllPlanners("MATCH (a:A)-->(b)-->(c) RETURN c.name")
    // Then
    result.columnAs("c.name").toSet[Node] should equal(Set("c11", "c12", "c21", "c22", "c31", "c32", "c41", "c42", "b1", "b2", "b3", "b4"))
  }

  // Negative predicate

  test("should handle triadic friend of a friend that is not a friend") {
    // Given
    val nodes = makeTriadicModel()
    // When
    val result = executeWithAllPlanners("MATCH (a:A)-[:KNOWS]->(b)-->(c) WHERE NOT (a)-[:KNOWS]->(c) RETURN c.name")
    // Then
    result.columnAs("c.name").toSet[Node] should equal(Set("c11", "c12", "c21", "c22", "b3"))
  }

  test("should handle triadic friend of a friend that is not a friend with different rel-type") {
    // Given
    val nodes = makeTriadicModel()
    // When
    val result = executeWithAllPlanners("MATCH (a:A)-[:KNOWS]->(b)-->(c) WHERE NOT (a)-[:FOLLOWS]->(c) RETURN c.name")
    // Then
    result.columnAs("c.name").toSet[Node] should equal(Set("c11", "c12", "c21", "c22", "b2"))
  }

  test("should handle triadic friend of a friend that is not a friend with superset of rel-type") {
    // Given
    val nodes = makeTriadicModel()
    // When
    val result = executeWithAllPlanners("MATCH (a:A)-[:KNOWS]->(b)-->(c) WHERE NOT (a)-->(c) RETURN c.name")
    // Then
    result.columnAs("c.name").toSet[Node] should equal(Set("c11", "c12", "c21", "c22"))
  }

  test("should handle triadic friend of a friend that is not a friend with implicit subset of rel-type") {
    // Given
    val nodes = makeTriadicModel()
    // When
    val result = executeWithCostPlannerOnly("MATCH (a:A)-->(b)-->(c) WHERE NOT (a)-[:KNOWS]->(c) RETURN c.name")
    // Then
    result.columnAs("c.name").toSet[Node] should equal(Set("c11", "c12", "c21", "c22", "c31", "c32", "c41", "c42", "b3", "b4"))
  }

  test("should handle triadic friend of a friend that is not a friend with explicit subset of rel-type") {
    // Given
    val nodes = makeTriadicModel()
    // When
    val result = executeWithAllPlanners("MATCH (a:A)-[:KNOWS|FOLLOWS]->(b)-->(c) WHERE NOT (a)-[:KNOWS]->(c) RETURN c.name")
    // Then
    result.columnAs("c.name").toSet[Node] should equal(Set("c11", "c12", "c21", "c22", "c31", "c32", "c41", "c42", "b3", "b4"))
  }

  test("should handle triadic friend of a friend that is not a friend with same labels") {
    // Given
    val nodes = makeTriadicModel(bLabel = "X", cLabel1 = "X", cLabel2 = "Y")
    // When
    val result = executeWithAllPlanners("MATCH (a:A)-[:KNOWS]->(b:X)-->(c:X) WHERE NOT (a)-[:KNOWS]->(c) RETURN c.name")
    // Then
    result.columnAs("c.name").toSet[Node] should equal(Set("c11", "c21", "b3"))
  }

  test("should handle triadic friend of a friend that is not a friend with different labels") {
    // Given
    val nodes = makeTriadicModel(bLabel = "X", cLabel1 = "X", cLabel2 = "Y")
    // When
    val result = executeWithAllPlanners("MATCH (a:A)-[:KNOWS]->(b:X)-->(c:Y) WHERE NOT (a)-[:KNOWS]->(c) RETURN c.name")
    // Then
    result.columnAs("c.name").toSet[Node] should equal(Set("c12", "c22"))
  }

  test("should handle triadic friend of a friend that is not a friend with implicit subset of labels") {
    // Given
    val nodes = makeTriadicModel(bLabel = "X", cLabel1 = "X", cLabel2 = "Y")
    // When
    val result = executeWithAllPlanners("MATCH (a:A)-[:KNOWS]->(b)-->(c:X) WHERE NOT (a)-[:KNOWS]->(c) RETURN c.name")
    // Then
    result.columnAs("c.name").toSet[Node] should equal(Set("c11", "c21", "b3"))
  }

  test("should handle triadic friend of a friend that is not a friend with implicit superset of labels") {
    // Given
    val nodes = makeTriadicModel(bLabel = "X", cLabel1 = "X", cLabel2 = "Y")
    // When
    val result = executeWithAllPlanners("MATCH (a:A)-[:KNOWS]->(b:X)-->(c) WHERE NOT (a)-[:KNOWS]->(c) RETURN c.name")
    // Then
    result.columnAs("c.name").toSet[Node] should equal(Set("c11", "c12", "c21", "c22", "b3"))
  }

  // Positive predicate

  test("should handle triadic friend of a friend that is a friend") {
    // Given
    val nodes = makeTriadicModel()
    // When
    val result = executeWithAllPlanners("MATCH (a:A)-[:KNOWS]->(b)-->(c) WHERE (a)-[:KNOWS]->(c) RETURN c.name")
    // Then
    result.columnAs("c.name").toSet[Node] should equal(Set("b2"))
  }

  test("should handle triadic friend of a friend that is a friend with different rel-type") {
    // Given
    val nodes = makeTriadicModel()
    // When
    val result = executeWithAllPlanners("MATCH (a:A)-[:KNOWS]->(b)-->(c) WHERE (a)-[:FOLLOWS]->(c) RETURN c.name")
    // Then
    result.columnAs("c.name").toSet[Node] should equal(Set("b3"))
  }

  test("should handle triadic friend of a friend that is a friend with superset of rel-type") {
    // Given
    val nodes = makeTriadicModel()
    // When
    val result = executeWithAllPlanners("MATCH (a:A)-[:KNOWS]->(b)-->(c) WHERE (a)-->(c) RETURN c.name")
    // Then
    result.columnAs("c.name").toSet[Node] should equal(Set("b2", "b3"))
  }

  test("should handle triadic friend of a friend that is a friend with implicit subset of rel-type") {
    // Given
    val nodes = makeTriadicModel()
    // When
    val result = executeWithAllPlanners("MATCH (a:A)-->(b)-->(c) WHERE (a)-[:KNOWS]->(c) RETURN c.name")
    // Then
    result.columnAs("c.name").toSet[Node] should equal(Set("b1", "b2"))
  }

  test("should handle triadic friend of a friend that is a friend with explicit subset of rel-type") {
    // Given
    val nodes = makeTriadicModel()
    // When
    val result = executeWithAllPlanners("MATCH (a:A)-[:KNOWS|FOLLOWS]->(b)-->(c) WHERE (a)-[:KNOWS]->(c) RETURN c.name")
    // Then
    result.columnAs("c.name").toSet[Node] should equal(Set("b1", "b2"))
  }

  test("should handle triadic friend of a friend that is a friend with same labels") {
    // Given
    val nodes = makeTriadicModel(bLabel = "X", cLabel1 = "X", cLabel2 = "Y")
    // When
    val result = executeWithAllPlanners("MATCH (a:A)-[:KNOWS]->(b:X)-->(c:X) WHERE (a)-[:KNOWS]->(c) RETURN c.name")
    // Then
    result.columnAs("c.name").toSet[Node] should equal(Set("b2"))
  }

  test("should handle triadic friend of a friend that is a friend with different labels") {
    // Given
    val nodes = makeTriadicModel(bLabel = "X", cLabel1 = "X", cLabel2 = "Y")
    // When
    val result = executeWithAllPlanners("MATCH (a:A)-[:KNOWS]->(b:X)-->(c:Y) WHERE (a)-[:KNOWS]->(c) RETURN c.name")
    // Then
    result.columnAs("c.name").toSet[Node] should equal(Set.empty)
  }

  test("should handle triadic friend of a friend that is a friend with implicit subset of labels") {
    // Given
    val nodes = makeTriadicModel(bLabel = "X", cLabel1 = "X", cLabel2 = "Y")
    // When
    val result = executeWithAllPlanners("MATCH (a:A)-[:KNOWS]->(b)-->(c:X) WHERE (a)-[:KNOWS]->(c) RETURN c.name")
    // Then
    result.columnAs("c.name").toSet[Node] should equal(Set("b2"))
  }

  test("should handle triadic friend of a friend that is a friend with implicit superset of labels") {
    // Given
    val nodes = makeTriadicModel(bLabel = "X", cLabel1 = "X", cLabel2 = "Y")
    // When
    val result = executeWithAllPlanners("MATCH (a:A)-[:KNOWS]->(b:X)-->(c) WHERE (a)-[:KNOWS]->(c) RETURN c.name")
    // Then
    result.columnAs("c.name").toSet[Node] should equal(Set("b2"))
  }

}
