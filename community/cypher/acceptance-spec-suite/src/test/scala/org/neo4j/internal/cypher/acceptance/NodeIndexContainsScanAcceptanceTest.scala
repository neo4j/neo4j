/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher.{CypherTypeException, ExecutionEngineFunSuite, NewPlannerTestSupport}

/**
 * These tests are testing the actual index implementation, thus they should all check the actual result.
 * If you only want to verify that plans using indexes are actually planned, please use
 * [[org.neo4j.cypher.internal.compiler.v3_0.planner.logical.LeafPlanningIntegrationTest]]
 */
class NodeIndexContainsScanAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport{

  test("should be case sensitive for CONTAINS with indexes") {
    val london = createLabeledNode(Map("name" -> "London"), "Location")
    createLabeledNode(Map("name" -> "LONDON"), "Location")
    graph.inTx {
      (1 to 100).foreach { _ =>
        createLabeledNode("Location")
      }
      (1 to 300).map { i =>
        createLabeledNode(Map("name" -> i.toString), "Location")
      }
    }

    graph.createIndex("Location", "name")

    val query = "MATCH (l:Location) WHERE l.name CONTAINS 'ondo' RETURN l"

    val result = executeWithAllPlannersAndCompatibilityMode(query)

    result should (use("NodeIndexContainsScan") and evaluateTo(List(Map("l" -> london))))
  }

  test("should be case sensitive for CONTAINS with unique indexes") {
    val london = createLabeledNode(Map("name" -> "London"), "Location")
    createLabeledNode(Map("name" -> "LONDON"), "Location")
    graph.inTx {
      (1 to 100).foreach { _ =>
        createLabeledNode("Location")
      }
      (1 to 300).map { i =>
        createLabeledNode(Map("name" -> i.toString), "Location")
      }
    }

    graph.createConstraint("Location", "name")

    val query = "MATCH (l:Location) WHERE l.name CONTAINS 'ondo' RETURN l"

    val result = executeWithAllPlannersAndCompatibilityMode(query)

    result should (use("NodeIndexContainsScan") and evaluateTo(List(Map("l" -> london))))
  }

  test("should be case sensitive for CONTAINS with multiple indexes and predicates") {
    val london = createLabeledNode(Map("name" -> "London", "country" -> "UK"), "Location")
    createLabeledNode(Map("name" -> "LONDON", "country" -> "UK"), "Location")
    graph.inTx {
      (1 to 100).foreach { _ =>
        createLabeledNode("Location")
      }
      (1 to 300).map { i =>
        createLabeledNode(Map("name" -> i.toString, "country" -> "UK"), "Location")
      }
    }

    graph.createIndex("Location", "name")
    graph.createIndex("Location", "country")

    val query = "MATCH (l:Location) WHERE l.name CONTAINS 'ondo' AND l.country = 'UK' RETURN l"

    val result = executeWithAllPlannersAndCompatibilityMode(query)

    result should (use("NodeIndexContainsScan") and evaluateTo(List(Map("l" -> london))))
  }

  test("should not use contains index with multiple indexes and predicates where other index is more selective") {
    val london = createLabeledNode(Map("name" -> "London", "country" -> "UK"), "Location")
    createLabeledNode(Map("name" -> "LONDON", "country" -> "UK"), "Location")
    graph.inTx {
      (1 to 100).foreach { _ =>
        createLabeledNode("Location")
      }
      (1 to 300).map { i =>
        createLabeledNode(Map("name" -> i.toString), "Location")
      }
    }

    graph.createIndex("Location", "name")
    graph.createIndex("Location", "country")

    val query = "MATCH (l:Location) WHERE l.name CONTAINS 'ondo' AND l.country = 'UK' RETURN l"

    val result = executeWithAllPlannersAndCompatibilityMode(query)

    result should (use("NodeIndexSeek") and evaluateTo(List(Map("l" -> london))))
  }

  test("should use contains index with multiple indexes and predicates where other index is more selective but we add index hint") {
    val london = createLabeledNode(Map("name" -> "London", "country" -> "UK"), "Location")
    createLabeledNode(Map("name" -> "LONDON", "country" -> "UK"), "Location")
    graph.inTx {
      (1 to 100).foreach { _ =>
        createLabeledNode("Location")
      }
      (1 to 300).map { i =>
        createLabeledNode(Map("name" -> i.toString), "Location")
      }
    }

    graph.createIndex("Location", "name")
    graph.createIndex("Location", "country")

    val query = "MATCH (l:Location) USING INDEX l:Location(name) WHERE l.name CONTAINS 'ondo' AND l.country = 'UK' RETURN l"

    // RULE has bug with this query
    val result = executeWithCostPlannerOnly(query)

    result should (use("NodeIndexContainsScan") and evaluateTo(List(Map("l" -> london))))
  }

  test("should return nothing when invoked with a null value") {
    createLabeledNode(Map("name" -> "London"), "Location")
    createLabeledNode(Map("name" -> "LONDON"), "Location")
    graph.inTx {
      (1 to 100).foreach { _ =>
        createLabeledNode("Location")
      }
      (1 to 300).map { i =>
        createLabeledNode(Map("name" -> i.toString), "Location")
      }
    }

    graph.createConstraint("Location", "name")

    val query = "MATCH (l:Location) WHERE l.name CONTAINS {param} RETURN l"

    val result = executeWithAllPlannersAndCompatibilityMode(query, "param" -> null)

    result should (use("NodeIndexContainsScan") and evaluateTo(List.empty))
  }

  test("throws appropriate type error") {
    createLabeledNode(Map("name" -> "London"), "Location")
    createLabeledNode(Map("name" -> "LONDON"), "Location")
    graph.inTx {
      (1 to 100).foreach { _ =>
        createLabeledNode("Location")
      }
      (1 to 300).map { i =>
        createLabeledNode(Map("name" -> i.toString), "Location")
      }
    }

    graph.createConstraint("Location", "name")

    val query = "MATCH (l:Location) WHERE l.name CONTAINS {param} RETURN l"

    intercept[CypherTypeException](executeWithAllPlannersAndCompatibilityMode(query, "param" -> 42))
  }
}
