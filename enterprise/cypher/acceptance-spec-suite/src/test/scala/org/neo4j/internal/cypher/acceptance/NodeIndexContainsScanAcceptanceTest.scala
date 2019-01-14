/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

/**
 * These tests are testing the actual index implementation, thus they should all check the actual result.
 * If you only want to verify that plans using indexes are actually planned, please use
 * [[org.neo4j.cypher.internal.compiler.v3_4.planner.logical.LeafPlanningIntegrationTest]]
 */
class NodeIndexContainsScanAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport{
  val expectedToSucceed = Configs.Interpreted
  val expectPlansToFail = Configs.AllRulePlanners + Configs.Cost2_3

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

    val result = executeWith(expectedToSucceed, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("NodeIndexContainsScan"), expectPlansToFail))

    result should evaluateTo(List(Map("l" -> london)))
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

    val result = executeWith(expectedToSucceed, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("NodeIndexContainsScan"), expectPlansToFail))

    result should evaluateTo(List(Map("l" -> london)))
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

    val result = executeWith(expectedToSucceed, query,
    planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("NodeIndexContainsScan"), expectPlansToFail))

    result should evaluateTo(List(Map("l" -> london)))
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

    val result = executeWith(Configs.Interpreted, query,
    planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("NodeIndexSeek"), expectPlansToFail = Configs.AllRulePlanners))

    result should evaluateTo(List(Map("l" -> london)))
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
    val result = executeWith(expectedToSucceed - Configs.Version2_3, query, expectedDifferentResults = Configs.AllRulePlanners)

    result should evaluateTo(List(Map("l" -> london)))
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

    val result = executeWith(expectedToSucceed, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("NodeIndexContainsScan"), expectPlansToFail),
      params = Map("param" -> null))

    result should evaluateTo(List.empty)
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

    failWithError(TestConfiguration(Versions.Default, Planners.Default, Runtimes(Runtimes.ProcedureOrSchema, Runtimes.Interpreted, Runtimes.Slotted)) +
      TestConfiguration(Versions.all, Planners.Cost, Runtimes(Runtimes.Interpreted, Runtimes.Default)) +
      TestConfiguration(Versions.V2_3, Planners.Rule, Runtimes(Runtimes.Interpreted, Runtimes.Default)),
      query, message = List("Expected a string value, but got 42","Expected a string value, but got Long(42)","Expected two strings, but got London and 42"),
      params = Map("param" -> 42))
  }
}
