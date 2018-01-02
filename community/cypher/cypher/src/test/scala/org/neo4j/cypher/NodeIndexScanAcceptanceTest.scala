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
package org.neo4j.cypher

import org.neo4j.cypher.internal.compiler.v2_3.pipes.{IndexSeekByRange, UniqueIndexSeekByRange}

/**
 * These tests are testing the actual index implementation, thus they should all check the actual result.
 * If you only want to verify that plans using indexes are actually planned, please use
 * [[org.neo4j.cypher.internal.compiler.v2_3.planner.logical.LeafPlanningIntegrationTest]]
 */
class NodeIndexScanAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport{

  test("should use index on has") {
    // Given
    val person = createLabeledNode(Map("name" -> "Smith"), "Person")
     1 to 100 foreach (_ => createLabeledNode("Person"))
    graph.createIndex("Person", "name")

    // When
    val result = executeWithCostPlannerOnly(
      "MATCH (p:Person) WHERE has(p.name) RETURN p")

    // Then
    result should (use("NodeIndexScan") and evaluateTo(List(Map("p" -> person))))
  }

  test("should use index on IS NOT NULL") {
    // Given
    val person = createLabeledNode(Map("name" -> "Smith"), "Person")
    1 to 100 foreach (_ => createLabeledNode("Person"))
    graph.createIndex("Person", "name")

    // When
    val result = executeWithCostPlannerOnly(
      "MATCH (p:Person) WHERE p.name IS NOT NULL RETURN p")

    // Then
    result should (use("NodeIndexScan") and evaluateTo(List(Map("p" -> person))))
  }

  test("should use index on exists") {
    // Given
    val person = createLabeledNode(Map("name" -> "Smith"), "Person")
    1 to 100 foreach (_ => createLabeledNode("Person"))
    graph.createIndex("Person", "name")

    // When
    val result = executeWithCostPlannerOnly(
      "MATCH (p:Person) WHERE exists(p.name) RETURN p")

    // Then
    result should (use("NodeIndexScan") and evaluateTo(List(Map("p" -> person))))
  }
}
