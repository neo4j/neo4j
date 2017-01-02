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
package org.neo4j.cypher

/**
 * These tests are testing the actual index implementation, thus they should all check the actual result.
 * If you only want to verify that plans using indexes are actually planned, please use
 * [[org.neo4j.cypher.internal.compiler.v3_1.planner.logical.LeafPlanningIntegrationTest]]
 */
class NodeIndexScanAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport{

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

  test("Regexp filter on top of NodeIndexScan (GH #7059)") {
    // Given
    graph.createIndex("phone_type", "label")
    createLabeledNode(Map("id" -> "8bbee2f14a493fa08bef918eaac0c57caa4f9799", "label" -> "ay"), "phone_type", "timed")
    createLabeledNode(Map("id" -> "5fe613811746aa2a1c29d04c6e107974c3a92486", "label" -> "aa"), "phone_type", "timed")
    createLabeledNode(Map("id" -> "a573480b0f13ca9f33d82df91392f9397031a687", "label" -> "g"), "phone_type", "timed")
    createLabeledNode(Map("id" -> "cf8d30601edd84e19f45e9dfd18d2342b92a36eb", "label" -> "t"), "phone_type", "timed")
    createLabeledNode(Map("id" -> "dd4121287c9b542269bda97744d9e828a46bdac4", "label" -> "ae"), "phone_type", "timed")
    createLabeledNode(Map("id" -> "cf406352f4436a5399025fa3f7a7336a24dabdd3", "label" -> "uw"), "phone_type", "timed")
    createLabeledNode(Map("id" -> "a7d330b126594cd47000193698c8d0f9650bc8c4", "label" -> "eh"), "phone_type", "timed")
    createLabeledNode(Map("id" -> "62021e8bd919b0e84427a1e08dfd7704e6a6bd88", "label" -> "r"), "phone_type", "timed")
    createLabeledNode(Map("id" -> "ea1e1deb3f3634ba823a2d5dac56f8bbd2b5b66d", "label" -> "k"), "phone_type", "timed")
    createLabeledNode(Map("id" -> "35321f07bc9b3028d3f5ee07808969a3bd7d76ee", "label" -> "s"), "phone_type", "timed")
    createLabeledNode(Map("id" -> "c960cff2a19a2088c5885f82725428db0694d7ae", "label" -> "d"), "phone_type", "timed")
    createLabeledNode(Map("id" -> "139dbf46f0dc8a325e27ffd118331ca2947e34f0", "label" -> "z"), "phone_type", "timed")

    // When
    val result = executeWithCostPlannerOnly("MATCH (n:phone_type:timed) where n.label =~ 'a.' return count(n)")

    // Then
    result should (use("NodeIndexScan") and evaluateTo(List(Map("count(n)" -> 3))))
  }
}
