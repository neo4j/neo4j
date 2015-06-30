/*
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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, IncomparableValuesException, NewPlannerTestSupport}

class WhereAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("NOT(p1 AND p2) should return true when p2 is false") {
    createNode("apa")

    val result = executeWithAllPlanners("match n where not(n.name = 'apa' and false) return n")

    result should have size 1
  }

  test("should throw exception if comparing string and number") {
    createLabeledNode(Map("prop" -> "15"), "Label")

    val query = "MATCH (n:Label) WHERE n.prop < 10 RETURN n.prop AS prop"

    a[IncomparableValuesException] should be thrownBy (executeWithCostPlannerOnly(query))
  }

  test("should be able to plan index seek for numerical less than") {
    // Given matches
    createLabeledNode(Map("prop" -> Double.NegativeInfinity), "Label")
    createLabeledNode(Map("prop" -> -5), "Label")
    createLabeledNode(Map("prop" -> 0), "Label")
    createLabeledNode(Map("prop" -> 5), "Label")
    createLabeledNode(Map("prop" -> 5.0), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> 10), "Label")
    createLabeledNode(Map("prop" -> 10.0), "Label")
    createLabeledNode(Map("prop" -> 100), "Label")
    createLabeledNode(Map("prop" -> Double.PositiveInfinity), "Label")
    createLabeledNode(Map("prop" -> Double.NaN), "Label")

    val query = "MATCH (n:Label) WHERE n.prop < 10 RETURN n.prop AS prop"

    // When
    val result = executeWithCostPlannerOnly(query)

    // Then
    result.columnAs[Number]("prop").toSet should equal(Set(Double.NegativeInfinity, -5, 0, 5, 5.0))
  }

  test("should be able to plan index seek for textual less than") {
    // Given matches
    createLabeledNode(Map("prop" -> ""), "Label")
    createLabeledNode(Map("prop" -> "-5"), "Label")
    createLabeledNode(Map("prop" -> "0"), "Label")
    createLabeledNode(Map("prop" -> "10"), "Label")
    createLabeledNode(Map("prop" -> "14whatever"), "Label")

    // Non-matches
    createLabeledNode(Map("prop" -> s"15${java.lang.Character.MIN_VALUE}"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")
    createLabeledNode(Map("prop" -> "5"), "Label")

    val query = "MATCH (n:Label) WHERE n.prop < '15' RETURN n.prop AS prop"

    // When
    val result = executeWithCostPlannerOnly(query)

    println(result.executionPlanDescription())

    // Then
    result.columnAs[Number]("prop").toSet should equal(Set("", "-5", "0", "10", "14whatever"))
  }
}
