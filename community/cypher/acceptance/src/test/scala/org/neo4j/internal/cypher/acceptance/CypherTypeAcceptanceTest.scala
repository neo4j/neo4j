/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.cypher.{CypherTypeException, ExecutionEngineFunSuite, NewPlannerTestSupport, SyntaxException}

class CypherTypeAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("does not lose precision") {
    // Given
    graph.execute("CREATE (:Label { id: 4611686018427387905 })")

    // When
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Number]("match (p:Label) return p.id")

    result should equal(4611686018427387905L)
  }

  test("equality takes the full value into consideration 1") {
    // Given
    graph.execute("CREATE (:Label { id: 4611686018427387905 })")

    // When
    val result = executeWithAllPlannersAndCompatibilityMode("match (p:Label {id: 4611686018427387905}) return p")

    result should not be empty
  }

  test("equality takes the full value into consideration 2") {
    // Given
    graph.execute("CREATE (:Label { id: 4611686018427387905 })")

    // When
    val result = executeWithAllPlannersAndCompatibilityMode("match (p:Label) where p.id = 4611686018427387905 return p")

    result should not be empty
  }

  test("equality takes the full value into consideration 3") {
    // Given
    graph.execute("CREATE (:Label { id: 4611686018427387905 })")

    // When
    val result = executeWithAllPlannersAndCompatibilityMode("match (p:Label) where p.id = 4611686018427387900 return p")

    result should be(empty)
  }

  test("equality takes the full value into consideration 4") {
    // Given
    graph.execute("CREATE (:Label { id: 4611686018427387905 })")

    // When
    val result = executeWithAllPlannersAndCompatibilityMode("match (p:Label {id : 4611686018427387900}) return p")

    result should be(empty)
  }

  test("should be consistent in failing either statically or at runtime when trying to access an array with a non-integer index") {
    a [SyntaxException] should be thrownBy executeWithAllPlannersAndCompatibilityMode("WITH [1,2,3,4,5] AS array, 3.14 AS idx RETURN array[idx]")
    a [CypherTypeException] should be thrownBy executeWithAllPlannersAndCompatibilityMode("WITH [1,2,3,4,5] AS array, {idx} AS idx RETURN array[idx]", "idx" -> 3.14d)
  }
}
