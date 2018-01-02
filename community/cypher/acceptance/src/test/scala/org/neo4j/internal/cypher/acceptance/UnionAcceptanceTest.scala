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

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}

class UnionAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {
  test("should be able to create text output from union queries") {
    // When
    val result = executeWithAllPlanners("MATCH (a:A) RETURN a AS a UNION MATCH (a:B) RETURN a AS a")

    // Then
    result.columns should not be empty
  }

  test("two elements, both unique, not distinct") {
    // When
    val result = executeWithAllPlanners("return 1 as x union all return 2 as x")

    // Then
    result.columns should not be empty
    result.toList should equal(List(Map("x" -> 1), Map("x" -> 2)))
  }

  test("two elements, both unique, distinct") {
    // When
    val result = executeWithAllPlanners("return 1 as x union return 2 as x")

    // Then
    result.columns should not be empty
    val cachedResult = result.toList
    cachedResult.toSet should equal(Set(Map("x" -> 2), Map("x" -> 1)))
    cachedResult should have size 2

  }

  test("three elements, two unique, distinct") {
    // When
    val result = executeWithAllPlanners(
      """return 2 as x
        |union
        |return 1 as x
        |union
        |return 2 as x""".stripMargin)

    // Then
    result.columns should not be empty
    val cachedResult = result.toList
    cachedResult.toSet should equal(Set(Map("x" -> 2), Map("x" -> 1)))
    cachedResult should have size 2
  }

  test("three elements, two unique, not distinct") {
    // When
    val result = executeWithAllPlanners(
      """return 2 as x
        |union all
        |return 1 as x
        |union all
        |return 2 as x""".stripMargin)

    // Then
    result.columns should not be empty
    result.toList should equal(List(Map("x" -> 2), Map("x" -> 1), Map("x" -> 2)))
  }
}
