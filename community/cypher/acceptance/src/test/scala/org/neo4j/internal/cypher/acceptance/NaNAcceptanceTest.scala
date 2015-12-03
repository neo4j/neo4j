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

import org.neo4j.cypher.{SyntaxException, ExecutionEngineFunSuite, NewPlannerTestSupport}

class NaNAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("square root of negative number should not produce nan") {
    val query = "WITH sqrt(-1) AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("shouldBeNull" -> null)))
  }

  test("log of negative number should not produce nan") {
    val query = "WITH log(-1) AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("shouldBeNull" -> null)))
  }

  test("log10 of negative number should not produce nan") {
    val query = "WITH log10(-1) AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("shouldBeNull" -> null)))
  }

  test("asin() outside of [-1, 1] should not produce nan") {
    val query = "WITH asin(2) AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("shouldBeNull" -> null)))
  }

  test("acos() outside of [-1, 1] should not produce nan") {
    val query = "WITH acos(2) AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("shouldBeNull" -> null)))
  }

  test("0 over 0 should not produce nan") {
    val query = s"WITH 0.0 / 0.0 AS shouldBeNull RETURN shouldBeNull"

    a [org.neo4j.cypher.ArithmeticException] should be thrownBy executeWithAllPlanners(query)
  }

  test("any other number over 0 should produce a too large number") {
    val query = s"WITH 10.0 / 0.0 AS inf RETURN inf"

    a [SyntaxException] should be thrownBy executeWithAllPlanners(query)
  }

}
