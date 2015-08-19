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

import org.neo4j.cypher.{NewPlannerTestSupport, ExecutionEngineFunSuite}

class InterpolationAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("should interpolate simple strings") {
    val query = "RETURN $'string' AS s"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("s" -> "string")))
  }

  test("should interpolate simple expression") {
    val query = "RETURN $'${1 + 3}' AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> "4")))
  }

  test("should interpolate simple expression with weird whitespaces") {
    val query = "RETURN $'${   1+ 3 }' AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> "4")))
  }

  test("should interpolate to null for nulls") {
    val query = "RETURN $'${1 + null}' AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> null)))
  }

  test("should interpolate an identifier") {
    val query = "WITH 1 AS n RETURN $'${n}' AS r"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("r" -> "1")))
  }

}
