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

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}

class NullAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  val anyNull: AnyRef = null.asInstanceOf[AnyRef]

  test("null nodes should be silently ignored when setting property") {
    // Given empty database

    // When
    val result = updateWithBothPlannersAndCompatibilityMode("optional match (a:DoesNotExist) set a.prop = 42 return a")

    // Then doesn't throw
    result.toList
  }

  test("null nodes should be silently ignored when remove property") {
    // Given empty database

    // When
    val result = updateWithBothPlannersAndCompatibilityMode("optional match (a:DoesNotExist) remove a.prop return a")

    // Then doesn't throw
    result.toList
  }

  test("null nodes should be silently ignored when setting property with +=") {
    // Given empty database

    // When
    val result = updateWithBothPlanners("optional match (a:DoesNotExist) set a += {prop: 42} return a")

    // Then doesn't throw
    result.toList
  }

  test("null nodes should be silently ignored when setting property with =") {
    // Given empty database

    // When
    val result = updateWithBothPlanners("optional match (a:DoesNotExist) set a = {prop: 42} return a")

    // Then doesn't throw
    result.toList
  }

  test("null nodes should be silently ignored when setting label") {
    // Given empty database

    // When
    val result = updateWithBothPlanners("optional match (a:DoesNotExist) set a:L return a")

    // Then doesn't throw
    result.toList
  }

  test("null nodes should be silently ignored when removing label") {
    // Given empty database

    // When
    val result = updateWithBothPlanners("optional match (a:DoesNotExist) remove a:L return a")

    // Then doesn't throw
    result.toList
  }

  test("null nodes should be silently ignored when deleting nodes") {
    // Given empty database

    // When
    val result = updateWithBothPlannersAndCompatibilityMode("optional match (a:DoesNotExist) delete a return a")

    // Then doesn't throw
    result.toList
  }

  test("null nodes should be silently ignored when deleting relationships") {
    // Given empty database

    // When
    val result = updateWithBothPlannersAndCompatibilityMode("optional match ()-[r: DoesNotExist]-() delete r return r")

    // Then doesn't throw
    result.toList
  }

  test("round(null) returns null") {
    executeScalarWithAllPlannersAndCompatibilityMode[Any]("RETURN round(null)") should equal(anyNull)
  }


  test("floor(null) returns null") {
    executeScalarWithAllPlannersAndCompatibilityMode[Any]("RETURN floor(null)") should equal(anyNull)
  }

  test("ceil(null) returns null") {
    executeScalarWithAllPlannersAndCompatibilityMode[Any]("RETURN ceil(null)") should equal(anyNull)
  }

  test("abs(null) returns null") {
    executeScalarWithAllPlannersAndCompatibilityMode[Any]("RETURN abs(null)") should equal(anyNull)
  }

  test("acos(null) returns null") {
    executeScalar[Any]("RETURN acos(null)") should equal(anyNull)
  }

  test("asin(null) returns null") {
    executeScalarWithAllPlannersAndCompatibilityMode[Any]("RETURN asin(null)") should equal(anyNull)
  }

  test("atan(null) returns null") {
    executeScalarWithAllPlannersAndCompatibilityMode[Any]("RETURN atan(null)") should equal(anyNull)
  }

  test("cos(null) returns null") {
    executeScalarWithAllPlannersAndCompatibilityMode[Any]("RETURN cos(null)") should equal(anyNull)
  }

  test("cot(null) returns null") {
    executeScalarWithAllPlannersAndCompatibilityMode[Any]("RETURN cot(null)") should equal(anyNull)
  }

  test("degrees(null) returns null") {
    executeScalar[Any]("RETURN degrees(null)") should equal(anyNull)
  }

  test("exp(null) returns null") {
    executeScalarWithAllPlannersAndCompatibilityMode[Any]("RETURN exp(null)") should equal(anyNull)
  }

  test("log(null) returns null") {
    executeScalarWithAllPlannersAndCompatibilityMode[Any]("RETURN log(null)") should equal(anyNull)
  }

  test("log10(null) returns null") {
    executeScalarWithAllPlannersAndCompatibilityMode[Any]("RETURN log10(null)") should equal(anyNull)
  }

  test("sin(null) returns null") {
    executeScalarWithAllPlannersAndCompatibilityMode[Any]("RETURN sin(null)") should equal(anyNull)
  }

  test("tan(null) returns null") {
    executeScalarWithAllPlannersAndCompatibilityMode[Any]("RETURN tan(null)") should equal(anyNull)
  }

  test("haversin(null) returns null") {
    executeScalarWithAllPlannersAndCompatibilityMode[Any]("RETURN haversin(null)") should equal(anyNull)
  }

  test("sqrt(null) returns null") {
    executeScalarWithAllPlannersAndCompatibilityMode[Any]("RETURN sqrt(null)") should equal(anyNull)
  }

  test("sign(null) returns null") {
    executeScalarWithAllPlannersAndCompatibilityMode[Any]("RETURN sign(null)") should equal(anyNull)
  }

  test("radians(null) returns null") {
    executeScalarWithAllPlannersAndCompatibilityMode[Any]("RETURN radians(null)") should equal(anyNull)
  }

  test("atan2(null, 0.3) returns null") {
    executeScalarWithAllPlannersAndCompatibilityMode[Any]("RETURN atan2(null, 0.3)") should equal(anyNull)
  }

  test("atan2(0.3, null) returns null") {
    executeScalarWithAllPlannersAndCompatibilityMode[Any]("RETURN atan2(0.3, null)") should equal(anyNull)
  }

  test("atan2(null, null) returns null") {
    executeScalarWithAllPlannersAndCompatibilityMode[Any]("RETURN atan2(null, null)") should equal(anyNull)
  }
}
