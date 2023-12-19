/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}

class NaNAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport {
  // This should probably be moved to the TCK
  test("should handle NaN comparisons correctly") {
    // Given
    createNode(Map("x" -> Double.NaN))

    // When
    val result = execute("MATCH (n) RETURN n.x > 0 AS gt, n.x < 0 AS lt, n.x >= 0 AS ge, n.x <= 0 AS le")

    // Then
    result.toList should equal(List(Map("gt" -> null, "lt" -> null, "le" -> null, "ge" -> null)))
  }

  test("should handle NaN comparisons with string correctly") {
    // Given
    createNode(Map("x" -> Double.NaN))

    // When
    val result = execute("MATCH (n) RETURN n.x > 'a' AS gt, n.x < 'a' AS lt, n.x >= 'a' AS ge, n.x <= 'a' AS le")

    // Then
    result.toList should equal(List(Map("gt" -> null, "lt" -> null, "le" -> null, "ge" -> null)))
  }

  test("should handle NaN compared to NaN") {
    // Given
    createNode(Map("x" -> Double.NaN))

    // When
    val result = execute("PROFILE MATCH (n) RETURN n.x > n.x AS gt, n.x < n.x AS lt, n.x <= n.x AS le, n.x >= n.x AS ge")

    // Then
    result.toList should equal(List(Map("gt" -> null, "lt" -> null, "le" -> null, "ge" -> null)))
  }

  test("should handle NaN null checks correctly") {
    // Given
    createNode(Map("x" -> Double.NaN))

    // When
    val result = execute("MATCH (n) RETURN n.x IS NULL AS nu")

    // Then
    result.toList should equal(List(Map("nu" -> false)))
  }

  test("should handle NaN null and not null checks correctly") {
    // Given
    createNode(Map("x" -> Double.NaN))

    // When
    val result = execute("MATCH (n) RETURN n.x IS NULL AS nu, n.x IS NOT NULL AS nnu")

    // Then
    result.toList should equal(List(Map("nu" -> false, "nnu" -> true)))
  }

  test("should handle NaN equality checks correctly") {
    // Given
    createNode(Map("x" -> Double.NaN))

    // When
    val result = execute("MATCH (n) RETURN n.x = n.x AS eq, n.x <> n.x as ne")

    // Then
    result.toList should equal(List(Map("eq" -> null, "ne" -> null)))
  }
}
