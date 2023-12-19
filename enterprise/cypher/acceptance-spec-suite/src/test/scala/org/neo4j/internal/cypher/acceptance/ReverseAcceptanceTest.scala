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
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Configs


class ReverseAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  test("reverse function should work on strings") {
    // When
    val result = executeWith(Configs.Interpreted, "RETURN reverse('raksO')").columnAs("reverse('raksO')").next().toString

    // Then
    result should equal("Oskar")
  }

  test("reverse function should work with collections of integers") {
    // When
    val result = graph.execute("with [4923,489,521,487] as ids RETURN reverse(ids)")

    val results= result.columnAs("reverse(ids)").next().toString

    // Then
    results should equal ("[487, 521, 489, 4923]")
  }

  test("reverse function should work with collections that contains null") {
    // When
    val result = graph.execute("with [4923,null,521,487] as ids RETURN reverse(ids)")

    val results= result.columnAs("reverse(ids)").next().toString

    // Then
    results should equal ("[487, 521, null, 4923]")
  }

  test("reverse function should work with empty collections") {
    // When
    val result = graph.execute("with [] as ids RETURN reverse(ids)")

    val results= result.columnAs("reverse(ids)").next().toString

    // Then
    results should equal ("[]")
  }

  test("reverse function should work with collections of mixed types") {
    // When
    val result = graph.execute("with [4923,'abc',521,487] as ids RETURN reverse(ids)")

    val results= result.columnAs("reverse(ids)").next().toString

    // Then
    results should equal ("[487, 521, abc, 4923]")
  }

  test("reverse should be able to concatenate to original list") {
    // When
    val query =
      """
        | WITH range(1, 2) AS xs
        | RETURN xs + reverse(xs) AS res
        | """.stripMargin

    val results = graph.execute(query).columnAs("res").next().toString

    // Then
    results should equal("[1, 2, 2, 1]")
  }
}
