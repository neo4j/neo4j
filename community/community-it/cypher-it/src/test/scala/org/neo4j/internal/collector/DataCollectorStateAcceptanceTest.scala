/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.collector

import org.neo4j.cypher._

class DataCollectorStateAcceptanceTest extends ExecutionEngineFunSuite {

  import DataCollectorMatchers._

  private val IDLE = "idle"
  private val COLLECTING = "collecting"

  test("QUERIES: happy path collect cycle") {
    assertStatus(IDLE)

    execute("CALL db.stats.collect('QUERIES')").single should beMap(
      "section" -> "QUERIES",
      "success" -> true,
      "message" -> "Collection started."
    )

    assertStatus(COLLECTING)

    execute("CALL db.stats.stop('QUERIES')").single should beMap(
      "section" -> "QUERIES",
      "success" -> true,
      "message" -> "Collection stopped."
    )

    assertStatus(IDLE)

    execute("CALL db.stats.collect('QUERIES')").single should beMap(
      "section" -> "QUERIES",
      "success" -> true,
      "message" -> "Collection started."
    )

    assertStatus(COLLECTING)
  }

  test("QUERIES: stop while idle is idempotent") {
    // when
    execute("CALL db.stats.stop('QUERIES')").single should beMap(
      "section" -> "QUERIES",
      "success" -> true,
      "message" -> "Collector is idle, no collection ongoing."
    )

    // then
    assertStatus(IDLE)
  }

  test("QUERIES: collect while collecting is idempotent") {
    // given
    execute("CALL db.stats.collect('QUERIES')")
    assertStatus(COLLECTING)

    // when
    execute("CALL db.stats.collect('QUERIES')").single should beMap(
      "section" -> "QUERIES",
      "success" -> true,
      "message" -> "Collection is already ongoing."
    )

    // then
    assertStatus(COLLECTING)
  }

  test("QUERIES: clear while collecting is not allowed") {
    // given
    execute("CALL db.stats.collect('QUERIES')")
    assertStatus(COLLECTING)

    // when
    execute("CALL db.stats.clear('QUERIES')").single should beMap(
      "section" -> "QUERIES",
      "success" -> false,
      "message" -> "Collected data cannot be cleared while collecting."
    )

    // then
    assertStatus(COLLECTING)
  }

  test("collect/stop/clear of invalid section should throw") {
    assertInvalidArgument("CALL db.stats.collect('TeddyBear')")
    assertInvalidArgument("CALL db.stats.stop('TeddyBear')")
    assertInvalidArgument("CALL db.stats.clear('TeddyBear')")
    assertInvalidArgument("CALL db.stats.collect('TOKENS')")
    assertInvalidArgument("CALL db.stats.stop('TOKENS')")
    assertInvalidArgument("CALL db.stats.clear('TOKENS')")
    assertInvalidArgument("CALL db.stats.collect('GRAPH COUNTS')")
    assertInvalidArgument("CALL db.stats.stop('GRAPH COUNTS')")
    assertInvalidArgument("CALL db.stats.clear('GRAPH COUNTS')")
  }

  private def assertInvalidArgument(query: String): Unit = {
     try {
       execute(query)
     } catch {
       case e: CypherExecutionException =>
         e.status should be(org.neo4j.kernel.api.exceptions.Status.General.InvalidArguments)
       case x =>
         x shouldBe a[CypherExecutionException]
     }
  }

  private def assertStatus(status: String): Unit = {
    val res = execute("CALL db.stats.status()").single
    res should beMapContaining(
      "status" -> status,
      "section" -> "QUERIES"
    )
  }
}
