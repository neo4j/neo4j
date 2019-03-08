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

class DataCollectorStateAcceptanceTest extends DataCollectorTestSupport {

  import DataCollectorMatchers._

  test("QUERIES: happy path collect cycle") {
    assertCollecting("QUERIES")

    execute("CALL db.stats.stop('QUERIES')").single should beMap(
      "section" -> "QUERIES",
      "success" -> true,
      "message" -> "Collection stopped."
    )

    assertIdle("QUERIES")

    execute("CALL db.stats.collect('QUERIES')").single should beMap(
      "section" -> "QUERIES",
      "success" -> true,
      "message" -> "Collection started."
    )

    assertCollecting("QUERIES")

    execute("CALL db.stats.stop('QUERIES')").single should beMap(
      "section" -> "QUERIES",
      "success" -> true,
      "message" -> "Collection stopped."
    )

    assertIdle("QUERIES")

    execute("CALL db.stats.collect('QUERIES')").single should beMap(
      "section" -> "QUERIES",
      "success" -> true,
      "message" -> "Collection started."
    )

    assertCollecting("QUERIES")
  }

  test("QUERIES: stop while idle is idempotent") {
    // given
    execute("CALL db.stats.stop('QUERIES')").single
    assertIdle("QUERIES")

    // when
    execute("CALL db.stats.stop('QUERIES')").single should beMap(
      "section" -> "QUERIES",
      "success" -> true,
      "message" -> "Collector is idle, no collection ongoing."
    )

    // then
    assertIdle("QUERIES")
  }

  test("QUERIES: collect while collecting is idempotent") {
    // given
    assertCollecting("QUERIES")

    // when
    execute("CALL db.stats.collect('QUERIES')").single should beMap(
      "section" -> "QUERIES",
      "success" -> true,
      "message" -> "Collection is already ongoing."
    )

    // then
    assertCollecting("QUERIES")
  }

  test("QUERIES: clear while collecting is not allowed") {
    // given
    assertCollecting("QUERIES")

    // when
    execute("CALL db.stats.clear('QUERIES')").single should beMap(
      "section" -> "QUERIES",
      "success" -> false,
      "message" -> "Collected data cannot be cleared while collecting."
    )

    // then
    assertCollecting("QUERIES")
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
}
