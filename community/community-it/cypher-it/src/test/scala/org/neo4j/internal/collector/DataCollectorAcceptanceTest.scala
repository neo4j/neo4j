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

import java.time.ZonedDateTime

import org.neo4j.cypher.{CypherExecutionException, ExecutionEngineFunSuite}
import org.neo4j.internal.collector.DataCollectorMatchers.{beListInOrder, beMapContaining, occurBetween}

class DataCollectorAcceptanceTest extends ExecutionEngineFunSuite {

  test("should retrieve correct section") {
    val res = execute("CALL db.stats.retrieve('GRAPH COUNTS')").single
    res should beMapContaining(
      "section" -> "GRAPH COUNTS"
    )
  }

  test("should fail on non-existing section") {
    intercept[CypherExecutionException](execute("CALL db.stats.retrieve('teddybear')"))
  }

  test("should retrieveAllAnonymized") {
    val before = ZonedDateTime.now()
    val res = execute("CALL db.stats.retrieveAllAnonymized('myToken')")
    val after = ZonedDateTime.now()
    res.toList should beListInOrder(
      beMapContaining(
        "section" -> "META",
        "data" -> beMapContaining(
          "graphToken" -> "myToken",
          "retrieveTime" -> occurBetween(before, after)
        )),
      beMapContaining(
        "section" -> "GRAPH COUNTS"
      )
    )
  }
}
