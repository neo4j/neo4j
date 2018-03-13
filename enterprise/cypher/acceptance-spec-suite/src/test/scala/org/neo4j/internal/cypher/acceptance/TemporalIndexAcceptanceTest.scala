/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import java.time.{ZoneId, ZoneOffset}

import org.neo4j.values.storable._

class TemporalIndexAcceptanceTest extends IndexingTestSupport {

  override val cypherComparisonSupport = true

  test("should seek") {
    createIndex()
    assertSeek(DateValue.epochDate(10000))
    assertSeek(DateTimeValue.datetime(10000, 100, ZoneOffset.UTC))
    assertSeek(LocalDateTimeValue.localDateTime(10000, 100))
    assertSeek(TimeValue.time(101010, ZoneOffset.UTC))
    assertSeek(LocalTimeValue.localTime(12345))
    assertSeek(DurationValue.duration(41, 32, 23, 14))
  }

  test("should range scan") {
    createIndex()
    assertRangeScan(
      DateValue.epochDate(10000),
      DateValue.epochDate(10002),
      DateValue.epochDate(10004),
      DateValue.epochDate(10006))

    assertRangeScan(
      LocalDateTimeValue.localDateTime(10000, 100000000),
      LocalDateTimeValue.localDateTime(10002, 100000000),
      LocalDateTimeValue.localDateTime(10004, 100000000),
      LocalDateTimeValue.localDateTime(10006, 100000000))

    assertRangeScan(
      DateTimeValue.datetime(10000, 100000000, ZoneId.of("Europe/Stockholm")),
      DateTimeValue.datetime(10002, 100000000, ZoneId.of("Europe/Stockholm")),
      DateTimeValue.datetime(10004, 100000000, ZoneId.of("Europe/Stockholm")),
      DateTimeValue.datetime(10006, 100000000, ZoneId.of("Europe/Stockholm")))

    assertRangeScan(
      LocalTimeValue.localTime(10000),
      LocalTimeValue.localTime(10002),
      LocalTimeValue.localTime(10004),
      LocalTimeValue.localTime(10006))

    assertRangeScan(
      TimeValue.time(10000, ZoneId.of("Europe/Stockholm")),
      TimeValue.time(10002, ZoneId.of("Europe/Stockholm")),
      TimeValue.time(10004, ZoneId.of("Europe/Stockholm")),
      TimeValue.time(10006, ZoneId.of("Europe/Stockholm")))
  }

  def assertSeek(value: Value): Unit = {
    val node = createIndexedNode(value)
    assertSeekMatchFor(value, node)
  }

  def assertRangeScan(v1: Value, v2: Value, v3: Value, v4: Value): Unit = {
    val n1 = createIndexedNode(v1)
    val n2 = createIndexedNode(v2)
    val n3 = createIndexedNode(v3)
    val n4 = createIndexedNode(v4)

    assertRangeScanFor(">", v1, "<", v4, n2, n3)
    assertRangeScanFor(">=", v1, "<=", v4, n1, n2, n3, n4)
  }
}
