/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import java.time._

import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.{ComparePlansWithAssertion, Configs, TestConfiguration}
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

  test("should seek for arrays") {
    createIndex()

    // Length 1
    assertSeek(Values.dateArray(Array(DateValue.epochDate(10000).asObjectCopy())))
    assertSeek(Values.dateTimeArray(Array(DateTimeValue.datetime(10000, 100, ZoneOffset.UTC).asObjectCopy())))
    assertSeek(Values.localDateTimeArray(Array(LocalDateTimeValue.localDateTime(10000, 100).asObjectCopy())))
    assertSeek(Values.timeArray(Array(TimeValue.time(101010, ZoneOffset.UTC).asObjectCopy())))
    assertSeek(Values.localTimeArray(Array(LocalTimeValue.localTime(12345).asObjectCopy())))
    assertSeek(Values.durationArray(Array(DurationValue.duration(41, 32, 23, 14).asObjectCopy())))

    // Length 2
    assertSeek(Values.dateArray(Array(DateValue.epochDate(10000).asObjectCopy(),
                                      DateValue.epochDate(20000).asObjectCopy())))

    assertSeek(Values.dateTimeArray(Array(DateTimeValue.datetime(10000, 100, ZoneOffset.UTC).asObjectCopy(),
                                           DateTimeValue.datetime(10000, 200, ZoneOffset.UTC).asObjectCopy())))

    assertSeek(Values.localDateTimeArray(Array(LocalDateTimeValue.localDateTime(10000, 100).asObjectCopy(),
                                               LocalDateTimeValue.localDateTime(10000, 200).asObjectCopy())))

    assertSeek(Values.timeArray(Array(TimeValue.time(101010, ZoneOffset.UTC).asObjectCopy(),
                                      TimeValue.time(202020, ZoneOffset.UTC).asObjectCopy())))

    assertSeek(Values.localTimeArray(Array(LocalTimeValue.localTime(12345).asObjectCopy(),
                                           LocalTimeValue.localTime(23456).asObjectCopy())))

    assertSeek(Values.durationArray(Array(DurationValue.duration(41, 32, 23, 14).asObjectCopy(),
                                          DurationValue.duration(12, 34, 56, 78).asObjectCopy())))
  }

  test("should distinguish between duration array and string array") {
    // Given
    val durArray = Values.durationArray(Array(DurationValue.duration(0, 0, 1800, 0), DurationValue.duration(0, 1, 0, 0))).asObject()
    val stringArray = Values.stringArray("PT30M", "P1D").asObject()

    val n1 = createLabeledNode(Map("results" -> durArray), "Runner")
    createLabeledNode(Map("results" -> stringArray), "Runner")

    // When
    val query =
      """
        |MATCH (n:Runner)
        |WHERE n.results = [duration('PT30M'), duration('P1D')]
        |RETURN n
      """.stripMargin

    val resultNoIndex = executeWith(Configs.Interpreted - Configs.OldAndRule, query)

    graph.createIndex("Runner", "results")
    val resultIndex = executeWith(Configs.Interpreted - Configs.OldAndRule, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        //THEN
        plan should useOperators("NodeIndexSeek")
      }))

    // Then
    resultNoIndex.toComparableResult should equal(List(Map("n" -> n1)))
    resultIndex.toComparableResult should equal(resultNoIndex.toComparableResult)
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
      TimeValue.time(10000, ZoneOffset.of("+01:00")),
      TimeValue.time(10002, ZoneOffset.of("+01:00")),
      TimeValue.time(10004, ZoneOffset.of("+01:00")),
      TimeValue.time(10006, ZoneOffset.of("+01:00")))
  }

  test("should handle datetime with named zone and second offset") {
    createIndex()
    val node1 = createIndexedNode(DateTimeValue.datetime(1818, 7, 15, 14, 12, 12, 0, "+01:12"))
    val node2 = createIndexedNode(DateTimeValue.datetime(1818, 7, 15, 14, 12, 12, 0, "Europe/Stockholm")) // corresponds to +01:12:12
    val node3 = createIndexedNode(DateTimeValue.datetime(1818, 7, 15, 14, 12, 12, 0, "+01:13"))

    val query =
      """
        | MATCH (n:Label)
        | WHERE n.prop > datetime("1818-07-15T12:59:59Z") AND n.prop < datetime("1818-07-15T13:00:01Z")
        | RETURN n
      """.stripMargin

    val result = executeWith(Configs.Interpreted - Configs.OldAndRule, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("NodeIndexSeekByRange")))

    result.toList should equal(List(Map("n" -> node2)))
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

    assertRangeScanFor(">", v2, n3, n4)
    assertRangeScanFor(">=", v2, n2, n3, n4)
    assertRangeScanFor("<", v3, n1, n2)
    assertRangeScanFor("<=", v3, n1, n2, n3)

    assertRangeScanFor(">", v1, "<", v4, n2, n3)
    assertRangeScanFor(">=", v1, "<=", v4, n1, n2, n3, n4)
  }
}
