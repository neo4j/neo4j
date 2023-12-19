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

import java.time._

import org.neo4j.cypher.{ExecutionEngineFunSuite, FakeClock}
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._
import org.neo4j.values.storable.DurationValue

class TemporalFunctionsAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport with FakeClock {

  val supported = (Configs.Version3_4 + Configs.Version3_3 + Configs.Version3_1) - Configs.Compiled

  test("should get current default datetime") {
    val result = executeWith(supported, "RETURN datetime() as now")

    val now = single(result.columnAs[ZonedDateTime]("now"))

    now shouldBe a[ZonedDateTime]
  }

  test("should get current 'realtime' datetime") {
    val result = executeWith(supported, "RETURN datetime.realtime() as now")

    val now = single(result.columnAs[ZonedDateTime]("now"))

    now shouldBe a[ZonedDateTime]
  }

  test("should get current default localdatetime") {
    val result = executeWith(supported, "RETURN localdatetime() as now")

    val now = single(result.columnAs[LocalDateTime]("now"))

    now shouldBe a[LocalDateTime]
  }

  test("should get current default date") {
    val result = executeWith(supported, "RETURN date() as now")

    val now = single(result.columnAs[LocalDate]("now"))

    now shouldBe a[LocalDate]
  }

  test("should get current default time") {
    val result = executeWith(supported, "RETURN time() as now")

    val now = single(result.columnAs[OffsetTime]("now"))

    now shouldBe a[OffsetTime]
  }

  test("should get current default localtime") {
    val result = executeWith(supported, "RETURN localtime() as now")

    val now = single(result.columnAs[LocalTime]("now"))

    now shouldBe a[LocalTime]
  }

  test("should get right precision on duration") {
    val result = executeWith(supported, "RETURN duration('P0.9Y') AS duration")
    val duration = single(result.columnAs[DurationValue]("duration"))

    duration should equal(DurationValue.parse("P10M24DT30196.8S"))
  }

  def single[T](values: Iterator[T]):T = {
    val value = values.next()
    values.hasNext shouldBe false
    value
  }
}
