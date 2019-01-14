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

import org.neo4j.cypher.{ExecutionEngineFunSuite, FakeClock}
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

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

  def single[T](values: Iterator[T]):T = {
    val value = values.next()
    values.hasNext shouldBe false
    value
  }
}
