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

import java.time.ZonedDateTime

import org.neo4j.cypher.{ExecutionEngineFunSuite, FakeClock}
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

class TemporalFunctionsAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport with FakeClock {

  val supported = (Configs.Version3_4 + Configs.Version3_3 + Configs.Version3_1) - Configs.Compiled

  test("should get current default datetime") {
    val result = executeWith(supported, "RETURN datetime() as now")

    val now = single(result.columnAs[ZonedDateTime]("now"))

    now shouldBe a[ZonedDateTime]
  }

  test("should get current 'reawltime' datetime") {
    val result = executeWith(supported, "RETURN datetime.realtime() as now")

    val now = single(result.columnAs[ZonedDateTime]("now"))

    now shouldBe a[ZonedDateTime]
  }

  def single[T](values: Iterator[T]):T = {
    val value = values.next()
    values.hasNext shouldBe false
    value
  }
}
