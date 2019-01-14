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

import java.time.{ZoneId, ZonedDateTime}

import org.neo4j.cypher._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Configs
import org.neo4j.test.TestGraphDatabaseFactory
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.utils.TemporalParseException

abstract class TimeZoneAcceptanceTest(timezone: String) extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  override def databaseConfig(): Map[Setting[_], String] = {
    Map(
      GraphDatabaseSettings.cypher_hints_error -> "true",
      GraphDatabaseSettings.db_temporal_timezone -> timezone)
  }

  test("should use default timezone for current date and time") {
    for (func <- Seq("date", "localtime", "time", "localdatetime", "datetime")) {
      val query = s"RETURN duration.inSeconds($func.statement(), $func.statement('$timezone')) as diff"
      val result = executeWith(Configs.Interpreted - Configs.Version2_3, query)
      result.toList should equal(List(Map("diff" -> DurationValue.duration(0, 0, 0, 0))))
    }
  }

  test("should get timezone for current datetime") {
    val query = s"RETURN datetime().timezone as tz"
    val result = executeWith(Configs.Interpreted - Configs.OldAndRule, query)
    result.toList should equal(List(Map("tz" -> timezone)))
  }

  test("should get timezone for parse time") {
    val query = s"RETURN time('12:00').timezone as tz"
    val result = executeWith(Configs.Interpreted - Configs.OldAndRule, query)
    result.toList should equal(List(Map("tz" -> ZonedDateTime.now(ZoneId.of(timezone)).getOffset.toString)))
  }

  test("should get timezone for parse datetime") {
    val query = s"RETURN datetime('2018-01-01T12:00').timezone as tz"
    val result = executeWith(Configs.Interpreted - Configs.OldAndRule, query)
    result.toList should equal(List(Map("tz" -> timezone)))
  }

  test("should get timezone for select time") {
    val query = s"RETURN time(localtime()).timezone as tz"
    val result = executeWith(Configs.Interpreted - Configs.OldAndRule, query)
    result.toList should equal(List(Map("tz" -> ZonedDateTime.now(ZoneId.of(timezone)).getOffset.toString)))
  }

  test("should get timezone for select datetime") {
    val query = s"RETURN datetime({date: date(), time: localtime()}).timezone as tz"
    val result = executeWith(Configs.Interpreted - Configs.OldAndRule, query)
    result.toList should equal(List(Map("tz" -> timezone)))
  }

  test("should get timezone for build time") {
    val query = s"RETURN time({hour: 12}).timezone as tz"
    val result = executeWith(Configs.Interpreted - Configs.OldAndRule, query)
    result.toList should equal(List(Map("tz" -> ZonedDateTime.now(ZoneId.of(timezone)).getOffset.toString)))
  }

  test("should get timezone for build datetime") {
    val query = s"RETURN datetime({year: 2018}).timezone as tz"
    val result = executeWith(Configs.Interpreted - Configs.OldAndRule, query)
    result.toList should equal(List(Map("tz" -> timezone)))
  }

  test("should get timezone for truncate time") {
    val query = s"RETURN time.truncate('minute', localtime()).timezone as tz"
    val result = executeWith(Configs.Interpreted - Configs.OldAndRule, query)
    result.toList should equal(List(Map("tz" -> ZonedDateTime.now(ZoneId.of(timezone)).getOffset.toString)))
  }

  test("should get timezone for truncate datetime") {
    val query = s"RETURN datetime.truncate('minute', localdatetime()).timezone as tz"
    val result = executeWith(Configs.Interpreted - Configs.OldAndRule, query)
    result.toList should equal(List(Map("tz" -> timezone)))
  }

}

class NamedTimeZoneAcceptanceTest extends TimeZoneAcceptanceTest("Europe/Berlin")

class OffsetTimeZoneAcceptanceTest extends TimeZoneAcceptanceTest("+03:00")

class InvalidTimeZoneConfigTest extends CypherFunSuite with GraphIcing {

  import scala.collection.JavaConverters._

  test("invalid timezone should fail startup") {
    val invalidConfig: Map[Setting[_], String] = Map(GraphDatabaseSettings.db_temporal_timezone -> "Europe/Satia")
    a[TemporalParseException] should be thrownBy {
      new TestGraphDatabaseFactory().newImpermanentDatabase(invalidConfig.asJava)
    }
  }
}