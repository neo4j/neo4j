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

import java.time.format.DateTimeParseException
import java.time.temporal.UnsupportedTemporalTypeException

import org.neo4j.cypher._
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.values.storable.DurationValue
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Configs

class TemporalAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  private val failConf1 = Configs.Interpreted + Configs.Procs - Configs.OldAndRule
  private val failConf2 = Configs.Interpreted + Configs.Procs - Configs.Version2_3

  // Getting current value of a temporal

  test("should return something for current time/date/etc") {
    for (s <- Seq("date", "localtime", "time", "localdatetime", "datetime")) {
      shouldReturnSomething(s"$s()")
      shouldReturnSomething(s"$s.transaction()")
      shouldReturnSomething(s"$s.statement()")
      shouldReturnSomething(s"$s.realtime()")
      shouldReturnSomething(s"$s.transaction('America/Los_Angeles')")
      shouldReturnSomething(s"$s.statement('America/Los_Angeles')")
      shouldReturnSomething(s"$s.realtime('America/Los_Angeles')")
      shouldReturnSomething(s"$s({timezone: '+01:00'})")
    }
    shouldReturnSomething("datetime({epochMillis:timestamp()})")
    shouldReturnSomething("datetime({epochSeconds:timestamp() / 1000})")
  }

  // Failing when skipping certain values in create or specifying conflicting values

  test("should not create date with missing values") {
    val queries = Seq("{}", "{year:1984, day:11}", "{year:1984, dayOfWeek:3}", "{year:1984, dayOfQuarter:45}")
    shouldNotConstructWithArg("date", queries)
  }

  test("should not create date with conflicting values") {
    val queries = Seq("{year:1984, month: 2, week:11}", "{year:1984, month: 2, dayOfWeek:6}",
      "{year:1984, month: 2, quarter:11}", "{year:1984, month: 2, dayOfQuarter:11}",
      "{year:1984, week: 2, day:11}", "{year:1984, week: 2, quarter:11}", "{year:1984, week: 2, dayOfQuarter:11}",
      "{year:1984, quarter: 2, day:11}", "{year:1984, quarter: 2, dayOfWeek:6}")
    shouldNotConstructWithArg("date", queries)
  }

  test("should not create local time with missing values") {
    val queries = Seq("{}", "{hour:12, minute:31, nanosecond: 645876123}", "{hour:12,  second:14, microsecond: 645876}",
      "{hour:12, millisecond: 645}", "{hour:12, second: 45}")
    shouldNotConstructWithArg("localtime", queries)
  }

  test("should not create time with missing values") {
    val queries = Seq("{}", "{hour:12, minute:31, nanosecond: 645876123}", "{hour:12,  second:14, microsecond: 645876}",
      "{hour:12, millisecond: 645}", "{hour:12, second: 45}")
    shouldNotConstructWithArg("time", queries)
  }

  test("should not create local date time with missing values") {
    val queries = Seq("{}", "{year:1984, day:11}", "{year:1984, dayOfWeek:3}", "{year:1984, dayOfQuarter:45}",
      "{year:1984, hour:11}", "{year:1984, minute:11}", "{year:1984, second:3}",
      "{year:1984, millisecond:45}", "{year:1984, microsecond:11}", "{year:1984, nanosecond:3}",
      "{year:1984, month: 2, hour:11}", "{year:1984, month: 2, minute:11}", "{year:1984, month: 2, second:3}",
      "{year:1984, month: 2, millisecond:45}", "{year:1984, month: 2, microsecond:11}", "{year:1984, month: 2, nanosecond:3}",
      "{year:1984, week: 2, hour:11}", "{year:1984, week: 2, minute:11}", "{year:1984, week: 2, second:3}",
      "{year:1984, week: 2, millisecond:45}", "{year:1984, week: 2, microsecond:11}", "{year:1984, week: 2, nanosecond:3}",
      "{year:1984, quarter: 2, hour:11}", "{year:1984, quarter: 2, minute:11}", "{year:1984, quarter: 2, second:3}",
      "{year:1984, quarter: 2, millisecond:45}", "{year:1984, quarter: 2, microsecond:11}", "{year:1984, quarter: 2, nanosecond:3}",
      "{year:1984, month:2, day:8, hour:12, minute:31, nanosecond: 645876123}", "{year:1984, month:2, day:8, hour:12, second:14, microsecond: 645876}",
      "{year:1984, month:2, day:8, hour:12, millisecond: 645}", "{year:1984, month:2, day:8, hour:12, second: 45}")
    shouldNotConstructWithArg("localdatetime", queries)
  }

  test("should not create local date time with conflicting values") {
    val queries = Seq("{year:1984, month: 2, week:11}", "{year:1984, month: 2, dayOfWeek:6}",
      "{year:1984, month: 2, quarter:11}", "{year:1984, month: 2, dayOfQuarter:11}",
      "{year:1984, week: 2, day:11}", "{year:1984, week: 2, quarter:11}", "{year:1984, week: 2, dayOfQuarter:11}",
      "{year:1984, quarter: 2, day:11}", "{year:1984, quarter: 2, dayOfWeek:6}", "{datetime: datetime(), date: date()}",
      "{datetime: datetime(), time: time()}")
    shouldNotConstructWithArg("localdatetime", queries)
  }

  test("should not create date time with missing values") {
    val queries = Seq("{}", "{year:1984, day:11}", "{year:1984, dayOfWeek:3}", "{year:1984, dayOfQuarter:45}",
      "{year:1984, hour:11}", "{year:1984, minute:11}", "{year:1984, second:3}",
      "{year:1984, millisecond:45}", "{year:1984, microsecond:11}", "{year:1984, nanosecond:3}",
      "{year:1984, month: 2, hour:11}", "{year:1984, month: 2, minute:11}", "{year:1984, month: 2, second:3}",
      "{year:1984, month: 2, millisecond:45}", "{year:1984, month: 2, microsecond:11}", "{year:1984, month: 2, nanosecond:3}",
      "{year:1984, week: 2, hour:11}", "{year:1984, week: 2, minute:11}", "{year:1984, week: 2, second:3}",
      "{year:1984, week: 2, millisecond:45}", "{year:1984, week: 2, microsecond:11}", "{year:1984, week: 2, nanosecond:3}",
      "{year:1984, quarter: 2, hour:11}", "{year:1984, quarter: 2, minute:11}", "{year:1984, quarter: 2, second:3}",
      "{year:1984, quarter: 2, millisecond:45}", "{year:1984, quarter: 2, microsecond:11}", "{year:1984, quarter: 2, nanosecond:3}",
      "{year:1984, month:2, day:8, hour:12, minute:31, nanosecond: 645876123}", "{year:1984, month:2, day:8, hour:12, second:14, microsecond: 645876}",
      "{year:1984, month:2, day:8, hour:12, millisecond: 645}", "{year:1984, month:2, day:8, hour:12, second: 45}")
    shouldNotConstructWithArg("datetime", queries)
  }

  test("should not create date time with conflicting values") {
    val queries = Seq("{year:1984, month: 2, week:11}", "{year:1984, month: 2, dayOfWeek:6}",
      "{year:1984, month: 2, quarter:11}", "{year:1984, month: 2, dayOfQuarter:11}",
      "{year:1984, week: 2, day:11}", "{year:1984, week: 2, quarter:11}", "{year:1984, week: 2, dayOfQuarter:11}",
      "{year:1984, quarter: 2, day:11}", "{year:1984, quarter: 2, dayOfWeek:6}", "{datetime: datetime(), date: date()}",
      "{datetime: datetime(), time: time()}", "{datetime: datetime(), epochSeconds:1}", "{datetime: datetime(), epochMillis: timestamp()}",
      "{date: date(), epochSeconds:1}","{date: date(), epochMillis: timestamp()}",
      "{time: time(), epochSeconds:1}", "{time: time(), epochMillis: timestamp()}", "{epochSeconds:1, epochMillis: timestamp()}")
    shouldNotConstructWithArg("datetime", queries)
  }

  test("should not create date time with conflicting time zones")
  {
    val query = "WITH datetime('1984-07-07T12:34+03:00[Europe/Stockholm]') as d RETURN d"
    val errorMsg = "Timezone and offset do not match"
    failWithError(Configs.Interpreted - Configs.Version2_3 + Configs.Procs, query, Seq(errorMsg), Seq("IllegalArgumentException"))
  }

  // Failing when selecting a wrong group

  test("should not select time from date") {
    shouldNotSelectWithArg("date({year:1984, month: 2, day:11})",
      Seq("localtime", "time", "localdatetime", "datetime"), Seq("{time:x}", "x"))
  }

  test("should not select date from time") {
    shouldNotSelectWithArg("time({hour: 12, minute: 30, second: 40, timezone:'+01:00'})",
      Seq("date", "localdatetime", "datetime"), Seq("{date:x}", "x"))
  }

  test("should not select date from local time") {
    shouldNotSelectWithArg("localtime({hour: 12, minute: 30, second: 40})",
      Seq("date", "localdatetime", "datetime"), Seq("{date:x}", "x"))
  }

  test("should not select datetime from date") {
    shouldNotSelectWithArg("date({year:1984, month: 2, day:11})",
      Seq("localdatetime", "datetime"), Seq("{datetime:x}", "x"))
  }

  test("should not select datetime from time") {
    shouldNotSelectWithArg("time({hour: 12, minute: 30, second: 40, timezone:'+01:00'})",
      Seq("localdatetime", "datetime"), Seq("{datetime:x}", "x"))
  }

  test("should not select datetime from local time") {
    shouldNotSelectWithArg("localtime({hour: 12, minute: 30, second: 40})",
      Seq("localdatetime", "datetime"), Seq("{datetime:x}", "x"))
  }

  test("should not select time into date") {
    shouldNotSelectWithArg("time({hour: 12, minute: 30, second: 40, timezone:'+01:00'})",
      Seq("date"), Seq("{time:x}", "{hour: 12}", "{minute: 30}", "{second: 40}", "{year:1984, month: 2, day:11, timezone: '+1:00'}"))
  }

  test("should not select date into time") {
    shouldNotSelectWithArg("date({year:1984, month: 2, day:11})",
      Seq("time"), Seq("{date:x}", "{year: 1984}", "{month: 2}", "{day: 11}"))
  }

  test("should not select date into local time") {
    shouldNotSelectWithArg("date({year:1984, month: 2, day:11})",
      Seq("localtime"), Seq("{date:x}", "{year: 1984}", "{month: 2}", "{day: 11}"))
  }

  test("should not select datetime into date") {
    shouldNotSelectWithArg("datetime({year:1984, month: 2, day:11, hour: 12, minute: 30, second: 40, timezone:'+01:00'})",
      Seq("date"), Seq("{datetime:x}"))
  }

  test("should not select datetime into time") {
    shouldNotSelectWithArg("datetime({year:1984, month: 2, day:11, hour: 12, minute: 30, second: 40, timezone:'+01:00'})",
      Seq("time"), Seq("{datetime:x}"))
  }

  test("should not select datetime into local time") {
    shouldNotSelectWithArg("datetime({year:1984, month: 2, day:11, hour: 12, minute: 30, second: 40, timezone:'+01:00'})",
      Seq("localtime"), Seq("{datetime:x}"))
  }

  // Truncating with wrong receiver or argument

  test("should not truncate to millennium with wrong receiver") {
    shouldNotTruncate(Seq("time", "localtime"), "millennium",
      Seq("datetime({year:1984, month: 2, day:11, hour: 12, minute: 30, second: 40, timezone:'+01:00'})"), "UnsupportedTemporalTypeException")
  }

  test("should not truncate to millennium with wrong argument") {
    shouldNotTruncate(Seq("datetime", "localdatetime", "date"), "millennium",
      Seq("time({hour: 12, minute: 30, second: 40, timezone:'+01:00'})", "localtime({hour: 12, minute: 30, second: 40})"), "IllegalArgumentException")
  }

  test("should not truncate to century with wrong receiver") {
    shouldNotTruncate(Seq("time", "localtime"), "century",
      Seq("datetime({year:1984, month: 2, day:11, hour: 12, minute: 30, second: 40, timezone:'+01:00'})"), "UnsupportedTemporalTypeException")
  }

  test("should not truncate to century with wrong argument") {
    shouldNotTruncate(Seq("datetime", "localdatetime", "date"), "century",
      Seq("time({hour: 12, minute: 30, second: 40, timezone:'+01:00'})", "localtime({hour: 12, minute: 30, second: 40})"), "IllegalArgumentException")
  }

  test("should not truncate to decade with wrong receiver") {
    shouldNotTruncate(Seq("time", "localtime"), "decade",
      Seq("datetime({year:1984, month: 2, day:11, hour: 12, minute: 30, second: 40, timezone:'+01:00'})"), "UnsupportedTemporalTypeException")
  }

  test("should not truncate to decade with wrong argument") {
    shouldNotTruncate(Seq("datetime", "localdatetime", "date"), "decade",
      Seq("time({hour: 12, minute: 30, second: 40, timezone:'+01:00'})", "localtime({hour: 12, minute: 30, second: 40})"), "IllegalArgumentException")
  }

  test("should not truncate to year with wrong receiver") {
    shouldNotTruncate(Seq("time", "localtime"), "year",
      Seq("datetime({year:1984, month: 2, day:11, hour: 12, minute: 30, second: 40, timezone:'+01:00'})"), "UnsupportedTemporalTypeException")
  }

  test("should not truncate to year with wrong argument") {
    shouldNotTruncate(Seq("datetime", "localdatetime", "date"), "year",
      Seq("time({hour: 12, minute: 30, second: 40, timezone:'+01:00'})", "localtime({hour: 12, minute: 30, second: 40})"), "IllegalArgumentException")
  }

  test("should not truncate to weekYear with wrong receiver") {
    shouldNotTruncate(Seq("time", "localtime"), "weekYear",
      Seq("datetime({year:1984, month: 2, day:11, hour: 12, minute: 30, second: 40, timezone:'+01:00'})"), "UnsupportedTemporalTypeException")
  }

  test("should not truncate to weekYear with wrong argument") {
    shouldNotTruncate(Seq("datetime", "localdatetime", "date"), "weekYear",
      Seq("time({hour: 12, minute: 30, second: 40, timezone:'+01:00'})", "localtime({hour: 12, minute: 30, second: 40})"), "IllegalArgumentException")
  }

  test("should not truncate to quarter with wrong receiver") {
    shouldNotTruncate(Seq("time", "localtime"), "quarter",
      Seq("datetime({year:1984, month: 2, day:11, hour: 12, minute: 30, second: 40, timezone:'+01:00'})"), "UnsupportedTemporalTypeException")
  }

  test("should not truncate to quarter with wrong argument") {
    shouldNotTruncate(Seq("datetime", "localdatetime", "date"), "quarter",
      Seq("time({hour: 12, minute: 30, second: 40, timezone:'+01:00'})", "localtime({hour: 12, minute: 30, second: 40})"), "IllegalArgumentException")
  }

  test("should not truncate to month with wrong receiver") {
    shouldNotTruncate(Seq("time", "localtime"), "month",
      Seq("datetime({year:1984, month: 2, day:11, hour: 12, minute: 30, second: 40, timezone:'+01:00'})"), "UnsupportedTemporalTypeException")
  }

  test("should not truncate to month with wrong argument") {
    shouldNotTruncate(Seq("datetime", "localdatetime", "date"), "month",
      Seq("time({hour: 12, minute: 30, second: 40, timezone:'+01:00'})", "localtime({hour: 12, minute: 30, second: 40})"), "IllegalArgumentException")
  }

  test("should not truncate to week with wrong receiver") {
    shouldNotTruncate(Seq("time", "localtime"), "week",
      Seq("datetime({year:1984, month: 2, day:11, hour: 12, minute: 30, second: 40, timezone:'+01:00'})"), "UnsupportedTemporalTypeException")
  }

  test("should not truncate to week with wrong argument") {
    shouldNotTruncate(Seq("datetime", "localdatetime", "date"), "week",
      Seq("time({hour: 12, minute: 30, second: 40, timezone:'+01:00'})", "localtime({hour: 12, minute: 30, second: 40})"), "IllegalArgumentException")
  }

  test("should not truncate to day with wrong argument") {
    shouldNotTruncate(Seq("datetime", "localdatetime", "date"), "day",
      Seq("time({hour: 12, minute: 30, second: 40, timezone:'+01:00'})", "localtime({hour: 12, minute: 30, second: 40})"), "IllegalArgumentException")
  }

  test("should not truncate to hour with wrong receiver") {
    shouldNotTruncate(Seq("date"), "hour",
      Seq("datetime({year:1984, month: 2, day:11, hour: 12, minute: 30, second: 40, timezone:'+01:00'})"), "UnsupportedTemporalTypeException")
  }

  test("should not truncate datetime to hour with wrong argument") {
    shouldNotTruncate(Seq("datetime"), "hour",
      Seq("date({year:1984, month: 2, day:11})",
        "time({hour: 12, minute: 30, second: 40, timezone:'+01:00'})",
        "localtime({hour: 12, minute: 30, second: 40})"), "IllegalArgumentException")
  }

  test("should not truncate localdatetime to hour with wrong argument") {
    shouldNotTruncate(Seq("localdatetime"), "hour",
      Seq("date({year:1984, month: 2, day:11})",
        "time({hour: 12, minute: 30, second: 40, timezone:'+01:00'})",
        "localtime({hour: 12, minute: 30, second: 40})"), "IllegalArgumentException")
  }

  test("should not truncate time to hour with wrong argument") {
    shouldNotTruncate(Seq("time"), "hour",
      Seq("date({year:1984, month: 2, day:11})"), "IllegalArgumentException")
  }

  test("should not truncate localtime to hour with wrong argument") {
    shouldNotTruncate(Seq("localtime"), "hour",
      Seq("date({year:1984, month: 2, day:11})"), "IllegalArgumentException")
  }

  test("should not truncate to minute with wrong receiver") {
    shouldNotTruncate(Seq("date"), "minute",
      Seq("datetime({year:1984, month: 2, day:11, hour: 12, minute: 30, second: 40, timezone:'+01:00'})"), "UnsupportedTemporalTypeException")
  }

  test("should not truncate datetime to minute with wrong argument") {
    shouldNotTruncate(Seq("datetime"), "minute",
      Seq("date({year:1984, month: 2, day:11})",
        "time({hour: 12, minute: 30, second: 40, timezone:'+01:00'})",
        "localtime({hour: 12, minute: 30, second: 40})"), "IllegalArgumentException")
  }

  test("should not truncate localdatetime to minute with wrong argument") {
    shouldNotTruncate(Seq("localdatetime"), "minute",
      Seq("date({year:1984, month: 2, day:11})",
        "time({hour: 12, minute: 30, second: 40, timezone:'+01:00'})",
        "localtime({hour: 12, minute: 30, second: 40})"), "IllegalArgumentException")
  }

  test("should not truncate time to minute with wrong argument") {
    shouldNotTruncate(Seq("time"), "minute",
      Seq("date({year:1984, month: 2, day:11})"), "IllegalArgumentException")
  }

  test("should not truncate localtime to minute with wrong argument") {
    shouldNotTruncate(Seq("localtime"), "minute",
      Seq("date({year:1984, month: 2, day:11})"), "IllegalArgumentException")
  }

  test("should not truncate to second with wrong receiver") {
    shouldNotTruncate(Seq("date"), "second",
      Seq("datetime({year:1984, month: 2, day:11, hour: 12, minute: 30, second: 40, timezone:'+01:00'})"), "UnsupportedTemporalTypeException")
  }

  test("should not truncate datetime to second with wrong argument") {
    shouldNotTruncate(Seq("datetime"), "second",
      Seq("date({year:1984, month: 2, day:11})",
        "time({hour: 12, minute: 30, second: 40, timezone:'+01:00'})",
        "localtime({hour: 12, minute: 30, second: 40})"), "IllegalArgumentException")
  }

  test("should not truncate localdatetime to second with wrong argument") {
    shouldNotTruncate(Seq("localdatetime"), "second",
      Seq("date({year:1984, month: 2, day:11})",
        "time({hour: 12, minute: 30, second: 40, timezone:'+01:00'})",
        "localtime({hour: 12, minute: 30, second: 40})"), "IllegalArgumentException")
  }

  test("should not truncate time to second with wrong argument") {
    shouldNotTruncate(Seq("time"), "second",
      Seq("date({year:1984, month: 2, day:11})"), "IllegalArgumentException")
  }

  test("should not truncate localtime to second with wrong argument") {
    shouldNotTruncate(Seq("localtime"), "second",
      Seq("date({year:1984, month: 2, day:11})"), "IllegalArgumentException")
  }

  test("should not truncate to millisecond with wrong receiver") {
    shouldNotTruncate(Seq("date"), "millisecond",
      Seq("datetime({year:1984, month: 2, day:11, hour: 12, minute: 30, second: 40, timezone:'+01:00'})"), "UnsupportedTemporalTypeException")
  }

  test("should not truncate datetime to millisecond with wrong argument") {
    shouldNotTruncate(Seq("datetime"), "millisecond",
      Seq("date({year:1984, month: 2, day:11})",
        "time({hour: 12, minute: 30, second: 40, timezone:'+01:00'})",
        "localtime({hour: 12, minute: 30, second: 40})"), "IllegalArgumentException")
  }

  test("should not truncate localdatetime to millisecond with wrong argument") {
    shouldNotTruncate(Seq("localdatetime"), "millisecond",
      Seq("date({year:1984, month: 2, day:11})",
        "time({hour: 12, minute: 30, second: 40, timezone:'+01:00'})",
        "localtime({hour: 12, minute: 30, second: 40})"), "IllegalArgumentException")
  }

  test("should not truncate time to millisecond with wrong argument") {
    shouldNotTruncate(Seq("time"), "millisecond",
      Seq("date({year:1984, month: 2, day:11})"), "IllegalArgumentException")
  }

  test("should not truncate localtime to millisecond with wrong argument") {
    shouldNotTruncate(Seq("localtime"), "millisecond",
      Seq("date({year:1984, month: 2, day:11})"), "IllegalArgumentException")
  }

  test("should not truncate to microsecond with wrong receiver") {
    shouldNotTruncate(Seq("date"), "microsecond",
      Seq("datetime({year:1984, month: 2, day:11, hour: 12, minute: 30, second: 40, timezone:'+01:00'})"), "UnsupportedTemporalTypeException")
  }

  test("should not truncate datetime to microsecond with wrong argument") {
    shouldNotTruncate(Seq("datetime"), "microsecond",
      Seq("date({year:1984, month: 2, day:11})",
        "time({hour: 12, minute: 30, second: 40, timezone:'+01:00'})",
        "localtime({hour: 12, minute: 30, second: 40})"), "IllegalArgumentException")
  }

  test("should not truncate localdatetime to microsecond with wrong argument") {
    shouldNotTruncate(Seq("localdatetime"), "microsecond",
      Seq("date({year:1984, month: 2, day:11})",
        "time({hour: 12, minute: 30, second: 40, timezone:'+01:00'})",
        "localtime({hour: 12, minute: 30, second: 40})"), "IllegalArgumentException")
  }

  test("should not truncate time to microsecond with wrong argument") {
    shouldNotTruncate(Seq("time"), "microsecond",
      Seq("date({year:1984, month: 2, day:11})"), "IllegalArgumentException")
  }

  test("should not truncate localtime to microsecond with wrong argument") {
    shouldNotTruncate(Seq("localtime"), "microsecond",
      Seq("date({year:1984, month: 2, day:11})"), "IllegalArgumentException")
  }

  // Arithmetic

  test("subtracting temporal instants should give meaningful error message") {
    for (func <- Seq("date", "localtime", "time", "localdatetime", "datetime")) {
      val query = s"RETURN $func() - $func()"
      val exception = intercept[QueryExecutionException] {
        println(graph.execute(query).next())
      }
      exception.getMessage should startWith("Type mismatch: expected Duration but was")
    }
  }

  // Parsing

  test("should not allow decimals on any but the least significant given value") {
    for (arg <- Seq("P1.5Y1M", "P1Y1.5M1D", "P1Y1M1.5DT1H", "P1Y1M1DT1.5H1M", "P1Y1M1DT1H1.5M1S")) {
      val query = s"RETURN duration('$arg')"
      val exception = intercept[DateTimeParseException] {
        println(graph.execute(query).next())
      }
      exception.getMessage should be ("Text cannot be parsed to a Duration")
    }
  }

  // Accessors

  test("should not provide undefined accessors for date") {
    shouldNotHaveAccessor("date", Seq("hour", "minute", "second", "millisecond", "microsecond", "nanosecond",
      "timezone", "offset", "offsetMinutes", "epochSeconds", "epochMillis",
      "years", "months", "days", "hours", "minutes", "seconds", "milliseconds", "microseconds", "nanoseconds"))
  }

  test("should not provide undefined accessors for local time") {
    shouldNotHaveAccessor("localtime", Seq("year", "quarter", "month", "week", "weekYear", "day",  "ordinalDay", "weekDay", "dayOfQuarter",
      "timezone", "offset", "offsetMinutes", "epochSeconds", "epochMillis",
      "years", "months", "days", "hours", "minutes", "seconds", "milliseconds", "microseconds", "nanoseconds"))
  }

  test("should not provide undefined accessors for time") {
    shouldNotHaveAccessor("time", Seq("year", "quarter", "month", "week", "weekYear", "day",  "ordinalDay", "weekDay", "dayOfQuarter",
      "epochSeconds", "epochMillis",
      "years", "months", "days", "hours", "minutes", "seconds", "milliseconds", "microseconds", "nanoseconds"))
  }

  test("should not provide undefined accessors for local date time") {
    shouldNotHaveAccessor("localdatetime", Seq("timezone", "offset", "offsetMinutes", "epochSeconds", "epochMillis",
      "years", "months", "days", "hours", "minutes", "seconds", "milliseconds", "microseconds", "nanoseconds"))
  }

  test("should not provide undefined accessors for date time") {
    shouldNotHaveAccessor("datetime", Seq("years", "months", "days", "hours", "minutes", "seconds", "milliseconds", "microseconds", "nanoseconds"))
  }

  test("should not provide undefined accessors for duration") {
    shouldNotHaveAccessor("duration", Seq("year", "quarter", "month", "week", "weekYear", "day",  "ordinalDay", "weekDay", "dayOfQuarter",
      "hour", "minute", "second", "millisecond", "microsecond", "nanosecond",
      "timezone", "offset", "offsetMinutes", "epochSeconds", "epochMillis"), "{days: 14, hours:16, minutes: 12}")
  }

  // Duration between

  test("should not compute the duration in day units between two time values") {
    val args = Seq("time()", "localtime()")
    for (func <- Seq("inMonths", "inDays"); arg1 <- args; arg2 <- args) {
      val query = s"RETURN duration.$func($arg1, $arg2)"
      withClue(s"Executing $query") {
        failWithError(failConf2, query, Seq.empty, Seq("UnsupportedTemporalTypeException"))
      }
    }
  }

  // Comparison of durations

  test("should not allow comparing durations") {
    for (op <- Seq("<", "<=", ">", ">=")) {
      val query = s"RETURN duration('P1Y1M') $op duration('P1Y30D')"
      withClue(s"Executing $query") {
        /**
          *  Version 3.3 returns null instead due to running with 3.4 runtime
          *  SyntaxException come from the 3.4 planner and IncomparableValuesException from earlier runtimes
          */
        failWithError(Configs.Version3_4 + Configs.OldAndRule + Configs.Procs, query, Seq.empty, Seq("SyntaxException", "IncomparableValuesException"))
      }
    }
  }

  test("should return null when comparing durations and not able to fail at compile time") {
    for (op <- Seq("<", "<=", ">", ">=")) {
      val query = "RETURN $d1 " + op + " $d2 as x"
      withClue(s"Executing $query") {
        // TODO: change to using executeWith when compiled supports temporal parameters
        val res = innerExecuteDeprecated(query, Map("d1" -> DurationValue.duration(1, 0, 0 ,0), "d2" -> DurationValue.duration(0, 30, 0 ,0))).toList
        res should be(List(Map("x" -> null)))
      }
    }
  }

  // Invalid signature

  test("should not accept 4 parameters") {
    for (func <- Seq("time", "localtime", "date", "datetime", "localdatetime", "duration")) {
      val query = s"RETURN $func('', '', '', '')"
      withClue(s"Executing $query") {
        failWithError(Configs.AbsolutelyAll - Configs.Version2_3, query,
          Seq("Function call does not provide the required number of arguments"), Seq("SyntaxException"))
      }
    }
  }

  // Time with named timezone

  test("parse time with named time zone should not be supported") {
    val query =
      """
        | WITH time("12:34:56[Europe/Stockholm]") as t
        | RETURN t
      """.stripMargin

    failWithError(failConf2, query, Seq("Text cannot be parsed to a Time"), Seq("DateTimeParseException"))
  }

  test("create time with named time zone should be supported") {
    // Will take the current offset of Europe/Stockholm so the actual value can not be asserted on due to daylight saving
    val query =
      """
        | WITH time({timezone: 'Europe/Stockholm'}).offset as currentOffset
        | WITH time({hour: 12, minute: 34, second: 56, timezone: currentOffset})  as currentCorrectTime
        | RETURN time({hour: 12, minute: 34, second: 56, timezone:'Europe/Stockholm'}) = currentCorrectTime as comparison
      """.stripMargin

    val result = executeWith(Configs.Interpreted - Configs.OldAndRule, query)
    result.toList should equal(List(Map("comparison" -> true)))
  }

  test("select and truncate time from datetime with named time zone should be supported") {
    val query =
      """
        | WITH datetime({year: 1984, month: 5, day: 5, hour:12, minute:31, second:14, timezone:'Europe/Stockholm'}) as dt
        | RETURN toString(time({time:dt})) as t1, toString(time.truncate('second', dt)) as t2
      """.stripMargin

    val result = executeWith(Configs.Interpreted - Configs.OldAndRule, query)
    result.toList should equal(List(Map("t1" -> "12:31:14+02:00", "t2" -> "12:31:14+02:00")))
  }

  test("select and truncate time with overwritten named time zone should be supported") {
    // Will take the current offset of Europe/Stockholm so the actual value can not be asserted on due to daylight saving
    val query =
      """
        | WITH localtime({hour:12, minute:31, second:14}) as ld, time({timezone: 'Europe/Stockholm'}).offset as currentOffset
        | WITH ld, time({time: ld, timezone: currentOffset})  as currentCorrectTime
        | RETURN time({time:ld, timezone:'Europe/Stockholm'}) = currentCorrectTime as comp1,
        |        time.truncate('second', ld, {timezone: 'Europe/Stockholm'}) = currentCorrectTime as comp2
      """.stripMargin

    val result = executeWith(Configs.Interpreted - Configs.OldAndRule, query)
    result.toList should equal(List(Map("comp1" -> true, "comp2" -> true)))
  }

  // Help methods

  private def shouldNotTruncate(receivers: Seq[String], truncationUnit: String, args: Seq[String], errorType: String): Unit = {
    for (receiver <- receivers; arg <- args) {
      val query = s"RETURN $receiver.truncate('$truncationUnit', $arg)"
      withClue(s"Executing $query") {
        failWithError(failConf2, query, Seq.empty, Seq(errorType))
      }
    }
  }

  private def shouldNotSelectWithArg(withX: String, returnFuncs: Seq[String], args: Seq[String]): Unit = {
    for (func <- returnFuncs; arg <- args) {
      val query = s"WITH $withX as x RETURN $func($arg)"
      withClue(s"Executing $query") {
        failWithError(failConf2, query, Seq.empty, Seq("IllegalArgumentException"))
      }
    }
  }

  private def shouldNotHaveAccessor(typ: String, accessors: Seq[String], args: String = ""): Unit = {
    for (acc <- accessors) {
      val query = s"RETURN $typ($args).$acc"
      withClue(s"Executing $query") {
        failWithError(failConf1, query, Seq.empty, Seq("UnsupportedTemporalTypeException"))
      }
    }
  }

  private def shouldNotConstructWithArg(func: String, args: Seq[String]): Unit = {
    for (arg <- args) {
      val query = s"RETURN $func($arg)"
      withClue(s"Executing $query") {
        failWithError(failConf2, query, Seq.empty, Seq("IllegalArgumentException"))
      }
    }
  }

  private def shouldReturnSomething(func: String): Unit = {
    val query = s"RETURN $func"
    graph.execute(query).next() should not be null
  }

}
