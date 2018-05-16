#
# Copyright (c) 2002-2018 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j Enterprise Edition. The included source
# code can be redistributed and/or modified under the terms of the
# GNU AFFERO GENERAL PUBLIC LICENSE Version 3
# (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
# Commons Clause, as found in the associated LICENSE.txt file.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# Neo4j object code can be licensed independently from the source
# under separate terms from the AGPL. Inquiries can be directed to:
# licensing@neo4j.com
#
# More information is also available at:
# https://neo4j.com/licensing/
#

Feature: TemporalAccessorAcceptance

  Scenario: Should provide accessors for date
    Given an empty graph
    And having executed:
      """
      CREATE (:Val {prop: date({year:1984, month:10, day:11}) })
      """
    When executing query:
      """
      MATCH (v:Val)
      WITH v.prop as d
      RETURN d.year, d.quarter, d.month, d.week, d.weekYear, d.day, d.ordinalDay, d.weekDay, d.dayOfQuarter
      """
    Then the result should be, in order:
      | d.year | d.quarter | d.month | d.week | d.weekYear | d.day | d.ordinalDay | d.weekDay | d.dayOfQuarter |
      | 1984   | 4         | 10      | 41     | 1984       | 11    | 285          | 4         | 11             |
    And no side effects

  Scenario: Should provide accessors for date in last weekYear
    Given an empty graph
    And having executed:
      """
      CREATE (:Val {prop: date({year:1984, month:01, day:01}) })
      """
    When executing query:
      """
      MATCH (v:Val)
      WITH v.prop as d
      RETURN d.year, d.weekYear, d.week, d.weekDay
      """
    Then the result should be, in order:
      | d.year | d.weekYear | d.week | d.weekDay |
      | 1984   | 1983       | 52     | 7         |
    And no side effects

  Scenario: Should provide accessors for local time
    Given an empty graph
    And having executed:
      """
      CREATE (:Val {prop: localtime({hour:12, minute:31, second:14, nanosecond: 645876123}) })
      """
    When executing query:
      """
      MATCH (v:Val)
      WITH v.prop as d
      RETURN d.hour, d.minute, d.second, d.millisecond, d.microsecond, d.nanosecond
      """
    Then the result should be, in order:
      | d.hour | d.minute | d.second | d.millisecond | d.microsecond | d.nanosecond |
      | 12     | 31       | 14       | 645           | 645876        | 645876123    |
    And no side effects

  Scenario: Should provide accessors for time
    Given an empty graph
    And having executed:
      """
      CREATE (:Val {prop: time({hour:12, minute:31, second:14, nanosecond: 645876123, timezone:'+01:00'}) })
      """
    When executing query:
      """
      MATCH (v:Val)
      WITH v.prop as d
      RETURN d.hour, d.minute, d.second, d.millisecond, d.microsecond, d.nanosecond, d.timezone, d.offset, d.offsetMinutes, d.offsetSeconds
      """
    Then the result should be, in order:
      | d.hour | d.minute | d.second | d.millisecond | d.microsecond | d.nanosecond | d.timezone | d.offset | d.offsetMinutes | d.offsetSeconds |
      | 12     | 31       | 14       | 645           | 645876        | 645876123    | '+01:00'   | '+01:00' | 60              | 3600            |
    And no side effects

  Scenario: Should provide accessors for local date time
    Given an empty graph
    And having executed:
      """
      CREATE (:Val {prop: localdatetime({year:1984, month:11, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}) })
      """
    When executing query:
      """
      MATCH (v:Val)
      WITH v.prop as d
      RETURN d.year, d.quarter, d.month, d.week, d.weekYear, d.day, d.ordinalDay, d.weekDay, d.dayOfQuarter,
             d.hour, d.minute, d.second, d.millisecond, d.microsecond, d.nanosecond
      """
    Then the result should be, in order:
      | d.year | d.quarter | d.month | d.week | d.weekYear | d.day | d.ordinalDay | d.weekDay | d.dayOfQuarter | d.hour | d.minute | d.second | d.millisecond | d.microsecond | d.nanosecond |
      | 1984   | 4         | 11      | 45     | 1984       | 11    | 316          | 7         | 42             | 12     | 31       | 14       | 645           | 645876        | 645876123    |
    And no side effects

  Scenario: Should provide accessors for date time
    Given an empty graph
    And having executed:
      """
      CREATE (:Val {prop: datetime({year:1984, month:11, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone:'Europe/Stockholm'}) })
      """
    When executing query:
      """
      MATCH (v:Val)
      WITH v.prop as d
      RETURN d.year, d.quarter, d.month, d.week, d.weekYear, d.day, d.ordinalDay, d.weekDay, d.dayOfQuarter,
             d.hour, d.minute, d.second, d.millisecond, d.microsecond, d.nanosecond,
             d.timezone, d.offset, d.offsetMinutes, d.offsetSeconds, d.epochSeconds, d.epochMillis
      """
    Then the result should be, in order:
      | d.year | d.quarter | d.month | d.week | d.weekYear | d.day | d.ordinalDay | d.weekDay | d.dayOfQuarter | d.hour | d.minute | d.second | d.millisecond | d.microsecond | d.nanosecond | d.timezone         | d.offset | d.offsetMinutes | d.offsetSeconds | d.epochSeconds | d.epochMillis |
      | 1984   | 4         | 11      | 45     | 1984       | 11    | 316          | 7         | 42             | 12     | 31       | 14       | 645           | 645876        | 645876123    | 'Europe/Stockholm' | '+01:00' | 60              | 3600            | 469020674      | 469020674645  |
    And no side effects

  Scenario: Should provide accessors for duration
    Given an empty graph
    And having executed:
      """
      CREATE (:Val {prop: duration({years: 1, months:4, days: 10, hours:1, minutes: 1, seconds: 1, nanoseconds: 111111111}) })
      """
    When executing query:
      """
      MATCH (v:Val)
      WITH v.prop as d
      RETURN d.years, d.quarters, d.months, d.weeks, d.days,
             d.hours, d.minutes, d.seconds, d.milliseconds, d.microseconds, d.nanoseconds,
             d.quartersOfYear, d.monthsOfQuarter, d.monthsOfYear, d.daysOfWeek, d.minutesOfHour, d.secondsOfMinute, d.millisecondsOfSecond, d.microsecondsOfSecond, d.nanosecondsOfSecond
      """
    Then the result should be, in order:
      | d.years | d.quarters | d.months | d.weeks | d.days | d.hours | d.minutes | d.seconds | d.milliseconds | d.microseconds | d.nanoseconds | d.quartersOfYear | d.monthsOfQuarter| d.monthsOfYear | d.daysOfWeek | d.minutesOfHour | d.secondsOfMinute | d.millisecondsOfSecond | d.microsecondsOfSecond | d.nanosecondsOfSecond |
      | 1       | 5          | 16       | 1       | 10     | 1       | 61        | 3661      |  3661111       | 3661111111     | 3661111111111 | 1                | 1                | 4              | 3            | 1               | 1                 | 111                    | 111111                 | 111111111             |
    And no side effects
