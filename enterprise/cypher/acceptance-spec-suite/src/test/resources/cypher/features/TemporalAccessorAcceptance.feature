#
# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

Feature: TemporalAccessorAcceptance

  Scenario: Should provide accessors for date
    Given an empty graph
    When executing query:
      """
      WITH date({year:1984, month:10, day:11}) as d
      RETURN d.year, d.quarter, d.month, d.week, d.weekYear, d.day, d.ordinalDay, d.weekDay, d.dayOfQuarter
      """
    Then the result should be, in order:
      | d.year | d.quarter | d.month | d.week | d.weekYear | d.day | d.ordinalDay | d.weekDay | d.dayOfQuarter |
      | 1984   | 4         | 10      | 41     | 1984       | 11    | 285          | 4         | 11             |
    And no side effects

  Scenario: Should provide accessors for date in last weekYear
    Given an empty graph
    When executing query:
      """
      WITH date({year:1984, month:01, day:01}) as d
      RETURN d.year, d.weekYear, d.week, d.weekDay
      """
    Then the result should be, in order:
      | d.year | d.weekYear | d.week | d.weekDay |
      | 1984   | 1983       | 52     | 7         |
    And no side effects

  Scenario: Should provide accessors for local time
    Given an empty graph
    When executing query:
      """
      WITH localtime({hour:12, minute:31, second:14, nanosecond: 645876123}) as d
      RETURN d.hour, d.minute, d.second, d.millisecond, d.microsecond, d.nanosecond
      """
    Then the result should be, in order:
      | d.hour | d.minute | d.second | d.millisecond | d.microsecond | d.nanosecond |
      | 12     | 31       | 14       | 645           | 645876        | 645876123    |
    And no side effects

  Scenario: Should provide accessors for time
    Given an empty graph
    When executing query:
      """
      WITH time({hour:12, minute:31, second:14, nanosecond: 645876123, timezone:'+01:00'}) as d
      RETURN d.hour, d.minute, d.second, d.millisecond, d.microsecond, d.nanosecond, d.timezone, d.offset
      """
    Then the result should be, in order:
      | d.hour | d.minute | d.second | d.millisecond | d.microsecond | d.nanosecond | d.timezone | d.offset |
      | 12     | 31       | 14       | 645           | 645876        | 645876123    | '+01:00'   | '+01:00' |
    And no side effects

  Scenario: Should provide accessors for local date time
    Given an empty graph
    When executing query:
      """
      WITH localdatetime({year:1984, month:11, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}) as d
      RETURN d.year, d.quarter, d.month, d.week, d.weekYear, d.day, d.ordinalDay, d.weekDay, d.dayOfQuarter,
             d.hour, d.minute, d.second, d.millisecond, d.microsecond, d.nanosecond
      """
    Then the result should be, in order:
      | d.year | d.quarter | d.month | d.week | d.weekYear | d.day | d.ordinalDay | d.weekDay | d.dayOfQuarter | d.hour | d.minute | d.second | d.millisecond | d.microsecond | d.nanosecond |
      | 1984   | 4         | 11      | 45     | 1984       | 11    | 316          | 7         | 42             | 12     | 31       | 14       | 645           | 645876        | 645876123    |
    And no side effects

  Scenario: Should provide accessors for date time
    Given an empty graph
    When executing query:
      """
      WITH datetime({year:1984, month:11, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone:'Europe/Stockholm'}) as d
      RETURN d.year, d.quarter, d.month, d.week, d.weekYear, d.day, d.ordinalDay, d.weekDay, d.dayOfQuarter,
             d.hour, d.minute, d.second, d.millisecond, d.microsecond, d.nanosecond,
             d.timezone, d.offset, d.epoch
      """
    Then the result should be, in order:
      | d.year | d.quarter | d.month | d.week | d.weekYear | d.day | d.ordinalDay | d.weekDay | d.dayOfQuarter | d.hour | d.minute | d.second | d.millisecond | d.microsecond | d.nanosecond | d.timezone         | d.offset | d.epoch      |
      | 1984   | 4         | 11      | 45     | 1984       | 11    | 316          | 7         | 42             | 12     | 31       | 14       | 645           | 645876        | 645876123    | 'Europe/Stockholm' | '+01:00' | 469020674645 |
    And no side effects

  Scenario: Should provide accessors for duration
    Given an empty graph
    When executing query:
      """
      WITH duration({years: 12, months:5, days: -14, hours:16, minutes: 12, seconds: 70, nanoseconds: 123456789}) as d
      RETURN d.years, d.months, d.days,
             d.hours, d.minutes, d.seconds, d.milliseconds, d.microseconds, d.nanoseconds
      """
    Then the result should be, in order:
      | d.years | d.months | d.days | d.hours | d.minutes | d.seconds | d.milliseconds | d.microseconds | d.nanoseconds |
      | 12      | 5        | -14    | 16      | 13        | 10        |  123           | 123456         | 123456789     |
    And no side effects
