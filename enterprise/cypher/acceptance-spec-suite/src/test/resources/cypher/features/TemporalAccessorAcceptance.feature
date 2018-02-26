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
      RETURN d.year, d.quarter, d.month, d.week, d.weekYear, d.day, d.ordinalDay, d.weekDay
      """
    Then the result should be, in order:
      | d            |
    And no side effects

  Scenario: Should provide accessors for local time
    Given an empty graph
    When executing query:
      """
      WITH localtime({hour:12, minute:31, second:14, nanosecond: 645876123}) as d
      RETURN d.hour, d.minute, d.second, d.millisecond, d.microsecond, d.nanosecond
      """
    Then the result should be, in order:
      | d            |
    And no side effects

  Scenario: Should provide accessors for time
    Given an empty graph
    When executing query:
      """
      WITH time({hour:12, minute:31, second:14, nanosecond: 645876123, timezone:'+01:00'}) as d
      RETURN d.hour, d.minute, d.second, d.millisecond, d.microsecond, d.nanosecond, d.timezone, d.offset
      """
    Then the result should be, in order:
      | d            |
    And no side effects

  Scenario: Should provide accessors for local date time
    Given an empty graph
    When executing query:
      """
      WITH localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}) as d
      RETURN d.year, d.quarter, d.month, d.week, d.weekYear, d.day, d.ordinalDay, d.weekDay,
             d.hour, d.minute, d.second, d.millisecond, d.microsecond, d.nanosecond
      """
    Then the result should be, in order:
      | d            |
    And no side effects

  Scenario: Should provide accessors for date time
    Given an empty graph
    When executing query:
      """
      WITH datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone:'+01:00'}) as d
      RETURN d.year, d.quarter, d.month, d.week, d.weekYear, d.day, d.ordinalDay, d.weekDay,
             d.hour, d.minute, d.second, d.millisecond, d.microsecond, d.nanosecond,
             d.timezone, d.offset, d.epoch
      """
    Then the result should be, in order:
      | d            |
    And no side effects

  Scenario: Should provide accessors for duration
    Given an empty graph
    When executing query:
      """
      WITH duration({years: 12, months:5, days: 14, hours:16, minutes: 12, seconds: 70, nanoseconds: 123456789}) as d
      RETURN d.years, d.months, d.weeks, d.days,
             d.hours, d.minutes, d.seconds, d.milliseconds, d.microseconds, d.nanoseconds
      """
    Then the result should be, in order:
      | d            |
    And no side effects
