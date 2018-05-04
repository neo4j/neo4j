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

Feature: TemporalToStringAcceptance

  Scenario: Should serialize date
    Given an empty graph
    When executing query:
      """
      WITH date({year:1984, month:10, day:11}) as d
      RETURN toString(d) as ts, date(toString(d)) = d as b
      """
    Then the result should be, in order:
      | ts            | b    |
      | '1984-10-11'  | true |
    And no side effects

  Scenario: Should serialize local time
    Given an empty graph
    When executing query:
      """
      WITH localtime({hour:12, minute:31, second:14, nanosecond: 645876123}) as d
      RETURN toString(d) as ts, localtime(toString(d)) = d as b
      """
    Then the result should be, in order:
      | ts                   | b    |
      | '12:31:14.645876123' | true |
    And no side effects

  Scenario: Should serialize time
    Given an empty graph
    When executing query:
      """
      WITH time({hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}) as d
      RETURN toString(d) as ts, time(toString(d)) = d as b
      """
    Then the result should be, in order:
      | ts                         | b    |
      | '12:31:14.645876123+01:00' | true |
    And no side effects

  Scenario: Should serialize local date time
    Given an empty graph
    When executing query:
      """
      WITH localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}) as d
      RETURN toString(d) as ts, localdatetime(toString(d)) = d as b
      """
    Then the result should be, in order:
      | ts                              | b    |
      | '1984-10-11T12:31:14.645876123' | true |
    And no side effects

  Scenario: Should serialize date time
    Given an empty graph
    When executing query:
      """
      WITH datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}) as d
      RETURN toString(d) as ts, datetime(toString(d)) = d as b
      """
    Then the result should be, in order:
      | ts                                    | b    |
      | '1984-10-11T12:31:14.645876123+01:00' | true |
    And no side effects

  Scenario: Should serialize duration
    Given an empty graph
    When executing query:
      """
      UNWIND [duration({years: 12, months:5, days: 14, hours:16, minutes: 12, seconds: 70, nanoseconds: 1}),
              duration({years: 12, months:5, days: -14, hours:16}),
              duration({minutes: 12, seconds: -60}),
              duration({seconds: 2, milliseconds: -1}),
              duration({seconds: -2, milliseconds: 1}),
              duration({seconds: -2, milliseconds: -1}),
              duration({days: 1, milliseconds: 1}),
              duration({days: 1, milliseconds: -1}),
              duration({seconds: 60, milliseconds: -1}),
              duration({seconds: -60, milliseconds: 1}),
              duration({seconds: -60, milliseconds: -1})
              ] as d
      RETURN toString(d) as ts, duration(toString(d)) = d as b
      """
    Then the result should be, in order:
      | ts                              | b    |
      | 'P12Y5M14DT16H13M10.000000001S' | true |
      | 'P12Y5M-14DT16H'                | true |
      | 'PT11M'                         | true |
      | 'PT1.999S'                      | true |
      | 'PT-1.999S'                     | true |
      | 'PT-2.001S'                     | true |
      | 'P1DT0.001S'                    | true |
      | 'P1DT-0.001S'                   | true |
      | 'PT59.999S'                     | true |
      | 'PT-59.999S'                    | true |
      | 'PT-1M-0.001S'                  | true |
    And no side effects

  Scenario: Should serialize timezones correctly
    Given an empty graph
    When executing query:
      """
      WITH datetime({year: 2017, month: 8, day: 8, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: 'Europe/Stockholm'}) as d
      RETURN toString(d) as ts
      """
    Then the result should be, in order:
      | ts                                                      |
      | '2017-08-08T12:31:14.645876123+02:00[Europe/Stockholm]' |
    And no side effects
