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
