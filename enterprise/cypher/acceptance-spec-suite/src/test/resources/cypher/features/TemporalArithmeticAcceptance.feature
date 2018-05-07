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

Feature: TemporalArithmeticAcceptance

  Scenario: Should add or subtract duration to or from date
    Given an empty graph
    And having executed:
      """
      CREATE (:Val {prop: date({year:1984, month:10, day:11})}),
             (:Durations {prop: [duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 2}),
              duration({months:1, days: -14, hours: 16, minutes: -12, seconds: 70}),
              duration({years: 12.5, months: 5.5, days: 14.5, hours: 16.5, minutes: 12.5, seconds: 70.5, nanoseconds: 3})] })
      """
    When executing query:
      """
      MATCH (v:Val), (d:Durations)
      WITH v.prop as x, d
      UNWIND d.prop as dur
      RETURN x+dur, x-dur
      """
    Then the result should be, in order:
      | x+dur        | x-dur |
      | '1997-03-25' | '1972-04-27' |
      | '1984-10-28' | '1984-09-25' |
      | '1997-10-11' | '1971-10-12' |
    And no side effects

  Scenario: Should add or subtract duration to or from local time
    Given an empty graph
    And having executed:
      """
      CREATE (:Val {prop: localtime({hour:12, minute:31, second:14, nanosecond: 1})}),
             (:Durations {prop: [duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 2}),
              duration({months:1, days: -14, hours: 16, minutes: -12, seconds: 70}),
              duration({years: 12.5, months: 5.5, days: 14.5, hours: 16.5, minutes: 12.5, seconds: 70.5, nanoseconds: 3})] })
      """
    When executing query:
      """
      MATCH (v:Val), (d:Durations)
      WITH v.prop as x, d
      UNWIND d.prop as dur
      RETURN x+dur, x-dur
      """
    Then the result should be, in order:
      | x+dur                | x-dur |
      | '04:44:24.000000003' | '20:18:03.999999999' |
      | '04:20:24.000000001' | '20:42:04.000000001' |
      | '22:29:27.500000004' | '02:33:00.499999998' |
    And no side effects

  Scenario: Should add or subtract duration to or from time
    Given an empty graph
    And having executed:
      """
      CREATE (:Val {prop: time({hour:12, minute:31, second:14, nanosecond: 1, timezone: '+01:00'})}),
             (:Durations {prop: [duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 2}),
              duration({months:1, days: -14, hours: 16, minutes: -12, seconds: 70}),
              duration({years: 12.5, months: 5.5, days: 14.5, hours: 16.5, minutes: 12.5, seconds: 70.5, nanoseconds: 3})] })
      """
    When executing query:
      """
      MATCH (v:Val), (d:Durations)
      WITH v.prop as x, d
      UNWIND d.prop as dur
      RETURN x+dur, x-dur
      """
    Then the result should be, in order:
      | x+dur                      | x-dur |
      | '04:44:24.000000003+01:00' | '20:18:03.999999999+01:00' |
      | '04:20:24.000000001+01:00' | '20:42:04.000000001+01:00' |
      | '22:29:27.500000004+01:00' | '02:33:00.499999998+01:00' |
    And no side effects

  Scenario: Should add or subtract duration to or from local date time
    Given an empty graph
    And having executed:
      """
      CREATE (:Val {prop: localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 1})}),
             (:Durations {prop:  [duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 2}),
              duration({months:1, days: -14, hours: 16, minutes: -12, seconds: 70}),
              duration({years: 12.5, months: 5.5, days: 14.5, hours: 16.5, minutes: 12.5, seconds: 70.5, nanoseconds: 3})] })
      """
    When executing query:
      """
      MATCH (v:Val), (d:Durations)
      WITH v.prop as x, d
      UNWIND d.prop as dur
      RETURN x+dur, x-dur
      """
    Then the result should be, in order:
      | x+dur                           | x-dur |
      | '1997-03-26T04:44:24.000000003' | '1972-04-26T20:18:03.999999999' |
      | '1984-10-29T04:20:24.000000001' | '1984-09-24T20:42:04.000000001' |
      | '1997-10-11T22:29:27.500000004' | '1971-10-12T02:33:00.499999998' |
    And no side effects

  Scenario: Should add or subtract duration to or from date time
    Given an empty graph
    And having executed:
      """
      CREATE (:Val {prop: datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 1, timezone: '+01:00'})}),
             (:Durations {prop: [duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 2}),
              duration({months:1, days: -14, hours: 16, minutes: -12, seconds: 70}),
              duration({years: 12.5, months: 5.5, days: 14.5, hours: 16.5, minutes: 12.5, seconds: 70.5, nanoseconds: 3})] })
      """
    When executing query:
      """
      MATCH (v:Val), (d:Durations)
      WITH v.prop as x, d
      UNWIND d.prop as dur
      RETURN x+dur, x-dur
      """
    Then the result should be, in order:
      | x+dur                                 | x-dur |
      | '1997-03-26T04:44:24.000000003+01:00' | '1972-04-26T20:18:03.999999999+01:00' |
      | '1984-10-29T04:20:24.000000001+01:00' | '1984-09-24T20:42:04.000000001+01:00' |
      | '1997-10-11T22:29:27.500000004+01:00' | '1971-10-12T02:33:00.499999998+01:00' |
    And no side effects

  Scenario: Should add or subtract durations
    Given an empty graph
    And having executed:
      """
      CREATE (:Durations {prop: [duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 1}),
            duration({months:1, days: -14, hours: 16, minutes: -12, seconds: 70}),
            duration({years: 12.5, months: 5.5, days: 14.5, hours: 16.5, minutes: 12.5, seconds: 70.5, nanoseconds: 3})] })
      """
    When executing query:
      """
      MATCH (d:Durations)
      UNWIND d.prop as dur
      UNWIND d.prop as dur2
      RETURN dur+dur2, dur-dur2
      """
    Then the result should be, in order:
      | dur+dur2                         | dur-dur2                             |
      | 'P24Y10M28DT32H26M20.000000002S' | 'PT0S'                               |
      | 'P12Y6MT32H2M20.000000001S'      | 'P12Y4M28DT24M0.000000001S'          |
      | 'P25Y4M43DT50H11M23.500000004S'  | 'P-6M-15DT-17H-45M-3.500000002S'     |
      | 'P12Y6MT32H2M20.000000001S'      | 'P-12Y-4M-28DT-24M-0.000000001S'     |
      | 'P2M-28DT31H38M20S'              | 'PT0S'                               |
      | 'P13Y15DT49H47M23.500000003S'    | 'P-12Y-10M-43DT-18H-9M-3.500000003S' |
      | 'P25Y4M43DT50H11M23.500000004S'  | 'P6M15DT17H45M3.500000002S'          |
      | 'P13Y15DT49H47M23.500000003S'    | 'P12Y10M43DT18H9M3.500000003S'       |
      | 'P25Y10M58DT67H56M27.000000006S' | 'PT0S'                               |
    And no side effects

  Scenario: Should multiply or divide durations by numbers
    Given an empty graph
    And having executed:
      """
      CREATE (:Durations {prop: duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 1}) })
      """
    When executing query:
      """
      MATCH (d:Durations)
      UNWIND [1, 2, 0.5] as num
      RETURN d.prop*num, d.prop/num
      """
    Then the result should be, in order:
      | d.prop*num                       | d.prop/num                       |
      | 'P12Y5M14DT16H13M10.000000001S'  | 'P12Y5M14DT16H13M10.000000001S'  |
      | 'P24Y10M28DT32H26M20.000000002S' | 'P6Y2M22DT13H21M8S'              |
      | 'P6Y2M22DT13H21M8S'              | 'P24Y10M28DT32H26M20.000000002S' |
    And no side effects
