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

Feature: TemporalSelectAcceptance

  Scenario: Should select date
    Given an empty graph
    When executing query:
      """
      UNWIND [date({year:1984, month:10, day:11}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              datetime({year:1984, month:10, day:11, hour:12, timezone: '+01:00'})] as dd
      RETURN date({date: dd}) as d,
             date({date: dd, day: 28}) as d2,
             date(dd) as d3
      """
    Then the result should be, in order:
      | d            | d2 | d3 |
    And no side effects

  Scenario: Should select local time
    Given an empty graph
    When executing query:
      """
      UNWIND [localtime({hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:12, minute:31, second:14, microsecond: 645876, timezone: '+01:00'}),
              localdatetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645}),
              datetime({year:1984, month:10, day:11, hour:12, timezone: '+01:00'})] as dd
      RETURN localtime({time:dd}) as d,
             localtime({time:dd, second: 42}) as d2,
             localtime(dd) as d3
      """
    Then the result should be, in order:
      | d                    | d2 | d3 |
    And no side effects

  Scenario: Should select time
    Given an empty graph
    When executing query:
      """
      UNWIND [localtime({hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:12, minute:31, second:14, microsecond: 645876, timezone: '+01:00'}),
              localdatetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645}),
              datetime({year:1984, month:10, day:11, hour:12, timezone: '+01:00'})] as dd
      RETURN time({time:dd}) as d,
             time({time:dd, timezone:'+05:00'}) as d2,
             time({time:dd, second: 42}) as d3,
             time({time:dd, second: 42, timezone:'+05:00'}) as d4,
             time(dd) as d5
      """
    Then the result should be, in order:
      | d                          | d2                         | d3 | d4 | d5|
    And no side effects

  Scenario: Should select local date time
    Given an empty graph
    When executing query:
      """
      UNWIND [date({year:1984, month:10, day:11}),
              localdatetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645}),
              datetime({year:1984, month:10, day:11, hour:12, timezone: '+01:00'})] as dd
      UNWIND [localtime({hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:12, minute:31, second:14, microsecond: 645876, timezone: '+01:00'}),
              localdatetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645}),
              datetime({year:1984, month:10, day:11, hour:12, timezone: '+01:00'})] as tt
      RETURN localdatetime({date:dd, time:tt}) as d,
             localdatetime({date:dd, time:tt, day: 28, second: 42}) as d2
      """
    Then the result should be, in order:
      | d                    | d2 |
    And no side effects

  Scenario: Should select local date time 2
    Given an empty graph
    When executing query:
      """
      UNWIND [localdatetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645}),
              datetime({year:1984, month:10, day:11, hour:12, timezone: '+01:00'})] as dd
      RETURN localdatetime({datetime:dd}) as d,
             localdatetime({datetime:dd, day: 28, second: 42}) as d2,
             localdatetime(dd) as d3
      """
    Then the result should be, in order:
      | d                    | d2 | d3 |
    And no side effects

  Scenario: Should select date time
    Given an empty graph
    When executing query:
      """
      UNWIND [date({year:1984, month:10, day:11}),
              localdatetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645}),
              datetime({year:1984, month:10, day:11, hour:12, timezone: '+01:00'})] as dd
      UNWIND [localtime({hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:12, minute:31, second:14, microsecond: 645876, timezone: '+01:00'}),
              localdatetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645}),
              datetime({year:1984, month:10, day:11, hour:12, timezone: '+01:00'})] as tt
      RETURN datetime({date:dd, time:tt}) as d,
             datetime({date:dd, time:tt, timezone:'+05:00'}) as d2,
             datetime({date:dd, time:tt, day: 28, second: 42}) as d3,
             datetime({date:dd, time:tt, day: 28, second: 42, timezone:'+05:00'}) as d4
      """
    Then the result should be, in order:
      | d                    | d2 | d3 | d4 |
    And no side effects

  Scenario: Should select date time 2
    Given an empty graph
    When executing query:
      """
      UNWIND [localdatetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645}),
              datetime({year:1984, month:10, day:11, hour:12, timezone: '+01:00'})] as dd
      RETURN datetime({datetime:dd}) as d,
             datetime({datetime:dd, day: 28, second: 42}) as d2,
             datetime({datetime:dd, day: 28, second: 42}) as d3,
             datetime({datetime:dd, day: 28, second: 42, timezone:'+05:00'}) as d4,
             datetime(dd) as d5
      """
    Then the result should be, in order:
      | d                    | d2 | d3 | d4 | d5 |
    And no side effects
