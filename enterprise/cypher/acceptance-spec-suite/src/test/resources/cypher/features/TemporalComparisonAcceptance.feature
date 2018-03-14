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

Feature: TemporalComparisonAcceptance

  Scenario: Should compare dates
    Given an empty graph
    When executing query:
      """
      UNWIND [date({year:1980, month:12, day:24}),
              date({year:1984, month:10, day:11})] as x
      UNWIND [date({year:1984, month:10, day:11}),
              localtime({hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              duration({years: 12, months:5, days: 14, hours:16, minutes: 12, seconds: 70})] as d
      RETURN x>d, x<d, x>=d, x<=d, x=d
      """
    Then the result should be, in order:
      | x>d   | x<d   | x>=d  | x<=d  | x=d    |
      | false | true  | false | true  | false  |
      | null  | null  | null  | null  | false  |
      | null  | null  | null  | null  | false  |
      | null  | null  | null  | null  | false  |
      | null  | null  | null  | null  | false  |
      | null  | null  | null  | null  | false  |
      | false | false | true  | true  | true   |
      | null  | null  | null  | null  | false  |
      | null  | null  | null  | null  | false  |
      | null  | null  | null  | null  | false  |
      | null  | null  | null  | null  | false  |
      | null  | null  | null  | null  | false  |
    And no side effects

  Scenario: Should compare local times
    Given an empty graph
    When executing query:
      """
      UNWIND [localtime({hour:10, minute:35}),
              localtime({hour:12, minute:31, second:14, nanosecond: 645876123})] as x
      UNWIND [date({year:1984, month:10, day:11}),
              localtime({hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              duration({years: 12, months:5, days: 14, hours:16, minutes: 12, seconds: 70})] as d
      RETURN x>d, x<d, x>=d, x<=d, x=d
      """
    Then the result should be, in order:
      | x>d   | x<d   | x>=d  | x<=d  | x=d   |
      | null  | null  | null  | null  | false |
      | false | true  | false | true  | false |
      | null  | null  | null  | null  | false |
      | null  | null  | null  | null  | false |
      | null  | null  | null  | null  | false |
      | null  | null  | null  | null  | false |
      | null  | null  | null  | null  | false |
      | false | false | true  | true  | true  |
      | null  | null  | null  | null  | false |
      | null  | null  | null  | null  | false |
      | null  | null  | null  | null  | false |
      | null  | null  | null  | null  | false |
    And no side effects

  Scenario: Should compare times
    Given an empty graph
    When executing query:
      """
      UNWIND [time({hour:10, minute:0, timezone: '+01:00'}),
              time({hour:9, minute:35, second:14, nanosecond: 645876123, timezone: '+00:00'})] as x
      UNWIND [date({year:1984, month:10, day:11}),
              localtime({hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:9, minute:35, second:14, nanosecond: 645876123, timezone: '+00:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              duration({years: 12, months:5, days: 14, hours:16, minutes: 12, seconds: 70})] as d
      RETURN x>d, x<d, x>=d, x<=d, x=d
      """
    Then the result should be, in order:
      | x>d   | x<d   | x>=d  | x<=d  | x=d   |
      | null  | null  | null  | null  | false |
      | null  | null  | null  | null  | false |
      | false | true  | false | true  | false |
      | null  | null  | null  | null  | false |
      | null  | null  | null  | null  | false |
      | null  | null  | null  | null  | false |
      | null  | null  | null  | null  | false |
      | null  | null  | null  | null  | false |
      | false | false | true  | true  | true  |
      | null  | null  | null  | null  | false |
      | null  | null  | null  | null  | false |
      | null  | null  | null  | null  | false |
    And no side effects

  Scenario: Should compare local date times
    Given an empty graph
    When executing query:
      """
      UNWIND [localdatetime({year:1980, month:12, day:11, hour:12, minute:31, second:14}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123})] as x
      UNWIND [date({year:1984, month:10, day:11}),
              localtime({hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:9, minute:35, second:14, nanosecond: 645876123, timezone: '+00:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              duration({years: 12, months:5, days: 14, hours:16, minutes: 12, seconds: 70})] as d
      RETURN x>d, x<d, x>=d, x<=d, x=d
      """
    Then the result should be, in order:
      | x>d   | x<d   | x>=d  | x<=d  | x=d    |
      | null  | null  | null  | null  | false  |
      | null  | null  | null  | null  | false  |
      | null  | null  | null  | null  | false  |
      | false | true  | false | true  | false  |
      | null  | null  | null  | null  | false  |
      | null  | null  | null  | null  | false  |
      | null  | null  | null  | null  | false  |
      | null  | null  | null  | null  | false  |
      | null  | null  | null  | null  | false  |
      | false | false | true  | true  | true   |
      | null  | null  | null  | null  | false  |
      | null  | null  | null  | null  | false  |
    And no side effects

  Scenario: Should compare date times
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1980, month:12, day:11, hour:12, minute:31, second:14, timezone: '+00:00'}),
              datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, timezone: '+05:00'})] as x
      UNWIND [date({year:1984, month:10, day:11}),
              localtime({hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:9, minute:35, second:14, nanosecond: 645876123, timezone: '+00:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, timezone: '+05:00'}),
              duration({years: 12, months:5, days: 14, hours:16, minutes: 12, seconds: 70})] as d
      RETURN x>d, x<d, x>=d, x<=d, x=d
      """
    Then the result should be, in order:
      | x>d   | x<d   | x>=d  | x<=d  | x=d  |
      | null  | null  | null  | null  | false|
      | null  | null  | null  | null  | false|
      | null  | null  | null  | null  | false|
      | null  | null  | null  | null  | false|
      | false | true  | false | true  | false|
      | null  | null  | null  | null  | false|
      | null  | null  | null  | null  | false|
      | null  | null  | null  | null  | false|
      | null  | null  | null  | null  | false|
      | null  | null  | null  | null  | false|
      | false | false | true  | true  | true |
      | null  | null  | null  | null  | false|
    And no side effects

  Scenario: Should compare durations for equality
    Given an empty graph
    When executing query:
      """
      WITH duration({years: 12, months:5, days: 14, hours:16, minutes: 12, seconds: 70}) as x
      UNWIND [date({year:1984, month:10, day:11}),
              localtime({hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:9, minute:35, second:14, nanosecond: 645876123, timezone: '+00:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, timezone: '+05:00'}),
              duration({years: 12, months:5, days: 14, hours:16, minutes: 12, seconds: 70}),
              duration({years: 12, months:5, days: 14, hours:16, minutes: 13, seconds: 10}),
              duration({years: 12, months:5, days: 13, hours:40, minutes: 13, seconds: 10})] as d
      RETURN x=d
      """
    Then the result should be, in order:
      | x=d    |
      | false  |
      | false  |
      | false  |
      | false  |
      | false  |
      | true   |
      | true   |
      | false  |
    And no side effects
