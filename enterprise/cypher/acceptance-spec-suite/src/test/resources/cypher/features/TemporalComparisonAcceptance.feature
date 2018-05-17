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
