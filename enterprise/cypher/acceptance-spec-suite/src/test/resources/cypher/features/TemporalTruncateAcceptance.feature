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

Feature: TemporalTruncateAcceptance

  Scenario: Should truncate to millennium
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              date({year:1984, month:10, day:11})] as d
      RETURN datetime.truncate('millennium', d), datetime.truncate('millennium', d, {day:2}),
             localdatetime.truncate('millennium', d), localdatetime.truncate('millennium', d, {day:2}),
             date.truncate('millennium', d), date.truncate('millennium', d, {day:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

  Scenario: Should truncate to millennium with time zone
    Given an empty graph
    When executing query:
      """
      UNWIND [localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              date({year:1984, month:10, day:11})] as d
      RETURN datetime.truncate('millennium', d, {timezone:'Europe/Stockholm'})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

  Scenario: Should truncate to century
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              date({year:1984, month:10, day:11})] as d
      RETURN datetime.truncate('century', d), datetime.truncate('century', d, {day:2}),
             localdatetime.truncate('century', d), localdatetime.truncate('century', d, {day:2}),
             date.truncate('century', d), date.truncate('century', d, {day:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

  Scenario: Should truncate to century with time zone
    Given an empty graph
    When executing query:
      """
      UNWIND [localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              date({year:1984, month:10, day:11})] as d
      RETURN datetime.truncate('century', d, {timezone:'Europe/Stockholm'})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

  Scenario: Should truncate to decade
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              date({year:1984, month:10, day:11})] as d
      RETURN datetime.truncate('decade', d), datetime.truncate('decade', d, {day:2}),
             localdatetime.truncate('decade', d), localdatetime.truncate('decade', d, {day:2}),
             date.truncate('decade', d), date.truncate('decade', d, {day:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

  Scenario: Should truncate to decade with time zone
    Given an empty graph
    When executing query:
      """
      UNWIND [localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              date({year:1984, month:10, day:11})] as d
      RETURN datetime.truncate('decade', d, {timezone:'Europe/Stockholm'})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

  Scenario: Should truncate to year
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              date({year:1984, month:10, day:11})] as d
      RETURN datetime.truncate('year', d), datetime.truncate('year', d, {day:2}),
             localdatetime.truncate('year', d), localdatetime.truncate('year', d, {day:2}),
             date.truncate('year', d), date.truncate('year', d, {day:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

  Scenario: Should truncate to year with time zone
    Given an empty graph
    When executing query:
      """
      UNWIND [localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              date({year:1984, month:10, day:11})] as d
      RETURN datetime.truncate('year', d, {timezone:'Europe/Stockholm'})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

  Scenario: Should truncate to quarter
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              date({year:1984, month:10, day:11})] as d
      RETURN datetime.truncate('quarter', d), datetime.truncate('quarter', d, {day:2}),
             localdatetime.truncate('quarter', d), localdatetime.truncate('quarter', d, {day:2}),
             date.truncate('quarter', d), date.truncate('quarter', d, {day:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

  Scenario: Should truncate to quarter with time zone
    Given an empty graph
    When executing query:
      """
      UNWIND [localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              date({year:1984, month:10, day:11})] as d
      RETURN datetime.truncate('quarter', d, {timezone:'Europe/Stockholm'})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

  Scenario: Should truncate to month
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              date({year:1984, month:10, day:11})] as d
      RETURN datetime.truncate('month', d), datetime.truncate('month', d, {day:2}),
             localdatetime.truncate('month', d), localdatetime.truncate('month', d, {day:2}),
             date.truncate('month', d), date.truncate('month', d, {day:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

  Scenario: Should truncate to month with time zone
    Given an empty graph
    When executing query:
      """
      UNWIND [localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              date({year:1984, month:10, day:11})] as d
      RETURN datetime.truncate('month', d, {timezone:'Europe/Stockholm'})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

 Scenario: Should truncate to week
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              date({year:1984, month:10, day:11})] as d
      RETURN datetime.truncate('week', d), datetime.truncate('week', d, {dayOfWeek:2}),
             localdatetime.truncate('week', d), localdatetime.truncate('week', d, {dayOfWeek:2}),
             date.truncate('week', d), date.truncate('week', d, {dayOfWeek:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

  Scenario: Should truncate to week with time zone
    Given an empty graph
    When executing query:
      """
      UNWIND [localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              date({year:1984, month:10, day:11})] as d
      RETURN datetime.truncate('week', d, {timezone:'Europe/Stockholm'})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

 Scenario: Should truncate to day
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              date({year:1984, month:10, day:11})] as d
      RETURN datetime.truncate('day', d), datetime.truncate('day', d, {nanosecond:2}),
             localdatetime.truncate('day', d), localdatetime.truncate('day', d, {nanosecond:2}),
             date.truncate('day', d)
      """
    Then the result should be, in order:
      | d            |
    And no side effects

  Scenario: Should truncate to day with time zone
    Given an empty graph
    When executing query:
      """
      UNWIND [localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              date({year:1984, month:10, day:11})] as d
      RETURN datetime.truncate('day', d, {timezone:'Europe/Stockholm'})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

 Scenario: Should truncate datetime to hour
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'})] as d
      RETURN datetime.truncate('hour', d), datetime.truncate('hour', d, {nanosecond:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

 Scenario: Should truncate localdatetime to hour
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123})] as d
      RETURN localdatetime.truncate('hour', d), localdatetime.truncate('hour', d, {nanosecond:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

 Scenario: Should truncate time to hour
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localtime({hour:12, minute:31, second:14, nanosecond: 645876123})] as d
      RETURN time.truncate('hour', d), time.truncate('hour', d, {nanosecond:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

 Scenario: Should truncate localtime to hour
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localtime({hour:12, minute:31, second:14, nanosecond: 645876123})] as d
      RETURN localtime.truncate('hour', d), localtime.truncate('hour', d, , {nanosecond:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

 Scenario: Should truncate datetime to minute
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'})] as d
      RETURN datetime.truncate('minute', d), datetime.truncate('minute', d, {nanosecond:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

 Scenario: Should truncate localdatetime to minute
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123})] as d
      RETURN localdatetime.truncate('minute', d), localdatetime.truncate('minute', d, {nanosecond:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

 Scenario: Should truncate time to minute
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localtime({hour:12, minute:31, second:14, nanosecond: 645876123})] as d
      RETURN time.truncate('minute', d), time.truncate('minute', d, {nanosecond:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

 Scenario: Should truncate localtime to minute
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localtime({hour:12, minute:31, second:14, nanosecond: 645876123})] as d
      RETURN localtime.truncate('minute', d), localtime.truncate('minute', d, {nanosecond:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

 Scenario: Should truncate datetime to second
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'})] as d
      RETURN datetime.truncate('second', d), datetime.truncate('second', d, {nanosecond:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

 Scenario: Should truncate localdatetime to second
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123})] as d
      RETURN localdatetime.truncate('second', d), localdatetime.truncate('second', d, {nanosecond:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

 Scenario: Should truncate time to second
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localtime({hour:12, minute:31, second:14, nanosecond: 645876123})] as d
      RETURN time.truncate('second', d), time.truncate('second', d, {nanosecond:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

 Scenario: Should truncate localtime to second
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localtime({hour:12, minute:31, second:14, nanosecond: 645876123})] as d
      RETURN localtime.truncate('second', d), localtime.truncate('second', d, {nanosecond:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

 Scenario: Should truncate datetime to millisecond
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'})] as d
      RETURN datetime.truncate('millisecond', d), datetime.truncate('millisecond', d, {nanosecond:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

 Scenario: Should truncate localdatetime to millisecond
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123})] as d
      RETURN localdatetime.truncate('millisecond', d), localdatetime.truncate('millisecond', d, {nanosecond:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

 Scenario: Should truncate time to millisecond
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localtime({hour:12, minute:31, second:14, nanosecond: 645876123})] as d
      RETURN time.truncate('millisecond', d), time.truncate('millisecond', d, {nanosecond:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

 Scenario: Should truncate localtime to millisecond
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localtime({hour:12, minute:31, second:14, nanosecond: 645876123})] as d
      RETURN localtime.truncate('millisecond', d), localtime.truncate('millisecond', d, {nanosecond:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

 Scenario: Should truncate datetime to microsecond
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'})] as d
      RETURN datetime.truncate('microsecond', d), datetime.truncate('microsecond', d, {nanosecond:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

 Scenario: Should truncate localdatetime to microsecond
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123})] as d
      RETURN localdatetime.truncate('microsecond', d), localdatetime.truncate('microsecond', d, {nanosecond:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

 Scenario: Should truncate time to microsecond
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localtime({hour:12, minute:31, second:14, nanosecond: 645876123})] as d
      RETURN time.truncate('microsecond', d), time.truncate('microsecond', d, {nanosecond:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects

 Scenario: Should truncate localtime to microsecond
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              localtime({hour:12, minute:31, second:14, nanosecond: 645876123})] as d
      RETURN localtime.truncate('microsecond', d), localtime.truncate('microsecond', d, {nanosecond:2})
      """
    Then the result should be, in order:
      | d            |
    And no side effects
