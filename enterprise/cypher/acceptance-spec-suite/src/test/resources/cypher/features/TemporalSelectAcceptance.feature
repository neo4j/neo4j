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
      UNWIND [date({year:1984, month:11, day:11}),
              localdatetime({year:1984, month:11, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              datetime({year:1984, month:11, day:11, hour:12, timezone: '+01:00'})] as dd
      RETURN date(dd) as d1,
             date({date: dd}) as d2,
             date({date: dd, year: 28}) as d3,
             date({date: dd, day: 28}) as d4,
             date({date: dd, week: 1}) as d5,
             date({date: dd, ordinalDay: 28}) as d6,
             date({date: dd, quarter: 3}) as d7
      """
    Then the result should be, in order:
      | d1           | d2           | d3           | d4           | d5           | d6           | d7           |
      | '1984-11-11' | '1984-11-11' | '0028-11-11' | '1984-11-28' | '1984-01-08' | '1984-01-28' | '1984-08-11' |
      | '1984-11-11' | '1984-11-11' | '0028-11-11' | '1984-11-28' | '1984-01-08' | '1984-01-28' | '1984-08-11' |
      | '1984-11-11' | '1984-11-11' | '0028-11-11' | '1984-11-28' | '1984-01-08' | '1984-01-28' | '1984-08-11' |
    And no side effects

  Scenario: Should select local time
    Given an empty graph
    When executing query:
      """
      UNWIND [localtime({hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:12, minute:31, second:14, microsecond: 645876, timezone: '+01:00'}),
              localdatetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645}),
              datetime({year:1984, month:10, day:11, hour:12, timezone: '+01:00'})] as dd
      RETURN localtime(dd) as d1,
             localtime({time:dd}) as d2,
             localtime({time:dd, second: 42}) as d3
      """
    Then the result should be, in order:
      | d1                   | d2                   | d3                   |
      | '12:31:14.645876123' | '12:31:14.645876123' | '12:31:42.645876123' |
      | '12:31:14.645876'    | '12:31:14.645876'    | '12:31:42.645876'    |
      | '12:31:14.645'       | '12:31:14.645'       | '12:31:42.645'       |
      | '12:00'              | '12:00'              | '12:00:42'           |
    And no side effects

  Scenario: Should select time
    Given an empty graph
    When executing query:
      """
      UNWIND [localtime({hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:12, minute:31, second:14, microsecond: 645876, timezone: '+01:00'}),
              localdatetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645}),
              datetime({year:1984, month:10, day:11, hour:12, timezone: 'Europe/Stockholm'})] as dd
      RETURN time(dd) as d1,
             time({time:dd}) as d2,
             time({time:dd, timezone:'+05:00'}) as d3,
             time({time:dd, second: 42}) as d4,
             time({time:dd, second: 42, timezone:'+05:00'}) as d5
      """
    Then the result should be, in order:
      | d1                      | d2                      | d3                           | d4                      | d5                           |
      | '12:31:14.645876123Z'   | '12:31:14.645876123Z'   | '12:31:14.645876123+05:00'   | '12:31:42.645876123Z'   | '12:31:42.645876123+05:00'   |
      | '12:31:14.645876+01:00' | '12:31:14.645876+01:00' | '16:31:14.645876+05:00'      | '12:31:42.645876+01:00' | '16:31:42.645876+05:00'      |
      | '12:31:14.645Z'         | '12:31:14.645Z'         | '12:31:14.645+05:00'         | '12:31:42.645Z'         | '12:31:42.645+05:00'         |
      | '12:00+01:00'           | '12:00+01:00'           | '16:00+05:00'                | '12:00:42+01:00'        | '16:00:42+05:00'             |
    And no side effects

  Scenario: Should select date into local date time
    Given an empty graph
    When executing query:
      """
      UNWIND [date({year:1984, month:10, day:11}),
              localdatetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645}),
              datetime({year:1984, month:10, day:11, hour:12, timezone: '+01:00'})] as dd
      RETURN localdatetime({date:dd, hour: 10, minute: 10, second: 10}) as d1,
             localdatetime({date:dd, day: 28, hour: 10, minute: 10, second: 10}) as d2
      """
    Then the result should be, in order:
      | d1                    | d2                    |
      | '1984-10-11T10:10:10' | '1984-10-28T10:10:10' |
      | '1984-03-07T10:10:10' | '1984-03-28T10:10:10' |
      | '1984-10-11T10:10:10' | '1984-10-28T10:10:10' |
    And no side effects

  Scenario: Should select time into local date time
    Given an empty graph
    When executing query:
      """
      UNWIND [localtime({hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:12, minute:31, second:14, microsecond: 645876, timezone: '+01:00'}),
              localdatetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645}),
              datetime({year:1984, month:10, day:11, hour:12, timezone: '+01:00'})] as tt
      RETURN localdatetime({year:1984, month:10, day:11, time:tt}) as d1,
             localdatetime({year:1984, month:10, day:11, time:tt, second: 42}) as d2
      """
    Then the result should be, in order:
      | d1                              | d2 |
      | '1984-10-11T12:31:14.645876123' | '1984-10-11T12:31:42.645876123' |
      | '1984-10-11T12:31:14.645876'    | '1984-10-11T12:31:42.645876' |
      | '1984-10-11T12:31:14.645'       | '1984-10-11T12:31:42.645' |
      | '1984-10-11T12:00'              | '1984-10-11T12:00:42' |
    And no side effects

  Scenario: Should select date and time into local date time
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
      RETURN localdatetime({date:dd, time:tt}) as d1,
             localdatetime({date:dd, time:tt, day: 28, second: 42}) as d2
      """
    Then the result should be, in order:
      | d1                              | d2 |
      | '1984-10-11T12:31:14.645876123' | '1984-10-28T12:31:42.645876123' |
      | '1984-10-11T12:31:14.645876'    | '1984-10-28T12:31:42.645876' |
      | '1984-10-11T12:31:14.645'       | '1984-10-28T12:31:42.645' |
      | '1984-10-11T12:00'              | '1984-10-28T12:00:42' |
      | '1984-03-07T12:31:14.645876123' | '1984-03-28T12:31:42.645876123' |
      | '1984-03-07T12:31:14.645876'    | '1984-03-28T12:31:42.645876' |
      | '1984-03-07T12:31:14.645'       | '1984-03-28T12:31:42.645' |
      | '1984-03-07T12:00'              | '1984-03-28T12:00:42' |
      | '1984-10-11T12:31:14.645876123' | '1984-10-28T12:31:42.645876123' |
      | '1984-10-11T12:31:14.645876'    | '1984-10-28T12:31:42.645876' |
      | '1984-10-11T12:31:14.645'       | '1984-10-28T12:31:42.645' |
      | '1984-10-11T12:00'              | '1984-10-28T12:00:42' |
    And no side effects

  Scenario: Should select datetime into local date time
    Given an empty graph
    When executing query:
      """
      UNWIND [localdatetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645}),
              datetime({year:1984, month:10, day:11, hour:12, timezone: '+01:00'})] as dd
      RETURN localdatetime(dd) as d1,
             localdatetime({datetime:dd}) as d2,
             localdatetime({datetime:dd, day: 28, second: 42}) as d3
      """
    Then the result should be, in order:
      | d1                        | d2                        | d3 |
      | '1984-03-07T12:31:14.645' | '1984-03-07T12:31:14.645' | '1984-03-28T12:31:42.645' |
      | '1984-10-11T12:00'        | '1984-10-11T12:00'        | '1984-10-28T12:00:42' |
    And no side effects

  Scenario: Should select date into date time
    Given an empty graph
    When executing query:
      """
      UNWIND [date({year:1984, month:10, day:11}),
              localdatetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645}),
              datetime({year:1984, month:10, day:11, hour:12, timezone: '+01:00'})] as dd
      RETURN datetime({date:dd, hour: 10, minute: 10, second: 10}) as d1,
             datetime({date:dd, hour: 10, minute: 10, second: 10, timezone:'+05:00'}) as d2,
             datetime({date:dd, day: 28, hour: 10, minute: 10, second: 10}) as d3,
             datetime({date:dd, day: 28, hour: 10, minute: 10, second: 10, timezone:'Pacific/Honolulu'}) as d4
      """
    Then the result should be, in order:
      | d1                     | d2                          | d3                     | d4                          |
      | '1984-10-11T10:10:10Z' | '1984-10-11T10:10:10+05:00' | '1984-10-28T10:10:10Z' | '1984-10-28T10:10:10-10:00[Pacific/Honolulu]' |
      | '1984-03-07T10:10:10Z' | '1984-03-07T10:10:10+05:00' | '1984-03-28T10:10:10Z' | '1984-03-28T10:10:10-10:00[Pacific/Honolulu]' |
      | '1984-10-11T10:10:10Z' | '1984-10-11T10:10:10+05:00' | '1984-10-28T10:10:10Z' | '1984-10-28T10:10:10-10:00[Pacific/Honolulu]' |
    And no side effects

  Scenario: Should select time into date time
    Given an empty graph
    When executing query:
      """
      UNWIND [localtime({hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:12, minute:31, second:14, microsecond: 645876, timezone: '+01:00'}),
              localdatetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645}),
              datetime({year:1984, month:10, day:11, hour:12, timezone: 'Europe/Stockholm'})] as tt
      RETURN datetime({year:1984, month:10, day:11, time:tt}) as d1,
             datetime({year:1984, month:10, day:11, time:tt, timezone:'+05:00'}) as d2,
             datetime({year:1984, month:10, day:11, time:tt, second: 42}) as d3,
             datetime({year:1984, month:10, day:11, time:tt, second: 42, timezone:'Pacific/Honolulu'}) as d4
      """
    Then the result should be, in order:
      | d1                                                  | d2                                    | d3                                                  | d4 |
      | '1984-10-11T12:31:14.645876123Z'                    | '1984-10-11T12:31:14.645876123+05:00' | '1984-10-11T12:31:42.645876123Z'                    | '1984-10-11T12:31:42.645876123-10:00[Pacific/Honolulu]' |
      | '1984-10-11T12:31:14.645876+01:00'                  | '1984-10-11T16:31:14.645876+05:00'    | '1984-10-11T12:31:42.645876+01:00'                  | '1984-10-11T01:31:42.645876-10:00[Pacific/Honolulu]' |
      | '1984-10-11T12:31:14.645Z'                          | '1984-10-11T12:31:14.645+05:00'       | '1984-10-11T12:31:42.645Z'                          | '1984-10-11T12:31:42.645-10:00[Pacific/Honolulu]' |
      | '1984-10-11T12:00+01:00[Europe/Stockholm]'          | '1984-10-11T16:00+05:00'              | '1984-10-11T12:00:42+01:00[Europe/Stockholm]'       | '1984-10-11T01:00:42-10:00[Pacific/Honolulu]' |
    And no side effects

  Scenario: Should select date and time into date time
    Given an empty graph
    When executing query:
      """
      UNWIND [date({year:1984, month:10, day:11}),
              localdatetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645}),
              datetime({year:1984, month:10, day:11, hour:12, timezone: '+01:00'})] as dd
      UNWIND [localtime({hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:12, minute:31, second:14, microsecond: 645876, timezone: '+01:00'}),
              localdatetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645}),
              datetime({year:1984, month:10, day:11, hour:12, timezone: 'Europe/Stockholm'})] as tt
      RETURN datetime({date:dd, time:tt}) as d1,
             datetime({date:dd, time:tt, timezone:'+05:00'}) as d2,
             datetime({date:dd, time:tt, day: 28, second: 42}) as d3,
             datetime({date:dd, time:tt, day: 28, second: 42, timezone:'Pacific/Honolulu'}) as d4
      """
    Then the result should be, in order:
      | d1                                         | d2                                    | d3                                            | d4 |
      | '1984-10-11T12:31:14.645876123Z'           | '1984-10-11T12:31:14.645876123+05:00' | '1984-10-28T12:31:42.645876123Z'              | '1984-10-28T12:31:42.645876123-10:00[Pacific/Honolulu]' |
      | '1984-10-11T12:31:14.645876+01:00'         | '1984-10-11T16:31:14.645876+05:00'    | '1984-10-28T12:31:42.645876+01:00'            | '1984-10-28T01:31:42.645876-10:00[Pacific/Honolulu]' |
      | '1984-10-11T12:31:14.645Z'                 | '1984-10-11T12:31:14.645+05:00'       | '1984-10-28T12:31:42.645Z'                    | '1984-10-28T12:31:42.645-10:00[Pacific/Honolulu]' |
      | '1984-10-11T12:00+01:00[Europe/Stockholm]' | '1984-10-11T16:00+05:00'              | '1984-10-28T12:00:42+01:00[Europe/Stockholm]' | '1984-10-28T01:00:42-10:00[Pacific/Honolulu]' |
      | '1984-03-07T12:31:14.645876123Z'           | '1984-03-07T12:31:14.645876123+05:00' | '1984-03-28T12:31:42.645876123Z'              | '1984-03-28T12:31:42.645876123-10:00[Pacific/Honolulu]' |
      | '1984-03-07T12:31:14.645876+01:00'         | '1984-03-07T16:31:14.645876+05:00'    | '1984-03-28T12:31:42.645876+01:00'            | '1984-03-28T01:31:42.645876-10:00[Pacific/Honolulu]' |
      | '1984-03-07T12:31:14.645Z'                 | '1984-03-07T12:31:14.645+05:00'       | '1984-03-28T12:31:42.645Z'                    | '1984-03-28T12:31:42.645-10:00[Pacific/Honolulu]' |
      | '1984-03-07T12:00+01:00[Europe/Stockholm]' | '1984-03-07T16:00+05:00'              | '1984-03-28T12:00:42+02:00[Europe/Stockholm]' | '1984-03-28T00:00:42-10:00[Pacific/Honolulu]' |
      | '1984-10-11T12:31:14.645876123Z'           | '1984-10-11T12:31:14.645876123+05:00' | '1984-10-28T12:31:42.645876123Z'              | '1984-10-28T12:31:42.645876123-10:00[Pacific/Honolulu]' |
      | '1984-10-11T12:31:14.645876+01:00'         | '1984-10-11T16:31:14.645876+05:00'    | '1984-10-28T12:31:42.645876+01:00'            | '1984-10-28T01:31:42.645876-10:00[Pacific/Honolulu]' |
      | '1984-10-11T12:31:14.645Z'                 | '1984-10-11T12:31:14.645+05:00'       | '1984-10-28T12:31:42.645Z'                    | '1984-10-28T12:31:42.645-10:00[Pacific/Honolulu]' |
      | '1984-10-11T12:00+01:00[Europe/Stockholm]' | '1984-10-11T16:00+05:00'              | '1984-10-28T12:00:42+01:00[Europe/Stockholm]' | '1984-10-28T01:00:42-10:00[Pacific/Honolulu]' |
    And no side effects

  Scenario: Should select datetime into date time
    Given an empty graph
    When executing query:
      """
      UNWIND [localdatetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645}),
              datetime({year:1984, month:10, day:11, hour:12, timezone: 'Europe/Stockholm'})] as dd
      RETURN datetime(dd) as d1,
             datetime({datetime:dd}) as d2,
             datetime({datetime:dd, timezone:'+05:00'}) as d3,
             datetime({datetime:dd, day: 28, second: 42}) as d4,
             datetime({datetime:dd, day: 28, second: 42, timezone:'Pacific/Honolulu'}) as d5

      """
    Then the result should be, in order:
      | d1                                         | d2                                         | d3                               | d4                                            | d5 |
      | '1984-03-07T12:31:14.645Z'                 | '1984-03-07T12:31:14.645Z'                 | '1984-03-07T12:31:14.645+05:00'  | '1984-03-28T12:31:42.645Z'                    | '1984-03-28T12:31:42.645-10:00[Pacific/Honolulu]' |
      | '1984-10-11T12:00+01:00[Europe/Stockholm]' | '1984-10-11T12:00+01:00[Europe/Stockholm]' | '1984-10-11T16:00+05:00'         | '1984-10-28T12:00:42+01:00[Europe/Stockholm]' | '1984-10-28T01:00:42-10:00[Pacific/Honolulu]' |
    And no side effects
