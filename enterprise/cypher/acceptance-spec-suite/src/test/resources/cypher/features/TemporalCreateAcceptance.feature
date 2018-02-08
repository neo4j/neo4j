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

Feature: TemporalCreateAcceptance

  Scenario: Should construct date
    Given an empty graph
    When executing query:
      """
      UNWIND [date({year:1984, month:10, day:11}),
              date({year:1984, month:10}),
              date({year:1984, week:10, dayOfWeek:3}),
              date({year:1984, week:10}),
              date({year:1984}),
              date({year:1984, ordinalDay:202}),
              date({year:1984, quarter:3, dayOfQuarter: 45}),
              date({year:1984, quarter:3})] as d
      RETURN d
      """
    Then the result should be, in order:
      | d            |
      | '1984-10-11' |
      | '1984-10-01' |
      | '1984-03-07' |
      | '1984-03-05' |
      | '1984-01-01' |
      | '1984-07-20' |
      | '1984-08-14' |
      | '1984-07-01' |
    And no side effects

  Scenario: Should construct local time
    Given an empty graph
    When executing query:
      """
      UNWIND [localtime({hour:12, minute:31, second:14, nanosecond: 645876123}),
              localtime({hour:12, minute:31, second:14, microsecond: 645876}),
              localtime({hour:12, minute:31, second:14, millisecond: 645}),
              localtime({hour:12, minute:31, second:14}),
              localtime({hour:12, minute:31}),
              localtime({hour:12})] as d
      RETURN d
      """
    Then the result should be, in order:
      | d                    |
      | '12:31:14.645876123' |
      | '12:31:14.645876'    |
      | '12:31:14.645'       |
      | '12:31:14'           |
      | '12:31'              |
      | '12:00'              |
    And no side effects

    # TODO timezone does not get through
  Scenario: Should construct time
    Given an empty graph
    When executing query:
      """
      UNWIND [time({hour:12, minute:31, second:14, nanosecond: 645876123}),
              time({hour:12, minute:31, second:14, microsecond: 645876}),
              time({hour:12, minute:31, second:14, millisecond: 645}),
              time({hour:12, minute:31, second:14}),
              time({hour:12, minute:31}),
              time({hour:12}),
              time({hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              time({hour:12, minute:31, second:14, microsecond: 645876, timezone: '+01:00'}),
              time({hour:12, minute:31, second:14, millisecond: 645, timezone: '+01:00'}),
              time({hour:12, minute:31, second:14, timezone: '+01:00'}),
              time({hour:12, minute:31, timezone: '+01:00'}),
              time({hour:12, timezone: '+01:00'}),
              time({hour:12, minute:31, second:14, nanosecond: 645876123, timezone: 'Europe/Stockholm'}),
              time({hour:12, minute:31, second:14, microsecond: 645876, timezone: 'Europe/Stockholm'}),
              time({hour:12, minute:31, second:14, millisecond: 645, timezone: 'Europe/Stockholm'}),
              time({hour:12, minute:31, second:14, timezone: 'Europe/Stockholm'}),
              time({hour:12, minute:31, timezone: 'Europe/Stockholm'}),
              time({hour:12, timezone: 'Europe/Stockholm'})] as d
      RETURN d
      """
    Then the result should be, in order:
      | d |
    And no side effects

  Scenario: Should construct local date time
    Given an empty graph
    When executing query:
      """
      UNWIND [localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, microsecond: 645876}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, millisecond: 645}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14}),
              localdatetime({year:1984, month:10, day:11, hour:12, minute:31}),
              localdatetime({year:1984, month:10, day:11, hour:12}),
              localdatetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, nanosecond: 645876123}),
              localdatetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, microsecond: 645876}),
              localdatetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645}),
              localdatetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14}),
              localdatetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31}),
              localdatetime({year:1984, week:10, dayOfWeek:3, hour:12}),
              localdatetime({year:1984, ordinalDay:202, hour:12, minute:31, second:14, nanosecond: 645876123}),
              localdatetime({year:1984, ordinalDay:202, hour:12, minute:31, second:14, microsecond: 645876}),
              localdatetime({year:1984, ordinalDay:202, hour:12, minute:31, second:14, millisecond: 645}),
              localdatetime({year:1984, ordinalDay:202, hour:12, minute:31, second:14}),
              localdatetime({year:1984, ordinalDay:202, hour:12, minute:31}),
              localdatetime({year:1984, ordinalDay:202, hour:12}),
              localdatetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12, minute:31, second:14, nanosecond: 645876123}),
              localdatetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12, minute:31, second:14, microsecond: 645876}),
              localdatetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12, minute:31, second:14, millisecond: 645}),
              localdatetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12, minute:31, second:14}),
              localdatetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12, minute:31}),
              localdatetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12})] as d
      RETURN d
      """
    Then the result should be, in order:
      | d |
    And no side effects

  Scenario: Should construct date time
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}),
              datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, microsecond: 645876}),
              datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, millisecond: 645}),
              datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14}),
              datetime({year:1984, month:10, day:11, hour:12, minute:31}),
              datetime({year:1984, month:10, day:11, hour:12}),
              datetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, nanosecond: 645876123}),
              datetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, microsecond: 645876}),
              datetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645}),
              datetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14}),
              datetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31}),
              datetime({year:1984, week:10, dayOfWeek:3, hour:12}),
              datetime({year:1984, ordinalDay:202, hour:12, minute:31, second:14, nanosecond: 645876123}),
              datetime({year:1984, ordinalDay:202, hour:12, minute:31, second:14, microsecond: 645876}),
              datetime({year:1984, ordinalDay:202, hour:12, minute:31, second:14, millisecond: 645}),
              datetime({year:1984, ordinalDay:202, hour:12, minute:31, second:14}),
              datetime({year:1984, ordinalDay:202, hour:12, minute:31}),
              datetime({year:1984, ordinalDay:202, hour:12}),
              datetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12, minute:31, second:14, nanosecond: 645876123}),
              datetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12, minute:31, second:14, microsecond: 645876}),
              datetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12, minute:31, second:14, millisecond: 645}),
              datetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12, minute:31, second:14}),
              datetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12, minute:31}),
              datetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12}),

              datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, microsecond: 645876, timezone: '+01:00'}),
              datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, millisecond: 645, timezone: '+01:00'}),
              datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, timezone: '+01:00'}),
              datetime({year:1984, month:10, day:11, hour:12, minute:31, timezone: '+01:00'}),
              datetime({year:1984, month:10, day:11, hour:12, timezone: '+01:00'}),
              datetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              datetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, microsecond: 645876, timezone: '+01:00'}),
              datetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645, timezone: '+01:00'}),
              datetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, timezone: '+01:00'}),
              datetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, timezone: '+01:00'}),
              datetime({year:1984, week:10, dayOfWeek:3, hour:12, timezone: '+01:00'}),
              datetime({year:1984, ordinalDay:202, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              datetime({year:1984, ordinalDay:202, hour:12, minute:31, second:14, microsecond: 645876, timezone: '+01:00'}),
              datetime({year:1984, ordinalDay:202, hour:12, minute:31, second:14, millisecond: 645, timezone: '+01:00'}),
              datetime({year:1984, ordinalDay:202, hour:12, minute:31, second:14, timezone: '+01:00'}),
              datetime({year:1984, ordinalDay:202, hour:12, minute:31, timezone: '+01:00'}),
              datetime({year:1984, ordinalDay:202, hour:12, timezone: '+01:00'}),
              datetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}),
              datetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12, minute:31, second:14, microsecond: 645876, timezone: '+01:00'}),
              datetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12, minute:31, second:14, millisecond: 645, timezone: '+01:00'}),
              datetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12, minute:31, second:14, timezone: '+01:00'}),
              datetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12, minute:31, timezone: '+01:00'}),
              datetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12, timezone: '+01:00'}),

              datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: 'Europe/Stockholm'}),
              datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, microsecond: 645876, timezone: 'Europe/Stockholm'}),
              datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, millisecond: 645, timezone: 'Europe/Stockholm'}),
              datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, timezone: 'Europe/Stockholm'}),
              datetime({year:1984, month:10, day:11, hour:12, minute:31, timezone: 'Europe/Stockholm'}),
              datetime({year:1984, month:10, day:11, hour:12, timezone: 'Europe/Stockholm'}),
              datetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: 'Europe/Stockholm'}),
              datetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, microsecond: 645876, timezone: 'Europe/Stockholm'}),
              datetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645, timezone: 'Europe/Stockholm'}),
              datetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, timezone: 'Europe/Stockholm'}),
              datetime({year:1984, week:10, dayOfWeek:3, hour:12, minute:31, timezone: 'Europe/Stockholm'}),
              datetime({year:1984, week:10, dayOfWeek:3, hour:12, timezone: 'Europe/Stockholm'}),
              datetime({year:1984, ordinalDay:202, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: 'Europe/Stockholm'}),
              datetime({year:1984, ordinalDay:202, hour:12, minute:31, second:14, microsecond: 645876, timezone: 'Europe/Stockholm'}),
              datetime({year:1984, ordinalDay:202, hour:12, minute:31, second:14, millisecond: 645, timezone: 'Europe/Stockholm'}),
              datetime({year:1984, ordinalDay:202, hour:12, minute:31, second:14, timezone: 'Europe/Stockholm'}),
              datetime({year:1984, ordinalDay:202, hour:12, minute:31, timezone: 'Europe/Stockholm'}),
              datetime({year:1984, ordinalDay:202, hour:12, timezone: 'Europe/Stockholm'}),
              datetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: 'Europe/Stockholm'}),
              datetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12, minute:31, second:14, microsecond: 645876, timezone: 'Europe/Stockholm'}),
              datetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12, minute:31, second:14, millisecond: 645, timezone: 'Europe/Stockholm'}),
              datetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12, minute:31, second:14, timezone: 'Europe/Stockholm'}),
              datetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12, minute:31, timezone: 'Europe/Stockholm'}),
              datetime({year:1984, quarter:3, dayOfQuarter: 45, hour:12, timezone: 'Europe/Stockholm'}),

              datetime.fromepoch(416779,999999999),
              datetime.fromepochmillis(237821673987)] as d
      RETURN d
      """
    Then the result should be, in order:
      | d |
    And no side effects

  Scenario: Should create duration
    Given an empty graph
    When executing query:
      """
      UNWIND [duration({days: 14, hours:16, minutes: 12}),
              duration({months: 5, days: 1.5}),
              duration({months: 0.75}),
              duration({weeks: 2.5}),
              duration({years: 12, months:5, days: 14, hours:16, minutes: 12, seconds: 70}),
              duration({days: 14, seconds: 70, millisecond: 1}),
              duration({days: 14, seconds: 70, microseconds: 1}),
              duration({days: 14, seconds: 70, nanoseconds: 1})] as d
      RETURN d
      """
    Then the result should be, in order:
      | d |
    And no side effects
