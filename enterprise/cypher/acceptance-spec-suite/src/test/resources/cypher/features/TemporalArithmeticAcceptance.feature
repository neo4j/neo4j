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
    When executing query:
      """
      WITH date({year:1984, month:10, day:11}) as x
      UNWIND [duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 123456789}),
              duration({months:1, days: -14, hours: 16, minutes: -12, seconds: 70}),
              duration({years: 12.5, months: 5.5, days: 14.5, hours: 16.5, minutes: 12.5, seconds: 70.5, nanoseconds: 123456789})] as dur
      RETURN x+dur, x-dur
      """
    Then the result should be, in order:
      | d            |
    And no side effects

  Scenario: Should add or subtract duration to or from local time
    Given an empty graph
    When executing query:
      """
      WITH localtime({hour:12, minute:31, second:14, nanosecond: 645876123}) as x
      UNWIND [duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 123456789}),
              duration({months:1, days: -14, hours: 16, minutes: -12, seconds: 70}),
              duration({years: 12.5, months: 5.5, days: 14.5, hours: 16.5, minutes: 12.5, seconds: 70.5, nanoseconds: 123456789})] as dur
      RETURN x+dur, x-dur
      """
    Then the result should be, in order:
      | d            |
    And no side effects

  Scenario: Should add or subtract duration to or from time
    Given an empty graph
    When executing query:
      """
      WITH time({hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}) as x
      UNWIND [duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 123456789}),
              duration({months:1, days: -14, hours: 16, minutes: -12, seconds: 70}),
              duration({years: 12.5, months: 5.5, days: 14.5, hours: 16.5, minutes: 12.5, seconds: 70.5, nanoseconds: 123456789})] as dur
      RETURN x+dur, x-dur
      """
    Then the result should be, in order:
      | d            |
    And no side effects

  Scenario: Should add or subtract duration to or from local date time
    Given an empty graph
    When executing query:
      """
      WITH localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123}) as x
      UNWIND [duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 123456789}),
              duration({months:1, days: -14, hours: 16, minutes: -12, seconds: 70}),
              duration({years: 12.5, months: 5.5, days: 14.5, hours: 16.5, minutes: 12.5, seconds: 70.5, nanoseconds: 123456789})] as dur
      RETURN x+dur, x-dur
      """
    Then the result should be, in order:
      | d            |

  Scenario: Should add or subtract duration to or from date time
    Given an empty graph
    When executing query:
      """
      WITH datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond: 645876123, timezone: '+01:00'}) as x
      UNWIND [duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 123456789}),
              duration({months:1, days: -14, hours: 16, minutes: -12, seconds: 70}),
              duration({years: 12.5, months: 5.5, days: 14.5, hours: 16.5, minutes: 12.5, seconds: 70.5, nanoseconds: 123456789})] as dur
      RETURN x+dur, x-dur
      """
    Then the result should be, in order:
      | d            |

  Scenario: Should add or subtract durations
    Given an empty graph
    When executing query:
      """
      UNWIND [duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 123456789}),
              duration({months:1, days: -14, hours: 16, minutes: -12, seconds: 70}),
              duration({years: 12.5, months: 5.5, days: 14.5, hours: 16.5, minutes: 12.5, seconds: 70.5, nanoseconds: 123456789})] as dur
      UNWIND [duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 123456789}),
              duration({months:1, days: -14, hours: 16, minutes: -12, seconds: 70}),
              duration({years: 12.5, months: 5.5, days: 14.5, hours: 16.5, minutes: 12.5, seconds: 70.5, nanoseconds: 123456789})] as dur2
      RETURN dur+dur2, dur-dur2, dur2-dur
      """
    Then the result should be, in order:
      | d            |

  Scenario: Should multiply or divide durations by numbers
    Given an empty graph
    When executing query:
      """
      UNWIND [duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 123456789}),
              duration({months:1, days: -14, hours: 16, minutes: -12, seconds: 70}),
              duration({years: 12.5, months: 5.5, days: 14.5, hours: 16.5, minutes: 12.5, seconds: 70.5, nanoseconds: 123456789})] as dur
      UNWIND [1, 1.5, 2, 10, 0.5] as num
      RETURN dur*num, dur/num
      """
    Then the result should be, in order:
      | d            |