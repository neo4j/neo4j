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

Feature: DurationBetweenAcceptance

    # TODO the commented lines should work too

  Scenario: Should compute duration between two temporals
    Given an empty graph
    When executing query:
      """
      UNWIND [duration.between(date("1984-10-11"), date("2015-06-24")),
              duration.between(date("1984-10-11"), localdatetime("2016-07-21T21:45:22.142")),
              duration.between(date("1984-10-11"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.between(date("1984-10-11"), localtime("16:30")),
              duration.between(date("1984-10-11"), time("16:30+0100")),

              duration.between(localtime("14:30"), date("2015-06-24")),
              duration.between(localtime("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.between(localtime("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.between(localtime("14:30"), localtime("16:30")),
              duration.between(localtime("14:30"), time("16:30+0100")),

              duration.between(time("14:30"), date("2015-06-24")),
              //duration.between(time("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.between(time("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.between(time("14:30"), localtime("16:30")),
              duration.between(time("14:30"), time("16:30+0100")),

              //duration.between(localdatetime("2015-07-21T21:40:32.142"), date("2015-06-24")),
              duration.between(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
              duration.between(localdatetime("2015-07-21T21:40:32.142"), datetime("2015-07-21T21:40:32.142+0100")),
              //duration.between(localdatetime("2015-07-21T21:40:32.142"), localtime("16:30")),
              //duration.between(localdatetime("2015-07-21T21:40:32.142"), time("16:30+0100")),

              //duration.between(datetime("2014-07-21T21:40:36.143+0200"), date("2015-06-24")),
              duration.between(datetime("2014-07-21T21:40:36.143+0200"), localdatetime("2016-07-21T21:45:22.142")),
              duration.between(datetime("2014-07-21T21:40:36.143+0200"), datetime("2015-07-21T21:40:32.142+0100"))
              //duration.between(datetime("2014-07-21T21:40:36.143+0200"), localtime("16:30")),
              //duration.between(datetime("2014-07-21T21:40:36.143+0200"), time("16:30+0100"))
              ] as d
      RETURN d
      """
    Then the result should be, in order:
      | d |
    And no side effects

  Scenario: Should compute duration between two temporals in years
    Given an empty graph
    When executing query:
      """
      UNWIND [duration.years(date("1984-10-11"), date("2015-06-24")),
              duration.years(date("1984-10-11"), localdatetime("2016-07-21T21:45:22.142")),
              duration.years(date("1984-10-11"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.years(date("1984-10-11"), localtime("16:30")),
              duration.years(date("1984-10-11"), time("16:30+0100")),

              duration.years(localtime("14:30"), date("2015-06-24")),
              duration.years(localtime("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.years(localtime("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.years(localtime("14:30"), localtime("16:30")),
              duration.years(localtime("14:30"), time("16:30+0100")),

              duration.years(time("14:30"), date("2015-06-24")),
              //duration.years(time("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.years(time("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.years(time("14:30"), localtime("16:30")),
              duration.years(time("14:30"), time("16:30+0100")),

              //duration.years(localdatetime("2015-07-21T21:40:32.142"), date("2015-06-24")),
              duration.years(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
              duration.years(localdatetime("2015-07-21T21:40:32.142"), datetime("2015-07-21T21:40:32.142+0100")),
              //duration.years(localdatetime("2015-07-21T21:40:32.142"), localtime("16:30")),
              //duration.years(localdatetime("2015-07-21T21:40:32.142"), time("16:30+0100")),

              //duration.years(datetime("2014-07-21T21:40:36.143+0200"), date("2015-06-24")),
              duration.years(datetime("2014-07-21T21:40:36.143+0200"), localdatetime("2016-07-21T21:45:22.142")),
              duration.years(datetime("2014-07-21T21:40:36.143+0200"), datetime("2015-07-21T21:40:32.142+0100"))
              //duration.years(datetime("2014-07-21T21:40:36.143+0200"), localtime("16:30")),
              //duration.years(datetime("2014-07-21T21:40:36.143+0200"), time("16:30+0100"))
              ] as d
      RETURN d
      """
    Then the result should be, in order:
      | d |
    And no side effects

  Scenario: Should compute duration between two temporals in quarters
    Given an empty graph
    When executing query:
      """
      UNWIND [duration.quarters(date("1984-10-11"), date("2015-06-24")),
              duration.quarters(date("1984-10-11"), localdatetime("2016-07-21T21:45:22.142")),
              duration.quarters(date("1984-10-11"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.quarters(date("1984-10-11"), localtime("16:30")),
              duration.quarters(date("1984-10-11"), time("16:30+0100")),

              duration.quarters(localtime("14:30"), date("2015-06-24")),
              duration.quarters(localtime("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.quarters(localtime("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.quarters(localtime("14:30"), localtime("16:30")),
              duration.quarters(localtime("14:30"), time("16:30+0100")),

              duration.quarters(time("14:30"), date("2015-06-24")),
              //duration.quarters(time("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.quarters(time("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.quarters(time("14:30"), localtime("16:30")),
              duration.quarters(time("14:30"), time("16:30+0100")),

              //duration.quarters(localdatetime("2015-07-21T21:40:32.142"), date("2015-06-24")),
              duration.quarters(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
              duration.quarters(localdatetime("2015-07-21T21:40:32.142"), datetime("2015-07-21T21:40:32.142+0100")),
              //duration.quarters(localdatetime("2015-07-21T21:40:32.142"), localtime("16:30")),
              //duration.quarters(localdatetime("2015-07-21T21:40:32.142"), time("16:30+0100")),

              //duration.quarters(datetime("2014-07-21T21:40:36.143+0200"), date("2015-06-24")),
              duration.quarters(datetime("2014-07-21T21:40:36.143+0200"), localdatetime("2016-07-21T21:45:22.142")),
              duration.quarters(datetime("2014-07-21T21:40:36.143+0200"), datetime("2015-07-21T21:40:32.142+0100"))
              //duration.quarters(datetime("2014-07-21T21:40:36.143+0200"), localtime("16:30")),
              //duration.quarters(datetime("2014-07-21T21:40:36.143+0200"), time("16:30+0100"))
              ] as d
      RETURN d
      """
    Then the result should be, in order:
      | d |
    And no side effects

  Scenario: Should compute duration between two temporals in months
    Given an empty graph
    When executing query:
      """
      UNWIND [duration.months(date("1984-10-11"), date("2015-06-24")),
              duration.months(date("1984-10-11"), localdatetime("2016-07-21T21:45:22.142")),
              duration.months(date("1984-10-11"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.months(date("1984-10-11"), localtime("16:30")),
              duration.months(date("1984-10-11"), time("16:30+0100")),

              duration.months(localtime("14:30"), date("2015-06-24")),
              duration.months(localtime("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.months(localtime("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.months(localtime("14:30"), localtime("16:30")),
              duration.months(localtime("14:30"), time("16:30+0100")),

              duration.months(time("14:30"), date("2015-06-24")),
              //duration.months(time("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.months(time("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.months(time("14:30"), localtime("16:30")),
              duration.months(time("14:30"), time("16:30+0100")),

              //duration.months(localdatetime("2015-07-21T21:40:32.142"), date("2015-06-24")),
              duration.months(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
              duration.months(localdatetime("2015-07-21T21:40:32.142"), datetime("2015-07-21T21:40:32.142+0100")),
              //duration.months(localdatetime("2015-07-21T21:40:32.142"), localtime("16:30")),
              //duration.months(localdatetime("2015-07-21T21:40:32.142"), time("16:30+0100")),

              //duration.months(datetime("2014-07-21T21:40:36.143+0200"), date("2015-06-24")),
              duration.months(datetime("2014-07-21T21:40:36.143+0200"), localdatetime("2016-07-21T21:45:22.142")),
              duration.months(datetime("2014-07-21T21:40:36.143+0200"), datetime("2015-07-21T21:40:32.142+0100"))
              //duration.months(datetime("2014-07-21T21:40:36.143+0200"), localtime("16:30")),
              //duration.months(datetime("2014-07-21T21:40:36.143+0200"), time("16:30+0100"))
              ] as d
      RETURN d
      """
    Then the result should be, in order:
      | d |
    And no side effects

  Scenario: Should compute duration between two temporals in weeks
    Given an empty graph
    When executing query:
      """
      UNWIND [duration.weeks(date("1984-10-11"), date("2015-06-24")),
              duration.weeks(date("1984-10-11"), localdatetime("2016-07-21T21:45:22.142")),
              duration.weeks(date("1984-10-11"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.weeks(date("1984-10-11"), localtime("16:30")),
              duration.weeks(date("1984-10-11"), time("16:30+0100")),

              duration.weeks(localtime("14:30"), date("2015-06-24")),
              duration.weeks(localtime("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.weeks(localtime("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.weeks(localtime("14:30"), localtime("16:30")),
              duration.weeks(localtime("14:30"), time("16:30+0100")),

              duration.weeks(time("14:30"), date("2015-06-24")),
              //duration.weeks(time("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.weeks(time("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.weeks(time("14:30"), localtime("16:30")),
              duration.weeks(time("14:30"), time("16:30+0100")),

              //duration.weeks(localdatetime("2015-07-21T21:40:32.142"), date("2015-06-24")),
              duration.weeks(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
              duration.weeks(localdatetime("2015-07-21T21:40:32.142"), datetime("2015-07-21T21:40:32.142+0100")),
              //duration.weeks(localdatetime("2015-07-21T21:40:32.142"), localtime("16:30")),
              //duration.weeks(localdatetime("2015-07-21T21:40:32.142"), time("16:30+0100")),

              //duration.weeks(datetime("2014-07-21T21:40:36.143+0200"), date("2015-06-24")),
              duration.weeks(datetime("2014-07-21T21:40:36.143+0200"), localdatetime("2016-07-21T21:45:22.142")),
              duration.weeks(datetime("2014-07-21T21:40:36.143+0200"), datetime("2015-07-21T21:40:32.142+0100"))
              //duration.weeks(datetime("2014-07-21T21:40:36.143+0200"), localtime("16:30")),
              //duration.weeks(datetime("2014-07-21T21:40:36.143+0200"), time("16:30+0100"))
              ] as d
      RETURN d
      """
    Then the result should be, in order:
      | d |
    And no side effects

  Scenario: Should compute duration between two temporals in days
    Given an empty graph
    When executing query:
      """
      UNWIND [duration.days(date("1984-10-11"), date("2015-06-24")),
              duration.days(date("1984-10-11"), localdatetime("2016-07-21T21:45:22.142")),
              duration.days(date("1984-10-11"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.days(date("1984-10-11"), localtime("16:30")),
              duration.days(date("1984-10-11"), time("16:30+0100")),

              duration.days(localtime("14:30"), date("2015-06-24")),
              duration.days(localtime("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.days(localtime("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.days(localtime("14:30"), localtime("16:30")),
              duration.days(localtime("14:30"), time("16:30+0100")),

              duration.days(time("14:30"), date("2015-06-24")),
              //duration.days(time("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.days(time("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.days(time("14:30"), localtime("16:30")),
              duration.days(time("14:30"), time("16:30+0100")),

              //duration.days(localdatetime("2015-07-21T21:40:32.142"), date("2015-06-24")),
              duration.days(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
              duration.days(localdatetime("2015-07-21T21:40:32.142"), datetime("2015-07-21T21:40:32.142+0100")),
              //duration.days(localdatetime("2015-07-21T21:40:32.142"), localtime("16:30")),
              //duration.days(localdatetime("2015-07-21T21:40:32.142"), time("16:30+0100")),

              //duration.days(datetime("2014-07-21T21:40:36.143+0200"), date("2015-06-24")),
              duration.days(datetime("2014-07-21T21:40:36.143+0200"), localdatetime("2016-07-21T21:45:22.142")),
              duration.days(datetime("2014-07-21T21:40:36.143+0200"), datetime("2015-07-21T21:40:32.142+0100"))
              //duration.days(datetime("2014-07-21T21:40:36.143+0200"), localtime("16:30")),
              //duration.days(datetime("2014-07-21T21:40:36.143+0200"), time("16:30+0100"))
              ] as d
      RETURN d
      """
    Then the result should be, in order:
      | d |
    And no side effects

  Scenario: Should compute duration between two temporals in hours
    Given an empty graph
    When executing query:
      """
      UNWIND [duration.hours(date("1984-10-11"), date("2015-06-24")),
              duration.hours(date("1984-10-11"), localdatetime("2016-07-21T21:45:22.142")),
              duration.hours(date("1984-10-11"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.hours(date("1984-10-11"), localtime("16:30")),
              duration.hours(date("1984-10-11"), time("16:30+0100")),

              duration.hours(localtime("14:30"), date("2015-06-24")),
              duration.hours(localtime("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.hours(localtime("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.hours(localtime("14:30"), localtime("16:30")),
              duration.hours(localtime("14:30"), time("16:30+0100")),

              duration.hours(time("14:30"), date("2015-06-24")),
              //duration.hours(time("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.hours(time("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.hours(time("14:30"), localtime("16:30")),
              duration.hours(time("14:30"), time("16:30+0100")),

              //duration.hours(localdatetime("2015-07-21T21:40:32.142"), date("2015-06-24")),
              duration.hours(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
              duration.hours(localdatetime("2015-07-21T21:40:32.142"), datetime("2015-07-21T21:40:32.142+0100")),
              //duration.hours(localdatetime("2015-07-21T21:40:32.142"), localtime("16:30")),
              //duration.hours(localdatetime("2015-07-21T21:40:32.142"), time("16:30+0100")),

              //duration.hours(datetime("2014-07-21T21:40:36.143+0200"), date("2015-06-24")),
              duration.hours(datetime("2014-07-21T21:40:36.143+0200"), localdatetime("2016-07-21T21:45:22.142")),
              duration.hours(datetime("2014-07-21T21:40:36.143+0200"), datetime("2015-07-21T21:40:32.142+0100"))
              //duration.hours(datetime("2014-07-21T21:40:36.143+0200"), localtime("16:30")),
              //duration.hours(datetime("2014-07-21T21:40:36.143+0200"), time("16:30+0100"))
              ] as d
      RETURN d
      """
    Then the result should be, in order:
      | d |
    And no side effects

  Scenario: Should compute duration between two temporals in minutes
    Given an empty graph
    When executing query:
      """
      UNWIND [duration.minutes(date("1984-10-11"), date("2015-06-24")),
              duration.minutes(date("1984-10-11"), localdatetime("2016-07-21T21:45:22.142")),
              duration.minutes(date("1984-10-11"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.minutes(date("1984-10-11"), localtime("16:30")),
              duration.minutes(date("1984-10-11"), time("16:30+0100")),

              duration.minutes(localtime("14:30"), date("2015-06-24")),
              duration.minutes(localtime("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.minutes(localtime("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.minutes(localtime("14:30"), localtime("16:30")),
              duration.minutes(localtime("14:30"), time("16:30+0100")),

              duration.minutes(time("14:30"), date("2015-06-24")),
              //duration.minutes(time("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.minutes(time("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.minutes(time("14:30"), localtime("16:30")),
              duration.minutes(time("14:30"), time("16:30+0100")),

              //duration.minutes(localdatetime("2015-07-21T21:40:32.142"), date("2015-06-24")),
              duration.minutes(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
              duration.minutes(localdatetime("2015-07-21T21:40:32.142"), datetime("2015-07-21T21:40:32.142+0100")),
              //duration.minutes(localdatetime("2015-07-21T21:40:32.142"), localtime("16:30")),
              //duration.minutes(localdatetime("2015-07-21T21:40:32.142"), time("16:30+0100")),

              //duration.minutes(datetime("2014-07-21T21:40:36.143+0200"), date("2015-06-24")),
              duration.minutes(datetime("2014-07-21T21:40:36.143+0200"), localdatetime("2016-07-21T21:45:22.142")),
              duration.minutes(datetime("2014-07-21T21:40:36.143+0200"), datetime("2015-07-21T21:40:32.142+0100"))
              //duration.minutes(datetime("2014-07-21T21:40:36.143+0200"), localtime("16:30")),
              //duration.minutes(datetime("2014-07-21T21:40:36.143+0200"), time("16:30+0100"))
              ] as d
      RETURN d
      """
    Then the result should be, in order:
      | d |
    And no side effects

  Scenario: Should compute duration between two temporals in seconds
    Given an empty graph
    When executing query:
      """
      UNWIND [duration.seconds(date("1984-10-11"), date("2015-06-24")),
              duration.seconds(date("1984-10-11"), localdatetime("2016-07-21T21:45:22.142")),
              duration.seconds(date("1984-10-11"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.seconds(date("1984-10-11"), localtime("16:30")),
              duration.seconds(date("1984-10-11"), time("16:30+0100")),

              duration.seconds(localtime("14:30"), date("2015-06-24")),
              duration.seconds(localtime("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.seconds(localtime("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.seconds(localtime("14:30"), localtime("16:30")),
              duration.seconds(localtime("14:30"), time("16:30+0100")),

              duration.seconds(time("14:30"), date("2015-06-24")),
              //duration.seconds(time("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.seconds(time("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.seconds(time("14:30"), localtime("16:30")),
              duration.seconds(time("14:30"), time("16:30+0100")),

              //duration.seconds(localdatetime("2015-07-21T21:40:32.142"), date("2015-06-24")),
              duration.seconds(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
              duration.seconds(localdatetime("2015-07-21T21:40:32.142"), datetime("2015-07-21T21:40:32.142+0100")),
              //duration.seconds(localdatetime("2015-07-21T21:40:32.142"), localtime("16:30")),
              //duration.seconds(localdatetime("2015-07-21T21:40:32.142"), time("16:30+0100")),

              //duration.seconds(datetime("2014-07-21T21:40:36.143+0200"), date("2015-06-24")),
              duration.seconds(datetime("2014-07-21T21:40:36.143+0200"), localdatetime("2016-07-21T21:45:22.142")),
              duration.seconds(datetime("2014-07-21T21:40:36.143+0200"), datetime("2015-07-21T21:40:32.142+0100"))
              //duration.seconds(datetime("2014-07-21T21:40:36.143+0200"), localtime("16:30")),
              //duration.seconds(datetime("2014-07-21T21:40:36.143+0200"), time("16:30+0100"))
              ] as d
      RETURN d
      """
    Then the result should be, in order:
      | d |
    And no side effects
