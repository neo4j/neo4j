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
              duration.between(time("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.between(time("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.between(time("14:30"), localtime("16:30")),
              duration.between(time("14:30"), time("16:30+0100")),

              duration.between(localdatetime("2015-07-21T21:40:32.142"), date("2015-06-24")),
              duration.between(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
              duration.between(localdatetime("2015-07-21T21:40:32.142"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.between(localdatetime("2015-07-21T21:40:32.142"), localtime("16:30")),
              duration.between(localdatetime("2015-07-21T21:40:32.142"), time("16:30+0100")),

              duration.between(datetime("2014-07-21T21:40:36.143+0200"), date("2015-06-24")),
              duration.between(datetime("2014-07-21T21:40:36.143+0200"), localdatetime("2016-07-21T21:45:22.142")),
              duration.between(datetime("2014-07-21T21:40:36.143+0200"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.between(datetime("2014-07-21T21:40:36.143+0200"), localtime("16:30")),
              duration.between(datetime("2014-07-21T21:40:36.143+0200"), time("16:30+0100"))
              ] as d
      RETURN d
      """
    Then the result should be, in order:
      | d |
      | 'P30Y8M13D' |
      | 'P31Y9M10DT21H45M22.142S' |
      | 'P30Y9M10DT21H40M32.142S' |
      | 'PT16H30M' |
      | 'PT16H30M' |
      | 'PT-14H-30M' |
      | 'PT7H15M22.142S' |
      | 'PT7H10M32.142S' |
      | 'PT2H' |
      | 'PT2H' |
      | 'PT-14H-30M' |
      | 'PT7H15M22.142S' |
      | 'PT6H10M32.142S' |
      | 'PT2H' |
      | 'PT1H' |
      | 'P-27DT-21H-40M-32.142S' |
      | 'P1YT4M50S' |
      | 'PT0S' |
      | 'PT-5H-10M-32.142S' |
      | 'PT-5H-10M-32.142S' |
      | 'P11M3DT-21H-40M-36.143S' |
      | 'P2YT4M45.999S' |
      | 'P1YT59M55.999S' |
      | 'PT-5H-10M-36.143S' |
      | 'PT-4H-10M-36.143S' |
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

              duration.years(time("14:30"), date("2015-06-24")),
              duration.years(time("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.years(time("14:30"), datetime("2015-07-21T21:40:32.142+0100")),

              duration.years(localdatetime("2015-07-21T21:40:32.142"), date("2015-06-24")),
              duration.years(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
              duration.years(localdatetime("2015-07-21T21:40:32.142"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.years(localdatetime("2015-07-21T21:40:32.142"), localtime("16:30")),
              duration.years(localdatetime("2015-07-21T21:40:32.142"), time("16:30+0100")),

              duration.years(datetime("2014-07-21T21:40:36.143+0200"), date("2015-06-24")),
              duration.years(datetime("2014-07-21T21:40:36.143+0200"), localdatetime("2016-07-21T21:45:22.142")),
              duration.years(datetime("2014-07-21T21:40:36.143+0200"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.years(datetime("2014-07-21T21:40:36.143+0200"), localtime("16:30")),
              duration.years(datetime("2014-07-21T21:40:36.143+0200"), time("16:30+0100"))
              ] as d
      RETURN d
      """
    Then the result should be, in order:
      | d |
      | 'P30Y' |
      | 'P31Y' |
      | 'P30Y' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'P1Y' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'P2Y' |
      | 'P1Y' |
      | 'PT0S' |
      | 'PT0S' |
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

              duration.quarters(time("14:30"), date("2015-06-24")),
              duration.quarters(time("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.quarters(time("14:30"), datetime("2015-07-21T21:40:32.142+0100")),

              duration.quarters(localdatetime("2015-07-21T21:40:32.142"), date("2015-06-24")),
              duration.quarters(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
              duration.quarters(localdatetime("2015-07-21T21:40:32.142"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.quarters(localdatetime("2015-07-21T21:40:32.142"), localtime("16:30")),
              duration.quarters(localdatetime("2015-07-21T21:40:32.142"), time("16:30+0100")),

              duration.quarters(datetime("2014-07-21T21:40:36.143+0200"), date("2015-06-24")),
              duration.quarters(datetime("2014-07-21T21:40:36.143+0200"), localdatetime("2016-07-21T21:45:22.142")),
              duration.quarters(datetime("2014-07-21T21:40:36.143+0200"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.quarters(datetime("2014-07-21T21:40:36.143+0200"), localtime("16:30")),
              duration.quarters(datetime("2014-07-21T21:40:36.143+0200"), time("16:30+0100"))
              ] as d
      RETURN d
      """
    Then the result should be, in order:
      | d |
      | 'P30Y6M' |
      | 'P31Y9M' |
      | 'P30Y9M' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'P1Y' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'P9M' |
      | 'P2Y' |
      | 'P1Y' |
      | 'PT0S' |
      | 'PT0S' |
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

              duration.months(time("14:30"), date("2015-06-24")),
              duration.months(time("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.months(time("14:30"), datetime("2015-07-21T21:40:32.142+0100")),

              duration.months(localdatetime("2015-07-21T21:40:32.142"), date("2015-06-24")),
              duration.months(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
              duration.months(localdatetime("2015-07-21T21:40:32.142"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.months(localdatetime("2015-07-21T21:40:32.142"), localtime("16:30")),
              duration.months(localdatetime("2015-07-21T21:40:32.142"), time("16:30+0100")),

              duration.months(datetime("2014-07-21T21:40:36.143+0200"), date("2015-06-24")),
              duration.months(datetime("2014-07-21T21:40:36.143+0200"), localdatetime("2016-07-21T21:45:22.142")),
              duration.months(datetime("2014-07-21T21:40:36.143+0200"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.months(datetime("2014-07-21T21:40:36.143+0200"), localtime("16:30")),
              duration.months(datetime("2014-07-21T21:40:36.143+0200"), time("16:30+0100"))
              ] as d
      RETURN d
      """
    Then the result should be, in order:
      | d |
      | 'P30Y8M' |
      | 'P31Y9M' |
      | 'P30Y9M' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'P1Y' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'P11M' |
      | 'P2Y' |
      | 'P1Y' |
      | 'PT0S' |
      | 'PT0S' |
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

              duration.weeks(time("14:30"), date("2015-06-24")),
              duration.weeks(time("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.weeks(time("14:30"), datetime("2015-07-21T21:40:32.142+0100")),

              duration.weeks(localdatetime("2015-07-21T21:40:32.142"), date("2015-06-24")),
              duration.weeks(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
              duration.weeks(localdatetime("2015-07-21T21:40:32.142"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.weeks(localdatetime("2015-07-21T21:40:32.142"), localtime("16:30")),
              duration.weeks(localdatetime("2015-07-21T21:40:32.142"), time("16:30+0100")),

              duration.weeks(datetime("2014-07-21T21:40:36.143+0200"), date("2015-06-24")),
              duration.weeks(datetime("2014-07-21T21:40:36.143+0200"), localdatetime("2016-07-21T21:45:22.142")),
              duration.weeks(datetime("2014-07-21T21:40:36.143+0200"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.weeks(datetime("2014-07-21T21:40:36.143+0200"), localtime("16:30")),
              duration.weeks(datetime("2014-07-21T21:40:36.143+0200"), time("16:30+0100"))
              ] as d
      RETURN d
      """
    Then the result should be, in order:
      | d |
      | 'P11207D' |
      | 'P11606D' |
      | 'P11235D' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'P-21D' |
      | 'P364D' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'P336D' |
      | 'P728D' |
      | 'P364D' |
      | 'PT0S' |
      | 'PT0S' |
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

              duration.days(time("14:30"), date("2015-06-24")),
              duration.days(time("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.days(time("14:30"), datetime("2015-07-21T21:40:32.142+0100")),

              duration.days(localdatetime("2015-07-21T21:40:32.142"), date("2015-06-24")),
              duration.days(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
              duration.days(localdatetime("2015-07-21T21:40:32.142"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.days(localdatetime("2015-07-21T21:40:32.142"), localtime("16:30")),
              duration.days(localdatetime("2015-07-21T21:40:32.142"), time("16:30+0100")),

              duration.days(datetime("2014-07-21T21:40:36.143+0200"), date("2015-06-24")),
              duration.days(datetime("2014-07-21T21:40:36.143+0200"), localdatetime("2016-07-21T21:45:22.142")),
              duration.days(datetime("2014-07-21T21:40:36.143+0200"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.days(datetime("2014-07-21T21:40:36.143+0200"), localtime("16:30")),
              duration.days(datetime("2014-07-21T21:40:36.143+0200"), time("16:30+0100"))
              ] as d
      RETURN d
      """
    Then the result should be, in order:
      | d |
      | 'P11213D' |
      | 'P11606D' |
      | 'P11240D' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'P-27D' |
      | 'P366D' |
      | 'PT0S' |
      | 'PT0S' |
      | 'PT0S' |
      | 'P337D' |
      | 'P731D' |
      | 'P365D' |
      | 'PT0S' |
      | 'PT0S' |
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
              duration.hours(time("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.hours(time("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.hours(time("14:30"), localtime("16:30")),
              duration.hours(time("14:30"), time("16:30+0100")),

              duration.hours(localdatetime("2015-07-21T21:40:32.142"), date("2015-06-24")),
              duration.hours(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
              duration.hours(localdatetime("2015-07-21T21:40:32.142"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.hours(localdatetime("2015-07-21T21:40:32.142"), localtime("16:30")),
              duration.hours(localdatetime("2015-07-21T21:40:32.142"), time("16:30+0100")),

              duration.hours(datetime("2014-07-21T21:40:36.143+0200"), date("2015-06-24")),
              duration.hours(datetime("2014-07-21T21:40:36.143+0200"), localdatetime("2016-07-21T21:45:22.142")),
              duration.hours(datetime("2014-07-21T21:40:36.143+0200"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.hours(datetime("2014-07-21T21:40:36.143+0200"), localtime("16:30")),
              duration.hours(datetime("2014-07-21T21:40:36.143+0200"), time("16:30+0100"))
              ] as d
      RETURN d
      """
    Then the result should be, in order:
      | d |
      | 'PT269112H'  |
      | 'PT278565H' |
      | 'PT269781H' |
      | 'PT16H' |
      | 'PT16H' |
      | 'PT-14H' |
      | 'PT7H' |
      | 'PT7H' |
      | 'PT2H' |
      | 'PT2H' |
      | 'PT-14H' |
      | 'PT7H' |
      | 'PT6H' |
      | 'PT2H' |
      | 'PT1H' |
      | 'PT-669H' |
      | 'PT8784H' |
      | 'PT0S' |
      | 'PT-5H' |
      | 'PT-5H' |
      | 'PT8090H' |
      | 'PT17544H' |
      | 'PT8760H' |
      | 'PT-5H' |
      | 'PT-4H' |
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
              duration.minutes(time("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.minutes(time("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.minutes(time("14:30"), localtime("16:30")),
              duration.minutes(time("14:30"), time("16:30+0100")),

              duration.minutes(localdatetime("2015-07-21T21:40:32.142"), date("2015-06-24")),
              duration.minutes(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
              duration.minutes(localdatetime("2015-07-21T21:40:32.142"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.minutes(localdatetime("2015-07-21T21:40:32.142"), localtime("16:30")),
              duration.minutes(localdatetime("2015-07-21T21:40:32.142"), time("16:30+0100")),

              duration.minutes(datetime("2014-07-21T21:40:36.143+0200"), date("2015-06-24")),
              duration.minutes(datetime("2014-07-21T21:40:36.143+0200"), localdatetime("2016-07-21T21:45:22.142")),
              duration.minutes(datetime("2014-07-21T21:40:36.143+0200"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.minutes(datetime("2014-07-21T21:40:36.143+0200"), localtime("16:30")),
              duration.minutes(datetime("2014-07-21T21:40:36.143+0200"), time("16:30+0100"))
              ] as d
      RETURN d
      """
    Then the result should be, in order:
      | d |
      | 'PT269112H'  |
      | 'PT278565H45M' |
      | 'PT269781H40M' |
      | 'PT16H30M' |
      | 'PT16H30M' |
      | 'PT-14H-30M' |
      | 'PT7H15M' |
      | 'PT7H10M' |
      | 'PT2H' |
      | 'PT2H' |
      | 'PT-14H-30M' |
      | 'PT7H15M' |
      | 'PT6H10M' |
      | 'PT2H' |
      | 'PT1H' |
      | 'PT-669H-40M' |
      | 'PT8784H4M' |
      | 'PT0S' |
      | 'PT-5H-10M' |
      | 'PT-5H-10M' |
      | 'PT8090H19M' |
      | 'PT17544H4M' |
      | 'PT8760H59M' |
      | 'PT-5H-10M' |
      | 'PT-4H-10M' |
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
              duration.seconds(time("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.seconds(time("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.seconds(time("14:30"), localtime("16:30")),
              duration.seconds(time("14:30"), time("16:30+0100")),

              duration.seconds(localdatetime("2015-07-21T21:40:32.142"), date("2015-06-24")),
              duration.seconds(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
              duration.seconds(localdatetime("2015-07-21T21:40:32.142"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.seconds(localdatetime("2015-07-21T21:40:32.142"), localtime("16:30")),
              duration.seconds(localdatetime("2015-07-21T21:40:32.142"), time("16:30+0100")),

              duration.seconds(datetime("2014-07-21T21:40:36.143+0200"), date("2015-06-24")),
              duration.seconds(datetime("2014-07-21T21:40:36.143+0200"), localdatetime("2016-07-21T21:45:22.142")),
              duration.seconds(datetime("2014-07-21T21:40:36.143+0200"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.seconds(datetime("2014-07-21T21:40:36.143+0200"), localtime("16:30")),
              duration.seconds(datetime("2014-07-21T21:40:36.143+0200"), time("16:30+0100"))
              ] as d
      RETURN d
      """
    Then the result should be, in order:
      | d |
      | 'PT269112H' |
      | 'PT278565H45M22.142S' |
      | 'PT269781H40M32.142S' |
      | 'PT16H30M' |
      | 'PT16H30M' |
      | 'PT-14H-30M' |
      | 'PT7H15M22.142S' |
      | 'PT7H10M32.142S' |
      | 'PT2H' |
      | 'PT2H' |
      | 'PT-14H-30M' |
      | 'PT7H15M22.142S' |
      | 'PT6H10M32.142S' |
      | 'PT2H' |
      | 'PT1H' |
      | 'PT-669H-40M-32.142S' |
      | 'PT8784H4M50S' |
      | 'PT0S' |
      | 'PT-5H-10M-32.142S' |
      | 'PT-5H-10M-32.142S' |
      | 'PT8090H19M23.857S' |
      | 'PT17544H4M45.999S' |
      | 'PT8760H59M55.999S' |
      | 'PT-5H-10M-36.143S' |
      | 'PT-4H-10M-36.143S' |
    And no side effects

  Scenario: Should compute duration between if they differ only by a fraction of a second and the first comes after the second.
    Given an empty graph
    When executing query:
      """
      RETURN duration.seconds(localdatetime("2014-07-21T21:40:36.143"), localdatetime("2014-07-21T21:40:36.142")) as d
      """
    Then the result should be, in order:
      | d |
      | 'PT-0.001S' |
    And no side effects

  Scenario: Should handle durations at daylight saving time day
    Given an empty graph
    When executing query:
    """
    UNWIND[ duration.hours(datetime({year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/Stockholm'}), localdatetime({year: 2017, month: 10, day: 29, hour: 4})),
            duration.between(datetime({year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/Stockholm'}), localdatetime({year: 2017, month: 10, day: 29, hour: 4})),
            duration.hours(datetime({year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/Stockholm'}), localtime({hour: 4})),
            duration.between(datetime({year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/Stockholm'}), localtime({hour: 4})),
            duration.hours(localdatetime({year: 2017, month: 10, day: 29, hour: 0 }), datetime({year: 2017, month: 10, day: 29, hour: 4, timezone: 'Europe/Stockholm'})),
            duration.between(localdatetime({year: 2017, month: 10, day: 29, hour: 0 }), datetime({year: 2017, month: 10, day: 29, hour: 4, timezone: 'Europe/Stockholm'})),
            duration.hours(localtime({hour: 0 }), datetime({year: 2017, month: 10, day: 29, hour: 4, timezone: 'Europe/Stockholm'})),
            duration.between(localtime({hour: 0 }), datetime({year: 2017, month: 10, day: 29, hour: 4, timezone: 'Europe/Stockholm'})),
            duration.hours(date({year: 2017, month: 10, day: 29}), datetime({year: 2017, month: 10, day: 29, hour: 4, timezone: 'Europe/Stockholm'})),
            duration.between(date({year: 2017, month: 10, day: 29}), datetime({year: 2017, month: 10, day: 29, hour: 4, timezone: 'Europe/Stockholm'})),
            duration.hours(datetime({year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/Stockholm'}), date({year: 2017, month: 10, day: 30}))
          ] as d
    RETURN d
    """
    Then the result should be, in order:
      | d      |
      | 'PT5H' |
      | 'PT5H' |
      | 'PT5H' |
      | 'PT5H' |
      | 'PT5H' |
      | 'PT5H' |
      | 'PT5H' |
      | 'PT5H' |
      | 'PT5H' |
      | 'PT5H' |
      | 'PT25H' |
    And no side effects

  Scenario: Should handle large durations
    Given an empty graph
    When executing query:
    """
    UNWIND[ duration.between(date("0001-01-01"), date("2001-01-01")),
            duration.seconds(date("0001-01-01"), date("2001-01-01"))
            ] as d
    RETURN d
    """
    Then the result should be, in order:
      | d      |
      | 'P2000Y' |
      | 'PT17531640H' |
    And no side effects
