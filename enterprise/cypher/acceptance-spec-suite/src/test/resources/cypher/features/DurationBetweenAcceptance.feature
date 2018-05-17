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

  Scenario: Should compute duration between two temporals in months
    Given an empty graph
    When executing query:
      """
      UNWIND [duration.inMonths(date("1984-10-11"), date("2015-06-24")),
              duration.inMonths(date("1984-10-11"), localdatetime("2016-07-21T21:45:22.142")),
              duration.inMonths(date("1984-10-11"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.inMonths(date("1984-10-11"), localtime("16:30")),
              duration.inMonths(date("1984-10-11"), time("16:30+0100")),

              duration.inMonths(localtime("14:30"), date("2015-06-24")),
              duration.inMonths(localtime("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.inMonths(localtime("14:30"), datetime("2015-07-21T21:40:32.142+0100")),

              duration.inMonths(time("14:30"), date("2015-06-24")),
              duration.inMonths(time("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.inMonths(time("14:30"), datetime("2015-07-21T21:40:32.142+0100")),

              duration.inMonths(localdatetime("2015-07-21T21:40:32.142"), date("2015-06-24")),
              duration.inMonths(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
              duration.inMonths(localdatetime("2015-07-21T21:40:32.142"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.inMonths(localdatetime("2015-07-21T21:40:32.142"), localtime("16:30")),
              duration.inMonths(localdatetime("2015-07-21T21:40:32.142"), time("16:30+0100")),

              duration.inMonths(datetime("2014-07-21T21:40:36.143+0200"), date("2015-06-24")),
              duration.inMonths(datetime("2014-07-21T21:40:36.143+0200"), localdatetime("2016-07-21T21:45:22.142")),
              duration.inMonths(datetime("2014-07-21T21:40:36.143+0200"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.inMonths(datetime("2014-07-21T21:40:36.143+0200"), localtime("16:30")),
              duration.inMonths(datetime("2014-07-21T21:40:36.143+0200"), time("16:30+0100"))
              ] as d
      RETURN d
      """
    Then the result should be, in order:
      | d        |
      | 'P30Y8M' |
      | 'P31Y9M' |
      | 'P30Y9M' |
      | 'PT0S'   |
      | 'PT0S'   |
      | 'PT0S'   |
      | 'PT0S'   |
      | 'PT0S'   |
      | 'PT0S'   |
      | 'PT0S'   |
      | 'PT0S'   |
      | 'PT0S'   |
      | 'P1Y'    |
      | 'PT0S'   |
      | 'PT0S'   |
      | 'PT0S'   |
      | 'P11M'   |
      | 'P2Y'    |
      | 'P1Y'    |
      | 'PT0S'   |
      | 'PT0S'   |

    And no side effects

  Scenario: Should compute duration between two temporals in days
    Given an empty graph
    When executing query:
      """
      UNWIND [duration.inDays(date("1984-10-11"), date("2015-06-24")),
              duration.inDays(date("1984-10-11"), localdatetime("2016-07-21T21:45:22.142")),
              duration.inDays(date("1984-10-11"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.inDays(date("1984-10-11"), localtime("16:30")),
              duration.inDays(date("1984-10-11"), time("16:30+0100")),

              duration.inDays(localtime("14:30"), date("2015-06-24")),
              duration.inDays(localtime("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.inDays(localtime("14:30"), datetime("2015-07-21T21:40:32.142+0100")),

              duration.inDays(time("14:30"), date("2015-06-24")),
              duration.inDays(time("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.inDays(time("14:30"), datetime("2015-07-21T21:40:32.142+0100")),

              duration.inDays(localdatetime("2015-07-21T21:40:32.142"), date("2015-06-24")),
              duration.inDays(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
              duration.inDays(localdatetime("2015-07-21T21:40:32.142"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.inDays(localdatetime("2015-07-21T21:40:32.142"), localtime("16:30")),
              duration.inDays(localdatetime("2015-07-21T21:40:32.142"), time("16:30+0100")),

              duration.inDays(datetime("2014-07-21T21:40:36.143+0200"), date("2015-06-24")),
              duration.inDays(datetime("2014-07-21T21:40:36.143+0200"), localdatetime("2016-07-21T21:45:22.142")),
              duration.inDays(datetime("2014-07-21T21:40:36.143+0200"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.inDays(datetime("2014-07-21T21:40:36.143+0200"), localtime("16:30")),
              duration.inDays(datetime("2014-07-21T21:40:36.143+0200"), time("16:30+0100"))
              ] as d
      RETURN d
      """
    Then the result should be, in order:
      | d |
      | 'P11213D' |
      | 'P11606D' |
      | 'P11240D' |
      | 'PT0S'    |
      | 'PT0S'    |
      | 'PT0S'    |
      | 'PT0S'    |
      | 'PT0S'    |
      | 'PT0S'    |
      | 'PT0S'    |
      | 'PT0S'    |
      | 'P-27D'   |
      | 'P366D'   |
      | 'PT0S'    |
      | 'PT0S'    |
      | 'PT0S'    |
      | 'P337D'   |
      | 'P731D'   |
      | 'P365D'   |
      | 'PT0S'    |
      | 'PT0S'    |
    And no side effects

  Scenario: Should compute duration between two temporals in seconds
    Given an empty graph
    When executing query:
      """
      UNWIND [duration.inSeconds(date("1984-10-11"), date("2015-06-24")),
              duration.inSeconds(date("1984-10-11"), localdatetime("2016-07-21T21:45:22.142")),
              duration.inSeconds(date("1984-10-11"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.inSeconds(date("1984-10-11"), localtime("16:30")),
              duration.inSeconds(date("1984-10-11"), time("16:30+0100")),

              duration.inSeconds(localtime("14:30"), date("2015-06-24")),
              duration.inSeconds(localtime("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.inSeconds(localtime("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.inSeconds(localtime("14:30"), localtime("16:30")),
              duration.inSeconds(localtime("14:30"), time("16:30+0100")),

              duration.inSeconds(time("14:30"), date("2015-06-24")),
              duration.inSeconds(time("14:30"), localdatetime("2016-07-21T21:45:22.142")),
              duration.inSeconds(time("14:30"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.inSeconds(time("14:30"), localtime("16:30")),
              duration.inSeconds(time("14:30"), time("16:30+0100")),

              duration.inSeconds(localdatetime("2015-07-21T21:40:32.142"), date("2015-06-24")),
              duration.inSeconds(localdatetime("2015-07-21T21:40:32.142"), localdatetime("2016-07-21T21:45:22.142")),
              duration.inSeconds(localdatetime("2015-07-21T21:40:32.142"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.inSeconds(localdatetime("2015-07-21T21:40:32.142"), localtime("16:30")),
              duration.inSeconds(localdatetime("2015-07-21T21:40:32.142"), time("16:30+0100")),

              duration.inSeconds(datetime("2014-07-21T21:40:36.143+0200"), date("2015-06-24")),
              duration.inSeconds(datetime("2014-07-21T21:40:36.143+0200"), localdatetime("2016-07-21T21:45:22.142")),
              duration.inSeconds(datetime("2014-07-21T21:40:36.143+0200"), datetime("2015-07-21T21:40:32.142+0100")),
              duration.inSeconds(datetime("2014-07-21T21:40:36.143+0200"), localtime("16:30")),
              duration.inSeconds(datetime("2014-07-21T21:40:36.143+0200"), time("16:30+0100"))
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
      RETURN duration.inSeconds(localdatetime("2014-07-21T21:40:36.143"), localdatetime("2014-07-21T21:40:36.142")) as d
      """
    Then the result should be, in order:
      | d |
      | 'PT-0.001S' |
    And no side effects

  Scenario: Should compute negative duration between in big units
    Given an empty graph
    When executing query:
      """
      WITH [[date("2018-03-11"), date("2016-06-24")],
              [date("2018-07-21"), datetime("2016-07-21T21:40:32.142+0100")],
              [localdatetime("2018-07-21T21:40:32.142"), date("2016-07-21")],
              [datetime("2018-07-21T21:40:36.143+0200"), localdatetime("2016-07-21T21:40:36.143")],
              [datetime("2018-07-21T21:40:36.143+0500"), datetime("1984-07-21T22:40:36.143+0200")]] as temporalCombos
      UNWIND temporalCombos as pair
      WITH pair[0] as first, pair[1] as second
      RETURN duration.inMonths(first, second) as months
      """
    Then the result should be, in order:
      | months      |
      | 'P-1Y-8M'   |
      | 'P-1Y-11M'  |
      | 'P-2Y'      |
      | 'P-2Y'      |
      | 'P-33Y-11M' |

    And no side effects

  Scenario: Should handle durations at daylight saving time day
    Given an empty graph
    When executing query:
    """
    UNWIND[ duration.inSeconds(datetime({year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/Stockholm'}), localdatetime({year: 2017, month: 10, day: 29, hour: 4})),
            duration.between(datetime({year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/Stockholm'}), localdatetime({year: 2017, month: 10, day: 29, hour: 4})),
            duration.inSeconds(datetime({year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/Stockholm'}), localtime({hour: 4})),
            duration.between(datetime({year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/Stockholm'}), localtime({hour: 4})),
            duration.inSeconds(localdatetime({year: 2017, month: 10, day: 29, hour: 0 }), datetime({year: 2017, month: 10, day: 29, hour: 4, timezone: 'Europe/Stockholm'})),
            duration.between(localdatetime({year: 2017, month: 10, day: 29, hour: 0 }), datetime({year: 2017, month: 10, day: 29, hour: 4, timezone: 'Europe/Stockholm'})),
            duration.inSeconds(localtime({hour: 0 }), datetime({year: 2017, month: 10, day: 29, hour: 4, timezone: 'Europe/Stockholm'})),
            duration.between(localtime({hour: 0 }), datetime({year: 2017, month: 10, day: 29, hour: 4, timezone: 'Europe/Stockholm'})),
            duration.inSeconds(date({year: 2017, month: 10, day: 29}), datetime({year: 2017, month: 10, day: 29, hour: 4, timezone: 'Europe/Stockholm'})),
            duration.between(date({year: 2017, month: 10, day: 29}), datetime({year: 2017, month: 10, day: 29, hour: 4, timezone: 'Europe/Stockholm'})),
            duration.inSeconds(datetime({year: 2017, month: 10, day: 29, hour: 0, timezone: 'Europe/Stockholm'}), date({year: 2017, month: 10, day: 30}))
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
    UNWIND[ duration.between(date("-999999999-01-01"), date("+999999999-12-31")),
            duration.inSeconds(localdatetime("-999999999-01-01"), localdatetime("+999999999-12-31T23:59:59"))
            ] as d
    RETURN d
    """
    Then the result should be, in order:
      | d      |
      | 'P1999999998Y11M30D' |
      | 'PT17531639991215H59M59S' |
    And no side effects

  Scenario: Should handle when seconds and subseconds have different signs
    Given an empty graph
    When executing query:
    """
    UNWIND[ duration.inSeconds(localtime("12:34:54.7"), localtime("12:34:54.3")),
            duration.inSeconds(localtime("12:34:54.3"), localtime("12:34:54.7")),

            duration.inSeconds(localtime("12:34:54.7"), localtime("12:34:55.3")),
            duration.inSeconds(localtime("12:34:54.7"), localtime("12:44:55.3")),
            duration.inSeconds(localtime("12:44:54.7"), localtime("12:34:55.3")),

            duration.inSeconds(localtime("12:34:56"), localtime("12:34:55.7")),
            duration.inSeconds(localtime("12:34:56"), localtime("12:44:55.7")),
            duration.inSeconds(localtime("12:44:56"), localtime("12:34:55.7")),

            duration.inSeconds(localtime("12:34:56.3"), localtime("12:34:54.7")),
            duration.inSeconds(localtime("12:34:54.7"), localtime("12:34:56.3"))
          ] as d
    RETURN d
    """
    Then the result should be, in order:
      | d             |
      | 'PT-0.4S'     |
      | 'PT0.4S'      |
      | 'PT0.6S'      |
      | 'PT10M0.6S'   |
      | 'PT-9M-59.4S' |
      | 'PT-0.3S'     |
      | 'PT9M59.7S'   |
      | 'PT-10M-0.3S' |
      | 'PT-1.6S'     |
      | 'PT1.6S'      |
    And no side effects

  Scenario: Should compute durations with no difference
    Given an empty graph
    When executing query:
    """
    UNWIND[ duration.inSeconds(localtime(), localtime()),
            duration.inSeconds(time(), time()),
            duration.inSeconds(date(), date()),
            duration.inSeconds(localdatetime(), localdatetime()),
            duration.inSeconds(datetime(), datetime())
          ] as d
    RETURN d
    """
    Then the result should be, in order:
      | d             |
      | 'PT0S'        |
      | 'PT0S'        |
      | 'PT0S'        |
      | 'PT0S'        |
      | 'PT0S'        |
    And no side effects
