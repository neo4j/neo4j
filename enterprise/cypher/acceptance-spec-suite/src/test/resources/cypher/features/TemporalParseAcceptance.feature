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

Feature: TemporalParseAcceptance

  Scenario: Should parse date from string
    Given an empty graph
    When executing query:
      """
      UNWIND [date('2015-07-21'),
              date('20150721'),
              date('2015-07'),
              date('201507'),
              date('2015-W30-2'),
              date('2015W302'),
              date('2015-W30'),
              date('2015W30'),
              date('2015-202'),
              date('2015202'),
              date('2015')] as d
      RETURN d
      """
    Then the result should be, in order:
      | d            |
      | '2015-07-21' |
      | '2015-07-21' |
      | '2015-07-01' |
      | '2015-07-01' |
      | '2015-07-21' |
      | '2015-07-21' |
      | '2015-07-20' |
      | '2015-07-20' |
      | '2015-07-21' |
      | '2015-07-21' |
      | '2015-01-01' |
    And no side effects

  Scenario: Should parse local time from string
    Given an empty graph
    When executing query:
      """
      UNWIND [localtime('21:40:32.142'),
              localtime('214032.142'),
              localtime('21:40:32'),
              localtime('214032'),
              localtime('21:40'),
              localtime('2140'),
              localtime('21')] as d
      RETURN d
      """
    Then the result should be, in order:
      | d              |
      | '21:40:32.142' |
      | '21:40:32.142' |
      | '21:40:32'     |
      | '21:40:32'     |
      | '21:40'        |
      | '21:40'        |
      | '21:00'        |
    And no side effects

  Scenario: Should parse time from string
    Given an empty graph
    When executing query:
      """
      UNWIND [time('21:40:32.142+0100'),
              time('214032.142Z'),
              time('21:40:32+01:00'),
              time('214032-0100'),
              time('21:40-01:30'),
              time('2140-00:00'),
              time('2140-02'),
              time('22+18:00')] as d
      RETURN d
      """
    Then the result should be, in order:
      | d                    |
      | '21:40:32.142+01:00' |
      | '21:40:32.142Z'      |
      | '21:40:32+01:00'     |
      | '21:40:32-01:00'     |
      | '21:40-01:30'        |
      | '21:40Z'             |
      | '21:40-02:00'        |
      | '22:00+18:00'        |
    And no side effects

  Scenario: Should parse local date time from string
    Given an empty graph
    When executing query:
      """
      UNWIND [localdatetime('2015-07-21T21:40:32.142'),
              localdatetime('2015-W30-2T214032.142'),
              localdatetime('2015-202T21:40:32'),
              localdatetime('2015T214032'),
              localdatetime('20150721T21:40'),
              localdatetime('2015-W30T2140'),
              localdatetime('2015202T21')] as d
      RETURN d
      """
    Then the result should be, in order:
      | d                         |
      | '2015-07-21T21:40:32.142' |
      | '2015-07-21T21:40:32.142' |
      | '2015-07-21T21:40:32'     |
      | '2015-01-01T21:40:32'     |
      | '2015-07-21T21:40'        |
      | '2015-07-20T21:40'        |
      | '2015-07-21T21:00'        |
    And no side effects

  Scenario: Should parse date time from string
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime('2015-07-21T21:40:32.142+0100'),
              datetime('2015-W30-2T214032.142Z'),
              datetime('2015-202T21:40:32+01:00'),
              datetime('2015T214032-0100'),
              datetime('20150721T21:40-01:30'),
              datetime('2015-W30T2140-00:00'),
              datetime('2015-W30T2140-02'),
              datetime('2015202T21+18:00')] as d
      RETURN d
      """
    Then the result should be, in order:
      | d                               |
      | '2015-07-21T21:40:32.142+01:00' |
      | '2015-07-21T21:40:32.142Z'      |
      | '2015-07-21T21:40:32+01:00'     |
      | '2015-01-01T21:40:32-01:00'     |
      | '2015-07-21T21:40-01:30'        |
      | '2015-07-20T21:40Z'             |
      | '2015-07-20T21:40-02:00'        |
      | '2015-07-21T21:00+18:00'        |
    And no side effects

  Scenario: Should parse date time with named time zone from string
    Given an empty graph
    When executing query:
      """
      UNWIND [datetime('2015-07-21T21:40:32.142+02:00[Europe/Stockholm]'),
              datetime('2015-07-21T21:40:32.142+0845[Australia/Eucla]'),
              datetime('2015-07-21T21:40:32.142-04[America/New_York]'),
              datetime('2015-07-21T21:40:32.142[Europe/London]'),
              datetime('1818-07-21T21:40:32.142[Europe/Stockholm]')
             ] as d
      RETURN d
      """
    Then the result should be, in order:
      | d                                                 |
      | '2015-07-21T21:40:32.142+02:00[Europe/Stockholm]'    |
      | '2015-07-21T21:40:32.142+08:45[Australia/Eucla]'     |
      | '2015-07-21T21:40:32.142-04:00[America/New_York]'    |
      | '2015-07-21T21:40:32.142+01:00[Europe/London]'       |
      | '1818-07-21T21:40:32.142+01:12:12[Europe/Stockholm]' |
    And no side effects

  Scenario: Should parse duration from string
    Given an empty graph
    When executing query:
      """
      UNWIND [duration("P14DT16H12M"),
              duration("P5M1.5D"),
              duration("P0.75M"),
              duration("PT0.75M"),
              duration("P2.5W"),
              duration("P12Y5M14DT16H12M70S"),
              duration("P2012-02-02T14:37:21.545")] as d
      RETURN d
      """
    Then the result should be, in order:
      | d                          |
      | 'P14DT16H12M'              |
      | 'P5M1DT12H'                |
      | 'P22DT19H51M49.5S'         |
      | 'PT45S'                    |
      | 'P17DT12H'                 |
      | 'P12Y5M14DT16H13M10S'      |
      | 'P2012Y2M2DT14H37M21.545S' |
    And no side effects
