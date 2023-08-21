#
# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

#encoding: utf-8

Feature: TernaryComparisonAcceptance

  Scenario Outline: List comparison test
    And parameters are:
      | lhs | <lhs> |
      | rhs | <rhs> |
    When executing query:
      """
      RETURN $lhs < $rhs AS lt, $lhs <= $rhs AS lte, $lhs > $rhs AS gt, $lhs >= $rhs AS gte
      """
    Then the result should be, in any order:
      | lt   | lte   | gt   | gte   |
      | <lt> | <lte> | <gt> | <gte> |
    And no side effects

    Examples:
      | lhs       | rhs         | lt    | lte  | gt    | gte   |
      | [1, 2, 4] | [1, 3, 4]   | true  | true | false | false |
      | [1, 2, 4] | [1, 2, 4]   | false | true | false | true  |
      | [1, 2, 4] | 'string'    | null  | null | null  | null  |
      | [1, 2]    | [1, null]   | null  | null | null  | null  |
      | [1, 2]    | [2, null]   | true  | true | false | false |

  Scenario Outline: Map comparison test
    And parameters are:
      | lhs | <lhs> |
      | rhs | <rhs> |
    When executing query:
      """
      RETURN $lhs < $rhs AS lt, $lhs <= $rhs AS lte, $lhs > $rhs AS gt, $lhs >= $rhs AS gte
      """
    Then the result should be, in any order:
      | lt   | lte   | gt   | gte   |
      | <lt> | <lte> | <gt> | <gte> |
    And no side effects

    Examples:
      | lhs       | rhs         | lt    | lte  | gt    | gte   |
      | {k: 42}   | {k: 42}    | false | true | false | true  |
      | {k: 42}   | {k: 43}    | true  | true | false | false |
      | {k1: 42}  | {k2: 42}   | true  | true | false | false |
      | {k: 42}   | {k: null}  | null  | null | null  | null  |
      | {k1: 42}  | {k2: null} | true  | true | false | false |
      | {k1: 42}  | 42         | null  | null | null  | null  |

  Scenario Outline: Spatial comparison test
    When executing query:
      """
      WITH point(<map1>) AS p1, point(<map2>) AS p2
      RETURN p1 < p2 AS lt, p1 <= p2 AS lte, p1 > p2 AS gt, p1 >= p2 AS gte
      """
    Then the result should be, in any order:
      | lt   | lte   | gt   | gte   |
      | <lt> | <lte> | <gt> | <gte> |
    And no side effects

    Examples:
      | map1         | map2                        | lt   | lte  | gt   | gte  |
      | {x: 1, y: 2} | {x: 1, y: 2}                | null | true | null | true |
      | {x: 1, y: 2} | {x: 2, y: 3}                | null | null | null | null |
      | {x: 1, y: 2} | {x: 2, y: 2}                | null | null | null | null |
      | {x: 1, y: 2} | {x: 1, y: 2, crs: 'wgs-84'} | null | null | null | null |
      | {x: 1, y: 2} | null                        | null | null | null | null |
      | {x: 1, y: 2} | {x: 1, y: null}             | null | null | null | null |

  Scenario: Duration comparison test of equal durations
    Given an empty graph
    And having executed:
      """
      CREATE (n:L {d: duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70})})
      """
    When executing query:
      """
      MATCH (n:L)
      RETURN n.d < n.d AS lt, n.d <= n.d AS lte, n.d > n.d AS gt, n.d >= n.d AS gte
      """
    Then the result should be, in any order:
      | lt   | lte   | gt    | gte  |
      | null | true | null | true |
    And no side effects

  Scenario: Duration comparison test of different durations
    Given an empty graph
    And having executed:
      """
      CREATE (n:L {d1: duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70}),
                   d2: duration({years: 12, months: 7, days: 14, hours: 16, minutes: 12, seconds: 70}) })
      """
    When executing query:
      """
      MATCH (n:L)
      RETURN n.d1 < n.d2 AS lt, n.d1 <= n.d2 AS lte, n.d1 > n.d2 AS gt, n.d1 >= n.d2 AS gte
      """
    Then the result should be, in any order:
      | lt   | lte   | gt   | gte  |
      | null | null  | null | null |
    And no side effects


