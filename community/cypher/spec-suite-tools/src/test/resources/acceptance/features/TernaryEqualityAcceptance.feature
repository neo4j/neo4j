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

Feature: TernaryEqualityAcceptance

  Scenario Outline: equality between different types should yield false
    And parameters are:
      | lhs | <lhs> |
      | rhs | <rhs> |
    When executing query:
      """
      RETURN $lhs = $rhs AS eq, $lhs <> $rhs AS neq
      """
    Then the result should be, in any order:
      | eq   | neq   |
      | <eq> | <neq> |
    And no side effects

    Examples:
      | lhs        | rhs                | eq   | neq  |
      | [1, 2, 4]  | 'string'           | false | true |
      | {k: 'foo'} | 1                  | false | true |
      | 1          | 'string'           | false | true |
      | true       | 'string'           | false | true |

  Scenario: List equality [1, 2, 3] and [1, null, 3]
    And parameters are:
      | lhs | [1, 2, 3]   |
      | rhs | [1, null, 3] |
    When executing query:
      """
      RETURN $lhs = $rhs AS eq, $lhs <> $rhs AS neq
      """
    Then the result should be, in any order:
      | eq   | neq   |
      | null | null |
    And no side effects

  Scenario: List equality [1, 2, 3] and [1, null, 4]
    And parameters are:
      | lhs | [1, 2, 3]   |
      | rhs | [1, null, 4] |
    When executing query:
      """
      RETURN $lhs = $rhs AS eq, $lhs <> $rhs AS neq
      """
    Then the result should be, in any order:
      | eq    | neq  |
      | false | true |
    And no side effects

  Scenario: List equality [1, 2, 3] and [1, 'two', 3]
    And parameters are:
      | lhs | [1, 2, 3]   |
      | rhs | [1, 'two', 4] |
    When executing query:
      """
      RETURN $lhs = $rhs AS eq, $lhs <> $rhs AS neq
      """
    Then the result should be, in any order:
      | eq    | neq  |
      | false | true |
    And no side effects

  Scenario: Map equality {k: 42} and {k: null}
    And parameters are:
      | lhs | {k: 42} |
      | rhs | {k: null}|
    When executing query:
      """
      RETURN $lhs = $rhs AS eq, $lhs <> $rhs AS neq
      """
    Then the result should be, in any order:
      | eq   | neq   |
      | null | null |
    And no side effects

  Scenario: Map equality {k1: 42} and {k2: null}
    And parameters are:
      | lhs | {k1: 42} |
      | rhs | {k2: null}|
    When executing query:
      """
      RETURN $lhs = $rhs AS eq, $lhs <> $rhs AS neq
      """
    Then the result should be, in any order:
      | eq    | neq   |
      | false | true |
    And no side effects

  @ignore
  Scenario: Map equality {k: 42} and {null: 42}
    And parameters are:
      | lhs | {k: 42} |
      | rhs | {null: 42}|
    When executing query:
      """
      RETURN $lhs = $rhs AS eq, $lhs <> $rhs AS neq
      """
    Then the result should be, in any order:
      | eq   | neq   |
      | null | null |
    And no side effects

    @ignore
  Scenario: Map equality {k: 42} and {null: 43}
    And parameters are:
      | lhs | {k: 42} |
      | rhs | {null: 43}|
    When executing query:
      """
      RETURN $lhs = $rhs AS eq, $lhs <> $rhs AS neq
      """
    Then the result should be, in any order:
      | eq    | neq  |
      | false | true |
    And no side effects

  Scenario: Map equality {k1: 42, k2: 43, k3: 44} and {k1: 42, k2: null, k3: 'fortyfour'}
    And parameters are:
      | lhs | {k1: 42, k2: 43, k3: 44}  |
      | rhs | {k1: 42, k2: null, k3: 'fortyfour'} |
    When executing query:
      """
      RETURN $lhs = $rhs AS eq, $lhs <> $rhs AS neq
      """
    Then the result should be, in any order:
      | eq    | neq  |
      | false | true |
    And no side effects

  Scenario Outline: Spatial equality test
    When executing query:
      """
      WITH point(<map1>) AS p1, point(<map2>) AS p2
      RETURN p1 = p2 AS eq, p1 <> p2 AS neq
      """
    Then the result should be, in any order:
      | eq   | neq   |
      | <eq> | <neq> |
    And no side effects

    Examples:
      | map1         | map2                        | eq    | neq   |
      | {x: 1, y: 2} | {x: 1, y: 2}                | true  | false |
      | {x: 1, y: 2} | {x: 2, y: 3}                | false | true  |
      | {x: 1, y: 2} | {x: 2, y: 2}                | false | true  |
      | {x: 1, y: 2} | {x: 1, y: 2, crs: 'wgs-84'} | false | true  |

  Scenario: Duration equality test of equal durations
    Given an empty graph
    And having executed:
      """
      CREATE (n:L {d: duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70})})
      """
    When executing query:
      """
      MATCH (n:L)
      RETURN n.d = n.d AS eq, n.d <> n.d AS neq
      """
    Then the result should be, in any order:
      | eq   | neq  |
      | true | false |
    And no side effects

  Scenario: Duration equality test of different durations
    Given an empty graph
    And having executed:
      """
      CREATE (n:L {d1: duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70}),
                   d2: duration({years: 12, months: 7, days: 14, hours: 16, minutes: 12, seconds: 70}) })
      """
    When executing query:
      """
      MATCH (n:L)
      RETURN n.d1 = n.d2 AS eq, n.d1 <> n.d2 AS neq
      """
    Then the result should be, in any order:
      | eq    | neq   |
      | false | true  |
    And no side effects


