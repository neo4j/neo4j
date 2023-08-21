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

Feature: NaNAcceptance

  Background:
    Given an empty graph

  Scenario: using a plain integer
    When executing query:
      """
      RETURN isNaN(1) AS result
      """
    Then the result should be, in any order:
      | result |
      | false  |
    And no side effects

  Scenario: using a plain float
    When executing query:
      """
      RETURN isNaN(1.0f) AS result
      """
    Then the result should be, in any order:
      | result |
      | false  |
    And no side effects

  Scenario: using a plain hexadecimal number
    When executing query:
      """
      RETURN isNaN(0x0ad) AS result
      """
    Then the result should be, in any order:
      | result |
      | false  |
    And no side effects

  Scenario: using a plain octal number
    When executing query:
      """
      RETURN isNaN(0o1) AS result
      """
    Then the result should be, in any order:
      | result |
      | false  |
    And no side effects

  Scenario: using a plain double
    When executing query:
      """
      RETURN isNaN(1.0) AS result
      """
    Then the result should be, in any order:
      | result |
      | false  |
    And no side effects

  Scenario: using a NaN value
    When executing query:
      """
      RETURN isNaN(0/0.0) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

  Scenario: using a Positive Infinity
    When executing query:
      """
      RETURN isNaN(1/0.0) AS result
      """
    Then the result should be, in any order:
      | result |
      | false  |
    And no side effects

  Scenario: using a Negative Infinity
    When executing query:
      """
      RETURN isNaN(-1/0.0) AS result
      """
    Then the result should be, in any order:
      | result |
      | false  |
    And no side effects

  Scenario: using null
    When executing query:
      """
      RETURN isNaN(null) AS result
      """
    Then the result should be, in any order:
      | result |
      | null   |
    And no side effects

  Scenario: using a non-number value
    When executing query:
      """
      RETURN isNaN("foo") AS result
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType

  Scenario: with chained numerical function value
    When executing query:
      """
      RETURN isNaN(abs(0/0.0)) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

  Scenario: with NOT of a less than or greater than inequality
    When executing query:
      """
      RETURN NOT(0.0 < (0.0/0.0)) AS result1, NOT(0.0 > (0.0/0.0)) AS result2
      """
    Then the result should be, in any order:
      | result1 | result2 |
      | true    | true    |
    And no side effects

  Scenario: with NOT of an inequality containing params
    And parameters are:
      | zero | 0.0 |
    When executing query:
      """
      RETURN NOT(0.0 <= (0.0/$zero)) AS result1, NOT(0.0 >= (0.0/$zero)) AS result2
      """
    Then the result should be, in any order:
      | result1 | result2 |
      | true    | true    |
    And no side effects

  Scenario: with NOT of an inequality containing a function
    When executing query:
      """
      RETURN NOT (ceil(0.0/0.0) < 0.0) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

  Scenario: with NOT of NOT of an inequality
    When executing query:
      """
      RETURN NOT (NOT (0.0 < (0.0/0.0))) AS result
      """
    Then the result should be, in any order:
      | result |
      | false  |
    And no side effects

  Scenario: with NOT of an equality
    When executing query:
      """
      RETURN NOT (0.0 = (0.0/0.0)) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

  Scenario: with NOT of an equality with NaN on both sides
    When executing query:
      """
      RETURN NOT ((0.0/0.0) = (0.0/0.0)) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

  Scenario: with NOT of an inequality
    When executing query:
      """
      RETURN NOT (0.0 <> (0.0/0.0)) AS result
      """
    Then the result should be, in any order:
      | result |
      | false   |
    And no side effects

  Scenario: with NOT of an inequality with NaN on both sides
    When executing query:
      """
      RETURN NOT ((0.0/0.0) <> (0.0/0.0)) AS result
      """
    Then the result should be, in any order:
      | result |
      | false  |
    And no side effects

  Scenario: with NOT of an inequality with NaN in a variable
    When executing query:
      """
      WITH (0.0/0.0) AS nan
      RETURN NOT (nan < 0.0) AS result
      """
    Then the result should be, in any order:
      | result |
      | true  |
    And no side effects

  Scenario: with NOT of an inequality with NaN in a property
    Given an empty graph
    And having executed:
    """
    CREATE ({nan : (0.0/0.0)})
    """
    When executing query:
      """
      MATCH (n)
      RETURN NOT (n.nan < 0.0) AS result
      """
    Then the result should be, in any order:
      | result |
      | true  |
    And no side effects
