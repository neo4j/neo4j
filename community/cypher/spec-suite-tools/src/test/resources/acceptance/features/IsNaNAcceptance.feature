#
# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

#encoding: utf-8

Feature: IsNaNAcceptance

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
    Then a TypeError should be raised at runtime: InvalidArgumentType

  Scenario: with chained numerical function value
    When executing query:
      """
      RETURN isNaN(abs(0/0.0)) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects
