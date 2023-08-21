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

Feature: IsEmptyFunctionAcceptance

  Background:
    Given any graph

  Scenario: isEmpty should be null-in-null-out
    When executing query:
      """
      RETURN isEmpty(null) AS result
      """
    Then the result should be, in order:
      | result |
      | null   |
    And no side effects

  Scenario: isEmpty should return true for empty list
    When executing query:
      """
      RETURN isEmpty([]) AS result
      """
    Then the result should be, in order:
      | result |
      | true   |
    And no side effects

  Scenario: isEmpty should return false for non-empty list
    When executing query:
      """
      RETURN isEmpty([1, 2, 3]) AS result
      """
    Then the result should be, in order:
      | result |
      | false  |
    And no side effects

  Scenario: isEmpty should return false for non-empty list containing null
    When executing query:
      """
      RETURN isEmpty([null]) AS result
      """
    Then the result should be, in order:
      | result |
      | false  |
    And no side effects

  Scenario: isEmpty should return true for empty map
    When executing query:
      """
      RETURN isEmpty({}) AS result
      """
    Then the result should be, in order:
      | result |
      | true   |
    And no side effects

  Scenario: isEmpty should return false for non-empty map
    When executing query:
      """
      RETURN isEmpty({key: 'value'}) AS result
      """
    Then the result should be, in order:
      | result |
      | false  |
    And no side effects

  Scenario: isEmpty should return false for non-empty map containing null
    When executing query:
      """
      RETURN isEmpty({key: null}) AS result
      """
    Then the result should be, in order:
      | result |
      | false  |
    And no side effects

  Scenario: isEmpty should return true for empty string
    When executing query:
      """
      RETURN isEmpty('') AS result
      """
    Then the result should be, in order:
      | result |
      | true   |
    And no side effects

  Scenario: isEmpty should return false for non-empty string
    When executing query:
      """
      RETURN isEmpty('hello') AS result
      """
    Then the result should be, in order:
      | result |
      | false  |
    And no side effects

  Scenario: isEmpty should not work for path
    Given an empty graph
    And having executed:
    """
    CREATE ()-[:REL]->()
    """
    When executing query:
      """
      MATCH path=(a)--(b)
      RETURN isEmpty(path) AS result
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType
    And no side effects

  Scenario: isEmpty should not work for node
    Given an empty graph
    And having executed:
    """
    CREATE ()
    """
    When executing query:
      """
      MATCH (a)
      RETURN isEmpty(a) AS result
      """
    Then a TypeError should be raised at runtime: InvalidArgumentValue
    And no side effects

  Scenario: isEmpty should not work for relationship
    Given an empty graph
    And having executed:
    """
    CREATE ()-[:TYPE]->()
    """
    When executing query:
      """
      MATCH ()-[r]-()
      RETURN isEmpty(r) AS result
      """
    Then a TypeError should be raised at runtime: InvalidArgumentValue
    And no side effects

  Scenario: isEmpty should work for properties
    Given an empty graph
    And having executed:
    """
    CREATE (n:Label {prop: [1, 2, 3]})
    """
    When executing query:
      """
      MATCH (n:Label)
      RETURN isEmpty(n.prop) AS result
      """
    Then the result should be, in order:
      | result |
      | false  |
    And no side effects

  Scenario: isEmpty should work dynamically
    Given an empty graph
    When executing query:
      """
      WITH [1, [1,2,3]] AS xs
      WITH xs[1] AS v
      RETURN isEmpty(v) AS result
      """
    Then the result should be, in order:
      | result |
      | false  |
    And no side effects
