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

Feature: NullIfFunctionAcceptance

  Background:
    Given an empty graph
    And having executed:
    """
    CREATE (a:A {prop: 1, listProp: []})-[r:Rel]->(b:B {prop: "Hello", listProp: [1]})
    """

  Scenario: Testing Simple Matching Cases return Null
    When executing query:
      """
      UNWIND [
        [1, 1.0], [1.0, 1.0], ['abc', 'abc'], [false, false], [{map: 1}, {map: 1}],
        [[1], [1]], [date(), date()], [localdatetime('2015185T19:32:24'), localdatetime('2015185T19:32:24')],
        [point({x: 3, y: 0}), point({x: 3, y: 0})]] AS matchingValues
      RETURN nullIf(matchingValues[0], matchingValues[1]) AS nullIf
      """
    Then the result should be, in any order:
      | nullIf |
      | null   |
      | null   |
      | null   |
      | null   |
      | null   |
      | null   |
      | null   |
      | null   |
      | null   |
    And no side effects

  Scenario: Testing Simple Non Matching Cases return first item
    When executing query:
      """
      UNWIND [2, 1.0, 'abc', false, {map: 1}, [1], date(),
      localdatetime('2015185T19:32:24'), point({x: 3, y: 0})] AS nonMatchingValue
      RETURN nullIf(13, nonMatchingValue) AS nullIf
      """
    Then the result should be, in any order:
      | nullIf |
      | 13     |
      | 13     |
      | 13     |
      | 13     |
      | 13     |
      | 13     |
      | 13     |
      | 13     |
      | 13     |
    And no side effects

  Scenario: Functions should be able to be nested
    When executing query:
      """
      RETURN nullIf('STRING NOT NULL', valueType('string')) AS nullIf
      """
    Then the result should be, in any order:
      | nullIf |
      | null   |
    And no side effects

  Scenario: Null as second param doesn't return null
    When executing query:
      """
      RETURN nullIf(1, null) AS nullIf
      """
    Then the result should be, in any order:
      | nullIf |
      | 1      |
    And no side effects

  Scenario: Null as first param returns null
    When executing query:
      """
      RETURN nullIf(null, 'hello') AS nullIf
      """
    Then the result should be, in any order:
      | nullIf |
      | null   |
    And no side effects

  Scenario: Null isn't matching, but first null is returned
    When executing query:
      """
      RETURN nullIf(null, null) AS nullIf
      """
    Then the result should be, in any order:
      | nullIf |
      | null   |
    And no side effects

  Scenario: Can be used as a filter
    When executing query:
      """
      MATCH (n)
      WHERE nullIf('Hello', n.prop) IS NULL
      RETURN n.prop as prop
      """
    Then the result should be, in any order:
      | prop    |
      | 'Hello' |
    And no side effects
