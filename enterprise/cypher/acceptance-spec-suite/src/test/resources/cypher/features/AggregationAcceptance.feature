#
# Copyright (c) 2002-2017 "Neo Technology,"
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

Feature: AggregationAcceptance

  Background:
    Given an empty graph

  Scenario: Using a optional match after aggregation and before an aggregation
    And having executed:
      """
      CREATE (:Z{key:1})-[:IS_A]->(:A)
      """
    When executing query:
      """
      MATCH (a:A)
      WITH count(*) AS aCount
      OPTIONAL MATCH (z:Z)-[IS_A]->()
      RETURN aCount, count(distinct z.key) as zCount
      """
    Then the result should be:
      | aCount | zCount |
      | 1      | 1      |
    And no side effects

  Scenario: max() over strings
    When executing query:
      """
      UNWIND ['a', 'b', 'B', null, 'abc', 'abc1'] AS i
      RETURN max(i)
      """
    Then the result should be:
      | max(i) |
      | 'b'    |
    And no side effects

  Scenario: min() over strings
    When executing query:
      """
      UNWIND ['a', 'b', 'B', null, 'abc', 'abc1'] AS i
      RETURN min(i)
      """
    Then the result should be:
      | min(i) |
      | 'B'    |
    And no side effects

  Scenario: max() over integers
    When executing query:
      """
      UNWIND [1, 2, 0, null, -1] AS x
      RETURN max(x)
      """
    Then the result should be:
      | max(x) |
      | 2      |
    And no side effects

  Scenario: min() over integers
    When executing query:
      """
      UNWIND [1, 2, 0, null, -1] AS x
      RETURN min(x)
      """
    Then the result should be:
      | min(x) |
      | -1     |
    And no side effects

  Scenario: max() over floats
    When executing query:
      """
      UNWIND [1.0, 2.0, 0.5, null] AS x
      RETURN max(x)
      """
    Then the result should be:
      | max(x) |
      | 2.0    |
    And no side effects

  Scenario: min() over floats
    When executing query:
      """
      UNWIND [1.0, 2.0, 0.5, null] AS x
      RETURN min(x)
      """
    Then the result should be:
      | min(x) |
      | 0.5    |
    And no side effects

  Scenario: max() over mixed numeric values
    When executing query:
      """
      UNWIND [1, 2.0, 5, null, 3.2, 0.1] AS x
      RETURN max(x)
      """
    Then the result should be:
      | max(x) |
      | 5      |
    And no side effects

  Scenario: min() over mixed numeric values
    When executing query:
      """
      UNWIND [1, 2.0, 5, null, 3.2, 0.1] AS x
      RETURN min(x)
      """
    Then the result should be:
      | min(x) |
      | 0.1    |
    And no side effects

  Scenario: max() over mixed values
    When executing query:
      """
      UNWIND [1, 'a', null, [1, 2], 0.2, 'b'] AS x
      RETURN max(x)
      """
    Then the result should be:
      | max(x) |
      | 1      |
    And no side effects

  Scenario: min() over mixed values
    When executing query:
      """
      UNWIND [1, 'a', null, [1, 2], 0.2, 'b'] AS x
      RETURN min(x)
      """
    Then the result should be:
      | min(x) |
      | [1, 2] |
    And no side effects

  Scenario: max() over list values
    When executing query:
      """
      UNWIND [[1], [2], [2, 1]] AS x
      RETURN max(x)
      """
    Then the result should be:
      | max(x) |
      | [2, 1] |
    And no side effects

  Scenario: min() over list values
    When executing query:
      """
      UNWIND [[1], [2], [2, 1]] AS x
      RETURN min(x)
      """
    Then the result should be:
      | min(x) |
      | [1]    |
    And no side effects