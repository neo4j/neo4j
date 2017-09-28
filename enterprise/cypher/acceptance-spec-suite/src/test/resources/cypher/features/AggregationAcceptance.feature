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

  Scenario: Should give max of integer values
    When executing query:
      """
      UNWIND [1,2] AS x RETURN max(x)
      """
    Then the result should be:
      | max(x) |
      | 2      |
    And no side effects

  Scenario: Should give max of float values
    When executing query:
      """
      UNWIND [1.0,2.0] AS x RETURN max(x)
      """
    Then the result should be:
      | max(x) |
      | 2.0    |
    And no side effects

  Scenario: Should give max of numerical values
    When executing query:
      """
      UNWIND [1,2.0] AS x RETURN max(x)
      """
    Then the result should be:
      | max(x) |
      | 2.0    |
    And no side effects

  Scenario: Should give max of text values
    When executing query:
      """
      UNWIND ['fu','bar'] AS x RETURN max(x)
      """
    Then the result should be:
      | max(x)  |
      | 'fu'    |
    And no side effects

  Scenario: Should give max of numerical and text values
    When executing query:
      """
      UNWIND [1,'a'] AS x RETURN max(x)
      """
    Then the result should be:
      | max(x) |
      | 1      |
    And no side effects

  Scenario: Should give max of list values
    When executing query:
      """
      UNWIND [[1],[2]] AS x RETURN max(x)
      """
    Then the result should be:
      | max(x) |
      | [2]    |
    And no side effects

  Scenario: Should give max of numerical and list values
    When executing query:
      """
      UNWIND [1,[2]] AS x RETURN max(x)
      """
    Then the result should be:
      | max(x) |
      | 1      |
    And no side effects

  Scenario: Should give max of numerical and null values
    When executing query:
      """
      UNWIND [1,null] AS x RETURN max(x)
      """
    Then the result should be:
      | max(x) |
      | 1      |
    And no side effects