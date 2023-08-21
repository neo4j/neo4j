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

Feature: ExplainAcceptance

  Background:
    Given an empty graph

  Scenario: Explanation of standalone procedure call
    And there exists a procedure test.labels() :: (label :: STRING?):
      | label |
      | 'A'   |
      | 'B'   |
      | 'C'   |
    When executing query:
    """
    EXPLAIN CALL test.labels()
    """
    Then the result should be, in any order:
      | label |
    And no side effects

  Scenario: Explanation of in-query procedure call
    And there exists a procedure test.labels() :: (label :: STRING?):
      | label |
      | 'A'   |
      | 'B'   |
      | 'C'   |
    When executing query:
    """
    EXPLAIN
    CALL test.labels() YIELD label
    RETURN *
    """
    Then the result should be, in any order:
      | label |
    And no side effects

  Scenario: Explanation of query with return columns
    When executing query:
    """
    EXPLAIN
    MATCH (a)-[r]->(b)
    RETURN a, r, b
    """
    Then the result should be, in any order:
      | a | r | b |
    And no side effects

  Scenario: Explanation of query without return columns
    When executing query:
    """
    EXPLAIN
    CREATE (a)
    """
    Then the result should be empty
    And no side effects

  Scenario: Explanation of query ending in unit subquery call
    When executing query:
    """
    EXPLAIN
    MATCH (n)
    CALL {
      CREATE (a)
    }
    """
    Then the result should be empty
    And no side effects
