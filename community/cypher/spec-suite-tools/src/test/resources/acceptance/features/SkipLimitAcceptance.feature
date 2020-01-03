#
# Copyright (c) 2002-2020 "Neo4j,"
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

Feature: SkipLimitAcceptance

  Background:
    Given an empty graph

  Scenario: Negative parameter for LIMIT should not generate errors
    And having executed:
      """
      CREATE (s:Person {name: 'Steven'}),
             (c:Person {name: 'Craig'})
      """
    And parameters are:
      | limit | -1 |
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name AS name
      LIMIT $limit
      """
    Then the result should be, in order:
      | name |
    And no side effects

  Scenario: Negative LIMIT should fail with a syntax exception
    And having executed:
      """
      CREATE (s:Person {name: 'Steven'}),
             (c:Person {name: 'Craig'})
      """
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name AS name
      LIMIT -1
      """
    Then a SyntaxError should be raised at compile time: NegativeIntegerArgument

  Scenario: Combining LIMIT and aggregation
    And having executed:
      """
      CREATE (s:Person {name: 'Steven'})
      """
    When executing query:
      """
      MATCH (p:Person)
      WITH p LIMIT 1
      RETURN count(p) AS count
      """
    Then the result should be, in order:
      | count |
      |   1   |
    And no side effects

  Scenario: Stand alone limit in the return clause
    And having executed:
      """
      CREATE (:A {id:0})-[:REL]->(:B {id:1})
      """
    When executing query:
      """
      MATCH (n:A) WITH n
      MATCH (n)-[:REL]->(m)
      RETURN m.id AS id
      LIMIT 1
      """
    Then the result should be, in order:
      | id |
      | 1  |
    And no side effects

  Scenario: Order by followed by limit in return clause
    And having executed:
      """
      CREATE (:A {id:0})-[:REL]->(:B {id:1})
      """
    When executing query:
      """
      MATCH (n:A) WITH n
      MATCH (n)-[:REL]->(m)
      RETURN m.id AS id
      ORDER BY id LIMIT 1
      """
    Then the result should be, in order:
      | id |
      | 1  |
    And no side effects

  Scenario: Limit in with clause
      And having executed:
      """
      CREATE (:A {id:0})-[:REL]->(:B {id:1})
      """
      When executing query:
      """
      MATCH (n:A) WITH n LIMIT 1
      MATCH (n)-[:REL]->(m)
      RETURN m.id AS id
      """
      Then the result should be, in order:
        | id |
        | 1  |
      And no side effects

  Scenario: Limit before sort
    And having executed:
      """
      CREATE (:A {id:0})-[:REL]->(:B {id:1})
      """
    When executing query:
      """
      MATCH (n:A) WITH n LIMIT 1
      MATCH (n)-[:REL]->(m)
      RETURN m.id AS id
      ORDER BY id
      """
    Then the result should be, in order:
      | id |
      | 1  |
    And no side effects

  Scenario: Limit before top
    And having executed:
      """
      CREATE (:A {id:0})-[:REL]->(:B {id:1})
      """
    When executing query:
      """
      MATCH (n:A) WITH n LIMIT 1
      MATCH (n)-[:REL]->(m)
      RETURN m.id AS id
      ORDER BY id LIMIT 1
      """
    Then the result should be, in order:
      | id |
      | 1  |
    And no side effects

  Scenario: Limit before distinct
    And having executed:
      """
      CREATE (:A {id:0})-[:REL]->(:B {id:1})
      """
    When executing query:
      """
      MATCH (n:A) WITH n LIMIT 1
      MATCH (n)-[:REL]->(m)
      RETURN DISTINCT m.id AS id
      """
    Then the result should be, in order:
      | id |
      | 1  |
    And no side effects

  Scenario: Top with limit 0
    And having executed:
      """
      CREATE (:A {id:0})
      """
    When executing query:
      """
      MATCH (n:A)
      RETURN n.id AS id
      ORDER BY id LIMIT 0
      """
    Then the result should be, in order:
      | id |
    And no side effects
