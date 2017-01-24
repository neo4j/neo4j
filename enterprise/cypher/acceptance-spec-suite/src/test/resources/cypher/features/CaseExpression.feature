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

Feature: CaseExpression

  Scenario: Case should handle mixed number types
    Given any graph
    When executing query:
      """
      WITH 0.5 AS x
      WITH (CASE WHEN x < 1 THEN 1 ELSE 2.0 END) AS x
      RETURN x + 1
      """
    Then the result should be:
      | x + 1 |
      | 2     |
    And no side effects

  Scenario: Case should handle mixed types
    Given any graph
    When executing query:
      """
      WITH 0.5 AS x
      WITH (CASE WHEN x < 1 THEN 'wow' ELSE true END) AS x
      RETURN x + '!'
      """
    Then the result should be:
      | x + '!' |
      | 'wow!'  |
    And no side effects

  Scenario: Returning a CASE expression into pattern expression
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (a)-[:T]->(:B),
             (a)-[:T]->(:C)
      """
    When executing query:
      """
      MATCH (n)
      RETURN CASE
               WHEN id(n) >= 0 THEN (n)-->()
               ELSE 42
             END AS p
      """
    Then the result should be:
      | p                                      |
      | [<(:A)-[:T]->(:C)>, <(:A)-[:T]->(:B)>] |
      | []                                     |
      | []                                     |
    And no side effects

  Scenario: Returning a CASE expression into integer
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (a)-[:T]->(:B),
             (a)-[:T]->(:C)
      """
    When executing query:
      """
      MATCH (n)
      RETURN CASE
               WHEN id(n) < 0 THEN (n)-->()
               ELSE 42
             END AS p
      """
    Then the result should be:
      | p  |
      | 42 |
      | 42 |
      | 42 |
    And no side effects

  Scenario: Returning a CASE expression with label predicates
    Given an empty graph
    And having executed:
      """
      CREATE (a1:A1), (b1:B1), (a2:A2), (b2:B2)
      CREATE (a1)-[:T1]->(b1),
             (a1)-[:T2]->(b1),
             (a2)-[:T1]->(b2),
             (a2)-[:T2]->(b2)
      """
    When executing query:
      """
      MATCH (n)
      RETURN CASE
               WHEN n:A1 THEN (n)-->(:B1)
               WHEN n:A2 THEN (n)-->(:B2)
               ELSE 42
             END AS p
      """
    Then the result should be:
      | p                                            |
      | [<(:A1)-[:T2]->(:B1)>, <(:A1)-[:T1]->(:B1)>] |
      | [<(:A2)-[:T2]->(:B2)>, <(:A2)-[:T1]->(:B2)>] |
      | 42                                           |
      | 42                                           |
    And no side effects

  Scenario: Using a CASE expression in a WITH, positive case
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (a)-[:T]->(:B),
             (a)-[:T]->(:C)
      """
    When executing query:
      """
      MATCH (n)
      WITH CASE
             WHEN id(n) >= 0 THEN (n)-->()
             ELSE 42
           END AS p, count(n) AS c
      RETURN p, c
      """
    Then the result should be:
      | p                                      | c |
      | [<(:A)-[:T]->(:C)>, <(:A)-[:T]->(:B)>] | 1 |
      | []                                     | 2 |
    And no side effects

  Scenario: Using a CASE expression in a WITH, negative case
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (a)-[:T]->(:B),
             (a)-[:T]->(:C)
      """
    When executing query:
      """
      MATCH (n)
      WITH CASE
             WHEN id(n) < 0 THEN (n)-->()
             ELSE 42
           END AS p, count(n) AS c
      RETURN p, c
      """
    Then the result should be:
      | p  | c |
      | 42 | 3 |
    And no side effects

  Scenario: Using a CASE expression with label predicates in a WITH
    Given an empty graph
    And having executed:
      """
      CREATE (a1:A1), (b1:B1), (a2:A2), (b2:B2)
      CREATE (a1)-[:T1]->(b1),
             (a1)-[:T2]->(b1),
             (a2)-[:T1]->(b2),
             (a2)-[:T2]->(b2)
      """
    When executing query:
      """
      MATCH (n)
      WITH CASE
             WHEN n:A1 THEN (n)-->(:B1)
             WHEN n:A2 THEN (n)-->(:B2)
             ELSE 42
           END AS p, count(n) AS c
      RETURN p, c
      """
    Then the result should be:
      | p                                            | c |
      | [<(:A1)-[:T2]->(:B1)>, <(:A1)-[:T1]->(:B1)>] | 1 |
      | [<(:A2)-[:T2]->(:B2)>, <(:A2)-[:T1]->(:B2)>] | 1 |
      | 42                                           | 2 |
    And no side effects

  Scenario: Using a CASE expression in a WHERE, positive case
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (a)-[:T]->(:B),
             (a)-[:T]->(:C)
      """
    When executing query:
      """
      MATCH (n)
      WHERE (CASE
               WHEN id(n) >= 0 THEN length((n)-->())
               ELSE 42
             END) > 0
      RETURN n
      """
    Then the result should be:
      | n    |
      | (:A) |
    And no side effects

  Scenario: Using a CASE expression in a WHERE, negative case
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (a)-[:T]->(:B),
             (a)-[:T]->(:C)
      """
    When executing query:
      """
      MATCH (n)
      WHERE (CASE
               WHEN id(n) < 0 THEN length((n)-->())
               ELSE 42
             END) > 0
      RETURN n
      """
    Then the result should be:
      | n    |
      | (:A) |
      | (:B) |
      | (:C) |
    And no side effects

  Scenario: Using a CASE expression in a WHERE, with relationship predicate
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (a)-[:T]->(:B),
             (a)-[:T]->(:C)
      """
    When executing query:
      """
      MATCH (n)
      WHERE (CASE
               WHEN id(n) < 0 THEN length((n)-[:X]->())
               ELSE 42
             END) > 0
      RETURN n
      """
    Then the result should be:
      | n    |
      | (:A) |
      | (:B) |
      | (:C) |
    And no side effects

  Scenario: Using a CASE expression in a WHERE, with label predicate
    Given an empty graph
    And having executed:
      """
      CREATE (a:A)
      CREATE (a)-[:T]->(:B),
             (a)-[:T]->(:C)
      """
    When executing query:
      """
      MATCH (n)
      WHERE (CASE
               WHEN id(n) < 0 THEN length((n)-->(:X))
               ELSE 42
             END) > 0
      RETURN n
      """
    Then the result should be:
      | n    |
      | (:A) |
      | (:B) |
      | (:C) |
    And no side effects

  Scenario: Returning a CASE expression with a pattern expression alternative
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {prop: 42})
      CREATE (a)-[:T]->(),
             (a)-[:T]->(),
             (a)-[:T]->()
      """
    When executing query:
      """
      MATCH (a:A)
      RETURN CASE
               WHEN a.prop = 42 THEN []
               ELSE (a)-->()
             END AS x
      """
    Then the result should be:
      | x  |
      | [] |
    And no side effects
