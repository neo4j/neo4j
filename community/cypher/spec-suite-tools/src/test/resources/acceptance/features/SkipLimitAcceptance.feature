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

Feature: SkipLimitAcceptance

  Background:
    Given an empty graph

  Scenario: Negative parameter for LIMIT with ORDER BY and an empty graph should fail
    And parameters are:
      | _limit | -1 |
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name AS name
      ORDER BY name LIMIT $_limit
      """
    Then an ArgumentError should be raised at runtime: NegativeIntegerArgument

  Scenario: Negative parameter for LIMIT and an empty graph should fail
    And parameters are:
      | _limit | -1 |
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name AS name
      LIMIT $_limit
      """
    Then an ArgumentError should be raised at runtime: NegativeIntegerArgument

  Scenario: Negative parameter for SKIP and an empty graph should fail
    And parameters are:
      | limit | -1 |
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name AS name
      SKIP $limit
      """
    Then an ArgumentError should be raised at runtime: NegativeIntegerArgument

  Scenario: Floating point parameter for LIMIT and an empty graph should fail
    And parameters are:
      | limit | 1.0 |
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name AS name
      LIMIT $limit
      """
    Then an ArgumentError should be raised at runtime: InvalidArgumentType

  Scenario: Floating point parameter for LIMIT with ORDER BY and an empty graph should fail
    And parameters are:
      | limit | 1.0 |
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name AS name
      ORDER BY name LIMIT $limit
      """
    Then an ArgumentError should be raised at runtime: InvalidArgumentType

  Scenario: Floating point parameter for SKIP and an empty graph should fail
    And parameters are:
      | limit | 1.0 |
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name AS name
      SKIP $limit
      """
    Then an ArgumentError should be raised at runtime: InvalidArgumentType

  Scenario: Graph touching LIMIT should fail with a syntax exception
    And having executed:
      """
      CREATE (s:Person {name: 'Steven'}),
             (c:Person {name: 'Craig'})
      """
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name AS name
      LIMIT reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
      """
    Then a SyntaxError should be raised at compile time: NonConstantExpression


  Scenario: Graph touching SKIP should fail with a syntax exception
    And having executed:
      """
      CREATE (s:Person {name: 'Steven'}),
             (c:Person {name: 'Craig'})
      """
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name AS name
      SKIP reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x)
      """
    Then a SyntaxError should be raised at compile time: NonConstantExpression


  Scenario: Graph touching LIMIT should fail with a syntax exception 2
    And having executed:
      """
      CREATE (s:Person {name: 'Steven'}),
             (c:Person {name: 'Craig'})
      """
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name AS name
      LIMIT size([(a)-->(b) | b.age])
      """
    Then a SyntaxError should be raised at compile time: NonConstantExpression


  Scenario: Graph touching SKIP should fail with a syntax exception 2
    And having executed:
      """
      CREATE (s:Person {name: 'Steven'}),
             (c:Person {name: 'Craig'})
      """
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name AS name
      SKIP size([(a)-->(b) | b.age])
      """
    Then a SyntaxError should be raised at compile time: NonConstantExpression


  Scenario: Graph touching LIMIT should fail with a syntax exception 3
    And having executed:
      """
      CREATE (s:Person {name: 'Steven'}),
             (c:Person {name: 'Craig'})
      """
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name AS name, p
      LIMIT size(()-->())
      """
    Then a SyntaxError should be raised at compile time: NonConstantExpression


  Scenario: Graph touching SKIP should fail with a syntax exception 3
    And having executed:
      """
      CREATE (s:Person {name: 'Steven'}),
             (c:Person {name: 'Craig'})
      """
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name AS name, p
      SKIP size(()-->())
      """
    Then a SyntaxError should be raised at compile time: NonConstantExpression


  Scenario: Graph touching LIMIT should fail with a syntax exception 4
    And having executed:
      """
      CREATE (s:Person {name: 'Steven'}),
             (c:Person {name: 'Craig'})
      """
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name AS name
      LIMIT reduce(sum=0, x IN [p.age] | sum + x)
      """
    Then a SyntaxError should be raised at compile time: NonConstantExpression


  Scenario: Graph touching SKIP should fail with a syntax exception 4
    And having executed:
      """
      CREATE (s:Person {name: 'Steven'}),
             (c:Person {name: 'Craig'})
      """
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name AS name
      SKIP reduce(sum=0, x IN [p.age] | sum + x)
      """
    Then a SyntaxError should be raised at compile time: NonConstantExpression

  Scenario: Reduce LIMIT should be allowed
    And having executed:
      """
      CREATE (s:Person {name: 'Steven'}),
             (c:Person {name: 'Craig'})
      """
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name AS name
      LIMIT reduce(sum=0, x IN [0, 2] | sum + x)
      """
    Then the result should be, in any order:
    | name     |
    | 'Steven' |
    | 'Craig'  |
    And no side effects


  Scenario: Reduce SKIP should be allowed
    And having executed:
      """
      CREATE (s:Person {name: 'Steven'}),
             (c:Person {name: 'Craig'})
      """
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name AS name
      SKIP reduce(sum=0, x IN [0, 2] | sum + x)
      """
    Then the result should be, in any order:
      | name     |
    And no side effects

  Scenario: Skipping more than Integer.Max rows should be allowed
    When executing query:
      """
      MATCH (p:Person)
      RETURN p
      SKIP 9223372036854775807 // Long.Max
      """
    Then the result should be, in any order:
      | p |
    And no side effects

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

  Scenario: PartialTop with limit 0
    When executing query:
      """
      UNWIND range(1, 10) AS i
      WITH i AS i, -i AS j ORDER BY i
      RETURN i, j ORDER BY i, j LIMIT 0
      """
    Then the result should be, in order:
      | i | j |
    And no side effects
