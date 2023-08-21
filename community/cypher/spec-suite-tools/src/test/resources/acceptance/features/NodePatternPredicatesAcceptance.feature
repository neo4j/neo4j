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

Feature: NodePatternPredicates
  Scenario: Predicate on a single node
    Given an empty graph
    And having executed:
      """
      CREATE (:A {prop: 1})-[:R]->(:B)
      CREATE (:A {prop: 2})-[:R]->(:B)
      CREATE (:A {prop: 3})-[:R]->(:B)
      """
    When executing query:
      """
      WITH 1 AS x
      MATCH (a:A WHERE a.prop > x)-[r]-(b:B)
      RETURN a.prop AS result
      """
    Then the result should be, in any order:
      | result |
      | 2      |
      | 3      |
    And no side effects

  Scenario: Predicates on multiple nodes
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {prop: 1})
      CREATE (a)-[:R]->(:B {prop: 100})
      CREATE (a)-[:R]->(:B {prop: 200})

      CREATE (:A {prop: 2})-[:R]->(:B {prop: 300})
      CREATE (:A {prop: 3})-[:R]->(:B {prop: 400})
      """
    When executing query:
      """
      MATCH (a:A WHERE a.prop < 3)-[r]-(b:B WHERE b.prop > 100)
      RETURN a.prop, b.prop
      """
    Then the result should be, in any order:
      | a.prop | b.prop |
      | 1      | 200    |
      | 2      | 300    |
    And no side effects

  Scenario: shortestPath with node pattern predicate
    Given an empty graph
    And having executed:
      """
      CREATE (start:Start)
      CREATE (start)-[:R]->()-[:R]->(:End {prop: 1})
      CREATE (start)-[:R]->()-[:R]->()-[:R]->(:End {prop: 2})
      CREATE (start)-[:R]->()-[:R]->()-[:R]->()-[:R]->(:End {prop: 2})
      CREATE (start)-[:R]->()-[:R]->()-[:R]->()-[:R]->(:End {prop: 3})
      """
    When executing query:
      """
      MATCH p = shortestPath((start:Start)-[:R*]->(end:End WHERE end.prop > 1))
      RETURN end.prop, length(p) AS len
      """
    Then the result should be, in any order:
      | end.prop | len |
      | 2        | 3   |
      | 2        | 4   |
      | 3        | 4   |
    And no side effects

  Scenario: Should allow reference to later elements of the pattern
    Given an empty graph
    And having executed:
      """
      CREATE (a)-[:R]->({prop: 100})
      CREATE (a)-[:R]->({prop: 200})
      """

    Given any graph
    When executing query:
      """
      MATCH (a WHERE b.prop > 100)-[r]-(b)
      RETURN b.prop AS result
      """
    Then the result should be, in any order:
      | result |
      | 200    |
    And no side effects

  Scenario: Should allow reference to earlier elements of the pattern
    Given an empty graph
    And having executed:
      """
      CREATE ({prop: 100})-[:R]->(b)
      CREATE ({prop: 200})-[:R]->(b)
      """

    Given any graph
    When executing query:
      """
      MATCH (a)-[r]-(b WHERE a.prop > 100)
      RETURN a.prop AS result
      """
    Then the result should be, in any order:
      | result |
      | 200    |
    And no side effects

  Scenario: Should allow reference to self and earlier and later elements of the pattern
    Given an empty graph
    And having executed:
      """
      CREATE ({prop: 100})-[:R]->({prop: 100})
      CREATE ({prop: 200})-[:R]->({prop: 250})
      """

    Given any graph
    When executing query:
      """
      MATCH (a)-[r]-(b WHERE b.prop > a.prop)
      RETURN b.prop AS result
      """
    Then the result should be, in any order:
      | result |
      | 250    |
    And no side effects

  Scenario: Pattern comprehension with predicate on a single node
    Given an empty graph
    And having executed:
      """
      CREATE (:A {prop: 1})-[:R]->(:B)
      CREATE (:A {prop: 2})-[:R]->(:B)
      CREATE (:A {prop: 3})-[:R]->(:B)
      """
    When executing query:
      """
      WITH 1 AS x
      RETURN [(a:A WHERE a.prop > x)-[r]-(b:B) | a.prop] AS result
      """
    Then the result should be (ignoring element order for lists):
      | result |
      | [2, 3] |
    And no side effects

  Scenario: Pattern comprehension with predicates on multiple nodes
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {prop: 1})
      CREATE (a)-[:R]->(:B {prop: 100})
      CREATE (a)-[:R]->(:B {prop: 200})

      CREATE (:A {prop: 2})-[:R]->(:B {prop: 300})
      CREATE (:A {prop: 3})-[:R]->(:B {prop: 400})
      """
    When executing query:
      """
      UNWIND [(a:A WHERE a.prop < 3)-[r]->(b:B WHERE b.prop > 100) | [a.prop, b.prop]] AS result
      RETURN result
      """
    Then the result should be, in any order:
      | result   |
      | [1, 200] |
      | [2, 300] |
    And no side effects

  Scenario: Should allow references other elements of the pattern in a pattern comprehension
    Given an empty graph
    And having executed:
      """
      CREATE (:A {prop: 1})-[:R]->(:B {prop: 100})
      CREATE (:A {prop: 2})-[:R]->(:B {prop: 200})
      """
    When executing query:
      """
      RETURN [(a:A WHERE b.prop > 100)-[r]-(b:B) | [a.prop, b.prop]] AS result
      """
    Then the result should be, in any order:
      | result     |
      | [[2, 200]] |
    And no side effects

  Scenario: Should allow references other elements of the pattern in a pattern comprehension
    Given an empty graph
    And having executed:
      """
      CREATE (:A {prop: 100})-[:R]->(:B {prop: 1})
      CREATE (:A {prop: 200})-[:R]->(:B {prop: 2})
      """
    When executing query:
      """
      RETURN [(a:A)-[r]-(b:B WHERE a.prop > 100) | [b.prop, a.prop]] AS result
      """
    Then the result should be, in any order:
      | result     |
      | [[2, 200]] |
    And no side effects

  Scenario: Should allow arbitrary search conditions
    Given an empty graph
    And having executed:
      """
      CREATE ({prop: 1})
      """
    When executing query:
      """
      MATCH (n WHERE true)
      RETURN n.prop AS result
      """
    Then the result should be, in any order:
      | result     |
      | 1          |
    And no side effects

  Scenario: Should allow mixing property specification and WHERE clause
    Given an empty graph
    And having executed:
      """
      CREATE ({p: 1, q: 100})
      CREATE ({p: 2, q: 200})
      CREATE ({p: 1, q: 300})
      """
    When executing query:
      """
      MATCH (n {p: 1} WHERE n.q > 100)
      RETURN n.q AS result
      """
    Then the result should be, in any order:
      | result     |
      | 300        |
    And no side effects
