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

Feature: RelationshipPatternPredicates
  Scenario: Predicate on a relationship
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:R {prop: 0}]->(:B)
      CREATE (:A)-[:R {prop: 1}]->(:B)
      CREATE (:A)-[:R {prop: 2}]->(:B)
      CREATE (:A)-[:R {prop: 3}]->(:B)
      """
    When executing query:
      """
      WITH 1 AS x
      MATCH (:A)-[r:R WHERE r.prop > x]-(b:B)
      RETURN r.prop AS result
      """
    Then the result should be, in any order:
      | result |
      | 2      |
      | 3      |
    And no side effects

  Scenario: Should allow reference to later elements of the pattern
    Given an empty graph
    And having executed:
      """
      CREATE (:A {prop: 100})-[:R {prop: 1}]->()
      CREATE (:A {prop: 200})-[:R {prop: 2}]->()
      """
    When executing query:
      """
      MATCH (a:A)-[r:R WHERE a.prop > 100]-()
      RETURN r.prop AS result
      """
    Then the result should be, in any order:
      | result |
      | 2      |
    And no side effects

  Scenario: Should allow reference to earlier elements of the pattern
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:R {prop: 1}]->(:B {prop: 100})
      CREATE ()-[:R {prop: 2}]->(:B {prop: 200})
      """
    When executing query:
      """
      MATCH ()-[r:R WHERE b.prop > 100]-(b:B)
      RETURN r.prop AS result
      """
    Then the result should be, in any order:
      | result |
      | 2      |
    And no side effects

  Scenario: Should allow reference to earlier elements of the pattern
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:R {prop: 1}]->(:B {prop: 1})
      CREATE ()-[:R {prop: 2}]->(:B {prop: 1})
      """
    When executing query:
      """
      MATCH ()-[r:R WHERE r.prop > b.prop]-(b:B)
      RETURN r.prop AS result
      """
    Then the result should be, in any order:
      | result |
      | 2      |
    And no side effects

  Scenario: Pattern comprehension with predicate on a relationship
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:R {prop: 1}]->(:B)
      CREATE (:A)-[:R {prop: 2}]->(:B)
      CREATE (:A)-[:R {prop: 3}]->(:B)
      """
    When executing query:
      """
      WITH 1 AS x
      RETURN [(a:A)-[r:R WHERE r.prop > x]-(b:B) | r.prop] AS result
      """
    Then the result should be (ignoring element order for lists):
      | result |
      | [2, 3] |
    And no side effects

  Scenario: Should allow references to earlier elements of the pattern in a pattern comprehension
    Given an empty graph
    And having executed:
      """
      CREATE (:A {prop: 1})-[:R {prop: 100}]->(:B)
      CREATE (:A {prop: 2})-[:R {prop: 200}]->(:B)
      """
    When executing query:
      """
      RETURN [(a:A)-[r WHERE a.prop > 1]-(:B) | [a.prop, r.prop]] AS result
      """
    Then the result should be, in any order:
      | result     |
      | [[2, 200]] |
    And no side effects

  Scenario: Should allow references to later elements of the pattern in a pattern comprehension
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:R {prop: 100}]->(:B {prop: 1})
      CREATE (:A)-[:R {prop: 200}]->(:B {prop: 2})
      """
    When executing query:
      """
      RETURN [(:A)-[r WHERE b.prop > 1]-(b:B) | [b.prop, r.prop]] AS result
      """
    Then the result should be, in any order:
      | result     |
      | [[2, 200]] |
    And no side effects

  Scenario: Should allow projected variables in relationship predicate
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:R {prop: 1}]->()
      """
    When executing query:
      """
      WITH true as x
      MATCH ()-[r WHERE x]->()
      RETURN r.prop AS result
      """
    Then the result should be, in any order:
      | result     |
      | 1          |
    And no side effects

  Scenario: Should allow arbitrary search conditions
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:R {prop: 1}]->()
      """
    When executing query:
      """
      MATCH ()-[r WHERE true]->()
      RETURN r.prop AS result
      """
    Then the result should be, in any order:
      | result     |
      | 1          |
    And no side effects

  Scenario: Should allow mixing property specification and WHERE clause
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:R {p: 1, q: 100}]->()
      CREATE ()-[:R {p: 2, q: 200}]->()
      CREATE ()-[:R {p: 1, q: 300}]->()
      """
    When executing query:
      """
      MATCH ()-[r:R {p: 1} WHERE r.q > 100]->()
      RETURN r.q AS result
      """
    Then the result should be, in any order:
      | result     |
      | 300        |
    And no side effects
