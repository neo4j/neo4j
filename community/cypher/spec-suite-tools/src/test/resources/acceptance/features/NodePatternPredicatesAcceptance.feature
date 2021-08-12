#
# Copyright (c) "Neo4j"
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

Feature: NodePatternPredicates
  Scenario: Predicate on a single node
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


  Scenario: Should not allow to reference other elements of the pattern
    Given any graph
    When executing query:
      """
      MATCH (a:A WHERE b.prop > 1)-[r]-(b:B)
      RETURN a.prop AS result
      """
    Then a SyntaxError should be raised at runtime: UndefinedVariable

  Scenario: Pattern comprehension with predicate on a single node
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
    Then the result should be, in any order:
      | result |
      | [2, 3] |
    And no side effects

  Scenario: Pattern comprehension with predicates on multiple nodes
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
      RETURN [(a:A WHERE a.prop < 3)-[r]->(b:B WHERE b.prop > 100) | [a.prop, b.prop]] AS result
      """
    Then the result should be, in any order:
      | result               |
      | [[1, 200], [2, 300]] |
    And no side effects

  Scenario: Should not allow to reference other elements of the pattern in a pattern comprehension
    Given any graph
    When executing query:
      """
      RETURN [(a:A WHERE b.prop > 1)-[r]-(b:B) | a]
      """
    Then a SyntaxError should be raised at runtime: UndefinedVariable