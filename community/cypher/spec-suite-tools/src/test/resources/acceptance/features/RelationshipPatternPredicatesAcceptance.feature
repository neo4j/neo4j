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

  Scenario: Should not allow to reference other elements of the pattern
    Given any graph
    When executing query:
      """
      MATCH (a:A)-[r:R WHERE a.prop > 1]-(b:B)
      RETURN a.prop AS result
      """
    Then a SyntaxError should be raised at runtime: UndefinedVariable

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
    Then the result should be, in any order:
      | result |
      | [2, 3] |
    And no side effects

  Scenario: Should not allow to reference other elements of the pattern in a pattern comprehension
    Given any graph
    When executing query:
      """
      RETURN [(a:A)-[r WHERE b.prop > 1]-(b:B) | a]
      """
    Then a SyntaxError should be raised at runtime: UndefinedVariable
