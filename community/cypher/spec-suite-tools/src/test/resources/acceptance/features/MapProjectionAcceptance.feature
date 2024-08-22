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

Feature: MapProjectionAcceptance

  Scenario: Should use custom key in node map projection
    Given an empty graph
    And having executed:
      """
      CREATE (:Label { prop1: 'hello', prop2: 'hi', prop3: 'hej' })
      """
    When executing query:
      """
      MATCH (n) RETURN n { customKey1: n.prop2, customKey2: n.prop3, customKey3: n.notThere, .prop1 } AS result
      """
    Then the result should be, in order:
      | result                                                                  |
      | {customKey1: 'hi', customKey2: 'hej', customKey3: null, prop1: 'hello'} |
    And no side effects

  Scenario: Should use custom key in relationship map projection
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:LIKES { prop1: 'hello', prop2: 'hi', prop3: 'hej' }]->(:B)
      """
    When executing query:
      """
      MATCH ()-[r]->() RETURN r { customKey3: r.notThere, customKey1: r.prop3, customKey2: r.prop1, .prop1 } AS result
      """
    Then the result should be, in order:
      | result                                                                     |
      | {customKey1: 'hej', customKey2: 'hello', customKey3: null, prop1: 'hello'} |
    And no side effects

  Scenario: Should use custom key in node and relationship map projection
    Given an empty graph
    And having executed:
      """
      CREATE (:A {prop1: 'a1'})-[:LIKES { prop1: 'r1', prop2: 'r2' }]->(:B {prop1: 'b1', prop2: 'b2'})
      """
    When executing query:
      """
      MATCH (a)-[r]->(b) RETURN r { k1: r.prop2, k2: a.prop1, k3: b.prop2 } AS result
      """
    Then the result should be, in order:
      | result                                   |
      | {k1: 'r2', k2: 'a1', k3: 'b2'} |
    And no side effects

  Scenario: Should use custom key in mixed map projection
    Given an empty graph
    And having executed:
      """
      CREATE (:A {prop_a1: 'a1'})-[:LIKES { prop_r1: 'r1' }]->(:B {prop_b1: 'b1'})
      """
    When executing query:
      """
      MATCH (a)-[r]->(b) RETURN a { .prop_a1, .prop_r1, .prop_b1, k2: b.prop_b1, k3: r.prop_r1, k4: 1 } AS result
      """
    Then the result should be, in order:
      | result                                                                   |
      | {prop_a1: 'a1', prop_r1: null, prop_b1: null, k2: 'b1', k3: 'r1', k4: 1} |
    And no side effects

  Scenario: Should return all properties when all-properties selector is used:
    Given an empty graph
    And having executed:
      """
      CREATE (:A {x: 1})
      CREATE (:B {x: true, y: false})
      CREATE (:C {x: 'hello', y: [1,2,3], z: 123})
      """
    When executing query:
      """
      MATCH (n)
      RETURN n AS node, n {.*} AS projection
      """
    Then the result should be, in any order:
      | node                                    | projection                         |
      | (:A {x: 1})                             | {x: 1}                             |
      | (:B {x: true, y: false})                | {x: true, y: false}                |
      | (:C {x: 'hello', y: [1, 2, 3], z: 123}) | {x: 'hello', y: [1, 2, 3], z: 123} |
    And no side effects