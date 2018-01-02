#
# Copyright (c) 2002-2018 "Neo Technology,"
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

Feature: VarLengthAcceptance

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (n1:A {name: 'n1', blocked: false}),
             (n2:B {name: 'n2', blocked: false}),
             (n3:B {name: 'n3', blocked: false}),
             (n4:C {name: 'n4', blocked: true}),
             (n1)-[:T {blocked: false}]->(n2),
             (n2)-[:T {blocked: false}]->(n3),
             (n3)-[:T {blocked: true}]->(n4)
      """

  Scenario: Handles checking properties on nodes in path - using in-pattern property value
    When executing query:
      """
      MATCH (a:A {name: 'n1'})
      MATCH (a)-[:T*]->(c {blocked: false})
      RETURN c.name
      """
    Then the result should be:
      | c.name |
      | 'n2'   |
      | 'n3'   |
    And no side effects

  Scenario: Handles checking properties on nodes in path - using ALL() function on path node properties
    When executing query:
      """
      MATCH (a:A {name: 'n1'})
      MATCH p = (a)-[:T*]->(c)
      WHERE ALL(n in nodes(p) WHERE n.blocked = false)
      RETURN c.name
      """
    Then the result should be:
      | c.name |
      | 'n2'   |
      | 'n3'   |
    And no side effects

  Scenario: Handles checking properties on nodes in multistep path - using ALL() function on path node properties
    When executing query:
      """
      MATCH (a:A {name: 'n1'})
      MATCH p = (a)-[:T*0..]->()-[:T]->(c)
      WHERE ALL(n in nodes(p) WHERE n.blocked = false)
      RETURN c.name
      """
    Then the result should be:
      | c.name |
      | 'n2'   |
      | 'n3'   |
    And no side effects

  Scenario: Handles checking properties on relationships in path - using in-pattern property value
    When executing query:
      """
      MATCH (a:A {name: 'n1'})
      MATCH (a)-[:T* {blocked: false}]->(c)
      RETURN c.name
      """
    Then the result should be:
      | c.name |
      | 'n2'   |
      | 'n3'   |
    And no side effects

  Scenario: Handles checking properties on relationships in path - using ALL() function on relationship identifier
    When executing query:
      """
      MATCH (a:A {name: 'n1'})
      MATCH (a)-[rels:T*]->(c)
      WHERE ALL(r in rels WHERE r.blocked = false)
      RETURN c.name
      """
    Then the result should be:
      | c.name |
      | 'n2'   |
      | 'n3'   |
    And no side effects

  Scenario: Handles checking properties on relationships in path - using ALL() function on path relationship properties
    When executing query:
      """
      MATCH (a:A {name: 'n1'})
      MATCH p = (a)-[:T*]->(c)
      WHERE ALL(r in relationships(p) WHERE r.blocked = false)
      RETURN c.name
      """
    Then the result should be:
      | c.name |
      | 'n2'   |
      | 'n3'   |
    And no side effects

  Scenario: Handles checking properties on relationships in multistep path - using ALL() function on path relationship properties
    When executing query:
      """
      MATCH (a:A {name: 'n1'})
      MATCH p = (a)-[:T*0..]->()-[:T*]->(c)
      WHERE ALL(r in relationships(p) WHERE r.blocked = false)
      RETURN c.name
      """
    Then the result should be:
      | c.name |
      | 'n2'   |
      | 'n3'   |
      | 'n3'   |
    And no side effects

