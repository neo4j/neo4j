#
# Copyright (c) 2002-2017 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
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

Feature: ReturnAcceptance

  Scenario: Filter should work
    Given an empty graph
    And having executed:
      """
      CREATE (a {foo: 1})-[:T]->({foo: 1}),
        (a)-[:T]->({foo: 2}),
        (a)-[:T]->({foo: 3})
      """
    When executing query:
      """
      MATCH (a {foo: 1})
      MATCH p=(a)-->()
      RETURN filter(x IN nodes(p) WHERE x.foo > 2) AS n
      """
    Then the result should be:
      | n            |
      | [({foo: 3})] |
      | []           |
      | []           |
    And no side effects

  Scenario: Accessing a list with null should return null
    Given any graph
    When executing query:
      """
      RETURN [1, 2, 3][null] AS result
      """
    Then the result should be:
      | result |
      | null   |
    And no side effects

  Scenario: Accessing a list with null as lower bound should return null
    Given any graph
    When executing query:
      """
      RETURN [1, 2, 3][null..5] AS result
      """
    Then the result should be:
      | result |
      | null   |
    And no side effects

  Scenario: Accessing a list with null as upper bound should return null
    Given any graph
    When executing query:
      """
      RETURN [1, 2, 3][1..null] AS result
      """
    Then the result should be:
      | result |
      | null   |
    And no side effects

  Scenario: Accessing a map with null should return null
    Given any graph
    When executing query:
      """
      RETURN {key: 1337}[null] AS result
      """
    Then the result should be:
      | result |
      | null   |
    And no side effects
