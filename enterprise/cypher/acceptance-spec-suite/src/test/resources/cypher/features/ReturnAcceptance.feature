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

  Scenario: LIMIT 0 should stop side effects
    Given an empty graph
    When executing query:
      """
      CREATE (n)
      RETURN n
      LIMIT 0
      """
    Then the result should be:
      | n            |
    And no side effects

  Scenario: Accessing list with null should yield a null
    Given an empty graph
    When executing query:
      """
      WITH [1,2,3] AS list RETURN list[null]
      """
    Then the result should be:
      | list[null] |
      | null       |
    And no side effects

