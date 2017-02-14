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

Feature: MatchAcceptance

  Scenario: Filter on path nodes
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {foo: 'bar'})-[:REL]->(b:B {foo: 'bar'})-[:REL]->(c:C {foo: 'bar'})-[:REL]->(d:D {foo: 'bar'})
      """
    When executing query:
      """
      MATCH p = (pA)-[:REL*3..3]->(pB)
      WHERE all(i IN nodes(p) WHERE i.foo = 'bar')
      RETURN pB
      """
    Then the result should be:
      | pB                |
      | (:D {foo: 'bar'}) |
    And no side effects

  Scenario: Filter with AND/OR
    Given an empty graph
    And having executed:
      """
      CREATE (:X   {foo: 1}),
             (:Y   {foo: 2}),
             (:Y   {id: 42, foo: 3}),
             (:Y:X {id: 42, foo: 4})
      """
    When executing query:
      """
      MATCH (n)
      WHERE n:X OR (n:Y AND n.id = 42)
      RETURN n.foo ORDER BY n.foo
      """
    Then the result should be:
      | n.foo |
      | 1     |
      | 3     |
      | 4     |
    And no side effects
