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

Feature: OptionalMatchAcceptance

  Scenario: Id on null
    Given an empty graph
    And having executed:
      """
      UNWIND range(1,10) AS i CREATE (:L1 {prop:i})<-[:R]-(:L2)
      """
    When executing query:
      """
      MATCH (n1 :L1 {prop: 3}) OPTIONAL MATCH (n2 :L2)<-[r]-(n1) RETURN id(n2), id(r)
      """
    Then the result should be:
      | id(n2) | id(r) |
      | null   | null  |
    And no side effects

  Scenario: type on null
    Given an empty graph
    And having executed:
      """
      UNWIND range(1,10) AS i CREATE (:L1 {prop:i})<-[:R]-(:L2)
      """
    When executing query:
      """
      MATCH (n1 :L1 {prop: 3}) OPTIONAL MATCH (n2 :L2)<-[r]-(n1) RETURN type(r)
      """
    Then the result should be:
      | type(r) |
      | null    |
    And no side effects

  Scenario: optional equality with boolean lists
    Given an empty graph
    And having executed:
      """
      CREATE ({prop: [false]})
      """
    When executing query:
      """
      OPTIONAL MATCH (n {prop: false}) RETURN n
      """
    Then the result should be:
      | n    |
      | null |
    And no side effects
