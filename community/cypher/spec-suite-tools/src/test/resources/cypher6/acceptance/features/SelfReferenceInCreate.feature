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
Feature: SelfReferenceInCreate

  @allowCustomErrors
  Scenario: Node self reference within pattern is not allowed
    Given an empty graph
    When executing query:
    """
    CREATE (a)-[:REL]->(b {prop: a.prop})
    """
    Then a SyntaxError should be raised at compile time: The Node variable 'a' is referencing a Node that is created in the same CREATE clause which is not allowed. Please only reference variables created in earlier clauses.

  @allowCustomErrors
  Scenario: Relationship self reference within pattern is not allowed
    Given an empty graph
    When executing query:
    """
    CREATE (a)-[r:REL]->(b {prop: r.prop})
    """
    Then a SyntaxError should be raised at compile time: The Relationship variable 'r' is referencing a Relationship that is created in the same CREATE clause which is not allowed. Please only reference variables created in earlier clauses.

  @allowCustomErrors
  Scenario: Node self reference across patterns is not allowed
    Given an empty graph
    When executing query:
    """
    CREATE (a), (b {prop: a.prop})
    """
    Then a SyntaxError should be raised at compile time: Creating an entity (a) and referencing that entity in a property definition in the same CREATE is not allowed. Only reference variables created in earlier clauses.

  @allowCustomErrors
  Scenario: Relationship self reference across patterns is not allowed
    Given an empty graph
    When executing query:
    """
    CREATE (a)-[r:REL]->(b), (c {prop: r.prop})
    """
    Then a SyntaxError should be raised at compile time: Creating an entity (r) and referencing that entity in a property definition in the same CREATE is not allowed. Only reference variables created in earlier clauses.
