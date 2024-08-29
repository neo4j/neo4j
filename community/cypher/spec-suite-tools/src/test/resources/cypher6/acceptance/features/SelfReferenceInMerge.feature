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
Feature: SelfReferenceInMerge

  @allowCustomErrors
  Scenario: Node self reference is not allowed
    Given an empty graph
    When executing query:
    """
    MERGE (a)-[:REL]->(b {prop: a.prop})
    """
    Then a SyntaxError should be raised at compile time: The Node variable 'a' is referencing a Node that is created in the same MERGE clause which is not allowed. Please only reference variables created in earlier clauses.

  @allowCustomErrors
  Scenario: Relationship self reference is not allowed
    Given an empty graph
    When executing query:
    """
    MERGE (a)-[r:REL]->(b {prop: r.prop})
    """
    Then a SyntaxError should be raised at compile time: The Relationship variable 'r' is referencing a Relationship that is created in the same MERGE clause which is not allowed. Please only reference variables created in earlier clauses.
