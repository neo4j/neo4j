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

Feature: ProcedureAcceptance

  Scenario: Standalone void-procedure call
    Given an empty graph
    And there exists a procedure proc.void() :: ():
      |
    When executing query:
      """
      CALL proc.void()
      """
    Then the result should be empty
    And no side effects

  Scenario: Void-procedure call after a MATCH clause
    Given an empty graph
    And having executed:
    """
      CREATE (:Node)
      CREATE (:Node)
     """
    And there exists a procedure proc.void() :: ():
      |
    When executing query:
      """
      MATCH (node:Node)
      CALL proc.void()
      """
    Then the result should be empty
    And no side effects

  @allowCustomErrors
  Scenario: Non-void-procedure call after a MATCH clause fails
    Given an empty graph
    And having executed:
    """
      CREATE (:Node)
      CREATE (:Node)
     """
    And there exists a procedure proc.yielding(in :: INTEGER?) :: (out :: INTEGER?):
      | in | out |
    When executing query:
      """
      MATCH (node:Node)
      CALL proc.yielding(5)
      """
    Then a SyntaxError should be raised at compile time: Procedure call inside a query does not support naming results implicitly (name explicitly using `YIELD` instead)

  Scenario: Procedure call after a MATCH clause fails when YIELD also present
    Given an empty graph
    And having executed:
    """
      CREATE (:Node)
      CREATE (:Node)
     """
    And there exists a procedure proc.yielding(in :: INTEGER?) :: (out :: INTEGER?):
      | in | out |
    When executing query:
      """
      MATCH (node:Node)
      CALL proc.yielding(5) YIELD out
      """
    Then a SyntaxError should be raised at compile time: InvalidClauseComposition
