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

Feature: GQLAliasFunctionNameAcceptance

  Scenario: Char Length
    Given an empty graph
    When executing query:
      """
      RETURN size('abc') as a, char_length('abc') as b, character_length('abc') as c
      """
    Then the result should be, in any order:
      | a | b | c |
      | 3 | 3 | 3 |
    And no side effects

  Scenario: Lower
    Given an empty graph
    When executing query:
      """
      RETURN toLower('ABC') as a, lower('ABC') as b
      """
    Then the result should be, in any order:
      | a     | b     |
      | 'abc' | 'abc' |
    And no side effects

  Scenario: Upper
    Given an empty graph
    When executing query:
      """
      RETURN toUpper('abc') as a, upper('abc') as b
      """
    Then the result should be, in any order:
      | a     | b     |
      | 'ABC' | 'ABC' |
    And no side effects