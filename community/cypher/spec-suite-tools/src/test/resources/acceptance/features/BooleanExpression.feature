#
# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
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

#encoding: utf-8

Feature: BooleanExpression

  Scenario: Combining IN predicates intersected with OR
    Given any graph
    When executing query:
      """
      RETURN (1 IN [1] AND FALSE) OR 1 in [2] AS result
      """
    Then the result should be:
      | result |
      | false  |
    And no side effects

  Scenario: Combining IN predicates intersected with AND
    Given any graph
    When executing query:
      """
      RETURN (1 IN [2] OR TRUE) AND 1 in [1] AS result
      """
    Then the result should be:
      | result |
      | true   |
    And no side effects

