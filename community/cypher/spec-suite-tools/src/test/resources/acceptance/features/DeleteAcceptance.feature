#
# Copyright (c) 2002-2019 "Neo4j,"
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

Feature: DeleteAcceptance

  Scenario: Return properties from deleted node
    Given an empty graph
    And having executed:
      """
      CREATE (:L {prop1: 42, prop2: 1337})
      """
    When executing query:
      """
      MATCH (n:L)
      WITH n, properties(n) AS props
      DELETE n
      RETURN props
      """
    Then the result should be:
      | props                    |
      | {prop1: 42, prop2: 1337} |
    And the side effects should be:
      | -nodes      | 1 |
      | -labels     | 1 |
      | -properties | 2 |

