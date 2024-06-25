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

Feature: RemoveAcceptance

  Scenario: Does not observe row-by-row visibility
    Given an empty graph
    And having executed:
      """
      CREATE ({prop: true})-[:REL]->()<-[:REL]-({prop: true})
      """
    When executing query:
      """
      MATCH (n)-->()
      REMOVE ([(n {prop: true})--()--({prop: true}) | n][0]).prop
      """
    Then the side effects should be:
      | -properties | 2 |

  Scenario: Does not observe item-by-item visibility
    Given an empty graph
    And having executed:
      """
      CREATE (n:N {prop: true}), (m {prop: true})
      """
    When executing query:
      """
      MATCH (n:N), (m {prop: true})
      REMOVE
        n:N,
        (CASE WHEN n:N THEN n ELSE m END).prop
      """
    Then the side effects should be:
      | -labels     | 1 |
      | -properties | 1 |

  Scenario: Remove Dynamic Labels should fail on feature flag
    Given an empty graph
    When executing query:
      """
      MATCH (n)
      REMOVE n:$("Label")
      """
    Then a SyntaxError should be raised at compile time: *