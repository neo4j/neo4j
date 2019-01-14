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

Feature: Create

  Scenario: Dependencies in creating multiple nodes
    Given an empty graph
    When executing query:
      """
      CREATE (a {prop: 1}), ({prop: a.prop})
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 2 |
      | +properties | 2 |

  Scenario: Dependencies in creating multiple rels
    Given an empty graph
    When executing query:
      """
      CREATE ()-[r:R {prop:1}]->(), ()-[q:R {prop:r.prop}]->()
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 4 |
      | +relationships | 2 |
      | +properties    | 2 |

