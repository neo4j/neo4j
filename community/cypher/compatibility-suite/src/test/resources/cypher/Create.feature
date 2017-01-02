#
# Copyright (c) 2002-2017 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
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

Feature: Create

  Scenario: Creating a node
    Given any graph
    When executing query: CREATE ()
    Then the result should be empty
    And the side effects should be:
      | +nodes | 1 |

  Scenario: Creating two nodes
    Given any graph
    When executing query: CREATE (), ()
    Then the result should be empty
    And the side effects should be:
      | +nodes | 2 |

  Scenario: Creating two nodes and a relationship
    Given any graph
    When executing query: CREATE ()-[:TYPE]->()
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |

  Scenario: Creating a node with a label
    Given any graph
    When executing query: CREATE (:Label)
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 1 |
      | +labels | 1 |

  Scenario: Creating a node with a property
    Given any graph
    When executing query: CREATE ({created: true})
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 1 |
