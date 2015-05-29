#
# Copyright (c) 2002-2015 "Neo Technology,"
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

Feature: MergeAcceptanceTest

  Scenario: identifiers of deleted nodes should not be able to cause errors in later merge actions that do not refer to them
    Given init: CREATE (:A)-[:R]->(:B)-[:R]->(:C)
    When running: MATCH (a:A)-[ab]->(b:B)-[bc]->(c:C) DELETE ab, bc, b, c MERGE (newB:B) MERGE (a)-[:REL]->(newB) MERGE (newC:C) MERGE (newB)-[:REL]->(newC);
    Then result:
      |  |

  Scenario: merges should not be able to match on deleted nodes
    Given init: CREATE (:A { property: 0 }), (:A { property: 0 });
    When running: MATCH (a:A) DELETE a MERGE (a2:A) RETURN a2;
    Then result:
      | a2   |
      | (:A) |
      | (:A) |

  Scenario: merges should not be able to match on deleted relationships
    Given init: CREATE (a)-[:R]->(b), (a)-[:R]->(b);
    When running: MATCH (a)-[r:R]->(b) DELETE r MERGE (a)-[r2:R]->(b) RETURN r2;
    Then result:
      | r2   |
      | [:R] |
      | [:R] |
