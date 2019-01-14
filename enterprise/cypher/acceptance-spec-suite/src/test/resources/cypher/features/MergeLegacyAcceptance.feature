#
# Copyright (c) 2002-2019 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j Enterprise Edition. The included source
# code can be redistributed and/or modified under the terms of the
# GNU AFFERO GENERAL PUBLIC LICENSE Version 3
# (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
# Commons Clause, as found in the associated LICENSE.txt file.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# Neo4j object code can be licensed independently from the source
# under separate terms from the AGPL. Inquiries can be directed to:
# licensing@neo4j.com
#
# More information is also available at:
# https://neo4j.com/licensing/
#

#encoding: utf-8

Feature: MergeLegacyAcceptance

  Scenario: Using a single bound node
    Given an empty graph
    And having executed:
      """
      CREATE (:A)
      """
    When executing query:
      """
      MATCH (a:A)
      MERGE (a)-[r:TYPE]->()
      RETURN count(r)
      """
    Then the result should be:
      | count(r) |
      | 1        |
    And the side effects should be:
      | +nodes         | 1 |
      | +relationships | 1 |

  Scenario: Using a longer pattern
    Given an empty graph
    And having executed:
      """
      CREATE (:A)
      """
    When executing query:
      """
      MATCH (a:A)
      MERGE (a)-[r:TYPE]->()<-[:TYPE]-()
      RETURN count(r)
      """
    Then the result should be:
      | count(r) |
      | 1        |
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 2 |

  Scenario: Using bound nodes in mid-pattern
    Given an empty graph
    And having executed:
      """
      CREATE (:B)
      """
    When executing query:
      """
      MATCH (b:B)
      MERGE (a)-[r1:TYPE]->(b)<-[r2:TYPE]-(c)
      RETURN type(r1), type(r2)
      """
    Then the result should be:
      | type(r1) | type(r2) |
      | 'TYPE'   | 'TYPE'   |
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 2 |

  Scenario: Using bound nodes in mid-pattern when pattern partly matches
    Given an empty graph
    And having executed:
      """
      CREATE (a:A), (b:B)
      CREATE (a)-[:TYPE]->(b)
      """
    When executing query:
      """
      MATCH (b:B)
      MERGE (a:A)-[r1:TYPE]->(b)<-[r2:TYPE]-(c:C)
      RETURN type(r1), type(r2)
      """
    Then the result should be:
      | type(r1) | type(r2) |
      | 'TYPE'   | 'TYPE'   |
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 2 |
      | +labels        | 1 |

  Scenario: Introduce named paths
    Given an empty graph
    When executing query:
      """
      MERGE (a:A)
      MERGE p = (a)-[:R]->()
      RETURN p
      """
    Then the result should be:
      | p               |
      | <(:A)-[:R]->()> |
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |
      | +labels        | 1 |

  Scenario: Unbound pattern
    Given an empty graph
    When executing query:
      """
      MERGE ({name: 'Andres'})-[:R]->({name: 'Emil'})
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |
      | +properties    | 2 |

  Scenario: Fail when imposing new predicates on a variable that is already bound
    Given any graph
    When executing query:
      """
      MERGE (a:Foo)-[r:KNOWS]->(a:Bar)
      """
    Then a SyntaxError should be raised at compile time: VariableAlreadyBound
