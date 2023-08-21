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

Feature: GraphPatternAcceptance

  Background:
    Given an empty graph

  Scenario: The same path variable cannot be used for more than one path pattern in a graph pattern
  When executing query:
    """
     MATCH p = (a)-[b]->(c), p = (f)-[e]->(g)
     RETURN *
    """
  Then a SyntaxError should be raised at compile time: VariableAlreadyBound

  Scenario: Subpath variable cannot appear more than once in a graph pattern
    Given an empty graph
    When executing query:
      """
      MATCH (p = ()-[:R]->())+ (p = ()-[:S]->())+
      RETURN p
      """
    Then a SyntaxError should be raised at compile time: VariableAlreadyBound

  Scenario: The same subpath variable can only be used within one path pattern in a graph pattern
    When executing query:
    """
     MATCH (a)(sp = ()-[h]->())+(j), (b)(sp = (x)-[y]->(z))*
     RETURN *
    """
    Then a SyntaxError should be raised at compile time: VariableAlreadyBound

  Scenario: The same subpath variable can only be used once within a path pattern
    When executing query:
    """
     MATCH (sp = ()-[h]->())+(j)(sp = (q)-->())+, (x)-[y]->(z)
     RETURN *
    """
    Then a SyntaxError should be raised at compile time: VariableAlreadyBound

  Scenario: A group variable may only appear in a single quantified sub path pattern in a graph pattern - grouped variable, singleton variable
    When executing query:
    """
     MATCH ((a)-[b]->(c))*(d), (f)-[e]->(a)
     RETURN *
    """
    Then a SyntaxError should be raised at compile time: VariableAlreadyBound

  Scenario: A group variable may only appear in a single quantified sub path pattern in a graph pattern - grouped variable, grouped variable (QPP)
    When executing query:
    """
     MATCH ((a)-[b]->(c))*(d), (h)-[e]->() ((a)-[f]->(g)){2,}
     RETURN *
    """
    Then a SyntaxError should be raised at compile time: VariableAlreadyBound

  Scenario: A group variable may only appear in a single quantified sub path pattern in a graph pattern - grouped variable, grouped variable (quantified relationship)
    When executing query:
    """
     MATCH ((a)-[b]->(c))*(d), (g)-[b]->+(f)
     RETURN *
    """
    Then a SyntaxError should be raised at compile time: VariableAlreadyBound

  Scenario: A group variable may only appear in a single quantified sub path pattern in a graph pattern - grouped variable, grouped variable (var-length relationship)
    When executing query:
    """
     MATCH ((a)-[b]->(c))*(d), (g)-[b*]->(f)
     RETURN *
    """
    # This does not check whether the relationship between g and f is variable-length and therefore triggers an error detailing that the relationship variable
    # is both group variable and singleton. This is fine, as we prohibit combining variable-length + QPP anyways.
    Then a SyntaxError should be raised at compile time: *

  Scenario: Conjunction of path patterns to form "T" pattern
    Given having executed:
      """
        CREATE (:A)-[:R]->(b:B)-[:S]->(:C),
               (:D)-[:T]->(b)-[:U]->(:E)
      """
    When executing query:
      """
        MATCH (n0)-->(n1)-->(n2), (n1)-->(n3)
        RETURN n0, n1, n2, n3
      """
    Then the result should be, in any order:
      | n0   | n1   | n2   | n3   |
      | (:A) | (:B) | (:C) | (:E) |
      | (:D) | (:B) | (:E) | (:C) |
      | (:A) | (:B) | (:E) | (:C) |
      | (:D) | (:B) | (:C) | (:E) |



  Scenario: Conjunction of path patterns without implicit join returns Cartesian product
    Given having executed:
      """
        CREATE (:A)-[:R]->(b:B)-[:S]->(:C),
               (:D)-[:T]->(b)-[:U]->(:E)
      """
    When executing query:
      """
        MATCH (n0)-->(n1:B), (n2:B)-->(n3)
        RETURN n0, n1, n2, n3
      """
    Then the result should be, in any order:
      | n0   | n1   | n2   | n3   |
      | (:A) | (:B) | (:B) | (:C) |
      | (:A) | (:B) | (:B) | (:E) |
      | (:D) | (:B) | (:B) | (:C) |
      | (:D) | (:B) | (:B) | (:E) |

  Scenario: Minimum node count for each top level path pattern in a graph pattern should be 1
    When executing query:
    """
     MATCH ((f)-[i]->(g))+, ((k)-[m]->(n))*
     RETURN *
    """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Singleton node variable can be repeated across graph pattern to express implicit join
    Given having executed:
      """
        CREATE (:A)-[:R]->(:B)-[:S]->(:C)-[:T]->(d:D),
               (:E)-[:U]->(:F)<-[:V]-(d)
      """
    When executing query:
    """
     MATCH (a)-[q]-(b)-[f]-(c)-->(n), (x)-[r]->(z)<-[]-(n)
     RETURN *
    """
    Then the result should be, in any order:
      | a    | b    | c    | f    | n    | q    | r    | x    | z    |
      | (:A) | (:B) | (:C) | [:S] | (:D) | [:R] | [:U] | (:E) | (:F) |

  Scenario: Node variable can be repeated in a path pattern inside a QPP
    Given having executed:
      """
        CREATE (a:A)-[:R]->(:B)-[:R]->(:C)-[:R]->(d:D),
        (a)-[:R]->(d)
      """
    When executing query:
    """
     MATCH ((a)-[q]->(b)-[c]->(d)-[r]->(f)<--(a))+
     RETURN *
    """
    Then the result should be, in any order:
      | a      | b      | c      | d      | f      | q      | r      |
      | [(:A)] | [(:B)] | [[:R]] | [(:C)] | [(:D)] | [[:R]] | [[:R]] |

  Scenario: Singleton element variable can be repeated across graph pattern to express implicit join
    Given having executed:
      """
        CREATE (a:A)-[:R]->(b:B)-[:S]->(:C)-[:T]->(d:D),
               (d)-[:U]->(b)
      """
    When executing query:
    """
     MATCH (a)-[r]-(b)-[f]-(c)-->(n), (x)-[r]->(z)<-[]-(n)
     RETURN *
    """
    Then the result should be, in any order:
      | a    | b    | c    | f    | n    | r    | x    | z    |

  Scenario: Element variable can be repeated in a path pattern inside a QPP
    Given having executed:
      """
        CREATE (a:A)-[:R]->(b:B),
               (b)-[:C]->(a)
      """
    When executing query:
    """
     MATCH ((a)-[r]->(b)-[c]->(d)-[r]->(f)<--(a))+
     RETURN *
    """
    Then the result should be, in any order:
      | a    | b    | c    | d    | f    | r    |

  Scenario: Group variable cannot be repeated across graph pattern
    When executing query:
    """
     MATCH ((a)-[r]->(b))+, (b)-[c]->(d)
     RETURN *
    """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Lateral joins are allowed
    Given having executed:
      """
        CREATE (a:A)-[:R]->(:B),
        (a)-[:S]->(:C)
      """
    When executing query:
    """
     MATCH (a)-[r]->(b), (c)-[s]->(d)
     MATCH (a)-[t]->(e), (c)-[u]->(f)
     RETURN *
    """
    Then the result should be, in any order:
      | a    | b    | c    | d    | e    | f    | r    | s    | t    | u    |
      | (:A) | (:B) | (:A) | (:C) | (:C) | (:B) | [:R] | [:S] | [:S] | [:R] |
      | (:A) | (:B) | (:A) | (:C) | (:B) | (:C) | [:R] | [:S] | [:R] | [:S] |
      | (:A) | (:C) | (:A) | (:B) | (:C) | (:B) | [:S] | [:R] | [:S] | [:R] |
      | (:A) | (:C) | (:A) | (:B) | (:B) | (:C) | [:S] | [:R] | [:R] | [:S] |