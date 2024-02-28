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

Feature: PathSelectorAcceptance

  Background:
    Given an empty graph

  Scenario Outline: Find same paths with different noise words (PATH, PATHS, GROUP vs GROUPS) - no predicate
    And having executed:
      """
      CREATE (a:A), (b:B), (c:C), (d:D), (x:X),
        (a)-[:R]->(b)-[:R]->(c)-[:R]->(d),
        (a)-[:R]->(x)-[:R]->(d)
      """
    When executing query:
      """
      MATCH p = <pathSelector> (:A)-->+(:D)
      WITH nodes(p) AS n ORDER BY size(n)
      RETURN collect(n) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    Examples:
      | pathSelector       | result                                         |
      | ANY SHORTEST       | [[(:A), (:X), (:D)]]                           |
      | ANY SHORTEST PATH  | [[(:A), (:X), (:D)]]                           |
      | ANY SHORTEST PATHS | [[(:A), (:X), (:D)]]                           |
      | SHORTEST 1         | [[(:A), (:X), (:D)]]                           |
      | SHORTEST 1 PATH    | [[(:A), (:X), (:D)]]                           |
      | SHORTEST 1 PATHS   | [[(:A), (:X), (:D)]]                           |
      | SHORTEST 2         | [[(:A), (:X), (:D)], [(:A), (:B), (:C), (:D)]] |
      | SHORTEST 2 PATH    | [[(:A), (:X), (:D)], [(:A), (:B), (:C), (:D)]] |
      | SHORTEST 2 PATHS   | [[(:A), (:X), (:D)], [(:A), (:B), (:C), (:D)]] |
      | SHORTEST 3 PATH    | [[(:A), (:X), (:D)], [(:A), (:B), (:C), (:D)]] |
      | SHORTEST 3 PATHS   | [[(:A), (:X), (:D)], [(:A), (:B), (:C), (:D)]] |
      | SHORTEST 3         | [[(:A), (:X), (:D)], [(:A), (:B), (:C), (:D)]] |
      | ALL SHORTEST       | [[(:A), (:X), (:D)]]                           |
      | ALL SHORTEST PATH  | [[(:A), (:X), (:D)]]                           |
      | ALL SHORTEST PATHS | [[(:A), (:X), (:D)]]                           |
      | SHORTEST GROUP     | [[(:A), (:X), (:D)]]                           |
      | SHORTEST GROUPS    | [[(:A), (:X), (:D)]]                           |
      | SHORTEST 1 GROUP   | [[(:A), (:X), (:D)]]                           |
      | SHORTEST 1 GROUPS  | [[(:A), (:X), (:D)]]                           |
      | SHORTEST 2 GROUP   | [[(:A), (:X), (:D)], [(:A), (:B), (:C), (:D)]] |
      | SHORTEST 2 GROUPS  | [[(:A), (:X), (:D)], [(:A), (:B), (:C), (:D)]] |
      | SHORTEST 3 GROUP   | [[(:A), (:X), (:D)], [(:A), (:B), (:C), (:D)]] |
      | SHORTEST 3 GROUPS  | [[(:A), (:X), (:D)], [(:A), (:B), (:C), (:D)]] |

  Scenario Outline: Find ANY paths with different noise words (PATH, PATHS) - no predicate
    And having executed:
      """
      CREATE (a:A), (b:B), (c:C), (d:D), (x:X),
        (a)-[:R]->(b)-[:R]->(c)-[:R]->(d),
        (a)-[:R]->(x)-[:R]->(d)
      """
    When executing query:
      """
      MATCH <pathSelector> (:A)-->+(:D)
      RETURN count(*) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    Examples:
      | pathSelector | result |
      | ANY          | 1      |
      | ANY PATH     | 1      |
      | ANY PATHS    | 1      |
      | ANY 1        | 1      |
      | ANY 1 PATH   | 1      |
      | ANY 1 PATHS  | 1      |
      | ANY 2        | 2      |
      | ANY 2 PATH   | 2      |
      | ANY 2 PATHS  | 2      |
      | ANY 3        | 2      |
      | ANY 3 PATH   | 2      |
      | ANY 3 PATHS  | 2      |

  Scenario Outline: Find same paths with different noise words (PATH, PATHS, GROUP vs GROUPS) - predicate
    And having executed:
      """
      CREATE (a:A), (b:B), (c:C), (d:D), (x:X),
        (a)-[:R]->(b)-[:R]->(c)-[:R]->(d),
        (a)-[:R]->(x)-[:R]->(d)
      """
    When executing query:
      """
      MATCH p = <pathSelector> (:A)(()-->(:!X))+(:D)
      WITH nodes(p) AS n ORDER BY size(n)
      RETURN collect(n) AS result
      """
    Then the result should be, in any order:
      | result                     |
      | [[(:A), (:B), (:C), (:D)]] |
    Examples:
      | pathSelector            |
      | ANY SHORTEST            |
      | ANY SHORTEST PATH       |
      | ANY SHORTEST PATHS      |
      | SHORTEST 1              |
      | SHORTEST 1 PATH         |
      | SHORTEST 1 PATHS        |
      | SHORTEST 2              |
      | SHORTEST 2 PATH         |
      | SHORTEST 2 PATHS        |
      | ALL SHORTEST            |
      | ALL SHORTEST PATH       |
      | ALL SHORTEST PATHS      |
      | SHORTEST GROUP          |
      | SHORTEST PATH GROUP     |
      | SHORTEST PATHS GROUP    |
      | SHORTEST GROUPS         |
      | SHORTEST GROUPS         |
      | SHORTEST GROUPS         |
      | SHORTEST 1 GROUP        |
      | SHORTEST 1 PATH GROUP   |
      | SHORTEST 1 PATHS GROUP  |
      | SHORTEST 1 GROUPS       |
      | SHORTEST 1 PATH GROUPS  |
      | SHORTEST 1 PATHS GROUPS |
      | SHORTEST 2 GROUP        |
      | SHORTEST 2 GROUPS       |
      | ANY                     |
      | ANY PATH                |
      | ANY PATHS               |
      | ANY 1                   |
      | ANY 1 PATH              |
      | ANY 1 PATHS             |
      | ANY 2                   |
      | ANY 2 PATH              |
      | ANY 2 PATHS             |

  Scenario Outline: Find ANY paths with different noise words (PATH, PATHS) - predicate
    And having executed:
      """
      CREATE (a:A), (b:B), (c:C), (d:D), (x:X),
        (a)-[:R]->(b)-[:R]->(c)-[:R]->(d),
        (a)-[:R]->(x)-[:R]->(d)
      """
    When executing query:
      """
      MATCH <pathSelector> (:A)(()-->(:!X))+(:D)
      RETURN count(*) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    Examples:
      | pathSelector | result |
      | ANY          | 1      |
      | ANY PATH     | 1      |
      | ANY PATHS    | 1      |
      | ANY 1        | 1      |
      | ANY 1 PATH   | 1      |
      | ANY 1 PATHS  | 1      |
      | ANY 2        | 1      |
      | ANY 2 PATH   | 1      |
      | ANY 2 PATHS  | 1      |

  Scenario Outline: Element pattern predicates are applied before path selector
    And having executed:
      """
        CREATE (a:A), (b:B),
          (a)-[:R]->()-[:R]->()-[:R]->(b),
          (a)-[:R]->()-[:X]->(b)
      """
    When executing query:
      """
      MATCH p = <pathSelector> (:A)-[r WHERE r:!X]->+(:B)
      RETURN count(*) AS result
      """
    Then the result should be, in any order:
      | result   |
      | 1        |
    Examples:
      | pathSelector       |
      | ANY SHORTEST       |
      | SHORTEST 1         |
      | SHORTEST 2         |
      | ALL SHORTEST       |
      | SHORTEST GROUP     |
      | SHORTEST 1 GROUP   |
      | SHORTEST 2 GROUP   |
      | ANY                |
      | ANY 1              |
      | ANY 2              |

  Scenario Outline: Path pattern predicates are applied before path selector
    And having executed:
      """
        CREATE (a:A), (b:B),
          (a)-[:R]->()-[:R]->()-[:R]->(b),
          (a)-[:R]->()-[:X]->(b)
      """
    When executing query:
      """
      MATCH <pathSelector> ((:A)-[r]->+(:B) WHERE none(rel IN r WHERE rel:X))
      RETURN count(*) AS result
      """
    Then the result should be, in any order:
      | result   |
      | 1        |
    Examples:
      | pathSelector       |
      | ANY SHORTEST       |
      | SHORTEST 1         |
      | SHORTEST 2         |
      | ALL SHORTEST       |
      | SHORTEST GROUP     |
      | SHORTEST 1 GROUP   |
      | SHORTEST 2 GROUP   |
      | ANY                |
      | ANY 1              |
      | ANY 2              |

  Scenario Outline: Graph pattern predicates are applied after path selector - un-parenthesised
    And having executed:
      """
        CREATE (a:A), (b:B),
          (a)-[:R]->()-[:R]->()-[:R]->(b),
          (a)-[:R]->()-[:X]->(b)
      """
    When executing query:
      """
      MATCH <pathSelector> (:A)-[r]->+(:B) WHERE none(rel IN r WHERE rel:X)
      RETURN count(*) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    Examples:
      | pathSelector       | result   |
      | ANY SHORTEST       | 0        |
      | SHORTEST 1         | 0        |
      | SHORTEST 2         | 1        |
      | ALL SHORTEST       | 0        |
      | SHORTEST GROUP     | 0        |
      | SHORTEST 1 GROUP   | 0        |
      | SHORTEST 2 GROUP   | 1        |

  Scenario Outline: Graph pattern predicates are applied after path selector - parenthesised
    And having executed:
      """
        CREATE (a:A), (b:B),
          (a)-[:R]->()-[:R]->()-[:R]->(b),
          (a)-[:R]->()-[:X]->(b)
      """
    When executing query:
      """
      MATCH <pathSelector> ((:A)-[r]->+(:B)) WHERE none(rel IN r WHERE rel:X)
      RETURN count(*) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    Examples:
      | pathSelector       | result   |
      | ANY SHORTEST       | 0        |
      | SHORTEST 1         | 0        |
      | SHORTEST 2         | 1        |
      | ALL SHORTEST       | 0        |
      | SHORTEST GROUP     | 0        |
      | SHORTEST 1 GROUP   | 0        |
      | SHORTEST 2 GROUP   | 1        |

  Scenario Outline: Different path selectors return correct number of paths where multiple paths have same length
    #       ┌─┐
    #     ┌─┤S├─┐
    #     │ └─┘ │
    #    ┌▼┐   ┌▼┐
    #  ┌─┤S├─┬─┤b├─┐
    #  │ └─┘ │ └─┘ │
    # ┌▼┐   ┌▼┐   ┌▼┐
    # │c│   │X│   │e│
    # └┬┘   └┬┘   └┬┘
    #  │ ┌─┐ │ ┌─┐ │
    #  └─►f◄─┴─►g◄─┘
    #    └┬┘   └┬┘
    #     │ ┌─┐ │
    #     └─►T◄─┘
    #       └─┘
    And having executed:
      """
      CREATE (s1:S {n: 's1'}), (s2:S {n: 's2'}), (t1:T {n: 't1'}), (x:X),
        (s1)-[:R]->(s2)-[:R]->(c)-[:R]->(f)-[:R]->(t1),
        (s1)-[:R]->(b)-[:R]->(e)-[:R]->(g)-[:R]->(t1),
        (s2)-[:R]->(x)-[:R]->(f),
        (b)-[:R]->(x)-[:R]->(g)
      """
    When executing query:
      """
      MATCH p = <pathSelector> (:S) (()--(<filter>))+ (:T)
      WITH [n in nodes(p) | n] as nodes, size(relationships(p)) AS pathLength
      WITH nodes[0].n AS first, nodes[-1].n AS last, pathLength, count(*) AS numMatches
      ORDER BY first, last, pathLength
      WITH first+'-'+last AS partition, collect([pathLength, numMatches]) AS matches
      RETURN collect([partition, matches]) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    Examples:
      | pathSelector       | filter | result                                                                              |
      | ANY SHORTEST       |        | [['s1-t1', [[4, 1]]], ['s2-t1', [[3, 1]]]]                                          |
      | SHORTEST 1         |        | [['s1-t1', [[4, 1]]], ['s2-t1', [[3, 1]]]]                                          |
      | SHORTEST 6         |        | [['s1-t1', [[4, 6]]], ['s2-t1', [[3, 3], [5, 3]]]]                                  |
      | SHORTEST 7         |        | [['s1-t1', [[4, 6], [6, 1]]], ['s2-t1', [[3, 3], [5, 4]]]]                          |
      | ALL SHORTEST       |        | [['s1-t1', [[4, 6]]], ['s2-t1', [[3, 3]]]]                                          |
      | SHORTEST GROUP     |        | [['s1-t1', [[4, 6]]], ['s2-t1', [[3, 3]]]]                                          |
      | SHORTEST 1 GROUP   |        | [['s1-t1', [[4, 6]]], ['s2-t1', [[3, 3]]]]                                          |
      | SHORTEST 4 GROUPS  |        | [['s1-t1', [[4, 6], [6, 4], [8, 6]]], ['s2-t1', [[3, 3], [5, 5], [7, 6], [9, 10]]]] |
      | ANY SHORTEST       | :!X    | [['s1-t1', [[4, 1]]], ['s2-t1', [[3, 1]]]]                                          |
      | SHORTEST 1         | :!X    | [['s1-t1', [[4, 1]]], ['s2-t1', [[3, 1]]]]                                          |
      | SHORTEST 2         | :!X    | [['s1-t1', [[4, 2]]], ['s2-t1', [[3, 1], [5, 1]]]]                                  |
      | SHORTEST 3         | :!X    | [['s1-t1', [[4, 2]]], ['s2-t1', [[3, 1], [5, 1]]]]                                  |
      | ALL SHORTEST       | :!X    | [['s1-t1', [[4, 2]]], ['s2-t1', [[3, 1]]]]                                          |
      | SHORTEST GROUP     | :!X    | [['s1-t1', [[4, 2]]], ['s2-t1', [[3, 1]]]]                                          |
      | SHORTEST 1 GROUP   | :!X    | [['s1-t1', [[4, 2]]], ['s2-t1', [[3, 1]]]]                                          |
      | SHORTEST 2 GROUPS  | :!X    | [['s1-t1', [[4, 2]]], ['s2-t1', [[3, 1], [5, 1]]]]                                  |
      | SHORTEST 3 GROUPS  | :!X    | [['s1-t1', [[4, 2]]], ['s2-t1', [[3, 1], [5, 1]]]]                                  |

  Scenario Outline: ANY path selectors return correct number of paths where multiple paths have same length
    #       ┌─┐
    #     ┌─┤S├─┐
    #     │ └─┘ │
    #    ┌▼┐   ┌▼┐
    #  ┌─┤S├─┬─┤b├─┐
    #  │ └─┘ │ └─┘ │
    # ┌▼┐   ┌▼┐   ┌▼┐
    # │c│   │X│   │e│
    # └┬┘   └┬┘   └┬┘
    #  │ ┌─┐ │ ┌─┐ │
    #  └─►f◄─┴─►g◄─┘
    #    └┬┘   └┬┘
    #     │ ┌─┐ │
    #     └─►T◄─┘
    #       └─┘
    And having executed:
      """
      CREATE (s1:S {n: 's1'}), (t1:T {n: 't1'}), (s2:S  {n: 's2'}), (x:X),
        (s1)-[:R]->(s2)-[:R]->(c)-[:R]->(f)-[:R]->(t1),
        (s1)-[:R]->(b)-[:R]->(e)-[:R]->(g)-[:R]->(t1),
        (s2)-[:R]->(x)-[:R]->(f),
        (b)-[:R]->(x)-[:R]->(g)
      """
    When executing query:
      """
      MATCH p = <pathSelector> (:S) (()--(<filter>))+ (:T)
      WITH [n in nodes(p) | n] as nodes
      WITH nodes[0].n + '-' + nodes[-1].n AS partition, count(*) AS numMatches
      ORDER BY partition
      RETURN collect([partition, numMatches]) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    Examples:
      | pathSelector | filter | result                         |
      | ANY          |        | [['s1-t1', 1], ['s2-t1', 1]]   |
      | ANY PATH     |        | [['s1-t1', 1], ['s2-t1', 1]]   |
      | ANY PATHS    |        | [['s1-t1', 1], ['s2-t1', 1]]   |
      | ANY 1        |        | [['s1-t1', 1], ['s2-t1', 1]]   |
      | ANY 1 PATH   |        | [['s1-t1', 1], ['s2-t1', 1]]   |
      | ANY 1 PATHS  |        | [['s1-t1', 1], ['s2-t1', 1]]   |
      | ANY 17       |        | [['s1-t1', 16], ['s2-t1', 17]] |
      | ANY          | :!X    | [['s1-t1', 1], ['s2-t1', 1]]   |
      | ANY 1        | :!X    | [['s1-t1', 1], ['s2-t1', 1]]   |
      | ANY 17       | :!X    | [['s1-t1', 2], ['s2-t1', 2]]   |

  Scenario Outline: ALL is non-selective and returns all matches
    #       ┌─┐
    #     ┌─┤S├─┐
    #     │ └─┘ │
    #    ┌▼┐   ┌▼┐
    #  ┌─┤S├─┬─┤b├─┐
    #  │ └─┘ │ └─┘ │
    # ┌▼┐   ┌▼┐   ┌▼┐
    # │c│   │X│   │e│
    # └┬┘   └┬┘   └┬┘
    #  │ ┌─┐ │ ┌─┐ │
    #  └─►f◄─┴─►g◄─┘
    #    └┬┘   └┬┘
    #     │ ┌─┐ │
    #     └─►T◄─┘
    #       └─┘
    And having executed:
      """
      CREATE (s1:S {n: 's1'}), (s2:S {n: 's2'}), (t1:T {n: 't1'}), (x:X),
        (s1)-[:R]->(s2)-[:R]->()-[:R]->(f)-[:R]->(t1),
        (s1)-[:R]->(b)-[:R]->(e)-[:R]->(g)-[:R]->(t1),
        (s2)-[:R]->(x)-[:R]->(f),
        (b)-[:R]->(x)-[:R]->(g)
      """
    When executing query:
      """
      MATCH p = <pathSelector> (:S) (()--(<filter>))+ (:T)
      WITH [n in nodes(p) | n] as nodes, size(relationships(p)) AS pathLength
      WITH nodes[0].n AS first, nodes[-1].n AS last, pathLength, count(*) AS numMatches
      ORDER BY first, last, pathLength
      WITH first+'-'+last AS partition, collect([pathLength, numMatches]) AS matches
      RETURN collect([partition, matches]) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    Examples:
      | pathSelector       | filter | result                                                                              |
      |                    |        | [['s1-t1', [[4, 6], [6, 4], [8, 6]]], ['s2-t1', [[3, 3], [5, 5], [7, 6], [9, 10]]]] |
      | ALL                |        | [['s1-t1', [[4, 6], [6, 4], [8, 6]]], ['s2-t1', [[3, 3], [5, 5], [7, 6], [9, 10]]]] |
      | ALL PATH           |        | [['s1-t1', [[4, 6], [6, 4], [8, 6]]], ['s2-t1', [[3, 3], [5, 5], [7, 6], [9, 10]]]] |
      | ALL PATHS          |        | [['s1-t1', [[4, 6], [6, 4], [8, 6]]], ['s2-t1', [[3, 3], [5, 5], [7, 6], [9, 10]]]] |
      |                    | :!X    | [['s1-t1', [[4, 2]]], ['s2-t1', [[3, 1], [5, 1]]]]                                  |
      | ALL                | :!X    | [['s1-t1', [[4, 2]]], ['s2-t1', [[3, 1], [5, 1]]]]                                  |
      | ALL PATH           | :!X    | [['s1-t1', [[4, 2]]], ['s2-t1', [[3, 1], [5, 1]]]]                                  |
      | ALL PATHS          | :!X    | [['s1-t1', [[4, 2]]], ['s2-t1', [[3, 1], [5, 1]]]]                                  |

  Scenario Outline: Return correct paths under different path selectors where there are multiple pairs of nodes
    And having executed:
      """
      CREATE (n1:A {p: 1}), (n2:A {p: 2}), (n3 {p: 3}), (n4 {p: 4}), (n5 {p: 5}),
        (n6:B {p: 6}), (n7:B {p: 7}),
        (n1)-[:R]->(n4)-[:R]->(n5)-[:R]->(n6)-[:R]->(n7),
        (n2)-[:R]->(n3)-[:R]->(n4)-[:R]->(n6)
      """
    When executing query:
      """
      MATCH p = <pathSelector> (:A)-->+(:B)
      WITH nodes(p) AS n ORDER BY head(n).p, size(n), head(reverse(n)).p
      RETURN collect([m IN n | m.p]) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    Examples:
      | pathSelector       | result                                                                                                                       |
      | ANY SHORTEST       | [[1, 4, 6], [1, 4, 6, 7], [2, 3, 4, 6], [2, 3, 4, 6, 7]]                                                                     |
      | SHORTEST 1         | [[1, 4, 6], [1, 4, 6, 7], [2, 3, 4, 6], [2, 3, 4, 6, 7]]                                                                     |
      | SHORTEST 2         | [[1, 4, 6], [1, 4, 5, 6], [1, 4, 6, 7], [1, 4, 5, 6, 7], [2, 3, 4, 6], [2, 3, 4, 5, 6], [2, 3, 4, 6, 7], [2, 3, 4, 5, 6, 7]] |
      | SHORTEST 3         | [[1, 4, 6], [1, 4, 5, 6], [1, 4, 6, 7], [1, 4, 5, 6, 7], [2, 3, 4, 6], [2, 3, 4, 5, 6], [2, 3, 4, 6, 7], [2, 3, 4, 5, 6, 7]] |
      | ALL SHORTEST       | [[1, 4, 6], [1, 4, 6, 7], [2, 3, 4, 6], [2, 3, 4, 6, 7]]                                                                     |
      | SHORTEST GROUP     | [[1, 4, 6], [1, 4, 6, 7], [2, 3, 4, 6], [2, 3, 4, 6, 7]]                                                                     |
      | SHORTEST 1 GROUP   | [[1, 4, 6], [1, 4, 6, 7], [2, 3, 4, 6], [2, 3, 4, 6, 7]]                                                                     |
      | SHORTEST 2 GROUPS  | [[1, 4, 6], [1, 4, 5, 6], [1, 4, 6, 7], [1, 4, 5, 6, 7], [2, 3, 4, 6], [2, 3, 4, 5, 6], [2, 3, 4, 6, 7], [2, 3, 4, 5, 6, 7]] |
      | SHORTEST 3 GROUPS  | [[1, 4, 6], [1, 4, 5, 6], [1, 4, 6, 7], [1, 4, 5, 6, 7], [2, 3, 4, 6], [2, 3, 4, 5, 6], [2, 3, 4, 6, 7], [2, 3, 4, 5, 6, 7]] |

  Scenario Outline: OPTIONAL MATCH does not reduce cardinality under different path selectors
    And having executed:
      """
      CREATE (a:A)-[:R]->()-[:R]->(:B)<-[:R]-(a)
      """
    When executing query:
      """
      MATCH (a:A), (b:B)
      OPTIONAL MATCH <pathSelector> (a)-[r:<type>]->+(b)
      WITH * ORDER BY size(r)
      RETURN collect([a, r, b]) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    Examples:
      | pathSelector       | type | result                                              |
      | ANY SHORTEST       | R    | [[(:A), [[:R]], (:B)]]                              |
      | SHORTEST 1         | R    | [[(:A), [[:R]], (:B)]]                              |
      | SHORTEST 2         | R    | [[(:A), [[:R]], (:B)], [(:A), [[:R], [:R]], (:B)]]  |
      | SHORTEST 3         | R    | [[(:A), [[:R]], (:B)], [(:A), [[:R], [:R]], (:B)]]  |
      | ALL SHORTEST       | R    | [[(:A), [[:R]], (:B)]]                              |
      | SHORTEST GROUP     | R    | [[(:A), [[:R]], (:B)]]                              |
      | SHORTEST 1 GROUP   | R    | [[(:A), [[:R]], (:B)]]                              |
      | SHORTEST 2 GROUPS  | R    | [[(:A), [[:R]], (:B)], [(:A), [[:R], [:R]], (:B)]]  |
      | SHORTEST 3 GROUPS  | R    | [[(:A), [[:R]], (:B)], [(:A), [[:R], [:R]], (:B)]]  |
      | ANY SHORTEST       | T    | [[(:A), null, (:B)]]                                |
      | SHORTEST 1         | T    | [[(:A), null, (:B)]]                                |
      | ALL SHORTEST       | T    | [[(:A), null, (:B)]]                                |
      | SHORTEST GROUP     | T    | [[(:A), null, (:B)]]                                |
      | SHORTEST 1 GROUP   | T    | [[(:A), null, (:B)]]                                |

  Scenario Outline: Find paths with two concatenated QPP under different path selectors
    # ┌──┐    ┌─┐      ┌──┐    ┌──┐
    # │A1├─R─►│2├──R──►│B4├─T─►│B5│
    # └──┘    └┬┘      └──┘    └──┘
    #          R  ┌─┐   ▲
    #          └─►│3├─T─┘
    #             └─┘
    And having executed:
      """
      CREATE (n1:A {p: 1})-[:R]->(n2 {p: 2})-[:R]->(n4:B {p: 4})-[:T]->(n5:B {p: 5}),
        (n2)-[:R]->(n3 {p: 3})-[:T]->(n4)
      """
    When executing query:
      """
      MATCH p = <pathSelector> (:A)-[:R]->+()-[:T]->*(:B)
      WITH nodes(p) AS n ORDER BY size(n), last(n).p
      RETURN collect([m IN n | m.p]) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    Examples:
      | pathSelector       | result                                                   |
      | ANY SHORTEST       | [[1, 2, 4], [1, 2, 4, 5]]                                |
      | SHORTEST 1         | [[1, 2, 4], [1, 2, 4, 5]]                                |
      | SHORTEST 2         | [[1, 2, 4], [1, 2, 3, 4], [1, 2, 4, 5], [1, 2, 3, 4, 5]] |
      | SHORTEST 3         | [[1, 2, 4], [1, 2, 3, 4], [1, 2, 4, 5], [1, 2, 3, 4, 5]] |
      | ALL SHORTEST       | [[1, 2, 4], [1, 2, 4, 5]]                                |
      | SHORTEST GROUP     | [[1, 2, 4], [1, 2, 4, 5]]                                |
      | SHORTEST 1 GROUP   | [[1, 2, 4], [1, 2, 4, 5]]                                |
      | SHORTEST 2 GROUPS  | [[1, 2, 4], [1, 2, 3, 4], [1, 2, 4, 5], [1, 2, 3, 4, 5]] |
      | SHORTEST 3 GROUPS  | [[1, 2, 4], [1, 2, 3, 4], [1, 2, 4, 5], [1, 2, 3, 4, 5]] |

  Scenario Outline: Find paths under different path selectors with QPP that contains a rigid path size greater than one
    #      ┌─┐  ┌─┐
    #  ┌──►│A├─►│B├────────┐
    #  │   └─┘  └─┘        │
    #  │                   ▼
    # ┌┴┐  ┌─┐  ┌─┐  ┌─┐  ┌──┐
    # │S├─►│A├─►│B├─►│A├─►│BT│◄─┐
    # └┬┘  └─┘  └─┘  └─┘  └──┘  │
    #  │                        │
    #  │   ┌─┐  ┌─┐  ┌─┐  ┌─┐  ┌┴┐
    #  └──►│A├─►│B├─►│A├─►│B├─►│A│
    #      └─┘  └─┘  └─┘  └─┘  └─┘
    And having executed:
      """
      CREATE (s:S)-[:R]->(:A)-[:R]->(:B)-[:R]->(:A)-[:R]->(t:B:T),
        (s)-[:R]->(:A)-[:R]->(:B)-[:R]->(:A)-[:R]->(:B)-[:R]->(:A)-[:R]->(t),
        (s)-[:R]->(:A)-[:R]->(:B)-[:R]->(t)
      """
    When executing query:
      """
      MATCH p = <pathSelector> (:S)(()-->(:A)-->(:B))+(:T)
      WITH nodes(p) AS n ORDER BY size(n)
      RETURN collect(n) AS n
      """
    Then the result should be, in any order:
      | n   |
      | <n> |
    Examples:
      | pathSelector       | n                                                                                |
      | ANY SHORTEST       | [[(:S), (:A), (:B), (:A), (:B:T)]]                                               |
      | SHORTEST 1         | [[(:S), (:A), (:B), (:A), (:B:T)]]                                               |
      | SHORTEST 2         | [[(:S), (:A), (:B), (:A), (:B:T)], [(:S), (:A), (:B), (:A), (:B), (:A), (:B:T)]] |
      | SHORTEST 3         | [[(:S), (:A), (:B), (:A), (:B:T)], [(:S), (:A), (:B), (:A), (:B), (:A), (:B:T)]] |
      | ALL SHORTEST       | [[(:S), (:A), (:B), (:A), (:B:T)]]                                               |
      | SHORTEST GROUP     | [[(:S), (:A), (:B), (:A), (:B:T)]]                                               |
      | SHORTEST 1 GROUP   | [[(:S), (:A), (:B), (:A), (:B:T)]]                                               |
      | SHORTEST 2 GROUPS  | [[(:S), (:A), (:B), (:A), (:B:T)], [(:S), (:A), (:B), (:A), (:B), (:A), (:B:T)]] |

  Scenario Outline: Find paths under different path selectors with fixed path concatenated with QPP
    #            ┌─────T─┐┌────T──┐
    #            ▼       │▼       │
    # ┌──┐     ┌──┐     ┌┘─┐     ┌┴┐
    # │A1├──T─►│B2├─S──►│B3├─R──►│4│
    # └──┘     └┬─┘     └──┘     └─┘
    #           R        ▲
    #           │        │
    #           │  ┌─┐   R
    #           └─►│5├───┘
    #              └─┘
    And having executed:
      """
      CREATE (n1:A {p: 1})-[:T]->(n2:B {p: 2})-[:S]->(n3:B {p: 3})-[:R]->(n4 {p: 4}),
        (n2)-[:R]->(n5 {p: 5})-[:R]->(n3), (n4)-[:T]->(n3)-[:T]->(n2)
      """
    When executing query:
      """
      MATCH p = <pathSelector> (:A)-[:!S]->*()-[:T]->(:B)
      WITH nodes(p) AS n ORDER BY size(n), last(n).p
      RETURN collect([m IN n | m.p]) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    Examples:
      | pathSelector       | result                                                               |
      | ANY SHORTEST       | [[1, 2], [1, 2, 5, 3, 4, 3]]                                         |
      | SHORTEST 1         | [[1, 2], [1, 2, 5, 3, 4, 3]]                                         |
      | SHORTEST 2         | [[1, 2], [1, 2, 5, 3, 2], [1, 2, 5, 3, 4, 3]]                        |
      | SHORTEST 3         | [[1, 2], [1, 2, 5, 3, 2], [1, 2, 5, 3, 4, 3], [1, 2, 5, 3, 4, 3, 2]] |
      | ALL SHORTEST       | [[1, 2], [1, 2, 5, 3, 4, 3]]                                         |
      | SHORTEST GROUP     | [[1, 2], [1, 2, 5, 3, 4, 3]]                                         |
      | SHORTEST 1 GROUP   | [[1, 2], [1, 2, 5, 3, 4, 3]]                                         |
      | SHORTEST 2 GROUPS  | [[1, 2], [1, 2, 5, 3, 2], [1, 2, 5, 3, 4, 3]]                        |
      | SHORTEST 3 GROUPS  | [[1, 2], [1, 2, 5, 3, 2], [1, 2, 5, 3, 4, 3], [1, 2, 5, 3, 4, 3, 2]] |

  Scenario Outline: Find ANY path under different path selectors with fixed path concatenated with QPP
    #            ┌─────T─┐┌────T──┐
    #            ▼       │▼       │
    # ┌──┐     ┌──┐     ┌┘─┐     ┌┴┐
    # │A1├──T─►│B2├─S──►│B3├─R──►│4│
    # └──┘     └┬─┘     └──┘     └─┘
    #           R        ▲
    #           │        │
    #           │  ┌─┐   R
    #           └─►│5├───┘
    #              └─┘
    And having executed:
      """
      CREATE (n1:A {p: 1})-[:T]->(n2:B {p: 2})-[:S]->(n3:B {p: 3})-[:R]->(n4 {p: 4}),
        (n2)-[:R]->(n5 {p: 5})-[:R]->(n3), (n4)-[:T]->(n3)-[:T]->(n2)
      """
    When executing query:
      """
      MATCH <pathSelector> (:A)-[:!S]->*()-[:T]->(:B)
      RETURN count(*) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    Examples:
      | pathSelector       | result  |
      | ANY                | 2       |
      | ANY 1              | 2       |
      | ANY 2              | 3       |
      | ANY 3              | 4       |
      | ANY 4              | 4       |

  Scenario Outline: Find shortest simple cycle under different path selectors
    #   ┌─────────────────────────┐
    #   ▼                         │
    # ┌──┐     ┌──┐     ┌──┐     ┌┴─┐
    # │A1├────►│B2├────►│A3├────►│B4│
    # └──┘     └──┘     └──┘     └┬─┘
    #   ▲                         │
    #   │      ┌──┐     ┌──┐      │
    #   └──────┤B6│◄────┤A5│◄─────┘
    #          └──┘     └──┘
    And having executed:
      """
        CREATE (n1:A {p: 1})-[:R]->(n2:B {p: 2})-[:R]->(n3:A {p: 3})-[:R]->(n4:B {p: 4})-[:R]->(n1),
          (n4)-[:R]->(n5:A {p: 5})-[:R]->(n6:B {p: 6})-[:R]->(n1)
      """
    When executing query:
      """
        MATCH p = <pathSelector> (n {p: 1})(()-->(:B)-->(:A))+(n)
        WITH nodes(p) AS n ORDER BY size(n), last(n).p
        RETURN collect([m IN n | m.p]) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    Examples:
      | pathSelector       | result                                   |
      | ANY SHORTEST       | [[1, 2, 3, 4, 1]]                        |
      | SHORTEST 1         | [[1, 2, 3, 4, 1]]                        |
      | SHORTEST 2         | [[1, 2, 3, 4, 1], [1, 2, 3, 4, 5, 6, 1]] |
      | SHORTEST 3         | [[1, 2, 3, 4, 1], [1, 2, 3, 4, 5, 6, 1]] |
      | ALL SHORTEST       | [[1, 2, 3, 4, 1]]                        |
      | SHORTEST GROUP     | [[1, 2, 3, 4, 1]]                        |
      | SHORTEST 1 GROUP   | [[1, 2, 3, 4, 1]]                        |
      | SHORTEST 2 GROUPS  | [[1, 2, 3, 4, 1], [1, 2, 3, 4, 5, 6, 1]] |
      | SHORTEST 3 GROUPS  | [[1, 2, 3, 4, 1], [1, 2, 3, 4, 5, 6, 1]] |

  Scenario Outline: Find ANY simple cycle under different path selectors
    #   ┌─────────────────────────┐
    #   ▼                         │
    # ┌──┐     ┌──┐     ┌──┐     ┌┴─┐
    # │A1├────►│B2├────►│A3├────►│B4│
    # └──┘     └──┘     └──┘     └┬─┘
    #   ▲                         │
    #   │      ┌──┐     ┌──┐      │
    #   └──────┤B6│◄────┤A5│◄─────┘
    #          └──┘     └──┘
    And having executed:
      """
        CREATE (n1:A {p: 1})-[:R]->(n2:B {p: 2})-[:R]->(n3:A {p: 3})-[:R]->(n4:B {p: 4})-[:R]->(n1),
          (n4)-[:R]->(n5:A {p: 5})-[:R]->(n6:B {p: 6})-[:R]->(n1)
      """
    When executing query:
      """
        MATCH p = <pathSelector> (n {p: 1})(()-->(:B)-->(:A))+(n)
        RETURN count(*) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    Examples:
      | pathSelector       | result |
      | ANY                | 1      |
      | ANY 1              | 1      |
      | ANY 2              | 2      |
      | ANY 3              | 2      |

  Scenario Outline: Lower bound of quantifier prunes some shortest paths under different path selectors
    #  ┌──┐     ┌──┐     ┌──┐     ┌──┐     ┌──┐
    #  │A1├────►│ 2├────►│B3├────►│B4├────►│B7│
    #  └┬─┘     └──┘     └──┘     └──┘     └──┘
    #   │                 ▲
    #   │  ┌──┐     ┌──┐  │
    #   └─►│ 5├────►│ 6├──┘
    #      └──┘     └──┘
    And having executed:
      """
        CREATE (n1:A {p: 1})-[:R]->(n2 {p: 2})-[:R]->(n3:B {p: 3})-[:R]->(n4:B {p: 4})-[:R]->(n7:B {p: 7}),
          (n1)-[:R]->(n5 {p: 5})-[:R]->(n6 {p: 6})-[:R]->(n3)
      """
    When executing query:
      """
        MATCH p = <pathSelector> (:A)-->{4,}(:B)
        WITH nodes(p) AS n ORDER BY size(n), last(n).p
        RETURN collect([m IN n | m.p]) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    Examples:
      | pathSelector       | result                                                 |
      | ANY SHORTEST       | [[1, 5, 6, 3, 4], [1, 2, 3, 4, 7]]                     |
      | SHORTEST 1         | [[1, 5, 6, 3, 4], [1, 2, 3, 4, 7]]                     |
      | SHORTEST 2         | [[1, 5, 6, 3, 4], [1, 2, 3, 4, 7], [1, 5, 6, 3, 4, 7]] |
      | SHORTEST 3         | [[1, 5, 6, 3, 4], [1, 2, 3, 4, 7], [1, 5, 6, 3, 4, 7]] |
      | ALL SHORTEST       | [[1, 5, 6, 3, 4], [1, 2, 3, 4, 7]]                     |
      | SHORTEST GROUP     | [[1, 5, 6, 3, 4], [1, 2, 3, 4, 7]]                     |
      | SHORTEST 1 GROUP   | [[1, 5, 6, 3, 4], [1, 2, 3, 4, 7]]                     |
      | SHORTEST 2 GROUPS  | [[1, 5, 6, 3, 4], [1, 2, 3, 4, 7], [1, 5, 6, 3, 4, 7]] |
      | SHORTEST 3 GROUPS  | [[1, 5, 6, 3, 4], [1, 2, 3, 4, 7], [1, 5, 6, 3, 4, 7]] |

  Scenario Outline: Lower bound of quantifier prunes ANY paths under different path selectors
    #  ┌──┐     ┌──┐     ┌──┐     ┌──┐     ┌──┐
    #  │A1├────►│ 2├────►│B3├────►│B4├────►│B7│
    #  └┬─┘     └──┘     └──┘     └──┘     └──┘
    #   │                 ▲
    #   │  ┌──┐     ┌──┐  │
    #   └─►│ 5├────►│ 6├──┘
    #      └──┘     └──┘
    And having executed:
      """
        CREATE (n1:A {p: 1})-[:R]->(n2 {p: 2})-[:R]->(n3:B {p: 3})-[:R]->(n4:B {p: 4})-[:R]->(n7:B {p: 7}),
          (n1)-[:R]->(n5 {p: 5})-[:R]->(n6 {p: 6})-[:R]->(n3)
      """
    When executing query:
      """
        MATCH <pathSelector> (:A)-->{4,}(:B)
        RETURN count(*) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    Examples:
      | pathSelector       | result |
      | ANY                | 2      |
      | ANY 1              | 2      |
      | ANY 2              | 3      |
      | ANY 3              | 3      |

  Scenario Outline: Path selectors can be used in EXISTS, COLLECT and COUNT
    #            ┌──┐     ┌──┐
    #     ┌─────►│ 4├────►│B5│
    #     │      └┬─┘     └──┘
    #     │       ▼
    #   ┌─┴┐     ┌──┐     ┌──┐
    # ┌─┤A1├────►│ 2├────►│B3│
    # │ └──┘     └──┘     └──┘
    # │           ▲
    # │ ┌──┐      │
    # └►│B6├──────┘
    #   └──┘
    And having executed:
      """
      CREATE (n1:A {p: 1})-[:R]->(n2 {p: 2})-[:R]->(n3:B {p: 3}),
        (n1)-[:R]->(n4 {p: 4})-[:R]->(n5:B {p: 5}),
        (n4)-[:R]->(n2),
        (n1)-[:R]->(n6:B {p: 6})-[:R]->(n2)
      """
    When executing query:
      """
      MATCH (m:A)
      RETURN <subqueryType> {
        MATCH p = <pathSelector> (m)-[r]->+(n:B)
        RETURN reduce(acc = '', n IN nodes(p) | acc + n.p) AS nodes
        ORDER BY size(r), nodes
      } AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    Examples:
      | subqueryType | pathSelector       | result                                                 |
      | EXISTS       | ANY SHORTEST       | true                                                   |
      | EXISTS       | SHORTEST 1         | true                                                   |
      | EXISTS       | ALL SHORTEST       | true                                                   |
      | EXISTS       | SHORTEST GROUP     | true                                                   |
      | COUNT        | ANY SHORTEST       | 3                                                      |
      | COUNT        | SHORTEST 1         | 3                                                      |
      | COUNT        | SHORTEST 2         | 4                                                      |
      | COUNT        | SHORTEST 3         | 5                                                      |
      | COUNT        | SHORTEST 4         | 5                                                      |
      | COUNT        | ALL SHORTEST       | 3                                                      |
      | COUNT        | SHORTEST GROUP     | 3                                                      |
      | COUNT        | SHORTEST 1 GROUP   | 3                                                      |
      | COUNT        | SHORTEST 2 GROUPS  | 5                                                      |
      | COUNT        | SHORTEST 3 GROUPS  | 5                                                      |
      | COUNT        | SHORTEST 4 GROUPS  | 5                                                      |
      | COLLECT      | ANY SHORTEST       | ['16', '123', '145']                                   |
      | COLLECT      | SHORTEST 1         | ['16', '123', '145']                                   |
      | COLLECT      | SHORTEST 3         | ['16', '123', '145', '1423', '1623']                   |
      | COLLECT      | SHORTEST 4         | ['16', '123', '145', '1423', '1623']                   |
      | COLLECT      | ALL SHORTEST       | ['16', '123', '145']                                   |
      | COLLECT      | SHORTEST GROUP     | ['16', '123', '145']                                   |
      | COLLECT      | SHORTEST 1 GROUP   | ['16', '123', '145']                                   |
      | COLLECT      | SHORTEST 2 GROUPS  | ['16', '123', '145', '1423', '1623']                   |
      | COLLECT      | SHORTEST 3 GROUPS  | ['16', '123', '145', '1423', '1623']                   |
      | COLLECT      | SHORTEST 4 GROUPS  | ['16', '123', '145', '1423', '1623']                   |

  Scenario Outline: ALL path selectors can be used in EXISTS, COLLECT and COUNT
    #            ┌──┐     ┌──┐
    #     ┌─────►│ 4├────►│B5│
    #     │      └┬─┘     └──┘
    #     │       ▼
    #   ┌─┴┐     ┌──┐     ┌──┐
    # ┌─┤A1├────►│ 2├────►│B3│
    # │ └──┘     └──┘     └──┘
    # │           ▲
    # │ ┌──┐      │
    # └►│B6├──────┘
    #   └──┘
    And having executed:
      """
      CREATE (n1:A {p: 1})-[:R]->(n2 {p: 2})-[:R]->(n3:B {p: 3}),
        (n1)-[:R]->(n4 {p: 4})-[:R]->(n5:B {p: 5}),
        (n4)-[:R]->(n2),
        (n1)-[:R]->(n6:B {p: 6})-[:R]->(n2)
      """
    When executing query:
      """
      MATCH (m:A)
      RETURN <subqueryType> {
        MATCH p = ALL (m)-[r]->+(n:B)
        RETURN reduce(acc = '', n IN nodes(p) | acc + n.p) AS nodes
        ORDER BY size(r), nodes
      } AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    Examples:
      | subqueryType | result                               |
      | EXISTS       | true                                 |
      | COUNT        | 5                                    |
      | COLLECT      | ['16', '123', '145', '1423', '1623'] |

  Scenario Outline: ANY path selectors can be used in EXISTS, COLLECT and COUNT
    #            ┌──┐     ┌──┐
    #     ┌─────►│ 4├────►│B5│
    #     │      └┬─┘     └──┘
    #     │       ▼
    #   ┌─┴┐     ┌──┐     ┌──┐
    # ┌─┤A1├────►│ 2├────►│B3│
    # │ └──┘     └──┘     └──┘
    # │           ▲
    # │ ┌──┐      │
    # └►│B6├──────┘
    #   └──┘
    And having executed:
      """
      CREATE (n1:A {p: 1})-[:R]->(n2 {p: 2})-[:R]->(n3:B {p: 3}),
        (n1)-[:R]->(n4 {p: 4})-[:R]->(n5:B {p: 5}),
        (n4)-[:R]->(n2),
        (n1)-[:R]->(n6:B {p: 6})-[:R]->(n2)
      """
    When executing query:
      """
      MATCH (m:A)
      RETURN <subqueryType> {
        MATCH <pathSelector> (m)-[r]->+(n:B)
        RETURN n.p ORDER BY n.p
      } AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    Examples:
      | subqueryType | pathSelector       | result                                                 |
      | EXISTS       | ANY                | true                                                   |
      | EXISTS       | ANY 1              | true                                                   |
      | COUNT        | ANY                | 3                                                      |
      | COUNT        | ANY 1              | 3                                                      |
      | COUNT        | ANY 2              | 4                                                      |
      | COUNT        | ANY 3              | 5                                                      |
      | COUNT        | ANY 4              | 5                                                      |
      | COLLECT      | ANY                | [3, 5, 6]                                              |
      | COLLECT      | ANY 1              | [3, 5, 6]                                              |
      | COLLECT      | ANY 2              | [3, 3, 5, 6]                                           |
      | COLLECT      | ANY 3              | [3, 3, 3, 5, 6]                                        |
      | COLLECT      | ANY 4              | [3, 3, 3, 5, 6]                                        |

  Scenario Outline: Multiple path patterns allowed in graph pattern if non-selective (CIP-60)
    And having executed:
      """
        CREATE (:A)-[:R]->(b:B)-[:R]->(:C),
          (:D)-[:R]->(b)-[:R]->(:E)
      """
    When executing query:
      """
        MATCH p = <pathSelector1> (n0:A)-->*(n1)-->*(n2:C), q = <pathSelector2> (n1)-->(n3:E)
        RETURN nodes(p) AS path1, nodes(q) AS path2
      """
    Then the result should be, in any order:
      | path1              | path2              |
      | [(:A), (:B), (:C)] | [(:B), (:E)]       |
    Examples:
      | pathSelector1      | pathSelector2      |
      |                    |                    |
      | ALL                |                    |
      | ALL                | ALL                |

  Scenario Outline: Only one selective path pattern allowed in graph pattern (CIP-60)
    When executing query:
      """
        MATCH p = <pathSelector1> (n0:A)-->*(n1)-->*(n2:C), <pathSelector2> (n1)-->+(:E)
        RETURN *
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | pathSelector1      | pathSelector2      |
      | ANY SHORTEST       |                    |
      | SHORTEST 1         |                    |
      | ALL SHORTEST       |                    |
      | SHORTEST GROUP     |                    |
      | SHORTEST 1 GROUP   |                    |
      | ANY SHORTEST       | ALL                |
      | SHORTEST 1         | ALL                |
      | ALL SHORTEST       | ALL                |
      | SHORTEST GROUP     | ALL                |
      | SHORTEST 1 GROUP   | ALL                |
      | ANY SHORTEST       | SHORTEST 1         |
      | SHORTEST 1         | ALL SHORTEST       |
      | ALL SHORTEST       | ANY SHORTEST       |
      | SHORTEST 1         | SHORTEST GROUP     |
      | SHORTEST 2 GROUPS  | SHORTEST GROUP     |

  Scenario Outline: Selective path patterns can be combined when in separate MATCH clauses
    #       ┌────────────┐
    #      ┌┴┐           │
    #  ┌──►│X├─────┐     │
    #  │   └─┘     │     │
    #  │    ▲      │     │
    #  │    │      ▼     ▼
    # ┌┴┐  ┌┴┐    ┌─┐   ┌─┐
    # │A│  │B│    │M├──►│C│
    # └┬┘  └┬┘    └─┘   └─┘
    #  │    │      ▲     ▲
    #  │    ▼      │     │
    #  │   ┌─┐    ┌┴┐    │
    #  └──►│Y├───►│N├────┘
    #      └─┘    └─┘
    And having executed:
      """
        CREATE (a:A)-[:R]->(x:X)-[:R]->(m:M)-[:R]->(c:C),
               (x)-[:R]->(c),
               (a)-[:R]->(y:Y)-[:R]->(n:N)-[:R]->(c),
               (n)-[:R]->(m),
               (b:B)-[:R]->(x),
               (b)-[:R]->(y)
      """
    When executing query:
      """
        MATCH p = <pathSelector1> (:A)-->+(x:X|Y)-->+(:C)
        MATCH q = <pathSelector2> (:B)-->+(x)-->+(:C)
        WITH nodes(p) AS np, nodes(q) AS nq
        WITH reduce(acc = '', n IN np | acc + labels(n)[0]) AS Ps,
             reduce(acc = '', n IN nq | acc + labels(n)[0]) AS Qs
        ORDER BY size(np), Ps, size(nq), Qs
        RETURN collect([Ps, Qs]) AS result
      """
    Then the result should be, in any order:
      | result    |
      | <result>  |
    Examples:
      | pathSelector1      | pathSelector2      | result                                                                                                                                           |
      | ANY SHORTEST       |                    | [['AXC', 'BXC'], ['AXC', 'BXMC']]                                                                                                                                 |
      | SHORTEST 1         |                    | [['AXC', 'BXC'], ['AXC', 'BXMC']]                                                                                                                                 |
      | SHORTEST 4         |                    | [['AXC', 'BXC'], ['AXC', 'BXMC'], ['AXMC', 'BXC'], ['AXMC', 'BXMC'], ['AYNC', 'BYNC'], ['AYNC', 'BYNMC'], ['AYNMC', 'BYNC'], ['AYNMC', 'BYNMC']] |
      | ALL SHORTEST       |                    | [['AXC', 'BXC'], ['AXC', 'BXMC']]                                                                                                                                 |
      | SHORTEST GROUP     |                    | [['AXC', 'BXC'], ['AXC', 'BXMC']]                                                                                                                                 |
      | SHORTEST 2 GROUPS  |                    | [['AXC', 'BXC'], ['AXC', 'BXMC'], ['AXMC', 'BXC'], ['AXMC', 'BXMC'], ['AYNC', 'BYNC'], ['AYNC', 'BYNMC']]                                        |
      | SHORTEST 3 GROUPS  |                    | [['AXC', 'BXC'], ['AXC', 'BXMC'], ['AXMC', 'BXC'], ['AXMC', 'BXMC'], ['AYNC', 'BYNC'], ['AYNC', 'BYNMC'], ['AYNMC', 'BYNC'], ['AYNMC', 'BYNMC']] |
      |                    | ANY SHORTEST       | [['AXC', 'BXC'], ['AXMC', 'BXC'], ['AYNC', 'BYNC'], ['AYNMC', 'BYNC']]                                                                           |
      |                    | SHORTEST 1         | [['AXC', 'BXC'], ['AXMC', 'BXC'], ['AYNC', 'BYNC'], ['AYNMC', 'BYNC']]                                                                           |
      |                    | SHORTEST 2         | [['AXC', 'BXC'], ['AXC', 'BXMC'], ['AXMC', 'BXC'], ['AXMC', 'BXMC'], ['AYNC', 'BYNC'], ['AYNC', 'BYNMC'], ['AYNMC', 'BYNC'], ['AYNMC', 'BYNMC']] |
      |                    | ALL SHORTEST       | [['AXC', 'BXC'], ['AXMC', 'BXC'], ['AYNC', 'BYNC'], ['AYNMC', 'BYNC']]                                                                           |
      |                    | SHORTEST GROUP     | [['AXC', 'BXC'], ['AXMC', 'BXC'], ['AYNC', 'BYNC'], ['AYNMC', 'BYNC']]                                                                           |
      |                    | SHORTEST 2 GROUPS  | [['AXC', 'BXC'], ['AXC', 'BXMC'], ['AXMC', 'BXC'], ['AXMC', 'BXMC'], ['AYNC', 'BYNC'], ['AYNC', 'BYNMC'], ['AYNMC', 'BYNC'], ['AYNMC', 'BYNMC']] |
      | ANY SHORTEST       | ANY SHORTEST       | [['AXC', 'BXC']]                                                                                                                                 |
      | ANY SHORTEST       | SHORTEST 2         | [['AXC', 'BXC'], ['AXC', 'BXMC']]                                                                                                                                 |
      | SHORTEST 4         | ANY SHORTEST       | [['AXC', 'BXC'], ['AXMC', 'BXC'], ['AYNC', 'BYNC'], ['AYNMC', 'BYNC']]                                                                           |
      | SHORTEST 4         | SHORTEST 2         | [['AXC', 'BXC'], ['AXC', 'BXMC'], ['AXMC', 'BXC'], ['AXMC', 'BXMC'], ['AYNC', 'BYNC'], ['AYNC', 'BYNMC'], ['AYNMC', 'BYNC'], ['AYNMC', 'BYNMC']] |
      | ALL SHORTEST       | ANY SHORTEST       | [['AXC', 'BXC']]                                                                                                                                 |
      | ALL SHORTEST       | SHORTEST 2         | [['AXC', 'BXC'], ['AXC', 'BXMC']]                                                                                                                |
      | SHORTEST GROUP     | ANY SHORTEST       | [['AXC', 'BXC']]                                                                                                                                 |
      | SHORTEST GROUP     | SHORTEST 2         | [['AXC', 'BXC'], ['AXC', 'BXMC']]                                                                                                                |
      | SHORTEST 3 GROUPS  | ANY SHORTEST       | [['AXC', 'BXC'], ['AXMC', 'BXC'], ['AYNC', 'BYNC'], ['AYNMC', 'BYNC']]                                                                           |
      | SHORTEST 3 GROUPS  | SHORTEST 2         | [['AXC', 'BXC'], ['AXC', 'BXMC'], ['AXMC', 'BXC'], ['AXMC', 'BXMC'], ['AYNC', 'BYNC'], ['AYNC', 'BYNMC'], ['AYNMC', 'BYNC'], ['AYNMC', 'BYNMC']] |

  Scenario Outline: Number of paths and groups specified must be a positive integer
    When executing query:
      """
        MATCH p = <pathSelector1> (:A)-->*(:C)
        RETURN *
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | pathSelector1      |
      | SHORTEST 0         |
      | SHORTEST 0 GROUP   |

  Scenario: Pre-GPM and GPM shortest can be mixed in query if in separate clauses (CIP-40)
    And having executed:
      """
        CREATE (a:A)-[:R]->(:B)-[:R]->(:C)-[:R]->(:D)-[:R]->(a)
      """
    When executing query:
      """
        MATCH p = shortestPath((:A)-[*]-(:D))
        MATCH q = SHORTEST 1 (:A)-[*]-(:D)
        RETURN nodes(p) = nodes(q) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |

  Scenario Outline: Fixed-length patterns allowed with path selectors
    And having executed:
      """
        CREATE (a1:A {p: 'a1'})-[:R]->(b:B {p: 'b'}), (a1)-[:R]->(b), (:A {p: 'a2'})-[:R]->(b)
      """
    When executing query:
      """
        MATCH <pathSelector> (a:A)-->(b:B)
        WITH a.p AS ap, b.p AS bp, count(*) AS count ORDER BY a.p, b.p
        RETURN collect([ap, bp, count]) AS result
      """
    Then the result should be, in any order:
      | result    |
      | <result>  |
    Examples:
      | pathSelector      | result                            |
      | ANY SHORTEST      | [['a1', 'b', 1], ['a2', 'b', 1]]  |
      | SHORTEST 1        | [['a1', 'b', 1], ['a2', 'b', 1]]  |
      | SHORTEST 2        | [['a1', 'b', 2], ['a2', 'b', 1]]  |
      | SHORTEST 3        | [['a1', 'b', 2], ['a2', 'b', 1]]  |
      | ALL SHORTEST      | [['a1', 'b', 2], ['a2', 'b', 1]]  |
      | SHORTEST GROUP    | [['a1', 'b', 2], ['a2', 'b', 1]]  |
      | SHORTEST 1 GROUP  | [['a1', 'b', 2], ['a2', 'b', 1]]  |
      | SHORTEST 2 GROUPS | [['a1', 'b', 2], ['a2', 'b', 1]]  |
      | ANY               | [['a1', 'b', 1], ['a2', 'b', 1]]  |
      | ANY 1             | [['a1', 'b', 1], ['a2', 'b', 1]]  |
      | ANY 2             | [['a1', 'b', 2], ['a2', 'b', 1]]  |

  Scenario Outline: Node pattern only allowed with path selectors
    And having executed:
      """
        CREATE (:A {p: 'a1'}), (:A {p: 'a2'})
      """
    When executing query:
      """
        MATCH <pathSelector> (a:A)
        WITH a.p AS ap, count(*) AS count ORDER BY a.p
        RETURN collect([ap, count]) AS result
      """
    Then the result should be, in any order:
      | result                  |
      | [['a1', 1], ['a2', 1]]  |
    Examples:
      | pathSelector      |
      | ANY SHORTEST      |
      | SHORTEST 1        |
      | SHORTEST 2        |
      | ALL SHORTEST      |
      | SHORTEST GROUP    |
      | SHORTEST 1 GROUP  |
      | SHORTEST 2 GROUPS |
      | ANY               |
      | ANY 1             |
      | ANY 2             |

  Scenario Outline: PathSelector should handle having nested predicates
    And having executed:
      """
        CREATE (:User)-[:R]->(v:V)-[:S1]->(:W), (v)-[:S2]->(n:N)
      """
    When executing query:
      """
        MATCH p = <pathSelector> ((u:User) ((a)-[r]->(b))+ (v)-[s]->(w) WHERE (v)-->(:N))
        RETURN p
      """
    Then the result should be, in any order:
      | p                                      |
      | <(:User)-[:R {}]->(:V)-[:S2 {}]->(:N)> |
      | <(:User)-[:R {}]->(:V)-[:S1 {}]->(:W)> |
    Examples:
      | pathSelector      |
      | ANY SHORTEST      |
      | SHORTEST 1        |
      | SHORTEST 2        |
      | ALL SHORTEST      |
      | SHORTEST GROUP    |
      | SHORTEST 1 GROUP  |
      | SHORTEST 2 GROUPS |
      | ANY               |
      | ANY 1             |
      | ANY 2             |

  Scenario Outline: PathSelector should accept single node as solution
    And having executed:
      """
        CREATE (:A:B {p: 'a1'})-[:REL]->(:A {p: 'a2'})
      """
    When executing query:
      """
        MATCH <pathSelector> (a:A)-->*(:B)
        WITH a.p AS ap, count(*) AS count ORDER BY a.p
        RETURN collect([ap, count]) AS result
      """
    Then the result should be, in any order:
      | result     |
      | [['a1', 1]] |
    Examples:
      | pathSelector      |
      | ANY SHORTEST      |
      | SHORTEST 1        |
      | SHORTEST 2        |
      | ALL SHORTEST      |
      | SHORTEST GROUP    |
      | SHORTEST 1 GROUP  |
      | SHORTEST 2 GROUPS |
      | ANY               |
      | ANY 1             |
      | ANY 2             |

  Scenario Outline: PathSelector should not find a path which is created later in the query
    And having executed:
      """
        CREATE ()-[:R]->()
      """
    When executing query:
      """
        MATCH p = <pathSelector> ((start)((a)-[r:R]->(b))+(end)) MERGE (start)-[t:R]-(:B) RETURN p
      """
    Then the result should be, in any order:
      | p                 |
      | <()-[:R {}]->()>  |
    Examples:
      | pathSelector      |
      | ANY SHORTEST      |
      | SHORTEST 1        |
      | SHORTEST 2        |
      | ALL SHORTEST      |
      | SHORTEST GROUP    |
      | SHORTEST 1 GROUP  |
      | SHORTEST 2 GROUPS |
      | ANY               |
      | ANY 1             |
      | ANY 2             |


  Scenario Outline: PathSelector should handle pre-filter predicates on the whole path
    And having executed:
      """
        CREATE ()-[:R]->()-[:R]->()-[:R]->()-[:R]->()
      """
    When executing query:
      """
        MATCH <pathSelector> (p = ((start)((a)-[r:R]->(b))+(end)) WHERE length(p) > 3) RETURN p
      """
    Then the result should be, in any order:
      | p                 |
      | <()-[:R]->()-[:R]->()-[:R]->()-[:R]->()>  |
    Examples:
      | pathSelector      |
      | ANY SHORTEST      |
      | SHORTEST 1        |
      | SHORTEST 2        |
      | ALL SHORTEST      |
      | SHORTEST GROUP    |
      | SHORTEST 1 GROUP  |
      | SHORTEST 2 GROUPS |
      | ANY               |
      | ANY 1             |
      | ANY 2             |

  Scenario Outline: PathSelector should handle legacy var-length
    And having executed:
      """
        CREATE ()-[:R]->()-[:T]->()
      """
    When executing query:
      """
        MATCH <pathSelector> (p=()-[*1]->()) RETURN p
      """
    Then the result should be, in any order:
      | p                |
      | <()-[:T {}]->()> |
      | <()-[:R {}]->()> |
    Examples:
      | pathSelector      |
      | ANY SHORTEST      |
      | SHORTEST 1        |
      | SHORTEST 2        |
      | ALL SHORTEST      |
      | SHORTEST GROUP    |
      | SHORTEST 1 GROUP  |
      | SHORTEST 2 GROUPS |
      | ANY               |
      | ANY 1             |
      | ANY 2             |

  Scenario Outline: PathSelector should handle legacy var-length with set upper bound
    And having executed:
      """
        CREATE (:A)-[:R]->(:B)-[:T]->(:B)
      """
    When executing query:
      """
        MATCH <pathSelector> (p=(a:A)-[*0..1]-(b:B)) RETURN p
      """
    Then the result should be, in any order:
      | p                    |
      | <(:A)-[:R {}]->(:B)> |
    Examples:
      | pathSelector      |
      | ANY SHORTEST      |
      | SHORTEST 1        |
      | SHORTEST 2        |
      | ALL SHORTEST      |
      | SHORTEST GROUP    |
      | SHORTEST 1 GROUP  |
      | SHORTEST 2 GROUPS |
      | ANY               |
      | ANY 1             |
      | ANY 2             |

  Scenario Outline: PathSelector should handle a previously bound boundary node
    And having executed:
      """
        CREATE (:L)-[:R]->()-[:R]->()
      """
    When executing query:
      """
        MATCH (start)
        MATCH <pathSelector> (p = (start:L)((a)-[r:R]->(b))+(end)) RETURN p
      """
    Then the result should be, in any order:
      | p                              |
      | <(:L)-[:R {}]->()>             |
      | <(:L)-[:R {}]->()-[:R {}]->()> |
    Examples:
      | pathSelector      |
      | ANY SHORTEST      |
      | SHORTEST 1        |
      | SHORTEST 2        |
      | ALL SHORTEST      |
      | SHORTEST GROUP    |
      | SHORTEST 1 GROUP  |
      | SHORTEST 2 GROUPS |
      | ANY               |
      | ANY 1             |
      | ANY 2             |

  Scenario Outline: PathSelector should handle a previously bound relationship
    And having executed:
      """
        CREATE ()-[:R]->()-[:T]->()
      """
    When executing query:
      """
        MATCH ()-[r]->()
        MATCH <pathSelector> (p = (start)-[r:R]->(a)((b)-[]->(c))+(end)) RETURN p
      """
    Then the result should be, in any order:
      | p                              |
      | <()-[:R {}]->()-[:T {}]->()>   |
    Examples:
      | pathSelector      |
      | ANY SHORTEST      |
      | SHORTEST 1        |
      | SHORTEST 2        |
      | ALL SHORTEST      |
      | SHORTEST GROUP    |
      | SHORTEST 1 GROUP  |
      | SHORTEST 2 GROUPS |
      | ANY               |
      | ANY 1             |
      | ANY 2             |

  Scenario Outline: Should support a shortest path pattern with a predicate on several entities inside a QPP
    And having executed:
      """
        CREATE (:User {prop: 4})-[:R]->(:B)-[:R]->({prop: 5})
      """
    When executing query:
      """
        MATCH p = <pathSelector> (u:User)(((n)-[r]->(c:B)-->(m)) WHERE n.prop <= m.prop)+ (v) RETURN p
      """
    Then the result should be, in any order:
      | p                                                      |
      | <(:User {prop: 4})-[:R {}]->(:B)-[:R {}]->({prop: 5})> |
    Examples:
      | pathSelector      |
      | ANY SHORTEST      |
      | SHORTEST 1        |
      | SHORTEST 2        |
      | ALL SHORTEST      |
      | SHORTEST GROUP    |
      | SHORTEST 1 GROUP  |
      | SHORTEST 2 GROUPS |
      | ANY               |
      | ANY 1             |
      | ANY 2             |

  Scenario Outline: PathSelector should handle multiple references to the same variable in a pattern
    And having executed:
      """
        CREATE (a:A:B)-[:R]->()-[:S]->(a)
      """
    When executing query:
      """
        MATCH <pathSelector> (p = (start)((a:A)-[]->()-[]->(a:B))+(end)) RETURN p
      """
    Then the result should be, in any order:
      | p                                    |
      | <(:A:B)-[:R {}]->()-[:S {}]->(:A:B)> |
    Examples:
      | pathSelector      |
      | ANY SHORTEST      |
      | SHORTEST 1        |
      | SHORTEST 2        |
      | ALL SHORTEST      |
      | SHORTEST GROUP    |
      | SHORTEST 1 GROUP  |
      | SHORTEST 2 GROUPS |
      | ANY               |
      | ANY 1             |
      | ANY 2             |

  Scenario Outline: Find shortest path with pattern expression in QPP
    Given having executed:
      """
        CREATE (start:Start)-[:R]->(:Wrong)-[:R]->(end:End)
        CREATE (start)-[:R]->(r1:Right)-[:R]->(r2:Right)-[:R]->(end)
        CREATE (r1)-[:R]->()-[:R]->(:N)
        CREATE (r2)-[:R]->()-[:R]->(:N)
        CREATE (end)-[:R]->()-[:R]->(:N)
      """
    When executing query:
      """
         MATCH p = SHORTEST 1 (start:Start)
                              (
                                (n)-[r]->(m)
                                  WHERE n <> m AND <pattern>
                              )+
                              (end:End)
         RETURN p
      """
    Then the result should be, in any order:
      | p                                                              |
      | <(:Start)-[:R {}]->(:Right)-[:R {}]->(:Right)-[:R {}]->(:End)> |
    Examples:
      | pattern                                                           |
      | (m)-->()-->(:N)                                                   |
      | CASE WHEN (m)-->() THEN EXISTS { (m)-->()-->(:N) } ELSE false END |
      | [p = (m)-->()-->(:N) \| length(p) ] <> []                         |
      | COUNT { (m)-->()-->(:N) } = 1                                     |
      | COUNT { (m)-->()-->(:N) } = 1 AND (m)-->()-->(:N)                 |
