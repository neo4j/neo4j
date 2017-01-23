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

Feature: MatchAcceptanceTest

  Scenario: path query should return results in written order
    Given an empty graph
      And having executed: CREATE (:label1)<-[:TYPE]-(:label2)
    When executing query: MATCH (a:label1) RETURN (a)<--(:label2) AS p
    Then the result should be:
      | p                                |
      | [<(:label1)<-[:TYPE]-(:label2)>] |

  Scenario: longer path query should return results in written order
    Given an empty graph
      And having executed: CREATE (:label1)<-[:T1]-(:label2)-[:T2]->(:label3)
    When executing query: MATCH (a:label1) RETURN (a)<--(:label2)--() AS p
    Then the result should be:
      | p                                               |
      | [<(:label1)<-[:T1]-(:label2)-[:T2]->(:label3)>] |

  Scenario: Get node degree via length of pattern expression
    Given an empty graph
      And having executed: CREATE (x:X), (x)-[:T]->(), (x)-[:T]->(), (x)-[:T]->()
    When executing query: MATCH (a:X) RETURN length((a)-->()) as length
    Then the result should be:
      | length |
      | 3      |

  Scenario: Get node degree via length of pattern expression that specifies a relationship type
    Given an empty graph
      And having executed: CREATE (x:X), (x)-[:T]->(), (x)-[:T]->(), (x)-[:T]->(), (x)-[:AFFE]->()
    When executing query: MATCH (a:X) RETURN length((a)-[:T]->()) as length
    Then the result should be:
      | length |
      | 3      |

  Scenario: Get node degree via length of pattern expression that specifies multiple relationship types
    Given an empty graph
      And having executed: CREATE (x:X), (x)-[:T]->(), (x)-[:T]->(), (x)-[:T]->(), (x)-[:AFFE]->()
    When executing query: MATCH (a:X) RETURN length((a)-[:T|AFFE]->()) as length
    Then the result should be:
      | length |
      | 4      |

  Scenario: should be able to use multiple MATCH clauses to do a cartesian product
    Given an empty graph
      And having executed: CREATE ({value: 1}), ({value: 2}), ({value: 3})
    When executing query: MATCH (n), (m) RETURN n.value AS n, m.value AS m
    Then the result should be:
      | n | m |
      | 1 | 1 |
      | 1 | 2 |
      | 1 | 3 |
      | 2 | 1 |
      | 2 | 2 |
      | 2 | 3 |
      | 3 | 3 |
      | 3 | 1 |
      | 3 | 2 |

  Scenario: should be able to use params in pattern matching predicates
    Given an empty graph
      And having executed: CREATE (:a)-[:A {foo: "bar"}]->(:b {name: 'me'})
      And parameters are:
        | param |
        | 'bar' |
    When executing query: MATCH (a)-[r]->(b) WHERE r.foo =~ {param} RETURN b
    Then the result should be:
      | b                  |
      | (:b {name: 'me'})  |

  Scenario: should filter out based on node prop name
    Given an empty graph
      And having executed: CREATE ({name: "Someone Else"})<-[:x]-()-[:x]->({name: "Andres"})
    When executing query: MATCH (start)-[rel:x]-(a) WHERE a.name = 'Andres' return a
    Then the result should be:
      | a                   |
      | ({name: 'Andres'})  |

  Scenario: should honour the column name for RETURN items
    Given an empty graph
      And having executed: CREATE ({name: "Someone Else"})
    When executing query: MATCH (a) WITH a.name AS a RETURN a
    Then the result should be:
      | a              |
      | 'Someone Else' |

  Scenario: should filter based on rel prop name
    Given an empty graph
      And having executed: CREATE (:a)<-[:KNOWS {name: "monkey"}]-()-[:KNOWS {name: "woot"}]->(:b)
    When executing query: match (node)-[r:KNOWS]->(a) WHERE r.name = 'monkey' RETURN a
    Then the result should be:
      | a    |
      | (:a) |

  Scenario: should cope with shadowed variables
    Given an empty graph
      And having executed: CREATE ({value: 1, name: 'King Kong'}), ({value: 2, name: 'Ann Darrow'})
    When executing query: MATCH (n) WITH n.name AS n RETURN n
    Then the result should be:
      | n            |
      | 'Ann Darrow' |
      | 'King Kong'  |

  Scenario: should get neighbours
    Given an empty graph
      And having executed: CREATE (a:A {value : 1})-[:KNOWS]->(b:B {value : 2})
    When executing query: MATCH (n1)-[rel:KNOWS]->(n2) RETURN n1, n2
    Then the result should be:
      | n1              | n2              |
      | (:A {value: 1}) | (:B {value: 2}) |

  Scenario: should get two related nodes
    Given an empty graph
      And having executed: CREATE (a:A {value: 1}), (a)-[:KNOWS]->(b:B {value: 2}), (a)-[:KNOWS]->(c:C {value: 3})
    When executing query: MATCH (start)-[rel:KNOWS]->(x) RETURN x
    Then the result should be:
      | x               |
      | (:B {value: 2}) |
      | (:C {value: 3}) |

  Scenario: should get related to related to
    Given an empty graph
      And having executed: CREATE (a:A {value: 1})-[:KNOWS]->(b:B {value: 2})-[:FRIEND]->(c:C {value: 3})
    When executing query: MATCH (n)-->(a)-->(b) RETURN b
    Then the result should be:
      | b               |
      | (:C {value: 3}) |

  Scenario: should handle comparison between node properties
    Given an empty graph
      And having executed: CREATE (a:A {animal: "monkey"}), (b:B {animal: "cow"}), (c:C {animal: "monkey"}), (d:D {animal: "cow"}), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c), (d)-[:KNOWS]->(b), (d)-[:KNOWS]->(c)
    When executing query: MATCH (n)-[rel]->(x) WHERE n.animal = x.animal RETURN n, x
    Then the result should be:
      | n                       | x                       |
      | (:A {animal: 'monkey'}) | (:C {animal: 'monkey'}) |
      | (:D {animal: 'cow'})    | (:B {animal: 'cow'})    |

  Scenario: should return two subgraphs with bound undirected relationship
    Given an empty graph
      And having executed: CREATE (a:A {value: 1})-[:REL {name: "r"}]->(b:B {value: 2})
    When executing query: match (a)-[r {name: 'r'}]-(b) RETURN a,b
    Then the result should be:
      | a               | b               |
      | (:B {value: 2}) | (:A {value: 1}) |
      | (:A {value: 1}) | (:B {value: 2}) |

  Scenario: should return two subgraphs with bound undirected relationship and optional relationship
    Given an empty graph
      And having executed: CREATE (a:A {value: 1})-[:REL {name: "r1"}]->(b:B {value: 2})-[:REL {name: "r2"}]->(c:C {value: 3})
    When executing query: MATCH (a)-[r {name:'r1'}]-(b) OPTIONAL MATCH (b)-[r2]-(c) WHERE r<>r2 RETURN a,b,c
    Then the result should be:
      | a               | b              | c              |
      | (:A {value: 1}) | (:B {value: 2}) | (:C {value: 3}) |
      | (:B {value: 2}) | (:A {value: 1}) | null           |

  Scenario: rel type function works as expected
    Given an empty graph
      And having executed: CREATE (a:A {name: "A"}), (b:B {name: "B"}), (c:C {name: "C"}), (a)-[:KNOWS]->(b), (a)-[:HATES]->(c)
    When executing query: MATCH (n {name:'A'})-[r]->(x) WHERE type(r) = 'KNOWS' RETURN x
    Then the result should be:
      | x                |
      | (:B {name: 'B'}) |

  Scenario: should walk alternative relationships
    Given an empty graph
      And having executed: CREATE (a {name: "A"}), (b {name: "B"}), (c {name: "C"}), (a)-[:KNOWS]->(b), (a)-[:HATES]->(c), (a)-[:WONDERS]->(c)
    When executing query: MATCH (n)-[r]->(x) WHERE type(r) = 'KNOWS' OR type(r) = 'HATES' RETURN r
    Then the result should be:
      | r        |
      | [:KNOWS] |
      | [:HATES] |

  Scenario: should handle OR in the WHERE clause
    Given an empty graph
      And having executed: CREATE (a:A {p1: 12}), (b:B {p2: 13}), (c:C)
    When executing query: MATCH (n) WHERE n.p1 = 12 OR n.p2 = 13 RETURN n
    Then the result should be:
      | n             |
      | (:A {p1: 12}) |
      | (:B {p2: 13}) |

  Scenario: should return a simple path
    Given an empty graph
      And having executed: CREATE (a:A {name: "A"})-[:KNOWS]->(b:B {name: "B"})
    When executing query: MATCH p=(a {name:'A'})-->(b) RETURN p
    Then the result should be:
      | p                                             |
      | <(:A {name: 'A'})-[:KNOWS]->(:B {name: 'B'})> |

  Scenario: should return a three node path
    Given an empty graph
      And having executed: CREATE (a:A {name: "A"})-[:KNOWS]->(b:B {name: "B"})-[:KNOWS]->(c:C {name: "C"})
    When executing query: MATCH p = (a {name:'A'})-[rel1]->(b)-[rel2]->(c) RETURN p
    Then the result should be:
      | p                                                                        |
      | <(:A {name: 'A'})-[:KNOWS]->(:B {name: 'B'})-[:KNOWS]->(:C {name: 'C'})> |

  Scenario: should not return anything because path length does not match
    Given an empty graph
      And having executed: CREATE (a:A {name: "A"})-[:KNOWS]->(b:B {name: "B"})
    When executing query: MATCH p = (n)-->(x) WHERE length(p) = 10 RETURN x
    Then the result should be empty

  Scenario: should pass the path length test
    Given an empty graph
      And having executed: CREATE (a:A {name: "A"})-[:KNOWS]->(b:B {name: "B"})
    When executing query: MATCH p = (n)-->(x) WHERE length(p)=1 RETURN x
    Then the result should be:
      | x                |
      | (:B {name: 'B'}) |

  Scenario: should be able to filter on path nodes
    Given an empty graph
      And having executed: CREATE (a:A {foo: "bar"})-[:REL]->(b:B {foo: "bar"})-[:REL]->(c:C {foo: "bar"})-[:REL]->(d:D {foo: "bar"})
    When executing query: MATCH p = (pA)-[:REL*3..3]->(pB) WHERE all(i in nodes(p) WHERE i.foo = 'bar') RETURN pB
    Then the result should be:
      | pB                |
      | (:D {foo: 'bar'}) |

  Scenario: should return relationships by fetching them from the path - starting from the end
    Given an empty graph
      And having executed: CREATE (a:A)-[:REL {value: 1}]->(b:B)-[:REL {value: 2}]->(e:End)
    When executing query: MATCH p = (a)-[:REL*2..2]->(b:End) RETURN relationships(p)
    Then the result should be:
      | relationships(p)                       |
      | [[:REL {value: 1}], [:REL {value: 2}]] |

  Scenario: should return relationships by fetching them from the path
    Given an empty graph
      And having executed: CREATE (s:Start)-[:REL {value: 1}]->(b:B)-[:REL {value: 2}]->(c:C)
    When executing query: MATCH p = (a:Start)-[:REL*2..2]->(b) RETURN relationships(p)
    Then the result should be:
      | relationships(p)                       |
      | [[:REL {value: 1}], [:REL {value: 2}]] |

  Scenario: should return relationships by collecting them as a list - wrong way
    Given an empty graph
      And having executed: CREATE (a:A)-[:REL {value: 1}]->(b:B)-[:REL {value: 2}]->(e:End)
    When executing query: MATCH (a)-[r:REL*2..2]->(b:End) RETURN r
    Then the result should be:
      | r                                      |
      | [[:REL {value: 1}], [:REL {value: 2}]] |

  Scenario: should return relationships by collecting them as a list - undirected
    Given an empty graph
      And having executed: CREATE (a:End {value: 1})-[:REL {value: 1}]->(b:B)-[:REL {value: 2}]->(c:End {value : 2})
    When executing query: MATCH (a)-[r:REL*2..2]-(b:End) RETURN r
    Then the result should be:
      | r                                    |
      | [[:REL {value:1}], [:REL {value:2}]] |
      | [[:REL {value:2}], [:REL {value:1}]] |

  Scenario: should return relationships by collecting them as a list
    Given an empty graph
      And having executed: CREATE (s:Start)-[:REL {value: 1}]->(b:B)-[:REL {value: 2}]->(c:C)
    When executing query: MATCH (a:Start)-[r:REL*2..2]-(b) RETURN r
    Then the result should be:
      | r                                      |
      | [[:REL {value: 1}], [:REL {value: 2}]] |

  Scenario: should return a var length path
    Given an empty graph
      And having executed: CREATE (a:A {name: "A"})-[:KNOWS {value: 1}]->(b:B {name: "B"})-[:KNOWS {value: 2}]->(c:C {name: "C"})
    When executing query: MATCH p=(n {name:'A'})-[:KNOWS*1..2]->(x) RETURN p
    Then the result should be:
      | p                                                                                              |
      | <(:A {name: 'A'})-[:KNOWS {value: 1}]->(:B {name: 'B'})>                                       |
      | <(:A {name: 'A'})-[:KNOWS {value: 1}]->(:B {name: 'B'})-[:KNOWS {value: 2}]->(:C {name: 'C'})> |

  Scenario: a var length path of length zero
    Given an empty graph
      And having executed: CREATE (a:A)-[:REL]->(b:B)
    When executing query: MATCH p=(a)-[*0..1]->(b) RETURN a,b, length(p) AS l
    Then the result should be:
      | a    | b    | l |
      | (:A) | (:A) | 0 |
      | (:B) | (:B) | 0 |
      | (:A) | (:B) | 1 |

  Scenario: a named var length path of length zero
    Given an empty graph
      And having executed: CREATE (a:A {name: "A"})-[:KNOWS]->(b:B {name: "B"})-[:FRIEND]->(c:C {name: "C"})
    When executing query: MATCH p=(a {name:'A'})-[:KNOWS*0..1]->(b)-[:FRIEND*0..1]->(c) RETURN p
    Then the result should be:
      | p                                                                         |
      | <(:A {name: 'A'})>                                                        |
      | <(:A {name: 'A'})-[:KNOWS]->(:B {name: 'B'})>                             |
      | <(:A {name: 'A'})-[:KNOWS]->(:B {name: 'B'})-[:FRIEND]->(:C {name: 'C'})> |
