#
# Copyright (c) 2002-2018 "Neo Technology,"
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

@db:cineast
Feature: MatchAcceptanceTest

  Scenario: path query should return results in written order
    Given init: CREATE (:label1)<-[:TYPE]-(:label2);
    When running: MATCH (a:label1) RETURN (a)<--(:label2) AS p;
    Then result:
      | p                              |
      | [(:label1)<-[:TYPE]-(:label2)] |

  Scenario: longer path query should return results in written order
    Given init: CREATE (:label1)<-[:T1]-(:label2)-[:T2]->(:label3);
    When running: MATCH (a:label1) RETURN (a)<--(:label2)--() AS p;
    Then result:
      | p                                             |
      | [(:label1)<-[:T1]-(:label2)-[:T2]->(:label3)] |

  Scenario: Get node degree via length of pattern expression
    Given init: CREATE (x:X), (x)-[:T]->(), (x)-[:T]->(), (x)-[:T]->();
    When running: MATCH (a:X) RETURN length(a-->()) as length;
    Then result:
      | length |
      | 3      |

  Scenario: Get node degree via length of pattern expression that specifies a relationship type
    Given init: CREATE (x:X), (x)-[:T]->(), (x)-[:T]->(), (x)-[:T]->(), (x)-[:AFFE]->();
    When running: MATCH (a:X) RETURN length(a-[:T]->()) as length;
    Then result:
      | length |
      | 3      |

  Scenario: Get node degree via length of pattern expression that specifies multiple relationship types
    Given init: CREATE (x:X), (x)-[:T]->(), (x)-[:T]->(), (x)-[:T]->(), (x)-[:AFFE]->();
    When running: MATCH (a:X) RETURN length(a-[:T|AFFE]->()) as length;
    Then result:
      | length |
      | 4      |

  Scenario: should be able to use multiple MATCH clauses to do a cartesian product
    Given init: CREATE ({value: 1}), ({value: 2}), ({value: 3});
    When running: MATCH n, m RETURN n.value AS n, m.value AS m;
    Then result:
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
    Given init: CREATE (:a)-[:A {foo: "bar"}]->(:b {name: 'me'})
    When running parametrized: match a-[r]->b where r.foo =~ {param} return b
      | param |
      | bar   |
    Then result:
      | b                 |
      | (:b {name: "me"}) |

  Scenario: should make query from existing database
    Given using: cineast
    When running: MATCH (n) RETURN count(n)
    Then result:
      | count(n) |
      | 63084    |

  Scenario: should filter out based on node prop name
    Given init: CREATE ({name: "Someone Else"})<-[:x]-()-[:x]->({name: "Andres"})
    When running: MATCH (start)-[rel:x]-(a) WHERE a.name = 'Andres' return a
    Then result:
      | a                  |
      | ({name: "Andres"}) |

  Scenario: should honour the column name for RETURN items
    Given init: CREATE ({name: "Someone Else"})
    When running: MATCH a WITH a.name AS a RETURN a
    Then result:
      | a            |
      | Someone Else |

  Scenario: should filter based on rel prop name
    Given init: CREATE (:a)<-[:KNOWS {name: "monkey"}]-()-[:KNOWS {name: "woot"}]->(:b)
    When running: match (node)-[r:KNOWS]->(a) WHERE r.name = 'monkey' RETURN a
    Then result:
      | a    |
      | (:a) |

  Scenario: should cope with shadowed variables
    Given init: CREATE ({value: 1, name: 'King Kong'}), ({value: 2, name: 'Ann Darrow'})
    When running: MATCH n WITH n.name AS n RETURN n
    Then result:
      | n          |
      | Ann Darrow |
      | King Kong  |

  Scenario: should get neighbours
    Given init: CREATE (a:A {value : 1})-[:KNOWS]->(b:B {value : 2})
    When running: MATCH (n1)-[rel:KNOWS]->(n2) RETURN n1, n2
    Then result:
      | n1              | n2              |
      | (:A {value: 1}) | (:B {value: 2}) |

  Scenario: should get two related nodes
    Given init: CREATE (a:A {value: 1}), (a)-[:KNOWS]->(b:B {value: 2}), (a)-[:KNOWS]->(c:C {value: 3})
    When running: MATCH (start)-[rel:KNOWS]->(x) RETURN x
    Then result:
      | x               |
      | (:B {value: 2}) |
      | (:C {value: 3}) |

  Scenario: should get related to related to
    Given init: CREATE (a:A {value: 1})-[:KNOWS]->(b:B {value: 2})-[:FRIEND]->(c:C {value: 3})
    When running: MATCH n-->a-->b RETURN b
    Then result:
      | b               |
      | (:C {value: 3}) |

  Scenario: should handle comparison between node properties
    Given init: CREATE (a:A {animal: "monkey"}), (b:B {animal: "cow"}), (c:C {animal: "monkey"}), (d:D {animal: "cow"}), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c), (d)-[:KNOWS]->(b), (d)-[:KNOWS]->(c)
    When running: MATCH (n)-[rel]->(x) WHERE n.animal = x.animal RETURN n, x
    Then result:
      | n                       | x                       |
      | (:A {animal: "monkey"}) | (:C {animal: "monkey"}) |
      | (:D {animal: "cow"})    | (:B {animal: "cow"})    |

  Scenario: should return two subgraphs with bound undirected relationship
    Given init: CREATE (a:A {value: 1})-[:REL {name: "r"}]->(b:B {value: 2})
    When running: MATCH a-[r {name: 'r'}]-b RETURN a,b
    Then result:
      | a               | b               |
      | (:B {value: 2}) | (:A {value: 1}) |
      | (:A {value: 1}) | (:B {value: 2}) |

  Scenario: should return two subgraphs with bound undirected relationship and optional relationship
    Given init: CREATE (a:A {value: 1})-[:REL {name: "r1"}]->(b:B {value: 2})-[:REL {name: "r2"}]->(c:C {value: 3})
    When running: MATCH (a)-[r {name:'r1'}]-(b) OPTIONAL MATCH (b)-[r2]-(c) WHERE r<>r2 RETURN a,b,c
    Then result:
      | a               | b               | c               |
      | (:A {value: 1}) | (:B {value: 2}) | (:C {value: 3}) |
      | (:B {value: 2}) | (:A {value: 1}) | null            |

  Scenario: rel type function works as expected
    Given init: CREATE (a:A {name: "A"}), (b:B {name: "B"}), (c:C {name: "C"}), (a)-[:KNOWS]->(b), (a)-[:HATES]->(c)
    When running: MATCH (n {name:'A'})-[r]->(x) WHERE type(r) = 'KNOWS' RETURN x
    Then result:
      | x                |
      | (:B {name: "B"}) |

  Scenario: should walk alternative relationships
    Given init: CREATE (a {name: "A"}), (b {name: "B"}), (c {name: "C"}), (a)-[:KNOWS]->(b), (a)-[:HATES]->(c), (a)-[:WONDERS]->(c)
    When running: MATCH (n)-[r]->(x) WHERE type(r) = 'KNOWS' OR type(r) = 'HATES' RETURN r
    Then result:
      | r        |
      | [:KNOWS] |
      | [:HATES] |

  Scenario: should handle OR in the WHERE clause
    Given init: CREATE (a:A {p1: 12}), (b:B {p2: 13}), (c:C)
    When running: MATCH (n) WHERE n.p1 = 12 OR n.p2 = 13 RETURN n
    Then result:
      | n             |
      | (:A {p1: 12}) |
      | (:B {p2: 13}) |

  Scenario: should return a simple path
    Given init: CREATE (a:A {name: "A"})-[:KNOWS]->(b:B {name: "B"})
    When running: MATCH p=(a {name:'A'})-->b RETURN p
    Then result:
      | p                                           |
      | (:A {name: "A"})-[:KNOWS]->(:B {name: "B"}) |

  Scenario: should return a three node path
    Given init: CREATE (a:A {name: "A"})-[:KNOWS]->(b:B {name: "B"})-[:KNOWS]->(c:C {name: "C"})
    When running: MATCH p = (a {name:'A'})-[rel1]->b-[rel2]->c RETURN p
    Then result:
      | p                                                                      |
      | (:A {name: "A"})-[:KNOWS]->(:B {name: "B"})-[:KNOWS]->(:C {name: "C"}) |

  Scenario: should not return anything because path length does not match
    Given init: CREATE (a:A {name: "A"})-[:KNOWS]->(b:B {name: "B"})
    When running: MATCH p = n-->x WHERE length(p) = 10 RETURN x
    Then result:
      |  |

  Scenario: should pass the path length test
    Given init: CREATE (a:A {name: "A"})-[:KNOWS]->(b:B {name: "B"})
    When running: MATCH p = n-->x WHERE length(p)=1 RETURN x
    Then result:
      | x                |
      | (:B {name: "B"}) |

  Scenario: should be able to filter on path nodes
    Given init: CREATE (a:A {foo: "bar"})-[:REL]->(b:B {foo: "bar"})-[:REL]->(c:C {foo: "bar"})-[:REL]->(d:D {foo: "bar"})
    When running: MATCH p = pA-[:REL*3..3]->pB WHERE all(i in nodes(p) WHERE i.foo = 'bar') RETURN pB
    Then result:
      | pB                |
      | (:D {foo: "bar"}) |

  Scenario: should return relationships by fetching them from the path - starting from the end
    Given init: CREATE (a:A)-[:REL {value: 1}]->(b:B)-[:REL {value: 2}]->(e:End)
    When running: MATCH p = a-[:REL*2..2]->(b:End) RETURN relationships(p)
    Then result:
      | relationships(p)                       |
      | [[:REL {value: 1}], [:REL {value: 2}]] |

  Scenario: should return relationships by fetching them from the path
    Given init: CREATE (s:Start)-[:REL {value: 1}]->(b:B)-[:REL {value: 2}]->(c:C)
    When running: MATCH p = (a:Start)-[:REL*2..2]->b RETURN relationships(p)
    Then result:
      | relationships(p)                       |
      | [[:REL {value: 1}], [:REL {value: 2}]] |

  Scenario: should return relationships by collecting them as a list - wrong way
    Given init: CREATE (a:A)-[:REL {value: 1}]->(b:B)-[:REL {value: 2}]->(e:End)
    When running: MATCH a-[r:REL*2..2]->(b:End) RETURN r
    Then result:
      | r                                      |
      | [[:REL {value: 1}], [:REL {value: 2}]] |

  Scenario: should return relationships by collecting them as a list - undirected
    Given init: CREATE (a:End {value: 1})-[:REL {value: 1}]->(b:B)-[:REL {value: 2}]->(c:End {value : 2})
    When running: MATCH a-[r:REL*2..2]-(b:End) RETURN r
    Then result:
      | r                                      |
      | [[:REL {value: 1}], [:REL {value: 2}]] |
      | [[:REL {value: 2}], [:REL {value: 1}]] |

  Scenario: should return relationships by collecting them as a list
    Given init: CREATE (s:Start)-[:REL {value: 1}]->(b:B)-[:REL {value: 2}]->(c:C)
    When running: MATCH (a:Start)-[r:REL*2..2]->b RETURN r
    Then result:
      | r                                      |
      | [[:REL {value: 1}], [:REL {value: 2}]] |

  Scenario: should return a var length path
    Given init: CREATE (a:A {name: "A"})-[:KNOWS {value: 1}]->(b:B {name: "B"})-[:KNOWS {value: 2}]->(c:C {name: "C"})
    When running: MATCH p=(n {name:'A'})-[:KNOWS*1..2]->x RETURN p
    Then result:
      | p                                                                                            |
      | (:A {name: "A"})-[:KNOWS {value: 1}]->(:B {name: "B"})                                       |
      | (:A {name: "A"})-[:KNOWS {value: 1}]->(:B {name: "B"})-[:KNOWS {value: 2}]->(:C {name: "C"}) |

  Scenario: a var length path of length zero
    Given init: CREATE (a:A)-[:REL]->(b:B)
    When running: MATCH p=a-[*0..1]->b RETURN a,b, length(p) AS l
    Then result:
      | a    | b    | l |
      | (:A) | (:A) | 0 |
      | (:B) | (:B) | 0 |
      | (:A) | (:B) | 1 |

  Scenario: a named var length path of length zero
    Given init: CREATE (a:A {name: "A"})-[:KNOWS]->(b:B {name: "B"})-[:FRIEND]->(c:C {name: "C"})
    When running: MATCH p=(a {name:'A'})-[:KNOWS*0..1]->b-[:FRIEND*0..1]->c RETURN p
    Then result:
      | p                                                                       |
      | (:A {name: "A"})                                                        |
      | (:A {name: "A"})-[:KNOWS]->(:B {name: "B"})                             |
      | (:A {name: "A"})-[:KNOWS]->(:B {name: "B"})-[:FRIEND]->(:C {name: "C"}) |
