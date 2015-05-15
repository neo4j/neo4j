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

