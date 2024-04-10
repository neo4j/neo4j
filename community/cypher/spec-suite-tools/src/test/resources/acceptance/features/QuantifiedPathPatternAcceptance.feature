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

Feature: QuantifiedPathPatternAcceptance

  Background:
    Given an empty graph

  Scenario: Simple quantified path pattern
    And having executed:
      """
      CREATE (),
             ()-[:REL]->(),
             ()-[:REL]->()-[:REL]->(),
             ()-[:REL]->()-[:REL]->()-[:REL]->()
      """

    When executing query:
      """
      MATCH () (()-->()){1, 2}
      RETURN count(*) AS count
      """
    Then the result should be, in any order:
      | count |
      | 9     |
    And no side effects

  Scenario Outline: Fail on quantification of single node
    When executing query:
      """
      MATCH (())<quantifier>
      RETURN count(*)
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | quantifier |
      | {1}        |
      | {0, 1}     |
      | {, 1}      |
      | {0, }      |
      | {1, 2}     |
      | +          |
      | *          |


  Scenario Outline: Fail on possibly empty quantification
    When executing query:
      """
      MATCH (()--())<quantifier>
      RETURN count(*)
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | quantifier |
      | {0, 1}     |
      | {, 1}      |
      | {0, }      |
      | *          |

  Scenario Outline: Do not fail on empty quantification with other patterns next to it.
    When executing query:
      """
      MATCH <pattern>
      RETURN count(*) AS count
      """
    Then the result should be, in any order:
      | count |
      | 0     |
    Examples:
      | pattern                            |
      | ()        (()--()){0, 1}           |
      |           (()--()){0, 1} ()        |
      | (()--())+ (()--()){0, 1}           |
      |           (()--()){0, 1} (()--())+ |


  Scenario: Top level path must have a minimum length > 0 - Kleene star
    When executing query:
      """
      MATCH ((a)-->(b))*
      RETURN *
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Top level path must have a minimum length > 0 - {0,} quantifier
    When executing query:
      """
      MATCH ((a)-->(b)){0,}
      RETURN *
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Top level path must have a minimum length > 0 - juxtaposed
    When executing query:
      """
      MATCH ((a)-->(b))*((c)-->(d)){0,}
      RETURN *
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario Outline: Quantifier lower bound must be less than or equal to upper bound, upper bound needs to be positive
    When executing query:
      """
      MATCH ()((a)-->(b))<quantifier>
      RETURN *
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | quantifier |
      | {3,2}      |
      | {-1}       |
      | {0}        |
      | {0, 0}     |
      | {, 0}      |

  Scenario Outline: Quantified path pattern cannot be nested
    When executing query:
      """
      MATCH <pattern>
      RETURN *
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | pattern                |
      | (((a)-[b]->(c))*)+     |
      | ((a)-->(b)-[r]->*(c))+ |

  Scenario Outline: A group variable can only appear in a single quantified path pattern - Single MATCH
    When executing query:
      """
      MATCH <pattern>
      RETURN *
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | pattern                                        |
      | ((a)-[b]->(c))* (d)-[e]->(a)                   |
      | ((a)-[b]->(c))* (d)-[b]->(f)                   |
      | ((a)-[b]->(c))*, (d)-[e]->(a)                  |
      | ((a)-[b]->(c))*, (d)-[b]->(f)                  |
      | ((a)-[b]->(c))* (d)-[e]->() ((a)-[f]->(g)){2,} |
      | ((a)-[b]->(c))* (d)-[b]->+(f)                  |
      | ((a)-[b]->(c))+ ((d)-[b]->(f))*                |
      | ((a)-[b]->(c))+ ((a)-[e]->(f))*                |

  Scenario Outline: A group variable can only appear in a single quantified path pattern - Multiple MATCH
    When executing query:
      """
      MATCH <pattern1>
      MATCH <pattern2>
      RETURN *
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | pattern1                   | pattern2                         |
      | (a)-->(b)                  | (x)--(y)((a)-->(t)){1,5}()-->(z) |
      | ((a)-->(b))+               | (x)--(y)((a)-->(t)){1,5}()-->(z) |
      | ((a)-[b]->(c))*            | (d)-[e]->(a)                     |
      | ((a)-[b]->(c))*            | (d)-[b]->(f)                     |
      | ((a)-[b]->(c))*(d)-[e]->() | ((a)-[f]->(g)){2,}               |
      | ((a)-[b]->(c))*            | (d)-[b]->+(f)                    |
      | ((a)-[b]->(c))+            | ((d)-[b]->(f))*                  |
      | ((a)-[b]->(c))+            | ((a)-[e]->(f))*                  |

  Scenario: Parenthesizing a path pattern without quantifying it is currently not allowed
    When executing query:
      """
      MATCH p1 = (a:X)-[b]->(c)(p2 = ((x:A)-[:R]->(z:B WHERE z.h > 2))*)
      RETURN *
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: equijoin with a group variable will always return a false
    When executing query:
      """
      MATCH (a)-[e]->*(b)-[f]->(c) WHERE a = e
      RETURN count(*) as count
      """
    Then the result should be, in any order:
      | count |
      | 0     |

  Scenario: Usage of group variables from other quantified path patterns that happen to be in scope is not allowed - single match
    When executing query:
      """
      MATCH ((a)-->(b))+(c)-->(d)((e)-->(f) WHERE e.x = a.x)+
      RETURN *
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Usage of group variables from other quantified path patterns that happen to be in scope is not allowed - multiple matches
    When executing query:
      """
      MATCH (r)-[s]->+(t)
      MATCH (x)-->(y)((a)-[e]->(b) WHERE e.h > s.h)*(t)-->(u)
      RETURN *
      """
    Then a SyntaxError should be raised at compile time: *

  Scenario: Matching path pattern 0 times - using Kleene star
    Given having executed:
      """
      CREATE ({prop: 42})
      """
    When executing query:
      """
      MATCH (a) ((:UnknownLabel)-->())*
      RETURN a.prop
      """
    Then the result should be, in any order:
      | a.prop |
      |     42 |

  Scenario: Matching path pattern 1+ times - using Kleene plus
    And having executed:
      """
      CREATE (:A)-[:R { id : 1} ]->(:B)-[:R { id : 2 } ]->(:C)
      """
    When executing query:
      """
      MATCH ((x)-[y]->(z))+
      RETURN x, y, z
      """
    Then the result should be, in any order:
      | x            | y                            | z            |
      | [(:A)]       | [[:R {id: 1}]]               | [(:B)]       |
      | [(:A), (:B)] | [[:R {id: 1}], [:R {id: 2}]] | [(:B), (:C)] |
      | [(:B)]       | [[:R {id: 2}]]               | [(:C)]       |

  Scenario: Matching path pattern exactly 2 times - using {2,2}
    And having executed:
      """
      CREATE (:A)-[:R { id : 1} ]->(:B)-[:R { id : 2 } ]->(:C)
      """
    When executing query:
      """
      MATCH ((x)-[y]->(z)){2,2}
      RETURN x, y, z
      """
    Then the result should be, in any order:
      | x            | y                            | z            |
      | [(:A), (:B)] | [[:R {id: 1}], [:R {id: 2}]] | [(:B), (:C)] |

  Scenario: Matching path pattern exactly 2 times - using {2}
    And having executed:
      """
      CREATE (:A)-[:R { id : 1} ]->(:B)-[:R { id : 2 } ]->(:C)
      """
    When executing query:
      """
      MATCH ((x)-[y]->(z)){2}
      RETURN x, y, z
      """
    Then the result should be, in any order:
      | x            | y                            | z            |
      | [(:A), (:B)] | [[:R {id: 1}], [:R {id: 2}]] | [(:B), (:C)] |

  Scenario: Matching path pattern 1+ times - using {1,}
    And having executed:
      """
      CREATE (:A)-[:R { id : 1} ]->(:B)-[:R { id : 2 } ]->(:C)
      """
    When executing query:
      """
      MATCH ((x)-[y]->(z)){1,}
      RETURN x, y, z
      """
    Then the result should be, in any order:
      | x            | y                            | z            |
      | [(:A)]       | [[:R {id: 1}]]               | [(:B)]       |
      | [(:A), (:B)] | [[:R {id: 1}], [:R {id: 2}]] | [(:B), (:C)] |
      | [(:B)]       | [[:R {id: 2}]]               | [(:C)]       |

  Scenario: Juxtaposing un-anchored node pattern and QPP
    And having executed:
      """
      CREATE (:A)-[:R { id : 1} ]->(:B)-[:R { id : 2 } ]->(:C)
      """
    When executing query:
      """
      MATCH (x)((y)-[]->())+
      RETURN x, y
      """
    Then the result should be, in any order:
      | x    | y            |
      | (:A) | [(:A)]       |
      | (:A) | [(:A), (:B)] |
      | (:B) | [(:B)]       |

  Scenario: Juxtaposing anchored node pattern and QPP
    And having executed:
      """
      CREATE (:A)-[:R { id : 1} ]->(:B)-[:R { id : 2 } ]->(:C)
      """
    When executing query:
      """
      MATCH (x:A)((y)-[]->())+
      RETURN x, y
      """
    Then the result should be, in any order:
      | x    | y            |
      | (:A) | [(:A)]       |
      | (:A) | [(:A), (:B)] |

  Scenario: Juxtaposing anchored node pattern and QPP with min length 0
    And having executed:
      """
      CREATE (:A)-[:R { id : 1} ]->(:B)-[:R { id : 2 } ]->(:C)
      """
    When executing query:
      """
      MATCH (x:A)((y)-[]->())*
      RETURN x, y
      """
    Then the result should be, in any order:
      | x    | y            |
      | (:A) | []           |
      | (:A) | [(:A)]       |
      | (:A) | [(:A), (:B)] |

  Scenario: Juxtaposing anchored node patterns that sandwich a QPP with min length 1
    And having executed:
      """
      CREATE (:A:B)-[:R]->(:A:C)-[:R]->(:D)
      """
    When executing query:
      """
      MATCH (l:A&!C)((m)-[]->(n))+(o:D)
      RETURN l, m, n, o
      """
    Then the result should be, in any order:
      | l      | m                | n              | o    |
      | (:A:B) | [(:A:B), (:A:C)] | [(:A:C), (:D)] | (:D) |

  Scenario: Juxtaposing node patterns sandwiching a QPP with min length 0
    And having executed:
      """
      CREATE (:A)-[:R]->(:B)-[:R]->(:C)
      """
    When executing query:
      """
      MATCH (l)((m)-[]->(n))*(o)
      RETURN l, m, n, o
      """
    Then the result should be, in any order:
      | l    | m            | n            | o    |
      | (:A) | []           | []           | (:A) |
      | (:A) | [(:A)]       | [(:B)]       | (:B) |
      | (:A) | [(:A), (:B)] | [(:B), (:C)] | (:C) |
      | (:B) | []           | []           | (:B) |
      | (:B) | [(:B)]       | [(:C)]       | (:C) |
      | (:C) | []           | []           | (:C) |

  Scenario: Juxtaposing fixed length pattern and QPP
    And having executed:
      """
      CREATE (:A)-[:R]->(:B)-[:R]->(:C)
      """
    When executing query:
      """
      MATCH (l)-->(m)((n)-[]->(o))*
      RETURN l, m, n, o
      """
    Then the result should be, in any order:
      | l    | m    | n      | o      |
      | (:A) | (:B) | []     | []     |
      | (:A) | (:B) | [(:B)] | [(:C)] |
      | (:B) | (:C) | []     | []     |

  Scenario: Quantified path pattern where pattern is length > 1
    And having executed:
      """
      CREATE (:A)-[:R]->(:B)<-[:S]-(:C)-[:R]->(:B)<-[:S]-(:A)
      """
    When executing query:
      """
      MATCH ((x)-[r:R]->(y)<-[s:S]-(z)){1,2}
      RETURN x, r, y, s, z
      """
    Then the result should be, in any order:
      | x            | r            | y            | s            | z            |
      | [(:A)]       | [[:R]]       | [(:B)]       | [[:S]]       | [(:C)]       |
      | [(:A), (:C)] | [[:R], [:R]] | [(:B), (:B)] | [[:S], [:S]] | [(:C), (:A)] |
      | [(:C)]       | [[:R]]       | [(:B)]       | [[:S]]       | [(:A)]       |

  Scenario: Graph pattern to match T-shaped sub-graphs
    And having executed:
      """
      CREATE (:n1)-[:F]->(x:n2)-[:F]->(y:n3)
      CREATE (x)-[:E]->(:n4)-[:E]->(:n5)
      CREATE (y)-[:F]->(:n6)
      """
    When executing query:
      """
      MATCH (x)-[:F]->(y)-[:F]->(z),
            (y)((a)-[:E]->(b))+
      RETURN x, y, z, a, b
      """
    Then the result should be, in any order:
      | x     | y     | z     | a              | b              |
      | (:n1) | (:n2) | (:n3) | [(:n2)]        | [(:n4)]        |
      | (:n1) | (:n2) | (:n3) | [(:n2), (:n4)] | [(:n4), (:n5)] |

  Scenario: Solutions can be assigned to path variable
    And having executed:
      """
      CREATE (:A)-[:R]->(:A:B)-[:R]->(:C)
      """
    When executing query:
      """
      MATCH p=((x:A)-[y]->(z))+
      RETURN p
      """
    Then the result should be, in any order:
      | p                              |
      | <(:A)-[:R]->(:A:B)>            |
      | <(:A:B)-[:R]->(:C)>            |
      | <(:A)-[:R]->(:A:B)-[:R]->(:C)> |

  Scenario: Solutions can be assigned to path variable with anonymous variable
    And having executed:
      """
      CREATE (:A)-[:R]->(:A:B)-[:R]->(:C)
      """
    When executing query:
      """
      MATCH p=((:A)-[y]->(z))+
      RETURN p
      """
    Then the result should be, in any order:
      | p                              |
      | <(:A)-[:R]->(:A:B)>            |
      | <(:A:B)-[:R]->(:C)>            |
      | <(:A)-[:R]->(:A:B)-[:R]->(:C)> |


  Scenario: Solutions can be assigned to path variable with two juxtaposed quantified path patterns
    And having executed:
      """
      CREATE (:A)-[:R]->(:B)-[:R]->(:C)-[:R]->(:D)
      """
    When executing query:
      """
      MATCH p = ((n)-[r]->(m)-[q]->(o))+ ((b)-[r2]->(y))*
      RETURN p
      """
    Then the result should be, in any order:
      | p                                                |
      | <(:A)-[:R {}]->(:B)-[:R {}]->(:C)>               |
      | <(:B)-[:R {}]->(:C)-[:R {}]->(:D)>               |
      | <(:A)-[:R {}]->(:B)-[:R {}]->(:C)-[:R {}]->(:D)> |

  Scenario: Path pattern can use label expressions for nodes
    And having executed:
      """
      CREATE (:A)-[:R]->(:A:B)-[:R]->(:A:C)-[:R]->(:D)
      """
    When executing query:
      """
      MATCH ((x:A&!C)-->(y))+
      RETURN x, y
      """
    Then the result should be, in any order:
      | x              | y                |
      | [(:A)]         | [(:A:B)]         |
      | [(:A), (:A:B)] | [(:A:B), (:A:C)] |
      | [(:A:B)]       | [(:A:C)]         |

  Scenario: Path patterns can use label expressions for relationships
    And having executed:
      """
      CREATE (:X)<-[:Z]-(:A)-[:R]->(:B)-[:S]->(:C)
      """
    When executing query:
      """
      MATCH (()-[x:R|S]->())+
      RETURN x
      """
    Then the result should be, in any order:
      | x            |
      | [[:R]]       |
      | [[:R], [:S]] |
      | [[:S]]       |

  Scenario Outline: Pattern expression/EXISTS inside a QPP is allowed
    And having executed:
      """
      CREATE (:X)<-[:Z]-(:A)-[:R]->(:B)-[:S]->(:C)
      """
    When executing query:
    """
     MATCH ( (a)-[]-(b)-[]-(c) WHERE <pattern> )+
     RETURN *
    """
    Then the result should be, in any order:
      | a      | b      | c      |
      | [(:A)] | [(:B)] | [(:C)] |
    Examples:
      | pattern                  |
      |          (a)-[]->(:X)    |
      | EXISTS { (a)-[]->(:X) }  |
      | EXISTS { (a)-[]->+(:X) } |

  Scenario: COUNT inside a QPP is allowed
    And having executed:
      """
      CREATE (:X)<-[:Z]-(a:A)-[:R]->(:B)-[:S]->(:C)
      """
    When executing query:
    """
     MATCH ( (a)-[]-(b)-[]-(c) WHERE COUNT { (a)-[]->+(:X) } > 0 )+
     RETURN *
    """
    Then the result should be, in any order:
      | a      | b      | c      |
      | [(:A)] | [(:B)] | [(:C)] |

  Scenario: QPP can appear in an EXISTS clause
    And having executed:
      """
      CREATE (:A {p: 1})
      CREATE (:A {p: 2})-[:K]->(:B)
      CREATE (:A {p: 3})-[:K]->()-[:K]->(:B)
      """
    When executing query:
      """
      MATCH (a:A) WHERE EXISTS { (a) (()-[:K]->())+ (:B) }
      RETURN a.p AS result
      """
    Then the result should be, in any order:
      | result |
      | 2      |
      | 3      |

  Scenario: QPP can appear in an OPTIONAL MATCH CLAUSE
    And having executed:
      """
      CREATE (:A)
      CREATE (:A)-[:K]->(:B {p: 1})
      CREATE (:A)-[:K]->()-[:K]->(:B {p: 2})
      """
    When executing query:
      """
      MATCH (a:A)
      OPTIONAL MATCH (a) (()-[:K]->())+ (b:B)
      RETURN b.p AS result
      """
    Then the result should be, in any order:
      | result |
      | null   |
      | 1      |
      | 2      |

  # Solved in https://trello.com/c/ufHPMj0x/
  # Assigning a Sub-path in a quantified path pattern is not yet supported
  @Fails
  Scenario: Path and subpath variables
    And having executed:
      """
      CREATE (n99:X)-[r0:Y]->(n1:A{h:1})-[r1:R]->(n2:A:B{h:3})-[r2:R]->(n3:A:B{h:5}),
             (n3)-[r3:R]->(n4:A{h:7}),
             (n3)-[r4:R]->(n5:A:B{h:9}),
             (n3)-[r5:R]->(n6:B{h:11}),
             (n3)-[r6:R]->(n7:A:B{h:1})
      """
    When executing query:
      """
      MATCH p1 = (a:X)-[b]->(c)(p2 = (x:A)-[:R]->(z:B WHERE z.h > 2))*
      RETURN p1, a, b, c, p2, x, z
      """
    Then the result should be, in any order:
      | p1                                                                       | a    | b    | c         | p2                                                                                           | x                                   | z                                     |
      | [(:X),[:Y],(:A{h:1})]                                                    | (:X) | [:Y] | (:A{h:1}) | []                                                                                           | []                                  | []                                    |
      | [(:X),[:Y],(:A{h:1}),[:R],(:A:B{h:3})]                                   | (:X) | [:Y] | (:A{h:1}) | [[(:A{h:1}),[:R],(:A:B{h:3})]]                                                               | [(:A{h:1})]                         | [(:A:B{h:3})]                         |
      | [(:X),[:Y],(:A{h:1}),[:R],(:A:B{h:3}),[:R],(:A:B{h:5})]                  | (:X) | [:Y] | (:A{h:1}) | [[(:A{h:1}),[:R],(:A:B{h:3})],[(:A:B{h:3}),[:R],(:A:B{h:5})]]                                | [(:A{h:1}),(:A:B{h:3})]             | [(:A:B{h:3}),(:A:B{h:5})]             |
      | [(:X),[:Y],(:A{h:1}),[:R],(:A:B{h:3}),[:R],(:A:B{h:5}),[:R],(:A:B{h:9})] | (:X) | [:Y] | (:A{h:1}) | [[(:A{h:1}),[:R],(:A:B{h:3})],[(:A:B{h:3}),[:R],(:A:B{h:5})],[(:A:B{h:5}),[:R],(:A:B{h:9})]] | [(:A{h:1}),(:A:B{h:3}),(:A:B{h:5})] | [(:A:B{h:3}),(:A:B{h:5}),(:A:B{h:9})] |
      | [(:X),[:Y],(:A{h:1}),[:R],(:A:B{h:3}),[:R],(:A:B{h:5}),[:R],(:B{h:11})]  | (:X) | [:Y] | (:A{h:1}) | [[(:A{h:1}),[:R],(:A:B{h:3})],[(:A:B{h:3}),[:R],(:A:B{h:5})],[(:A:B{h:5}),[:R],(:B{h:11})]]  | [(:A{h:1}),(:A:B{h:3}),(:A:B{h:5})] | [(:A:B{h:3}),(:A:B{h:5}),(:B{h:11})]  |


  # Leaving out nodes is currently not supported. Might be handled in https://trello.com/c/rcFdSCvc/
  @Fails
  Scenario: Leaving out the nodes adjacent to a QPP
    And having executed:
      """
      CREATE (:A)-[:R]->(:B)-[:R]->(:C)-[:R]->(:D)
      """
    When executing query:
      """
      MATCH (a)-[b]->((c)-[d]->(e))*-[f]->+(g)
      RETURN *
      """
    Then the result should be, in any order:
      | a    | b    | c      | d      | e      | f      | g      |
      | (:A) | (:R) | []     | []     | []     | [[:R]] | [(:C)] |
      | (:A) | (:R) | []     | []     | []     | [[:R]] | [(:D)] |
      | (:B) | (:R) | []     | []     | []     | [[:R]] | [(:D)] |
      | (:A) | (:R) | [(:B)] | [[:R]] | [(:C)] | [[:R]] | [(:D)] |

  Scenario: Multiple use of variable in single Quantified Path Pattern
    And having executed:
      """
      CREATE (n:A)-[:R]->(:B)-[:R]->(n)
      CREATE (n)-[:R]->(:C)-[:R]->(:D)
      """
    When executing query:
      """
      MATCH ((a)-[e]->(b)-[f]->(a))+(p)-[g]->(r)-[q]->(s)
      RETURN a, b, p, r, s
      """
    Then the result should be, in any order:
      | a      | b      | p    | r    | s    |
      | [(:A)] | [(:B)] | (:A) | (:C) | (:D) |

  Scenario: An equijoin is valid provided this is on unconditional singletons only
    And having executed:
      """
      CREATE (n:A)-[:R]->(:B)-[:R]->(n)
      """
    When executing query:
      """
      MATCH (a)-[e]->*(b)-[f]->(c) WHERE a = c
      RETURN a, b, c
      """
    Then the result should be, in any order:
      | a    | b    | c    |
      | (:B) | (:A) | (:B) |
      | (:A) | (:B) | (:A) |

  Scenario: Local predicate in the unconditional singleton context, scoped within the quantified path pattern
    And having executed:
      """
      CREATE (:A {h: 13})-[:R]->(:B{h: 14})-[:R]->(:C)
      """
    When executing query:
      """
      MATCH ((a WHERE a.h > 12)-[e]->(b))+
      RETURN a, b
      """
    Then the result should be, in any order:
      | a                            | b                    |
      | [(:A {h: 13})]               | [(:B {h: 14})]       |
      | [(:B {h: 14})]               | [(:C)]               |
      | [(:A {h: 13}), (:B {h: 14})] | [(:B {h: 14}), (:C)] |

  Scenario: Local comparison predicate in the unconditional singleton context, scoped within the quantified path pattern
    And having executed:
      """
      CREATE (:A {h: 15})-[:R]->(:A:B {h: 14})-[:R]->(:B{h: 13})
      """
    When executing query:
      """
      MATCH ((a:A)-[e]->(b:B) WHERE a.h > b.h){2,}
      RETURN a, b
      """
    Then the result should be, in any order:
      | a                              | b                              |
      | [(:A {h: 15}), (:A:B {h: 14})] | [(:A:B {h: 14}), (:B {h: 13})] |

  Scenario: Non-local unconditional singletons that are already in scope can be used in the unconditional singleton context, scoped within the quantified path pattern
    And having executed:
      """
      CREATE (:A {h: 11})-[:R]->(:B {h: 12})-[:R]->(:C{h: 13})-[:R]->(:D{h: 14})
      """
    When executing query:
      """
      MATCH (m)-->(n)
      MATCH (x)-->(y)((a)-[e]->(b) WHERE a.h > m.h)+(s)-->(u)
      RETURN m,n,x,y,a,b,s,u
      """
    Then the result should be, in any order:
      | m            | n            | x            | y            | a              | b              | s            | u            |
      | (:A {h: 11}) | (:B {h: 12}) | (:A {h: 11}) | (:B {h: 12}) | [(:B {h: 12})] | [(:C {h: 13})] | (:C {h: 13}) | (:D {h: 14}) |

  # Solved in https://trello.com/c/hO4INisk/
  # Horizontal aggregation not implemented
  @Fails
  Scenario: Referencing previously-bound, non-local unconditional singleton
    And having executed:
      """
      CREATE (:A {h: 3})-[:R{weight: 4}]->(:B {h: 2})-[:R{weight: 5}]->(:C{h: 4})-[:R{weight: 0}]->(:C{h: 3})-[:R{weight: 6}]->(:D{h: 2})
      """
    When executing query:
      """
      MATCH (m)-->(n)
      MATCH (x)-->(y)((a)-[e]->(b)){,8}(s)-->(u) WHERE AVG(e.weight) > m.h
      RETURN *
      """
    Then the result should be, in any order:
      | a                          | b                          | e                                    | m           | n           | s           | u           | x           | y           |
      | [(:B {h: 2})]              | [(:C {h: 4})]              | [[:R {weight: 5}]]                   | (:A {h: 3}) | (:B {h: 2}) | (:C {h: 4}) | (:C {h: 3}) | (:A {h: 3}) | (:B {h: 2}) |
      | [(:B {h: 2})]              | [(:C {h: 4})]              | [[:R {weight: 5}]]                   | (:B {h: 2}) | (:C {h: 4}) | (:C {h: 4}) | (:C {h: 3}) | (:A {h: 3}) | (:B {h: 2}) |
      | [(:B {h: 2})]              | [(:C {h: 4})]              | [[:R {weight: 5}]]                   | (:C {h: 4}) | (:C {h: 3}) | (:C {h: 4}) | (:C {h: 3}) | (:A {h: 3}) | (:B {h: 2}) |
      | [(:B {h: 2})]              | [(:C {h: 4})]              | [[:R {weight: 5}]]                   | (:C {h: 3}) | (:D {h: 2}) | (:C {h: 4}) | (:C {h: 3}) | (:A {h: 3}) | (:B {h: 2}) |
      | [(:C {h: 4}), (:B {h: 2})] | [(:C {h: 3}), (:C {h: 4})] | [[:R {weight: 0}], [:R {weight: 5}]] | (:A {h: 3}) | (:B {h: 2}) | (:C {h: 3}) | (:D {h: 2}) | (:A {h: 3}) | (:B {h: 2}) |
      | [(:C {h: 4}), (:B {h: 2})] | [(:C {h: 3}), (:C {h: 4})] | [[:R {weight: 0}], [:R {weight: 5}]] | (:C {h: 4}) | (:C {h: 3}) | (:C {h: 3}) | (:D {h: 2}) | (:A {h: 3}) | (:B {h: 2}) |
      | [(:C {h: 4}), (:B {h: 2})] | [(:C {h: 3}), (:C {h: 4})] | [[:R {weight: 0}], [:R {weight: 5}]] | (:C {h: 3}) | (:D {h: 2}) | (:C {h: 3}) | (:D {h: 2}) | (:A {h: 3}) | (:B {h: 2}) |

  # Solved in https://trello.com/c/hO4INisk/
  # Horizontal aggregation not implemented
  @Fails
  Scenario: Referencing non-local unconditional singletons within the same path pattern containing the quantified path pattern
    And having executed:
      """
      CREATE (:A {h: 3})-[:R{weight: 4}]->(:B {h: 2})-[:R{weight: 5}]->(:C{h: 4})-[:R{weight: 0}]->(:C{h: 3})-[:R{weight: 6}]->(:D{h: 2})
      """
    When executing query:
      """
      MATCH (x)-->(y)((a)-[e]->(b)){,8}(s)-->(u) WHERE AVG(e.weight) > x.h
      RETURN *
      """
    Then the result should be, in any order:
      | a             | b             | e                  | s           | u           | x           | y           |
      | [(:B {h: 2})] | [(:C {h: 4})] | [[:R {weight: 5}]] | (:C {h: 4}) | (:C {h: 3}) | (:A {h: 3}) | (:B {h: 2}) |


  # Solved in https://trello.com/c/hO4INisk/
  # Horizontal aggregation not implemented
  @Fails
  Scenario: Using an unconditional singleton together with a group variable to formulate an expression as input to an aggregating operation
    And having executed:
      """
      CREATE (:A {h: 11})-[:R{weight: 4}]->(:B {h: 12})-[:R{weight: 5}]->(:C{h: 13})-[:R{weight: 0}]->(:C{h: 13})-[:R{weight: 6}]->(:D{h: 14})
      """
    When executing query:
      """
      MATCH (x)-->(y)((a)-[e]->(b)){,8}(s)-->(u) WHERE AVG(e.weight * x.h) > 34
      RETURN *
      """
    Then the result should be, in any order:
      | a              | b              | e                  | s            | u            | x            | y            |
      | [(:B {h: 12})] | [(:C {h: 13})] | [[:R {weight: 5}]] | (:C {h: 13}) | (:C {h: 13}) | (:A {h: 11}) | (:B {h: 12}) |

  # Solved in https://trello.com/c/hO4INisk/
  # Horizontal aggregation not implemented
  @Fails
  Scenario: Using an multiple unconditional singletons together with a group variable to formulate an expression as input to an aggregating operation
    And having executed:
      """
      CREATE (:A {h: 11, k: 2})-[:R{weight: 4}]->(:B {h: 12, k: 2})-[:R{weight: 5}]->(:C{h: 13, k: 2})-[:R{weight: 0}]->(:C{h: 13, k: 2})-[:R{weight: 6}]->(:D{h: 14, k: 2})
      """
    When executing query:
      """
      MATCH (x)-->(y)((a)-[e]->(b)){,8}(s)-->(u) WHERE AVG(e.weight * x.h * s.k) > 55
      RETURN *
      """
    Then the result should be, in any order:
      | a                    | b                    | e                  | s                  | u                  | x                  | y                  |
      | [(:B {h: 12, k: 2})] | [(:C {h: 13, k: 2})] | [[:R {weight: 5}]] | (:C {h: 13, k: 2}) | (:C {h: 13, k: 2}) | (:A {h: 11, k: 2}) | (:B {h: 12, k: 2}) |

  # Solved in https://trello.com/c/hO4INisk/
  # Horizontal aggregation not implemented
  @Fails
  Scenario: Multiple references to the same group variable within an aggregating operation
    And having executed:
      """
      CREATE (:A {h: 11, k: 4})-[:R{weight: 4}]->(:B {h: 12, k: 4})-[:R{weight: 5}]->(:C{h: 13, k: 4})-[:R{weight: 0}]->(:D{h: 13, k: 4})
      """
    When executing query:
      """
      MATCH (x)-->() ((a)-[e]->(b)){,8} ()-->(u) WHERE SUM(b.h * b.k) > 55
      RETURN *
      """
    Then the result should be, in any order:
      | a                                        | b                                        | e                                    | u                  | x                  |
      | [(:A {h: 11, k: 4}), (:B {h: 12, k: 4})] | [(:B {h: 12, k: 4}), (:C {h: 13, k: 4})] | [[:R {weight: 4}], [:R {weight: 5}]] | (:D {h: 13, k: 4}) | (:A {h: 11, k: 4}) |

  # Solved in https://trello.com/c/hO4INisk/
  # Horizontal aggregation not implemented
  @Fails
  Scenario: Multiple aggregating operations
    And having executed:
      """
      CREATE (:A {h: 11, k: 4})-[:R{weight: 5}]->(:B {h: 12, k: 4})-[:R{weight: 4}]->(:C{h: 13, k: 4})-[:R{weight: 0}]->(:D{h: 13, k: 4})
      """
    When executing query:
      """
      MATCH (x)-->() ((a)-[e]->(b)){,8} ()-->(u) WHERE AVG(e.weight) + AVG(b.h) > 16
      RETURN *
      """
    Then the result should be, in any order:
      | a                                        | b                                        | e                                    | u                  | x                  |
      | [(:A {h: 11, k: 4}), (:B {h: 12, k: 4})] | [(:B {h: 12, k: 4}), (:C {h: 13, k: 4})] | [[:R {weight: 5}], [:R {weight: 4}]] | (:D {h: 13, k: 4}) | (:A {h: 11, k: 4}) |

  Scenario Outline: References to non-local unconditional singletons that are dependent on the evaluation of the quantification
    And having executed:
      """
      CREATE (:A {h: 11})-[:R]->(:B {h: 12})-[:R]->(:C{h: 13})-[:R]->(:D{h: 10})
      """
    When executing query:
      """
      <pattern>
      RETURN x, y, a, e, b, s, u
      """
    Then the result should be, in any order:
      | x            | y            | a              | e      | b              | s            | u            |
      | (:A {h: 11}) | (:B {h: 12}) | []             | []     | []             | (:B {h: 12}) | (:C {h: 13}) |
      | (:B {h: 12}) | (:C {h: 13}) | []             | []     | []             | (:C {h: 13}) | (:D {h: 10}) |
      | (:A {h: 11}) | (:B {h: 12}) | [(:B {h: 12})] | [[:R]] | [(:C {h: 13})] | (:C {h: 13}) | (:D {h: 10}) |
    Examples:
      | pattern                                                 |
      | MATCH (x)-->(y)((a)-[e]->(b) WHERE a.h > x.h)*(s)-->(u) |
      | MATCH (x)-->(y)((a)-[e]->(b) WHERE a.h > u.h)*(s)-->(u) |

  Scenario: Concatenated path patterns with quantification - Relationship quantified
    And having executed:
      """
      CREATE (:A)-[:R]->(:B)-[:R]->(:C)
      """
    When executing query:
      """
      MATCH (b)-[r]->(c)((f)-[i]->(g))*
      RETURN *
      """
    Then the result should be, in any order:
      | b    | c    | f      | g      | i      | r    |
      | (:A) | (:B) | []     | []     | []     | [:R] |
      | (:B) | (:C) | []     | []     | []     | [:R] |
      | (:A) | (:B) | [(:B)] | [(:C)] | [[:R]] | [:R] |

  Scenario: Concatenated path patterns with quantification - Quantified relationship juxtaposed on quantified pattern
    And having executed:
      """
      CREATE (:A)-[:R]->(:B)-[:R]->(:C)
      """
    When executing query:
      """
      MATCH (p)-[e]->*(q)((f)-[i]->(g))*
      RETURN *
      """
    Then the result should be, in any order:
      | e           | f           | g           | i           | p    | q    |
      | []          | []          | []          | []          | (:A) | (:A) |
      | []          | [(:A)]      | [(:B)]      | [[:R]]      | (:A) | (:A) |
      | []          | [(:A),(:B)] | [(:B),(:C)] | [[:R],[:R]] | (:A) | (:A) |
      | []          | []          | []          | []          | (:B) | (:B) |
      | [[:R]]      | []          | []          | []          | (:A) | (:B) |
      | []          | [(:B)]      | [(:C)]      | [[:R]]      | (:B) | (:B) |
      | [[:R]]      | [(:B)]      | [(:C)]      | [[:R]]      | (:A) | (:B) |
      | []          | []          | []          | []          | (:C) | (:C) |
      | [[:R]]      | []          | []          | []          | (:B) | (:C) |
      | [[:R],[:R]] | []          | []          | []          | (:A) | (:C) |

  Scenario: Concatenated path patterns with quantification - Juxtaposed quantified patterns
    And having executed:
      """
      CREATE (:A)-[:R]->(:B)-[:R]->(:C)
      """
    When executing query:
      """
      MATCH ((f)-[i]->(g))+((k)-[m]->(n))*
      RETURN *
      """
    Then the result should be, in any order:
      | f            | g            | i            | k      | m      | n      |
      | [(:A)]       | [(:B)]       | [[:R]]       | []     | []     | []     |
      | [(:A)]       | [(:B)]       | [[:R]]       | [(:B)] | [[:R]] | [(:C)] |
      | [(:A), (:B)] | [(:B), (:C)] | [[:R], [:R]] | []     | []     | []     |
      | [(:B)]       | [(:C)]       | [[:R]]       | []     | []     | []     |

  Scenario: Quantified path pattern with multiple relationships with several iterations
    And having executed:
      """
      CREATE (a:A)-[:R]->(:B)-[:S]->(:C)-[:T]->(:D)-[:U]->(:E)
                  -[:R]->(:B)-[:S]->(:C)-[:T]->(:D)-[:U]->(:E)
                  -[:R]->(:B)-[:S]->(:C)-[:T]->(:D)-[:U]->(:E)
      """
    When executing query:
      """
      MATCH ((a)-[f]->(b)-[g]->(c)-[h]->(d)-[i]->(e))+
      RETURN *
      """
    Then the result should be, in any order:
      | a                  | b                  | c                  | d                  | e                  | f                  | g                  | h                  | i                  |
      | [(:A)]             | [(:B)]             | [(:C)]             | [(:D)]             | [(:E)]             | [[:R]]             | [[:S]]             | [[:T]]             | [[:U]]             |
      | [(:B)]             | [(:C)]             | [(:D)]             | [(:E)]             | [(:B)]             | [[:S]]             | [[:T]]             | [[:U]]             | [[:R]]             |
      | [(:C)]             | [(:D)]             | [(:E)]             | [(:B)]             | [(:C)]             | [[:T]]             | [[:U]]             | [[:R]]             | [[:S]]             |
      | [(:D)]             | [(:E)]             | [(:B)]             | [(:C)]             | [(:D)]             | [[:U]]             | [[:R]]             | [[:S]]             | [[:T]]             |
      | [(:E)]             | [(:B)]             | [(:C)]             | [(:D)]             | [(:E)]             | [[:R]]             | [[:S]]             | [[:T]]             | [[:U]]             |
      | [(:B)]             | [(:C)]             | [(:D)]             | [(:E)]             | [(:B)]             | [[:S]]             | [[:T]]             | [[:U]]             | [[:R]]             |
      | [(:C)]             | [(:D)]             | [(:E)]             | [(:B)]             | [(:C)]             | [[:T]]             | [[:U]]             | [[:R]]             | [[:S]]             |
      | [(:D)]             | [(:E)]             | [(:B)]             | [(:C)]             | [(:D)]             | [[:U]]             | [[:R]]             | [[:S]]             | [[:T]]             |
      | [(:E)]             | [(:B)]             | [(:C)]             | [(:D)]             | [(:E)]             | [[:R]]             | [[:S]]             | [[:T]]             | [[:U]]             |
      | [(:A), (:E)]       | [(:B), (:B)]       | [(:C), (:C)]       | [(:D), (:D)]       | [(:E), (:E)]       | [[:R], [:R]]       | [[:S], [:S]]       | [[:T], [:T]]       | [[:U], [:U]]       |
      | [(:B), (:B)]       | [(:C), (:C)]       | [(:D), (:D)]       | [(:E), (:E)]       | [(:B), (:B)]       | [[:S], [:S]]       | [[:T], [:T]]       | [[:U], [:U]]       | [[:R], [:R]]       |
      | [(:C), (:C)]       | [(:D), (:D)]       | [(:E), (:E)]       | [(:B), (:B)]       | [(:C), (:C)]       | [[:T], [:T]]       | [[:U], [:U]]       | [[:R], [:R]]       | [[:S], [:S]]       |
      | [(:D), (:D)]       | [(:E), (:E)]       | [(:B), (:B)]       | [(:C), (:C)]       | [(:D), (:D)]       | [[:U], [:U]]       | [[:R], [:R]]       | [[:S], [:S]]       | [[:T], [:T]]       |
      | [(:E), (:E)]       | [(:B), (:B)]       | [(:C), (:C)]       | [(:D), (:D)]       | [(:E), (:E)]       | [[:R], [:R]]       | [[:S], [:S]]       | [[:T], [:T]]       | [[:U], [:U]]       |
      | [(:A), (:E), (:E)] | [(:B), (:B), (:B)] | [(:C), (:C), (:C)] | [(:D), (:D), (:D)] | [(:E), (:E), (:E)] | [[:R], [:R], [:R]] | [[:S], [:S], [:S]] | [[:T], [:T], [:T]] | [[:U], [:U], [:U]] |

  Scenario: Quantified path pattern with same Relationship Type reference and same variable reference on path pattern
    And having executed:
      """
      CREATE (x:A:User)-[:KNOWS]->(x)-[:KNOWS]->(b:B),
      (:D)-[:LIKES]->(b)
      """
    When executing query:
      """
      MATCH (x:A)-[y:KNOWS]->(x:User)((a)-[e:KNOWS]->(b)<-[f:LIKES]-())+
      RETURN *
      """
    Then the result should be, in any order:
      | a | b | e | f | x | y |
      | [(:A:User)] | [(:B)] | [[:KNOWS]] | [[:LIKES]] | (:A:User) | [:KNOWS] |
