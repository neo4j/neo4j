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

Feature: SubqueryAcceptance

  Background:
    Given an empty graph

  Scenario: CALL around single query - using returned var in outer query
    When executing query:
      """
      CALL { RETURN 1 as x } RETURN x
      """
    Then the result should be, in any order:
      | x |
      | 1 |
    And no side effects

  Scenario: Post-processing of a subquery result
    When executing query:
      """
      CALL { UNWIND [1, 2, 3, 4] AS x RETURN x } WITH x WHERE x > 2 RETURN sum(x) AS sum
      """
    Then the result should be, in any order:
      | sum |
      | 7   |
    And no side effects

  Scenario: Executes subquery for all incoming rows
    When executing query:
      """
      UNWIND [1, 2, 3] AS i
      CALL { RETURN 'x' as x } RETURN i, x
      """
    Then the result should be, in any order:
      | i | x   |
      | 1 | 'x' |
      | 2 | 'x' |
      | 3 | 'x' |
    And no side effects

  Scenario: CALLs in sequence
    When executing query:
      """
      CALL { UNWIND [1, 2, 3] AS x RETURN x }
      CALL { UNWIND ['a', 'b'] AS y RETURN y }
      RETURN x, y
      """
    Then the result should be, in any order:
      | x | y   |
      | 1 | 'a' |
      | 1 | 'b' |
      | 2 | 'a' |
      | 2 | 'b' |
      | 3 | 'a' |
      | 3 | 'b' |
    And no side effects

  Scenario: Simple nested subqueries
    When executing query:
      """
      CALL { CALL { CALL { RETURN 1 as x } RETURN x } RETURN x } RETURN x
      """
    Then the result should be, in any order:
      | x |
      | 1 |
    And no side effects

  Scenario: Nested subqueries
    And having executed:
    """
    CREATE (:A), (:B), (:C)
    """
    When executing query:
      """
      CALL {
        CALL {
          CALL {
            MATCH (a:A) RETURN a
          }
          MATCH (b:B) RETURN a, b
        }
        MATCH (c:C) RETURN a, b, c
      }
      RETURN a, b, c
      """
    Then the result should be, in any order:
      | a    | b    | c    |
      | (:A) | (:B) | (:C) |
    And no side effects

  Scenario: CALL around union query - using returned var in outer query
    When executing query:
      """
      CALL { RETURN 1 as x UNION RETURN 2 as x } RETURN x
      """
    Then the result should be, in any order:
      | x |
      | 1 |
      | 2 |
    And no side effects

  Scenario: CALL around union query with different return column orders - using returned vars in outer query
    When executing query:
      """
      CALL { RETURN 1 as x, 2 AS y UNION RETURN 3 AS y, 2 as x } RETURN x, y
      """
    Then the result should be, in any order:
      | x | y |
      | 1 | 2 |
      | 2 | 3 |
    And no side effects

  Scenario: Aggregating top and bottom results
    And having executed:
    """
    UNWIND range(1, 10) as p
    CREATE ({prop: p})
    """
    When executing query:
      """
      CALL {
        MATCH (x) WHERE x.prop > 0 RETURN x ORDER BY x.prop LIMIT 3
        UNION
        MATCH (x) WHERE x.prop > 0 RETURN x ORDER BY x.prop DESC LIMIT 3
      }
      RETURN sum(x.prop) AS sum
      """
    Then the result should be, in any order:
      | sum |
      | 33  |
    And no side effects

  Scenario: Should treat variables with the same name but different scopes correctly
    And having executed:
    """
    CREATE (), ()
    """
    When executing query:
      """
      MATCH (x)
      CALL {
        MATCH (x) RETURN x AS y
      }
      RETURN count(*) AS count
      """
    Then the result should be, in any order:
      | count |
      | 4     |
    And no side effects

  Scenario: Should work with preceding MATCH and aggregation
    And having executed:
    """
    CREATE (:Person {age: 20, name: 'Alice'}),
           (:Person {age: 27, name: 'Bob'}),
           (:Person {age: 65, name: 'Charlie'}),
           (:Person {age: 30, name: 'Dora'})
    """
    When executing query:
      """
      MATCH (p:Person)
      CALL {
        UNWIND range(1, 5) AS i
        RETURN count(i) AS numberOfClones
      }
      RETURN p.name, numberOfClones
      """
    Then the result should be, in any order:
      | p.name    | numberOfClones |
      | 'Alice'   | 5              |
      | 'Bob'     | 5              |
      | 'Charlie' | 5              |
      | 'Dora'    | 5              |
    And no side effects

  Scenario: Should allow importing variables into a subquery
    And having executed:
    """
    CREATE (:Person {age: 20, name: 'Alice'}),
           (:Person {age: 27, name: 'Bob'}),
           (:Person {age: 65, name: 'Charlie'}),
           (:Person {age: 30, name: 'Dora'})
    """
    When executing query:
      """
      MATCH (p:Person)
      CALL {
        WITH p
        RETURN p.name AS innerName
      }
      RETURN innerName
      """
    Then the result should be, in any order:
      | innerName |
      | 'Alice'   |
      | 'Bob'     |
      | 'Charlie' |
      | 'Dora'    |
    And no side effects

  Scenario: Should not allow to use unimported variables in a subquery
    When executing query:
      """
      MATCH (a), (b)
      CALL {
        WITH a
        RETURN b AS c
      }
      RETURN c
      """
    Then a SyntaxError should be raised at compile time: UndefinedVariable

  Scenario: Should allow to remove imported variables from subquery scope
    When executing query:
      """
      MATCH (a)
      CALL {
        WITH a
        WITH 1 AS b
        RETURN a AS c
      }
      RETURN c
      """
    Then a SyntaxError should be raised at compile time: UndefinedVariable

  Scenario: Aggregating top and bottom results from correlated subquery
    And having executed:
    """
    CREATE (:Config {threshold: 2})
    WITH *
    UNWIND range(1, 10) as p
    CREATE (:Node {prop: p})
    """
    When executing query:
      """
      MATCH (c:Config)
      CALL {
        WITH c MATCH (x:Node) WHERE x.prop > c.threshold RETURN x ORDER BY x.prop LIMIT 3
        UNION
        WITH c MATCH (x:Node) WHERE x.prop > c.threshold RETURN x ORDER BY x.prop DESC LIMIT 3
      }
      RETURN sum(x.prop) AS sum
      """
    Then the result should be, in any order:
      | sum |
      | 39  |
    And no side effects

  Scenario: Aggregation on imported variables
    When executing query:
      """
      UNWIND [0, 1, 2] AS x
      CALL {
        WITH x
        RETURN max(x) AS xMax
      }
      RETURN x, xMax
      """
    Then the result should be, in any order:
      | x | xMax |
      | 0 | 0    |
      | 1 | 1    |
      | 2 | 2    |
    And no side effects

  Scenario: Aggregating top and bottom results within correlated subquery
    And having executed:
    """
    CREATE (:Config {threshold: 2})
    WITH *
    UNWIND range(1, 10) as p
    CREATE (:Node {prop: p})
    """
    When executing query:
      """
      MATCH (c:Config)
      CALL {
        WITH c MATCH (x:Node) WHERE x.prop > c.threshold WITH x.prop AS metric ORDER BY metric LIMIT 3 RETURN sum(metric) AS y
        UNION
        WITH c MATCH (x:Node) WHERE x.prop > c.threshold WITH x.prop AS metric ORDER BY metric DESC LIMIT 3 RETURN sum(metric) AS y
      }
      RETURN sum(y) AS sum
      """
    Then the result should be, in any order:
      | sum |
      | 39  |
    And no side effects

  Scenario: Grouping and aggregating within correlated subquery
    And having executed:
    """
    CREATE (:Config {threshold: 2})
    WITH *
    UNWIND range(1, 10) as p
    CREATE (:Node {prop: p, category: p % 2})
    """
    When executing query:
      """
      MATCH (c:Config)
      CALL {
          WITH c MATCH (x:Node)
          WHERE x.prop > c.threshold
          WITH x.prop AS metric, x.category AS cat
          ORDER BY metric LIMIT 3
          RETURN cat, sum(metric) AS y
        UNION
          WITH c MATCH (x:Node)
          WHERE x.prop > c.threshold
          WITH x.prop AS metric, x.category AS cat
          ORDER BY metric DESC LIMIT 3
          RETURN cat, sum(metric) AS y
      }
      RETURN cat, sum(y) AS sum
      """
    Then the result should be, in any order:
      | cat | sum |
      | 0   | 22  |
      | 1   | 17  |
    And no side effects

  Scenario: Sorting in a subquery
    When executing query:
      """
       WITH 1 AS x
       CALL {
         WITH x
         WITH count(*) AS y
         WITH y AS z
         RETURN z ORDER BY z
       }
       RETURN z
      """
    Then the result should be, in any order:
      | z |
      | 1 |
    And no side effects

  Scenario: Map projections in uncorrelated single subquery are OK
    When executing query:
      """
      CALL {
        MATCH (n)
        RETURN n {.prop, .foo}
      }
      RETURN n
      """
    Then the result should be, in any order:
      | n |
    And no side effects

  Scenario: Map projections in uncorrelated union subquery are OK
    When executing query:
      """
      CALL {
        MATCH (n)
        RETURN n {.prop, .foo}
        UNION
        MATCH (n)
        RETURN n {.prop, .foo}
      }
      RETURN n
      """
    Then the result should be, in any order:
      | n |
    And no side effects

  Scenario: Map projections in correlated single subquery are OK
    When executing query:
      """
      MATCH (n)
      CALL {
        WITH n
        MATCH (n)--(m)
        RETURN m {.prop, .foo}
      }
      RETURN m
      """
    Then the result should be, in any order:
      | m |
    And no side effects

  Scenario: Map projections in correlated union subquery are OK
    When executing query:
      """
      MATCH (n)
      CALL {
        WITH n
        MATCH (n)--(m)
        RETURN m {.prop, .foo}
        UNION
        WITH n
        MATCH (n)--(m)
        RETURN m {.prop, .foo}
      }
      RETURN m
      """
    Then the result should be, in any order:
      | m |
    And no side effects

    Scenario: Importing path expressions into subqueries
      And having executed:
        """
        CREATE (:N), (:N)
        """
      When executing query:
        """
        MATCH p = (n)
        CALL {
          WITH p
          RETURN length(p) AS l
        }
        RETURN p, l
        """
      Then the result should be, in any order:
        | p      | l |
        | <(:N)> | 0 |
        | <(:N)> | 0 |
      And no side effects

  Scenario: Importing path expressions into union subqueries
    And having executed:
      """
      CREATE (:N), (:N)
      """
    When executing query:
      """
      MATCH p = (n)
      CALL {
        WITH p
        RETURN length(p) AS l
        UNION ALL
        WITH p
        RETURN length(p) AS l
      }
      RETURN p, l
      """
    Then the result should be, in any order:
      | p      | l |
      | <(:N)> | 0 |
      | <(:N)> | 0 |
      | <(:N)> | 0 |
      | <(:N)> | 0 |
    And no side effects

  Scenario: Side effects in uncorrelated subquery
    And having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    When executing query:
      """
      MATCH (x)
      CALL {
        CREATE (y:Label)
        RETURN *
      }
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 3     |
    And the side effects should be:
      | +nodes  | 3 |

  Scenario: Side effects in uncorrelated subquery with FINISH
    And having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    When executing query:
      """
      MATCH (x)
      CALL {
        CREATE (y:Label)
        FINISH
      }
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 3     |
    And the side effects should be:
      | +nodes  | 3 |

  Scenario: Side effects in order dependant subquery
    And having executed:
      """
      CREATE ({value: 3})
      """
    When executing query:
      """
      UNWIND [1, 2, 3] AS i
      WITH i ORDER BY i DESC
      CALL {
        WITH i
        MATCH (n {value: i})
        CREATE (m {value: i - 1})
        RETURN m
      }
      RETURN count(*) as count
      """
    Then the result should be, in order:
      | count |
      | 3     |
    And the side effects should be:
      | +nodes       | 3 |
      | +properties  | 3 |

  Scenario: Side effects in order dependant subquery with FINISH
    And having executed:
      """
      CREATE ({value: 3})
      """
    When executing query:
      """
      UNWIND [1, 2, 3] AS i
      WITH i ORDER BY i DESC
      CALL {
        WITH i
        MATCH (n {value: i})
        CREATE (m {value: i - 1})
        FINISH
      }
      RETURN count(*) as count
      """
    Then the result should be, in order:
      | count |
      | 3     |
    And the side effects should be:
      | +nodes       | 3 |
      | +properties  | 3 |

  Scenario: Side effects in subquery with update that depending on previous updates
    And having executed:
      """
      CREATE (:Number {value: 19})
      """
    When executing query:
      """
      WITH 100 AS maxIterations
      UNWIND range(1, maxIterations) AS i
      CALL {
        MATCH (n:Number) WHERE n.value <> 1
        WITH CASE n.value % 2
          WHEN 0 THEN n.value / 2
          WHEN 1 THEN 3*n.value + 1
        END AS newVal, n AS n
        SET n.value = newVal
        RETURN newVal
      }
      RETURN i, newVal
      """
    Then the result should be, in order:
      | i  | newVal  |
      | 1  | 58      |
      | 2  | 29      |
      | 3  | 88      |
      | 4  | 44      |
      | 5  | 22      |
      | 6  | 11      |
      | 7  | 34      |
      | 8  | 17      |
      | 9  | 52      |
      | 10 | 26      |
      | 11 | 13      |
      | 12 | 40      |
      | 13 | 20      |
      | 14 | 10      |
      | 15 | 5       |
      | 16 | 16      |
      | 17 | 8       |
      | 18 | 4       |
      | 19 | 2       |
      | 20 | 1       |
    And the side effects should be:
      | -properties  | 1 |
      | +properties  | 1 |

  Scenario: Uncorrelated unit subquery
    And having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    When executing query:
      """
      MATCH (x)
      CALL {
        CREATE (:Label)
      }
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 3     |
    And the side effects should be:
      | +nodes  | 3 |

  Scenario: Uncorrelated unit subquery with FINISH
    And having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    When executing query:
      """
      MATCH (x)
      CALL {
        CREATE (:Label)
        FINISH
      }
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 3     |
    And the side effects should be:
      | +nodes  | 3 |

  Scenario: Correlated unit subquery
    And having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    When executing query:
      """
      MATCH (x)
      CALL {
        WITH x
        SET x.prop = 1
      }
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 3     |
    And the side effects should be:
      | +properties    | 3 |

  Scenario: Correlated unit subquery with FINISH
    And having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    When executing query:
      """
      MATCH (x)
      CALL {
        WITH x
        SET x.prop = 1
        FINISH
      }
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 3     |
    And the side effects should be:
      | +properties    | 3 |

  Scenario: Uncorrelated unit subquery with shadowed variable
    And having executed:
      """
      CREATE (:Label {prop: 1}), (:Label {prop: 2}), (:Label {prop: 3})
      """
    When executing query:
      """
      MATCH (n)
      CALL {
        WITH 1 AS n
        CREATE (x: Foo)
        SET x.prop = n
      }
      RETURN n.prop
      """
    Then the result should be, in any order:
      | n.prop |
      | 1      |
      | 2      |
      | 3      |
    And the side effects should be:
      | +properties | 3 |
      | +nodes      | 3 |
      | +labels     | 1 |

  Scenario: Correlated union unit subquery
    When having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    And executing query:
      """
      MATCH (x)
      CALL {
        WITH x
        SET x.prop = 1
        UNION
        CREATE (y:A)
      }
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 3     |
    And the side effects should be:
      | +properties | 3 |
      | +nodes      | 3 |
      | +labels     | 1 |

  Scenario: Uncorrelated unit subquery with increasing cardinality
    When having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    And executing query:
      """
      MATCH (n)
      CALL {
        UNWIND [1, 2] AS i
        CREATE (x: Foo)
      }
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 3     |
    And the side effects should be:
      | +nodes      | 6 |
      | +labels     | 1 |

  Scenario: Uncorrelated unit subquery with increasing cardinality with FINISH
    When having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    And executing query:
      """
      MATCH (n)
      CALL {
        UNWIND [1, 2] AS i
        CREATE (x: Foo)
        FINISH
      }
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 3     |
    And the side effects should be:
      | +nodes      | 6 |
      | +labels     | 1 |

  Scenario: Unit subquery under limit
    And having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    When executing query:
      """
      MATCH (x)
      CALL {
        CREATE (:Label)
      }
      RETURN x LIMIT 0
      """
    Then the result should be, in order:
      | x |
    And the side effects should be:
      | +nodes  | 3 |

  Scenario: Unit subquery under FINISH
    And having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    When executing query:
      """
      MATCH (x)
      CALL {
        CREATE (:Label)
      }
      FINISH
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 3 |

  Scenario: Embedded nested unit subquery call
    And having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    When executing query:
      """
      MATCH (n)
      CALL {
        CALL {
          CREATE (x: Foo)
        }
      }
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 3     |
    And the side effects should be:
      | +nodes  | 3 |
      | +labels | 1 |

  Scenario: Embedded nested unit subquery call with FINISH
    And having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    When executing query:
      """
      MATCH (n)
      CALL {
        CALL {
          CREATE (x: Foo)
          FINISH
        }
      }
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 3     |
    And the side effects should be:
      | +nodes  | 3 |
      | +labels | 1 |

  Scenario: Embedded nested unit subquery call with FINISH on all nested levels
    And having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    When executing query:
      """
      MATCH (n)
      CALL {
        CALL {
          CREATE (x: Foo)
          FINISH
        }
        FINISH
      }
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 3     |
    And the side effects should be:
      | +nodes  | 3 |
      | +labels | 1 |

  Scenario: Ending unit subquery call
    And having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    When executing query:
      """
      MATCH (n)
      CALL {
        CREATE (x: Foo)
      }
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 3 |
      | +labels | 1 |

  Scenario: Ending unit subquery call with FINISH
    And having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    When executing query:
      """
      MATCH (n)
      CALL {
        CREATE (x: Foo)
        FINISH
      }
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 3 |
      | +labels | 1 |

  Scenario: Ending nested unit subquery call
    And having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    When executing query:
      """
      MATCH (n)
      CALL {
        CALL {
          CREATE (x: Foo)
        }
      }
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 3 |
      | +labels | 1 |

  Scenario: Ending nested unit subquery call with FINISH
    And having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    When executing query:
      """
      MATCH (n)
      CALL {
        CALL {
          CREATE (x: Foo)
          FINISH
        }
      }
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 3 |
      | +labels | 1 |

  Scenario: Ending union unit subquery call
    And having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    When executing query:
      """
      MATCH (n)
      CALL {
        CREATE (x: Foo)
          UNION
        CREATE (x: Foo)
      }
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 6 |
      | +labels | 1 |

  Scenario: Ending union unit subquery call with FINISH
    And having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    When executing query:
      """
      MATCH (n)
      CALL {
        CREATE (x: Foo)
        FINISH
          UNION
        CREATE (x: Foo)
        FINISH
      }
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 6 |
      | +labels | 1 |

  Scenario: Side effects from unit subquery are visible after subquery
    When having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    And executing query:
      """
      MATCH (x)
      CALL {
        WITH x
        SET x.prop = 1
      }
      RETURN x.prop AS prop
      """
    Then the result should be, in any order:
      | prop |
      | 1    |
      | 1    |
      | 1    |
    And the side effects should be:
      | +properties | 3 |

  Scenario: Side effects from unit subquery are visible after subquery, when previously read
    When having executed:
      """
      CREATE (:Label {prop: 1}), (:Label {prop: 1}), (:Label {prop: 1})
      """
    And executing query:
      """
      MATCH (x)
      WITH x, x.prop AS prop1
      CALL {
        WITH x
        SET x.prop = 2
      }
      RETURN prop1, x.prop AS prop2
      """
    Then the result should be, in any order:
      | prop1 | prop2 |
      | 1     | 2     |
      | 1     | 2     |
      | 1     | 2     |
    And the side effects should be:
      | -properties | 3 |
      | +properties | 3 |

  Scenario: Return items in uncorrelated single subquery must be aliased
    Given any graph
    When executing query:
      """
      CALL {
        RETURN 5
      }
      RETURN `5` AS five
      """
    Then a SyntaxError should be raised at compile time: NoExpressionAlias

  Scenario: Return items in uncorrelated union subquery must be aliased
    Given any graph
    When executing query:
      """
      CALL {
        RETURN 5 UNION RETURN 5
      }
      RETURN `5` AS five
      """
    Then a SyntaxError should be raised at compile time: NoExpressionAlias

  Scenario: Return items in correlated single subquery must be aliased
    Given any graph
    When executing query:
      """
      MATCH (n)
      CALL {
        WITH n
        RETURN 5 + 5
      }
      RETURN `5 + 5` AS plus
      """
    Then a SyntaxError should be raised at compile time: NoExpressionAlias

  Scenario: Return items in correlated union subquery must be aliased
    Given any graph
    When executing query:
      """
      MATCH (n)
      CALL {
        WITH n
        RETURN 5 + 5
        UNION
        WITH n
        RETURN 5 + 5
      }
      RETURN `5 + 5` AS plus
      """
    Then a SyntaxError should be raised at compile time: NoExpressionAlias

  Scenario: Map projections in uncorrelated single subquery are OK
    Given an empty graph
    When executing query:
      """
      CALL {
        MATCH (n)
        RETURN n {.prop, .foo}
      }
      RETURN n
      """
    Then the result should be, in any order:
      | n |
    And no side effects

  Scenario: Map projections in uncorrelated union subquery are OK
    Given an empty graph
    When executing query:
      """
      CALL {
        MATCH (n)
        RETURN n {.prop, .foo}
        UNION
        MATCH (n)
        RETURN n {.prop, .foo}
      }
      RETURN n
      """
    Then the result should be, in any order:
      | n |
    And no side effects

  Scenario: Map projections in correlated single subquery are OK
    Given an empty graph
    When executing query:
      """
      MATCH (n)
      CALL {
        WITH n
        MATCH (n)--(m)
        RETURN m {.prop, .foo}
      }
      RETURN m
      """
    Then the result should be, in any order:
      | m |
    And no side effects

  Scenario: Map projections in correlated union subquery are OK
    Given an empty graph
    When executing query:
      """
      MATCH (n)
      CALL {
        WITH n
        MATCH (n)--(m)
        RETURN m {.prop, .foo}
        UNION
        WITH n
        MATCH (n)--(m)
        RETURN m {.prop, .foo}
      }
      RETURN m
      """
    Then the result should be, in any order:
      | m |
    And no side effects

  Scenario: Accessing variables from outer query
    When executing query:
      """
      UNWIND [1,2] AS x
      WITH 2*x AS xx, x
      CALL {
        WITH x
        UNWIND [10,20,30] AS y
        RETURN x+y AS xy
      }
      RETURN x, xx, xy
      """
    Then the result should be, in order:
      | x | xx | xy |
      | 1 |  2 | 11 |
      | 1 |  2 | 21 |
      | 1 |  2 | 31 |
      | 2 |  4 | 12 |
      | 2 |  4 | 22 |
      | 2 |  4 | 32 |
    And no side effects

  Scenario: A subquery with FINISH
    When executing query:
      """
      CALL {
        FINISH
      }
      RETURN 1 AS x
      """
    Then the result should be, in order:
      | x |
      | 1 |
    And no side effects

  Scenario: Nested subqueries with FINISH
    When executing query:
      """
      CALL {
        CALL {
          FINISH
        }
      }
      RETURN 1 AS x
      """
    Then the result should be, in order:
      | x |
      | 1 |
    And no side effects

  Scenario: Nested subqueries with FINISH on all levels
    When executing query:
      """
      CALL {
        CALL {
          FINISH
        }
        FINISH
      }
      FINISH
      """
    Then the result should be empty
    And no side effects

  Scenario: An uncorrelated subquery with FINISH does not change the cardinality of the outer query with cardinality equal 0
    When executing query:
      """
      UNWIND [] AS x
      CALL {
        WITH toInteger(rand()*101) AS nInner
        UNWIND [0] + range(1, nInner) AS y
        FINISH
      }
      RETURN x AS invariant
      """
    Then the result should be, in order:
      | invariant |
    And no side effects

  Scenario: An uncorrelated subquery with FINISH does not change the cardinality of the outer query with cardinality greater 0
    When executing query:
      """
      WITH toInteger(rand()*101) AS n
      UNWIND [0] + range(1, n) AS x
      CALL {
        WITH toInteger(rand()*101) AS nInner
        UNWIND range(1, nInner) AS y
        FINISH
      }
      WITH n + 1 AS n, COUNT(*) AS card
      RETURN n = card AS invariant
      """
    Then the result should be, in order:
      | invariant |
      | true      |
    And no side effects

  Scenario: A correlated subquery with FINISH does not change the cardinality of the outer query with cardinality equal 0
    When executing query:
      """
      WITH toInteger(rand()*101) AS n
      UNWIND [] AS x
      CALL {
        WITH n
        WITH toInteger(n * (rand() + rand())) AS nInner
        UNWIND [0] + range(1, nInner) AS y
        FINISH
      }
      RETURN x AS invariant
      """
    Then the result should be, in order:
      | invariant |
    And no side effects

  Scenario: A correlated subquery with FINISH does not change the cardinality of the outer query with cardinality greater 0
    When executing query:
      """
      WITH toInteger(rand()*101) AS n
      UNWIND [0] + range(1, n) AS x
      CALL {
        WITH n
        WITH toInteger(n * (rand() + rand())) AS nInner
        UNWIND range(1, nInner) AS y
        FINISH
      }
      WITH n + 1 AS n, COUNT(*) AS card
      RETURN n = card AS invariant
      """
    Then the result should be, in order:
      | invariant |
      | true      |
    And no side effects


  Scenario: Each execution of a CALL subquery can observe changes from previous executions, projections should capture the state of the current execution
    When having executed:
      """
      CREATE (:Counter {count: 0})
      """
    And executing query:
      """
      UNWIND [0, 1, 2] AS x
      CALL {
        MATCH (n:Counter)
          SET n.count = n.count + 1
        RETURN n.count AS innerCount
      }
      WITH innerCount
      MATCH (n:Counter)
      RETURN
        innerCount,
        n.count AS totalCount
    """
    Then the result should be, in any order:
      | innerCount | totalCount |
      | 1          | 3          |
      | 2          | 3          |
      | 3          | 3          |
    And the side effects should be:
      | -properties | 1 |
      | +properties | 1 |

  # Subquery call with importing variable scope
  # Positive Tests

  Scenario: Empty importing clause
    When executing query:
      """
      WITH 1 AS a
      CALL () {
        RETURN 1 AS res
      }
      RETURN res
      """
    Then the result should be, in any order:
      | res |
      | 1   |
    And no side effects

  Scenario: Single reference in importing clause
    When executing query:
      """
      WITH 1 AS a
      CALL (a) {
        RETURN a AS res
      }
      RETURN res
      """
    Then the result should be, in any order:
      | res |
      | 1   |
    And no side effects

  Scenario: Multiple references in importing clause
    When executing query:
      """
      WITH 1 AS a, 1 AS b
      CALL (a, b) {
        RETURN a + b AS res
      }
      RETURN res
      """
    Then the result should be, in any order:
      | res |
      | 2   |
    And no side effects

  Scenario: Wildcard in importing clause
    When executing query:
      """
      WITH 1 AS a
      CALL (*) {
        RETURN a AS res
      }
      RETURN res
      """
    Then the result should be, in any order:
      | res |
      | 1   |
    And no side effects


  Scenario: Not possible to delist imported variable
    When executing query:
      """
      WITH 1 AS a
      CALL (a) {
        WITH a
        WITH 1 AS b
        RETURN a + b AS c
      }
      RETURN a, c
      """
    Then the result should be, in any order:
      | a | c |
      | 1 | 2 |
    And no side effects

  Scenario: Nested subquery calls with importing clauses
    When executing query:
      """
      WITH 1 AS a
      CALL (a) {
        WITH 2 AS b
        CALL (a, b) {
          WITH 3 AS c
          CALL (a, b, c) {
            WITH 4 AS d
            RETURN a + b + c + d AS e
          }
          RETURN e
        }
        RETURN e
      }
      RETURN a, e
      """
    Then the result should be, in any order:
      | a | e |
      | 1 | 10 |
    And no side effects

  Scenario: Scope clause: UNION subquery using scope clause
    When executing query:
      """
      WITH 1 AS a
      CALL (a) {
        RETURN a AS ignored
        UNION
        RETURN a AS ignored
      }
      RETURN a
      """
    Then the result should be, in any order:
      | a |
      | 1 |
    And no side effects

  Scenario: Scope clause with multiple UNIONs in subquery
    When executing query:
      """
      WITH 1 AS a
      CALL (a) {
        RETURN a AS b
        UNION ALL
        RETURN a + 1 AS b
        UNION ALL
        RETURN a + 2 AS b
        UNION ALL
        RETURN a + 2 AS b
      }
      RETURN a, b
      """
    Then the result should be, in any order:
      | a | b |
      | 1 | 1 |
      | 1 | 2 |
      | 1 | 3 |
      | 1 | 3 |
    And no side effects

  Scenario: Scope clause importing all with multiple UNIONs in subquery
    When executing query:
      """
      WITH 1 AS a
      CALL (*) {
        RETURN a AS b
        UNION ALL
        RETURN a + 1 AS b
        UNION ALL
        RETURN a + 2 AS b
        UNION ALL
        RETURN a + 2 AS b
      }
      RETURN a, b
      """
    Then the result should be, in any order:
      | a | b |
      | 1 | 1 |
      | 1 | 2 |
      | 1 | 3 |
      | 1 | 3 |
    And no side effects

  Scenario: Scope clause able to redefine non imported variables
    When executing query:
      """
      WITH 1 AS a
      CALL () {
        WITH 2 AS a
        RETURN a AS b
      }
      RETURN a, b
      """
    Then the result should be, in any order:
      | a | b |
      | 1 | 2 |
    And no side effects

  Scenario: Aggregation on imported variables
    When executing query:
      """
      UNWIND [0, 1, 2] AS x
      CALL(x) {
        RETURN max(x) AS xMax
      }
      RETURN x, xMax
      """
    Then the result should be, in any order:
      | x | xMax |
      | 0 | 0    |
      | 1 | 1    |
      | 2 | 2    |
    And no side effects

  # Negative tests

  Scenario: Scope clause disables IMPORTING WITH
    When executing query:
      """
      WITH 1 AS a, 2 AS b
      CALL (a) {
        WITH b
        RETURN a + b AS c
      }
      RETURN c
      """
    Then a SyntaxError should be raised at compile time: UndefinedVariable

  Scenario: Scope clause contains illegal variable reference (not in scope)
    When executing query:
      """
      CALL (a) {
        RETURN 1 AS res
      }
      RETURN res
      """
    Then a SyntaxError should be raised at compile time: UndefinedVariable

  Scenario: Scope clause: unimported outer-scope variable cannot be referenced in the subquery
    When executing query:
      """
      WITH 1 AS a
      CALL () {
        RETURN a AS ignored
      }
      RETURN a
      """
    Then a SyntaxError should be raised at compile time: UndefinedVariable

  Scenario: Side effects in uncorrelated subquery with FINISH and Scope Clause
    And having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    When executing query:
      """
      MATCH (x)
      CALL () {
        CREATE (y:Label)
        FINISH
      }
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 3     |
    And the side effects should be:
      | +nodes  | 3 |

  Scenario: Side effects in order dependant subquery with FINISH and Scope Clause
    And having executed:
      """
      CREATE ({value: 3})
      """
    When executing query:
      """
      UNWIND [1, 2, 3] AS i
      WITH i ORDER BY i DESC
      CALL (i) {
        MATCH (n {value: i})
        CREATE (m {value: i - 1})
        FINISH
      }
      RETURN count(*) as count
      """
    Then the result should be, in order:
      | count |
      | 3     |
    And the side effects should be:
      | +nodes       | 3 |
      | +properties  | 3 |

  Scenario: Correlated unit subquery with FINISH and Scope Clause
    And having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    When executing query:
      """
      MATCH (x)
      CALL (x) {
        SET x.prop = 1
        FINISH
      }
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 3     |
    And the side effects should be:
      | +properties    | 3 |

  Scenario: Embedded nested unit subquery call with FINISH with Scope Clause
    And having executed:
      """
      CREATE (:Label), (:Label), (:Label)
      """
    When executing query:
      """
      MATCH (n)
      CALL () {
        CALL () {
          CREATE (x: Foo)
          FINISH
        }
      }
      RETURN count(*) AS count
      """
    Then the result should be, in order:
      | count |
      | 3     |
    And the side effects should be:
      | +nodes  | 3 |
      | +labels | 1 |

  Scenario: A correlated subquery with FINISH does not change the cardinality of the outer query with cardinality equal 0
    When executing query:
      """
      WITH toInteger(rand()*101) AS n
      UNWIND [] AS x
      CALL (n) {
        WITH toInteger(n * (rand() + rand())) AS nInner
        UNWIND [0] + range(1, nInner) AS y
        FINISH
      }
      RETURN x AS invariant
      """
    Then the result should be, in order:
      | invariant |
    And no side effects

  Scenario: A correlated subquery with FINISH does not change the cardinality of the outer query with cardinality greater 0
    When executing query:
      """
      WITH toInteger(rand()*101) AS n
      UNWIND [0] + range(1, n) AS x
      CALL (n) {
        WITH toInteger(n * (rand() + rand())) AS nInner
        UNWIND range(1, nInner) AS y
        FINISH
      }
      WITH n + 1 AS n, COUNT(*) AS card
      RETURN n = card AS invariant
      """
    Then the result should be, in order:
      | invariant |
      | true      |
    And no side effects

  Scenario: Should allow importing variables into a subquery with Scope Clause
    And having executed:
    """
    CREATE (:Person {age: 20, name: 'Alice'}),
           (:Person {age: 27, name: 'Bob'}),
           (:Person {age: 65, name: 'Charlie'}),
           (:Person {age: 30, name: 'Dora'})
    """
    When executing query:
      """
      MATCH (p:Person)
      CALL (p) {
        RETURN p.name AS innerName
      }
      RETURN innerName
      """
    Then the result should be, in any order:
      | innerName |
      | 'Alice'   |
      | 'Bob'     |
      | 'Charlie' |
      | 'Dora'    |
    And no side effects

  Scenario: Grouping and aggregating within correlated subquery with Scope Clause
    And having executed:
    """
    CREATE (:Config {threshold: 2})
    WITH *
    UNWIND range(1, 10) as p
    CREATE (:Node {prop: p, category: p % 2})
    """
    When executing query:
      """
      MATCH (c:Config)
      CALL (c) {
          MATCH (x:Node)
          WHERE x.prop > c.threshold
          WITH x.prop AS metric, x.category AS cat
          ORDER BY metric LIMIT 3
          RETURN cat, sum(metric) AS y
        UNION
          MATCH (x:Node)
          WHERE x.prop > c.threshold
          WITH x.prop AS metric, x.category AS cat
          ORDER BY metric DESC LIMIT 3
          RETURN cat, sum(metric) AS y
      }
      RETURN cat, sum(y) AS sum
      """
    Then the result should be, in any order:
      | cat | sum |
      | 0   | 22  |
      | 1   | 17  |
    And no side effects

  Scenario: Referenced variables are the same
    And having executed:
      """
      CREATE (n1 {node: 1}), (n2 {node: 2}), (n3 {node: 3}), (n4 {node: 4})
      CREATE (n1)-[:X {rel: 1}]->(n2),
             (n3)-[:X {rel: 2}]->(n4)
      """
    When executing query:
      """
      MATCH (n)
      CALL (n) {
        MATCH (n)-[r]->(m)
        RETURN m AS w
      }
      MATCH (n)-[r]->(m)
      RETURN m, w
      """
    Then the result should be, in any order:
      | m           | w           |
      | ({node: 2}) | ({node: 2}) |
      | ({node: 4}) | ({node: 4}) |
    And no side effects
