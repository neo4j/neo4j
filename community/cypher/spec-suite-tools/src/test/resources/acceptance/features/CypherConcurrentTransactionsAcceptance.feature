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

Feature: CypherConcurrentTransactionsAcceptance

  Background:
    Given an empty graph

  Scenario: Uncorrelated transactional unit subquery with single transaction
    When executing query:
      """
      CALL {
        CREATE (:A)
      } IN CONCURRENT TRANSACTIONS OF 1 ROW
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 1 |
      | +labels | 1 |

  Scenario: Uncorrelated transactional unit subquery

  Subquery is executed once per incoming row.

    When executing query:
      """
      UNWIND [1, 2, 3] AS i
      CALL {
        CREATE (:A)
      } IN CONCURRENT TRANSACTIONS OF 1 ROW
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 3 |
      | +labels | 1 |

  Scenario: Uncorrelated transactional unit subquery in batches, batchSize aligned

  Subquery is executed once per incoming row.
  Transaction is opened for each batch.

    When executing query:
      """
      UNWIND range(1, 10) AS i
      CALL {
        CREATE (:A)
      } IN CONCURRENT TRANSACTIONS OF 2 ROWS
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 10 |
      | +labels | 1  |

  Scenario: Uncorrelated transactional unit subquery in batches, batchSize unaligned

  Subquery is executed once per incoming row.
  Transaction is opened for each batch.

    When executing query:
      """
      UNWIND range(1, 10) AS i
      CALL {
        CREATE (:A)
      } IN CONCURRENT TRANSACTIONS OF 3 ROWS
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 10 |
      | +labels | 1  |

  Scenario: Uncorrelated transactional unit subquery in batches, batchSize == input size

  Subquery is executed once per incoming row.
  Transaction is opened for each batch.

    When executing query:
      """
      UNWIND range(1, 10) AS i
      CALL {
        CREATE (:A)
      } IN CONCURRENT TRANSACTIONS OF 10 ROWS
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 10 |
      | +labels | 1  |

  Scenario: Uncorrelated transactional unit subquery in batches, batchSize > input size

  Subquery is executed once per incoming row.
  Transaction is opened for each batch.

    When executing query:
      """
      UNWIND range(1, 10) AS i
      CALL {
        CREATE (:A)
      } IN CONCURRENT TRANSACTIONS OF 100 ROWS
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 10 |
      | +labels | 1  |

  Scenario: Uncorrelated transactional unit subquery  by LIMIT should execute all side effects

  Subquery is executed once per incoming row.
  Transaction is opened for each batch.

    When executing query:
      """
      UNWIND range(1, 10) AS i
      CALL {
        CREATE (:A)
      } IN CONCURRENT TRANSACTIONS OF 1 ROW
      RETURN i LIMIT 1
      """
    Then the side effects should be:
      | +nodes  | 10 |
      | +labels | 1  |

  Scenario: Uncorrelated transactional unit subquery in batches followed by LIMIT should execute all side effects

  Subquery is executed once per incoming row.
  Transaction is opened for each batch.

    When executing query:
      """
      UNWIND range(1, 10) AS i
      CALL {
        CREATE (:A)
      } IN CONCURRENT TRANSACTIONS OF 5 ROWS
      RETURN i LIMIT 1
      """
    Then the side effects should be:
      | +nodes  | 10 |
      | +labels | 1  |

  Scenario: Correlated transactional unit subquery

  Subquery is executed once per incoming row.

    When executing query:
      """
      UNWIND [1, 2, 3] AS i
      CALL {
        WITH i
        CREATE (:A {i: i})
      } IN CONCURRENT TRANSACTIONS OF 1 ROW
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 3 |
      | +properties | 3 |
      | +labels     | 1 |

  Scenario: Correlated transactional unit subquery in batches

  Subquery is executed once per incoming row.

    When executing query:
      """
      UNWIND range(1, 10) AS i
      CALL {
        WITH i
        CREATE (:A {i: i})
      } IN CONCURRENT TRANSACTIONS OF 5 ROW
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 10 |
      | +properties | 10 |
      | +labels     | 1  |

  Scenario: Correlated transactional unit subquery with multiple writes

  Each subquery execution is correlated with one incoming row.
  Primitive values can be passed into subquery transactions.

    When executing query:
      """
      UNWIND [1, 2, 3] AS i
      CALL {
        WITH i
        UNWIND range(1, i) AS j
        CREATE (:A {i: i, j: j})
      } IN CONCURRENT TRANSACTIONS
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 6  |
      | +properties | 12 |
      | +labels     | 1  |

  Scenario: Uncorrelated transactional unit subquery preceded by match

  Reads appearing before the subquery clause should not observe any writes from subquery executions.

    And having executed:
      """
      CREATE (:A)
      """
    When executing query:
      """
      MATCH (n:A)
      CALL {
        CREATE (a:A)
      } IN CONCURRENT TRANSACTIONS
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes | 1 |

  Scenario: Correlated transactional unit subquery using entity values

  Each subquery execution is correlated with one incoming row.
  Entity values can be passed into subquery transactions.

    And having executed:
      """
      CREATE (:A), (:B), (:C)
      """
    When executing query:
      """
      MATCH (n)
      CALL {
        WITH n
        SET n.i = 1
      } IN CONCURRENT TRANSACTIONS
      """
    Then the result should be empty
    And the side effects should be:
      | +properties | 3 |

  Scenario: Correlated transactional unit subquery followed by property reads

  Reads appearing after the clause should observe writes from all subquery executions.

    And having executed:
      """
      CREATE (:A), (:B), (:C)
      """
    When executing query:
      """
      MATCH (n)
      CALL {
        WITH n
        SET n.i = 1
      } IN CONCURRENT TRANSACTIONS
      RETURN n.i AS ni
      """
    Then the result should be, in any order:
      | ni |
      | 1  |
      | 1  |
      | 1  |
    And the side effects should be:
      | +properties | 3 |

  Scenario: Correlated transactional unit subquery preceded and followed by property reads

  Reads appearing before the clause should not observe any writes from subquery executions.
  Reads appearing after the clause should observe writes from all subquery executions.

    And having executed:
      """
      CREATE (:A {i: 1}), (:B {i: 2}), (:C {i: 3})
      """
    When executing query:
      """
      MATCH (n)
      WITH n, n.i AS ni1
      CALL {
        WITH n
        SET n.i = n.i * 10
      } IN CONCURRENT TRANSACTIONS
      RETURN ni1, n.i AS ni2
      """
    Then the result should be, in any order:
      | ni1 | ni2 |
      | 1   | 10  |
      | 2   | 20  |
      | 3   | 30  |
    And the side effects should be:
      | -properties | 3 |
      | +properties | 3 |

  Scenario: Uncorrelated transactional unit subquery followed by match

  Reads appearing after the subquery clause should observe writes from all subquery executions.

    When executing query:
      """
      UNWIND [1, 2, 3] AS i
      CALL {
        CREATE (:A)
      } IN CONCURRENT TRANSACTIONS
      MATCH (n)
      RETURN count(n) AS nc
      """
    Then the result should be, in any order:
      | nc |
      | 9  |
    And the side effects should be:
      | +nodes  | 3 |
      | +labels | 1 |

  Scenario: Uncorrelated transactional unit subquery containing match and create

  Subquery executions should observe writes done in previous executions.

    And having executed:
      """
      CREATE (:A)
      """
    When executing query:
      """
      UNWIND [1, 2, 3] AS i
      CALL {
        MATCH (n)
        CREATE (:B)
      } IN CONCURRENT TRANSACTIONS
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 7 |
      | +labels | 1 |

  Scenario: Uncorrelated transactional unit subquery containing match and create followed by match

  Subquery executions should observe writes done in previous executions.
  Reads appearing after the clause should observe writes from all subquery executions.

    And having executed:
      """
      CREATE (:A)
      """
    When executing query:
      """
      UNWIND [1, 2, 3] AS i
      CALL {
        MATCH (n)
        CREATE (:B)
      } IN CONCURRENT TRANSACTIONS
      MATCH (n)
      RETURN i, count(n) AS nc
      """
    Then the result should be, in any order:
      | i | nc |
      | 1 | 8  |
      | 2 | 8  |
      | 3 | 8  |
    And the side effects should be:
      | +nodes  | 7 |
      | +labels | 1 |

  Scenario: should support call in tx importing values
    When executing query:
      """
      UNWIND range(1, 5) as i
      CALL {
        WITH i
        CREATE ({prop: i})
      } IN CONCURRENT TRANSACTIONS
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 5 |
      | +properties | 5 |

  Scenario: should support call in tx returning values
    Given having executed:
      """
      UNWIND range(1, 5) as i
      CREATE ( {prop: i})
      """
    When executing query:
      """
      CALL {
        MATCH (n)
        RETURN n.prop AS prop
      } IN CONCURRENT TRANSACTIONS
      RETURN prop
      """
    Then the result should be, in any order:
      | prop |
      | 1    |
      | 2    |
      | 3    |
      | 4    |
      | 5    |
    And no side effects

  Scenario: should support call in tx importing nodes
    Given having executed:
      """
      UNWIND range(1, 5) as i
      CREATE ( {prop: i})
      """
    When executing query:
      """
      MATCH (n)
      CALL {
        WITH n
        SET n.prop = 10 * n.prop
      } IN CONCURRENT TRANSACTIONS
      RETURN n.prop AS prop
      """
    Then the result should be, in any order:
      | prop |
      | 10   |
      | 20   |
      | 30   |
      | 40   |
      | 50   |
    And the side effects should be:
      | -properties | 5 |
      | +properties | 5 |

  Scenario: should support call in tx returning nodes
    When executing query:
      """
      UNWIND range(1, 5) as i
      CALL {
        WITH i
        CREATE (n {prop: i})
        RETURN n
      } IN CONCURRENT TRANSACTIONS
      RETURN n.prop AS prop
      """
    Then the result should be, in any order:
      | prop |
      | 1    |
      | 2    |
      | 3    |
      | 4    |
      | 5    |
    And the side effects should be:
      | +nodes      | 5 |
      | +properties | 5 |

  Scenario: should support call in tx importing rels
    Given having executed:
      """
      UNWIND range(1, 5) as i
      CREATE ()-[:R {prop: i}]->()
      """
    When executing query:
      """
      MATCH ()-[r]->()
      CALL {
        WITH r
        SET r.prop = 10 * r.prop
      } IN CONCURRENT TRANSACTIONS
      RETURN r.prop AS prop
      """
    Then the result should be, in any order:
      | prop |
      | 10   |
      | 20   |
      | 30   |
      | 40   |
      | 50   |
    And the side effects should be:
      | -properties | 5 |
      | +properties | 5 |

  Scenario: should support call in tx returning rels
    When executing query:
      """
      UNWIND range(1, 5) as i
      CALL {
        WITH i
        CREATE ()-[r:R {prop: i}]->()
        RETURN r
      } IN CONCURRENT TRANSACTIONS
      RETURN r.prop AS prop
      """
    Then the result should be, in any order:
      | prop |
      | 1    |
      | 2    |
      | 3    |
      | 4    |
      | 5    |
    And the side effects should be:
      | +relationships | 5  |
      | +nodes         | 10 |
      | +properties    | 5  |

  Scenario: should support call in tx importing paths
    Given having executed:
      """
      UNWIND range(1, 5) as i
      CREATE ()-[:R {prop: i}]->()
      """
    When executing query:
      """
      MATCH p=()-[]->()
      CALL {
        WITH p
        UNWIND relationships(p) AS r
        SET r.prop = 10 * r.prop
      } IN CONCURRENT TRANSACTIONS
      UNWIND relationships(p) AS r
      RETURN r.prop AS prop
      """
    Then the result should be, in any order:
      | prop |
      | 10   |
      | 20   |
      | 30   |
      | 40   |
      | 50   |
    And the side effects should be:
      | -properties | 5 |
      | +properties | 5 |

  Scenario: should support call in tx returning paths
    When executing query:
      """
      UNWIND range(1, 5) as i
      CALL {
        WITH i
        CREATE p=()-[r:R {prop: i}]->()
        RETURN p
      } IN CONCURRENT TRANSACTIONS
      UNWIND relationships(p) AS r
      RETURN r.prop AS prop
      """
    Then the result should be, in any order:
      | prop |
      | 1    |
      | 2    |
      | 3    |
      | 4    |
      | 5    |
    And the side effects should be:
      | +relationships | 5  |
      | +nodes         | 10 |
      | +properties    | 5  |

  Scenario: Observe changes from within uncorrelated transactional unit subqueries and not use stale property caches
    Given having executed:
      """
      CREATE ( { prop: 1 } )
      """
    When executing query:
      """
        MATCH (n)
        WITH n, n.prop as prop
        CALL {
          MATCH (m)
          SET m.prop = 42
        } IN CONCURRENT TRANSACTIONS OF 1 ROW
        RETURN n.prop
      """
    Then the result should be, in any order:
      | n.prop |
      | 42     |

  Scenario: Observe changes between uncorrelated transactional unit subqueries and not use stale property caches
    Given having executed:
      """
      CREATE ( { prop: 1 } )
      """
    When executing query:
      """
        MATCH (n)
        WITH n, n.prop as prop
        UNWIND range(1,41) as i
        CALL {
          MATCH (m)
          SET m.prop = m.prop + 1
        } IN CONCURRENT TRANSACTIONS OF 1 ROW
        RETURN n.prop LIMIT 1
      """
    Then the result should be, in any order:
      | n.prop |
      | 42     |

  Scenario: Observe changes between correlated transactional unit subqueries and not use stale property caches
    Given having executed:
      """
      CREATE (:A { prop: 1} )-[:R]->(:B { prop: 1} )
      """
    When executing query:
      """
        MATCH (n:A)--(m:B)
        WITH n, m, n.prop as prop, m.prop as mprop
        UNWIND range(1,41) as i
        CALL {
          WITH n, m
          SET n.prop = n.prop + 1
          SET m.prop = m.prop + 1
        } IN CONCURRENT TRANSACTIONS OF 1 ROW
        RETURN n.prop LIMIT 1
      """
    Then the result should be, in any order:
      | n.prop |
      | 42     |

#  Scenario: Observe changes from within correlated transactional unit subqueries and not use stale property caches
#
#    The property a.prop is written only in the second iteration of the subquery.
#    Reads of a.prop preceding/succeeding this side-effect should observe the old/new value.
#
#    Given having executed:
#      """
#      CREATE (:A { prop: 'old' } )
#      CREATE (:B { i: 0 } ), (:B { i: 1 } ), (:B { i: 2 } )
#      """
#    When executing query:
#      """
#        MATCH (a:A)
#        WITH *, a.prop as prop1
#        MATCH (b:B)
#        WITH *, b.i AS i ORDER BY i
#        CALL {
#          WITH a, b, i
#          FOREACH (ignored in CASE i WHEN 1 THEN [1] ELSE [] END | SET a.prop = 'new' )
#          SET b.prop = a.prop
#        } IN CONCURRENT TRANSACTIONS OF 1 ROW
#        RETURN i, prop1, b.prop, a.prop
#      """
#    Then the result should be, in any order:
#      | i | prop1 | b.prop | a.prop |
#      | 0 | 'old' | 'old'  | 'new'  |
#      | 1 | 'old' | 'new'  | 'new'  |
#      | 2 | 'old' | 'new'  | 'new'  |

#  Scenario: Observe changes from within correlated transactional returning subqueries and not use stale property caches
#
#    The property n.prop is written only in the second iteration of the subquery.
#    Reads of n.prop preceding/succeeding this side-effect should observe the old/new value.
#
#    Given having executed:
#      """
#      CREATE ( { prop: 'old' } )
#      """
#    When executing query:
#      """
#        MATCH (n)
#        WITH n, n.prop as prop
#        UNWIND [0, 1, 2] as i
#        CALL {
#          WITH n, i
#          FOREACH (ignored in CASE i WHEN 1 THEN [1] ELSE [] END | SET n.prop = 'new' )
#          RETURN n.prop as prop2
#        } IN CONCURRENT TRANSACTIONS OF 1 ROW
#        RETURN i, prop, prop2, n.prop
#      """
#    Then the result should be, in any order:
#      | i | prop  | prop2 | n.prop |
#      | 0 | 'old' | 'old' | 'new'  |
#      | 1 | 'old' | 'new' | 'new'  |
#      | 2 | 'old' | 'new' | 'new'  |

  Scenario: Non-positive parameter for batchSize and an empty graph should fail
    And parameters are:
      | batchSize | 0 |
    When executing query:
      """
      UNWIND range(0, 100) AS x
      CALL {
        CREATE (:A)
      } IN CONCURRENT TRANSACTIONS OF $batchSize ROWS
      """
    Then an ArgumentError should be raised at runtime: InvalidArgumentType

  Scenario: Negative parameter for batchSize and an empty graph should fail
    And parameters are:
      | batchSize | -1 |
    When executing query:
      """
      UNWIND range(0, 100) AS x
      CALL {
        CREATE (:A)
      } IN CONCURRENT TRANSACTIONS OF $batchSize ROWS
      """
    Then an ArgumentError should be raised at runtime: NegativeIntegerArgument

  Scenario: Floating point parameter for batchSize and an empty graph should fail
    And parameters are:
      | batchSize | 1.0 |
    When executing query:
      """
      UNWIND range(0, 100) AS x
      CALL {
        CREATE (:A)
      } IN CONCURRENT TRANSACTIONS OF $batchSize ROWS
      """
    Then an ArgumentError should be raised at runtime: InvalidArgumentType

  Scenario: Graph touching batchSize should fail with a syntax exception
    And having executed:
      """
      CREATE (s:Person {name: 'Steven'}),
             (c:Person {name: 'Craig'})
      """
    When executing query:
      """
      UNWIND range(0, 100) AS x
      CALL {
        CREATE (:A)
      } IN CONCURRENT TRANSACTIONS OF reduce(sum=0, x IN [(a)-->(b) | b.age] | sum + x) ROWS
      """
    Then a SyntaxError should be raised at compile time: NonConstantExpression

  Scenario: Graph touching batchSize should fail with a syntax exception 2
    And having executed:
      """
      CREATE (s:Person {name: 'Steven'}),
             (c:Person {name: 'Craig'})
      """
    When executing query:
      """
      UNWIND range(0, 100) AS x
      CALL {
        CREATE (:A)
      } IN CONCURRENT TRANSACTIONS OF size([(a)-->(b) | b.age]) ROWS
      """
    Then a SyntaxError should be raised at compile time: NonConstantExpression

  Scenario: Graph touching batchSize should fail with a syntax exception 3
    And having executed:
      """
      CREATE (s:Person {name: 'Steven'}),
             (c:Person {name: 'Craig'})
      """
    When executing query:
      """
      UNWIND range(0, 100) AS x
      CALL {
        CREATE (:A)
      } IN CONCURRENT TRANSACTIONS OF size(()-->()) ROWS
      """
    Then a SyntaxError should be raised at compile time: NonConstantExpression

  Scenario: Graph touching batchSize should fail with a syntax exception 4
    And having executed:
      """
      CREATE (s:Person {name: 'Steven'}),
             (c:Person {name: 'Craig'})
      """
    When executing query:
      """
      UNWIND range(0, 100) AS x
      CALL {
        CREATE (:A)
      } IN CONCURRENT TRANSACTIONS OF reduce(sum=0, x IN [p.age] | sum + x) ROWS
      """
    Then a SyntaxError should be raised at compile time: NonConstantExpression

  Scenario: Reduce batchSize should be allowed
    When executing query:
      """
      UNWIND range(1, 10) AS x
      CALL {
        CREATE (:A)
      } IN CONCURRENT TRANSACTIONS OF reduce(sum=0, x IN [0, 2] | sum + x) ROWS
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 10 |
      | +labels | 1  |

  Scenario: BatchSize of more than Integer.Max rows should be allowed
    When executing query:
      """
      UNWIND range(1, 10) AS x
      CALL {
        CREATE (:A)
      } IN CONCURRENT TRANSACTIONS OF 9223372036854775807 ROWS // Long.Max
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 10 |
      | +labels | 1  |

  Scenario: Using values read from a CSV
    Given an empty graph
    And there exists a CSV file with URL as $param, with rows:
      | name       | age |
      | 'David'    | 55  |
      | 'Tim'      | 32  |
      | 'Gareth'   | 39  |
      | 'Dawn'     | 35  |
      | 'Jennifer' | 45  |

    When executing query:
      """
      LOAD CSV WITH HEADERS from $param AS row
      CALL {
        WITH row
        CREATE (n {name: row.name, age: toInteger(row.age)})
        RETURN n
      } IN CONCURRENT TRANSACTIONS
      RETURN n.name, n.age ORDER BY n.age ASC
      """
    Then the result should be, in order:
      | n.name     | n.age |
      | 'Tim'      | 32    |
      | 'Dawn'     | 35    |
      | 'Gareth'   | 39    |
      | 'Jennifer' | 45    |
      | 'David'    | 55    |
    And the side effects should be:
      | +nodes       | 5  |
      | +properties  | 10 |

  Scenario: Handle an empty map and list before CALL IN CONCURRENT TRANSACTIONS
    When executing query:
      """
      WITH {} AS emptyMap, [] AS emptyList
      CALL {
        WITH 42 AS theValue
        RETURN theValue
      } IN CONCURRENT TRANSACTIONS
      RETURN theValue
      """
    Then the result should be, in any order:
      | theValue |
      | 42       |
