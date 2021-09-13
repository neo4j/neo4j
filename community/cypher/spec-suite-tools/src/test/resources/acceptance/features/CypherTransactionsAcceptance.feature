#
# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
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

#encoding: utf-8

Feature: CypherTransactionsAcceptance

  Background:
    Given an empty graph

  Scenario: Uncorrelated transactional unit subquery with single transaction
    When executing query:
      """
      CALL {
        CREATE (:A)
      } IN TRANSACTIONS
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
      } IN TRANSACTIONS
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 3 |
      | +labels | 1 |

  Scenario: Correlated transactional unit subquery

    Subquery is executed once per incoming row.

    When executing query:
      """
      UNWIND [1, 2, 3] AS i
      CALL {
        WITH i
        CREATE (:A {i: i})
      } IN TRANSACTIONS
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 3 |
      | +properties | 3 |
      | +labels     | 1 |

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
      } IN TRANSACTIONS
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
      } IN TRANSACTIONS
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
      } IN TRANSACTIONS
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
      } IN TRANSACTIONS
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
      } IN TRANSACTIONS
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
      } IN TRANSACTIONS
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
      } IN TRANSACTIONS
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
      } IN TRANSACTIONS
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

  Scenario: Uncorrelated transactional unit subquery updating the same property value

    Subquery executions should observe writes done in previous executions.
    Reads appearing after the clause should observe writes from all subquery executions.

    And having executed:
      """
      CREATE (:A {i: 1})
      """
    When executing query:
      """
      UNWIND [1, 2, 3] AS i
      MATCH (n)
      CALL {
        MATCH (m)
        SET m.i = m.i * 10
      } IN TRANSACTIONS
      RETURN i, n.i
      """
    Then the result should be, in any order:
      | i | n.i  |
      | 1 | 1000 |
      | 2 | 1000 |
      | 3 | 1000 |
    And the side effects should be:
      | -properties | 1 |
      | +properties | 1 |

  Scenario: Correlated transactional unit subquery updating the same property list value in sequence

    Correlated subquery executions happen in the order of input rows.

    And having executed:
      """
      CREATE (:A {is: [0]})
      """
    When executing query:
      """
      MATCH (n)
      UNWIND [1, 2, 3] AS i
      WITH * ORDER BY i
      CALL {
        WITH n, i
        SET n.is = n.is + [i]
      } IN TRANSACTIONS
      RETURN n.is
      """
    Then the result should be, in any order:
      | n.is         |
      | [0, 1, 2, 3] |
      | [0, 1, 2, 3] |
      | [0, 1, 2, 3] |
    And the side effects should be:
      | -properties | 1 |
      | +properties | 1 |

  Scenario: should support call in tx importing values
    When executing query:
      """
      UNWIND range(1, 5) as i
      CALL { WITH i CREATE ({prop: i}) } IN TRANSACTIONS
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
      CALL { MATCH (n) RETURN n.prop AS prop } IN TRANSACTIONS
      RETURN prop ORDER BY prop
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
      CALL { WITH n SET n.prop = 10 * n.prop } IN TRANSACTIONS
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
    Given having executed:
      """
      UNWIND range(1, 5) as i
      CREATE ( {prop: i})
      """
    When executing query:
      """
      MATCH ()
      CALL { CREATE (n {prop: 1}) RETURN n } IN TRANSACTIONS
      RETURN n.prop AS prop ORDER BY prop
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
      CALL { WITH r SET r.prop = 10 * r.prop } IN TRANSACTIONS
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
    Given having executed:
      """
      UNWIND range(1, 5) as i
      CREATE ( {prop: i})
      """
    When executing query:
      """
      MATCH ()
      CALL { CREATE ()-[r:R {prop: 1}]->() RETURN r } IN TRANSACTIONS
      RETURN r.prop AS prop ORDER BY prop
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
      CALL { WITH p UNWIND relationships(p) AS r SET r.prop = 10 * r.prop } IN TRANSACTIONS
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
    Given having executed:
      """
      UNWIND range(1, 5) as i
      CREATE ( {prop: i})
      """
    When executing query:
      """
      MATCH ()
      CALL { CREATE p=()-[r:R {prop: 1}]->() RETURN p } IN TRANSACTIONS
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
          MATCH (n)
          SET n.prop = 42
        } IN TRANSACTIONS
        RETURN n.prop
      """
    Then the result should be, in any order:
      | n.prop |
      |   42   |

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
          MATCH (n)
          SET n.prop = n.prop + 1
        } IN TRANSACTIONS
        RETURN n.prop LIMIT 1
      """
    Then the result should be, in any order:
      | n.prop |
      |   42   |

  Scenario: Observe changes between correlated transactional unit subqueries and not use stale property caches
    Given having executed:
      """
      CREATE (:A { prop: 1} )-[:R]->(:B { prop: 1} )
      """
    When executing query:
      """
        MATCH (n:A)--(m:B)
        WITH n, m, n.prop as prop, m.prop as mprop
        UNWIND range(1,42) as i
        CALL {
          WITH n, m
          SET n.prop = m.prop
          SET m.prop = n.prop + 1
        } IN TRANSACTIONS
        RETURN n.prop LIMIT 1
      """
    Then the result should be, in any order:
      | n.prop |
      |   42   |

  Scenario: Observe changes from within correlated transactional unit subqueries and not use stale property caches
    Given having executed:
      """
      CREATE ( { prop: 42 } )
      """
    When executing query:
      """
        MATCH (n)
        WITH n, n.prop as prop
        UNWIND [0, 1, 2] as i
        CALL {
          WITH n, i
          FOREACH (ignored in CASE i WHEN 1 THEN [1] ELSE [] END | SET n.prop = i )
          WITH n, n.prop as prop2
          SET n.prop2 = "dummy"
        } IN TRANSACTIONS
        RETURN prop, n.prop
      """
    Then the result should be, in any order:
      | prop | n.prop |
      |  42  |   1    |
      |  42  |   1    |
      |  42  |   1    |

  Scenario: Observe changes from within correlated transactional returning subqueries and not use stale property caches
    Given having executed:
      """
      CREATE ( { prop: 42 } )
      """
    When executing query:
      """
        MATCH (n)
        WITH n, n.prop as prop
        UNWIND [0, 1, 2] as i
        CALL {
          WITH n, i
          FOREACH (ignored in CASE i WHEN 1 THEN [1] ELSE [] END | SET n.prop = i )
          RETURN n.prop as prop2
        } IN TRANSACTIONS
        RETURN prop, n.prop, prop2
      """
    Then the result should be, in any order:
      | prop | n.prop | prop2 |
      |  42  |   1    |   42  |
      |  42  |   1    |    1  |
      |  42  |   1    |    1  |
