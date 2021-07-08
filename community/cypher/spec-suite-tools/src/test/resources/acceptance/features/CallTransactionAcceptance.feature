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

Feature: CallTransactionAcceptance

  Background:
    Given an empty graph

  Scenario: Uncorrelated unit subquery transaction
    When executing query:
      """
      CALL TRANSACTION {
        CREATE (:A)
      }
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 1 |
      | +labels | 1 |

  Scenario: Uncorrelated unit subquery transactions

    Subquery is executed once per incoming row.

    When executing query:
      """
      UNWIND [1, 2, 3] AS i
      CALL TRANSACTION {
        CREATE (:A)
      }
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 3 |
      | +labels | 1 |

  Scenario: Correlated unit subquery transactions

    Subquery is executed once per incoming row.

    When executing query:
      """
      UNWIND [1, 2, 3] AS i
      CALL TRANSACTION {
        WITH i
        CREATE (:A {i: i})
      }
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 3 |
      | +properties | 3 |
      | +labels     | 1 |

  Scenario: Correlated unit subquery transactions with multiple writes

    Each subquery execution is correlated with one incoming row.
    Primitive values can be passed into subquery transactions.

    When executing query:
      """
      UNWIND [1, 2, 3] AS i
      CALL TRANSACTION {
        WITH i
        UNWIND range(1, i) AS j
        CREATE (:A {i: i, j: j})
      }
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 6  |
      | +properties | 12 |
      | +labels     | 1  |

  Scenario: Uncorrelated unit subquery transaction preceded by match

    Reads appearing before the subquery clause should not observe any writes from subquery executions.

    And having executed:
      """
      CREATE (:A)
      """
    When executing query:
      """
      MATCH (n:A)
      CALL TRANSACTION {
        CREATE (a:A)
      }
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes | 1 |

  Scenario: Correlated unit subquery transactions using entity values

    Each subquery execution is correlated with one incoming row.
    Entity values can be passed into subquery transactions.

    And having executed:
      """
      CREATE (:A), (:B), (:C)
      """
    When executing query:
      """
      MATCH (n)
      CALL TRANSACTION {
        WITH n
        SET n.i = 1
      }
      """
    Then the result should be empty
    And the side effects should be:
      | +properties | 3 |

  Scenario: Correlated unit subquery transactions followed by property reads

    Reads appearing after the clause should observe writes from all subquery executions.

    And having executed:
      """
      CREATE (:A), (:B), (:C)
      """
    When executing query:
      """
      MATCH (n)
      CALL TRANSACTION {
        WITH n
        SET n.i = 1
      }
      RETURN n.i AS ni
      """
    Then the result should be, in any order:
      | ni |
      | 1  |
      | 1  |
      | 1  |
    And the side effects should be:
      | +properties | 3 |

  Scenario: Correlated unit subquery transactions preceded and followed by property reads

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
      CALL TRANSACTION {
        WITH n
        SET n.i = n.i * 10
      }
      RETURN n.i AS ni2
      """
    Then the result should be, in any order:
      | ni1 | ni2 |
      | 1   | 10  |
      | 2   | 20  |
      | 3   | 30  |
    And the side effects should be:
      | -properties | 3 |
      | +properties | 3 |

  Scenario: Uncorrelated unit subquery transactions followed by match

    Reads appearing after the subquery clause should observe writes from all subquery executions.

    When executing query:
      """
      UNWIND [1, 2, 3] AS i
      CALL TRANSACTION {
        CREATE (:A)
      }
      MATCH (n)
      RETURN count(n) AS nc
      """
    Then the result should be, in any order:
      | nc |
      | 3  |
      | 3  |
      | 3  |
    And the side effects should be:
      | +nodes  | 3 |
      | +labels | 1 |

  Scenario: Uncorrelated unit subquery transactions containing match and create

    Subquery executions should observe writes done in previous executions.

    And having executed:
      """
      CREATE (:A)
      """
    When executing query:
      """
      UNWIND [1, 2, 3] AS i
      CALL TRANSACTION {
        MATCH (n)
        CREATE (:B)
      }
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 7 |
      | +labels | 1 |

  Scenario: Uncorrelated unit subquery transactions containing match and create followed by match

    Subquery executions should observe writes done in previous executions.
    Reads appearing after the clause should observe writes from all subquery executions.

    And having executed:
      """
      CREATE (:A)
      """
    When executing query:
      """
      UNWIND [1, 2, 3] AS i
      CALL TRANSACTION {
        MATCH (n)
        CREATE (:B)
      }
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

  Scenario: Uncorrelated unit subquery transactions updating the same property value

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
      CALL TRANSACTION {
        MATCH (m)
        SET m.i = m.i * 10
      }
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

  Scenario: Correlated unit subquery transactions updating the same property list value in sequence

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
      CALL TRANSACTION {
        WITH n, i
        SET n.is = n.is + [i]
      }
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
