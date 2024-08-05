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

# If you plan to make changes here, you probably want to change
# CallInTransactionsErrorHandlingWithReturn.feature too
Feature: CallInTransactionsErrorHandling

 Scenario Outline: Create in transactions of default size <onErrorBehaviour>
    Given an empty graph
    When executing query:
      """
      UNWIND range(1, 10) AS i
      CALL {
        WITH i
        UNWIND [1, 2] AS j
        CREATE (n:N {i: i, j: j})
      } IN TRANSACTIONS
        <onErrorBehaviour>
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 20 |
      | +properties | 40 |
      | +labels     |  1 |
   Examples:
     | onErrorBehaviour  |
     | ON ERROR CONTINUE |
     | ON ERROR BREAK    |

  Scenario Outline: Create in transactions of default size <onErrorBehaviour> with Scope Clause
    Given an empty graph
    When executing query:
      """
      UNWIND range(1, 10) AS i
      CALL (i) {
        UNWIND [1, 2] AS j
        CREATE (n:N {i: i, j: j})
      } IN TRANSACTIONS
        <onErrorBehaviour>
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 20 |
      | +properties | 40 |
      | +labels     |  1 |
    Examples:
      | onErrorBehaviour  |
      | ON ERROR CONTINUE |
      | ON ERROR BREAK    |

  Scenario Outline: Create in transactions of <rows> rows <onErrorBehaviour>
    Given an empty graph
    When executing query:
      """
      UNWIND range(1, 10) AS i
      CALL {
        WITH i
        UNWIND [1, 2] AS j
        CREATE (n:N {i: i, j: j})
      } IN TRANSACTIONS
        OF <rows> ROWS
        <onErrorBehaviour>
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 20 |
      | +properties | 40 |
      | +labels     |  1 |
    Examples:
      | rows | onErrorBehaviour  |
      |    1 | ON ERROR CONTINUE |
      |    1 | ON ERROR BREAK    |
      |    3 | ON ERROR CONTINUE |
      |    3 | ON ERROR BREAK    |
      |    5 | ON ERROR CONTINUE |
      |    5 | ON ERROR BREAK    |
      |   10 | ON ERROR CONTINUE |
      |   10 | ON ERROR BREAK    |
      |   77 | ON ERROR CONTINUE |
      |   77 | ON ERROR BREAK    |

  Scenario Outline: Create in transactions of <rows> rows <onErrorBehaviour> with Scope Clause
    Given an empty graph
    When executing query:
      """
      UNWIND range(1, 10) AS i
      CALL (i) {
        UNWIND [1, 2] AS j
        CREATE (n:N {i: i, j: j})
      } IN TRANSACTIONS
        OF <rows> ROWS
        <onErrorBehaviour>
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 20 |
      | +properties | 40 |
      | +labels     |  1 |
    Examples:
      | rows | onErrorBehaviour  |
      |    1 | ON ERROR CONTINUE |
      |    1 | ON ERROR BREAK    |
      |    3 | ON ERROR CONTINUE |
      |    3 | ON ERROR BREAK    |
      |    5 | ON ERROR CONTINUE |
      |    5 | ON ERROR BREAK    |
      |   10 | ON ERROR CONTINUE |
      |   10 | ON ERROR BREAK    |
      |   77 | ON ERROR CONTINUE |
      |   77 | ON ERROR BREAK    |

  Scenario Outline: Create in transactions of default size with status <onErrorBehaviour>
    Given an empty graph
    When executing query:
      """
      UNWIND range(0, 9) AS i
      CALL {
          WITH i
          UNWIND [1, 2] AS j
          CREATE (n:N {i: i, j: j})
      } IN TRANSACTIONS
        <onErrorBehaviour>
        REPORT STATUS AS status
      RETURN
        i,
        status { .started, .committed, .errorMessage } AS status,
        status.transactionId IS NOT NULL AS hasTxId
      """
    Then the result should be, in order:
      | i | status                                               | hasTxId |
      | 0 | {started: true, committed: true, errorMessage: null} | true    |
      | 1 | {started: true, committed: true, errorMessage: null} | true    |
      | 2 | {started: true, committed: true, errorMessage: null} | true    |
      | 3 | {started: true, committed: true, errorMessage: null} | true    |
      | 4 | {started: true, committed: true, errorMessage: null} | true    |
      | 5 | {started: true, committed: true, errorMessage: null} | true    |
      | 6 | {started: true, committed: true, errorMessage: null} | true    |
      | 7 | {started: true, committed: true, errorMessage: null} | true    |
      | 8 | {started: true, committed: true, errorMessage: null} | true    |
      | 9 | {started: true, committed: true, errorMessage: null} | true    |
    And the side effects should be:
      | +nodes      | 20 |
      | +properties | 40 |
      | +labels     |  1 |
    Examples:
      | onErrorBehaviour  |
      | ON ERROR CONTINUE |
      | ON ERROR BREAK    |

  Scenario Outline: Create in transactions of <rows> rows with status <onErrorBehaviour>
    Given an empty graph
    When executing query:
      """
      UNWIND range(0, 9) AS i
      CALL {
          WITH i
          UNWIND [1, 2] AS j
          CREATE (n:N {i: i, j: j})
      } IN TRANSACTIONS
        OF <rows> ROWS
        <onErrorBehaviour>
        REPORT STATUS AS status
      WITH
        status.transactionId AS tx,
        collect(i) AS is,
        collect(DISTINCT status { .started, .committed, .errorMessage }) AS distinct_status
      RETURN collect(is) AS is, collect(DISTINCT distinct_status) AS distinct_status
      """
    Then the result should be, in order:
      | is   | distinct_status                                          |
      | <is> | [[{started: true, committed: true, errorMessage: null}]] |
    And the side effects should be:
      | +nodes      | 20 |
      | +properties | 40 |
      | +labels     |  1 |
    Examples:
      | rows | onErrorBehaviour    | is                                                 |
      |  1   | ON ERROR CONTINUE | [[0], [1], [2], [3], [4], [5], [6], [7], [8], [9]] |
      |  1   | ON ERROR BREAK    | [[0], [1], [2], [3], [4], [5], [6], [7], [8], [9]] |
      |  3   | ON ERROR CONTINUE | [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]             |
      |  3   | ON ERROR BREAK    | [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]             |
      |  5   | ON ERROR CONTINUE | [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]                 |
      |  5   | ON ERROR BREAK    | [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]                 |
      | 13   | ON ERROR CONTINUE | [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]]                   |
      | 13   | ON ERROR BREAK    | [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]]                   |

# Expected errors with side effects not supported yet
#  @allowCustomErrors
#  Scenario Outline: Create with rollback in transactions of <rows> rows
#    Given an empty graph
#    When executing query:
#      """
#      UNWIND [1, 1, 1, 0, 1] AS i
#      CALL {
#        WITH i
#        UNWIND [1, 0] AS j
#        CREATE (n:N {p: 1/(i + j)})
#      } IN TRANSACTIONS
#        OF <rows> ROWS
#      """
#    Then a CypherExecutorException should be raised at runtime, with side effects: *
#    And the side effects should be:
#      | +nodes      | <createdNodes> |
#      | +properties | <createdNodes> |
#      | +labels     | <createdNodes> |
#    Examples:
#      | rows | createdNodes |
#      |    1 |            6 |
#      |    2 |            4 |
#      |    3 |            6 |
#      |    4 |            0 |
#      |    5 |            0 |
#      |    6 |            0 |

  Scenario Outline: Create with rollback in transactions of <rows> rows ON ERROR BREAK
    Given an empty graph
    When executing query:
      """
      UNWIND [1, 1, 1, 0, 1, 1] AS i
      CALL {
        WITH i
        UNWIND [1, 0] AS j
        CREATE (n:N {p: 1/(i + j)})
      } IN TRANSACTIONS
        OF <rows> ROWS
        ON ERROR BREAK
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | <createdNodes> |
      | +properties | <createdNodes> |
      | +labels     | <addedLabels>  |
    Examples:
      | rows | createdNodes | addedLabels |
      |    1 |            6 |           1 |
      |    2 |            4 |           1 |
      |    3 |            6 |           1 |
      |    4 |            0 |           0 |
      |    5 |            0 |           0 |
      |    6 |            0 |           0 |

  Scenario Outline: Create with rollback in transactions of <rows> rows ON ERROR CONTINUE
    Given an empty graph
    When executing query:
      """
      UNWIND [1, 1, 1, 0, 1, 1] AS i
      CALL {
        WITH i
        UNWIND [1, 0] AS j
        CREATE (n:N {p: 1/(i + j)})
      } IN TRANSACTIONS
        OF <rows> ROWS
        ON ERROR CONTINUE
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | <createdNodes> |
      | +properties | <createdNodes> |
      | +labels     | <addedLabels>  |
    Examples:
      | rows | createdNodes | addedLabels |
      |    1 |           10 |           1 |
      |    2 |            8 |           1 |
      |    3 |            6 |           1 |
      |    4 |            4 |           1 |
      |    5 |            2 |           1 |
      |    6 |            0 |           0 |

#  Expected errors with side effects not supported yet
#  @allowCustomErrors
#  Scenario: Create and report status with rollback in transactions ON ERROR FAIL
#    Given an empty graph
#    When executing query:
#      """
#      UNWIND [1, 1, 1, 0, 1] AS i
#      CALL {
#        WITH i
#        UNWIND [1, 0] AS j
#        CREATE (n:N {p: 1/(i + j)})
#      } IN TRANSACTIONS
#        ON ERROR FAIL
#        OF 2 ROWS
#        REPORT STATUS AS status
#      RETURN i, status
#      """
#    Then a CypherExecutorException should be raised at runtime, with side effects: *
#    And the side effects should be:
#      | +nodes      | 6 |
#      | +properties | 6 |
#      | +labels     | 1 |

  Scenario: Create and report status with rollback in transactions ON ERROR BREAK
    Given an empty graph
    When executing query:
      """
      UNWIND [1, 2, 3, 0, 4] AS i
      CALL {
        WITH i
        UNWIND [1, 0] AS j
        CREATE (n:N {p: 1/(i + j)})
      } IN TRANSACTIONS
        OF 2 ROWS
        ON ERROR BREAK
        REPORT STATUS AS status
      RETURN
        i,
        status.transactionId IS NOT NULL AS hasTxId,
        status.started AS started,
        status.committed AS committed,
        status.errorMessage IS NOT NULL AS hasErrorMessage
      """
    Then the result should be, in order:
      | i | hasTxId | started | committed | hasErrorMessage |
      | 1 | true    | true    | true      | false           |
      | 2 | true    | true    | true      | false           |
      | 3 | true    | true    | false     | true            |
      | 0 | true    | true    | false     | true            |
      | 4 | false   | false   | false     | false           |
    And the side effects should be:
      | +nodes      | 4 |
      | +properties | 4 |
      | +labels     | 1 |

  Scenario: Create and report status with rollback in transactions ON ERROR BREAK with Scope Clause
    Given an empty graph
    When executing query:
      """
      UNWIND [1, 2, 3, 0, 4] AS i
      CALL (i) {
        UNWIND [1, 0] AS j
        CREATE (n:N {p: 1/(i + j)})
      } IN TRANSACTIONS
        OF 2 ROWS
        ON ERROR BREAK
        REPORT STATUS AS status
      RETURN
        i,
        status.transactionId IS NOT NULL AS hasTxId,
        status.started AS started,
        status.committed AS committed,
        status.errorMessage IS NOT NULL AS hasErrorMessage
      """
    Then the result should be, in order:
      | i | hasTxId | started | committed | hasErrorMessage |
      | 1 | true    | true    | true      | false           |
      | 2 | true    | true    | true      | false           |
      | 3 | true    | true    | false     | true            |
      | 0 | true    | true    | false     | true            |
      | 4 | false   | false   | false     | false           |
    And the side effects should be:
      | +nodes      | 4 |
      | +properties | 4 |
      | +labels     | 1 |

  Scenario: Create and report status with rollback in transactions ON ERROR CONTINUE
    Given an empty graph
    When executing query:
      """
      UNWIND [1, 2, 3, 0, 4] AS i
      CALL {
        WITH i
        UNWIND [1, 0] AS j
        CREATE (n:N {p: 1/(i + j)})
      } IN TRANSACTIONS
        OF 2 ROWS
        ON ERROR CONTINUE
        REPORT STATUS AS status
      RETURN
        i,
        status.transactionId IS NOT NULL AS hasTxId,
        status.started AS started,
        status.committed AS committed,
        status.errorMessage IS NOT NULL AS hasErrorMessage
      """
    Then the result should be, in order:
      | i | hasTxId | started | committed | hasErrorMessage |
      | 1 | true    | true    | true      | false           |
      | 2 | true    | true    | true      | false           |
      | 3 | true    | true    | false     | true            |
      | 0 | true    | true    | false     | true            |
      | 4 | true    | true    | true      | false           |
    And the side effects should be:
      | +nodes      | 6 |
      | +properties | 6 |
      | +labels     | 1 |
