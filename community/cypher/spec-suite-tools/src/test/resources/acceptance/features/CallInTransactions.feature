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
Feature: CallInTransactions

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
     |                   |
     | ON ERROR FAIL     |

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
      |                   |
      | ON ERROR FAIL     |

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
      |    1 |                   |
      |    1 | ON ERROR FAIL     |
      |    3 |                   |
      |    3 | ON ERROR FAIL     |
      |    5 |                   |
      |    5 | ON ERROR FAIL     |
      |   10 |                   |
      |   10 | ON ERROR FAIL     |
      |   77 |                   |
      |   77 | ON ERROR FAIL     |

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
      |    1 |                   |
      |    1 | ON ERROR FAIL     |
      |    3 |                   |
      |    3 | ON ERROR FAIL     |
      |    5 |                   |
      |    5 | ON ERROR FAIL     |
      |   10 |                   |
      |   10 | ON ERROR FAIL     |
      |   77 |                   |
      |   77 | ON ERROR FAIL     |

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

  Scenario Outline: Create and return in transactions of default size <onErrorBehaviour>
    Given an empty graph
    When executing query:
      """
      UNWIND range(1, 5) AS i
      CALL {
        WITH i
        UNWIND [1, 2] AS j
        CREATE (n:N {i: i, j: j})
        RETURN j
      } IN TRANSACTIONS
        <onErrorBehaviour>
      RETURN i, j
      """
    Then the result should be, in order:
      | i | j |
      | 1 | 1 |
      | 1 | 2 |
      | 2 | 1 |
      | 2 | 2 |
      | 3 | 1 |
      | 3 | 2 |
      | 4 | 1 |
      | 4 | 2 |
      | 5 | 1 |
      | 5 | 2 |
    And the side effects should be:
      | +nodes      | 10 |
      | +properties | 20 |
      | +labels     |  1 |
    Examples:
      | onErrorBehaviour  |
      |                   |
      | ON ERROR FAIL     |

  Scenario Outline: Create and return in transactions of <rows> rows <onErrorBehaviour>
    Given an empty graph
    When executing query:
      """
      UNWIND range(0, 9) AS i
      CALL {
        WITH i
        UNWIND [1, 2] AS j
        CREATE (n:N {i: i, j: j})
        RETURN j
      } IN TRANSACTIONS
        OF <rows> ROWS
        <onErrorBehaviour>
      RETURN i, j
      """
    Then the result should be, in order:
      | i | j |
      | 0 | 1 |
      | 0 | 2 |
      | 1 | 1 |
      | 1 | 2 |
      | 2 | 1 |
      | 2 | 2 |
      | 3 | 1 |
      | 3 | 2 |
      | 4 | 1 |
      | 4 | 2 |
      | 5 | 1 |
      | 5 | 2 |
      | 6 | 1 |
      | 6 | 2 |
      | 7 | 1 |
      | 7 | 2 |
      | 8 | 1 |
      | 8 | 2 |
      | 9 | 1 |
      | 9 | 2 |
    And the side effects should be:
      | +nodes      | 20 |
      | +properties | 40 |
      | +labels     |  1 |
    Examples:
      | rows | onErrorBehaviour  |
      |    1 |                   |
      |    1 | ON ERROR FAIL     |
      |    3 |                   |
      |    3 | ON ERROR FAIL     |
      |    5 |                   |
      |    5 | ON ERROR FAIL     |
      |   10 |                   |
      |   10 | ON ERROR FAIL     |
      |   77 |                   |
      |   77 | ON ERROR FAIL     |

#  Expected errors with side effects not supported yet
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
#        RETURN j
#      } IN TRANSACTIONS
#        OF <rows> ROWS
#      RETURN i, j
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
#        RETURN j
#      } IN TRANSACTIONS
#        ON ERROR FAIL
#        OF 2 ROWS
#        REPORT STATUS AS status
#      RETURN i, j, status
#      """
#    Then a CypherExecutorException should be raised at runtime, with side effects: *
#    And the side effects should be:
#      | +nodes      | 6 |
#      | +properties | 6 |
#      | +labels     | 1 |
