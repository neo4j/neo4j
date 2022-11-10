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

# If you plan to make changes here, you probably want to change
# CallInTransactionsWithReturn.feature too
Feature: CallInTransactionsFeatureFlag

 Scenario Outline: Call in transactions error handling and status should be disabled
    Given an empty graph
    When executing query:
      """
      UNWIND [0] AS i
      CALL {
        WITH i
        UNWIND [1] AS j
        CREATE (n:N {i: i, j: j})
      } IN TRANSACTIONS
        <extra>
      """
   Then a SyntaxError should be raised at compile time: *
   Examples:
     | extra                                     |
     | ON ERROR FAIL                             |
     | ON ERROR CONTINUE                         |
     | ON ERROR BREAK                            |
     | REPORT STATUS AS status                   |
     | ON ERROR FAIL REPORT STATUS AS status     |
     | ON ERROR CONTINUE REPORT STATUS AS status |
     | ON ERROR BREAK REPORT STATUS AS status    |

  Scenario Outline: Call in transactions error handling and status should be disabled with return
    Given an empty graph
    When executing query:
      """
      UNWIND [0] AS i
      CALL {
        WITH i
        UNWIND [1] AS j
        CREATE (n:N {i: i, j: j})
        RETURN j
      } IN TRANSACTIONS
        <extra>
      """
    Then a SyntaxError should be raised at compile time: *
    Examples:
      | extra                                     |
      | ON ERROR FAIL                             |
      | ON ERROR CONTINUE                         |
      | ON ERROR BREAK                            |
      | REPORT STATUS AS status                   |
      | ON ERROR FAIL REPORT STATUS AS status     |
      | ON ERROR CONTINUE REPORT STATUS AS status |
      | ON ERROR BREAK REPORT STATUS AS status    |

  Scenario: Regular call in transactions should work
    Given an empty graph
    When executing query:
      """
      UNWIND [0] AS i
      CALL {
        WITH i
        UNWIND [1] AS j
        CREATE (n:N {i: i, j: j})
      } IN TRANSACTIONS
      """
    Then the result should be empty