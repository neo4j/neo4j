#
# Copyright (c) 2002-2019 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j Enterprise Edition. The included source
# code can be redistributed and/or modified under the terms of the
# GNU AFFERO GENERAL PUBLIC LICENSE Version 3
# (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
# Commons Clause, as found in the associated LICENSE.txt file.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# Neo4j object code can be licensed independently from the source
# under separate terms from the AGPL. Inquiries can be directed to:
# licensing@neo4j.com
#
# More information is also available at:
# https://neo4j.com/licensing/
#

#encoding: utf-8

Feature: ListComprehensionAcceptance

    Scenario: Should find all the variables in list comprehension
      Given an empty graph
      When executing query:
      """
      RETURN
        [x IN [1] WHERE x > 0 ] AS res1,
        [x IN [1] WHERE x > 0 ] AS res2,
        [x IN [1]] AS res3
      """
      Then the result should be:
        | res1 | res2 | res3 |
        | [1]  | [1]  | [1]  |
      And no side effects

      Scenario: Should handle list comprehension with AND and one NOT
        Given an empty graph
        And having executed:
        """
        CREATE (:Company {nbr: 1, status: 'ACTIVE'})
        CREATE (:Company {nbr: 2, status: 'INACTIVE'})
        CREATE (:Company {nbr: 3, status: 'DELETED'})
        CREATE (:Company {nbr: 4, status: 'X'})
        """
        When executing query:
        """
        MATCH (company :Company)
        WHERE company.status IN ["ACTIVE", "INACTIVE"] AND NOT company.status IN ["INACTIVE", "DELETED"]
        RETURN company.nbr AS nbr
        """
        Then the result should be:
          | nbr |
          | 1   |
        And no side effects

  Scenario: Should handle list comprehension with AND and two NOT
    Given an empty graph
    And having executed:
        """
        CREATE (:Company {nbr: 1, status: 'ACTIVE'})
        CREATE (:Company {nbr: 2, status: 'INACTIVE'})
        CREATE (:Company {nbr: 3, status: 'DELETED'})
        CREATE (:Company {nbr: 4, status: 'X'})
        """
    When executing query:
        """
        MATCH (company :Company)
        WHERE NOT company.status IN ["ACTIVE", "INACTIVE"] AND NOT company.status IN ["INACTIVE", "DELETED"]
        RETURN company.nbr AS nbr
        """
    Then the result should be:
      | nbr |
      | 4   |
    And no side effects

  Scenario: Should handle list comprehension with OR and one NOT
    Given an empty graph
    And having executed:
        """
        CREATE (:Company {nbr: 1, status: 'ACTIVE'})
        CREATE (:Company {nbr: 2, status: 'INACTIVE'})
        CREATE (:Company {nbr: 3, status: 'DELETED'})
        CREATE (:Company {nbr: 4, status: 'X'})
        """
    When executing query:
        """
        MATCH (company :Company)
        WHERE company.status IN ["ACTIVE", "INACTIVE"] OR NOT company.status IN ["INACTIVE", "DELETED"]
        WITH company.nbr AS nbr
        RETURN nbr ORDER BY nbr
        """
    Then the result should be:
      | nbr |
      | 1   |
      | 2   |
      | 4   |
    And no side effects

  Scenario: Should handle list comprehension with OR and two NOT
    Given an empty graph
    And having executed:
        """
        CREATE (:Company {nbr: 1, status: 'ACTIVE'})
        CREATE (:Company {nbr: 2, status: 'INACTIVE'})
        CREATE (:Company {nbr: 3, status: 'DELETED'})
        CREATE (:Company {nbr: 4, status: 'X'})
        """
    When executing query:
        """
        MATCH (company :Company)
        WHERE NOT company.status IN ["ACTIVE", "INACTIVE"] OR NOT company.status IN ["INACTIVE", "DELETED"]
        WITH company.nbr AS nbr
        RETURN nbr ORDER BY nbr
        """
    Then the result should be:
      | nbr |
      | 1   |
      | 3   |
      | 4   |
    And no side effects
