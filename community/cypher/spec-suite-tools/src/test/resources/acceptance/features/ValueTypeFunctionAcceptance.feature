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

Feature: ValueTypeFunctionAcceptance

  Background:
    Given an empty graph
    And having executed:
    """
    CREATE (a:A {prop: 1, listProp: []})-[r:Rel]->(b:B {prop: "STRING NOT NULL", listProp: [1]})
    """

  Scenario: Testing Simple Literals
    When executing query:
      """
      UNWIND ["abc", true, 1, 2.0, {a : 1}, [], date(), time(), localTime(), datetime(), localDatetime(), duration('P5M'),  point({x: 3, y: 0})] AS value
      RETURN valueType(value) AS valueType
      """
    Then the result should be, in any order:
      | valueType                 |
      | 'STRING NOT NULL'         |
      | 'BOOLEAN NOT NULL'        |
      | 'INTEGER NOT NULL'        |
      | 'FLOAT NOT NULL'          |
      | 'MAP NOT NULL'            |
      | 'LIST<NOTHING> NOT NULL'  |
      | 'DATE NOT NULL'           |
      | 'ZONED TIME NOT NULL'     |
      | 'LOCAL TIME NOT NULL'     |
      | 'ZONED DATETIME NOT NULL' |
      | 'LOCAL DATETIME NOT NULL' |
      | 'DURATION NOT NULL'       |
      | 'POINT NOT NULL'          |
    And no side effects

  Scenario: Testing Graph type values
    When executing query:
      """
      MATCH p = (a)-[r]->(b)
      RETURN valueType(p) AS pType, valueType(a) AS aType, valueType(r) AS rType
      """
    Then the result should be, in any order:
      | pType           | aType           | rType                   |
      | 'PATH NOT NULL' | 'NODE NOT NULL' | 'RELATIONSHIP NOT NULL' |
    And no side effects

  Scenario: Testing LIST type values
    When executing query:
      """
      UNWIND [[1], [2.0, 2], ["3", true], [], [[1, 2]], [null], [1, null]] AS value
      RETURN valueType(value) AS valueType
      """
    Then the result should be, in any order:
      | valueType                                            |
      | 'LIST<INTEGER NOT NULL> NOT NULL'                    |
      | 'LIST<INTEGER NOT NULL \| FLOAT NOT NULL> NOT NULL'  |
      | 'LIST<BOOLEAN NOT NULL \| STRING NOT NULL> NOT NULL' |
      | 'LIST<NOTHING> NOT NULL'                             |
      | 'LIST<LIST<INTEGER NOT NULL> NOT NULL> NOT NULL'     |
      | 'LIST<NULL> NOT NULL'                                |
      | 'LIST<INTEGER> NOT NULL'                             |

    And no side effects

  Scenario: Testing more complex LIST type value
    When executing query:
      """
      WITH [1, [], [2, [null]], [2.0, 2]] AS value
      RETURN valueType(value) AS valueType
      """
    Then the result should be, in any order:
      | valueType                                                                                                                                        |
      | 'LIST<INTEGER NOT NULL \| LIST<INTEGER NOT NULL \| FLOAT NOT NULL> NOT NULL \| LIST<INTEGER NOT NULL \| LIST<NULL> NOT NULL> NOT NULL> NOT NULL' |

    And no side effects

  Scenario: Testing more special type values
    When executing query:
      """
      UNWIND [null, [], 0/0.0, -1/0.0, 1/0.0] AS value
      RETURN valueType(value) AS valueType
      """
    Then the result should be, in any order:
      | valueType                |
      | 'NULL'                   |
      | 'LIST<NOTHING> NOT NULL' |
      | 'FLOAT NOT NULL'         |
      | 'FLOAT NOT NULL'         |
      | 'FLOAT NOT NULL'         |

    And no side effects

  Scenario: Inside an EXISTS
    When executing query:
      """
      RETURN EXISTS { RETURN valueType(date()) AS valueType } as exists
      """
    Then the result should be, in any order:
      | exists |
      | true   |

    And no side effects

  Scenario: Using the output of an EXISTS
    When executing query:
      """
      RETURN valueType(EXISTS { RETURN 1 }) as boolList
      """
    Then the result should be, in any order:
      | boolList           |
      | 'BOOLEAN NOT NULL' |

    And no side effects

  Scenario: Using the output of a COLLECT
    When executing query:
      """
      RETURN valueType(COLLECT { RETURN 1 }) as intList
      """
    Then the result should be, in any order:
      | intList                           |
      | 'LIST<INTEGER NOT NULL> NOT NULL' |

    And no side effects

  Scenario: Using the output of a COUNT
    When executing query:
      """
      RETURN valueType(COUNT { RETURN 1 }) as int
      """
    Then the result should be, in any order:
      | int                |
      | 'INTEGER NOT NULL' |

    And no side effects

  Scenario: Inside a CREATE statement
    When executing query:
      """
      CREATE (n {prop: valueType({map: 1})})
      RETURN n.prop
      """
    Then the result should be, in any order:
      | n.prop         |
      | 'MAP NOT NULL' |

    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 1 |

  Scenario: Inside a SET statement
    When executing query:
      """
      MATCH (n:A)
      SET n.prop1 = valueType(point({x: 3, y: 0}))
      RETURN n.prop1
      """
    Then the result should be, in any order:
      | n.prop1           |
      | 'POINT NOT NULL'  |

    And the side effects should be:
      | +properties | 1 |

  Scenario: As a filter statement
    When executing query:
      """
      MATCH (n {prop: valueType("")})
      RETURN n.prop
      """
    Then the result should be, in any order:
      | n.prop             |
      | 'STRING NOT NULL'  |

    And no side effects

  Scenario: Aggregation of COLLECTED properties
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {prop: 1})-[r1:Rel]->(b:B {prop: "STRING NOT NULL"})
      CREATE (c:A)-[r2:Rel]->(d:B {prop: 2.0})
      """
    When executing query:
      """
      RETURN valueType(COLLECT {
        MATCH (n)
        RETURN n.prop
      }) as propTypes
      """
    Then the result should be, in any order:
      | propTypes                                   |
      | 'LIST<STRING \| INTEGER \| FLOAT> NOT NULL' |

    And no side effects

  Scenario: Inside a case statement
    Given an empty graph
    When executing query:
      """
      UNWIND [1, "1", 1.0, true, []] as value
      RETURN value, CASE
          WHEN valueType(value) = "STRING NOT NULL" THEN "I am a string"
          WHEN valueType(value) = "INTEGER NOT NULL" THEN "I am an int"
          WHEN valueType(value) = "FLOAT NOT NULL" THEN "I am a float"
          WHEN valueType(value) = "BOOLEAN NOT NULL" THEN "I am a bool"
          ELSE "I am something else!"
       END
       AS result
      """
    Then the result should be, in any order:
      | value | result                 |
      | 1     | 'I am an int'          |
      | '1'   | 'I am a string'        |
      | 1.0   | 'I am a float'         |
      | true  | 'I am a bool'          |
      | []    | 'I am something else!' |

    And no side effects

  Scenario: Test stored list values
    Given an empty graph
    When executing query:
      """
      MATCH (n)
      RETURN valueType(n.listProp) AS result
      """
    Then the result should be, in any order:
      | result                            |
      | 'LIST<NOTHING> NOT NULL'          |
      | 'LIST<INTEGER NOT NULL> NOT NULL' |

    And no side effects