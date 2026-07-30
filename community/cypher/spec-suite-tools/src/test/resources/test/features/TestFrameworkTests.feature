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

# Tests to make sure it's possible to fail this kind of test.
Feature: TestFrameworkTests


  Scenario: [001] Incorrect result value ordered
    Given an empty graph
    When executing query:
      """
      RETURN 2 AS res
      """
    Then the result should be, in order:
      | res     |
      | 'wrong' |

  Scenario: [002] Incorrect result value any order
    Given an empty graph
    When executing query:
      """
      RETURN 2 AS res
      """
    Then the result should be, in any order:
      | res     |
      | 'wrong' |

  Scenario Outline: [003] Incorrect result value ordered, any list order
    Given an empty graph
    When executing query:
      """
      UNWIND [1,2,3] AS x WITH x, rand() AS order ORDER BY order RETURN collect(x) AS res
      """
    Then the result should be, in order (ignoring element order for lists):
      | res   |
      | <res> |
    Examples:
      | res       |
      | [1,2,2]   |
      | [1,2]     |
      | [1,2,3,4] |

  Scenario Outline: [004] Incorrect result value any order, any list order
    Given an empty graph
    When executing query:
      """
      UNWIND [1,2,3] AS x WITH x,  rand() AS order ORDER BY order RETURN collect(x) AS res
      """
    Then the result should be (ignoring element order for lists):
      | res   |
      | <res> |
    Examples:
      | res       |
      | [1,2,2]   |
      | [1,2]     |
      | [1,2,3,4] |

  Scenario: [005] Incorrect row count ordered
    Given an empty graph
    When executing query:
      """
      RETURN 1 AS res
      """
    Then the result should be, in order:
      | res |
      | 1   |
      | 1   |

  Scenario: [006] Incorrect row count any order
    Given an empty graph
    When executing query:
      """
      RETURN 1 AS res
      """
    Then the result should be, in any order:
      | res |
      | 1   |
      | 1   |

  Scenario: [007] Incorrect row count ordered, any list order
    Given an empty graph
    When executing query:
      """
      RETURN [1] AS res
      """
    Then the result should be, in order (ignoring element order for lists):
      | res |
      | [1] |
      | [1] |

  Scenario: [008] Incorrect row count any order, any list order
    Given an empty graph
    When executing query:
      """
      RETURN [1] AS res
      """
    Then the result should be (ignoring element order for lists):
      | res |
      | [1] |
      | [1] |

  Scenario: [009] Incorrect result headers ordered
    Given an empty graph
    When executing query:
      """
      RETURN 1 AS res
      """
    Then the result should be, in order:
      | wrong |
      | 1     |

  Scenario: [010] Incorrect result headers any order
    Given an empty graph
    When executing query:
      """
      RETURN 1 AS res
      """
    Then the result should be, in any order:
      | wrong |
      | 1     |

  Scenario: [011] Incorrect result headers ordered, any list order
    Given an empty graph
    When executing query:
      """
      RETURN [1] AS res
      """
    Then the result should be, in order (ignoring element order for lists):
      | wrong |
      | [1]   |

  Scenario: [012] Incorrect result headers any order, any list order
    Given an empty graph
    When executing query:
      """
      RETURN [1] AS res
      """
    Then the result should be (ignoring element order for lists):
      | wrong |
      | [1]   |

  Scenario Outline: [013] Incorrect side effects
    Given an empty graph
    And having executed:
      """
      CREATE (:A {p:1})-[r:A {p:1}]->(:B {p:1})
      CREATE (:C {p:1})
      """
    When executing query:
      """
      <query>
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 0 |
      | -nodes         | 0 |
      | +relationships | 0 |
      | -relationships | 0 |
      | +labels        | 0 |
      | -labels        | 0 |
      | +properties    | 0 |
      | -properties    | 0 |
    Examples:
      | query                                            |
      | CREATE (:A {p:1})-[r:A {p:1}]->(:B {p:1})        |
      | CREATE ()                                        |
      | CREATE (:C)                                      |
      | MATCH (c:C) DELETE c                             |
      | MATCH (a:A) SET a.p = '1'                        |
      | MATCH (a:A) SET a.a = 1                          |
      | MATCH (a:A) SET a:AAA                            |
      | MATCH (a:A) REMOVE a.p                           |
      | MATCH (a:A), (b:B) CREATE (a)-[r:RRR]->(b)       |
      | MATCH (a:A), (b:B) CREATE (a)-[r:RRR {p:1}]->(b) |
      | MATCH ()-[r]->() DELETE r                        |
      | MATCH ()-[r]->() SET r.p = '1'                   |
      | MATCH ()-[r]->() SET r.a = '1'                   |
      | MATCH ()-[r]->() REMOVE r.p                      |

  Scenario Outline: [014] Query failure
    Given an empty graph
    When executing query:
      """
      <query>
      """
    Then the result should be empty
    Examples:
      | query                            |
      | INVALID QUERY                    |
      | UNWIND [1,2] AS x RETURN 1/(x-2) |

  Scenario: [015] Incorrect config
    Given an empty graph
    When executing query:
      """
      call dbms.listConfig('dbms.security.procedures.unrestricted') yield name, value
      """
    Then the result should be, in order:
      | name                                    | value |
      | 'dbms.security.procedures.unrestricted' | 'yo'  |


  Scenario Outline: [016] Most types work in cucumber tests
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {pv: <cypher>, pl: [<cypher>]})
      CREATE (b:B)
      CREATE (a)-[:R {pv: <cypher>, pl: [<cypher>]}]->(b)
      """
    When executing query:
      """
      MATCH (n:A)-[r:R]->() RETURN n, r
      """
    Then the result should be, in order:
      | n                                       | r                                       |
      | (:A {pv: <cucumber>, pl: [<cucumber>]}) | [:R {pv: <cucumber>, pl: [<cucumber>]}] |
    When executing query:
      """
      MATCH (n:A)-[r:R]->() RETURN n.pv, n.pl, r.pv, r.pl
      """
    Then the result should be, in order:
      | n.pv       | n.pl         | r.pv       | r.pl         |
      | <cucumber> | [<cucumber>] | <cucumber> | [<cucumber>] |
    When executing query:
      """
      UNWIND [<cypher>, [<cypher>], [[<cypher>], <cypher>], {k:<cypher>}, {k:{k:<cypher>}}] AS v
      WITH *, rand() AS order
      RETURN v
      ORDER BY order
      """
    Then the result should be, in any order:
      | v                          |
      | <cucumber>                 |
      | [<cucumber>]               |
      | [[<cucumber>], <cucumber>] |
      | {k: <cucumber>}            |
      | {k: {k: <cucumber>}}       |
    Examples:
      | cypher                                                                                                                                  | cucumber                              |
      | ''                                                                                                                                      | ''                                    |
      | '  '                                                                                                                                    | '  '                                  |
      | '\\u2028'                                                                                                                               | ' '                                |
      | '1'                                                                                                                                     | '1'                                   |
      | -1                                                                                                                                      | -1                                    |
      | 0                                                                                                                                       | 0                                     |
      | 1                                                                                                                                       | 1                                     |
      | 9223372036854775807                                                                                                                     | 9223372036854775807                   |
      | -9223372036854775808                                                                                                                    | -9223372036854775808                  |
      | 1.0                                                                                                                                     | 1.0                                   |
      | 0.0                                                                                                                                     | 0.0                                   |
      | -1.0                                                                                                                                    | -1.0                                  |
      | -1.7976931348623157E308                                                                                                                 | -1.7976931348623157E308               |
      | 1.7976931348623157E308                                                                                                                  | 1.7976931348623157E308                |
      | false                                                                                                                                   | false                                 |
      | date({year: 1, month: 1, day: 1})                                                                                                       | '0001-01-01'                          |
      | datetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, millisecond: 1, microsecond: 1, nanosecond: 1, timezone: '+01:00'}) | '0001-01-01T01:01:01.001001001+01:00' |
      | time({hour: 1, minute: 1, second: 1, millisecond: 1, microsecond: 1, nanosecond: 1, timezone: '+01:00'})                                | '01:01:01.001001001+01:00'            |
      | localdatetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, millisecond: 1, microsecond: 1, nanosecond: 1})                | '0001-01-01T01:01:01.001001001'       |
      | localtime({hour: 1, minute: 1, second: 1, millisecond: 1, microsecond: 1, nanosecond: 1})                                               | '01:01:01.001001001'                  |
      | duration({years: 1, months: 1, weeks: 1, days: 1, hours: 1, minutes: 1, seconds: 1, milliseconds: 1, microseconds: 1, nanoseconds: 1})  | 'P1Y1M8DT1H1M1.001001001S'            |


  Scenario Outline: [017] Most storable types can fail in cucumber tests
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {p: <cypher>})
      """
    When executing query:
      """
      MATCH (n:A) RETURN n.p
      """
    Then the result should be, in order:
      | n.p        |
      | <cucumber> |
    Examples:
      | cypher                                                                                                                                  | cucumber                              |
      | ''                                                                                                                                      | ' '                                   |
      | '  '                                                                                                                                    | ' '                                   |
      | '\\u2028'                                                                                                                               | '\n'                                  |
      | '1'                                                                                                                                     | '0'                                   |
      | -1                                                                                                                                      | 1                                     |
      | 0                                                                                                                                       | 1                                     |
      | 1                                                                                                                                       | 2                                     |
      | 9223372036854775807                                                                                                                     | 9223372036854775806                   |
      | -9223372036854775808                                                                                                                    | -9223372036854775807                  |
      | 1.0                                                                                                                                     | 1.01                                  |
      | 0.0                                                                                                                                     | 0.01                                  |
      | -1.0                                                                                                                                    | 1.0                                   |
      | -1.7976931348623157E308                                                                                                                 | -1.7976931348623157                   |
      | 1.7976931348623157E308                                                                                                                  | 1.7976931348623157                    |
      | false                                                                                                                                   | true                                  |
      | date({year: 1, month: 1, day: 1})                                                                                                       | '0001-01-02'                          |
      | datetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, millisecond: 1, microsecond: 1, nanosecond: 1, timezone: '+01:00'}) | '0001-01-01T01:01:01.001001001+02:00' |
      | datetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, millisecond: 1, microsecond: 1, nanosecond: 1, timezone: '+01:00'}) | '0001-01-01T01:01:01.001001002+01:00' |
      | time({hour: 1, minute: 1, second: 1, millisecond: 1, microsecond: 1, nanosecond: 1, timezone: '+01:00'})                                | '01:01:01.001001001+02:00'            |
      | time({hour: 1, minute: 1, second: 1, millisecond: 1, microsecond: 1, nanosecond: 1, timezone: '+01:00'})                                | '01:01:01.001001002+01:00'            |
      | localdatetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, millisecond: 1, microsecond: 1, nanosecond: 1})                | '0001-01-01T01:01:01.001001002'       |
      | localtime({hour: 1, minute: 1, second: 1, millisecond: 1, microsecond: 1, nanosecond: 1})                                               | '01:01:01.001001002'                  |
      | duration({years: 1, months: 1, weeks: 1, days: 1, hours: 1, minutes: 1, seconds: 1, milliseconds: 1, microseconds: 1, nanoseconds: 1})  | 'P1Y1M8DT1H1M1.001001002S'            |


  Scenario Outline: [018] Most types can fail in cucumber tests
    Given an empty graph
    When executing query:
      """
      RETURN <cypher> AS value
      """
    Then the result should be, in order:
      | value      |
      | <cucumber> |
    Examples:
      | cypher                                                                                                                                  | cucumber                              |
      | ''                                                                                                                                      | ' '                                   |
      | '  '                                                                                                                                    | ' '                                   |
      | '\\u2028'                                                                                                                               | '\n'                                  |
      | '1'                                                                                                                                     | '0'                                   |
      | -1                                                                                                                                      | 1                                     |
      | 0                                                                                                                                       | 1                                     |
      | 1                                                                                                                                       | 2                                     |
      | 9223372036854775807                                                                                                                     | 9223372036854775806                   |
      | -9223372036854775808                                                                                                                    | -9223372036854775807                  |
      | 1.0                                                                                                                                     | 1.01                                  |
      | 0.0                                                                                                                                     | 0.01                                  |
      | -1.0                                                                                                                                    | 1.0                                   |
      | -1.7976931348623157E308                                                                                                                 | -1.7976931348623157                   |
      | 1.7976931348623157E308                                                                                                                  | 1.7976931348623157                    |
      | false                                                                                                                                   | true                                  |
      | date({year: 1, month: 1, day: 1})                                                                                                       | '0001-01-02'                          |
      | datetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, millisecond: 1, microsecond: 1, nanosecond: 1, timezone: '+01:00'}) | '0001-01-01T01:01:01.001001001+02:00' |
      | datetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, millisecond: 1, microsecond: 1, nanosecond: 1, timezone: '+01:00'}) | '0001-01-01T01:01:01.001001002+01:00' |
      | time({hour: 1, minute: 1, second: 1, millisecond: 1, microsecond: 1, nanosecond: 1, timezone: '+01:00'})                                | '01:01:01.001001001+02:00'            |
      | time({hour: 1, minute: 1, second: 1, millisecond: 1, microsecond: 1, nanosecond: 1, timezone: '+01:00'})                                | '01:01:01.001001002+01:00'            |
      | localdatetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1, millisecond: 1, microsecond: 1, nanosecond: 1})                | '0001-01-01T01:01:01.001001002'       |
      | localtime({hour: 1, minute: 1, second: 1, millisecond: 1, microsecond: 1, nanosecond: 1})                                               | '01:01:01.001001002'                  |
      | duration({years: 1, months: 1, weeks: 1, days: 1, hours: 1, minutes: 1, seconds: 1, milliseconds: 1, microseconds: 1, nanoseconds: 1})  | 'P1Y1M8DT1H1M1.001001002S'            |

  @conf:incorrect.conf=true
  Scenario: [019] Incorrect conf tag
    Given an empty graph
    When executing query:
      """
      RETURN 1 AS res
      """
    Then the result should be, in order:
      | res |
      | 1   |

  Scenario: [023] Open tx: Incorrect result value ordered
    Given an empty graph
    Given an open transaction
    When executing query, in open tx:
      """
      RETURN 2 AS res
      """
    Then the result should be, in order:
      | res     |
      | 'wrong' |

  Scenario: [024] Open tx: Incorrect result value any order
    Given an empty graph
    Given an open transaction
    When executing query, in open tx:
      """
      RETURN 2 AS res
      """
    Then the result should be, in any order:
      | res     |
      | 'wrong' |

  Scenario Outline: [025] Open tx: Incorrect result value ordered, any list order
    Given an empty graph
    Given an open transaction
    When executing query, in open tx:
      """
      UNWIND [1,2,3] AS x WITH x, rand() AS order ORDER BY order RETURN collect(x) AS res
      """
    Then the result should be, in order (ignoring element order for lists):
      | res   |
      | <res> |
    Examples:
      | res       |
      | [1,2,2]   |
      | [1,2]     |
      | [1,2,3,4] |

  Scenario Outline: [026] Open tx: Incorrect result value any order, any list order
    Given an empty graph
    Given an open transaction
    When executing query, in open tx:
      """
      UNWIND [1,2,3] AS x WITH x,  rand() AS order ORDER BY order RETURN collect(x) AS res
      """
    Then the result should be (ignoring element order for lists):
      | res   |
      | <res> |
    Examples:
      | res       |
      | [1,2,2]   |
      | [1,2]     |
      | [1,2,3,4] |

  Scenario: [027] Open tx: Incorrect row count ordered
    Given an empty graph
    Given an open transaction
    When executing query, in open tx:
      """
      RETURN 1 AS res
      """
    Then the result should be, in order:
      | res |
      | 1   |
      | 1   |

  Scenario: [028] Open tx: Incorrect row count any order
    Given an empty graph
    Given an open transaction
    When executing query, in open tx:
      """
      RETURN 1 AS res
      """
    Then the result should be, in any order:
      | res |
      | 1   |
      | 1   |

  Scenario: [029] Open tx: Incorrect row count ordered, any list order
    Given an empty graph
    Given an open transaction
    When executing query, in open tx:
      """
      RETURN [1] AS res
      """
    Then the result should be, in order (ignoring element order for lists):
      | res |
      | [1] |
      | [1] |

  Scenario: [030] Open tx: Incorrect row count any order, any list order
    Given an empty graph
    Given an open transaction
    When executing query, in open tx:
      """
      RETURN [1] AS res
      """
    Then the result should be (ignoring element order for lists):
      | res |
      | [1] |
      | [1] |

  Scenario: [031] Open tx: Incorrect result headers ordered
    Given an empty graph
    Given an open transaction
    When executing query, in open tx:
      """
      RETURN 1 AS res
      """
    Then the result should be, in order:
      | wrong |
      | 1     |

  Scenario: [032] Open tx: Incorrect result headers any order
    Given an empty graph
    Given an open transaction
    When executing query, in open tx:
      """
      RETURN 1 AS res
      """
    Then the result should be, in any order:
      | wrong |
      | 1     |

  Scenario: [033] Open tx: Incorrect result headers ordered, any list order
    Given an empty graph
    Given an open transaction
    When executing query, in open tx:
      """
      RETURN [1] AS res
      """
    Then the result should be, in order (ignoring element order for lists):
      | wrong |
      | [1]   |

  Scenario: [034] Open tx: Incorrect result headers any order, any list order
    Given an empty graph
    Given an open transaction
    When executing query, in open tx:
      """
      RETURN [1] AS res
      """
    Then the result should be (ignoring element order for lists):
      | wrong |
      | [1]   |

  Scenario Outline: [035] Open tx: Incorrect side effects
    Given an empty graph
    Given an open transaction
    And having executed, in open tx:
      """
      CREATE (:A {p:1})-[r:A {p:1}]->(:B {p:1})
      CREATE (:C {p:1})
      """
    When executing query, in open tx:
      """
      <query>
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 0 |
      | -nodes         | 0 |
      | +relationships | 0 |
      | -relationships | 0 |
      | +labels        | 0 |
      | -labels        | 0 |
      | +properties    | 0 |
      | -properties    | 0 |
    Examples:
      | query                                            |
      | CREATE (:A {p:1})-[r:A {p:1}]->(:B {p:1})        |
      | CREATE ()                                        |
      | CREATE (:C)                                      |
      | MATCH (c:C) DELETE c                             |
      | MATCH (a:A) SET a.p = '1'                        |
      | MATCH (a:A) SET a.a = 1                          |
      | MATCH (a:A) SET a:AAA                            |
      | MATCH (a:A) REMOVE a.p                           |
      | MATCH (a:A), (b:B) CREATE (a)-[r:RRR]->(b)       |
      | MATCH (a:A), (b:B) CREATE (a)-[r:RRR {p:1}]->(b) |
      | MATCH ()-[r]->() DELETE r                        |
      | MATCH ()-[r]->() SET r.p = '1'                   |
      | MATCH ()-[r]->() SET r.a = '1'                   |
      | MATCH ()-[r]->() REMOVE r.p                      |

  Scenario Outline: [036] Open tx: Query failure
    Given an empty graph
    Given an open transaction
    When executing query, in open tx:
      """
      <query>
      """
    Then the result should be empty
    Examples:
      | query                            |
      | INVALID QUERY                    |
      | UNWIND [1,2] AS x RETURN 1/(x-2) |

  Scenario: [037] Warning has incorrect code
    Given an empty graph
    When executing query:
      """
      RETURN id(null) AS id
      """
    Then the result should be (ignoring element order for lists):
      | id   |
      | null |
    And notifications should be raised:
      | code      | description |
      | 123TECHNO | ${regex:.*} |

  Scenario: [038] Warning has incorrect code and correct message
    Given an empty graph
    When executing query:
      """
      RETURN id(null) AS id
      """
    Then the result should be (ignoring element order for lists):
      | id   |
      | null |
    And notifications should be raised:
      | code      | description                                                  |
      | 123TECHNO | warn: feature deprecated with replacement. id is deprecated. |

  Scenario: [039] Warning has correct code and incorrect message
    Given an empty graph
    When executing query:
      """
      RETURN id(null) AS id
      """
    Then the result should be (ignoring element order for lists):
      | id   |
      | null |
    And notifications should be raised:
      | code  | description       |
      | 01N01 | Incorrect message |

  Scenario: [040] Warning has correct code
    Given an empty graph
    When executing query:
      """
      RETURN id(null) AS id
      """
    Then the result should be (ignoring element order for lists):
      | id   |
      | null |
    And notifications should be raised:
      | code  | description |
      | 01N01 | ${regex:.*} |

  Scenario: [041] Warning has correct code and correct message
    Given an empty graph
    When executing query:
      """
      RETURN id(null) AS id
      """
    Then the result should be (ignoring element order for lists):
      | id   |
      | null |
    And notifications should be raised:
      | code  | description                                                                                                                             |
      | 01N01 | warn: feature deprecated with replacement. id is deprecated. It is replaced by elementId or consider using an application-generated id. |

  Scenario Outline: [042] Floating point precision can be specified
    Given an empty graph
    When executing query:
        """
        RETURN <value> AS result
        """
    Then the result should be, in order, to within 0.0001:
      | result |
      | 1.0    |
    Examples:
      | value    |
      | 0.99999  |
      | 0.999995 |
      | 1.0      |
      | 1.00001  |

  Scenario Outline: [043] Floating point precision is exact by default
    Given an empty graph
    When executing query:
        """
        RETURN <value> AS result
        """
    Then the result should be, in order:
      | result |
      | 1.0    |
    Examples:
      | value    |
      | 0.99999  |
      | 0.999995 |
      | 1.00001  |

  Scenario: [044] Syntax error is correct
    Given an empty graph
    When executing query:
      """
      RETURN x
      """
    Then an error should be raised:
      | code  | classification | description                                                                                    |
      | 42001 | CLIENT_ERROR   | error: syntax error or access rule violation - invalid syntax                                  |
      | 42N62 | CLIENT_ERROR   | error: syntax error or access rule violation - variable not defined. Variable `x` not defined. |

  Scenario: [045] Syntax error has incorrect code 1
    Given an empty graph
    When executing query:
      """
      RETURN x
      """
    Then an error should be raised:
      | code  | classification | description                                                                                    |
      | 42001 | CLIENT_ERROR   | error: syntax error or access rule violation - invalid syntax                                  |
      | WRONG | CLIENT_ERROR   | error: syntax error or access rule violation - variable not defined. Variable `x` not defined. |

  Scenario: [046] Syntax error has incorrect code 2
    Given an empty graph
    When executing query:
      """
      RETURN x
      """
    Then an error should be raised:
      | code  | classification | description                                                                                    |
      | WRONG | CLIENT_ERROR   | error: syntax error or access rule violation - invalid syntax                                  |
      | 42N62 | CLIENT_ERROR   | error: syntax error or access rule violation - variable not defined. Variable `x` not defined. |

  Scenario: [047] Syntax error has incorrect classification 1
    Given an empty graph
    When executing query:
      """
      RETURN x
      """
    Then an error should be raised:
      | code  | classification | description                                                                                    |
      | 42001 | CLIENT_ERROR   | error: syntax error or access rule violation - invalid syntax                                  |
      | 42N62 | WRONG          | error: syntax error or access rule violation - variable not defined. Variable `x` not defined. |

  Scenario: [048] Syntax error has incorrect classification 2
    Given an empty graph
    When executing query:
      """
      RETURN x
      """
    Then an error should be raised:
      | code  | classification | description                                                                                    |
      | 42001 | WRONG          | error: syntax error or access rule violation - invalid syntax                                  |
      | 42N62 | CLIENT_ERROR   | error: syntax error or access rule violation - variable not defined. Variable `x` not defined. |

  Scenario: [049] Syntax error has incorrect message 1
    Given an empty graph
    When executing query:
      """
      RETURN x
      """
    Then an error should be raised:
      | code  | classification | description                                                                                    |
      | 42001 | CLIENT_ERROR   | error: syntax error or access rule violation                                                   |
      | 42N62 | CLIENT_ERROR   | error: syntax error or access rule violation - variable not defined. Variable `x` not defined. |

  Scenario: [050] Syntax error has incorrect message 3
    Given an empty graph
    When executing query:
      """
      RETURN x
      """
    Then an error should be raised:
      | code  | classification | description                                                   |
      | 42001 | CLIENT_ERROR   | error: syntax error or access rule violation - invalid syntax |
      | 42N62 | CLIENT_ERROR   | error: syntax error or access rule violation                  |

  Scenario: [051] Syntax error has incorrect message 4
    Given an empty graph
    When executing query:
      """
      RETURN x
      """
    Then an error should be raised:
      | code  | classification | description                                                   |
      | 42001 | CLIENT_ERROR   | error: syntax error or access rule violation - invalid syntax |
      | 42N62 | CLIENT_ERROR   | error: syntax error or access rule WRONG ${regex:.*}          |

  Scenario: [052] Syntax error has incorrect error count
    Given an empty graph
    When executing query:
      """
      RETURN x
      """
    Then an error should be raised:
      | code  | classification | description                                                                                    |
      | 42001 | CLIENT_ERROR   | error: syntax error or access rule violation - invalid syntax                                  |
      | 42N62 | CLIENT_ERROR   | error: syntax error or access rule violation - variable not defined. Variable `x` not defined. |
      | 42N62 | CLIENT_ERROR   | error: syntax error or access rule violation - variable not defined. Variable `x` not defined. |

  Scenario: [053] Approximate result - exact match
    Given  an empty graph
    When executing query:
      """
      UNWIND [42, 38] as x
      RETURN x
      """
    Then the approximate result should be 2 rows, in order:
      | x  | mandatory |
      | 42 | true      |
      | 38 | false     |

  Scenario: [054] Approximate result - without optional row
    Given  an empty graph
    When executing query:
      """
      UNWIND [42] as x
      RETURN x
      """
    Then the approximate result should be 1 rows, in order:
      | x  | mandatory |
      | 42 | true      |
      | 38 | false     |

  Scenario: [055] Approximate result - without mandatory row
    Given  an empty graph
    When executing query:
      """
      UNWIND [42] as x
      RETURN x
      """
    Then the approximate result should be 1 rows, in order:
      | x  | mandatory |
      | 42 | true      |
      | 38 | true      |

  Scenario: [056] Approximate result - with extra row
    Given  an empty graph
    When executing query:
      """
      UNWIND [42, 27, 38] as x
      RETURN x
      """
    Then the approximate result should be 3 rows, in order:
      | x  | mandatory |
      | 42 | true      |
      | 38 | false     |

  Scenario: [057] Approximate result - with wrong order
    Given  an empty graph
    When executing query:
      """
      UNWIND [42, 27, 38] as x
      RETURN x
      """
    Then the approximate result should be 3 rows, in order:
      | x  | mandatory |
      | 42 | true      |
      | 38 | false     |
      | 27 | true      |
      | 21 | false     |

  Scenario: [058] Approximate result - with wrong number of results
    Given  an empty graph
    When executing query:
      """
      UNWIND [42, 38, 27] as x
      RETURN x
      """
    Then the approximate result should be 2 rows, in order:
      | x  | mandatory |
      | 42 | true      |
      | 38 | false     |
      | 27 | true      |
      | 21 | false     |


  Scenario: [059] Approximate result - without mandatory column
    Given  an empty graph
    When executing query:
      """
      UNWIND [true, false] as x
      RETURN x
      """
    Then the approximate result should be 2 rows, in order:
      | x     |
      | true  |
      | false |

  Scenario: [060] Approximate result - with wrongly named mandatory column
    Given  an empty graph
    When executing query:
      """
      UNWIND [42, 38] as x
      RETURN x
      """
    Then the approximate result should be 2 rows, in order:
      | x  | optional |
      | 42 | true     |
      | 38 | false    |

  Scenario: [061] Approximate result - with wrong type in mandatory column
    Given  an empty graph
    When executing query:
      """
      UNWIND [42, 38] as x
      RETURN x
      """
    Then the approximate result should be 2 rows, in order:
      | x  | mandatory |
      | 42 | true      |
      | 38 | 'false'   |

  Scenario: [062] Query has warnings but asserts on no warnings
    Given an empty graph
    When executing query:
      """
      RETURN id(null) AS id
      """
    Then the result should be (ignoring element order for lists):
      | id   |
      | null |
    And no notifications should be raised

  Scenario: [063] Query should not fail - success ignores rows and headers
    Given an empty graph
    When executing query:
      """
      UNWIND [1, 2] AS x
      RETURN x
      """
    Then the query should not fail

  Scenario: [064] Query should not fail - failure
    Given an empty graph
    When executing query:
      """
      UNWIND [1,2] AS x RETURN 1/(x-2)
      """
    Then the query should not fail
