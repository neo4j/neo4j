/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.ast.factory.neo4j.privilege

import org.neo4j.cypher.internal.ast.factory.neo4j.ParserComparisonTestBase
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalatest.FunSuiteLike

class ShowPrivilegesAdministrationCommandJavaccParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName {

  // Show privileges

  test("SHOW PRIVILEGES") {
    assertSameAST(testName)
  }

  test("SHOW PRIVILEGE") {
    assertSameAST(testName)
  }

  test("use system show privileges") {
    assertSameAST(testName)
  }

  test("SHOW ALL PRIVILEGES") {
    assertSameAST(testName)
  }

  // Show role privileges

  test("SHOW ROLE role PRIVILEGES") {
    assertSameAST(testName)
  }

  test("SHOW ROLE role PRIVILEGE") {
    assertSameAST(testName)
  }

  test("SHOW ROLE $role PRIVILEGES") {
    assertSameAST(testName)
  }

  test("SHOW ROLES `ro%le` PRIVILEGES") {
    assertSameAST(testName)
  }

  test("SHOW ROLE role1, $roleParam, role2, role3 PRIVILEGES") {
    assertSameAST(testName)
  }

  test("SHOW ROLES role1, $roleParam1, role2, $roleParam2 PRIVILEGES") {
    assertSameAST(testName)
  }

  test("SHOW ROLES privilege PRIVILEGE") {
    assertSameAST(testName)
  }

  test("SHOW ROLE privilege, privileges PRIVILEGES") {
    assertSameAST(testName)
  }

  test(s"SHOW ROLES yield, where PRIVILEGES") {
    assertSameAST(testName)
  }

  test(s"SHOW ROLES with PRIVILEGES") {
    assertSameAST(testName)
  }

  // Show user privileges

  test("SHOW USER user PRIVILEGES") {
    assertSameAST(testName)
  }

  test("SHOW USERS $user PRIVILEGES") {
    assertSameAST(testName)
  }

  test("SHOW USER `us%er` PRIVILEGES") {
    assertSameAST(testName)
  }

  test("SHOW USER user, $user PRIVILEGES") {
    assertSameAST(testName)
  }

  test("SHOW USER user, $user PRIVILEGE") {
    assertSameAST(testName)
  }

  test("SHOW USERS user1, $user, user2 PRIVILEGES") {
    assertSameAST(testName)
  }

  test("SHOW USER PRIVILEGES") {
    assertSameAST(testName)
  }

  test("SHOW USERS PRIVILEGES") {
    assertSameAST(testName)
  }

  test("SHOW USER privilege PRIVILEGE") {
    assertSameAST(testName)
  }

  test("SHOW USER privilege, privileges PRIVILEGES") {
    assertSameAST(testName)
  }

  test(s"SHOW USER defined PRIVILEGES") {
    assertSameAST(testName)
  }

  test(s"SHOW USERS yield, where PRIVILEGES") {
    assertSameAST(testName)
  }

  // Show privileges as commands

  test("SHOW PRIVILEGES AS COMMAND") {
    assertSameAST(testName)
  }

  test("SHOW PRIVILEGES AS COMMANDS") {
    assertSameAST(testName)
  }

  test("SHOW PRIVILEGES AS REVOKE COMMAND") {
    assertSameAST(testName)
  }

  test("SHOW PRIVILEGES AS REVOKE COMMANDS") {
    assertSameAST(testName)
  }

  test("SHOW ALL PRIVILEGES AS COMMAND") {
    assertSameAST(testName)
  }

  test("SHOW ALL PRIVILEGE AS COMMAND") {
    assertSameAST(testName)
  }

  test("SHOW ALL PRIVILEGES AS REVOKE COMMANDS") {
    assertSameAST(testName)
  }

  test("SHOW USER user PRIVILEGES AS COMMANDS") {
    assertSameAST(testName)
  }

  test("SHOW USERS $user PRIVILEGES AS REVOKE COMMAND") {
    assertSameAST(testName)
  }

  test("SHOW USER `us%er` PRIVILEGES AS COMMANDS") {
    assertSameAST(testName)
  }

  test("SHOW USER `us%er` PRIVILEGE AS COMMANDS") {
    assertSameAST(testName)
  }

  test("SHOW USER user, $user PRIVILEGES AS REVOKE COMMANDS") {
    assertSameAST(testName)
  }

  test("SHOW USER PRIVILEGES AS COMMAND") {
    assertSameAST(testName)
  }

  test("SHOW USERS PRIVILEGES AS REVOKE COMMANDS") {
    assertSameAST(testName)
  }

  test("SHOW USERS PRIVILEGE AS REVOKE COMMANDS") {
    assertSameAST(testName)
  }

  test("SHOW ROLE role PRIVILEGES AS COMMANDS") {
    assertSameAST(testName)
  }

  test("SHOW ROLE role PRIVILEGE AS COMMANDS") {
    assertSameAST(testName)
  }

  test("SHOW ROLE $role PRIVILEGES AS REVOKE COMMAND") {
    assertSameAST(testName)
  }

  // yield / skip / limit / order by / where

    Seq(
    " AS COMMANDS",
    " AS REVOKE COMMANDS",
    ""
  ).foreach { optionalAsRev: String =>
      Seq(
        "",
        "ALL",
        "USER",
        "USER neo4j",
        "USERS neo4j, $user",
        "ROLES $role",
        "ROLE $role, reader"
      ).foreach { privType =>
        Seq(
          "PRIVILEGE",
          "PRIVILEGES"
        ).foreach { privilegeOrPrivileges =>
          test(s"SHOW $privType $privilegeOrPrivileges$optionalAsRev WHERE access = 'GRANTED'") {
            assertSameAST(testName)
          }

          test(s"SHOW $privType $privilegeOrPrivileges$optionalAsRev WHERE access = 'GRANTED' AND action = 'match'") {
            assertSameAST(testName)
          }

          test(s"SHOW $privType $privilegeOrPrivileges$optionalAsRev YIELD access ORDER BY access") {
            assertSameAST(testName)
          }

          test(s"SHOW $privType $privilegeOrPrivileges$optionalAsRev YIELD access ORDER BY access WHERE access ='none'") {
            assertSameAST(testName)
          }

          test(s"SHOW $privType $privilegeOrPrivileges$optionalAsRev YIELD access ORDER BY access SKIP 1 LIMIT 10 WHERE access ='none'") {
            assertSameAST(testName)
          }

          test(s"SHOW $privType $privilegeOrPrivileges$optionalAsRev YIELD access SKIP -1") {
            assertSameAST(testName)
          }

          test(s"SHOW $privType $privilegeOrPrivileges$optionalAsRev YIELD access, action RETURN access, count(action) ORDER BY access") {
            assertSameAST(testName)
          }

          test(s"SHOW $privType $privilegeOrPrivileges$optionalAsRev YIELD access, action SKIP 1 RETURN access, action") {
            assertSameAST(testName)
          }

          test(s"SHOW $privType $privilegeOrPrivileges$optionalAsRev YIELD access, action WHERE access = 'none' RETURN action") {
            assertSameAST(testName)
          }

          test(s"SHOW $privType $privilegeOrPrivileges$optionalAsRev YIELD * RETURN *") {
            assertSameAST(testName)
          }
        }
      }

      // yield and where edge cases

      Seq(
        "USER",
        "USERS",
        "ROLE",
        "ROLES"
      ).foreach { privType =>
        test(s"SHOW $privType yield PRIVILEGES$optionalAsRev YIELD access RETURN *") {
          assertSameAST(testName)
        }

        test(s"SHOW $privType yield, where PRIVILEGES$optionalAsRev YIELD access RETURN *") {
          assertSameAST(testName)
        }

        test(s"SHOW $privType where PRIVILEGE$optionalAsRev WHERE access = 'none'") {
          assertSameAST(testName)
        }

        test(s"SHOW $privType privilege PRIVILEGE$optionalAsRev YIELD access RETURN *") {
          assertSameAST(testName)
        }

        test(s"SHOW $privType privileges PRIVILEGES$optionalAsRev YIELD access RETURN *") {
          assertSameAST(testName)
        }

        test(s"SHOW $privType privilege, privileges PRIVILEGES$optionalAsRev YIELD access RETURN *") {
          assertSameAST(testName)
        }
      }
    }

  // Fails to parse

  test("SHOW PRIVILAGES") {
    val exceptionMessage =
      s"""Invalid input 'PRIVILAGES': expected
         |  "ALL"
         |  "BTREE"
         |  "BUILT"
         |  "CONSTRAINT"
         |  "CONSTRAINTS"
         |  "CURRENT"
         |  "DATABASE"
         |  "DATABASES"
         |  "DEFAULT"
         |  "EXIST"
         |  "EXISTENCE"
         |  "EXISTS"
         |  "FULLTEXT"
         |  "FUNCTION"
         |  "FUNCTIONS"
         |  "HOME"
         |  "INDEX"
         |  "INDEXES"
         |  "LOOKUP"
         |  "NODE"
         |  "POINT"
         |  "POPULATED"
         |  "PRIVILEGE"
         |  "PRIVILEGES"
         |  "PROCEDURE"
         |  "PROCEDURES"
         |  "PROPERTY"
         |  "RANGE"
         |  "REL"
         |  "RELATIONSHIP"
         |  "ROLE"
         |  "ROLES"
         |  "TEXT"
         |  "TRANSACTION"
         |  "TRANSACTIONS"
         |  "UNIQUE"
         |  "USER"
         |  "USERS" (line 1, column 6 (offset: 5))""".stripMargin

    assertJavaCCException(testName, exceptionMessage)
  }

  test("SHOW ROLE PRIVILEGES") {
    assertJavaCCException(testName, """Invalid input '': expected ",", "PRIVILEGE" or "PRIVILEGES" (line 1, column 21 (offset: 20))""")
  }

  test("SHOW ALL ROLE role PRIVILEGES") {
    val exceptionMessage =
      s"""Invalid input 'ROLE': expected
         |  "CONSTRAINT"
         |  "CONSTRAINTS"
         |  "FUNCTION"
         |  "FUNCTIONS"
         |  "INDEX"
         |  "INDEXES"
         |  "PRIVILEGE"
         |  "PRIVILEGES"
         |  "ROLES" (line 1, column 10 (offset: 9))""".stripMargin

    assertJavaCCException(testName, exceptionMessage)
  }

  test("SHOW ALL USER user PRIVILEGES") {
    val exceptionMessage =
      s"""Invalid input 'USER': expected
         |  "CONSTRAINT"
         |  "CONSTRAINTS"
         |  "FUNCTION"
         |  "FUNCTIONS"
         |  "INDEX"
         |  "INDEXES"
         |  "PRIVILEGE"
         |  "PRIVILEGES"
         |  "ROLES" (line 1, column 10 (offset: 9))""".stripMargin

    assertJavaCCException(testName, exceptionMessage)
  }

  test("SHOW USER us%er PRIVILEGES") {
    assertJavaCCException(testName, """Invalid input '%': expected ",", "PRIVILEGE" or "PRIVILEGES" (line 1, column 13 (offset: 12))""")
  }

  test("SHOW USER user PRIVILEGES YIELD *, blah RETURN user") {
    val exceptionMessage =
      s"""Invalid input ',': expected
         |  "LIMIT"
         |  "ORDER"
         |  "RETURN"
         |  "SKIP"
         |  "WHERE"
         |  <EOF> (line 1, column 34 (offset: 33))""".stripMargin

    assertJavaCCException(testName, exceptionMessage)
  }

  test("SHOW PRIVILEGES COMMANDS") {
    assertSameAST(testName)
  }

  test("SHOW PRIVILEGES REVOKE") {
    assertSameAST(testName)
  }

  test("SHOW PRIVILEGES AS REVOKE COMMAND COMMANDS") {
    assertSameAST(testName)
  }

  test("SHOW PRIVILEGES AS COMMANDS REVOKE") {
    assertSameAST(testName)
  }

  test("SHOW PRIVILEGES AS COMMANDS USER user") {
    assertSameAST(testName)
  }
}
