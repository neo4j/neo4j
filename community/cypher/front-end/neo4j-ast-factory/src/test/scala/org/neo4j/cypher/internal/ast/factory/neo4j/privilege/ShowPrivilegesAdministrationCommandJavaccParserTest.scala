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

  // yield / skip / limit / order by / where

  Seq(
    "",
    "ALL",
    "USER",
    "USER neo4j",
    "USERS neo4j, $user",
  ).foreach { privType =>
    Seq(
      "PRIVILEGE",
      "PRIVILEGES"
    ).foreach { privilegeOrPrivileges =>
      test(s"SHOW $privType $privilegeOrPrivileges WHERE access = 'GRANTED'") {
        assertSameAST(testName)
      }

      test(s"SHOW $privType $privilegeOrPrivileges WHERE access = 'GRANTED' AND action = 'match'") {
        assertSameAST(testName)
      }

      test(s"SHOW $privType $privilegeOrPrivileges YIELD access ORDER BY access") {
        assertSameAST(testName)
      }

      test(s"SHOW $privType $privilegeOrPrivileges YIELD access ORDER BY access WHERE access ='none'") {
        assertSameAST(testName)
      }

      test(s"SHOW $privType $privilegeOrPrivileges YIELD access ORDER BY access SKIP 1 LIMIT 10 WHERE access ='none'") {
        assertSameAST(testName)
      }

      test(s"SHOW $privType $privilegeOrPrivileges YIELD access SKIP -1") {
        assertSameAST(testName)
      }

      test(s"SHOW $privType $privilegeOrPrivileges YIELD access, action RETURN access, count(action) ORDER BY access") {
        assertSameAST(testName)
      }

      test(s"SHOW $privType $privilegeOrPrivileges YIELD access, action SKIP 1 RETURN access, action") {
        assertSameAST(testName)
      }

      test(s"SHOW $privType $privilegeOrPrivileges YIELD access, action WHERE access = 'none' RETURN action") {
        assertSameAST(testName)
      }

      test(s"SHOW $privType $privilegeOrPrivileges YIELD * RETURN *") {
        assertSameAST(testName)
      }
    }
  }

  // yield and where edge cases

  Seq(
    "USER",
    "USERS"
  ).foreach { privType =>
    test(s"SHOW $privType yield PRIVILEGES YIELD access RETURN *") {
      assertSameAST(testName)
    }

    test(s"SHOW $privType yield, where PRIVILEGES YIELD access RETURN *") {
      assertSameAST(testName)
    }

    test(s"SHOW $privType where PRIVILEGE WHERE access = 'none'") {
      assertSameAST(testName)
    }

    test(s"SHOW $privType privilege PRIVILEGE YIELD access RETURN *") {
      assertSameAST(testName)
    }

    test(s"SHOW $privType privileges PRIVILEGES YIELD access RETURN *") {
      assertSameAST(testName)
    }

    test(s"SHOW $privType privilege, privileges PRIVILEGES YIELD access RETURN *") {
      assertSameAST(testName)
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
         |  "ROLES"
         |  "TEXT"
         |  "TRANSACTION"
         |  "TRANSACTIONS"
         |  "UNIQUE"
         |  "USER"
         |  "USERS" (line 1, column 6 (offset: 5))""".stripMargin

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
}
