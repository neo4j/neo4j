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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalatest.FunSuiteLike

class RoleAdministrationCommandJavaCcParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName {

  test("USE GRAPH SYSTEM SHOW ROLES") {
    assertSameAST(testName)
  }

  test("SHOW ROLES") {
    assertSameAST(testName)
  }

  test("CATALOG SHOW ALL ROLES") {
    assertSameAST(testName)
  }

  test("CATALOG SHOW POPULATED ROLES") {
    assertSameAST(testName)
  }

  test("SHOW ROLES WITH USERS") {
    assertSameAST(testName)
  }

  test("CATALOG SHOW ALL ROLES WITH USERS") {
    assertSameAST(testName)
  }

  test("SHOW POPULATED ROLES WITH USERS") {
    assertSameAST(testName)
  }

  test("CATALOG SHOW ALL ROLES YIELD role") {
    assertSameAST(testName)
  }

  test("CATALOG SHOW ALL ROLES WHERE role='PUBLIC'") {
    assertSameAST(testName)
  }

  test("SHOW ALL ROLES YIELD role RETURN role") {
    assertSameAST(testName)
  }

  test("SHOW ALL ROLES YIELD role RETURN") {
    assertJavaCCException(testName, "Invalid input '': expected \"*\", \"DISTINCT\" or an expression (line 1, column 33 (offset: 32))")
  }

  test("SHOW POPULATED ROLES YIELD role WHERE role='PUBLIC' RETURN role") {
    assertSameAST(testName)
  }

  test("SHOW POPULATED ROLES YIELD * RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW ROLES WITH USERS YIELD * LIMIT 10 WHERE foo='bar' RETURN some,columns LIMIT 10") {
    assertSameAST(testName)
  }

  test("SHOW ALL ROLES YIELD return, return RETURN return") {
    assertSameAST(testName)
  }

  test("CATALOG CATALOG SHOW ROLES") {
    val exceptionMessage =
      s"""Invalid input 'CATALOG': expected
         |  "ALTER"
         |  "DENY"
         |  "DROP"
         |  "GRANT"
         |  "RENAME"
         |  "REVOKE"
         |  "SHOW"
         |  "START"
         |  "STOP" (line 1, column 9 (offset: 8))""".stripMargin

    assertJavaCCException(testName, exceptionMessage)
  }

  test("CATALOG SHOW ROLE") {
    val exceptionMessage =
      s"""Invalid input 'ROLE': expected
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
         |  "POPULATED"
         |  "PROCEDURE"
         |  "PROCEDURES"
         |  "PROPERTY"
         |  "REL"
         |  "RELATIONSHIP"
         |  "ROLES"
         |  "TEXT"
         |  "UNIQUE"
         |  "USER"
         |  "USERS" (line 1, column 14 (offset: 13))""".stripMargin

    assertJavaCCException(testName, exceptionMessage)
  }

  test("SHOW ALL ROLE") {
    assertJavaCCException(testName,
      """Invalid input 'ROLE': expected
        |  "CONSTRAINT"
        |  "CONSTRAINTS"
        |  "FUNCTION"
        |  "FUNCTIONS"
        |  "INDEX"
        |  "INDEXES"
        |  "ROLES" (line 1, column 10 (offset: 9))""".stripMargin)
  }

  test("SHOW POPULATED ROLE") {
    assertJavaCCException(testName, "Invalid input 'ROLE': expected \"ROLES\" (line 1, column 16 (offset: 15))")
  }

  test("SHOW ROLE role") {
    val exceptionMessage =
      s"""Invalid input 'ROLE': expected
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
         |  "POPULATED"
         |  "PROCEDURE"
         |  "PROCEDURES"
         |  "PROPERTY"
         |  "REL"
         |  "RELATIONSHIP"
         |  "ROLES"
         |  "TEXT"
         |  "UNIQUE"
         |  "USER"
         |  "USERS" (line 1, column 6 (offset: 5))""".stripMargin

    assertJavaCCException(testName, exceptionMessage)
  }

  test("SHOW ROLE WITH USERS") {
    val exceptionMessage =
      s"""Invalid input 'ROLE': expected
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
         |  "POPULATED"
         |  "PROCEDURE"
         |  "PROCEDURES"
         |  "PROPERTY"
         |  "REL"
         |  "RELATIONSHIP"
         |  "ROLES"
         |  "TEXT"
         |  "UNIQUE"
         |  "USER"
         |  "USERS" (line 1, column 6 (offset: 5))""".stripMargin

    assertJavaCCException(testName, exceptionMessage)
  }

  test("CATALOG SHOW ROLES WITH USER") {
    assertJavaCCException(testName, "Invalid input 'USER': expected \"USERS\" (line 1, column 25 (offset: 24))")
  }

  test("SHOW ROLE WITH USER") {
    val exceptionMessage =
      s"""Invalid input 'ROLE': expected
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
         |  "POPULATED"
         |  "PROCEDURE"
         |  "PROCEDURES"
         |  "PROPERTY"
         |  "REL"
         |  "RELATIONSHIP"
         |  "ROLES"
         |  "TEXT"
         |  "UNIQUE"
         |  "USER"
         |  "USERS" (line 1, column 6 (offset: 5))""".stripMargin

    assertJavaCCException(testName, exceptionMessage)
  }

  test("SHOW ALL ROLE WITH USERS") {
    assertJavaCCException(testName,
      """Invalid input 'ROLE': expected
        |  "CONSTRAINT"
        |  "CONSTRAINTS"
        |  "FUNCTION"
        |  "FUNCTIONS"
        |  "INDEX"
        |  "INDEXES"
        |  "ROLES" (line 1, column 10 (offset: 9))""".stripMargin)
  }

  test("SHOW ALL ROLES WITH USER") {
    assertJavaCCException(testName, "Invalid input 'USER': expected \"USERS\" (line 1, column 21 (offset: 20))")
  }

  test("SHOW ALL ROLE WITH USER") {
    assertJavaCCException(testName,
      """Invalid input 'ROLE': expected
        |  "CONSTRAINT"
        |  "CONSTRAINTS"
        |  "FUNCTION"
        |  "FUNCTIONS"
        |  "INDEX"
        |  "INDEXES"
        |  "ROLES" (line 1, column 10 (offset: 9))""".stripMargin)
  }

  test("YIELD a, b, c WHERE a = b") {
    assertSameAST(testName)
  }

  test("SHOW POPULATED ROLES YIELD role ORDER BY role SKIP -1") {
    assertSameAST(testName)
  }

  test("SHOW POPULATED ROLES YIELD role ORDER BY role SKIP -1*4 + 2") {
    assertSameAST(testName)
  }

  test("SHOW POPULATED ROLES YIELD role ORDER BY role LIMIT -1") {
    assertSameAST(testName)
  }

  test("CATALOG SHOW POPULATED ROLE WITH USERS") {
    assertJavaCCException(testName, "Invalid input 'ROLE': expected \"ROLES\" (line 1, column 24 (offset: 23))")
  }

  test("CATALOG SHOW POPULATED ROLES WITH USER") {
    assertJavaCCException(testName, "Invalid input 'USER': expected \"USERS\" (line 1, column 35 (offset: 34))")
  }

  test("CATALOG SHOW POPULATED ROLE WITH USER") {
    assertJavaCCException(testName, "Invalid input 'ROLE': expected \"ROLES\" (line 1, column 24 (offset: 23))")
  }

  test("CATALOG SHOW ROLES WITH USER user") {
    assertJavaCCException(testName, "Invalid input 'USER': expected \"USERS\" (line 1, column 25 (offset: 24))")
  }

  test("SHOW POPULATED ROLES YIELD *,blah RETURN role") {
    val exceptionMessage =
      s"""Invalid input ',': expected
         |  "LIMIT"
         |  "ORDER"
         |  "RETURN"
         |  "SKIP"
         |  "WHERE"
         |  <EOF> (line 1, column 29 (offset: 28))""".stripMargin
    assertJavaCCException(testName, exceptionMessage)
  }

  //  Creating roles

  test("CREATE ROLE foo") {
    assertSameAST(testName)
  }

  test("CREATE ROLE $foo") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE ROLE `fo!$o`") {
    assertSameAST(testName)
  }

  test("CREATE ROLE ``") {
    assertSameAST(testName)
  }

  test("CREATE ROLE foo AS COPY OF bar") {
    assertSameAST(testName)
  }

  test("CREATE ROLE foo AS COPY OF $bar") {
    assertSameAST(testName)
  }

  test("CREATE ROLE foo AS COPY OF ``") {
    assertSameAST(testName)
  }

  test("CREATE ROLE `` AS COPY OF bar") {
    assertSameAST(testName)
  }

  test("CREATE ROLE foo IF NOT EXISTS") {
    assertSameAST(testName)
  }

  test("CREATE ROLE foo IF NOT EXISTS AS COPY OF bar") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE ROLE foo") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE ROLE foo AS COPY OF bar") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE ROLE foo IF NOT EXISTS") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE ROLE foo IF NOT EXISTS AS COPY OF bar") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE ROLE \"foo\"") {
    assertSameAST(testName)
  }

  test("CREATE ROLE f%o") {
    assertSameAST(testName)
  }

  test("CREATE ROLE  IF NOT EXISTS") {
    assertSameAST(testName)
  }

  test("CREATE ROLE foo IF EXISTS") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE ROLE") {
    assertJavaCCException(testName, "Invalid input '': expected a parameter or an identifier (line 1, column 23 (offset: 22))")
  }

  test("CREATE ROLE foo AS COPY OF") {
    assertJavaCCException(testName, "Invalid input '': expected a parameter or an identifier (line 1, column 27 (offset: 26))")
  }

  test("CREATE ROLE foo IF NOT EXISTS AS COPY OF") {
    assertJavaCCException(testName, "Invalid input '': expected a parameter or an identifier (line 1, column 41 (offset: 40))")
  }

  test("CREATE OR REPLACE ROLE foo AS COPY OF") {
    assertJavaCCException(testName, "Invalid input '': expected a parameter or an identifier (line 1, column 38 (offset: 37))")
  }

  test("CREATE ROLE foo UNION CREATE ROLE foo2") {
    assertJavaCCException(testName, "Invalid input 'UNION': expected \"AS\", \"IF\" or <EOF> (line 1, column 17 (offset: 16))")
  }

  // Renaming role

  test("RENAME ROLE foo TO bar") {
    assertSameAST(testName)
  }

  test("RENAME ROLE foo TO $bar") {
    assertSameAST(testName)
  }

  test("RENAME ROLE $foo TO bar") {
    assertSameAST(testName)
  }

  test("RENAME ROLE $foo TO $bar") {
    assertSameAST(testName)
  }

  test("RENAME ROLE foo IF EXISTS TO bar") {
    assertSameAST(testName)
  }

  test("RENAME ROLE foo IF EXISTS TO $bar") {
    assertSameAST(testName)
  }

  test("RENAME ROLE $foo IF EXISTS TO bar") {
    assertSameAST(testName)
  }

  test("RENAME ROLE $foo IF EXISTS TO $bar") {
    assertSameAST(testName)
  }

  test("RENAME ROLE foo TO ``") {
    assertSameAST(testName)
  }

  test("RENAME ROLE `` TO bar") {
    assertSameAST(testName)
  }

  test("RENAME ROLE foo TO") {
    assertJavaCCException(testName, "Invalid input '': expected a parameter or an identifier (line 1, column 19 (offset: 18))")
  }

  test("RENAME ROLE TO bar") {
    assertJavaCCException(testName, "Invalid input 'bar': expected \"IF\" or \"TO\" (line 1, column 16 (offset: 15))")
  }

  test("RENAME ROLE TO") {
    assertJavaCCException(testName, "Invalid input '': expected \"IF\" or \"TO\" (line 1, column 15 (offset: 14))")
  }

  test("RENAME ROLE foo SET NAME TO bar") {
    assertJavaCCException(testName, "Invalid input 'SET': expected \"IF\" or \"TO\" (line 1, column 17 (offset: 16))")
  }

  test("RENAME ROLE foo SET NAME bar") {
    assertJavaCCException(testName, "Invalid input 'SET': expected \"IF\" or \"TO\" (line 1, column 17 (offset: 16))")
  }

  test("ALTER ROLE foo SET NAME bar") {
    assertJavaCCException(testName, """Invalid input 'ROLE': expected "CURRENT" or "USER" (line 1, column 7 (offset: 6))""")
  }

  test("RENAME ROLE foo IF EXIST TO bar") {
    assertJavaCCException(testName, "Invalid input 'EXIST': expected \"EXISTS\" (line 1, column 20 (offset: 19))")
  }

  test("RENAME ROLE foo IF NOT EXISTS TO bar") {
    assertJavaCCException(testName, "Invalid input 'NOT': expected \"EXISTS\" (line 1, column 20 (offset: 19))")
  }

  test("RENAME ROLE foo TO bar IF EXISTS") {
    assertJavaCCException(testName, "Invalid input 'IF': expected <EOF> (line 1, column 24 (offset: 23))")
  }

  test("RENAME IF EXISTS ROLE foo TO bar") {
    assertJavaCCException(testName, "Invalid input 'IF': expected \"ROLE\" or \"USER\" (line 1, column 8 (offset: 7))")
  }

  test("RENAME OR REPLACE ROLE foo TO bar") {
    assertJavaCCException(testName, "Invalid input 'OR': expected \"ROLE\" or \"USER\" (line 1, column 8 (offset: 7))")
  }

  //  Dropping role

  test("DROP ROLE foo") {
    assertSameAST(testName)
  }

  test("DROP ROLE $foo") {
    assertSameAST(testName)
  }

  test("DROP ROLE ``") {
    assertSameAST(testName)
  }

  test("DROP ROLE foo IF EXISTS") {
    assertSameAST(testName)
  }

  test("DROP ROLE `` IF EXISTS") {
    assertSameAST(testName)
  }

  test("DROP ROLE ") {
    assertJavaCCException(testName, "Invalid input '': expected a parameter or an identifier (line 1, column 10 (offset: 9))")
  }

  test("DROP ROLE  IF EXISTS") {
    assertSameAST(testName)
  }

  test("DROP ROLE foo IF NOT EXISTS") {
    assertSameAST(testName)
  }

  //  Granting and Revoking role(s)

  Seq("ROLE", "ROLES").foreach {
    roleKeyword =>

      Seq(
        ("GRANT", "TO"),
        ("REVOKE", "FROM")
      ).foreach {
        case (verb: String, preposition: String) =>

          test(s"$verb $roleKeyword foo $preposition abc") {
            assertSameAST(testName)
          }

          test(s"CATALOG $verb $roleKeyword foo $preposition abc") {
            assertSameAST(testName)
          }

          test(s"$verb $roleKeyword " +
            s"catalog, show, populated, roles, role, users, replace, grant, revoke, if, copy, of, to " +
            s"$preposition abc") {
            assertSameAST(testName)
          }

          test(s"$verb $roleKeyword foo $preposition abc, def") {
            assertSameAST(testName)
          }

          test(s"$verb $roleKeyword foo,bla,roo $preposition bar, baz,abc,  def") {
            assertSameAST(testName)
          }

          test(s"$verb $roleKeyword `fo:o` $preposition bar") {
            assertSameAST(testName)
          }

          test(s"$verb $roleKeyword foo $preposition `b:ar`") {
            assertSameAST(testName)
          }

          test(s"$verb $roleKeyword `$$f00`,bar $preposition abc,`$$a&c`") {
            assertSameAST(testName)
          }

          // Should fail to parse if not following the pattern $command $roleKeyword role(s) $preposition user(s)

          test(s"$verb $roleKeyword") {
            val expected = roleKeyword match {
              case "ROLE" => """Invalid input '': expected "MANAGEMENT", a parameter or an identifier"""
              case _      => """Invalid input '': expected a parameter or an identifier"""
            }
            assertJavaCCExceptionStart(testName, expected)
          }

          test(s"$verb $roleKeyword foo") {
            assertJavaCCExceptionStart(testName, s"""Invalid input '': expected "," or "$preposition"""")
          }

          test(s"$verb $roleKeyword foo $preposition") {
            assertJavaCCExceptionStart(testName, "Invalid input '': expected a parameter or an identifier")
          }

          test(s"$verb $roleKeyword $preposition abc") {
            assertSameAST(testName)
          }

          // Should fail to parse when invalid user or role name

          test(s"$verb $roleKeyword fo:o $preposition bar") {
            assertSameAST(testName)
          }

          test(s"$verb $roleKeyword foo $preposition b:ar") {
            assertSameAST(testName)
          }
      }

      // Should fail to parse when mixing TO and FROM

      test(s"GRANT $roleKeyword foo FROM abc") {
        assertSameAST(testName)
      }

      test(s"REVOKE $roleKeyword foo TO abc") {
        assertSameAST(testName)
      }
  }

  test("GRANT ROLE $a TO $x") {
    assertSameAST(testName)
  }

  test("REVOKE ROLE $a FROM $x") {
    assertSameAST(testName)
  }

  test("GRANT ROLES a, $b, $c TO $x, y, z") {
    assertSameAST(testName)
  }

  test("REVOKE ROLES a, $b, $c FROM $x, y, z") {
    assertSameAST(testName)
  }

  test(s"DENY ROLE foo TO abc") {
    assertJavaCCExceptionStart(testName, """Invalid input 'foo': expected "MANAGEMENT"""")
  }

  // ROLES TO USER only have GRANT and REVOKE and not DENY

  test("DENY ROLES foo TO abc") {
    assertJavaCCExceptionStart(testName,
      """Invalid input 'ROLES': expected
        |  "ACCESS"
        |  "ALL"
        |  "ALTER"
        |  "ASSIGN"
        |  "CONSTRAINT"
        |  "CONSTRAINTS"
        |  "CREATE"""".stripMargin)
  }
}
