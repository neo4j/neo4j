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

class UserAdministrationCommandJavaCcParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName {

  //  Showing user

  test("SHOW USERS") {
    assertSameAST(testName)
  }

  test("CATALOG SHOW USERS") {
    assertSameAST(testName)
  }

  test("USE system SHOW USERS") {
    assertSameAST(testName)
  }

  test("SHOW USERS WHERE user = 'alice'") {
    assertSameAST(testName)
  }

  test("SHOW USERS WHERE user = 'alice' OR user = 'bob'") {
    assertSameAST(testName)
  }

  test("SHOW USERS YIELD user ORDER BY user") {
    assertSameAST(testName)
  }

  test("SHOW USERS YIELD user ORDER BY user WHERE user ='none'") {
    assertSameAST(testName)
  }

  test("SHOW USERS YIELD user ORDER BY user SKIP 1 LIMIT 10 WHERE user ='none'") {
    assertSameAST(testName)
  }

  test("SHOW USERS YIELD user SKIP -1") {
    assertSameAST(testName)
  }

  test("SHOW USERS YIELD user RETURN user ORDER BY user") {
    assertSameAST(testName)
  }

  test("SHOW USERS YIELD user, suspended as suspended WHERE suspended RETURN DISTINCT user") {
    assertSameAST(testName)
  }

  test("SHOW USERS YIELD * RETURN *") {
    assertSameAST(testName)
  }

  test("CATALOG SHOW USER") {
    assertJavaCCException(testName, "Invalid input '': expected \"DEFINED\" (line 1, column 18 (offset: 17))")
  }

  test("SHOW USERS YIELD *,blah RETURN user") {
    assertSameAST(testName)
  }

  test("SHOW USERS YIELD (123 + xyz)") {
    assertSameAST(testName)
  }

  test("SHOW USERS YIELD (123 + xyz) AS foo") {
    assertSameAST(testName)
  }

  // Showing current user

  test("CATALOG SHOW CURRENT USER") {
    assertSameAST(testName)
  }

  test("SHOW CURRENT USER") {
    assertSameAST(testName)
  }

  test("SHOW CURRENT USER YIELD * WHERE suspended = false RETURN roles") {
    assertSameAST(testName)
  }

  //  Creating user

  test("CATALOG CREATE USER foo SET PASSWORD 'password'") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE USER $foo SET PASSWORD 'password'") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE USER foo SET PLAINTEXT PASSWORD 'password'") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE USER foo SET PLAINTEXT PASSWORD $password") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE USER $bar SET PASSWORD $password") {
    assertSameAST(testName)
  }

  test("CREATE USER `foo` SET PASSwORD 'password'") {
    assertSameAST(testName)
  }

  test("CREATE USER `!#\"~` SeT PASSWORD 'password'") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SeT PASSWORD 'pasS5Wor%d'") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PASSwORD ''") {
    assertSameAST(testName)
  }

  test("CREATE uSER foo SET PASSWORD $password") {
    assertSameAST(testName)
  }

  test("CREaTE USER foo SET PASSWORD 'password' CHANGE REQUIRED") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE USER foo SET PASSWORD $password CHANGE REQUIRED") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE required") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PASSWORD 'password' CHAngE NOT REQUIRED") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE NOT REQUIRED") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PASSWORD $password SET  PASSWORD CHANGE NOT REQUIRED") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE USER foo SET PASSWORD 'password' SET STATUS SUSPENDed") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS ACtiVE") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE NOT REQUIRED SET   STATuS SUSPENDED") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED SET STATUS SUSPENDED") {
    assertSameAST(testName)
  }

  test("CREATE USER `` SET PASSwORD 'password'") {
    assertSameAST(testName)
  }

  test("CREATE USER `f:oo` SET PASSWORD 'password'") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE USER foo IF NOT EXISTS SET PASSWORD 'password'") {
    assertSameAST(testName)
  }

  test("CREATE uSER foo IF NOT EXISTS SET PASSWORD $password") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE USER foo IF NOT EXISTS SET PASSWORD $password CHANGE REQUIRED") {
    assertSameAST(testName)
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD $password SET STATUS SUSPENDED") {
    assertSameAST(testName)
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD $password CHANGE REQUIRED SET STATUS SUSPENDED") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE OR REPLACE USER foo SET PASSWORD 'password'") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE uSER foo SET PASSWORD $password") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE OR REPLACE USER foo SET PASSWORD $password CHANGE REQUIRED") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD $password SET STATUS SUSPENDED") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD $password CHANGE REQUIRED SET STATUS SUSPENDED") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE USER foo IF NOT EXISTS SET PASSWORD 'password'") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET ENCRYPTED PASSWORD '1,04773b8510aea96ca2085cb81764b0a2,75f4201d047191c17c5e236311b7c4d77e36877503fe60b1ca6d4016160782ab'") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE USER $foo SET encrYPTEd PASSWORD 'password'") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE USER $bar SET ENCRYPTED Password $password") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE USER foo SET encrypted password 'sha256,x1024,0x2460294fe,b3ddb287a'") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET password 'password' SET HOME DATABASE db1") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET password 'password' SET HOME DATABASE $db") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE USER foo SET password 'password' SET HOME DATABASE db1") {
    assertSameAST(testName)
  }

  test("CREATE USER foo IF NOT EXISTS SET password 'password' SET HOME DATABASE db1") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET password 'password' SET PASSWORD CHANGE NOT REQUIRED SET HOME DAtabase $db") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET password 'password' SET HOME DATABASE `#dfkfop!`") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET password 'password' SET HOME DATABASE null") {
    assertSameAST(testName)
  }

  Seq(
    ("CHANGE REQUIRED", "SET STATUS ACTIVE", "SET HOME DATABASE db1"),
    ("CHANGE REQUIRED", "SET HOME DATABASE db1", "SET STATUS ACTIVE")
  ).foreach {
    case (first: String, second: String, third: String) =>
      test(s"CREATE USER foo SET password 'password' $first $second $third") {
        assertSameAST(testName)
      }
  }

  Seq("SET PASSWORD CHANGE REQUIRED", "SET STATUS ACTIVE", "SET HOME DATABASE db1")
    .permutations.foreach {
    clauses =>
      test(s"CREATE USER foo SET password 'password' ${clauses.mkString(" ")}") {
        assertSameAST(testName)
      }
  }

  test("CREATE USER foo") {
    assertSameAST(testName)
  }

  test("CREATE USER \"foo\" SET PASSwORD 'password'") {
    assertSameAST(testName)
  }

  test("CREATE USER !#\"~ SeT PASSWORD 'password'") {
    assertSameAST(testName)
  }

  test("CATALOG CREATE USER fo,o SET PASSWORD 'password'") {
    assertSameAST(testName)
  }

  test("CREATE USER f:oo SET PASSWORD 'password'") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PASSWORD") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET ENCRYPTED PASSWORD 123") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET ENCRYPTED PASSWORD") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PLAINTEXT PASSWORD") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PASSWORD 'password' SET ENCRYPTED PASSWORD") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PASSWORD 'password' ENCRYPTED") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PASSwORD 'passwordString'+$passwordexpressions.Parameter") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PASSWORD null CHANGE REQUIRED") {
    assertSameAST(testName)
  }

  test("CREATE USER foo PASSWORD 'password'") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS ACTIVE CHANGE NOT REQUIRED") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PASSWORD 'password' SET HOME DATABASE db1 CHANGE NOT REQUIRED") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PASSWORD 'password' SET DEFAULT DATABASE db1") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STAUS ACTIVE") {
    assertJavaCCException(testName, "Invalid input 'STAUS': expected \"HOME\", \"PASSWORD\" or \"STATUS\" (line 1, column 45 (offset: 44))")
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS IMAGINARY") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PASSWORD CHANGE REQUIRED") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET STATUS SUSPENDED") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PASSWORD CHANGE REQUIRED SET STATUS ACTIVE") {
    assertSameAST(testName)
  }

  test("CREATE USER foo IF EXISTS SET PASSWORD 'bar'") {
    assertSameAST(testName)
  }

  test("CREATE USER foo IF NOT EXISTS") {
    assertSameAST(testName)
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD") {
    assertSameAST(testName)
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD CHANGE REQUIRED") {
    assertSameAST(testName)
  }

  test("CREATE USER foo IF NOT EXISTS SET STATUS ACTIVE") {
    assertSameAST(testName)
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE USER foo") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD CHANGE NOT REQUIRED") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE USER foo SET STATUS SUSPENDED") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD CHANGE REQUIRED SET STATUS ACTIVE") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PASSWORD 'bar' SET HOME DATABASE 123456") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PASSWORD 'bar' SET HOME DATABASE #dfkfop!") {
    assertSameAST(testName)
  }

  test("CREATE USER foo SET PASSWORD $password CHANGE NOT REQUIRED SET PASSWORD CHANGE REQUIRED") {
    val exceptionMessage =
      s"""Duplicate SET PASSWORD CHANGE [NOT] REQUIRED clause (line 1, column 60 (offset: 59))""".stripMargin
    assertJavaCCException(testName, exceptionMessage)
  }

  test("CREATE USER foo SET PASSWORD $password SET STATUS ACTIVE SET STATUS SUSPENDED") {
    val exceptionMessage =
      s"""Duplicate SET STATUS {SUSPENDED|ACTIVE} clause (line 1, column 58 (offset: 57))""".stripMargin
    assertJavaCCException(testName, exceptionMessage)
  }

  test("CREATE USER foo SET PASSWORD $password SET HOME DATABASE db SET HOME DATABASE db") {
    val exceptionMessage =
      s"""Duplicate SET HOME DATABASE clause (line 1, column 61 (offset: 60))""".stripMargin
    assertJavaCCException(testName, exceptionMessage)
  }

  // Renaming user

  test("RENAME USER foo TO bar") {
    assertSameAST(testName)
  }

  test("RENAME USER foo TO $bar") {
    assertSameAST(testName)
  }

  test("RENAME USER $foo TO bar") {
    assertSameAST(testName)
  }

  test("RENAME USER $foo TO $bar") {
    assertSameAST(testName)
  }

  test("RENAME USER foo IF EXISTS TO bar") {
    assertSameAST(testName)
  }

  test("RENAME USER foo IF EXISTS TO $bar") {
    assertSameAST(testName)
  }

  test("RENAME USER $foo IF EXISTS TO bar") {
    assertSameAST(testName)
  }

  test("RENAME USER $foo IF EXISTS TO $bar") {
    assertSameAST(testName)
  }

  test("RENAME USER foo TO ``") {
    assertSameAST(testName)
  }

  test("RENAME USER `` TO bar") {
    assertSameAST(testName)
  }

  test("RENAME USER foo TO") {
    assertJavaCCException(testName, "Invalid input '': expected a parameter or an identifier (line 1, column 19 (offset: 18))")
  }

  test("RENAME USER TO bar") {
    assertJavaCCException(testName, "Invalid input 'bar': expected \"IF\" or \"TO\" (line 1, column 16 (offset: 15))")
  }

  test("RENAME USER TO") {
    assertJavaCCException(testName, "Invalid input '': expected \"IF\" or \"TO\" (line 1, column 15 (offset: 14))")
  }

  test("RENAME USER foo SET NAME TO bar") {
    assertJavaCCException(testName, "Invalid input 'SET': expected \"IF\" or \"TO\" (line 1, column 17 (offset: 16))")
  }

  test("RENAME USER foo SET NAME bar") {
    assertJavaCCException(testName, "Invalid input 'SET': expected \"IF\" or \"TO\" (line 1, column 17 (offset: 16))")
  }

  test("ALTER USER foo SET NAME bar") {
    val exceptionMessage =
      s"""Invalid input 'NAME': expected
         |  "ENCRYPTED"
         |  "HOME"
         |  "PASSWORD"
         |  "PLAINTEXT"
         |  "STATUS" (line 1, column 20 (offset: 19))""".stripMargin

    assertJavaCCException(testName, exceptionMessage)
  }

  test("ALTER USER foo SET PASSWORD 'secret' SET NAME bar") {
      s"""Invalid input 'NAME': expected
         |  "ALL"
         |  "CURRENT"
         |  "DATABASE"
         |  "DATABASES"
         |  "DEFAULT"
         |  "HOME"
         |  "POPULATED"
         |  "ROLES"
         |  "USERS" (line 1, column 42 (offset: 41))""".stripMargin
  }

  test("RENAME USER foo IF EXIST TO bar") {
    assertJavaCCException(testName, "Invalid input 'EXIST': expected \"EXISTS\" (line 1, column 20 (offset: 19))")
  }

  test("RENAME USER foo IF NOT EXISTS TO bar") {
    assertJavaCCException(testName, "Invalid input 'NOT': expected \"EXISTS\" (line 1, column 20 (offset: 19))")
  }

  test("RENAME USER foo TO bar IF EXISTS") {
    assertJavaCCException(testName, "Invalid input 'IF': expected <EOF> (line 1, column 24 (offset: 23))")
  }

  test("RENAME IF EXISTS USER foo TO bar") {
    assertJavaCCException(testName, "Invalid input 'IF': expected \"ROLE\" or \"USER\" (line 1, column 8 (offset: 7))")
  }

  test("RENAME OR REPLACE USER foo TO bar") {
    assertJavaCCException(testName, "Invalid input 'OR': expected \"ROLE\" or \"USER\" (line 1, column 8 (offset: 7))")
  }

  //  Dropping user

  test("DROP USER foo") {
    assertSameAST(testName)
  }

  test("DROP USER $foo") {
    assertSameAST(testName)
  }

  test("DROP USER ``") {
    assertSameAST(testName)
  }

  test("DROP USER `f:oo`") {
    assertSameAST(testName)
  }

  test("DROP USER foo IF EXISTS") {
    assertSameAST(testName)
  }

  test("DROP USER `` IF EXISTS") {
    assertSameAST(testName)
  }

  test("DROP USER `f:oo` IF EXISTS") {
    assertSameAST(testName)
  }

  test("DROP USER ") {
    assertSameAST(testName)
  }

  test("DROP USER  IF EXISTS") {
    assertSameAST(testName)
  }

  test("DROP USER foo IF NOT EXISTS") {
    assertSameAST(testName)
  }

  //  Altering user

  test("CATALOG ALTER USER foo SET PASSWORD 'password'") {
    assertSameAST(testName)
  }

  test("CATALOG ALTER USER $foo SET PASSWORD 'password'") {
    assertSameAST(testName)
  }

  test("CATALOG ALTER USER foo SET PLAINTEXT PASSWORD 'password'") {
    assertSameAST(testName)
  }

  test("CATALOG ALTER USER foo SET PLAINTEXT PASSWORD $password") {
    assertSameAST(testName)
  }

  test("ALTER USER `` SET PASSWORD 'password'") {
    assertSameAST(testName)
  }

  test("ALTER USER `f:oo` SET PASSWORD 'password'") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET PASSWORD ''") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET PASSWORD $password") {
    assertSameAST(testName)
  }

  test("ALTER USER foo IF EXISTS SET PASSWORD $password") {
    assertSameAST(testName)
  }

  test("CATALOG ALTER USER foo SET ENCRYPTED Password $password") {
    assertSameAST(testName)
  }

  test("CATALOG ALTER USER foo SET ENCRYPTED PASSWORD 'password'") {
    assertSameAST(testName)
  }

  test("CATALOG ALTER USER $foo SET ENCRYPTED PASSWORD 'password'") {
    assertSameAST(testName)
  }

  test("ALTER USER `` SET ENCRYPTED PASSWORD 'password'") {
    assertSameAST(testName)
  }

  test("CATALOG ALTER USER foo SET ENCRYPTED PASSWORD '1,04773b8510aea96ca2085cb81764b0a2,75f4201d047191c17c5e236311b7c4d77e36877503fe60b1ca6d4016160782ab'") {
    assertSameAST(testName)
  }

  test("CATALOG ALTER USER foo SET PASSWORD CHANGE REQUIRED") {
    assertSameAST(testName)
  }

  test("CATALOG ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED") {
    assertSameAST(testName)
  }

  test("ALTER USER foo IF EXISTS SET PASSWORD CHANGE NOT REQUIRED") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET STATUS SUSPENDED") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET STATUS ACTIVE") {
    assertSameAST(testName)
  }

  test("CATALOG ALTER USER foo SET PASSWORD 'password' CHANGE REQUIRED") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET PASSWORD $password SET PASSWORD CHANGE NOT REQUIRED") {
    assertSameAST(testName)
  }

  test("CATALOG ALTER USER foo SET PASSWORD 'password' SET STATUS ACTIVE") {
    assertSameAST(testName)
  }

  test("CATALOG ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED SET STATUS ACTIVE") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET PASSWORD $password SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED") {
    assertSameAST(testName)
  }

  test("ALTER USER foo IF EXISTS SET PASSWORD 'password' SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET HOME DATABASE db1") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET HOME DATABASE $db") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET HOME DATABASE null") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET PASSWORD CHANGE REQUIRED SET HOME DATABASE db1") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET password 'password' SET HOME DATABASE db1") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET password 'password' SET PASSWORD CHANGE NOT REQUIRED SET HOME DAtabase $db") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET HOME DATABASE `#dfkfop!`") {
    assertSameAST(testName)
  }

  Seq("SET PASSWORD CHANGE REQUIRED", "SET STATUS ACTIVE", "SET HOME DATABASE db1"
  ).permutations.foreach {
    clauses =>
      test(s"ALTER USER foo ${clauses.mkString(" ")}") {
        assertSameAST(testName)
      }
  }

  Seq("SET PASSWORD 'password' CHANGE NOT REQUIRED", "SET STATUS ACTIVE", "SET HOME DATABASE db1"
  ).permutations.foreach {
    clauses =>
      test(s"ALTER USER foo ${clauses.mkString(" ")}") {
        assertSameASTForQueries(testName,
          "ALTER USER foo SET PASSWORD 'password' CHANGE NOT REQUIRED SET STATUS ACTIVE SET HOME DATABASE db1",
          comparePosition = false)
      }
  }

  Seq("SET PASSWORD 'password'", "SET PASSWORD CHANGE REQUIRED", "SET STATUS ACTIVE", "SET HOME DATABASE db1"
  ).permutations.foreach {
    clauses =>
      test(s"ALTER USER foo ${clauses.mkString(" ")}") {
        assertSameASTForQueries(
          testName,
          "ALTER USER foo SET PASSWORD 'password' CHANGE REQUIRED SET STATUS ACTIVE SET HOME DATABASE db1",
          comparePosition = false
        )
      }
  }

  test("ALTER USER foo REMOVE HOME DATABASE") {
    assertSameAST(testName)
  }

  test("ALTER USER foo IF EXISTS REMOVE HOME DATABASE") {
    assertSameAST(testName)
  }

  test("ALTER USER foo") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET PASSWORD null") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET PASSWORD 123") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET PASSWORD") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET ENCRYPTED PASSWORD 123") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET PLAINTEXT PASSWORD") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET ENCRYPTED PASSWORD") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET PASSWORD 'password' SET ENCRYPTED PASSWORD") {
    assertJavaCCException(testName, "Duplicate SET PASSWORD clause (line 1, column 40 (offset: 39))")
  }

  test("ALTER USER foo SET PASSWORD 'password' ENCRYPTED") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET PASSWORD 'password' SET STATUS ACTIVE CHANGE NOT REQUIRED") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET STATUS") {
    assertSameAST(testName)
  }

  test("ALTER USER foo PASSWORD CHANGE NOT REQUIRED") {
    assertSameAST(testName)
  }

  test("ALTER USER foo CHANGE NOT REQUIRED") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET PASSWORD 'password' SET PASSWORD SET STATUS ACTIVE") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET PASSWORD STATUS ACTIVE") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET HOME DATABASE 123456") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET HOME DATABASE #dfkfop!") {
    assertSameAST(testName)
  }

  test("CATALOG ALTER USER foo SET PASSWORD 'password' SET STATUS IMAGINARY") {
    assertSameAST(testName)
  }

  test("ALTER USER foo IF NOT EXISTS SET PASSWORD 'password'") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET STATUS SUSPENDED REMOVE HOME DATABASE") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET HOME DATABASE db1 REMOVE HOME DATABASE") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET DEFAULT DATABASE db1") {
    assertSameAST(testName)
  }

  test("ALTER USER foo REMOVE DEFAULT DATABASE") {
    assertSameAST(testName)
  }

  test("ALTER USER foo SET PASSWORD $password SET PASSWORD 'password'") {
    val exceptionMessage =
      s"""Duplicate SET PASSWORD clause (line 1, column 39 (offset: 38))""".stripMargin
    assertJavaCCException(testName, exceptionMessage)
  }

  test("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED SET PASSWORD CHANGE REQUIRED") {
    val exceptionMessage =
      s"""Duplicate SET PASSWORD CHANGE [NOT] REQUIRED clause (line 1, column 49 (offset: 48))""".stripMargin
    assertJavaCCException(testName, exceptionMessage)
  }

  test("ALTER USER foo SET STATUS ACTIVE SET STATUS SUSPENDED") {
    val exceptionMessage =
      s"""Duplicate SET STATUS {SUSPENDED|ACTIVE} clause (line 1, column 34 (offset: 33))""".stripMargin
    assertJavaCCException(testName, exceptionMessage)
  }

  test("ALTER USER foo SET HOME DATABASE db SET HOME DATABASE db") {
    val exceptionMessage =
      s"""Duplicate SET HOME DATABASE clause (line 1, column 37 (offset: 36))""".stripMargin
    assertJavaCCException(testName, exceptionMessage)
  }

  // Changing own password

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO 'new'") {
    assertSameAST(testName)
  }

  test("alter current user set password from 'current' to ''") {
    assertSameAST(testName)
  }

  test("alter current user set password from '' to 'new'") {
    assertSameAST(testName)
  }

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO 'passWORD123%!'") {
    assertSameAST(testName)
  }

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO $newPassword") {
    assertSameAST(testName)
  }

  test("ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO 'new'") {
    assertSameAST(testName)
  }

  test("alter current user set password from $currentPassword to ''") {
    assertSameAST(testName)
  }

  test("ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO 'passWORD123%!'") {
    assertSameAST(testName)
  }

  test("ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword") {
    assertSameAST(testName)
  }

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO null") {
    assertSameAST(testName)
  }

  test("ALTER CURRENT USER SET PASSWORD FROM $current TO 123") {
    assertSameAST(testName)
  }

  test("ALTER PASSWORD FROM 'current' TO 'new'") {
    assertSameAST(testName)
  }

  test("ALTER CURRENT PASSWORD FROM 'current' TO 'new'") {
    assertSameAST(testName)
  }

  test("ALTER CURRENT USER PASSWORD FROM 'current' TO 'new'") {
    assertSameAST(testName)
  }

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO") {
    assertSameAST(testName)
  }

  test("ALTER CURRENT USER SET PASSWORD FROM TO 'new'") {
    assertSameAST(testName)
  }

  test("ALTER CURRENT USER SET PASSWORD TO 'new'") {
    assertSameAST(testName)
  }
}
