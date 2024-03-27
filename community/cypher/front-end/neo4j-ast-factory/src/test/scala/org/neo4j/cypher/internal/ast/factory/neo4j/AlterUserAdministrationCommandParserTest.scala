/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.expressions.SensitiveParameter
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral

import java.nio.charset.StandardCharsets.UTF_8
import java.util

class AlterUserAdministrationCommandParserTest extends UserAdministrationCommandParserTestBase {

  //  Alter user

  test("ALTER USER foo SET PASSWORD 'password'") {
    parsesTo[Statements](
      ast.AlterUser(
        literalFoo,
        isEncryptedPassword = Some(false),
        Some(password),
        ast.UserOptions(None, None, None),
        ifExists = false
      )(pos)
    )
  }

  test("ALTER USER $foo SET PASSWORD 'password'") {
    parsesTo[Statements](
      ast.AlterUser(
        paramFoo,
        isEncryptedPassword = Some(false),
        Some(password),
        ast.UserOptions(None, None, None),
        ifExists = false
      )(pos)
    )
  }

  test("ALTER USER foo SET PLAINTEXT PASSWORD 'password'") {
    parsesTo[Statements](
      ast.AlterUser(
        literalFoo,
        isEncryptedPassword = Some(false),
        Some(password),
        ast.UserOptions(None, None, None),
        ifExists = false
      )(pos)
    )
  }

  test(s"ALTER USER foo SET PLAINTEXT PASSWORD $pwParamString") {
    parsesTo[Statements](
      ast.AlterUser(
        literalFoo,
        isEncryptedPassword = Some(false),
        Some(paramPassword),
        ast.UserOptions(None, None, None),
        ifExists = false
      )(pos)
    )
  }

  test("ALTER USER `` SET PASSWORD 'password'") {
    parsesTo[Statements](ast.AlterUser(
      literalEmpty,
      isEncryptedPassword = Some(false),
      Some(password),
      ast.UserOptions(None, None, None),
      ifExists = false
    )(pos))
  }

  test("ALTER USER `f:oo` SET PASSWORD 'password'") {
    parsesTo[Statements](ast.AlterUser(
      literalFColonOo,
      isEncryptedPassword = Some(false),
      Some(password),
      ast.UserOptions(None, None, None),
      ifExists = false
    )(pos))
  }

  test("ALTER USER foo SET PASSWORD ''") {
    parsesTo[Statements](ast.AlterUser(
      literalFoo,
      isEncryptedPassword = Some(false),
      Some(passwordEmpty),
      ast.UserOptions(None, None, None),
      ifExists = false
    )(pos))
  }

  test(s"ALTER USER foo SET PASSWORD $pwParamString") {
    parsesTo[Statements](ast.AlterUser(
      literalFoo,
      isEncryptedPassword = Some(false),
      Some(paramPassword),
      ast.UserOptions(None, None, None),
      ifExists = false
    )(pos))
  }

  test(s"ALTER USER foo IF EXISTS SET PASSWORD $pwParamString") {
    parsesTo[Statements](ast.AlterUser(
      literalFoo,
      isEncryptedPassword = Some(false),
      Some(paramPassword),
      ast.UserOptions(None, None, None),
      ifExists = true
    )(pos))
  }

  test(s"ALTER USER foo SET ENCRYPTED Password $pwParamString") {
    parsesTo[Statements](
      ast.AlterUser(
        literalFoo,
        isEncryptedPassword = Some(true),
        Some(paramPassword),
        ast.UserOptions(None, None, None),
        ifExists = false
      )(pos)
    )
  }

  test("ALTER USER foo SET ENCRYPTED PASSWORD 'password'") {
    parsesTo[Statements](
      ast.AlterUser(
        literalFoo,
        isEncryptedPassword = Some(true),
        Some(password),
        ast.UserOptions(None, None, None),
        ifExists = false
      )(pos)
    )
  }

  test("ALTER USER $foo SET ENCRYPTED PASSWORD 'password'") {
    parsesTo[Statements](
      ast.AlterUser(
        paramFoo,
        isEncryptedPassword = Some(true),
        Some(password),
        ast.UserOptions(None, None, None),
        ifExists = false
      )(pos)
    )
  }

  test("ALTER USER `` SET ENCRYPTED PASSWORD 'password'") {
    parsesTo[Statements](ast.AlterUser(
      literalEmpty,
      isEncryptedPassword = Some(true),
      Some(password),
      ast.UserOptions(None, None, None),
      ifExists = false
    )(pos))
  }

  test(
    "ALTER USER foo SET ENCRYPTED PASSWORD '1,04773b8510aea96ca2085cb81764b0a2,75f4201d047191c17c5e236311b7c4d77e36877503fe60b1ca6d4016160782ab'"
  ) {
    parsesTo[Statements](
      ast.AlterUser(
        literalFoo,
        isEncryptedPassword = Some(true),
        Some(pw("1,04773b8510aea96ca2085cb81764b0a2,75f4201d047191c17c5e236311b7c4d77e36877503fe60b1ca6d4016160782ab")),
        ast.UserOptions(None, None, None),
        ifExists = false
      )(pos)
    )
  }

  test("ALTER USER foo SET PASSWORD CHANGE REQUIRED") {
    parsesTo[Statements](
      ast.AlterUser(
        literalFoo,
        None,
        None,
        ast.UserOptions(requirePasswordChange = Some(true), None, None),
        ifExists = false
      )(pos)
    )
  }

  test("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED") {
    parsesTo[Statements](
      ast.AlterUser(
        literalFoo,
        None,
        None,
        ast.UserOptions(requirePasswordChange = Some(false), None, None),
        ifExists = false
      )(pos)
    )
  }

  test("ALTER USER foo IF EXISTS SET PASSWORD CHANGE NOT REQUIRED") {
    parsesTo[Statements](ast.AlterUser(
      literalFoo,
      None,
      None,
      ast.UserOptions(requirePasswordChange = Some(false), None, None),
      ifExists = true
    )(pos))
  }

  test("ALTER USER foo SET STATUS SUSPENDED") {
    parsesTo[Statements](ast.AlterUser(
      literalFoo,
      None,
      None,
      ast.UserOptions(None, suspended = Some(true), None),
      ifExists = false
    )(pos))
  }

  test("ALTER USER foo SET STATUS ACTIVE") {
    parsesTo[Statements](ast.AlterUser(
      literalFoo,
      None,
      None,
      ast.UserOptions(None, suspended = Some(false), None),
      ifExists = false
    )(pos))
  }

  test("ALTER USER foo SET PASSWORD 'password' CHANGE REQUIRED") {
    parsesTo[Statements](
      ast.AlterUser(
        literalFoo,
        isEncryptedPassword = Some(false),
        Some(password),
        ast.UserOptions(requirePasswordChange = Some(true), None, None),
        ifExists = false
      )(pos)
    )
  }

  test(s"ALTER USER foo SET PASSWORD $pwParamString SET PASSWORD CHANGE NOT REQUIRED") {
    parsesTo[Statements](ast.AlterUser(
      literalFoo,
      isEncryptedPassword = Some(false),
      Some(paramPassword),
      ast.UserOptions(requirePasswordChange = Some(false), None, None),
      ifExists = false
    )(pos))
  }

  test("ALTER USER foo SET PASSWORD 'password' SET STATUS ACTIVE") {
    parsesTo[Statements](
      ast.AlterUser(
        literalFoo,
        isEncryptedPassword = Some(false),
        Some(password),
        ast.UserOptions(None, suspended = Some(false), None),
        ifExists = false
      )(pos)
    )
  }

  test("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED SET STATUS ACTIVE") {
    parsesTo[Statements](
      ast.AlterUser(
        literalFoo,
        None,
        None,
        ast.UserOptions(requirePasswordChange = Some(false), suspended = Some(false), None),
        ifExists = false
      )(pos)
    )
  }

  test(s"ALTER USER foo SET PASSWORD $pwParamString SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED") {
    parsesTo[Statements](ast.AlterUser(
      literalFoo,
      isEncryptedPassword = Some(false),
      Some(paramPassword),
      ast.UserOptions(requirePasswordChange = Some(false), suspended = Some(true), None),
      ifExists = false
    )(pos))
  }

  test("ALTER USER foo IF EXISTS SET PASSWORD 'password' SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED") {
    parsesTo[Statements](ast.AlterUser(
      literalFoo,
      isEncryptedPassword = Some(false),
      Some(password),
      ast.UserOptions(requirePasswordChange = Some(false), suspended = Some(true), None),
      ifExists = true
    )(pos))
  }

  test("ALTER USER foo SET HOME DATABASE db1") {
    parsesTo[Statements](ast.AlterUser(
      literalFoo,
      None,
      None,
      ast.UserOptions(None, None, Some(ast.SetHomeDatabaseAction(namespacedName("db1")))),
      ifExists = false
    )(pos))
  }

  test("ALTER USER foo SET HOME DATABASE $db") {
    parsesTo[Statements](ast.AlterUser(
      literalFoo,
      None,
      None,
      ast.UserOptions(None, None, Some(ast.SetHomeDatabaseAction(paramDb))),
      ifExists = false
    )(pos))
  }

  test("ALTER USER foo SET HOME DATABASE null") {
    parsesTo[Statements](ast.AlterUser(
      literalFoo,
      None,
      None,
      ast.UserOptions(None, None, Some(ast.SetHomeDatabaseAction(namespacedName("null")))),
      ifExists = false
    )(pos))
  }

  test("ALTER USER foo SET PASSWORD CHANGE REQUIRED SET HOME DATABASE db1") {
    parsesTo[Statements](ast.AlterUser(
      literalFoo,
      None,
      None,
      ast.UserOptions(
        requirePasswordChange = Some(true),
        suspended = None,
        Some(ast.SetHomeDatabaseAction(namespacedName("db1")))
      ),
      ifExists = false
    )(pos))
  }

  test("ALTER USER foo SET password 'password' SET HOME DATABASE db1") {
    parsesTo[Statements](ast.AlterUser(
      literalFoo,
      Some(false),
      Some(password),
      ast.UserOptions(None, None, Some(ast.SetHomeDatabaseAction(namespacedName("db1")))),
      ifExists = false
    )(pos))
  }

  test("ALTER USER foo SET password 'password' SET PASSWORD CHANGE NOT REQUIRED SET HOME DAtabase $db") {
    parsesTo[Statements](ast.AlterUser(
      literalFoo,
      Some(false),
      Some(password),
      ast.UserOptions(requirePasswordChange = Some(false), None, Some(ast.SetHomeDatabaseAction(paramDb))),
      ifExists = false
    )(pos))
  }

  test("ALTER USER foo SET HOME DATABASE `#dfkfop!`") {
    parsesTo[Statements](ast.AlterUser(
      literalFoo,
      None,
      None,
      ast.UserOptions(None, None, Some(ast.SetHomeDatabaseAction(namespacedName("#dfkfop!")))),
      ifExists = false
    )(pos))
  }

  test("ALTER USER foo REMOVE HOME DATABASE") {
    parsesTo[Statements](ast.AlterUser(
      literalFoo,
      None,
      None,
      ast.UserOptions(None, None, Some(ast.RemoveHomeDatabaseAction)),
      ifExists = false
    )(pos))
  }

  test("ALTER USER foo IF EXISTS REMOVE HOME DATABASE") {
    parsesTo[Statements](ast.AlterUser(
      literalFoo,
      None,
      None,
      ast.UserOptions(None, None, Some(ast.RemoveHomeDatabaseAction)),
      ifExists = true
    )(pos))
  }

  // clause ordering tests

  Seq("SET PASSWORD CHANGE REQUIRED", "SET STATUS ACTIVE", "SET HOME DATABASE db1").permutations.foreach {
    clauses =>
      test(s"ALTER USER foo ${clauses.mkString(" ")}") {
        parsesTo[Statements](ast.AlterUser(
          literalFoo,
          None,
          None,
          ast.UserOptions(Some(true), Some(false), Some(ast.SetHomeDatabaseAction(namespacedName("db1")))),
          ifExists = false
        )(pos))
      }
  }

  Seq(
    "SET PASSWORD 'password' CHANGE NOT REQUIRED",
    "SET STATUS ACTIVE",
    "SET HOME DATABASE db1"
  ).permutations.foreach {
    clauses =>
      test(s"ALTER USER foo ${clauses.mkString(" ")}") {
        parsesTo[Statements](ast.AlterUser(
          literalFoo,
          Some(false),
          Some(password),
          ast.UserOptions(Some(false), Some(false), Some(ast.SetHomeDatabaseAction(namespacedName("db1")))),
          ifExists = false
        )(pos))
      }
  }

  Seq(
    "SET PASSWORD 'password'",
    "SET PASSWORD CHANGE REQUIRED",
    "SET STATUS ACTIVE",
    "SET HOME DATABASE db1"
  ).permutations.foreach {
    clauses =>
      test(s"ALTER USER foo ${clauses.mkString(" ")}") {
        parsesTo[Statements](ast.AlterUser(
          literalFoo,
          Some(false),
          Some(password),
          ast.UserOptions(Some(true), Some(false), Some(ast.SetHomeDatabaseAction(namespacedName("db1")))),
          ifExists = false
        )(pos))
      }
  }

  // offset/position tests

  test("ALTER user command finds password literal at correct offset") {
    parsing[Statements]("ALTER USER foo SET PASSWORD 'password'").shouldVerify { statement =>
      val passwords = statement.folder.findAllByClass[SensitiveStringLiteral].map(l => (l.value, l.position.offset))
      passwords.foreach { case (pw, offset) =>
        withClue("Expecting password = password, offset = 28") {
          util.Arrays.equals(toUtf8Bytes("password"), pw) shouldBe true
          offset shouldBe 28
        }
      }
    }
  }

  test("ALTER user command finds password parameter at correct offset") {
    parsing[Statements](s"ALTER USER foo SET PASSWORD $pwParamString").shouldVerify { statement =>
      val passwords = statement.folder.findAllByClass[SensitiveParameter].map(p => (p.name, p.position.offset))
      passwords should equal(Seq("password" -> 28))
    }
  }

  // fails parsing

  test("ALTER USER foo") {
    failsParsing[Statements]
  }

  test("ALTER USER foo SET NAME bar") {
    val exceptionMessage =
      s"""Invalid input 'NAME': expected
         |  "ENCRYPTED"
         |  "HOME"
         |  "PASSWORD"
         |  "PLAINTEXT"
         |  "STATUS" (line 1, column 20 (offset: 19))""".stripMargin

    assertFailsWithMessage[Statements](testName, exceptionMessage)
  }

  test("ALTER USER foo SET PASSWORD 'secret' SET NAME bar") {
    val exceptionMessage =
      s"""Invalid input 'NAME': expected
         |  "ENCRYPTED"
         |  "HOME"
         |  "PASSWORD"
         |  "PLAINTEXT"
         |  "STATUS" (line 1, column 42 (offset: 41))""".stripMargin

    assertFailsWithMessage[Statements](testName, exceptionMessage)
  }

  test("ALTER USER foo RENAME TO bar") {
    failsParsing[Statements]
  }

  test("ALTER USER foo SET PASSWORD null") {
    failsParsing[Statements]
  }

  test("ALTER USER foo SET PASSWORD 123") {
    failsParsing[Statements]
  }

  test("ALTER USER foo SET PASSWORD") {
    failsParsing[Statements]
  }

  test("ALTER USER foo SET ENCRYPTED PASSWORD 123") {
    failsParsing[Statements]
  }

  test("ALTER USER foo SET PLAINTEXT PASSWORD") {
    failsParsing[Statements]
  }

  test("ALTER USER foo SET ENCRYPTED PASSWORD") {
    failsParsing[Statements]
  }

  test("ALTER USER foo SET PASSWORD 'password' SET ENCRYPTED PASSWORD") {
    assertFailsWithMessage[Statements](testName, "Duplicate SET PASSWORD clause (line 1, column 40 (offset: 39))")
  }

  test("ALTER USER foo SET PASSWORD 'password' ENCRYPTED") {
    failsParsing[Statements]
  }

  test("ALTER USER foo SET PASSWORD 'password' SET STATUS ACTIVE CHANGE NOT REQUIRED") {
    failsParsing[Statements]
  }

  test("ALTER USER foo SET STATUS") {
    failsParsing[Statements]
  }

  test("ALTER USER foo PASSWORD CHANGE NOT REQUIRED") {
    failsParsing[Statements]
  }

  test("ALTER USER foo CHANGE NOT REQUIRED") {
    failsParsing[Statements]
  }

  test("ALTER USER foo SET PASSWORD 'password' SET PASSWORD SET STATUS ACTIVE") {
    failsParsing[Statements]
  }

  test("ALTER USER foo SET PASSWORD STATUS ACTIVE") {
    failsParsing[Statements]
  }

  test("ALTER USER foo SET HOME DATABASE 123456") {
    failsParsing[Statements]
  }

  test("ALTER USER foo SET HOME DATABASE #dfkfop!") {
    failsParsing[Statements]
  }

  test("ALTER USER foo SET PASSWORD 'password' SET STATUS IMAGINARY") {
    failsParsing[Statements]
  }

  test("ALTER USER foo IF NOT EXISTS SET PASSWORD 'password'") {
    failsParsing[Statements]
  }

  test("ALTER USER foo SET STATUS SUSPENDED REMOVE HOME DATABASE") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input 'REMOVE': expected "SET" or <EOF> (line 1, column 37 (offset: 36))"""
    )
  }

  test("ALTER USER foo SET HOME DATABASE db1 REMOVE HOME DATABASE") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input 'REMOVE': expected ".", "SET" or <EOF> (line 1, column 38 (offset: 37))"""
    )
  }

  test("ALTER USER foo REMOVE HOME DATABASE SET PASSWORD CHANGE REQUIRED") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input 'SET': expected <EOF> (line 1, column 37 (offset: 36))"""
    )
  }

  test("ALTER USER foo SET DEFAULT DATABASE db1") {
    failsParsing[Statements]
  }

  test("ALTER USER foo REMOVE DEFAULT DATABASE") {
    failsParsing[Statements]
  }

  test("ALTER USER foo SET PASSWORD $password SET PASSWORD 'password'") {
    val exceptionMessage =
      s"""Duplicate SET PASSWORD clause (line 1, column 39 (offset: 38))""".stripMargin
    assertFailsWithMessage[Statements](testName, exceptionMessage)
  }

  test("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED SET PASSWORD CHANGE REQUIRED") {
    val exceptionMessage =
      s"""Duplicate SET PASSWORD CHANGE [NOT] REQUIRED clause (line 1, column 49 (offset: 48))""".stripMargin
    assertFailsWithMessage[Statements](testName, exceptionMessage)
  }

  test("ALTER USER foo SET STATUS ACTIVE SET STATUS SUSPENDED") {
    val exceptionMessage =
      s"""Duplicate SET STATUS {SUSPENDED|ACTIVE} clause (line 1, column 34 (offset: 33))""".stripMargin
    assertFailsWithMessage[Statements](testName, exceptionMessage)
  }

  test("ALTER USER foo SET HOME DATABASE db SET HOME DATABASE db") {
    val exceptionMessage =
      s"""Duplicate SET HOME DATABASE clause (line 1, column 37 (offset: 36))""".stripMargin
    assertFailsWithMessage[Statements](testName, exceptionMessage)
  }

  // Alter current user/Change own password

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO 'new'") {
    parsesTo[Statements](ast.SetOwnPassword(passwordNew, passwordCurrent)(pos))
  }

  test("alter current user set password from 'current' to ''") {
    parsesTo[Statements](ast.SetOwnPassword(passwordEmpty, passwordCurrent)(pos))
  }

  test("alter current user set password from '' to 'new'") {
    parsesTo[Statements](ast.SetOwnPassword(passwordNew, passwordEmpty)(pos))
  }

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO 'passWORD123%!'") {
    parsesTo[Statements](ast.SetOwnPassword(pw("passWORD123%!"), passwordCurrent)(pos))
  }

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO $newPassword") {
    parsesTo[Statements](ast.SetOwnPassword(paramPasswordNew, passwordCurrent)(pos))
  }

  test("ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO 'new'") {
    parsesTo[Statements](ast.SetOwnPassword(passwordNew, paramPasswordCurrent)(pos))
  }

  test("alter current user set password from $currentPassword to ''") {
    parsesTo[Statements](ast.SetOwnPassword(passwordEmpty, paramPasswordCurrent)(pos))
  }

  test("ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO 'passWORD123%!'") {
    parsesTo[Statements](ast.SetOwnPassword(pw("passWORD123%!"), paramPasswordCurrent)(pos))
  }

  test("ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword") {
    parsesTo[Statements](ast.SetOwnPassword(paramPasswordNew, paramPasswordCurrent)(pos))
  }

  // offset/position tests

  test("ALTER CURRENT USER command finds password literal at correct offset") {
    parsing[Statements]("ALTER CURRENT USER SET PASSWORD FROM 'current' TO 'new'").shouldVerify { statement =>
      val passwords = statement.folder.findAllByClass[SensitiveStringLiteral].map(l =>
        (new String(l.value, UTF_8), l.position.offset)
      )
      passwords.toSet should equal(Set("current" -> 37, "new" -> 50))
    }
  }

  test("ALTER CURRENT USER command finds password parameter at correct offset") {
    parsing[Statements]("ALTER CURRENT USER SET PASSWORD FROM $current TO $new").shouldVerify { statement =>
      val passwords = statement.folder.findAllByClass[SensitiveParameter].map(p => (p.name, p.position.offset))
      passwords.toSet should equal(Set("current" -> 37, "new" -> 49))
    }
  }

  // fails parsing

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO null") {
    failsParsing[Statements]
  }

  test("ALTER CURRENT USER SET PASSWORD FROM $current TO 123") {
    failsParsing[Statements]
  }

  test("ALTER PASSWORD FROM 'current' TO 'new'") {
    failsParsing[Statements]
  }

  test("ALTER CURRENT PASSWORD FROM 'current' TO 'new'") {
    failsParsing[Statements]
  }

  test("ALTER CURRENT USER PASSWORD FROM 'current' TO 'new'") {
    failsParsing[Statements]
  }

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO") {
    failsParsing[Statements]
  }

  test("ALTER CURRENT USER SET PASSWORD FROM TO 'new'") {
    failsParsing[Statements]
  }

  test("ALTER CURRENT USER SET PASSWORD TO 'new'") {
    failsParsing[Statements]
  }
}
