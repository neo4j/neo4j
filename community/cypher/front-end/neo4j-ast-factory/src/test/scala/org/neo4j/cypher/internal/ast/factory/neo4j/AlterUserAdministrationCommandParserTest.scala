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
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Antlr
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.JavaCc
import org.neo4j.cypher.internal.expressions.SensitiveParameter
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral
import org.neo4j.exceptions.SyntaxException

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
      .parseIn(JavaCc)(_.withMessage(
        "Invalid input '': expected \"IF\", \"REMOVE\" or \"SET\" (line 1, column 15 (offset: 14))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input '': expected 'IF', 'SET', 'REMOVE' (line 1, column 15 (offset: 14))
          |"ALTER USER foo"
          |               ^""".stripMargin
      ))
  }

  test("ALTER USER foo SET NAME bar") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessage(
        s"""Invalid input 'NAME': expected
           |  "ENCRYPTED"
           |  "HOME"
           |  "PASSWORD"
           |  "PLAINTEXT"
           |  "STATUS" (line 1, column 20 (offset: 19))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        "Mismatched input 'NAME': expected 'ENCRYPTED', 'PLAINTEXT', 'PASSWORD', 'STATUS', 'HOME' (line 1, column 20 (offset: 19))"
      ))
  }

  test("ALTER USER foo SET PASSWORD 'secret' SET NAME bar") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessage(
        s"""Invalid input 'NAME': expected
           |  "ENCRYPTED"
           |  "HOME"
           |  "PASSWORD"
           |  "PLAINTEXT"
           |  "STATUS" (line 1, column 42 (offset: 41))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        "Mismatched input 'NAME': expected 'ENCRYPTED', 'PLAINTEXT', 'PASSWORD', 'STATUS', 'HOME' (line 1, column 42 (offset: 41))"
      ))
  }

  test("ALTER USER foo RENAME TO bar") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessage(
        "Invalid input 'RENAME': expected \"IF\", \"REMOVE\" or \"SET\" (line 1, column 16 (offset: 15))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        "Mismatched input 'RENAME': expected 'IF', 'SET', 'REMOVE' (line 1, column 16 (offset: 15))"
      ))
  }

  test("ALTER USER foo SET PASSWORD null") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessage(
        "Invalid input 'null': expected \"CHANGE\", \"\\\"\", \"\\'\" or a parameter (line 1, column 29 (offset: 28))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        "No viable alternative (line 1, column 29 (offset: 28))"
      ))
  }

  test("ALTER USER foo SET PASSWORD 123") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessage(
        "Invalid input '123': expected \"CHANGE\", \"\\\"\", \"\\'\" or a parameter (line 1, column 29 (offset: 28))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        "No viable alternative (line 1, column 29 (offset: 28))"
      ))
  }

  test("ALTER USER foo SET PASSWORD") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessage(
        "Invalid input '': expected \"CHANGE\", \"\\\"\", \"\\'\" or a parameter (line 1, column 28 (offset: 27))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        "No viable alternative (line 1, column 28 (offset: 27))"
      ))
  }

  test("ALTER USER foo SET ENCRYPTED PASSWORD 123") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessage(
        "Invalid input '123': expected \"\\\"\", \"\\'\" or a parameter (line 1, column 39 (offset: 38))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        "Mismatched input '123': expected a string value, '$' (line 1, column 39 (offset: 38))"
      ))
  }

  test("ALTER USER foo SET PLAINTEXT PASSWORD") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected \"\\\"\", \"\\'\" or a parameter (line 1, column 38 (offset: 37))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        """Mismatched input '': expected a string value, '$' (line 1, column 38 (offset: 37))"""
      ))
  }

  test("ALTER USER foo SET ENCRYPTED PASSWORD") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected \"\\\"\", \"\\'\" or a parameter (line 1, column 38 (offset: 37))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        """Mismatched input '': expected a string value, '$' (line 1, column 38 (offset: 37))"""
      ))
  }

  test("ALTER USER foo SET PASSWORD 'password' SET ENCRYPTED PASSWORD") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Duplicate SET PASSWORD clause (line 1, column 40 (offset: 39))"))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        """Mismatched input '': expected a string value, '$' (line 1, column 62 (offset: 61))
          |"ALTER USER foo SET PASSWORD 'password' SET ENCRYPTED PASSWORD"
          |                                                              ^""".stripMargin
      ))
  }

  test("ALTER USER foo SET PASSWORD 'password' SET ENCRYPTED PASSWORD 'password'") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Duplicate SET PASSWORD clause (line 1, column 40 (offset: 39))"))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        "Duplicate SET PASSWORD clause (line 1, column 44 (offset: 43))"
      ))
  }

  test("ALTER USER foo SET PASSWORD 'password' ENCRYPTED") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input 'ENCRYPTED'"))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart("Extraneous input 'ENCRYPTED'"))
  }

  test("ALTER USER foo SET PASSWORD 'password' SET STATUS ACTIVE CHANGE NOT REQUIRED") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'CHANGE'"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        "Mismatched input 'CHANGE': expected ';', <EOF> (line 1, column 58 (offset: 57))"
      ))
  }

  test("ALTER USER foo SET STATUS") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input '': expected \"ACTIVE\" or \"SUSPENDED\""))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Missing 'ACTIVE', 'SUSPENDED' at '' (line 1, column 26 (offset: 25))
          |"ALTER USER foo SET STATUS"
          |                          ^""".stripMargin
      ))
  }

  test("ALTER USER foo PASSWORD CHANGE NOT REQUIRED") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input 'PASSWORD'"))
      .parseIn(Antlr)(
        _.throws[SyntaxException].withMessageStart("Mismatched input 'PASSWORD': expected 'IF', 'SET', 'REMOVE'")
      )
  }

  test("ALTER USER foo CHANGE NOT REQUIRED") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input 'CHANGE'"))
      .parseIn(Antlr)(
        _.throws[SyntaxException].withMessageStart("Mismatched input 'CHANGE': expected 'IF', 'SET', 'REMOVE'")
      )
  }

  test("ALTER USER foo SET PASSWORD 'password' SET PASSWORD SET STATUS ACTIVE") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input 'SET'"))
      .parseIn(Antlr)(
        _.throws[SyntaxException].withMessageStart("No viable alternative (line 1, column 53 (offset: 52))")
      )
  }

  test("ALTER USER foo SET PASSWORD STATUS ACTIVE") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input 'STATUS'"))
      .parseIn(Antlr)(
        _.throws[SyntaxException].withMessageStart("No viable alternative (line 1, column 29 (offset: 28))")
      )
  }

  test("ALTER USER foo SET HOME DATABASE 123456") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input '123456'"))
      .parseIn(Antlr)(
        _.throws[SyntaxException].withMessageStart("Mismatched input '123456': expected an identifier, '$'")
      )
  }

  test("ALTER USER foo SET HOME DATABASE #dfkfop!") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input '#'"))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart("Extraneous input '#': expected an identifier, '$'"))
  }

  test("ALTER USER foo SET PASSWORD 'password' SET STATUS IMAGINARY") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input 'IMAGINARY'"))
      .parseIn(Antlr)(
        _.throws[SyntaxException].withMessageStart("Mismatched input 'IMAGINARY': expected 'ACTIVE', 'SUSPENDED'")
      )
  }

  test("ALTER USER foo IF NOT EXISTS SET PASSWORD 'password'") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input 'NOT'"))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart("Extraneous input 'NOT': expected 'EXISTS'"))
  }

  test("ALTER USER foo SET STATUS SUSPENDED REMOVE HOME DATABASE") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'REMOVE': expected "SET" or <EOF> (line 1, column 37 (offset: 36))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        "Mismatched input 'REMOVE': expected ';', <EOF>"
      ))
  }

  test("ALTER USER foo SET HOME DATABASE db1 REMOVE HOME DATABASE") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'REMOVE': expected ".", "SET" or <EOF> (line 1, column 38 (offset: 37))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
        "Mismatched input 'REMOVE': expected ';', <EOF>"
      ))
  }

  test("ALTER USER foo REMOVE HOME DATABASE SET PASSWORD CHANGE REQUIRED") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input 'SET': expected <EOF> (line 1, column 37 (offset: 36))"))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart("Mismatched input 'SET'"))
  }

  test("ALTER USER foo SET DEFAULT DATABASE db1") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input 'DEFAULT': expected"))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart("Mismatched input 'DEFAULT'"))
  }

  test("ALTER USER foo REMOVE DEFAULT DATABASE") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input 'DEFAULT'"))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input 'DEFAULT': expected 'HOME' (line 1, column 23 (offset: 22))
          |"ALTER USER foo REMOVE DEFAULT DATABASE"
          |                       ^""".stripMargin
      ))
  }

  test("ALTER USER foo SET PASSWORD $password SET PASSWORD 'password'") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessage(
        "Duplicate SET PASSWORD clause (line 1, column 39 (offset: 38))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Duplicate SET PASSWORD clause (line 1, column 43 (offset: 42))
          |"ALTER USER foo SET PASSWORD $password SET PASSWORD 'password'"
          |                                           ^""".stripMargin
      ))
  }

  test("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED SET PASSWORD CHANGE REQUIRED") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessage(
        "Duplicate SET PASSWORD CHANGE [NOT] REQUIRED clause (line 1, column 49 (offset: 48))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Duplicate SET PASSWORD CHANGE [NOT] REQUIRED clause (line 1, column 53 (offset: 52))
          |"ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED SET PASSWORD CHANGE REQUIRED"
          |                                                     ^""".stripMargin
      ))
  }

  test("ALTER USER foo SET STATUS ACTIVE SET STATUS SUSPENDED") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessage(
        "Duplicate SET STATUS {SUSPENDED|ACTIVE} clause (line 1, column 34 (offset: 33))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Duplicate SET STATUS {SUSPENDED|ACTIVE} clause (line 1, column 38 (offset: 37))
          |"ALTER USER foo SET STATUS ACTIVE SET STATUS SUSPENDED"
          |                                      ^""".stripMargin
      ))
  }

  test("ALTER USER foo SET HOME DATABASE db SET HOME DATABASE db") {
    failsParsing[Statements]
      .parseIn(JavaCc)(_.withMessage(
        "Duplicate SET HOME DATABASE clause (line 1, column 37 (offset: 36))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Duplicate SET HOME DATABASE clause (line 1, column 41 (offset: 40))
          |"ALTER USER foo SET HOME DATABASE db SET HOME DATABASE db"
          |                                         ^""".stripMargin
      ))
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
      .parseIn(JavaCc)(_.withMessageStart("Invalid input 'null': expected \"\\\"\", \"\\'\" or a parameter"))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input 'null': expected a string value, '$' (line 1, column 51 (offset: 50))
          |"ALTER CURRENT USER SET PASSWORD FROM 'current' TO null"
          |                                                   ^""".stripMargin
      ))
  }

  test("ALTER CURRENT USER SET PASSWORD FROM $current TO 123") {
    failsParsing[Statements]
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input '123': expected a string value, '$' (line 1, column 50 (offset: 49))
          |"ALTER CURRENT USER SET PASSWORD FROM $current TO 123"
          |                                                  ^""".stripMargin
      ))
  }

  test("ALTER PASSWORD FROM 'current' TO 'new'") {
    failsParsing[Statements]
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input 'PASSWORD': expected 'ALIAS', 'CURRENT', 'DATABASE', 'USER', 'SERVER' (line 1, column 7 (offset: 6))
          |"ALTER PASSWORD FROM 'current' TO 'new'"
          |       ^""".stripMargin
      ))
  }

  test("ALTER CURRENT PASSWORD FROM 'current' TO 'new'") {
    failsParsing[Statements]
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input 'PASSWORD': expected 'USER' (line 1, column 15 (offset: 14))
          |"ALTER CURRENT PASSWORD FROM 'current' TO 'new'"
          |               ^""".stripMargin
      ))
  }

  test("ALTER CURRENT USER PASSWORD FROM 'current' TO 'new'") {
    failsParsing[Statements]
      .parseIn(Antlr)(
        _.throws[SyntaxException].withMessage("""Missing 'SET' at 'PASSWORD' (line 1, column 20 (offset: 19))
                                                |"ALTER CURRENT USER PASSWORD FROM 'current' TO 'new'"
                                                |                    ^""".stripMargin)
      )
  }

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO") {
    failsParsing[Statements]
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input '': expected a string value, '$' (line 1, column 50 (offset: 49))
          |"ALTER CURRENT USER SET PASSWORD FROM 'current' TO"
          |                                                  ^""".stripMargin
      ))
  }

  test("ALTER CURRENT USER SET PASSWORD FROM TO 'new'") {
    failsParsing[Statements]
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Extraneous input 'TO': expected a string value, '$' (line 1, column 38 (offset: 37))
          |"ALTER CURRENT USER SET PASSWORD FROM TO 'new'"
          |                                      ^""".stripMargin
      ))
  }

  test("ALTER CURRENT USER SET PASSWORD TO 'new'") {
    failsParsing[Statements]
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input 'TO': expected 'FROM' (line 1, column 33 (offset: 32))
          |"ALTER CURRENT USER SET PASSWORD TO 'new'"
          |                                 ^""".stripMargin
      ))
  }
}
