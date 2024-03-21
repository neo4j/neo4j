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
import org.neo4j.cypher.internal.ast.ParameterName
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.SensitiveParameter
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral

import java.nio.charset.StandardCharsets.UTF_8
import java.util

class UserAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {
  private val userString = "user"
  private val varUser = varFor(userString)
  private val password = pw("password")
  private val passwordNew = pw("new")
  private val passwordCurrent = pw("current")
  private val passwordEmpty = pw("")
  private val paramPassword: Parameter = pwParam("password")
  private val paramPasswordNew: Parameter = pwParam("newPassword")
  private val paramPasswordCurrent: Parameter = pwParam("currentPassword")
  private val paramDb: ParameterName = stringParamName("db")
  private val pwParamString = s"$$password"
  private val paramString = s"$$param"
  private val paramAst: Parameter = stringParam("param")

  //  Showing user
  test("SHOW USERS") {
    yields[Statements](ast.ShowUsers(None))
  }

  test("SHOW USER") {
    yields[Statements](ast.ShowUsers(None))
  }

  test("USE system SHOW USERS") {
    yields[Statements](ast.ShowUsers(None))
  }

  test("SHOW USERS WHERE user = 'GRANTED'") {
    yields[Statements](ast.ShowUsers(Some(Right(where(equals(varUser, grantedString))))))
  }

  test("SHOW USER WHERE user = 'GRANTED'") {
    yields[Statements](ast.ShowUsers(Some(Right(where(equals(varUser, grantedString))))))
  }

  test("SHOW USERS WHERE user = 'GRANTED' AND action = 'match'") {
    val accessPredicate = equals(varUser, grantedString)
    val matchPredicate = equals(varFor(actionString), literalString("match"))
    yields[Statements](ast.ShowUsers(Some(Right(where(and(accessPredicate, matchPredicate))))))
  }

  test("SHOW USERS WHERE user = 'GRANTED' OR action = 'match'") {
    val accessPredicate = equals(varUser, grantedString)
    val matchPredicate = equals(varFor(actionString), literalString("match"))
    yields[Statements](ast.ShowUsers(Some(Right(where(or(accessPredicate, matchPredicate))))))
  }

  test("SHOW USERS YIELD user ORDER BY user") {
    val columns = yieldClause(returnItems(variableReturnItem(userString)), Some(orderBy(sortItem(varUser))))
    yields[Statements](ast.ShowUsers(Some(Left((columns, None)))))
  }

  test("SHOW USERS YIELD user ORDER BY user WHERE user ='none'") {
    val orderByClause = orderBy(sortItem(varUser))
    val whereClause = where(equals(varUser, noneString))
    val columns =
      yieldClause(returnItems(variableReturnItem(userString)), Some(orderByClause), where = Some(whereClause))
    yields[Statements](ast.ShowUsers(Some(Left((columns, None)))))
  }

  test("SHOW USERS YIELD user ORDER BY user SKIP 1 LIMIT 10 WHERE user ='none'") {
    val orderByClause = orderBy(sortItem(varUser))
    val whereClause = where(equals(varUser, noneString))
    val columns = yieldClause(
      returnItems(variableReturnItem(userString)),
      Some(orderByClause),
      Some(skip(1)),
      Some(limit(10)),
      Some(whereClause)
    )
    yields[Statements](ast.ShowUsers(Some(Left((columns, None)))))
  }

  test("SHOW USERS YIELD user SKIP -1") {
    val columns = yieldClause(returnItems(variableReturnItem(userString)), skip = Some(skip(-1)))
    yields[Statements](ast.ShowUsers(Some(Left((columns, None)))))
  }

  test("SHOW USERS YIELD user RETURN user ORDER BY user") {
    yields[Statements](ast.ShowUsers(
      Some(Left((
        yieldClause(returnItems(variableReturnItem(userString))),
        Some(returnClause(returnItems(variableReturnItem(userString)), Some(orderBy(sortItem(varUser)))))
      )))
    ))
  }

  test("SHOW USERS YIELD user, suspended as suspended WHERE suspended RETURN DISTINCT user") {
    val suspendedVar = varFor("suspended")
    yields[Statements](ast.ShowUsers(
      Some(Left((
        yieldClause(
          returnItems(variableReturnItem(userString), aliasedReturnItem(suspendedVar)),
          where = Some(where(suspendedVar))
        ),
        Some(returnClause(returnItems(variableReturnItem(userString)), distinct = true))
      )))
    ))
  }

  test("SHOW USERS YIELD * RETURN *") {
    yields[Statements](ast.ShowUsers(Some(Left((yieldClause(returnAllItems), Some(returnClause(returnAllItems)))))))
  }

  test("SHOW USERS YIELD *") {
    yields[Statements](ast.ShowUsers(Some(Left((yieldClause(returnAllItems), None)))))
  }

  test("SHOW USER YIELD *") {
    yields[Statements](ast.ShowUsers(Some(Left((yieldClause(returnAllItems), None)))))
  }

  test("SHOW USERS YIELD *,blah RETURN user") {
    failsToParse[Statements]
  }

  test("SHOW USERS YIELD (123 + xyz)") {
    failsToParse[Statements]
  }

  test("SHOW USERS YIELD (123 + xyz) AS foo") {
    failsToParse[Statements]
  }

  // Showing current user

  test("SHOW CURRENT USER") {
    yields[Statements](ast.ShowCurrentUser(None))
  }

  test("SHOW CURRENT USER YIELD * WHERE suspended = false RETURN roles") {
    val suspendedVar = varFor("suspended")
    val yield_ = yieldClause(returnAllItems, where = Some(where(equals(suspendedVar, falseLiteral))))
    val return_ = returnClause(returnItems(variableReturnItem("roles")))
    val yieldOrWhere = Some(Left(yield_ -> Some(return_)))
    yields[Statements](ast.ShowCurrentUser(yieldOrWhere))
  }

  test("SHOW CURRENT USER YIELD *") {
    yields[Statements](ast.ShowCurrentUser(Some(Left((yieldClause(returnAllItems), None)))))
  }

  test("SHOW CURRENT USER WHERE user = 'GRANTED'") {
    yields[Statements](ast.ShowCurrentUser(Some(Right(where(equals(varUser, grantedString))))))
  }

  test("SHOW CURRENT USERS") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input 'USERS': expected "USER" (line 1, column 14 (offset: 13))"""
    )
  }

  test("SHOW CURRENT USERS YIELD *") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input 'USERS': expected "USER" (line 1, column 14 (offset: 13))"""
    )
  }

  test("SHOW CURRENT USERS WHERE user = 'GRANTED'") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input 'USERS': expected "USER" (line 1, column 14 (offset: 13))"""
    )
  }

  //  Creating user

  test("CREATE USER foo SET PASSWORD 'password'") {
    parsesTo[Statements](
      ast.CreateUser(
        literalFoo,
        isEncryptedPassword = false,
        password,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsThrowError
      )(pos)
    )
  }

  test("CREATE USER $foo SET PASSWORD 'password'") {
    parsesTo[Statements](
      ast.CreateUser(
        paramFoo,
        isEncryptedPassword = false,
        password,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsThrowError
      )(pos)
    )
  }

  test("CREATE USER foo SET PLAINTEXT PASSWORD 'password'") {
    parsesTo[Statements](
      ast.CreateUser(
        literalFoo,
        isEncryptedPassword = false,
        password,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsThrowError
      )(pos)
    )
  }

  test(s"CREATE USER foo SET PLAINTEXT PASSWORD $pwParamString") {
    parsesTo[Statements](
      ast.CreateUser(
        literalFoo,
        isEncryptedPassword = false,
        paramPassword,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsThrowError
      )(pos)
    )
  }

  test(s"CREATE USER $paramString SET PASSWORD $pwParamString") {
    parsesTo[Statements](
      ast.CreateUser(
        paramAst,
        isEncryptedPassword = false,
        paramPassword,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsThrowError
      )(pos)
    )
  }

  test("CREATE USER `foo` SET PASSwORD 'password'") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER `!#\"~` SeT PASSWORD 'password'") {
    parsesTo[Statements](ast.CreateUser(
      literal("!#\"~"),
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER foo SeT PASSWORD 'pasS5Wor%d'") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      pw("pasS5Wor%d"),
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER foo SET PASSwORD ''") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      passwordEmpty,
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test(s"CREATE uSER foo SET PASSWORD $pwParamString") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      paramPassword,
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREaTE USER foo SET PASSWORD 'password' CHANGE REQUIRED") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test(s"CREATE USER foo SET PASSWORD $pwParamString CHANGE REQUIRED") {
    parsesTo[Statements](
      ast.CreateUser(
        literalFoo,
        isEncryptedPassword = false,
        paramPassword,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsThrowError
      )(pos)
    )
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE required") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER foo SET PASSWORD 'password' CHAngE NOT REQUIRED") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(false), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE NOT REQUIRED") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(false), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test(s"CREATE USER foo SET PASSWORD $pwParamString SET  PASSWORD CHANGE NOT REQUIRED") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      paramPassword,
      ast.UserOptions(Some(false), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS SUSPENDed") {
    parsesTo[Statements](
      ast.CreateUser(
        literalFoo,
        isEncryptedPassword = false,
        password,
        ast.UserOptions(Some(true), Some(true), None),
        ast.IfExistsThrowError
      )(pos)
    )
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS ACtiVE") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), Some(false), None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE NOT REQUIRED SET   STATuS SUSPENDED") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(false), Some(true), None),
      ast.IfExistsThrowError
    )(pos))
  }

  test(s"CREATE USER foo SET PASSWORD $pwParamString CHANGE REQUIRED SET STATUS SUSPENDED") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      paramPassword,
      ast.UserOptions(Some(true), Some(true), None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER `` SET PASSwORD 'password'") {
    parsesTo[Statements](ast.CreateUser(
      literalEmpty,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER `f:oo` SET PASSWORD 'password'") {
    parsesTo[Statements](ast.CreateUser(
      literalFColonOo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD 'password'") {
    parsesTo[Statements](
      ast.CreateUser(
        literalFoo,
        isEncryptedPassword = false,
        password,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsDoNothing
      )(pos)
    )
  }

  test(s"CREATE uSER foo IF NOT EXISTS SET PASSWORD $pwParamString") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      paramPassword,
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsDoNothing
    )(pos))
  }

  test(s"CREATE USER foo IF NOT EXISTS SET PASSWORD $pwParamString CHANGE REQUIRED") {
    parsesTo[Statements](
      ast.CreateUser(
        literalFoo,
        isEncryptedPassword = false,
        paramPassword,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsDoNothing
      )(pos)
    )
  }

  test(s"CREATE USER foo IF NOT EXISTS SET PASSWORD $pwParamString SET STATUS SUSPENDED") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      paramPassword,
      ast.UserOptions(Some(true), Some(true), None),
      ast.IfExistsDoNothing
    )(pos))
  }

  test(s"CREATE USER foo IF NOT EXISTS SET PASSWORD $pwParamString CHANGE REQUIRED SET STATUS SUSPENDED") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      paramPassword,
      ast.UserOptions(Some(true), Some(true), None),
      ast.IfExistsDoNothing
    )(pos))
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD 'password'") {
    parsesTo[Statements](
      ast.CreateUser(
        literalFoo,
        isEncryptedPassword = false,
        password,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsReplace
      )(pos)
    )
  }

  test(s"CREATE OR REPLACE uSER foo SET PASSWORD $pwParamString") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      paramPassword,
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsReplace
    )(pos))
  }

  test(s"CREATE OR REPLACE USER foo SET PASSWORD $pwParamString CHANGE REQUIRED") {
    parsesTo[Statements](
      ast.CreateUser(
        literalFoo,
        isEncryptedPassword = false,
        paramPassword,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsReplace
      )(pos)
    )
  }

  test(s"CREATE OR REPLACE USER foo SET PASSWORD $pwParamString SET STATUS SUSPENDED") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      paramPassword,
      ast.UserOptions(Some(true), Some(true), None),
      ast.IfExistsReplace
    )(pos))
  }

  test(s"CREATE OR REPLACE USER foo SET PASSWORD $pwParamString CHANGE REQUIRED SET STATUS SUSPENDED") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      paramPassword,
      ast.UserOptions(Some(true), Some(true), None),
      ast.IfExistsReplace
    )(pos))
  }

  test("CREATE OR REPLACE USER foo IF NOT EXISTS SET PASSWORD 'password'") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsInvalidSyntax
    )(pos))
  }

  test(
    "CREATE USER foo SET ENCRYPTED PASSWORD '1,04773b8510aea96ca2085cb81764b0a2,75f4201d047191c17c5e236311b7c4d77e36877503fe60b1ca6d4016160782ab'"
  ) {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = true,
      pw("1,04773b8510aea96ca2085cb81764b0a2,75f4201d047191c17c5e236311b7c4d77e36877503fe60b1ca6d4016160782ab"),
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER $foo SET encrYPTEd PASSWORD 'password'") {
    parsesTo[Statements](
      ast.CreateUser(
        paramFoo,
        isEncryptedPassword = true,
        password,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsThrowError
      )(pos)
    )
  }

  test(s"CREATE USER $paramString SET ENCRYPTED Password $pwParamString") {
    parsesTo[Statements](
      ast.CreateUser(
        paramAst,
        isEncryptedPassword = true,
        paramPassword,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsThrowError
      )(pos)
    )
  }

  test("CREATE OR REPLACE USER foo SET encrypted password 'sha256,x1024,0x2460294fe,b3ddb287a'") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = true,
      pw("sha256,x1024,0x2460294fe,b3ddb287a"),
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsReplace
    )(pos))
  }

  test("CREATE USER foo SET password 'password' SET HOME DATABASE db1") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, Some(ast.SetHomeDatabaseAction(namespacedName("db1")))),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER foo SET password 'password' SET HOME DATABASE $db") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, Some(ast.SetHomeDatabaseAction(paramDb))),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE OR REPLACE USER foo SET password 'password' SET HOME DATABASE db1") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, Some(ast.SetHomeDatabaseAction(namespacedName("db1")))),
      ast.IfExistsReplace
    )(pos))
  }

  test("CREATE USER foo IF NOT EXISTS SET password 'password' SET HOME DATABASE db1") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, Some(ast.SetHomeDatabaseAction(namespacedName("db1")))),
      ast.IfExistsDoNothing
    )(pos))
  }

  test("CREATE USER foo SET password 'password' SET PASSWORD CHANGE NOT REQUIRED SET HOME DAtabase $db") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(false), None, Some(ast.SetHomeDatabaseAction(paramDb))),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER foo SET password 'password' SET HOME DATABASE `#dfkfop!`") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, Some(ast.SetHomeDatabaseAction(namespacedName("#dfkfop!")))),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER foo SET password 'password' SET HOME DATABASE null") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, Some(ast.SetHomeDatabaseAction(namespacedName("null")))),
      ast.IfExistsThrowError
    )(pos))
  }

  Seq(
    ("CHANGE REQUIRED", "SET STATUS ACTIVE", "SET HOME DATABASE db1"),
    ("CHANGE REQUIRED", "SET HOME DATABASE db1", "SET STATUS ACTIVE")
  ).foreach {
    case (first: String, second: String, third: String) =>
      test(s"CREATE USER foo SET password 'password' $first $second $third") {
        parsesTo[Statements](ast.CreateUser(
          literalFoo,
          isEncryptedPassword = false,
          password,
          ast.UserOptions(Some(true), Some(false), Some(ast.SetHomeDatabaseAction(namespacedName("db1")))),
          ast.IfExistsThrowError
        )(pos))
      }
  }

  Seq("SET PASSWORD CHANGE REQUIRED", "SET STATUS ACTIVE", "SET HOME DATABASE db1")
    .permutations.foreach {
      clauses =>
        test(s"CREATE USER foo SET password 'password' ${clauses.mkString(" ")}") {
          parsesTo[Statements](ast.CreateUser(
            literalFoo,
            isEncryptedPassword = false,
            password,
            ast.UserOptions(Some(true), Some(false), Some(ast.SetHomeDatabaseAction(namespacedName("db1")))),
            ast.IfExistsThrowError
          )(pos))
        }
    }

  test("CREATE command finds password literal at correct offset") {
    parsing[Statements]("CREATE USER foo SET PASSWORD 'password'").shouldVerify { statement =>
      val passwords = statement.folder.findAllByClass[SensitiveStringLiteral].map(l => (l.value, l.position.offset))
      passwords.foreach { case (pw, offset) =>
        withClue("Expecting password = password, offset = 29") {
          util.Arrays.equals(toUtf8Bytes("password"), pw) shouldBe true
          offset shouldBe 29
        }
      }
    }
  }

  test("CREATE command finds password parameter at correct offset") {
    parsing[Statements](s"CREATE USER foo SET PASSWORD $pwParamString").shouldVerify { statement =>
      val passwords = statement.folder.findAllByClass[SensitiveParameter].map(p => (p.name, p.position.offset))
      passwords should equal(Seq("password" -> 29))
    }
  }

  test("CREATE USER foo") {
    failsParsing[Statements]
  }

  test("CREATE USER \"foo\" SET PASSwORD 'password'") {
    failsParsing[Statements]
  }

  test("CREATE USER !#\"~ SeT PASSWORD 'password'") {
    failsParsing[Statements]
  }

  test("CREATE USER fo,o SET PASSWORD 'password'") {
    failsParsing[Statements]
  }

  test("CREATE USER f:oo SET PASSWORD 'password'") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET ENCRYPTED PASSWORD 123") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET ENCRYPTED PASSWORD") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PLAINTEXT PASSWORD") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD 'password' SET ENCRYPTED PASSWORD") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD 'password' ENCRYPTED") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSwORD 'passwordString'+" + pwParamString + "expressions.Parameter") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD null CHANGE REQUIRED") {
    failsParsing[Statements]
  }

  test("CREATE USER foo PASSWORD 'password'") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS ACTIVE CHANGE NOT REQUIRED") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD 'password' SET HOME DATABASE db1 CHANGE NOT REQUIRED") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD 'password' SET DEFAULT DATABASE db1") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STAUS ACTIVE") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input 'STAUS': expected \"HOME\", \"PASSWORD\" or \"STATUS\" (line 1, column 45 (offset: 44))"
    )
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS IMAGINARY") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD CHANGE REQUIRED") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET STATUS SUSPENDED") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD CHANGE REQUIRED SET STATUS ACTIVE") {
    failsParsing[Statements]
  }

  test("CREATE USER foo IF EXISTS SET PASSWORD 'bar'") {
    failsParsing[Statements]
  }

  test("CREATE USER foo IF NOT EXISTS") {
    failsParsing[Statements]
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD") {
    failsParsing[Statements]
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD CHANGE REQUIRED") {
    failsParsing[Statements]
  }

  test("CREATE USER foo IF NOT EXISTS SET STATUS ACTIVE") {
    failsParsing[Statements]
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED") {
    failsParsing[Statements]
  }

  test("CREATE OR REPLACE USER foo") {
    failsParsing[Statements]
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD") {
    failsParsing[Statements]
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD CHANGE NOT REQUIRED") {
    failsParsing[Statements]
  }

  test("CREATE OR REPLACE USER foo SET STATUS SUSPENDED") {
    failsParsing[Statements]
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD CHANGE REQUIRED SET STATUS ACTIVE") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD 'bar' SET HOME DATABASE 123456") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD 'bar' SET HOME DATABASE #dfkfop!") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD $password CHANGE NOT REQUIRED SET PASSWORD CHANGE REQUIRED") {
    val exceptionMessage =
      s"""Duplicate SET PASSWORD CHANGE [NOT] REQUIRED clause (line 1, column 60 (offset: 59))""".stripMargin
    assertFailsWithMessage[Statements](testName, exceptionMessage)
  }

  test("CREATE USER foo SET PASSWORD $password SET STATUS ACTIVE SET STATUS SUSPENDED") {
    val exceptionMessage =
      s"""Duplicate SET STATUS {SUSPENDED|ACTIVE} clause (line 1, column 58 (offset: 57))""".stripMargin
    assertFailsWithMessage[Statements](testName, exceptionMessage)
  }

  test("CREATE USER foo SET PASSWORD $password SET HOME DATABASE db SET HOME DATABASE db") {
    val exceptionMessage =
      s"""Duplicate SET HOME DATABASE clause (line 1, column 61 (offset: 60))""".stripMargin
    assertFailsWithMessage[Statements](testName, exceptionMessage)
  }

  // Renaming role

  test("RENAME USER foo TO bar") {
    parsesTo[Statements](ast.RenameUser(literalFoo, literalBar, ifExists = false)(pos))
  }

  test("RENAME USER foo TO $bar") {
    parsesTo[Statements](ast.RenameUser(literalFoo, stringParam("bar"), ifExists = false)(pos))
  }

  test("RENAME USER $foo TO bar") {
    parsesTo[Statements](ast.RenameUser(stringParam("foo"), literalBar, ifExists = false)(pos))
  }

  test("RENAME USER $foo TO $bar") {
    parsesTo[Statements](ast.RenameUser(stringParam("foo"), stringParam("bar"), ifExists = false)(pos))
  }

  test("RENAME USER foo IF EXISTS TO bar") {
    parsesTo[Statements](ast.RenameUser(literalFoo, literalBar, ifExists = true)(pos))
  }

  test("RENAME USER foo IF EXISTS TO $bar") {
    parsesTo[Statements](ast.RenameUser(literalFoo, stringParam("bar"), ifExists = true)(pos))
  }

  test("RENAME USER $foo IF EXISTS TO bar") {
    parsesTo[Statements](ast.RenameUser(stringParam("foo"), literalBar, ifExists = true)(pos))
  }

  test("RENAME USER $foo IF EXISTS TO $bar") {
    parsesTo[Statements](ast.RenameUser(stringParam("foo"), stringParam("bar"), ifExists = true)(pos))
  }

  test("RENAME USER foo TO ``") {
    parsesTo[Statements](ast.RenameUser(literalFoo, literalEmpty, ifExists = false)(pos))
  }

  test("RENAME USER `` TO bar") {
    parsesTo[Statements](ast.RenameUser(literalEmpty, literalBar, ifExists = false)(pos))
  }

  test("RENAME USER foo TO") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input '': expected a parameter or an identifier (line 1, column 19 (offset: 18))"
    )
  }

  test("RENAME USER TO bar") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input 'bar': expected \"IF\" or \"TO\" (line 1, column 16 (offset: 15))"
    )
  }

  test("RENAME USER TO") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input '': expected \"IF\" or \"TO\" (line 1, column 15 (offset: 14))"
    )
  }

  test("RENAME USER foo SET NAME TO bar") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input 'SET': expected \"IF\" or \"TO\" (line 1, column 17 (offset: 16))"
    )
  }

  test("RENAME USER foo SET NAME bar") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input 'SET': expected \"IF\" or \"TO\" (line 1, column 17 (offset: 16))"
    )
  }

  test("RENAME USER foo IF EXIST TO bar") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input 'EXIST': expected \"EXISTS\" (line 1, column 20 (offset: 19))"
    )
  }

  test("RENAME USER foo IF NOT EXISTS TO bar") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input 'NOT': expected \"EXISTS\" (line 1, column 20 (offset: 19))"
    )
  }

  test("RENAME USER foo TO bar IF EXISTS") {
    assertFailsWithMessage[Statements](testName, "Invalid input 'IF': expected <EOF> (line 1, column 24 (offset: 23))")
  }

  test("RENAME IF EXISTS USER foo TO bar") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input 'IF': expected \"ROLE\", \"SERVER\" or \"USER\" (line 1, column 8 (offset: 7))"
    )
  }

  test("RENAME OR REPLACE USER foo TO bar") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input 'OR': expected \"ROLE\", \"SERVER\" or \"USER\" (line 1, column 8 (offset: 7))"
    )
  }

  test("RENAME USER foo TO bar SET PASSWORD 'secret'") {
    failsToParse[Statements]
  }

  //  Dropping user

  test("DROP USER foo") {
    parsesTo[Statements](ast.DropUser(literalFoo, ifExists = false)(pos))
  }

  test("DROP USER $foo") {
    parsesTo[Statements](ast.DropUser(paramFoo, ifExists = false)(pos))
  }

  test("DROP USER ``") {
    parsesTo[Statements](ast.DropUser(literalEmpty, ifExists = false)(pos))
  }

  test("DROP USER `f:oo`") {
    parsesTo[Statements](ast.DropUser(literalFColonOo, ifExists = false)(pos))
  }

  test("DROP USER foo IF EXISTS") {
    parsesTo[Statements](ast.DropUser(literalFoo, ifExists = true)(pos))
  }

  test("DROP USER `` IF EXISTS") {
    parsesTo[Statements](ast.DropUser(literalEmpty, ifExists = true)(pos))
  }

  test("DROP USER `f:oo` IF EXISTS") {
    parsesTo[Statements](ast.DropUser(literalFColonOo, ifExists = true)(pos))
  }

  test("DROP USER ") {
    failsParsing[Statements]
  }

  test("DROP USER  IF EXISTS") {
    failsParsing[Statements]
  }

  test("DROP USER foo IF NOT EXISTS") {
    failsParsing[Statements]
  }

  //  Altering user

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

  test("ALTER USER foo RENAME TO bar") {
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

  test("ALTER USER foo") {
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

  // Changing own password

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
