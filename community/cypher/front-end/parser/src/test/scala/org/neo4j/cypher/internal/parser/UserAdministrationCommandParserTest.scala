/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.parser

import java.util

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.expressions.SensitiveParameter
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral

class UserAdministrationCommandParserTest extends AdministrationCommandParserTestBase {
  private val userString = "user"
  private val varUser = varFor(userString)
  private val password = pw("password")
  private val passwordNew = pw("new")
  private val passwordCurrent = pw("current")
  private val passwordEmpty = pw("")
  private val paramPassword = pwParam("password")
  private val paramPasswordNew = pwParam("newPassword")
  private val paramPasswordCurrent = pwParam("currentPassword")
  private val paramDb = param("db")

  //  Showing user

  test("SHOW USERS") {
    yields(ast.ShowUsers(None))
  }

  test("CATALOG SHOW USERS") {
    yields(ast.ShowUsers(None))
  }

  test("SHOW USERS WHERE user = 'GRANTED'") {
    yields(ast.ShowUsers(Some(Right(where(equals(varUser, grantedString))))))
  }

  test("SHOW USERS WHERE user = 'GRANTED' AND action = 'match'") {
    val accessPredicate = equals(varUser, grantedString)
    val matchPredicate = equals(varFor(actionString), literalString("match"))
    yields(ast.ShowUsers(Some(Right(where(and(accessPredicate, matchPredicate))))))
  }

  test("SHOW USERS YIELD user ORDER BY user") {
    val columns = yieldClause(returnItems(variableReturnItem(userString)), Some(orderBy(sortItem(varUser))))
    yields(ast.ShowUsers(Some(Left((columns, None)))))
  }

  test("SHOW USERS YIELD user ORDER BY user WHERE user ='none'") {
    val orderByClause = orderBy(sortItem(varUser))
    val whereClause = where(equals(varUser, noneString))
    val columns = yieldClause(returnItems(variableReturnItem(userString)), Some(orderByClause), where = Some(whereClause))
    yields(ast.ShowUsers(Some(Left((columns, None)))))
  }

  test("SHOW USERS YIELD user ORDER BY user SKIP 1 LIMIT 10 WHERE user ='none'") {
    val orderByClause = orderBy(sortItem(varUser))
    val whereClause = where(equals(varUser, noneString))
    val columns = yieldClause(returnItems(variableReturnItem(userString)), Some(orderByClause), Some(skip(1)), Some(limit(10)), Some(whereClause))
    yields(ast.ShowUsers(Some(Left((columns, None)))))
  }

  test("SHOW USERS YIELD user SKIP -1") {
    val columns = yieldClause(returnItems(variableReturnItem(userString)), skip = Some(skip(-1)))
    yields(ast.ShowUsers(Some(Left((columns, None)))))
  }

  test("SHOW USERS YIELD user RETURN user ORDER BY user") {
    yields(ast.ShowUsers(
      Some(Left((yieldClause(returnItems(variableReturnItem(userString))),
      Some(returnClause(returnItems(variableReturnItem(userString)), Some(orderBy(sortItem(varUser)))))
    )))))
  }

  test("SHOW USERS YIELD user, suspended as suspended WHERE suspended RETURN DISTINCT user") {
    val suspendedVar = varFor("suspended")
    yields(ast.ShowUsers(
      Some(Left((yieldClause(returnItems(variableReturnItem(userString), aliasedReturnItem(suspendedVar)), where = Some(where(suspendedVar))),
      Some(returnClause(returnItems(variableReturnItem(userString)), distinct = true))
    )))))
  }

  test("SHOW USERS YIELD * RETURN *") {
    yields(ast.ShowUsers(Some(Left((yieldClause(returnAllItems), Some(returnClause(returnAllItems)))))))
  }

  test("CATALOG SHOW USER") {
    failsToParse
  }

  test("SHOW USERS YIELD *,blah RETURN user") {
    failsToParse
  }

  // Showing current user

  test("CATALOG SHOW CURRENT USER") {
    yields(ast.ShowCurrentUser(None))
  }

  test("SHOW CURRENT USER") {
    yields(ast.ShowCurrentUser(None))
  }

  test("SHOW CURRENT USER YIELD * WHERE suspended = false RETURN roles") {
    val suspendedVar = varFor("suspended")
    yields(ast.ShowCurrentUser(Some(Left(yieldClause(returnAllItems, where = Some(where(equals(suspendedVar, falseLiteral)))),
      Some(returnClause(returnItems(variableReturnItem("roles"))))))))
  }

  //  Creating user

  test("CATALOG CREATE USER foo SET PASSWORD 'password'") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, password, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError, None))
  }

  test("CATALOG CREATE USER $foo SET PASSWORD 'password'") {
    yields(ast.CreateUser(paramFoo, isEncryptedPassword = false, password, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError, None))
  }

  test("CATALOG CREATE USER foo SET PLAINTEXT PASSWORD 'password'") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, password, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError, None))
  }

  test("CATALOG CREATE USER foo SET PLAINTEXT PASSWORD $password") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, paramPassword, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError, None))
  }

  test("CATALOG CREATE USER $bar SET PASSWORD $password") {
    yields(ast.CreateUser(param("bar"), isEncryptedPassword = false, paramPassword, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError, None))
  }

  test("CREATE USER `foo` SET PASSwORD 'password'") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, password, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError, None))
  }

  test("CREATE USER `!#\"~` SeT PASSWORD 'password'") {
    yields(ast.CreateUser(literal("!#\"~"), isEncryptedPassword = false, password, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError, None))
  }

  test("CREATE USER foo SeT PASSWORD 'pasS5Wor%d'") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, pw("pasS5Wor%d"), requirePasswordChange = true, suspended = None, ast.IfExistsThrowError, None))
  }

  test("CREATE USER foo SET PASSwORD ''") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, passwordEmpty, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError, None))
  }

  test("CREATE uSER foo SET PASSWORD $password") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, paramPassword, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError, None))
  }

  test("CREaTE USER foo SET PASSWORD 'password' CHANGE REQUIRED") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, password, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError, None))
  }

  test("CATALOG CREATE USER foo SET PASSWORD $password CHANGE REQUIRED") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, paramPassword, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError, None))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE required") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, password, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError, None))
  }

  test("CREATE USER foo SET PASSWORD 'password' CHAngE NOT REQUIRED") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, password, requirePasswordChange = false, suspended = None, ast.IfExistsThrowError, None))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE NOT REQUIRED") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, password, requirePasswordChange = false, suspended = None, ast.IfExistsThrowError, None))
  }

  test("CREATE USER foo SET PASSWORD $password SET  PASSWORD CHANGE NOT REQUIRED") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, paramPassword, requirePasswordChange = false, suspended = None, ast.IfExistsThrowError, None))
  }

  test("CATALOG CREATE USER foo SET PASSWORD 'password' SET STATUS SUSPENDed") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, password, requirePasswordChange = true, suspended = Some(true), ast.IfExistsThrowError, None))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS ACtiVE") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, password, requirePasswordChange = true, suspended = Some(false), ast.IfExistsThrowError, None))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE NOT REQUIRED SET   STATuS SUSPENDED") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, password, requirePasswordChange = false, suspended = Some(true), ast.IfExistsThrowError, None))
  }

  test("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED SET STATUS SUSPENDED") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, paramPassword, requirePasswordChange = true, suspended = Some(true), ast.IfExistsThrowError, None))
  }

  test("CREATE USER `` SET PASSwORD 'password'") {
    yields(ast.CreateUser(literalEmpty, isEncryptedPassword = false, password, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError, None))
  }

  test("CREATE USER `f:oo` SET PASSWORD 'password'") {
    yields(ast.CreateUser(literalFColonOo, isEncryptedPassword = false, password, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError, None))
  }

  test("CATALOG CREATE USER foo IF NOT EXISTS SET PASSWORD 'password'") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, password, requirePasswordChange = true, suspended = None, ast.IfExistsDoNothing, None))
  }

  test("CREATE uSER foo IF NOT EXISTS SET PASSWORD $password") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, paramPassword, requirePasswordChange = true, suspended = None, ast.IfExistsDoNothing, None))
  }

  test("CATALOG CREATE USER foo IF NOT EXISTS SET PASSWORD $password CHANGE REQUIRED") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, paramPassword, requirePasswordChange = true, suspended = None, ast.IfExistsDoNothing, None))
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD $password SET STATUS SUSPENDED") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, paramPassword, requirePasswordChange = true, suspended = Some(true), ast.IfExistsDoNothing, None))
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD $password CHANGE REQUIRED SET STATUS SUSPENDED") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, paramPassword, requirePasswordChange = true, suspended = Some(true), ast.IfExistsDoNothing, None))
  }

  test("CATALOG CREATE OR REPLACE USER foo SET PASSWORD 'password'") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, password, requirePasswordChange = true, suspended = None, ast.IfExistsReplace, None))
  }

  test("CREATE OR REPLACE uSER foo SET PASSWORD $password") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, paramPassword, requirePasswordChange = true, suspended = None, ast.IfExistsReplace, None))
  }

  test("CATALOG CREATE OR REPLACE USER foo SET PASSWORD $password CHANGE REQUIRED") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, paramPassword, requirePasswordChange = true, suspended = None, ast.IfExistsReplace, None))
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD $password SET STATUS SUSPENDED") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, paramPassword, requirePasswordChange = true, suspended = Some(true), ast.IfExistsReplace, None))
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD $password CHANGE REQUIRED SET STATUS SUSPENDED") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, paramPassword, requirePasswordChange = true, suspended = Some(true), ast.IfExistsReplace, None))
  }

  test("CREATE OR REPLACE USER foo IF NOT EXISTS SET PASSWORD 'password'") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, password, requirePasswordChange = true, suspended = None, ast.IfExistsInvalidSyntax, None))
  }

  test("CREATE USER foo SET ENCRYPTED PASSWORD '1,04773b8510aea96ca2085cb81764b0a2,75f4201d047191c17c5e236311b7c4d77e36877503fe60b1ca6d4016160782ab'") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = true, pw("1,04773b8510aea96ca2085cb81764b0a2,75f4201d047191c17c5e236311b7c4d77e36877503fe60b1ca6d4016160782ab"), requirePasswordChange = true, suspended = None, ast.IfExistsThrowError, None))
  }

  test("CATALOG CREATE USER $foo SET encrYPTEd PASSWORD 'password'") {
    yields(ast.CreateUser(paramFoo, isEncryptedPassword = true, password, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError, None))
  }

  test("CATALOG CREATE USER $bar SET ENCRYPTED Password $password") {
    yields(ast.CreateUser(param("bar"), isEncryptedPassword = true, paramPassword, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError, None))
  }

  test("CREATE OR REPLACE USER foo SET encrypted password 'sha256,x1024,0x2460294fe,b3ddb287a'") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = true, pw("sha256,x1024,0x2460294fe,b3ddb287a"), requirePasswordChange = true, suspended = None, ast.IfExistsReplace, None))
  }

  test("CREATE USER foo SET password 'password' DEFAULT DATABASE db1") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, password, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError, Some(Left("db1"))))
  }

  test("CREATE USER foo SET password 'password' DEFAULT DATABASE $db") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, password, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError, Some(paramDb)))
  }

  test("CREATE OR REPLACE USER foo SET password 'password' DEFAULT DATABASE db1") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, password, requirePasswordChange = true, suspended = None, ast.IfExistsReplace, Some(Left("db1"))))
  }

  test("CREATE USER foo IF NOT EXISTS SET password 'password' DEFAULT DATABASE db1") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, password, requirePasswordChange = true, suspended = None, ast.IfExistsDoNothing, Some(Left("db1"))))
  }

  test("CREATE USER foo SET password 'password' SET PASSWORD CHANGE NOT REQUIRED DEFAULT database $db") {
    yields(ast.CreateUser(literalFoo, isEncryptedPassword = false, password, requirePasswordChange = false, suspended = None, ast.IfExistsThrowError, Some(paramDb)))
  }

  test("CREATE command finds password literal at correct offset") {
    parsing("CREATE USER foo SET PASSWORD 'password'").shouldVerify { statement =>
      val passwords = statement.findByAllClass[SensitiveStringLiteral].map(l => (l.value, l.position.offset))
      passwords.foreach { case (pw, offset) =>
        withClue("Expecting password = password, offset = 29") {
          util.Arrays.equals(toUtf8Bytes("password"), pw) shouldBe true
          offset shouldBe 29
        }
      }
    }
  }

  test("CREATE command finds password parameter at correct offset") {
    parsing("CREATE USER foo SET PASSWORD $param").shouldVerify { statement =>
      val passwords = statement.findByAllClass[SensitiveParameter].map(p => (p.name, p.position.offset))
      passwords should equal(Seq("param" -> 29))
    }
  }

  test("CREATE USER foo") {
    failsToParse
  }

  test("CREATE USER \"foo\" SET PASSwORD 'password'") {
    failsToParse
  }

  test("CREATE USER !#\"~ SeT PASSWORD 'password'") {
    failsToParse
  }

  test("CATALOG CREATE USER fo,o SET PASSWORD 'password'") {
    failsToParse
  }

  test("CREATE USER f:oo SET PASSWORD 'password'") {
    failsToParse
  }

  test("CREATE USER foo SET PASSWORD") {
    failsToParse
  }

  test("CREATE USER foo SET ENCRYPTED PASSWORD 123") {
    failsToParse
  }

  test("CREATE USER foo SET ENCRYPTED PASSWORD") {
    failsToParse
  }

  test("CREATE USER foo SET PLAINTEXT PASSWORD") {
    failsToParse
  }

  test("CREATE USER foo SET PASSWORD 'password' SET ENCRYPTED PASSWORD") {
    failsToParse
  }

  test("CREATE USER foo SET PASSWORD 'password' ENCRYPTED") {
    failsToParse
  }

  test("CREATE USER foo SET PASSwORD 'passwordString'+$passwordexpressions.Parameter") {
    failsToParse
  }

  test("CREATE USER foo SET PASSWORD null CHANGE REQUIRED") {
    failsToParse
  }

  test("CREATE USER foo PASSWORD 'password'") {
    failsToParse
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STAUS ACTIVE") {
    failsToParse
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS IMAGINARY") {
    failsToParse
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS") {
    failsToParse
  }

  test("CREATE USER foo SET PASSWORD CHANGE REQUIRED") {
    failsToParse
  }

  test("CREATE USER foo SET STATUS SUSPENDED") {
    failsToParse
  }

  test("CREATE USER foo SET PASSWORD CHANGE REQUIRED SET STATUS ACTIVE") {
    failsToParse
  }

  test("CREATE USER foo IF EXISTS SET PASSWORD 'bar'") {
    failsToParse
  }

  test("CREATE USER foo IF NOT EXISTS") {
    failsToParse
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD") {
    failsToParse
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD CHANGE REQUIRED") {
    failsToParse
  }

  test("CREATE USER foo IF NOT EXISTS SET STATUS ACTIVE") {
    failsToParse
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED") {
    failsToParse
  }

  test("CREATE OR REPLACE USER foo") {
    failsToParse
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD") {
    failsToParse
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD CHANGE NOT REQUIRED") {
    failsToParse
  }

  test("CREATE OR REPLACE USER foo SET STATUS SUSPENDED") {
    failsToParse
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD CHANGE REQUIRED SET STATUS ACTIVE") {
    failsToParse
  }

  //  Dropping user

  test("DROP USER foo") {
    yields(ast.DropUser(literalFoo, ifExists = false))
  }

  test("DROP USER $foo") {
    yields(ast.DropUser(paramFoo, ifExists = false))
  }

  test("DROP USER ``") {
    yields(ast.DropUser(literalEmpty, ifExists = false))
  }

  test("DROP USER `f:oo`") {
    yields(ast.DropUser(literalFColonOo, ifExists = false))
  }

  test("DROP USER foo IF EXISTS") {
    yields(ast.DropUser(literalFoo, ifExists = true))
  }

  test("DROP USER `` IF EXISTS") {
    yields(ast.DropUser(literalEmpty, ifExists = true))
  }

  test("DROP USER `f:oo` IF EXISTS") {
    yields(ast.DropUser(literalFColonOo, ifExists = true))
  }

  test("DROP USER ") {
    failsToParse
  }

  test("DROP USER  IF EXISTS") {
    failsToParse
  }

  test("DROP USER foo IF NOT EXISTS") {
    failsToParse
  }

  //  Altering user

  test("CATALOG ALTER USER foo SET PASSWORD 'password'") {
    yields(ast.AlterUser(literalFoo, isEncryptedPassword = Some(false), Some(password), None, None, None))
  }

  test("CATALOG ALTER USER $foo SET PASSWORD 'password'") {
    yields(ast.AlterUser(paramFoo, isEncryptedPassword = Some(false), Some(password), None, None, None))
  }

  test("CATALOG ALTER USER foo SET PLAINTEXT PASSWORD 'password'") {
    yields(ast.AlterUser(literalFoo, isEncryptedPassword = Some(false), Some(password), None, None, None))
  }

  test("CATALOG ALTER USER foo SET PLAINTEXT PASSWORD $password") {
    yields(ast.AlterUser(literalFoo, isEncryptedPassword = Some(false), Some(paramPassword), None, None, None))
  }

  test("ALTER USER `` SET PASSWORD 'password'") {
    yields(ast.AlterUser(literalEmpty, isEncryptedPassword = Some(false), Some(password), None, None, None))
  }

  test("ALTER USER `f:oo` SET PASSWORD 'password'") {
    yields(ast.AlterUser(literalFColonOo, isEncryptedPassword = Some(false), Some(password), None, None, None))
  }

  test("ALTER USER foo SET PASSWORD ''") {
    yields(ast.AlterUser(literalFoo, isEncryptedPassword = Some(false), Some(passwordEmpty), None, None, None))
  }

  test("ALTER USER foo SET PASSWORD $password") {
    yields(ast.AlterUser(literalFoo, isEncryptedPassword = Some(false), Some(paramPassword), None, None, None))
  }

  test("CATALOG ALTER USER foo SET ENCRYPTED Password $password") {
    yields(ast.AlterUser(literalFoo, isEncryptedPassword = Some(true), Some(paramPassword), None, None, None))
  }

  test("CATALOG ALTER USER foo SET ENCRYPTED PASSWORD 'password'") {
    yields(ast.AlterUser(literalFoo, isEncryptedPassword = Some(true), Some(password), None, None, None))
  }

  test("CATALOG ALTER USER $foo SET ENCRYPTED PASSWORD 'password'") {
    yields(ast.AlterUser(paramFoo, isEncryptedPassword = Some(true), Some(password), None, None, None))
  }

  test("ALTER USER `` SET ENCRYPTED PASSWORD 'password'") {
    yields(ast.AlterUser(literalEmpty, isEncryptedPassword = Some(true), Some(password), None, None, None))
  }

  test("CATALOG ALTER USER foo SET ENCRYPTED PASSWORD '1,04773b8510aea96ca2085cb81764b0a2,75f4201d047191c17c5e236311b7c4d77e36877503fe60b1ca6d4016160782ab'") {
    yields(ast.AlterUser(literalFoo, isEncryptedPassword = Some(true), Some(pw("1,04773b8510aea96ca2085cb81764b0a2,75f4201d047191c17c5e236311b7c4d77e36877503fe60b1ca6d4016160782ab")), None, None, None))
  }

  test("CATALOG ALTER USER foo SET PASSWORD CHANGE REQUIRED") {
    yields(ast.AlterUser(literalFoo, None, None, requirePasswordChange = Some(true), None, None))
  }

  test("CATALOG ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED") {
    yields(ast.AlterUser(literalFoo, None, None, requirePasswordChange = Some(false), None, None))
  }

  test("ALTER USER foo SET STATUS SUSPENDED") {
    yields(ast.AlterUser(literalFoo, None, None, None, suspended = Some(true), None))
  }

  test("ALTER USER foo SET STATUS ACTIVE") {
    yields(ast.AlterUser(literalFoo, None, None, None, suspended = Some(false), None))
  }

  test("CATALOG ALTER USER foo SET PASSWORD 'password' CHANGE REQUIRED") {
    yields(ast.AlterUser(literalFoo, isEncryptedPassword = Some(false), Some(password), requirePasswordChange = Some(true), None, None))
  }

  test("ALTER USER foo SET PASSWORD $password SET PASSWORD CHANGE NOT REQUIRED") {
    yields(ast.AlterUser(literalFoo, isEncryptedPassword = Some(false), Some(paramPassword), requirePasswordChange = Some(false), None, None))
  }

  test("CATALOG ALTER USER foo SET PASSWORD 'password' SET STATUS ACTIVE") {
    yields(ast.AlterUser(literalFoo, isEncryptedPassword = Some(false), Some(password), None, suspended = Some(false), None))
  }

  test("CATALOG ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED SET STATUS ACTIVE") {
    yields(ast.AlterUser(literalFoo, None, None, requirePasswordChange = Some(false), suspended = Some(false), None))
  }

  test("ALTER USER foo SET PASSWORD $password SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED") {
    yields(ast.AlterUser(literalFoo, isEncryptedPassword = Some(false), Some(paramPassword), requirePasswordChange = Some(false), suspended = Some(true), None))
  }

  test("ALTER USER foo DEFAULT DATABASE db1") {
    yields(ast.AlterUser(literalFoo, None, None, None, suspended = None, Some(Left("db1"))))
  }

  test("ALTER USER foo DEFAULT DATABASE $db") {
    yields(ast.AlterUser(literalFoo, None, None, None, suspended = None, Some(paramDb)))
  }

  test("ALTER USER foo SET PASSWORD CHANGE REQUIRED DEFAULT DATABASE db1") {
    yields(ast.AlterUser(literalFoo, None, None, requirePasswordChange = Some(true), suspended = None, Some(Left("db1"))))
  }

  test("ALTER USER foo SET password 'password' DEFAULT DATABASE db1") {
    yields(ast.AlterUser(literalFoo, Some(false), Some(password), None, suspended = None, Some(Left("db1"))))
  }

  test("ALTER USER foo SET password 'password' SET PASSWORD CHANGE NOT REQUIRED DEFAULT database $db") {
    yields(ast.AlterUser(literalFoo, Some(false), Some(password), requirePasswordChange = Some(false), suspended = None, Some(paramDb)))
  }

  test("ALTER user command finds password literal at correct offset") {
    parsing("ALTER USER foo SET PASSWORD 'password'").shouldVerify { statement =>
      val passwords = statement.findByAllClass[SensitiveStringLiteral].map(l => (l.value, l.position.offset))
      passwords.foreach { case (pw, offset) =>
        withClue("Expecting password = password, offset = 28") {
          util.Arrays.equals(toUtf8Bytes("password"), pw) shouldBe true
          offset shouldBe 28
        }
      }
    }
  }

  test("ALTER user command finds password parameter at correct offset") {
    parsing("ALTER USER foo SET PASSWORD $param").shouldVerify { statement =>
      val passwords = statement.findByAllClass[SensitiveParameter].map(p => (p.name, p.position.offset))
      passwords should equal(Seq("param" -> 28))
    }
  }

  test("ALTER USER foo") {
    failsToParse
  }

  test("ALTER USER foo SET PASSWORD null") {
    failsToParse
  }

  test("ALTER USER foo SET PASSWORD 123") {
    failsToParse
  }

  test("ALTER USER foo SET PASSWORD") {
    failsToParse
  }

  test("ALTER USER foo SET ENCRYPTED PASSWORD 123") {
    failsToParse
  }

  test("ALTER USER foo SET PLAINTEXT PASSWORD") {
    failsToParse
  }

  test("ALTER USER foo SET ENCRYPTED PASSWORD") {
    failsToParse
  }

  test("ALTER USER foo SET PASSWORD 'password' SET ENCRYPTED PASSWORD") {
    failsToParse
  }

  test("ALTER USER foo SET PASSWORD 'password' ENCRYPTED") {
    failsToParse
  }

  test("ALTER USER foo SET STATUS") {
    failsToParse
  }

  test("ALTER USER foo SET PASSWORD 'password' SET PASSWORD SET STATUS ACTIVE") {
    failsToParse
  }

  test("ALTER USER foo SET PASSWORD STATUS ACTIVE") {
    failsToParse
  }

  test("CATALOG ALTER USER foo SET PASSWORD 'password' SET STATUS IMAGINARY") {
    failsToParse
  }

  // Changing own password

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO 'new'") {
    yields(ast.SetOwnPassword(passwordNew, passwordCurrent))
  }

  test("alter current user set password from 'current' to ''") {
    yields(ast.SetOwnPassword(passwordEmpty, passwordCurrent))
  }

  test("alter current user set password from '' to 'new'") {
    yields(ast.SetOwnPassword(passwordNew, passwordEmpty))
  }

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO 'passWORD123%!'") {
    yields(ast.SetOwnPassword(pw("passWORD123%!"), passwordCurrent))
  }

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO $newPassword") {
    yields(ast.SetOwnPassword(paramPasswordNew, passwordCurrent))
  }

  test("ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO 'new'") {
    yields(ast.SetOwnPassword(passwordNew, paramPasswordCurrent))
  }

  test("alter current user set password from $currentPassword to ''") {
    yields(ast.SetOwnPassword(passwordEmpty, paramPasswordCurrent))
  }

  test("ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO 'passWORD123%!'") {
    yields(ast.SetOwnPassword(pw("passWORD123%!"), paramPasswordCurrent))
  }

  test("ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword") {
    yields(ast.SetOwnPassword(paramPasswordNew, paramPasswordCurrent))
  }

  test("ALTER CURRENT USER command finds password literal at correct offset") {
    parsing("ALTER CURRENT USER SET PASSWORD FROM 'current' TO 'new'").shouldVerify { statement =>
      val passwords = statement.findByAllClass[SensitiveStringLiteral].map(l => (new String(l.value, "utf-8"), l.position.offset))
      passwords.toSet should equal(Set("current" -> 37, "new" -> 50))
    }
  }

  test("ALTER CURRENT USER command finds password parameter at correct offset") {
    parsing("ALTER CURRENT USER SET PASSWORD FROM $current TO $new").shouldVerify { statement =>
      val passwords = statement.findByAllClass[SensitiveParameter].map(p => (p.name, p.position.offset))
      passwords.toSet should equal(Set("current" -> 37, "new" -> 49))
    }
  }

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO null") {
    failsToParse
  }

  test("ALTER CURRENT USER SET PASSWORD FROM $current TO 123") {
    failsToParse
  }

  test("ALTER PASSWORD FROM 'current' TO 'new'") {
    failsToParse
  }

  test("ALTER CURRENT PASSWORD FROM 'current' TO 'new'") {
    failsToParse
  }

  test("ALTER CURRENT USER PASSWORD FROM 'current' TO 'new'") {
    failsToParse
  }

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO") {
    failsToParse
  }

  test("ALTER CURRENT USER SET PASSWORD FROM TO 'new'") {
    failsToParse
  }

  test("ALTER CURRENT USER SET PASSWORD TO 'new'") {
    failsToParse
  }
}
