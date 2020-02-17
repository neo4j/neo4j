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

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.PasswordString
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.SensitiveParameter
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral
import org.neo4j.cypher.internal.util.symbols.CTAny

class UserAdministrationCommandParserTest extends AdministrationCommandParserTestBase {

  //  Showing user

  test("SHOW USERS") {
    yields(ast.ShowUsers())
  }

  test("CATALOG SHOW USERS") {
    yields(ast.ShowUsers())
  }

  test("CATALOG SHOW USER") {
    failsToParse
  }

  //  Creating user

  test("CATALOG CREATE USER foo SET PASSWORD 'password'") {
    yields(ast.CreateUser("foo", Some(PasswordString("password")(_)), None, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError()))
  }

  test("CREATE USER `foo` SET PASSwORD 'password'") {
    yields(ast.CreateUser("foo", Some(PasswordString("password")(_)), None, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError()))
  }

  test("CREATE USER `!#\"~` SeT PASSWORD 'password'") {
    yields(ast.CreateUser("!#\"~", Some(PasswordString("password")(_)), None, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError()))
  }

  test("CREATE USER foo SeT PASSWORD 'pasS5Wor%d'") {
    yields(ast.CreateUser("foo", Some(PasswordString("pasS5Wor%d")(_)), None, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError()))
  }

  test("CREATE USER foo SET PASSwORD ''") {
    yields(ast.CreateUser("foo", Some(PasswordString("")(_)), None, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError()))
  }

  test("CREATE uSER foo SET PASSWORD $password") {
    yields(ast.CreateUser("foo", None, Some(expressions.Parameter("password", CTAny)(_)), requirePasswordChange = true, suspended = None, ast.IfExistsThrowError()))
  }

  test("CREaTE USER foo SET PASSWORD 'password' CHANGE REQUIRED") {
    yields(ast.CreateUser("foo", Some(PasswordString("password")(_)), None, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError()))
  }

  test("CATALOG CREATE USER foo SET PASSWORD $password CHANGE REQUIRED") {
    yields(ast.CreateUser("foo", None, Some(expressions.Parameter("password", CTAny)(_)), requirePasswordChange = true, suspended = None, ast.IfExistsThrowError()))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE required") {
    yields(ast.CreateUser("foo", Some(PasswordString("password")(_)), None, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError()))
  }

  test("CREATE USER foo SET PASSWORD 'password' CHAngE NOT REQUIRED") {
    yields(ast.CreateUser("foo", Some(PasswordString("password")(_)), None, requirePasswordChange = false, suspended = None, ast.IfExistsThrowError()))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE NOT REQUIRED") {
    yields(ast.CreateUser("foo", Some(PasswordString("password")(_)), None, requirePasswordChange = false, suspended = None, ast.IfExistsThrowError()))
  }

  test("CREATE USER foo SET PASSWORD $password SET  PASSWORD CHANGE NOT REQUIRED") {
    yields(ast.CreateUser("foo", None, Some(expressions.Parameter("password", CTAny)(_)), requirePasswordChange = false, suspended = None, ast.IfExistsThrowError()))
  }

  test("CATALOG CREATE USER foo SET PASSWORD 'password' SET STATUS SUSPENDed") {
    yields(ast.CreateUser("foo", Some(PasswordString("password")(_)), None, requirePasswordChange = true, suspended = Some(true), ast.IfExistsThrowError()))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS ACtiVE") {
    yields(ast.CreateUser("foo", Some(PasswordString("password")(_)), None, requirePasswordChange = true, suspended = Some(false), ast.IfExistsThrowError()))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE NOT REQUIRED SET   STATuS SUSPENDED") {
    yields(ast.CreateUser("foo", Some(PasswordString("password")(_)), None, requirePasswordChange = false, suspended = Some(true), ast.IfExistsThrowError()))
  }

  test("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED SET STATUS SUSPENDED") {
    yields(ast.CreateUser("foo", None, Some(expressions.Parameter("password", CTAny)(_)), requirePasswordChange = true, suspended = Some(true), ast.IfExistsThrowError()))
  }

  test("CREATE USER `` SET PASSwORD 'password'") {
    yields(ast.CreateUser("", Some(PasswordString("password")(_)), None, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError()))
  }

  test("CREATE USER `f:oo` SET PASSWORD 'password'") {
    yields(ast.CreateUser("f:oo", Some(PasswordString("password")(_)), None, requirePasswordChange = true, suspended = None, ast.IfExistsThrowError()))
  }

  test("CATALOG CREATE USER foo IF NOT EXISTS SET PASSWORD 'password'") {
    yields(ast.CreateUser("foo", Some(PasswordString("password")(_)), None, requirePasswordChange = true, suspended = None, ast.IfExistsDoNothing()))
  }

  test("CREATE uSER foo IF NOT EXISTS SET PASSWORD $password") {
    yields(ast.CreateUser("foo", None, Some(expressions.Parameter("password", CTAny)(_)), requirePasswordChange = true, suspended = None, ast.IfExistsDoNothing()))
  }

  test("CATALOG CREATE USER foo IF NOT EXISTS SET PASSWORD $password CHANGE REQUIRED") {
    yields(ast.CreateUser("foo", None, Some(expressions.Parameter("password", CTAny)(_)), requirePasswordChange = true, suspended = None, ast.IfExistsDoNothing()))
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD $password CHANGE REQUIRED SET STATUS SUSPENDED") {
    yields(ast.CreateUser("foo", None, Some(expressions.Parameter("password", CTAny)(_)), requirePasswordChange = true, suspended = Some(true), ast.IfExistsDoNothing()))
  }

  test("CATALOG CREATE OR REPLACE USER foo SET PASSWORD 'password'") {
    yields(ast.CreateUser("foo", Some(PasswordString("password")(_)), None, requirePasswordChange = true, suspended = None, ast.IfExistsReplace()))
  }

  test("CREATE OR REPLACE uSER foo SET PASSWORD $password") {
    yields(ast.CreateUser("foo", None, Some(expressions.Parameter("password", CTAny)(_)), requirePasswordChange = true, suspended = None, ast.IfExistsReplace()))
  }

  test("CATALOG CREATE OR REPLACE USER foo SET PASSWORD $password CHANGE REQUIRED") {
    yields(ast.CreateUser("foo", None, Some(expressions.Parameter("password", CTAny)(_)), requirePasswordChange = true, suspended = None, ast.IfExistsReplace()))
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD $password CHANGE REQUIRED SET STATUS SUSPENDED") {
    yields(ast.CreateUser("foo", None, Some(expressions.Parameter("password", CTAny)(_)), requirePasswordChange = true, suspended = Some(true), ast.IfExistsReplace()))
  }

  test("CREATE OR REPLACE USER foo IF NOT EXISTS SET PASSWORD 'password'") {
    yields(ast.CreateUser("foo", Some(PasswordString("password")(_)), None, requirePasswordChange = true, suspended = None, ast.IfExistsInvalidSyntax()))
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

  test("CREATE USER foo IF NOT EXISTS") {
    failsToParse
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD") {
    failsToParse
  }

  test("CREATE OR REPLACE USER foo") {
    failsToParse
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD") {
    failsToParse
  }

  test("CREATE command finds password literal at correct offset") {
    parsing("CREATE USER foo SET PASSWORD 'password'").shouldVerify { statement =>
      val passwords = statement.findByAllClass[SensitiveStringLiteral].map(l => (l.value, l.position.offset))
      passwords should equal(Seq("password" -> 29))
    }
  }

  test("CREATE command finds password parameter at correct offset") {
    parsing("CREATE USER foo SET PASSWORD $param").shouldVerify { statement =>
      val passwords = statement.findByAllClass[SensitiveParameter].map(p => (p.name, p.position.offset))
      passwords should equal(Seq("param" -> 29))
    }
  }

  //  Dropping user

  test("DROP USER foo") {
    yields(ast.DropUser("foo", ifExists = false))
  }

  test("DROP USER ``") {
    yields(ast.DropUser("", ifExists = false))
  }

  test("DROP USER `f:oo`") {
    yields(ast.DropUser("f:oo", ifExists = false))
  }

  test("DROP USER foo IF EXISTS") {
    yields(ast.DropUser("foo", ifExists = true))
  }

  test("DROP USER `` IF EXISTS") {
    yields(ast.DropUser("", ifExists = true))
  }

  test("DROP USER `f:oo` IF EXISTS") {
    yields(ast.DropUser("f:oo", ifExists = true))
  }

  test("DROP USER ") {
    failsToParse
  }

  test("DROP USER  IF EXISTS") {
    failsToParse
  }

  //  Altering user

  test("CATALOG ALTER USER foo SET PASSWORD 'password'") {
    yields(ast.AlterUser("foo", Some(PasswordString("password")(_)), None, None, None))
  }

  test("ALTER USER `` SET PASSWORD 'password'") {
    yields(ast.AlterUser("", Some(PasswordString("password")(_)), None, None, None))
  }

  test("ALTER USER `f:oo` SET PASSWORD 'password'") {
    yields(ast.AlterUser("f:oo", Some(PasswordString("password")(_)), None, None, None))
  }

  test("ALTER USER foo SET PASSWORD ''") {
    yields(ast.AlterUser("foo", Some(PasswordString("")(_)), None, None, None))
  }

  test("ALTER USER foo SET PASSWORD $password") {
    yields(ast.AlterUser("foo", None, Some(expressions.Parameter("password", CTAny)(_)), None, None))
  }

  test("CATALOG ALTER USER foo SET PASSWORD CHANGE REQUIRED") {
    yields(ast.AlterUser("foo", None, None, requirePasswordChange = Some(true), None))
  }

  test("CATALOG ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED") {
    yields(ast.AlterUser("foo", None, None, requirePasswordChange = Some(false), None))
  }

  test("ALTER USER foo SET STATUS SUSPENDED") {
    yields(ast.AlterUser("foo", None, None, None, suspended = Some(true)))
  }

  test("ALTER USER foo SET STATUS ACTIVE") {
    yields(ast.AlterUser("foo", None, None, None, suspended = Some(false)))
  }

  test("CATALOG ALTER USER foo SET PASSWORD 'password' CHANGE REQUIRED") {
    yields(ast.AlterUser("foo", Some(PasswordString("password")(_)), None, requirePasswordChange = Some(true), None))
  }

  test("ALTER USER foo SET PASSWORD $password SET PASSWORD CHANGE NOT REQUIRED") {
    yields(ast.AlterUser("foo", None, Some(expressions.Parameter("password", CTAny)(_)), requirePasswordChange = Some(false), None))
  }

  test("CATALOG ALTER USER foo SET PASSWORD 'password' SET STATUS ACTIVE") {
    yields(ast.AlterUser("foo", Some(PasswordString("password")(_)), None, None, suspended = Some(false)))
  }

  test("CATALOG ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED SET STATUS ACTIVE") {
    yields(ast.AlterUser("foo", None, None, requirePasswordChange = Some(false), suspended = Some(false)))
  }

  test("ALTER USER foo SET PASSWORD $password SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED") {
    yields(ast.AlterUser("foo", None, Some(expressions.Parameter("password", CTAny)(_)), requirePasswordChange = Some(false), suspended = Some(true)))
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

  test("ALTER user command finds password literal at correct offset") {
    parsing("ALTER USER foo SET PASSWORD 'password'").shouldVerify { statement =>
      val passwords = statement.findByAllClass[SensitiveStringLiteral].map(l => (l.value, l.position.offset))
      passwords should equal(Seq("password" -> 28))
    }
  }

  test("ALTER user command finds password parameter at correct offset") {
    parsing("ALTER USER foo SET PASSWORD $param").shouldVerify { statement =>
      val passwords = statement.findByAllClass[SensitiveParameter].map(p => (p.name, p.position.offset))
      passwords should equal(Seq("param" -> 28))
    }
  }

  // Changing own password

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO 'new'") {
    yields(ast.SetOwnPassword(Some(PasswordString("new")(_)), None, Some(PasswordString("current")(_)), None))
  }

  test("alter current user set password from 'current' to ''") {
    yields(ast.SetOwnPassword(Some(PasswordString("")(_)), None, Some(PasswordString("current")(_)), None))
  }

  test("alter current user set password from '' to 'new'") {
    yields(ast.SetOwnPassword(Some(PasswordString("new")(_)), None, Some(PasswordString("")(_)), None))
  }

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO 'passWORD123%!'") {
    yields(ast.SetOwnPassword(Some(PasswordString("passWORD123%!")(_)), None, Some(PasswordString("current")(_)), None))
  }

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO $newPassword") {
    yields(ast.SetOwnPassword(None, Some(expressions.Parameter("newPassword", CTAny)(_)), Some(PasswordString("current")(_)), None))
  }

  test("ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO 'new'") {
    yields(ast.SetOwnPassword(Some(PasswordString("new")(_)), None, None, Some(expressions.Parameter("currentPassword", CTAny)(_))))
  }

  test("alter current user set password from $currentPassword to ''") {
    yields(ast.SetOwnPassword(Some(PasswordString("")(_)), None, None, Some(expressions.Parameter("currentPassword", CTAny)(_))))
  }

  test("ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO 'passWORD123%!'") {
    yields(ast.SetOwnPassword(Some(PasswordString("passWORD123%!")(_)), None, None, Some(expressions.Parameter("currentPassword", CTAny)(_))))
  }

  test("ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword") {
    yields(ast.SetOwnPassword(None, Some(expressions.Parameter("newPassword", CTAny)(_)), None, Some(expressions.Parameter("currentPassword", CTAny)(_))))
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

  test("ALTER CURRENT USER command finds password literal at correct offset") {
    parsing("ALTER CURRENT USER SET PASSWORD FROM 'current' TO 'new'").shouldVerify { statement =>
      val passwords = statement.findByAllClass[SensitiveStringLiteral].map(l => (l.value, l.position.offset))
      passwords.toSet should equal(Set("current" -> 37, "new" -> 50))
    }
  }

  test("ALTER CURRENT USER command finds password parameter at correct offset") {
    parsing("ALTER CURRENT USER SET PASSWORD FROM $current TO $new").shouldVerify { statement =>
      val passwords = statement.findByAllClass[SensitiveParameter].map(p => (p.name, p.position.offset))
      passwords.toSet should equal(Set("current" -> 37, "new" -> 49))
    }
  }
}
