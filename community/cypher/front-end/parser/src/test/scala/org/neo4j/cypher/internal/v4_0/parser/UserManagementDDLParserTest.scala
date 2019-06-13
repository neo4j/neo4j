/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v4_0.parser

import org.neo4j.cypher.internal.v4_0.ast
import org.neo4j.cypher.internal.v4_0.expressions.{Parameter => Param}
import org.neo4j.cypher.internal.v4_0.util.symbols.CTAny

class UserManagementDDLParserTest extends DDLParserTestBase {

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
    yields(ast.CreateUser("foo", Some("password"), None, requirePasswordChange = true, suspended = None))
  }

  test("CREATE USER `foo` SET PASSwORD 'password'") {
    yields(ast.CreateUser("foo", Some("password"), None, requirePasswordChange = true, suspended = None))
  }

  test("CREATE USER `!#\"~` SeT PASSWORD 'password'") {
    yields(ast.CreateUser("!#\"~", Some("password"), None, requirePasswordChange = true, suspended = None))
  }

  test("CREATE USER foo SeT PASSWORD 'pasS5Wor%d'") {
    yields(ast.CreateUser("foo", Some("pasS5Wor%d"), None, requirePasswordChange = true, suspended = None))
  }

  test("CREATE USER foo SET PASSwORD ''") {
    yields(ast.CreateUser("foo", Some(""), None, requirePasswordChange = true, suspended = None))
  }

  test("CREATE uSER foo SET PASSWORD $password") {
    yields(ast.CreateUser("foo", None, Some(Param("password", CTAny)(_)), requirePasswordChange = true, suspended = None))
  }

  test("CREaTE USER foo SET PASSWORD 'password' CHANGE REQUIRED") {
    yields(ast.CreateUser("foo", Some("password"), None, requirePasswordChange = true, suspended = None))
  }

  test("CATALOG CREATE USER foo SET PASSWORD $password CHANGE REQUIRED") {
    yields(ast.CreateUser("foo", None, Some(Param("password", CTAny)(_)), requirePasswordChange = true, suspended = None))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE required") {
    yields(ast.CreateUser("foo", Some("password"), None, requirePasswordChange = true, suspended = None))
  }

  test("CREATE USER foo SET PASSWORD 'password' CHAngE NOT REQUIRED") {
    yields(ast.CreateUser("foo", Some("password"), None, requirePasswordChange = false, suspended = None))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE NOT REQUIRED") {
    yields(ast.CreateUser("foo", Some("password"), None, requirePasswordChange = false, suspended = None))
  }

  test("CREATE USER foo SET PASSWORD $password SET  PASSWORD CHANGE NOT REQUIRED") {
    yields(ast.CreateUser("foo", None, Some(Param("password", CTAny)(_)), requirePasswordChange = false, suspended = None))
  }

  test("CATALOG CREATE USER foo SET PASSWORD 'password' SET STATUS SUSPENDed") {
    yields(ast.CreateUser("foo", Some("password"), None, requirePasswordChange = true, suspended = Some(true)))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS ACtiVE") {
    yields(ast.CreateUser("foo", Some("password"), None, requirePasswordChange = true, suspended = Some(false)))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE NOT REQUIRED SET   STATuS SUSPENDED") {
    yields(ast.CreateUser("foo", Some("password"), None, requirePasswordChange = false, suspended = Some(true)))
  }

  test("CREATE USER foo SET PASSWORD $password CHANGE REQUIRED SET STATUS SUSPENDED") {
    yields(ast.CreateUser("foo", None, Some(Param("password", CTAny)(_)), requirePasswordChange = true, suspended = Some(true)))
  }

  test("CREATE USER `` SET PASSwORD 'password'") {
    yields(ast.CreateUser("", Some("password"), None, requirePasswordChange = true, suspended = None))
  }

  test("CREATE USER `f:oo` SET PASSWORD 'password'") {
    yields(ast.CreateUser("f:oo", Some("password"), None, requirePasswordChange = true, suspended = None))
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

  test("CREATE USER foo SET PASSwORD 'passwordString'+$passwordParam") {
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

  test("CREATE USER foo SET PASSWORD CHANGE REQUIRED") {
    failsToParse
  }

  test("CREATE USER foo SET STATUS SUSPENDED") {
    failsToParse
  }

  test("CREATE USER foo SET PASSWORD CHANGE REQUIRED SET STATUS ACTIVE") {
    failsToParse
  }

  //  Dropping user

  test("DROP USER foo") {
    yields(ast.DropUser("foo"))
  }

  test("DROP USER ``") {
    yields(ast.DropUser(""))
  }

  test("DROP USER `f:oo`") {
    yields(ast.DropUser("f:oo"))
  }

  test("DROP USER ") {
    failsToParse
  }

  //  Altering user

  test("CATALOG ALTER USER foo SET PASSWORD 'password'") {
    yields(ast.AlterUser("foo", Some("password"), None, None, None))
  }

  test("ALTER USER `` SET PASSWORD 'password'") {
    yields(ast.AlterUser("", Some("password"), None, None, None))
  }

  test("ALTER USER `f:oo` SET PASSWORD 'password'") {
    yields(ast.AlterUser("f:oo", Some("password"), None, None, None))
  }

  test("ALTER USER foo SET PASSWORD ''") {
    yields(ast.AlterUser("foo", Some(""), None, None, None))
  }

  test("ALTER USER foo SET PASSWORD $password") {
    yields(ast.AlterUser("foo", None, Some(Param("password", CTAny)(_)), None, None))
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
    yields(ast.AlterUser("foo", Some("password"), None, requirePasswordChange = Some(true), None))
  }

  test("ALTER USER foo SET PASSWORD $password SET PASSWORD CHANGE NOT REQUIRED") {
    yields(ast.AlterUser("foo", None, Some(Param("password", CTAny)(_)), requirePasswordChange = Some(false), None))
  }

  test("CATALOG ALTER USER foo SET PASSWORD 'password' SET STATUS ACTIVE") {
    yields(ast.AlterUser("foo", Some("password"), None, None, suspended = Some(false)))
  }

  test("CATALOG ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED SET STATUS ACTIVE") {
    yields(ast.AlterUser("foo", None, None, requirePasswordChange = Some(false), suspended = Some(false)))
  }

  test("ALTER USER foo SET PASSWORD $password SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED") {
    yields(ast.AlterUser("foo", None, Some(Param("password", CTAny)(_)), requirePasswordChange = Some(false), suspended = Some(true)))
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
}
