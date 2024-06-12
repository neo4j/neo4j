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
package org.neo4j.cypher.internal.ast.factory.ddl

import org.neo4j.cypher.internal.ast.AlterUser
import org.neo4j.cypher.internal.ast.Auth
import org.neo4j.cypher.internal.ast.RemoveAuth
import org.neo4j.cypher.internal.ast.RemoveHomeDatabaseAction
import org.neo4j.cypher.internal.ast.SetHomeDatabaseAction
import org.neo4j.cypher.internal.ast.SetOwnPassword
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.UserOptions
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.util.symbols.CTAny

import scala.util.Random

class AlterUserAdministrationCommandParserTest extends UserAdministrationCommandParserTestBase {

  //  Alter user

  test("ALTER USER foo SET PASSWORD 'password'") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(password(password)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER $foo SET PASSWORD 'password'") {
    parsesTo[Statements](AlterUser(
      paramFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(password(password)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET PLAINTEXT PASSWORD 'password'") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(password(password)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test(s"ALTER USER foo SET PLAINTEXT PASSWORD $pwParamString") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(password(paramPassword)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER `` SET PASSWORD 'password'") {
    parsesTo[Statements](AlterUser(
      literalEmpty,
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(password(password)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER `f:oo` SET PASSWORD 'password'") {
    parsesTo[Statements](AlterUser(
      literalFColonOo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(password(password)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET PASSWORD ''") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(password(passwordEmpty)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test(s"ALTER USER foo SET PASSWORD $pwParamString") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(password(paramPassword)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test(s"ALTER USER foo IF EXISTS SET PASSWORD $pwParamString") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = true,
      List(),
      Some(Auth("native", List(password(paramPassword)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test(s"ALTER USER foo SET ENCRYPTED Password $pwParamString") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(password(paramPassword, isEncrypted = true)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET ENCRYPTED PASSWORD 'password'") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(password(password, isEncrypted = true)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER $foo SET ENCRYPTED PASSWORD 'password'") {
    parsesTo[Statements](AlterUser(
      paramFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(password(password, isEncrypted = true)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER `` SET ENCRYPTED PASSWORD 'password'") {
    parsesTo[Statements](AlterUser(
      literalEmpty,
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(password(password, isEncrypted = true)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test(
    "ALTER USER foo SET ENCRYPTED PASSWORD '1,04773b8510aea96ca2085cb81764b0a2,75f4201d047191c17c5e236311b7c4d77e36877503fe60b1ca6d4016160782ab'"
  ) {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth(
        "native",
        List(password(
          pw("1,04773b8510aea96ca2085cb81764b0a2,75f4201d047191c17c5e236311b7c4d77e36877503fe60b1ca6d4016160782ab"),
          isEncrypted = true
        ))
      )(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET PASSWORD CHANGE REQUIRED") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(passwordChange(true)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(passwordChange(false)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo IF EXISTS SET PASSWORD CHANGE NOT REQUIRED") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = true,
      List(),
      Some(Auth("native", List(passwordChange(false)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET STATUS SUSPENDED") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(suspended = Some(true), None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET STATUS ACTIVE") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(suspended = Some(false), None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET PASSWORD 'password' CHANGE REQUIRED") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth(
        "native",
        List(password(password), passwordChange(true))
      )(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test(s"ALTER USER foo SET PASSWORD $pwParamString SET PASSWORD CHANGE NOT REQUIRED") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth(
        "native",
        List(password(paramPassword), passwordChange(false))
      )(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET PASSWORD 'password' SET STATUS ACTIVE") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(suspended = Some(false), None),
      ifExists = false,
      List(),
      Some(Auth("native", List(password(password)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED SET STATUS ACTIVE") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(suspended = Some(false), None),
      ifExists = false,
      List(),
      Some(Auth("native", List(passwordChange(false)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test(s"ALTER USER foo SET PASSWORD $pwParamString SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(suspended = Some(true), None),
      ifExists = false,
      List(),
      Some(Auth(
        "native",
        List(password(paramPassword), passwordChange(false))
      )(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo IF EXISTS SET PASSWORD 'password' SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(suspended = Some(true), None),
      ifExists = true,
      List(),
      Some(Auth(
        "native",
        List(password(password), passwordChange(false))
      )(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET HOME DATABASE db1") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, Some(SetHomeDatabaseAction(namespacedName("db1")))),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET HOME DATABASE $db") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, Some(SetHomeDatabaseAction(paramDb))),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET HOME DATABASE null") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, Some(SetHomeDatabaseAction(namespacedName("null")))),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET PASSWORD CHANGE REQUIRED SET HOME DATABASE db1") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(
        suspended = None,
        Some(SetHomeDatabaseAction(namespacedName("db1")))
      ),
      ifExists = false,
      List(),
      Some(Auth("native", List(passwordChange(true)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET password 'password' SET HOME DATABASE db1") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, Some(SetHomeDatabaseAction(namespacedName("db1")))),
      ifExists = false,
      List(),
      Some(Auth("native", List(password(password)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET password 'password' SET PASSWORD CHANGE NOT REQUIRED SET HOME DAtabase $db") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, Some(SetHomeDatabaseAction(paramDb))),
      ifExists = false,
      List(),
      Some(Auth(
        "native",
        List(password(password), passwordChange(false))
      )(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET HOME DATABASE `#dfkfop!`") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, Some(SetHomeDatabaseAction(namespacedName("#dfkfop!")))),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET AUTH PROVIDER 'native' { SET PASSWORD 'password' }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(password(password)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET AUTH 'native' { SET PLAINTEXT PASSWORD 'password' }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(password(password)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo IF EXISTS SET AUTH 'native' { SET PASSWORD 'password' }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = true,
      List(Auth("native", List(password(password)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE REQUIRED }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(passwordChange(true)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET AUTH PROVIDER 'native' { SET PASSWORD 'password' SET PASSWORD CHANGE REQUIRED }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(password(password), passwordChange(true)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test(
    "ALTER USER foo SET AUTH 'native' { SET PASSWORD CHANGE NOT REQUIRED SET ENCRYPTED PASSWORD $password }"
  ) {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(passwordChange(false), password(paramPassword, isEncrypted = true)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test(s"ALTER USER foo SET AUTH 'foo' { SET ID $paramString }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId(paramAst)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test(s"ALTER USER foo IF EXISTS SET AUTH 'foo' { SET ID $paramString }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = true,
      List(Auth("foo", List(authId(paramAst)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET AUTH 'foo' { SET ID 'bar' } SET AUTH PROVIDER 'baz' { SET ID 'qux' }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")))(pos), Auth("baz", List(authId("qux")))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET AUTH 'foo' { SET ID 'bar' } SET PASSWORD 'password'") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")))(pos)),
      Some(Auth("native", List(password(password)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test(
    "ALTER USER foo SET AUTH 'native' { SET PASSWORD 'password' } SET AUTH 'foo' { SET ID 'bar' }"
  ) {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(password(password)))(pos), Auth("foo", List(authId("bar")))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test(
    "ALTER USER foo SET AUTH 'foo' { SET ID 'bar' } SET PASSWORD 'password' SET AUTH PROVIDER 'baz' { SET ID 'qux' }"
  ) {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")))(pos), Auth("baz", List(authId("qux")))(pos)),
      Some(Auth("native", List(password(password)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo REMOVE HOME DATABASE") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, Some(RemoveHomeDatabaseAction)),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo IF EXISTS REMOVE HOME DATABASE") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, Some(RemoveHomeDatabaseAction)),
      ifExists = true,
      List(),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo REMOVE HOME DATABASE SET HOME DATABASE bar") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, Some(SetHomeDatabaseAction(namespacedName("bar")))),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo REMOVE HOME DATABASE REMOVE HOME DATABASE") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, Some(RemoveHomeDatabaseAction)),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo REMOVE HOME DATABASE SET PASSWORD CHANGE REQUIRED") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, Some(RemoveHomeDatabaseAction)),
      ifExists = false,
      List(),
      Some(Auth("native", List(passwordChange(true)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo REMOVE ALL AUTH PROVIDERS") {
    // leaves no auth, fails at runtime
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = true, List.empty)
    )(pos))
  }

  test("ALTER USER foo REMOVE ALL AUTH PROVIDER") {
    // leaves no auth, fails at runtime
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = true, List.empty)
    )(pos))
  }

  test("ALTER USER foo REMOVE ALL AUTH") {
    // leaves no auth, fails at runtime
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = true, List.empty)
    )(pos))
  }

  test("ALTER USER foo REMOVE AUTH PROVIDERS 'foo'") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List(literalString("foo")))
    )(pos))
  }

  test("ALTER USER foo REMOVE AUTH PROVIDER $param") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List(parameter("param", CTAny)))
    )(pos))
  }

  test("ALTER USER foo REMOVE AUTH 'foo'") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List(literalString("foo")))
    )(pos))
  }

  test("ALTER USER foo REMOVE AUTH ['foo', 'bar', 'baz']") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List(listOfString("foo", "bar", "baz")))
    )(pos))
  }

  test("ALTER USER foo REMOVE AUTH 'foo' REMOVE AUTH 'foo'") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List(literalString("foo"), literalString("foo")))
    )(pos))
  }

  test("ALTER USER foo REMOVE AUTH 'foo' REMOVE AUTH ['bar', 'baz']") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List(literalString("foo"), listOfString("bar", "baz")))
    )(pos))
  }

  test("ALTER USER foo REMOVE AUTH ['bar', 'baz'] REMOVE AUTH 'foo'") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List(listOfString("bar", "baz"), literalString("foo")))
    )(pos))
  }

  test("ALTER USER foo REMOVE AUTH 'foo' REMOVE AUTH ['bar', 'foo']") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List(literalString("foo"), listOfString("bar", "foo")))
    )(pos))
  }

  test("ALTER USER foo REMOVE AUTH ['foo', 'baz'] REMOVE AUTH 'foo'") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List(listOfString("foo", "baz"), literalString("foo")))
    )(pos))
  }

  test("ALTER USER foo REMOVE AUTH ['bar', 'baz'] REMOVE ALL AUTH") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = true, List(listOfString("bar", "baz")))
    )(pos))
  }

  test("ALTER USER foo REMOVE AUTH 'foo' REMOVE ALL AUTH REMOVE AUTH ['bar', 'baz']") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = true, List(literalString("foo"), listOfString("bar", "baz")))
    )(pos))
  }

  test("ALTER USER foo REMOVE ALL AUTH REMOVE ALL AUTH") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = true, List.empty)
    )(pos))
  }

  test("ALTER USER foo REMOVE ALL AUTH REMOVE AUTH 'bar'") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = true, List(literalString("bar")))
    )(pos))
  }

  test("ALTER USER foo REMOVE ALL AUTH SET AUTH PROVIDER 'foo' { SET ID 'bar' }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")))(pos)),
      None,
      RemoveAuth(all = true, List.empty)
    )(pos))
  }

  test("ALTER USER foo REMOVE AUTH 'baz' SET AUTH PROVIDER 'foo' { SET ID 'bar' }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")))(pos)),
      None,
      RemoveAuth(all = false, List(literalString("baz")))
    )(pos))
  }

  test("ALTER USER foo REMOVE AUTH ['foo', 'bar', 'baz'] SET AUTH 'native' { SET PASSWORD 'password' }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(password(password)))(pos)),
      None,
      RemoveAuth(all = false, List(listOfString("foo", "bar", "baz")))
    )(pos))
  }

  test("ALTER USER foo REMOVE AUTH ['foo', 'bar', 'baz'] SET PASSWORD 'password'") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List.empty,
      Some(Auth("native", List(password(password)))(pos)),
      RemoveAuth(all = false, List(listOfString("foo", "bar", "baz")))
    )(pos))
  }

  // clause ordering tests

  private val removeClauses: Set[(Seq[String], RemoveAuth)] = Set(
    ("REMOVE AUTH ['foo']", RemoveAuth(all = false, List(listOfString("foo")))),
    ("REMOVE AUTH 'foo'", RemoveAuth(all = false, List(literalString("foo")))),
    ("REMOVE ALL AUTH", RemoveAuth(all = true, List.empty))
  ).flatMap {
    case (s, c) => Set((Seq(s, "REMOVE HOME DATABASE"), c), (Seq("REMOVE HOME DATABASE", s), c))
  }

  private def addRemoveClausesAndRemoveSetHomeDb(setClauses: Seq[Seq[String]]): Set[(Seq[String], RemoveAuth)] =
    setClauses.map(_.filterNot(_.equals("SET HOME DATABASE db1")))
      .flatMap(cl => removeClauses.map { case (rc, ra) => (rc ++ cl, ra) })
      .toSet

  // Test ordering when altering only `password change required` and no other auth relevant clauses
  Seq("SET PASSWORD CHANGE REQUIRED", "SET STATUS ACTIVE", "SET HOME DATABASE db1").permutations.foreach {
    clauses =>
      test(s"ALTER USER foo ${clauses.mkString(" ")}") {
        parsesTo[Statements](AlterUser(
          literalFoo,
          UserOptions(Some(false), Some(SetHomeDatabaseAction(namespacedName("db1")))),
          ifExists = false,
          List(),
          Some(Auth("native", List(passwordChange(true)))(pos)),
          RemoveAuth(all = false, List.empty)
        )(pos))
      }
  }

  // Test set permutations with CHANGE REQUIRED as part of SET PASSWORD
  Random.shuffle(setClausesOldPasswordVersion)
    .take(10) // Limit the number of tests run (and time for test setup)
    .foreach {
      clauses =>
        test(s"ALTER USER foo ${clauses.mkString(" ")}") {
          parsesTo[Statements](AlterUser(
            literalFoo,
            UserOptions(Some(false), Some(SetHomeDatabaseAction(namespacedName("db1")))),
            ifExists = false,
            List(Auth("foo", List(authId("bar")))(pos)),
            Some(Auth(
              "native",
              List(password(password), passwordChange(true))
            )(pos)),
            RemoveAuth(all = false, List.empty)
          )(pos))
        }
    }

  // Test set permutations with everything as individual clauses (old password syntax)
  Random.shuffle(setClausesSplitPasswordVersion)
    .take(10) // Limit the number of tests run (and time for test setup)
    .foreach {
      clauses =>
        test(s"ALTER USER foo ${clauses.mkString(" ")}") {
          parsesTo[Statements](AlterUser(
            literalFoo,
            UserOptions(Some(false), Some(SetHomeDatabaseAction(namespacedName("db1")))),
            ifExists = false,
            List(Auth("foo", List(authId("bar")))(pos)),
            Some(Auth("native", getNativeAuthAttributeList)(pos)),
            RemoveAuth(all = false, List.empty)
          )(pos))
        }
    }

  // Test set permutations with everything as individual clauses (new password syntax)
  Random.shuffle(setClausesNewPasswordVersion)
    .take(10) // Limit the number of tests run (and time for test setup)
    .foreach {
      clauses =>
        test(s"ALTER USER foo ${clauses.mkString(" ")}") {
          parsesTo[Statements](AlterUser(
            literalFoo,
            UserOptions(Some(false), Some(SetHomeDatabaseAction(namespacedName("db1")))),
            ifExists = false,
            getAuthListIncludingNewSyntaxNativeAuth,
            None,
            RemoveAuth(all = false, List.empty)
          )(pos))
        }
    }

  // Test remove and set permutations with everything as individual clauses (old password syntax)
  Random.shuffle(addRemoveClausesAndRemoveSetHomeDb(setClausesSplitPasswordVersion))
    .take(10) // Limit the number of tests run (and time for test setup)
    .foreach {
      case (clauses, removeAuth) =>
        test(s"ALTER USER foo ${clauses.mkString(" ")}") {
          parsesTo[Statements](AlterUser(
            literalFoo,
            UserOptions(Some(false), Some(RemoveHomeDatabaseAction)),
            ifExists = false,
            List(Auth("foo", List(authId("bar")))(pos)),
            Some(Auth("native", getNativeAuthAttributeList)(pos)),
            removeAuth
          )(pos))
        }
    }

  // Test remove and set permutations with everything as individual clauses (new password syntax)
  Random.shuffle(addRemoveClausesAndRemoveSetHomeDb(setClausesNewPasswordVersion))
    .take(10) // Limit the number of tests run (and time for test setup)
    .foreach {
      case (clauses, removeAuth) =>
        test(s"ALTER USER foo ${clauses.mkString(" ")}") {
          parsesTo[Statements](AlterUser(
            literalFoo,
            UserOptions(Some(false), Some(RemoveHomeDatabaseAction)),
            ifExists = false,
            getAuthListIncludingNewSyntaxNativeAuth,
            None,
            removeAuth
          )(pos))
        }
    }

  // Test ordering of the inner clauses of SET AUTH (most of these will fail semantic checking)
  Random.shuffle(innerNewSyntaxAtLeastTwoClauses)
    .take(10) // Limit the number of tests run (and time for test setup)
    .foreach {
      case (clauses, authAttrList) =>
        test(s"ALTER USER foo SET AUTH 'irrelevantInParsing' { ${clauses.mkString(" ")} }") {
          parsesTo[Statements](AlterUser(
            literalFoo,
            UserOptions(None, None),
            ifExists = false,
            List(Auth("irrelevantInParsing", authAttrList)(pos)),
            None,
            RemoveAuth(all = false, List.empty)
          )(pos))
        }
    }

  // offset/position tests

  test("ALTER user command finds password literal at correct offset - old syntax") {
    "ALTER USER foo SET PASSWORD 'password'" should parse[Statements].withAstLike { statement =>
      findPasswordLiteralOffset(statement) should equal(Seq("password" -> 28))
    }
  }

  test("ALTER user command finds password literal at correct offset - new syntax") {
    "ALTER USER foo SET AUTH 'native' {SET PASSWORD 'password'}" should parse[Statements].withAstLike { statement =>
      findPasswordLiteralOffset(statement) should equal(Seq("password" -> 47))
    }
    "ALTER USER foo SET AUTH 'bar' {SET PASSWORD 'password'}" should parse[Statements].withAstLike { statement =>
      findPasswordLiteralOffset(statement) should equal(Seq("password" -> 44))
    }
  }

  test("ALTER user command finds password parameter at correct offset - old syntax") {
    s"ALTER USER foo SET PASSWORD $pwParamString" should parse[Statements].withAstLike { statement =>
      findPasswordParamOffset(statement) should equal(Seq("password" -> 28))
    }
  }

  test("ALTER user command finds password parameter at correct offset - new syntax") {
    s"ALTER USER foo SET AUTH 'native' {SET PASSWORD $pwParamString}" should parse[Statements].withAstLike {
      statement =>
        findPasswordParamOffset(statement) should equal(Seq("password" -> 47))
    }
    s"ALTER USER foo SET AUTH 'bar' {SET PASSWORD $pwParamString}" should parse[Statements].withAstLike { statement =>
      findPasswordParamOffset(statement) should equal(Seq("password" -> 44))
    }
  }

  // success in parsing but failure in semantic check

  test("ALTER USER foo") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET PASSWORD 'password' SET ENCRYPTED PASSWORD $password") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(password(password), password(paramPassword, isEncrypted = true)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET PASSWORD $password SET PASSWORD 'password'") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(password(paramPassword), password(password)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET PASSWORD 'password' SET PASSWORD 'password'") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(password(password), password(password)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET PASSWORD CHANGE NOT REQUIRED SET PASSWORD CHANGE REQUIRED") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(),
      Some(Auth("native", List(passwordChange(false), passwordChange(true)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET AUTH PROVIDER 'native' { SET ID 'foo' }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(authId("foo")))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET AUTH PROVIDER 'foo' { SET PASSWORD 'password' }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(password(password)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test(
    "ALTER USER foo SET AUTH PROVIDER 'native' { SET PASSWORD 'password' } SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE REQUIRED }"
  ) {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(password(password)))(pos), Auth("native", List(passwordChange(true)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET AUTH PROVIDER 'native' { SET PASSWORD 'password' } SET PASSWORD CHANGE REQUIRED") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(password(password)))(pos)),
      Some(Auth("native", List(passwordChange(true)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET PASSWORD 'password' SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE REQUIRED }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(passwordChange(true)))(pos)),
      Some(Auth("native", List(password(password)))(pos)),
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET AUTH 'foo' { SET ID 'bar' } SET AUTH PROVIDER 'foo' { SET ID 'bar' }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")))(pos), Auth("foo", List(authId("bar")))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' } SET AUTH 'foo' { SET ID 'qux' }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar")))(pos), Auth("foo", List(authId("qux")))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' SET ID 'qux' }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar"), authId("qux")))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' SET PASSWORD 'password' }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("bar"), password(password)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET AUTH PROVIDER 'native' { SET ID 'bar' SET PASSWORD 'password' }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(authId("bar"), password(password)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET AUTH '' { SET PASSWORD '' }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("", List(password(passwordEmpty)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET AUTH PROVIDER '' { SET ID '' }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("", List(authId("")))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET AUTH PROVIDER 'native' { SET PASSWORD '' }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("native", List(password(passwordEmpty)))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo SET AUTH 'foo' { SET ID '' }") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List(Auth("foo", List(authId("")))(pos)),
      None,
      RemoveAuth(all = false, List.empty)
    )(pos))
  }

  test("ALTER USER foo REMOVE AUTH PROVIDER []") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List.empty,
      None,
      RemoveAuth(all = false, List(listOfString()))
    )(pos))
  }

  test("ALTER USER foo REMOVE AUTH PROVIDER ['']") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List.empty,
      None,
      RemoveAuth(all = false, List(listOfString("")))
    )(pos))
  }

  test("ALTER USER foo REMOVE AUTH PROVIDER ''") {
    parsesTo[Statements](AlterUser(
      literalFoo,
      UserOptions(None, None),
      ifExists = false,
      List.empty,
      None,
      RemoveAuth(all = false, List(literalString("")))
    )(pos))
  }

  // fails parsing

  test("ALTER USER foo SET NAME bar") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(
          s"""Invalid input 'NAME': expected
             |  "AUTH"
             |  "ENCRYPTED"
             |  "HOME"
             |  "PASSWORD"
             |  "PLAINTEXT"
             |  "STATUS" (line 1, column 20 (offset: 19))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'NAME': expected 'AUTH', 'HOME DATABASE', 'ENCRYPTED', 'PASSWORD', 'PLAINTEXT' or 'STATUS' (line 1, column 20 (offset: 19))
            |"ALTER USER foo SET NAME bar"
            |                    ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo SET PASSWORD 'secret' SET NAME bar") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(
          s"""Invalid input 'NAME': expected
             |  "AUTH"
             |  "ENCRYPTED"
             |  "HOME"
             |  "PASSWORD"
             |  "PLAINTEXT"
             |  "STATUS" (line 1, column 42 (offset: 41))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'NAME': expected 'AUTH', 'HOME DATABASE', 'ENCRYPTED', 'PASSWORD', 'PLAINTEXT' or 'STATUS' (line 1, column 42 (offset: 41))
            |"ALTER USER foo SET PASSWORD 'secret' SET NAME bar"
            |                                          ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo RENAME TO bar") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(
          "Invalid input 'RENAME': expected \"IF\", \"REMOVE\", \"SET\" or <EOF> (line 1, column 16 (offset: 15))"
        )
      case _ => _.withSyntaxError(
          """Invalid input 'RENAME': expected 'IF EXISTS', 'REMOVE', 'SET' or <EOF> (line 1, column 16 (offset: 15))
            |"ALTER USER foo RENAME TO bar"
            |                ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo SET PASSWORD null") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(
          "Invalid input 'null': expected \"CHANGE\", \"\\\"\", \"\\'\" or a parameter (line 1, column 29 (offset: 28))"
        )
      case _ => _.withSyntaxError(
          """Invalid input 'null': expected a parameter, a string or 'CHANGE' (line 1, column 29 (offset: 28))
            |"ALTER USER foo SET PASSWORD null"
            |                             ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo SET PASSWORD 123") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(
          "Invalid input '123': expected \"CHANGE\", \"\\\"\", \"\\'\" or a parameter (line 1, column 29 (offset: 28))"
        )
      case _ => _.withSyntaxError(
          """Invalid input '123': expected a parameter, a string or 'CHANGE' (line 1, column 29 (offset: 28))
            |"ALTER USER foo SET PASSWORD 123"
            |                             ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo SET PASSWORD") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(
          "Invalid input '': expected \"CHANGE\", \"\\\"\", \"\\'\" or a parameter (line 1, column 28 (offset: 27))"
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected a parameter, a string or 'CHANGE' (line 1, column 28 (offset: 27))
            |"ALTER USER foo SET PASSWORD"
            |                            ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo SET ENCRYPTED PASSWORD 123") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(
          "Invalid input '123': expected \"\\\"\", \"\\'\" or a parameter (line 1, column 39 (offset: 38))"
        )
      case _ => _.withSyntaxError(
          """Invalid input '123': expected a parameter or a string (line 1, column 39 (offset: 38))
            |"ALTER USER foo SET ENCRYPTED PASSWORD 123"
            |                                       ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo SET PLAINTEXT PASSWORD") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input '': expected \"\\\"\", \"\\'\" or a parameter (line 1, column 38 (offset: 37))"
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected a parameter or a string (line 1, column 38 (offset: 37))
            |"ALTER USER foo SET PLAINTEXT PASSWORD"
            |                                      ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo SET ENCRYPTED PASSWORD") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input '': expected \"\\\"\", \"\\'\" or a parameter (line 1, column 38 (offset: 37))"
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected a parameter or a string (line 1, column 38 (offset: 37))
            |"ALTER USER foo SET ENCRYPTED PASSWORD"
            |                                      ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo SET PASSWORD 'password' SET ENCRYPTED PASSWORD") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Invalid input '': expected "\"", "\'" or a parameter (line 1, column 62 (offset: 61))""")
      case _ => _.withSyntaxError(
          """Invalid input '': expected a parameter or a string (line 1, column 62 (offset: 61))
            |"ALTER USER foo SET PASSWORD 'password' SET ENCRYPTED PASSWORD"
            |                                                              ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo SET PASSWORD 'password' ENCRYPTED") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'ENCRYPTED'")
      case _ => _.withSyntaxError(
          """Invalid input 'ENCRYPTED': expected 'CHANGE', 'SET' or <EOF> (line 1, column 40 (offset: 39))
            |"ALTER USER foo SET PASSWORD 'password' ENCRYPTED"
            |                                        ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo SET PASSWORD 'password' SET STATUS ACTIVE CHANGE NOT REQUIRED") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input 'CHANGE'")
      case _ => _.withSyntaxError(
          """Invalid input 'CHANGE': expected 'SET' or <EOF> (line 1, column 58 (offset: 57))
            |"ALTER USER foo SET PASSWORD 'password' SET STATUS ACTIVE CHANGE NOT REQUIRED"
            |                                                          ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo SET STATUS") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '': expected \"ACTIVE\" or \"SUSPENDED\"")
      case _ => _.withSyntaxError(
          """Invalid input '': expected 'ACTIVE' or 'SUSPENDED' (line 1, column 26 (offset: 25))
            |"ALTER USER foo SET STATUS"
            |                          ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo PASSWORD CHANGE NOT REQUIRED") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'PASSWORD'")
      case _ => _.withSyntaxError(
          """Invalid input 'PASSWORD': expected 'IF EXISTS', 'REMOVE', 'SET' or <EOF> (line 1, column 16 (offset: 15))
            |"ALTER USER foo PASSWORD CHANGE NOT REQUIRED"
            |                ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo CHANGE NOT REQUIRED") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'CHANGE'")
      case _ => _.withSyntaxErrorContaining(
          """Invalid input 'CHANGE': expected 'IF EXISTS', 'REMOVE', 'SET' or <EOF> (line 1, column 16 (offset: 15))
            |"ALTER USER foo CHANGE NOT REQUIRED"
            |                ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo SET PASSWORD 'password' SET PASSWORD SET STATUS ACTIVE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'SET'")
      case _ => _.withSyntaxErrorContaining(
          """Invalid input 'SET': expected a parameter, a string or 'CHANGE' (line 1, column 53 (offset: 52))
            |"ALTER USER foo SET PASSWORD 'password' SET PASSWORD SET STATUS ACTIVE"
            |                                                     ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo SET PASSWORD STATUS ACTIVE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'STATUS'")
      case _ => _.withSyntaxErrorContaining(
          """Invalid input 'STATUS': expected a parameter, a string or 'CHANGE' (line 1, column 29 (offset: 28))
            |"ALTER USER foo SET PASSWORD STATUS ACTIVE"
            |                             ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo SET HOME DATABASE 123456") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '123456'")
      case _ => _.withSyntaxErrorContaining(
          """Invalid input '123456': expected a database name or a parameter (line 1, column 34 (offset: 33))
            |"ALTER USER foo SET HOME DATABASE 123456"
            |                                  ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo SET HOME DATABASE #dfkfop!") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '#'")
      case _ => _.withSyntaxError(
          """Invalid input '#': expected a database name or a parameter (line 1, column 34 (offset: 33))
            |"ALTER USER foo SET HOME DATABASE #dfkfop!"
            |                                  ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo SET PASSWORD 'password' SET STATUS IMAGINARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'IMAGINARY'")
      case _ => _.withSyntaxErrorContaining(
          """Invalid input 'IMAGINARY': expected 'ACTIVE' or 'SUSPENDED' (line 1, column 51 (offset: 50))
            |"ALTER USER foo SET PASSWORD 'password' SET STATUS IMAGINARY"
            |                                                   ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo IF NOT EXISTS SET PASSWORD 'password'") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'NOT'")
      case _ => _.withSyntaxError(
          """Invalid input 'NOT': expected 'EXISTS' (line 1, column 19 (offset: 18))
            |"ALTER USER foo IF NOT EXISTS SET PASSWORD 'password'"
            |                   ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo SET STATUS SUSPENDED REMOVE HOME DATABASE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Invalid input 'REMOVE': expected "SET" or <EOF> (line 1, column 37 (offset: 36))""")
      case _ => _.withSyntaxError(
          """Invalid input 'REMOVE': expected 'SET' or <EOF> (line 1, column 37 (offset: 36))
            |"ALTER USER foo SET STATUS SUSPENDED REMOVE HOME DATABASE"
            |                                     ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo SET HOME DATABASE db1 REMOVE HOME DATABASE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'REMOVE': expected ".", "SET" or <EOF> (line 1, column 38 (offset: 37))"""
        )
      case _ => _.withSyntaxError(
          """Invalid input 'REMOVE': expected a database name, 'SET' or <EOF> (line 1, column 38 (offset: 37))
            |"ALTER USER foo SET HOME DATABASE db1 REMOVE HOME DATABASE"
            |                                      ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo SET DEFAULT DATABASE db1") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'DEFAULT': expected")
      case _ => _.withSyntaxError(
          """Invalid input 'DEFAULT': expected 'AUTH', 'HOME DATABASE', 'ENCRYPTED', 'PASSWORD', 'PLAINTEXT' or 'STATUS' (line 1, column 20 (offset: 19))
            |"ALTER USER foo SET DEFAULT DATABASE db1"
            |                    ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo REMOVE DEFAULT DATABASE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'DEFAULT'")
      case _ => _.withSyntaxError(
          """Invalid input 'DEFAULT': expected 'AUTH', 'ALL AUTH' or 'HOME DATABASE' (line 1, column 23 (offset: 22))
            |"ALTER USER foo REMOVE DEFAULT DATABASE"
            |                       ^""".stripMargin
        )
    }
  }

  test("ALTER USER foo SET STATUS ACTIVE SET STATUS SUSPENDED") {
    val exceptionMessageStart = "Duplicate SET STATUS {SUSPENDED|ACTIVE} clause"
    val exceptionMessageJavaCC = s"$exceptionMessageStart (line 1, column 34 (offset: 33))"
    val exceptionMessageAntlr = s"$exceptionMessageStart (line 1, column 38 (offset: 37))"
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(exceptionMessageJavaCC)
      case _             => _.withSyntaxErrorContaining(exceptionMessageAntlr)
    }
  }

  test("ALTER USER foo SET HOME DATABASE db SET HOME DATABASE db") {
    val exceptionMessageStart = "Duplicate SET HOME DATABASE clause"
    val exceptionMessageJavaCC = s"$exceptionMessageStart (line 1, column 37 (offset: 36))"
    val exceptionMessageAntlr = s"$exceptionMessageStart (line 1, column 41 (offset: 40))"
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(exceptionMessageJavaCC)
      case _             => _.withSyntaxErrorContaining(exceptionMessageAntlr)
    }
  }

  test("ALTER USER foo SET AUTH PROVIDER 'foo' { }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input '}': expected "SET" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input '}': expected 'SET' (line")
    }
  }

  test("ALTER USER foo SET AUTH PROVIDER 'native' { SET PASSWORD 'password' CHANGE REQUIRED }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'CHANGE': expected "SET" or "}" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input 'CHANGE': expected 'SET' or '}' (line")
    }
  }

  test("ALTER USER foo SET AUTH PROVIDER 'foo' { SET PASSWORD 'password' CHANGE NOT REQUIRED }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'CHANGE': expected "SET" or "}" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input 'CHANGE': expected 'SET' or '}' (line")
    }
  }

  test("ALTER USER foo SET AUTH PROVIDER $param { SET ID 'bar' }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input '$': expected "\"" or "\'" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input '$': expected a string (line")
    }
  }

  test("ALTER USER foo SET AUTH PROVIDER foo { SET ID 'bar' }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'foo': expected "\"" or "\'" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input 'foo': expected a string (line")
    }
  }

  test("ALTER USER foo SET AUTH PROVIDER 'foo' { SET ID bar }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'bar': expected "\"", "\'" or a parameter (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input 'bar': expected a parameter or a string (line")
    }
  }

  test("ALTER USER foo AUTH PROVIDER 'foo' { SET ID 'bar' }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Invalid input 'AUTH': expected "IF", "REMOVE", "SET" or <EOF> (line""")
      case _ =>
        _.withSyntaxErrorContaining("Invalid input 'AUTH': expected 'IF EXISTS', 'REMOVE', 'SET' or <EOF> (line")
    }
  }

  test("ALTER USER foo AUTH 'foo' { SET ID 'bar' }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Invalid input 'AUTH': expected "IF", "REMOVE", "SET" or <EOF> (line""")
      case _ =>
        _.withSyntaxErrorContaining("Invalid input 'AUTH': expected 'IF EXISTS', 'REMOVE', 'SET' or <EOF> (line")
    }
  }

  test("ALTER USER foo SET AUTH PROVIDERS 'foo' { SET ID 'bar' }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'PROVIDERS': expected "PROVIDER", "\"" or "\'" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input 'PROVIDERS': expected a string (line")
    }
  }

  test("ALTER USER foo SET AUTH 'foo' { SET UNKNOWN 'bar' }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Invalid input 'UNKNOWN': expected "ENCRYPTED", "ID", "PASSWORD" or "PLAINTEXT" (line""")
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'UNKNOWN': expected 'ENCRYPTED', 'ID', 'PASSWORD' or 'PLAINTEXT' (line"
        )
    }
  }

  test("ALTER USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' } REMOVE ALL AUTH") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'REMOVE': expected "SET" or <EOF> (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input 'REMOVE': expected 'SET' or <EOF> (line")
    }
  }

  test("ALTER USER foo SET AUTH PROVIDER 'native' { SET PASSWORD 'password' } REMOVE AUTH 'foo'") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'REMOVE': expected "SET" or <EOF> (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input 'REMOVE': expected 'SET' or <EOF> (line")
    }
  }

  test("ALTER USER foo SET PASSWORD 'password' REMOVE ALL AUTH") {
    val exceptionJavaCC =
      """Invalid input 'REMOVE': expected "CHANGE", "SET" or <EOF> (line 1, column 40 (offset: 39))"""
    val exceptionAntlr = "Invalid input 'REMOVE': expected 'CHANGE', 'SET' or <EOF> (line 1, column 40 (offset: 39))"
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(exceptionJavaCC)
      case _             => _.withSyntaxErrorContaining(exceptionAntlr)
    }
  }

  test("ALTER USER foo SET STATUS ACTIVE REMOVE ALL AUTH") {
    val exceptionJavaCC = """Invalid input 'REMOVE': expected "SET" or <EOF> (line 1, column 34 (offset: 33))"""
    val exceptionAntlr = "Invalid input 'REMOVE': expected 'SET' or <EOF> (line 1, column 34 (offset: 33))"
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(exceptionJavaCC)
      case _             => _.withSyntaxErrorContaining(exceptionAntlr)
    }
  }

  test("ALTER USER foo SET AUTH PROVIDER 42 { SET ID 'bar' }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input '42': expected "\"" or "\'" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input '42': expected a string (line")
    }
  }

  test("ALTER USER foo SET AUTH PROVIDER 'bar' { SET ID 42 }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input '42': expected "\"", "\'" or a parameter (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input '42': expected a parameter or a string (line")
    }
  }

  test("ALTER USER foo REMOVE AUTH 42") {
    val exceptionJavaCC =
      """Invalid input '42': expected
        |  "PROVIDER"
        |  "PROVIDERS"
        |  "["
        |  "\""
        |  "\'"
        |  a parameter (line""".stripMargin
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(exceptionJavaCC)
      case _ => _.withSyntaxErrorContaining(
          "Invalid input '42': expected a parameter, a string, 'PROVIDER', 'PROVIDERS' or '[' (line"
        )
    }
  }

  test("ALTER USER foo REMOVE AUTH PROVIDER 42") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input '42': expected "[", "\"", "\'" or a parameter (line""")
      case _ => _.withSyntaxErrorContaining("Invalid input '42': expected a parameter, a string or '[' (line")
    }
  }

  test("ALTER USER foo REMOVE AUTH PROVIDER [42, 'foo']") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input '42': expected "\"", "\'" or "]" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input '42': expected a string or ']' (line")
    }
  }

  test("ALTER USER foo REMOVE AUTH PROVIDER ['foo', 42]") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input '42': expected "\"" or "\'" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input '42': expected a string (line")
    }
  }

  // Alter current user/Change own password

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO 'new'") {
    parsesTo[Statements](SetOwnPassword(passwordNew, passwordCurrent)(pos))
  }

  test("alter current user set password from 'current' to ''") {
    parsesTo[Statements](SetOwnPassword(passwordEmpty, passwordCurrent)(pos))
  }

  test("alter current user set password from '' to 'new'") {
    parsesTo[Statements](SetOwnPassword(passwordNew, passwordEmpty)(pos))
  }

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO 'passWORD123%!'") {
    parsesTo[Statements](SetOwnPassword(pw("passWORD123%!"), passwordCurrent)(pos))
  }

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO $newPassword") {
    parsesTo[Statements](SetOwnPassword(paramPasswordNew, passwordCurrent)(pos))
  }

  test("ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO 'new'") {
    parsesTo[Statements](SetOwnPassword(passwordNew, paramPasswordCurrent)(pos))
  }

  test("alter current user set password from $currentPassword to ''") {
    parsesTo[Statements](SetOwnPassword(passwordEmpty, paramPasswordCurrent)(pos))
  }

  test("ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO 'passWORD123%!'") {
    parsesTo[Statements](SetOwnPassword(pw("passWORD123%!"), paramPasswordCurrent)(pos))
  }

  test("ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword") {
    parsesTo[Statements](SetOwnPassword(paramPasswordNew, paramPasswordCurrent)(pos))
  }

  // offset/position tests

  test("ALTER CURRENT USER command finds password literal at correct offset") {
    "ALTER CURRENT USER SET PASSWORD FROM 'current' TO 'new'" should parse[Statements].withAstLike { statement =>
      findPasswordLiteralOffset(statement).toSet should equal(Set("current" -> 37, "new" -> 50))
    }
  }

  test("ALTER CURRENT USER command finds password parameter at correct offset") {
    "ALTER CURRENT USER SET PASSWORD FROM $current TO $new" should parse[Statements].withAstLike { statement =>
      findPasswordParamOffset(statement).toSet should equal(Set("current" -> 37, "new" -> 49))
    }
  }

  // fails parsing

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO null") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'null': expected \"\\\"\", \"\\'\" or a parameter")
      case _ => _.withSyntaxError(
          """Invalid input 'null': expected a parameter or a string (line 1, column 51 (offset: 50))
            |"ALTER CURRENT USER SET PASSWORD FROM 'current' TO null"
            |                                                   ^""".stripMargin
        )
    }
  }

  test("ALTER CURRENT USER SET PASSWORD FROM $current TO 123") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => identity
      case _ => _.withSyntaxError(
          """Invalid input '123': expected a parameter or a string (line 1, column 50 (offset: 49))
            |"ALTER CURRENT USER SET PASSWORD FROM $current TO 123"
            |                                                  ^""".stripMargin
        )
    }
  }

  test("ALTER PASSWORD FROM 'current' TO 'new'") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => identity
      case _ => _.withSyntaxError(
          """Invalid input 'PASSWORD': expected 'ALIAS', 'DATABASE', 'CURRENT USER SET PASSWORD FROM', 'SERVER' or 'USER' (line 1, column 7 (offset: 6))
            |"ALTER PASSWORD FROM 'current' TO 'new'"
            |       ^""".stripMargin
        )
    }
  }

  test("ALTER CURRENT PASSWORD FROM 'current' TO 'new'") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => identity
      case _ => _.withSyntaxError(
          """Invalid input 'PASSWORD': expected 'USER SET PASSWORD FROM' (line 1, column 15 (offset: 14))
            |"ALTER CURRENT PASSWORD FROM 'current' TO 'new'"
            |               ^""".stripMargin
        )
    }

  }

  test("ALTER CURRENT USER PASSWORD FROM 'current' TO 'new'") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => identity
      case _ =>
        _.withSyntaxError(
          """Invalid input 'PASSWORD': expected 'SET PASSWORD FROM' (line 1, column 20 (offset: 19))
            |"ALTER CURRENT USER PASSWORD FROM 'current' TO 'new'"
            |                    ^""".stripMargin
        )
    }
  }

  test("ALTER CURRENT USER SET PASSWORD FROM 'current' TO") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => identity
      case _ => _.withSyntaxError(
          """Invalid input '': expected a parameter or a string (line 1, column 50 (offset: 49))
            |"ALTER CURRENT USER SET PASSWORD FROM 'current' TO"
            |                                                  ^""".stripMargin
        )
    }
  }

  test("ALTER CURRENT USER SET PASSWORD FROM TO 'new'") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => identity
      case _ => _.withSyntaxError(
          """Invalid input 'TO': expected a parameter or a string (line 1, column 38 (offset: 37))
            |"ALTER CURRENT USER SET PASSWORD FROM TO 'new'"
            |                                      ^""".stripMargin
        )
    }
  }

  test("ALTER CURRENT USER SET PASSWORD TO 'new'") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => identity
      case _ => _.withSyntaxError(
          """Invalid input 'TO': expected 'FROM' (line 1, column 33 (offset: 32))
            |"ALTER CURRENT USER SET PASSWORD TO 'new'"
            |                                 ^""".stripMargin
        )
    }
  }
}
