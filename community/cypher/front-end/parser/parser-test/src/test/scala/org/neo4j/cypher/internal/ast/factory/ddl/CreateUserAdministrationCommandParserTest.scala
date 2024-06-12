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

import org.neo4j.cypher.internal.ast.Auth
import org.neo4j.cypher.internal.ast.CreateUser
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsInvalidSyntax
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.SetHomeDatabaseAction
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.UserOptions
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc

import scala.util.Random

class CreateUserAdministrationCommandParserTest extends UserAdministrationCommandParserTestBase {

  test("CREATE USER foo SET PASSWORD 'password'") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(password)))(pos))
    )(pos))
  }

  test("CREATE USER $foo SET PASSWORD 'password'") {
    parsesTo[Statements](CreateUser(
      paramFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(password)))(pos))
    )(pos))
  }

  test("CREATE USER foo SET PLAINTEXT PASSWORD 'password'") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(password)))(pos))
    )(pos))
  }

  test(s"CREATE USER foo SET PLAINTEXT PASSWORD $pwParamString") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(paramPassword)))(pos))
    )(pos))
  }

  test(s"CREATE USER $paramString SET PASSWORD $pwParamString") {
    parsesTo[Statements](CreateUser(
      paramAst,
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(paramPassword)))(pos))
    )(pos))
  }

  test("CREATE USER `foo` SET PASSwORD 'password'") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(password)))(pos))
    )(pos))
  }

  test("CREATE USER `!#\"~` SeT PASSWORD 'password'") {
    parsesTo[Statements](CreateUser(
      literal("!#\"~"),
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(password)))(pos))
    )(pos))
  }

  test("CREATE USER foo SeT PASSWORD 'pasS5Wor%d'") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(pw("pasS5Wor%d"))))(pos))
    )(pos))
  }

  test("CREATE USER foo SET PASSwORD ''") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(passwordEmpty)))(pos))
    )(pos))
  }

  test(s"CREATE uSER foo SET PASSWORD $pwParamString") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(paramPassword)))(pos))
    )(pos))
  }

  test("CREaTE USER foo SET PASSWORD 'password' CHANGE REQUIRED") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth(
        "native",
        List(password(password), passwordChange(true))
      )(pos))
    )(pos))
  }

  test(s"CREATE USER foo SET PASSWORD $pwParamString CHANGE REQUIRED") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth(
        "native",
        List(password(paramPassword), passwordChange(true))
      )(pos))
    )(pos))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE required") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth(
        "native",
        List(password(password), passwordChange(true))
      )(pos))
    )(pos))
  }

  test("CREATE USER foo SET PASSWORD 'password' CHAngE NOT REQUIRED") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth(
        "native",
        List(password(password), passwordChange(false))
      )(pos))
    )(pos))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE NOT REQUIRED") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth(
        "native",
        List(password(password), passwordChange(false))
      )(pos))
    )(pos))
  }

  test(s"CREATE USER foo SET PASSWORD $pwParamString SET  PASSWORD CHANGE NOT REQUIRED") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth(
        "native",
        List(password(paramPassword), passwordChange(false))
      )(pos))
    )(pos))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS SUSPENDed") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(Some(true), None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(password)))(pos))
    )(pos))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS ACtiVE") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(Some(false), None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(password)))(pos))
    )(pos))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE NOT REQUIRED SET   STATuS SUSPENDED") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(Some(true), None),
      IfExistsThrowError,
      List(),
      Some(Auth(
        "native",
        List(password(password), passwordChange(false))
      )(pos))
    )(pos))
  }

  test(s"CREATE USER foo SET PASSWORD $pwParamString CHANGE REQUIRED SET STATUS SUSPENDED") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(Some(true), None),
      IfExistsThrowError,
      List(),
      Some(Auth(
        "native",
        List(password(paramPassword), passwordChange(true))
      )(pos))
    )(pos))
  }

  test("CREATE USER `` SET PASSwORD 'password'") {
    parsesTo[Statements](CreateUser(
      literalEmpty,
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(password)))(pos))
    )(pos))
  }

  test("CREATE USER `f:oo` SET PASSWORD 'password'") {
    parsesTo[Statements](CreateUser(
      literalFColonOo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(password)))(pos))
    )(pos))
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD 'password'") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsDoNothing,
      List(),
      Some(Auth("native", List(password(password)))(pos))
    )(pos))
  }

  test(s"CREATE uSER foo IF NOT EXISTS SET PASSWORD $pwParamString") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsDoNothing,
      List(),
      Some(Auth("native", List(password(paramPassword)))(pos))
    )(pos))
  }

  test(s"CREATE USER foo IF NOT EXISTS SET PASSWORD $pwParamString CHANGE REQUIRED") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsDoNothing,
      List(),
      Some(Auth(
        "native",
        List(password(paramPassword), passwordChange(true))
      )(pos))
    )(pos))
  }

  test(s"CREATE USER foo IF NOT EXISTS SET PASSWORD $pwParamString SET STATUS SUSPENDED") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(Some(true), None),
      IfExistsDoNothing,
      List(),
      Some(Auth("native", List(password(paramPassword)))(pos))
    )(pos))
  }

  test(s"CREATE USER foo IF NOT EXISTS SET PASSWORD $pwParamString CHANGE REQUIRED SET STATUS SUSPENDED") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(Some(true), None),
      IfExistsDoNothing,
      List(),
      Some(Auth(
        "native",
        List(password(paramPassword), passwordChange(true))
      )(pos))
    )(pos))
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD 'password'") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsReplace,
      List(),
      Some(Auth("native", List(password(password)))(pos))
    )(pos))
  }

  test(s"CREATE OR REPLACE uSER foo SET PASSWORD $pwParamString") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsReplace,
      List(),
      Some(Auth("native", List(password(paramPassword)))(pos))
    )(pos))
  }

  test(s"CREATE OR REPLACE USER foo SET PASSWORD $pwParamString CHANGE REQUIRED") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsReplace,
      List(),
      Some(Auth(
        "native",
        List(password(paramPassword), passwordChange(true))
      )(pos))
    )(pos))
  }

  test(s"CREATE OR REPLACE USER foo SET PASSWORD $pwParamString SET STATUS SUSPENDED") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(Some(true), None),
      IfExistsReplace,
      List(),
      Some(Auth("native", List(password(paramPassword)))(pos))
    )(pos))
  }

  test(s"CREATE OR REPLACE USER foo SET PASSWORD $pwParamString CHANGE REQUIRED SET STATUS SUSPENDED") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(Some(true), None),
      IfExistsReplace,
      List(),
      Some(Auth(
        "native",
        List(password(paramPassword), passwordChange(true))
      )(pos))
    )(pos))
  }

  test(
    "CREATE USER foo SET ENCRYPTED PASSWORD '1,04773b8510aea96ca2085cb81764b0a2,75f4201d047191c17c5e236311b7c4d77e36877503fe60b1ca6d4016160782ab'"
  ) {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth(
        "native",
        List(password(
          pw("1,04773b8510aea96ca2085cb81764b0a2,75f4201d047191c17c5e236311b7c4d77e36877503fe60b1ca6d4016160782ab"),
          isEncrypted = true
        ))
      )(pos))
    )(pos))
  }

  test("CREATE USER $foo SET encrYPTEd PASSWORD 'password'") {
    parsesTo[Statements](CreateUser(
      paramFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(password, isEncrypted = true)))(pos))
    )(pos))
  }

  test(s"CREATE USER $paramString SET ENCRYPTED Password $pwParamString") {
    parsesTo[Statements](CreateUser(
      paramAst,
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(paramPassword, isEncrypted = true)))(pos))
    )(pos))
  }

  test("CREATE OR REPLACE USER foo SET encrypted password 'sha256,x1024,0x2460294fe,b3ddb287a'") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsReplace,
      List(),
      Some(Auth("native", List(password(pw("sha256,x1024,0x2460294fe,b3ddb287a"), isEncrypted = true)))(pos))
    )(pos))
  }

  test("CREATE USER foo SET password 'password' SET HOME DATABASE db1") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, Some(SetHomeDatabaseAction(namespacedName("db1")))),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(password)))(pos))
    )(pos))
  }

  test("CREATE USER foo SET password 'password' SET HOME DATABASE $db") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, Some(SetHomeDatabaseAction(paramDb))),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(password)))(pos))
    )(pos))
  }

  test("CREATE OR REPLACE USER foo SET password 'password' SET HOME DATABASE db1") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, Some(SetHomeDatabaseAction(namespacedName("db1")))),
      IfExistsReplace,
      List(),
      Some(Auth("native", List(password(password)))(pos))
    )(pos))
  }

  test("CREATE USER foo IF NOT EXISTS SET password 'password' SET HOME DATABASE db1") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, Some(SetHomeDatabaseAction(namespacedName("db1")))),
      IfExistsDoNothing,
      List(),
      Some(Auth("native", List(password(password)))(pos))
    )(pos))
  }

  test("CREATE USER foo SET password 'password' SET PASSWORD CHANGE NOT REQUIRED SET HOME DAtabase $db") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, Some(SetHomeDatabaseAction(paramDb))),
      IfExistsThrowError,
      List(),
      Some(Auth(
        "native",
        List(password(password), passwordChange(false))
      )(pos))
    )(pos))
  }

  test("CREATE USER foo SET password 'password' SET HOME DATABASE `#dfkfop!`") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, Some(SetHomeDatabaseAction(namespacedName("#dfkfop!")))),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(password)))(pos))
    )(pos))
  }

  test("CREATE USER foo SET password 'password' SET HOME DATABASE null") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, Some(SetHomeDatabaseAction(namespacedName("null")))),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(password)))(pos))
    )(pos))
  }

  test("CREATE USER foo SET AUTH PROVIDER 'native' { SET PASSWORD 'password' }") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(password(password)))(pos)),
      None
    )(pos))
  }

  test("CREATE USER foo SET AUTH 'native' { SET PASSWORD 'password' }") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(password(password)))(pos)),
      None
    )(pos))
  }

  test("CREATE USER foo IF NOT EXISTS SET AUTH 'native' { SET PLAINTEXT PASSWORD 'password' }") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsDoNothing,
      List(Auth("native", List(password(password)))(pos)),
      None
    )(pos))
  }

  test("CREATE USER foo SET AUTH PROVIDER 'native' { SET PASSWORD 'password' SET PASSWORD CHANGE REQUIRED }") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(password(password), passwordChange(true)))(pos)),
      None
    )(pos))
  }

  test(
    "CREATE USER foo SET AUTH 'native' { SET PASSWORD CHANGE NOT REQUIRED SET ENCRYPTED PASSWORD $password }"
  ) {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(passwordChange(false), password(paramPassword, isEncrypted = true)))(pos)),
      None
    )(pos))
  }

  test("CREATE USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' }") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId("bar")))(pos)),
      None
    )(pos))
  }

  test("CREATE OR REPLACE USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' }") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsReplace,
      List(Auth("foo", List(authId("bar")))(pos)),
      None
    )(pos))
  }

  test(s"CREATE USER foo SET AUTH 'foo' { SET ID $paramString }") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId(paramAst)))(pos)),
      None
    )(pos))
  }

  test("CREATE USER foo SET AUTH 'foo' { SET ID 'bar' } SET AUTH PROVIDER 'baz' { SET ID 'qux' }") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId("bar")))(pos), Auth("baz", List(authId("qux")))(pos)),
      None
    )(pos))
  }

  test("CREATE USER foo SET AUTH 'foo' { SET ID 'bar' } SET PASSWORD 'password'") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId("bar")))(pos)),
      Some(Auth("native", List(password(password)))(pos))
    )(pos))
  }

  test(
    "CREATE USER foo SET AUTH 'native' { SET PASSWORD 'password' } SET AUTH 'foo' { SET ID 'bar' }"
  ) {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(password(password)))(pos), Auth("foo", List(authId("bar")))(pos)),
      None
    )(pos))
  }

  test(
    "CREATE USER foo SET AUTH 'foo' { SET ID 'bar' } SET PASSWORD 'password' SET AUTH PROVIDER 'baz' { SET ID 'qux' }"
  ) {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId("bar")))(pos), Auth("baz", List(authId("qux")))(pos)),
      Some(Auth("native", List(password(password)))(pos))
    )(pos))
  }

  // clause ordering tests

  // Test permutations with CHANGE REQUIRED as part of SET PASSWORD
  Random.shuffle(setClausesOldPasswordVersion)
    .take(10) // Limit the number of tests run (and time for test setup)
    .foreach {
      clauses =>
        test(s"CREATE USER foo ${clauses.mkString(" ")}") {
          parsesTo[Statements](CreateUser(
            literalFoo,
            UserOptions(Some(false), Some(SetHomeDatabaseAction(namespacedName("db1")))),
            IfExistsThrowError,
            List(Auth("foo", List(authId("bar")))(pos)),
            Some(Auth(
              "native",
              List(password(password), passwordChange(true))
            )(pos))
          )(pos))
        }
    }

  // Test permutations with everything as individual clauses (old password syntax)
  Random.shuffle(setClausesSplitPasswordVersion)
    .take(10) // Limit the number of tests run (and time for test setup)
    .foreach {
      clauses =>
        test(s"CREATE USER foo ${clauses.mkString(" ")}") {
          parsesTo[Statements](CreateUser(
            literalFoo,
            UserOptions(Some(false), Some(SetHomeDatabaseAction(namespacedName("db1")))),
            IfExistsThrowError,
            List(Auth("foo", List(authId("bar")))(pos)),
            Some(Auth("native", getNativeAuthAttributeList)(pos))
          )(pos))
        }
    }

  // Test permutations with everything as individual clauses (new password syntax)
  Random.shuffle(setClausesNewPasswordVersion)
    .take(10) // Limit the number of tests run (and time for test setup)
    .foreach {
      clauses =>
        test(s"CREATE USER foo ${clauses.mkString(" ")}") {
          parsesTo[Statements](CreateUser(
            literalFoo,
            UserOptions(Some(false), Some(SetHomeDatabaseAction(namespacedName("db1")))),
            IfExistsThrowError,
            getAuthListIncludingNewSyntaxNativeAuth,
            None
          )(pos))
        }
    }

  // Test ordering of the inner clauses of SET AUTH (most of these will fail semantic checking)
  Random.shuffle(innerNewSyntaxAtLeastTwoClauses)
    .take(10) // Limit the number of tests run (and time for test setup)
    .foreach {
      case (clauses, authAttrList) =>
        test(s"CREATE USER foo SET AUTH 'irrelevantInParsing' { ${clauses.mkString(" ")} }") {
          parsesTo[Statements](CreateUser(
            literalFoo,
            UserOptions(None, None),
            IfExistsThrowError,
            List(Auth("irrelevantInParsing", authAttrList)(pos)),
            None
          )(pos))
        }
    }

  // offset/position tests

  test("CREATE command finds password literal at correct offset - old syntax") {
    "CREATE USER foo SET PASSWORD 'password'" should parse[Statements].withAstLike { statement =>
      findPasswordLiteralOffset(statement) should equal(Seq("password" -> 29))
    }
  }

  test("CREATE command finds password literal at correct offset - new syntax") {
    "CREATE USER foo SET AUTH 'native' {SET PASSWORD 'password'}" should parse[Statements].withAstLike { statement =>
      findPasswordLiteralOffset(statement) should equal(Seq("password" -> 48))
    }
    "CREATE USER foo SET AUTH 'bar' {SET PASSWORD 'password'}" should parse[Statements].withAstLike { statement =>
      findPasswordLiteralOffset(statement) should equal(Seq("password" -> 45))
    }
  }

  test("CREATE command finds password parameter at correct offset - old syntax") {
    s"CREATE USER foo SET PASSWORD $pwParamString" should parse[Statements].withAstLike { statement =>
      findPasswordParamOffset(statement) should equal(Seq("password" -> 29))
    }
  }

  test("CREATE command finds password parameter at correct offset - new syntax") {
    s"CREATE USER foo SET AUTH 'native' {SET PASSWORD $pwParamString}" should parse[Statements].withAstLike {
      statement =>
        findPasswordParamOffset(statement) should equal(Seq("password" -> 48))
    }
    s"CREATE USER foo SET AUTH 'bar' {SET PASSWORD $pwParamString}" should parse[Statements].withAstLike { statement =>
      findPasswordParamOffset(statement) should equal(Seq("password" -> 45))
    }
  }

  // success in parsing but failure in semantic check

  test("CREATE OR REPLACE USER foo IF NOT EXISTS SET PASSWORD 'password'") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsInvalidSyntax,
      List(),
      Some(Auth("native", List(password(password)))(pos))
    )(pos))
  }

  test("CREATE OR REPLACE USER foo IF NOT EXISTS SET AUTH 'native' { SET PASSWORD 'password' }") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsInvalidSyntax,
      List(Auth("native", List(password(password)))(pos)),
      None
    )(pos))
  }

  test("CREATE OR REPLACE USER foo IF NOT EXISTS SET AUTH 'foo' { SET ID 'bar' }") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsInvalidSyntax,
      List(Auth("foo", List(authId("bar")))(pos)),
      None
    )(pos))
  }

  test("CREATE USER foo SET PASSWORD CHANGE REQUIRED") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(passwordChange(true)))(pos))
    )(pos))
  }

  test("CREATE USER foo SET STATUS SUSPENDED") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(Some(true), None),
      IfExistsThrowError,
      List(),
      None
    )(pos))
  }

  test("CREATE USER foo SET PASSWORD CHANGE REQUIRED SET STATUS ACTIVE") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(Some(false), None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(passwordChange(true)))(pos))
    )(pos))
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD CHANGE REQUIRED") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsDoNothing,
      List(),
      Some(Auth("native", List(passwordChange(true)))(pos))
    )(pos))
  }

  test("CREATE USER foo IF NOT EXISTS SET STATUS ACTIVE") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(Some(false), None),
      IfExistsDoNothing,
      List(),
      None
    )(pos))
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(Some(true), None),
      IfExistsDoNothing,
      List(),
      Some(Auth("native", List(passwordChange(false)))(pos))
    )(pos))
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD CHANGE NOT REQUIRED") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsReplace,
      List(),
      Some(Auth("native", List(passwordChange(false)))(pos))
    )(pos))
  }

  test("CREATE OR REPLACE USER foo SET STATUS SUSPENDED") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(Some(true), None),
      IfExistsReplace,
      List(),
      None
    )(pos))
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD CHANGE REQUIRED SET STATUS ACTIVE") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(Some(false), None),
      IfExistsReplace,
      List(),
      Some(Auth("native", List(passwordChange(true)))(pos))
    )(pos))
  }

  test("CREATE USER foo SET PASSWORD $password CHANGE NOT REQUIRED SET PASSWORD CHANGE REQUIRED") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(paramPassword), passwordChange(false), passwordChange(true)))(pos))
    )(pos))
  }

  test("CREATE USER foo SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE REQUIRED }") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(passwordChange(true)))(pos)),
      None
    )(pos))
  }

  test("CREATE USER foo SET AUTH PROVIDER 'native' { SET ID 'foo' }") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(authId("foo")))(pos)),
      None
    )(pos))
  }

  test("CREATE USER foo SET AUTH PROVIDER 'foo' { SET PASSWORD 'password' }") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(password(password)))(pos)),
      None
    )(pos))
  }

  test(
    "CREATE USER foo SET AUTH PROVIDER 'native' { SET PASSWORD 'password' } SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE REQUIRED }"
  ) {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(password(password)))(pos), Auth("native", List(passwordChange(true)))(pos)),
      None
    )(pos))
  }

  test("CREATE USER foo SET AUTH PROVIDER 'native' { SET PASSWORD 'password' } SET PASSWORD CHANGE REQUIRED") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(password(password)))(pos)),
      Some(Auth("native", List(passwordChange(true)))(pos))
    )(pos))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET AUTH PROVIDER 'native' { SET PASSWORD CHANGE REQUIRED }") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(passwordChange(true)))(pos)),
      Some(Auth("native", List(password(password)))(pos))
    )(pos))
  }

  test("CREATE USER foo SET AUTH 'foo' { SET ID 'bar' } SET AUTH PROVIDER 'foo' { SET ID 'bar' }") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId("bar")))(pos), Auth("foo", List(authId("bar")))(pos)),
      None
    )(pos))
  }

  test("CREATE USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' } SET AUTH 'foo' { SET ID 'qux' }") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId("bar")))(pos), Auth("foo", List(authId("qux")))(pos)),
      None
    )(pos))
  }

  test("CREATE USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' SET ID 'qux' }") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId("bar"), authId("qux")))(pos)),
      None
    )(pos))
  }

  test("CREATE USER foo SET AUTH PROVIDER 'foo' { SET ID 'bar' SET PASSWORD 'password' }") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId("bar"), password(password)))(pos)),
      None
    )(pos))
  }

  test("CREATE USER foo SET AUTH PROVIDER 'native' { SET ID 'bar' SET PASSWORD 'password' }") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(authId("bar"), password(password)))(pos)),
      None
    )(pos))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD 'password'") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(),
      Some(Auth("native", List(password(password), password(password)))(pos))
    )(pos))
  }

  test("CREATE USER foo SET AUTH '' { SET PASSWORD '' }") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("", List(password(passwordEmpty)))(pos)),
      None
    )(pos))
  }

  test("CREATE USER foo SET AUTH PROVIDER '' { SET ID '' }") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("", List(authId("")))(pos)),
      None
    )(pos))
  }

  test("CREATE USER foo SET AUTH PROVIDER 'native' { SET PASSWORD '' }") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("native", List(password(passwordEmpty)))(pos)),
      None
    )(pos))
  }

  test("CREATE USER foo SET AUTH 'foo' { SET ID '' }") {
    parsesTo[Statements](CreateUser(
      literalFoo,
      UserOptions(None, None),
      IfExistsThrowError,
      List(Auth("foo", List(authId("")))(pos)),
      None
    )(pos))
  }

  // fails parsing

  test("CREATE USER foo") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input '': expected \"IF\" or \"SET\" (line 1, column 16 (offset: 15))")
      case _ => _.withSyntaxError(
          """Invalid input '': expected 'IF NOT EXISTS' or 'SET' (line 1, column 16 (offset: 15))
            |"CREATE USER foo"
            |                ^""".stripMargin
        )
    }
  }

  test("CREATE USER \"foo\" SET PASSwORD 'password'") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input 'foo': expected a parameter or an identifier (line 1, column 13 (offset: 12))"
        )
      case _ => _.withSyntaxError(
          """Invalid input '"foo"': expected a graph pattern, a parameter or an identifier (line 1, column 13 (offset: 12))
            |"CREATE USER "foo" SET PASSwORD 'password'"
            |             ^""".stripMargin
        )
    }
  }

  test("CREATE USER !#\"~ SeT PASSWORD 'password'") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input '!': expected a parameter or an identifier (line 1, column 13 (offset: 12))"
        )
      case _ => _.withSyntaxError(
          """Invalid input '!': expected a graph pattern, a parameter or an identifier (line 1, column 13 (offset: 12))
            |"CREATE USER !#"~ SeT PASSWORD 'password'"
            |             ^""".stripMargin
        )
    }
  }

  test("CREATE USER fo,o SET PASSWORD 'password'") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input ',': expected \"IF\" or \"SET\" (line 1, column 15 (offset: 14))")
      case _ => _.withSyntaxError(
          """Invalid input ',': expected 'IF NOT EXISTS' or 'SET' (line 1, column 15 (offset: 14))
            |"CREATE USER fo,o SET PASSWORD 'password'"
            |               ^""".stripMargin
        )
    }
  }

  test("CREATE USER f:oo SET PASSWORD 'password'") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input ':': expected \"IF\" or \"SET\" (line 1, column 14 (offset: 13))")
      case _ => _.withSyntaxError(
          """Invalid input ':': expected 'IF NOT EXISTS' or 'SET' (line 1, column 14 (offset: 13))
            |"CREATE USER f:oo SET PASSWORD 'password'"
            |              ^""".stripMargin
        )
    }
  }

  test("CREATE USER foo SET PASSWORD") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input '': expected \"CHANGE\", \"\\\"\", \"\\'\" or a parameter (line 1, column 29 (offset: 28))"
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected a parameter, a string or 'CHANGE' (line 1, column 29 (offset: 28))
            |"CREATE USER foo SET PASSWORD"
            |                             ^""".stripMargin
        )
    }
  }

  test("CREATE USER foo SET ENCRYPTED PASSWORD 123") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input '123': expected \"\\\"\", \"\\'\" or a parameter (line 1, column 40 (offset: 39))"
        )
      case _ => _.withSyntaxError(
          """Invalid input '123': expected a parameter or a string (line 1, column 40 (offset: 39))
            |"CREATE USER foo SET ENCRYPTED PASSWORD 123"
            |                                        ^""".stripMargin
        )
    }
  }

  test("CREATE USER foo SET ENCRYPTED PASSWORD") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input '': expected \"\\\"\", \"\\'\" or a parameter (line 1, column 39 (offset: 38))"
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected a parameter or a string (line 1, column 39 (offset: 38))
            |"CREATE USER foo SET ENCRYPTED PASSWORD"
            |                                       ^""".stripMargin
        )
    }
  }

  test("CREATE USER foo SET PLAINTEXT PASSWORD") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input '': expected \"\\\"\", \"\\'\" or a parameter (line 1, column 39 (offset: 38))"
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected a parameter or a string (line 1, column 39 (offset: 38))
            |"CREATE USER foo SET PLAINTEXT PASSWORD"
            |                                       ^""".stripMargin
        )
    }
  }

  test("CREATE USER foo SET PASSWORD 'password' SET ENCRYPTED PASSWORD") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input '': expected \"\\\"\", \"\\'\" or a parameter (line 1, column 63 (offset: 62))"
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected a parameter or a string (line 1, column 63 (offset: 62))
            |"CREATE USER foo SET PASSWORD 'password' SET ENCRYPTED PASSWORD"
            |                                                               ^""".stripMargin
        )
    }
  }

  test("CREATE USER foo SET PASSWORD 'password' ENCRYPTED") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input 'ENCRYPTED': expected \"CHANGE\", \"SET\" or <EOF> (line 1, column 41 (offset: 40))"
        )
      case _ => _.withSyntaxError(
          """Invalid input 'ENCRYPTED': expected 'CHANGE', 'SET' or <EOF> (line 1, column 41 (offset: 40))
            |"CREATE USER foo SET PASSWORD 'password' ENCRYPTED"
            |                                         ^""".stripMargin
        )
    }
  }

  test("CREATE USER foo SET PASSwORD 'passwordString'+" + pwParamString + "expressions.Parameter") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input '+': expected \"CHANGE\", \"SET\" or <EOF> (line 1, column 46 (offset: 45))"
        )
      case _ => _.withSyntaxError(
          """Invalid input '+': expected 'CHANGE', 'SET' or <EOF> (line 1, column 46 (offset: 45))
            |"CREATE USER foo SET PASSwORD 'passwordString'+$passwordexpressions.Parameter"
            |                                              ^""".stripMargin
        )
    }
  }

  test("CREATE USER foo SET PASSWORD null CHANGE REQUIRED") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input 'null': expected \"CHANGE\", \"\\\"\", \"\\'\" or a parameter (line 1, column 30 (offset: 29))"
        )
      case _ => _.withSyntaxError(
          """Invalid input 'null': expected a parameter, a string or 'CHANGE' (line 1, column 30 (offset: 29))
            |"CREATE USER foo SET PASSWORD null CHANGE REQUIRED"
            |                              ^""".stripMargin
        )
    }
  }

  test("CREATE USER foo PASSWORD 'password'") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input 'PASSWORD': expected \"IF\" or \"SET\" (line 1, column 17 (offset: 16))")
      case _ => _.withSyntaxError(
          """Invalid input 'PASSWORD': expected 'IF NOT EXISTS' or 'SET' (line 1, column 17 (offset: 16))
            |"CREATE USER foo PASSWORD 'password'"
            |                 ^""".stripMargin
        )
    }
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS ACTIVE CHANGE NOT REQUIRED") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input 'CHANGE': expected \"SET\" or <EOF> (line 1, column 59 (offset: 58))")
      case _ => _.withSyntaxError(
          """Invalid input 'CHANGE': expected 'SET' or <EOF> (line 1, column 59 (offset: 58))
            |"CREATE USER foo SET PASSWORD 'password' SET STATUS ACTIVE CHANGE NOT REQUIRED"
            |                                                           ^""".stripMargin
        )
    }
  }

  test("CREATE USER foo SET PASSWORD 'password' SET HOME DATABASE db1 CHANGE NOT REQUIRED") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input 'CHANGE': expected \".\", \"SET\" or <EOF> (line 1, column 63 (offset: 62))")
      case _ => _.withSyntaxError(
          """Invalid input 'CHANGE': expected a database name, 'SET' or <EOF> (line 1, column 63 (offset: 62))
            |"CREATE USER foo SET PASSWORD 'password' SET HOME DATABASE db1 CHANGE NOT REQUIRED"
            |                                                               ^""".stripMargin
        )
    }
  }

  test("CREATE USER foo SET PASSWORD 'password' SET DEFAULT DATABASE db1") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'DEFAULT': expected
            |  "AUTH"
            |  "ENCRYPTED"
            |  "HOME"
            |  "PASSWORD"
            |  "PLAINTEXT"
            |  "STATUS" (line 1, column 45 (offset: 44))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'DEFAULT': expected 'AUTH', 'HOME DATABASE', 'ENCRYPTED', 'PASSWORD', 'PLAINTEXT' or 'STATUS' (line 1, column 45 (offset: 44))
            |"CREATE USER foo SET PASSWORD 'password' SET DEFAULT DATABASE db1"
            |                                             ^""".stripMargin
        )
    }
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STAUS ACTIVE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          s"""Invalid input 'STAUS': expected
             |  "AUTH"
             |  "ENCRYPTED"
             |  "HOME"
             |  "PASSWORD"
             |  "PLAINTEXT"
             |  "STATUS" (line 1, column 45 (offset: 44))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'STAUS': expected 'AUTH', 'HOME DATABASE', 'ENCRYPTED', 'PASSWORD', 'PLAINTEXT' or 'STATUS' (line 1, column 45 (offset: 44))
            |"CREATE USER foo SET PASSWORD 'password' SET STAUS ACTIVE"
            |                                             ^""".stripMargin
        )
    }
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS IMAGINARY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input 'IMAGINARY': expected \"ACTIVE\" or \"SUSPENDED\" (line 1, column 52 (offset: 51))"
        )
      case _ => _.withSyntaxError(
          """Invalid input 'IMAGINARY': expected 'ACTIVE' or 'SUSPENDED' (line 1, column 52 (offset: 51))
            |"CREATE USER foo SET PASSWORD 'password' SET STATUS IMAGINARY"
            |                                                    ^""".stripMargin
        )
    }
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input '': expected \"ACTIVE\" or \"SUSPENDED\" (line 1, column 51 (offset: 50))")
      case _ => _.withSyntaxError(
          """Invalid input '': expected 'ACTIVE' or 'SUSPENDED' (line 1, column 51 (offset: 50))
            |"CREATE USER foo SET PASSWORD 'password' SET STATUS"
            |                                                   ^""".stripMargin
        )
    }
  }

  test("CREATE USER foo IF EXISTS SET PASSWORD 'bar'") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input 'EXISTS': expected \"NOT\" (line 1, column 20 (offset: 19))")
      case _ => _.withSyntaxError(
          """Invalid input 'EXISTS': expected 'NOT EXISTS' (line 1, column 20 (offset: 19))
            |"CREATE USER foo IF EXISTS SET PASSWORD 'bar'"
            |                    ^""".stripMargin
        )
    }
  }

  test("CREATE USER foo IF NOT EXISTS") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '': expected \"SET\" (line 1, column 30 (offset: 29))")
      case _ => _.withSyntaxError(
          """Invalid input '': expected 'SET' (line 1, column 30 (offset: 29))
            |"CREATE USER foo IF NOT EXISTS"
            |                              ^""".stripMargin
        )
    }
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input '': expected \"CHANGE\", \"\\\"\", \"\\'\" or a parameter (line 1, column 43 (offset: 42))"
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected a parameter, a string or 'CHANGE' (line 1, column 43 (offset: 42))
            |"CREATE USER foo IF NOT EXISTS SET PASSWORD"
            |                                           ^""".stripMargin
        )
    }
  }

  test("CREATE OR REPLACE USER foo") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input '': expected \"IF\" or \"SET\" (line 1, column 27 (offset: 26))")
      case _ => _.withSyntaxError(
          """Invalid input '': expected 'IF NOT EXISTS' or 'SET' (line 1, column 27 (offset: 26))
            |"CREATE OR REPLACE USER foo"
            |                           ^""".stripMargin
        )
    }
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input '': expected \"CHANGE\", \"\\\"\", \"\\'\" or a parameter (line 1, column 40 (offset: 39))"
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected a parameter, a string or 'CHANGE' (line 1, column 40 (offset: 39))
            |"CREATE OR REPLACE USER foo SET PASSWORD"
            |                                        ^""".stripMargin
        )
    }
  }

  test("CREATE USER foo SET PASSWORD 'bar' SET HOME DATABASE 123456") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input '123456': expected a parameter or an identifier (line 1, column 54 (offset: 53))"
        )
      case _ => _.withSyntaxError(
          """Invalid input '123456': expected a database name or a parameter (line 1, column 54 (offset: 53))
            |"CREATE USER foo SET PASSWORD 'bar' SET HOME DATABASE 123456"
            |                                                      ^""".stripMargin
        )
    }
  }

  test("CREATE USER foo SET PASSWORD 'bar' SET HOME DATABASE #dfkfop!") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '#': expected a parameter or an identifier")
      case _ => _.withSyntaxError(
          """Invalid input '#': expected a database name or a parameter (line 1, column 54 (offset: 53))
            |"CREATE USER foo SET PASSWORD 'bar' SET HOME DATABASE #dfkfop!"
            |                                                      ^""".stripMargin
        )
    }
  }

  test("CREATE USER foo SET PASSWORD $password SET STATUS ACTIVE SET STATUS SUSPENDED") {
    val exceptionMessage =
      s"""Duplicate SET STATUS {SUSPENDED|ACTIVE} clause (line 1, column 58 (offset: 57))""".stripMargin
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(exceptionMessage)
      case _ => _.withSyntaxError(
          """Duplicate SET STATUS {SUSPENDED|ACTIVE} clause (line 1, column 62 (offset: 61))
            |"CREATE USER foo SET PASSWORD $password SET STATUS ACTIVE SET STATUS SUSPENDED"
            |                                                              ^""".stripMargin
        )
    }
  }

  test("CREATE USER foo SET PASSWORD $password SET HOME DATABASE db SET HOME DATABASE db") {
    val exceptionMessage =
      s"""Duplicate SET HOME DATABASE clause (line 1, column 61 (offset: 60))""".stripMargin
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(exceptionMessage)
      case _ => _.withSyntaxError(
          """Duplicate SET HOME DATABASE clause (line 1, column 65 (offset: 64))
            |"CREATE USER foo SET PASSWORD $password SET HOME DATABASE db SET HOME DATABASE db"
            |                                                                 ^""".stripMargin
        )
    }
  }

  test("CREATE USER foo SET AUTH PROVIDER 'foo' { }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input '}': expected "SET" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input '}': expected 'SET' (line")
    }
  }

  test("CREATE USER foo SET AUTH PROVIDER 'native' { SET PASSWORD 'password' CHANGE REQUIRED }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'CHANGE': expected "SET" or "}" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input 'CHANGE': expected 'SET' or '}' (line")
    }
  }

  test("CREATE USER foo SET AUTH PROVIDER 'foo' { SET PASSWORD 'password' CHANGE NOT REQUIRED }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'CHANGE': expected "SET" or "}" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input 'CHANGE': expected 'SET' or '}' (line")
    }
  }

  test("CREATE USER foo SET AUTH PROVIDER $param { SET ID 'bar' }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input '$': expected "\"" or "\'" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input '$': expected a string (line")
    }
  }

  test("CREATE USER foo SET AUTH PROVIDER foo { SET ID 'bar' }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'foo': expected "\"" or "\'" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input 'foo': expected a string (line")
    }
  }

  test("CREATE USER foo SET AUTH PROVIDER 'foo' { SET ID bar }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'bar': expected "\"", "\'" or a parameter (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input 'bar': expected a parameter or a string (line")
    }
  }

  test("CREATE USER foo AUTH PROVIDER 'foo' { SET ID 'bar' }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'AUTH': expected "IF" or "SET" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input 'AUTH': expected 'IF NOT EXISTS' or 'SET' (line")
    }
  }

  test("CREATE USER foo AUTH 'foo' { SET ID 'bar' }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'AUTH': expected "IF" or "SET" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input 'AUTH': expected 'IF NOT EXISTS' or 'SET' (line")
    }
  }

  test("CREATE USER foo SET AUTH PROVIDERS 'foo' { SET ID 'bar' }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'PROVIDERS': expected "PROVIDER", "\"" or "\'" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input 'PROVIDERS': expected a string (line")
    }
  }

  test("CREATE USER foo SET AUTH 'foo' { SET UNKNOWN 'bar' }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Invalid input 'UNKNOWN': expected "ENCRYPTED", "ID", "PASSWORD" or "PLAINTEXT" (line""")
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'UNKNOWN': expected 'ENCRYPTED', 'ID', 'PASSWORD' or 'PLAINTEXT' (line"
        )
    }
  }

  test("CREATE USER foo SET AUTH PROVIDER 42 { SET ID 'bar' }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input '42': expected "\"" or "\'" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input '42': expected a string (line")
    }
  }

  test("CREATE USER foo SET AUTH PROVIDER 'bar' { SET ID 42 }") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input '42': expected "\"", "\'" or a parameter (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input '42': expected a parameter or a string (line")
    }
  }
}
