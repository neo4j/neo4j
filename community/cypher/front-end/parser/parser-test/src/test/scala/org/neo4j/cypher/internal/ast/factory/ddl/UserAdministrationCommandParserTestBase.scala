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
import org.neo4j.cypher.internal.ast.AuthAttribute
import org.neo4j.cypher.internal.ast.AuthId
import org.neo4j.cypher.internal.ast.NativeAuthAttribute
import org.neo4j.cypher.internal.ast.ParameterName
import org.neo4j.cypher.internal.ast.Password
import org.neo4j.cypher.internal.ast.PasswordChange
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.SensitiveParameter
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition

import java.nio.charset.StandardCharsets.UTF_8

abstract class UserAdministrationCommandParserTestBase extends AdministrationAndSchemaCommandParserTestBase {
  protected val userString = "user"
  protected val varUser: Variable = varFor(userString)
  protected val password: InputPosition => SensitiveStringLiteral = pw("password")
  protected val passwordNew: InputPosition => SensitiveStringLiteral = pw("new")
  protected val passwordCurrent: InputPosition => SensitiveStringLiteral = pw("current")
  protected val passwordEmpty: InputPosition => SensitiveStringLiteral = pw("")
  protected val paramPassword: Parameter = pwParam("password")
  protected val paramPasswordNew: Parameter = pwParam("newPassword")
  protected val paramPasswordCurrent: Parameter = pwParam("currentPassword")
  protected val paramDb: ParameterName = stringParamName("db")
  protected val pwParamString = s"$$password"
  protected val paramString = s"$$param"
  protected val paramAst: Parameter = stringParam("param")

  protected def authId(id: String): AuthId = authId(literalString(id))
  protected def authId(id: Expression): AuthId = AuthId(id)(pos)
  protected def password(pass: Expression, isEncrypted: Boolean = false): Password = Password(pass, isEncrypted)(pos)
  protected def passwordChange(requireChange: Boolean): PasswordChange = PasswordChange(requireChange)(pos)

  protected def findPasswordLiteralOffset(statement: Statements): Seq[(String, Int)] =
    statement.folder.findAllByClass[SensitiveStringLiteral].map(l => (new String(l.value, UTF_8), l.position.offset))

  protected def findPasswordParamOffset(statement: Statements): Seq[(String, Int)] =
    statement.folder.findAllByClass[SensitiveParameter].map(p => (p.name, p.position.offset))

  // Helper lists for clause ordering in create and alter user

  private val nonPasswordClauses: Seq[String] = Seq(
    "SET AUTH 'foo' { SET ID 'bar' }",
    "SET STATUS ACTIVE",
    "SET HOME DATABASE db1"
  )

  protected val setClausesOldPasswordVersion: Seq[Seq[String]] =
    (nonPasswordClauses :+ "SET password 'password' CHANGE REQUIRED").permutations.toSeq

  protected val setClausesSplitPasswordVersion: Seq[Seq[String]] =
    (nonPasswordClauses ++ Seq("SET PASSWORD 'password'", "SET PASSWORD CHANGE REQUIRED")).permutations.toSeq

  private val newPasswordClauseStrings: Seq[String] =
    Seq("SET PASSWORD 'password'", "SET PASSWORD CHANGE REQUIRED")
      .permutations
      .map(_.mkString("SET AUTH 'native' {", " ", "}"))
      .toSeq

  protected val setClausesNewPasswordVersion: Seq[Seq[String]] =
    newPasswordClauseStrings.map(s => nonPasswordClauses :+ s).flatMap(ls => ls.permutations)

  protected def getNativeAuthAttributeList: List[NativeAuthAttribute] = {
    val indexPassword = testName.indexOf("SET PASSWORD 'password'")
    val indexPasswordChange = testName.indexOf("SET PASSWORD CHANGE REQUIRED")
    val unorderedNativeAuthAttributeList = List(password(password), passwordChange(true))
    if (indexPassword < indexPasswordChange) unorderedNativeAuthAttributeList
    else unorderedNativeAuthAttributeList.reverse
  }

  protected def getAuthListIncludingNewSyntaxNativeAuth: List[Auth] = {
    val indexNativeAuth = testName.indexOf("SET AUTH 'native'")
    val indexExternalAuth = testName.indexOf("SET AUTH 'foo'")
    val unorderedAuthList = List(Auth("foo", List(authId("bar")))(pos), Auth("native", getNativeAuthAttributeList)(pos))
    if (indexExternalAuth < indexNativeAuth) unorderedAuthList
    else unorderedAuthList.reverse
  }

  private val passwordClauseVariations: Set[(String, AuthAttribute)] = Set(
    ("SET ENCRYPTED PASSWORD 'secret'", password(pw("secret"), isEncrypted = true)),
    ("SET PLAINTEXT PASSWORD 'secret'", password(pw("secret"))),
    ("SET PASSWORD 'secret'", password(pw("secret"))),
    ("SET ENCRYPTED PASSWORD $secret", password(pwParam("secret"), isEncrypted = true)),
    ("SET PLAINTEXT PASSWORD $secret", password(pwParam("secret"))),
    ("SET PASSWORD $secret", password(pwParam("secret")))
  )

  private val passwordChangeRequiredClauseVariations: Set[(String, AuthAttribute)] = Set(
    ("SET PASSWORD CHANGE NOT REQUIRED", passwordChange(false)),
    ("SET PASSWORD CHANGE REQUIRED", passwordChange(true))
  )

  private val idClauseVariations: Set[(String, AuthAttribute)] = Set(
    ("SET ID 'id'", authId("id")),
    ("SET ID $id", authId(stringParam("id")))
  )

  // combinations of:
  // * SET PW and SET PW
  // * SET PW CHANGE and SET PW CHANGE
  // * SET ID and SET ID
  private val doubleClauses: Set[(List[String], List[AuthAttribute])] =
    passwordClauseVariations.flatMap { case (clause1, ast1) =>
      passwordClauseVariations.flatMap { case (clause2, ast2) =>
        Set((List(clause1, clause2), List(ast1, ast2)))
      }
    } ++ idClauseVariations.flatMap { case (clause1, ast1) =>
      idClauseVariations.flatMap { case (clause2, ast2) =>
        Set((List(clause1, clause2), List(ast1, ast2)))
      }
    } ++ Set(
      // Change required needs to be handled differently when combined with itself as the parser cannot go back from change not required once one has been found
      // so if CHANGE NOT REQUIRED is followed by CHANGE REQUIRED both ast parts will say false,
      // this isn't an issue as the query will throw on multiple CHANGE REQUIRED clauses in semantic checking
      (
        List("SET PASSWORD CHANGE REQUIRED", "SET PASSWORD CHANGE REQUIRED"),
        List(passwordChange(true), passwordChange(true))
      ),
      (
        List("SET PASSWORD CHANGE REQUIRED", "SET PASSWORD CHANGE NOT REQUIRED"),
        List(passwordChange(true), passwordChange(false))
      ),
      (
        List("SET PASSWORD CHANGE NOT REQUIRED", "SET PASSWORD CHANGE REQUIRED"),
        List(passwordChange(false), passwordChange(false))
      ),
      (
        List("SET PASSWORD CHANGE NOT REQUIRED", "SET PASSWORD CHANGE NOT REQUIRED"),
        List(passwordChange(false), passwordChange(false))
      )
    )

  // combinations of:
  // * SET PW and SET PW
  // * SET PW and SET PW CHANGE
  // * SET PW and SET ID
  // * SET PW CHANGE and SET PW
  // * SET PW CHANGE and SET PW CHANGE
  // * SET PW CHANGE and SET ID
  // * SET ID and SET PW
  // * SET ID and SET PW CHANGE
  // * SET ID and SET ID
  // * SET PW and SET PW CHANGE and SET ID
  protected val innerNewSyntaxAtLeastTwoClauses: Set[(List[String], List[AuthAttribute])] =
    passwordClauseVariations.flatMap { case (pwClause, pwAst) =>
      passwordChangeRequiredClauseVariations.flatMap { case (crClause, crAst) =>
        val passwordAndChangeRequiredCombinations =
          Set((List(pwClause, crClause), List(pwAst, crAst)), (List(crClause, pwClause), List(crAst, pwAst)))

        idClauseVariations.flatMap { case (idClause, idAst) =>
          val idAndPasswordCombinations =
            Set((List(pwClause, idClause), List(pwAst, idAst)), (List(idClause, pwClause), List(idAst, pwAst)))
          val idAndChangeRequiredCombinations =
            Set((List(crClause, idClause), List(crAst, idAst)), (List(idClause, crClause), List(idAst, crAst)))
          val allClauseCombinations = Set(
            ((pwClause, pwAst), (crClause, crAst), (idClause, idAst)),
            ((pwClause, pwAst), (idClause, idAst), (crClause, crAst)),
            ((crClause, crAst), (pwClause, pwAst), (idClause, idAst)),
            ((crClause, crAst), (idClause, idAst), (pwClause, pwAst)),
            ((idClause, idAst), (pwClause, pwAst), (crClause, crAst)),
            ((idClause, idAst), (crClause, crAst), (pwClause, pwAst))
          ).map {
            case ((clause1, ast1), (clause2, ast2), (clause3, ast3)) =>
              (List(clause1, clause2, clause3), List(ast1, ast2, ast3))
          }

          allClauseCombinations ++ idAndPasswordCombinations ++ idAndChangeRequiredCombinations
        } ++ passwordAndChangeRequiredCombinations
      }
    } ++ doubleClauses
}
