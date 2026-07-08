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

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.AstConstructionTestSupportWithPosConversion
import org.neo4j.cypher.internal.ast.prettifier.Prettifier.maybeImmutable
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.test.util.Parses
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.gqlstatus.GqlStatusInfoCodes

import java.nio.charset.StandardCharsets

import scala.language.implicitConversions

class AdministrationAndSchemaCommandParserTestBase extends AstParsingTestBase
    with AstConstructionTestSupportWithPosConversion {

  protected def assertAst(
    expected: ast.Statement,
    comparePosition: Boolean = true,
    supportedInCypher5: Boolean = true,
    obfuscator: Boolean = true
  ): Unit =
    parsesIn[ast.Statements] {
      case Cypher5 if !supportedInCypher5 =>
        _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input ",
          fuzzyStatusDescr = true
        )
      case _ if comparePosition => _.toAstWith(ast.Statements(Seq(expected)), obfuscator = obfuscator)
      case _ => _.toAstWith(ast.Statements(Seq(expected)), comparePositions = false, obfuscator = obfuscator)
    }

  protected def assertAstVersionBased(
    expected: Boolean => ast.Statements,
    comparePosition: Boolean = true,
    supportedInCypher5: Boolean = true,
    obfuscator: Boolean = true
  ): Unit =
    parsesIn[ast.Statements] {
      case Cypher5 if !supportedInCypher5 =>
        _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input ",
          fuzzyStatusDescr = true
        )
      case Cypher5 if comparePosition => _.toAstPositioned(expected(true), obfuscator)
      case Cypher5                    => _.toAstWith(expected(true), obfuscator = obfuscator)
      case _ if comparePosition       => _.toAstPositioned(expected(false), obfuscator)
      case _                          => _.toAstWith(expected(false), obfuscator = obfuscator)
    }

  implicit def stringToLeftConvertor(s: String): Either[String, Parameter] = Left(s)
  implicit def stringToExpressionConvertor(s: String): Expression = literalString(s)
  implicit def namespacedNameConvertor(s: String): ast.DatabaseName = ast.NamespacedName(s)(pos)

  protected val showCurrentGraphTypeCypher5Error: Parses[ast.Statements] => Parses[ast.Statements] =
    _.withSyntaxErrorContaining(
      "Invalid input ",
      GqlStatusInfoCodes.STATUS_42I06,
      "error: syntax error or access rule violation - invalid input. Invalid input 'GRAPH', expected: 'USER'."
    )

  val propSeq: Seq[String] = Seq("prop")
  val accessString = "access"
  val actionString = "action"
  val grantedString: StringLiteral = literalString("GRANTED")
  val noneString: StringLiteral = literalString("none")
  def literalEmpty[T](implicit convertor: String => T): T = literal("")
  val literalUser: StringLiteral = literalString("user")
  val literalUser1: StringLiteral = literalString("user1")
  def literalFoo[T](implicit convertor: String => T): T = literal("foo")
  def literalFColonOo[T](implicit convertor: String => T): T = literal("f:oo")
  def literalBar[T](implicit convertor: String => T): T = literal("bar")
  val literalRole: Expression = literal("role")
  val literalRColonOle: Expression = literal("r:ole")
  val literalRole1: Expression = literal("role1")
  val literalRole2: Expression = literal("role2")
  val paramUser: Parameter = stringParam("user")
  val paramFoo: Parameter = stringParam("foo")
  val paramBar: Parameter = stringParam("bar")
  val namespacedParamFoo: ast.ParameterName = stringParamName("foo")
  val paramRole: Expression = stringParam("role")
  val paramRole1: Expression = stringParam("role1")
  val paramRole2: Expression = stringParam("role2")
  val accessVar: Variable = varFor(accessString)
  val labelQualifierA: InputPosition => ast.LabelQualifier = ast.LabelQualifier("A")(_)
  val labelQualifierB: InputPosition => ast.LabelQualifier = ast.LabelQualifier("B")(_)
  val relQualifierA: InputPosition => ast.RelationshipQualifier = ast.RelationshipQualifier("A")(_)
  val relQualifierB: InputPosition => ast.RelationshipQualifier = ast.RelationshipQualifier("B")(_)
  val elemQualifierA: InputPosition => ast.ElementQualifier = ast.ElementQualifier("A")(_)
  val elemQualifierB: InputPosition => ast.ElementQualifier = ast.ElementQualifier("B")(_)
  val graphScopeFoo: InputPosition => ast.NamedGraphsScope = ast.NamedGraphsScope(Seq(literalFoo))(_)
  val graphScopeParamFoo: InputPosition => ast.NamedGraphsScope = ast.NamedGraphsScope(Seq(namespacedParamFoo))(_)
  val graphScopeFooBaz: InputPosition => ast.NamedGraphsScope = ast.NamedGraphsScope(Seq(literalFoo, literal("baz")))(_)

  def literal[T](name: String)(implicit convertor: String => T): T = convertor(name)

  def stringParam(name: String): Parameter = parameter(name, CTString)
  def anyParam(name: String): Parameter = parameter(name, CTAny)
  def stringParamName(name: String): ast.ParameterName = ast.ParameterName(parameter(name, CTString))(pos)
  def intParam(name: String): Parameter = parameter(name, CTInteger)

  def namespacedName(fromCypher5: Boolean, nameParts: String*): ast.NamespacedName = if (fromCypher5) {
    namespacedName(nameParts: _*)
  } else {
    // Cypher 25 never sets an explicit namespace in the AST,
    // namespace is inferred from what is available in the DBMS
    ast.NamespacedName(List(nameParts.mkString(".")), None)(_)
  }

  def namespacedName(nameParts: String*): ast.NamespacedName =
    if (nameParts.size == 1) ast.NamespacedName(nameParts.head)(_)
    else ast.NamespacedName(nameParts.tail.toList, Some(nameParts.head))(_)

  def toUtf8Bytes(pw: String): Array[Byte] = pw.getBytes(StandardCharsets.UTF_8)

  def pw(password: String): InputPosition => SensitiveStringLiteral =
    p => SensitiveStringLiteral(toUtf8Bytes(password))(p.withInputLength(0))

  def pwParam(name: String): Parameter = parameter(name, CTString)

  def commandResultItem(original: String): ast.CommandResultItem =
    commandResultItem(original, alias = None)

  def commandResultItem(original: String, alias: Option[String]): ast.CommandResultItem =
    ast.CommandResultItem(original, alias.map(varFor).getOrElse(varFor(original)))(pos)

  def commandResultItem(
    original: String,
    varIsEscaped: Boolean,
    alias: Option[(String, Boolean)] = None
  ): ast.CommandResultItem = {
    ast.CommandResultItem(
      original,
      alias.map { case (name, isEscaped) => varFor(name, isEscaped) }.getOrElse(varFor(original, varIsEscaped))
    )(pos)
  }

  type Immutable = Boolean

  def maybeImmutablePad(immutable: Immutable): String = " " * maybeImmutable(immutable).length

  type resourcePrivilegeFunc = (
    ast.PrivilegeType,
    ast.ActionResourceBase,
    List[ast.GraphPrivilegeQualifier],
    Seq[Expression],
    Immutable
  ) => InputPosition => ast.Statement

  type noResourcePrivilegeFunc =
    (
      ast.PrivilegeType,
      List[ast.GraphPrivilegeQualifier],
      Seq[Expression],
      Immutable
    ) => InputPosition => ast.Statement

  type databasePrivilegeFunc =
    (
      ast.DatabaseAction,
      ast.DatabaseScope,
      Seq[Expression],
      Immutable
    ) => InputPosition => ast.Statement

  type loadPrivilegeFunc =
    (
      ast.LoadActions,
      ast.LoadPrivilegeQualifier,
      Seq[Expression],
      Immutable
    ) => InputPosition => ast.Statement

  type transactionPrivilegeFunc = (
    ast.DatabaseAction,
    ast.DatabaseScope,
    List[ast.DatabasePrivilegeQualifier],
    Seq[Expression],
    Immutable
  ) => InputPosition => ast.Statement

  type adminPrivilegeFunc = (ast.AdministrationAction, Seq[Expression], Immutable) => InputPosition => ast.Statement

  type dbmsPrivilegeFunc = (ast.DbmsAction, Seq[Expression], Immutable) => InputPosition => ast.Statement

  type executeProcedurePrivilegeFunc =
    (
      ast.DbmsAction,
      List[ast.ProcedurePrivilegeQualifier],
      Seq[Expression],
      Immutable
    ) => InputPosition => ast.Statement

  type executeFunctionPrivilegeFunc =
    (
      ast.DbmsAction,
      List[ast.FunctionPrivilegeQualifier],
      Seq[Expression],
      Immutable
    ) => InputPosition => ast.Statement

  type settingPrivilegeFunc =
    (
      ast.DbmsAction,
      List[ast.SettingPrivilegeQualifier],
      Seq[Expression],
      Immutable
    ) => InputPosition => ast.Statement

  type secretsPrivilegeFunc =
    (
      ast.DbmsAction,
      List[ast.SecretPrivilegeQualifier],
      Seq[Expression],
      Immutable
    ) => InputPosition => ast.Statement

  def grantGraphPrivilege(
    p: ast.PrivilegeType,
    a: ast.ActionResourceBase,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.GrantPrivilege(p, i, Some(a), q, r)

  def grantGraphPrivilege(
    p: ast.PrivilegeType,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.GrantPrivilege(p, i, None, q, r)

  def grantDatabasePrivilege(
    d: ast.DatabaseAction,
    s: ast.DatabaseScope,
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.GrantPrivilege(
      ast.DatabasePrivilege(d, s)(pos),
      i,
      None,
      List(ast.AllDatabasesQualifier()(pos)),
      r
    )

  def grantTransactionPrivilege(
    d: ast.DatabaseAction,
    s: ast.DatabaseScope,
    q: List[ast.DatabasePrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.GrantPrivilege(ast.DatabasePrivilege(d, s)(pos), i, None, q, r)

  def grantDbmsPrivilege(
    a: ast.AdministrationAction,
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement = {

    val (privilege, qualifier) = a match {
      case dbmsAction: ast.DbmsAction =>
        (ast.DbmsPrivilege(dbmsAction)(pos), ast.AllQualifier()(pos))
      case databaseAction: ast.DatabaseAndDbmsAction =>
        (ast.DatabasePrivilege(databaseAction, ast.AllDatabasesScope()(pos))(pos), ast.AllDatabasesQualifier()(pos))
      case _ => throw new IllegalStateException(a.toString)
    }
    ast.GrantPrivilege(
      privilege,
      i,
      None,
      List(qualifier),
      r
    )
  }

  def grantExecuteProcedurePrivilege(
    a: ast.DbmsAction,
    q: List[ast.ProcedurePrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    grantQualifiedDbmsPrivilege(a, q, r, i)

  def grantExecuteFunctionPrivilege(
    a: ast.DbmsAction,
    q: List[ast.FunctionPrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    grantQualifiedDbmsPrivilege(a, q, r, i)

  def grantShowSettingPrivilege(
    a: ast.DbmsAction,
    q: List[ast.SettingPrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    grantQualifiedDbmsPrivilege(a, q, r, i)

  def grantReadSecretsPrivilege(
    a: ast.DbmsAction,
    q: List[ast.SecretPrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    grantQualifiedDbmsPrivilege(a, q, r, i)

  def grantQualifiedDbmsPrivilege(
    a: ast.DbmsAction,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.GrantPrivilege(ast.DbmsPrivilege(a)(pos), i, None, q, r)

  def denyGraphPrivilege(
    p: ast.PrivilegeType,
    a: ast.ActionResourceBase,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.DenyPrivilege(p, i, Some(a), q, r)

  def denyGraphPrivilege(
    p: ast.PrivilegeType,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.DenyPrivilege(p, i, None, q, r)

  def denyDatabasePrivilege(
    d: ast.DatabaseAction,
    s: ast.DatabaseScope,
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.DenyPrivilege(
      ast.DatabasePrivilege(d, s)(pos),
      i,
      None,
      List(ast.AllDatabasesQualifier()(pos)),
      r
    )

  def denyTransactionPrivilege(
    d: ast.DatabaseAction,
    s: ast.DatabaseScope,
    q: List[ast.DatabasePrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.DenyPrivilege(ast.DatabasePrivilege(d, s)(pos), i, None, q, r)

  def denyDbmsPrivilege(
    a: ast.AdministrationAction,
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement = {
    val (privilege, qualifier) = a match {
      case dbmsAction: ast.DbmsAction =>
        (ast.DbmsPrivilege(dbmsAction)(pos), ast.AllQualifier()(pos))
      case databaseAction: ast.DatabaseAndDbmsAction =>
        (ast.DatabasePrivilege(databaseAction, ast.AllDatabasesScope()(pos))(pos), ast.AllDatabasesQualifier()(pos))
      case _ => throw new IllegalStateException(a.toString)
    }
    ast.DenyPrivilege(
      privilege,
      i,
      None,
      List(qualifier),
      r
    )
  }

  def denyExecuteProcedurePrivilege(
    a: ast.DbmsAction,
    q: List[ast.ProcedurePrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    denyQualifiedDbmsPrivilege(a, q, r, i)

  def denyExecuteFunctionPrivilege(
    a: ast.DbmsAction,
    q: List[ast.FunctionPrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    denyQualifiedDbmsPrivilege(a, q, r, i)

  def denyShowSettingPrivilege(
    a: ast.DbmsAction,
    q: List[ast.SettingPrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    denyQualifiedDbmsPrivilege(a, q, r, i)

  def denyReadSecretsPrivilege(
    a: ast.DbmsAction,
    q: List[ast.SecretPrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    denyQualifiedDbmsPrivilege(a, q, r, i)

  def denyQualifiedDbmsPrivilege(
    a: ast.DbmsAction,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.DenyPrivilege(ast.DbmsPrivilege(a)(pos), i, None, q, r)

  def revokeGrantGraphPrivilege(
    p: ast.PrivilegeType,
    a: ast.ActionResourceBase,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege(p, i, Some(a), q, r, ast.RevokeGrantType()(pos))

  def revokeGrantGraphPrivilege(
    p: ast.PrivilegeType,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege(p, i, None, q, r, ast.RevokeGrantType()(pos))

  def revokeGrantDatabasePrivilege(
    d: ast.DatabaseAction,
    s: ast.DatabaseScope,
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    revokeDatabasePrivilege(ast.RevokeGrantType()(pos), d, s, r, i)

  def revokeGrantTransactionPrivilege(
    d: ast.DatabaseAction,
    s: ast.DatabaseScope,
    q: List[ast.DatabasePrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    revokeQualifiedDatabasePrivilege(ast.RevokeGrantType()(pos), d, s, q, r, i)

  def revokeGrantDbmsPrivilege(
    a: ast.AdministrationAction,
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    revokeDbmsPrivilege(ast.RevokeGrantType()(pos), a, r, i)

  def revokeGrantExecuteProcedurePrivilege(
    a: ast.DbmsAction,
    q: List[ast.ProcedurePrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    revokeQualifiedDbmsPrivilege(ast.RevokeGrantType()(pos), a, q, r, i)

  def revokeGrantExecuteFunctionPrivilege(
    a: ast.DbmsAction,
    q: List[ast.FunctionPrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    revokeQualifiedDbmsPrivilege(ast.RevokeGrantType()(pos), a, q, r, i)

  def revokeGrantShowSettingPrivilege(
    a: ast.DbmsAction,
    q: List[ast.SettingPrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    revokeQualifiedDbmsPrivilege(ast.RevokeGrantType()(pos), a, q, r, i)

  def revokeGrantReadSecretsPrivilege(
    a: ast.DbmsAction,
    q: List[ast.SecretPrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    revokeQualifiedDbmsPrivilege(ast.RevokeGrantType()(pos), a, q, r, i)

  def revokeDenyGraphPrivilege(
    p: ast.PrivilegeType,
    a: ast.ActionResourceBase,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege(p, i, Some(a), q, r, ast.RevokeDenyType()(pos))

  def revokeDenyGraphPrivilege(
    p: ast.PrivilegeType,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege(p, i, None, q, r, ast.RevokeDenyType()(pos))

  def revokeDenyDatabasePrivilege(
    d: ast.DatabaseAction,
    s: ast.DatabaseScope,
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    revokeDatabasePrivilege(ast.RevokeDenyType()(pos), d, s, r, i)

  def revokeDenyTransactionPrivilege(
    d: ast.DatabaseAction,
    s: ast.DatabaseScope,
    q: List[ast.DatabasePrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    revokeQualifiedDatabasePrivilege(ast.RevokeDenyType()(pos), d, s, q, r, i)

  def revokeDenyDbmsPrivilege(
    a: ast.AdministrationAction,
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    revokeDbmsPrivilege(ast.RevokeDenyType()(pos), a, r, i)

  def revokeDenyExecuteProcedurePrivilege(
    a: ast.DbmsAction,
    q: List[ast.ProcedurePrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    revokeQualifiedDbmsPrivilege(ast.RevokeDenyType()(pos), a, q, r, i)

  def revokeDenyExecuteFunctionPrivilege(
    a: ast.DbmsAction,
    q: List[ast.FunctionPrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    revokeQualifiedDbmsPrivilege(ast.RevokeDenyType()(pos), a, q, r, i)

  def revokeDenyShowSettingPrivilege(
    a: ast.DbmsAction,
    q: List[ast.SettingPrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    revokeQualifiedDbmsPrivilege(ast.RevokeDenyType()(pos), a, q, r, i)

  def revokeDenyReadSecretsPrivilege(
    a: ast.DbmsAction,
    q: List[ast.SecretPrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    revokeQualifiedDbmsPrivilege(ast.RevokeDenyType()(pos), a, q, r, i)

  def revokeGraphPrivilege(
    p: ast.PrivilegeType,
    a: ast.ActionResourceBase,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege(p, i, Some(a), q, r, ast.RevokeBothType()(pos))

  def revokeGraphPrivilege(
    p: ast.PrivilegeType,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege(p, i, None, q, r, ast.RevokeBothType()(pos))

  def revokeDatabasePrivilege(
    d: ast.DatabaseAction,
    s: ast.DatabaseScope,
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    revokeDatabasePrivilege(ast.RevokeBothType()(pos), d, s, r, i)

  def revokeTransactionPrivilege(
    d: ast.DatabaseAction,
    s: ast.DatabaseScope,
    q: List[ast.DatabasePrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    revokeQualifiedDatabasePrivilege(ast.RevokeBothType()(pos), d, s, q, r, i)

  def revokeDbmsPrivilege(
    a: ast.AdministrationAction,
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    revokeDbmsPrivilege(ast.RevokeBothType()(pos), a, r, i)

  def revokeExecuteProcedurePrivilege(
    a: ast.DbmsAction,
    q: List[ast.ProcedurePrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    revokeQualifiedDbmsPrivilege(ast.RevokeBothType()(pos), a, q, r, i)

  def revokeExecuteFunctionPrivilege(
    a: ast.DbmsAction,
    q: List[ast.FunctionPrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    revokeQualifiedDbmsPrivilege(ast.RevokeBothType()(pos), a, q, r, i)

  def revokeShowSettingPrivilege(
    a: ast.DbmsAction,
    q: List[ast.SettingPrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    revokeQualifiedDbmsPrivilege(ast.RevokeBothType()(pos), a, q, r, i)

  def revokeReadSecretsPrivilege(
    a: ast.DbmsAction,
    q: List[ast.SecretPrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    revokeQualifiedDbmsPrivilege(ast.RevokeBothType()(pos), a, q, r, i)

  def revokeDatabasePrivilege(
    rt: ast.RevokeType,
    d: ast.DatabaseAction,
    s: ast.DatabaseScope,
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege(
      ast.DatabasePrivilege(d, s)(pos),
      i,
      None,
      List(ast.AllDatabasesQualifier()(pos)),
      r,
      rt
    )

  def revokeQualifiedDatabasePrivilege(
    rt: ast.RevokeType,
    d: ast.DatabaseAction,
    s: ast.DatabaseScope,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege(ast.DatabasePrivilege(d, s)(pos), i, None, q, r, rt)

  def revokeDbmsPrivilege(
    rt: ast.RevokeType,
    a: ast.AdministrationAction,
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement = {
    val (privilege, qualifier) = a match {
      case dbmsAction: ast.DbmsAction =>
        (ast.DbmsPrivilege(dbmsAction)(pos), ast.AllQualifier()(pos))
      case databaseAction: ast.DatabaseAndDbmsAction =>
        (ast.DatabasePrivilege(databaseAction, ast.AllDatabasesScope()(pos))(pos), ast.AllDatabasesQualifier()(pos))
      case _ => throw new IllegalStateException(a.toString)
    }

    ast.RevokePrivilege(
      privilege,
      i,
      None,
      List(qualifier),
      r,
      rt
    )
  }

  def revokeQualifiedDbmsPrivilege(
    rt: ast.RevokeType,
    a: ast.DbmsAction,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Expression],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege(ast.DbmsPrivilege(a)(pos), i, None, q, r, rt)

  def returnClause(
    returnItems: ast.ReturnItems,
    orderBy: Option[ast.OrderBy] = None,
    limit: Option[ast.Limit] = None,
    distinct: Boolean = false,
    skip: Option[ast.Skip] = None
  ): ast.Return =
    ast.Return(distinct, returnItems, None, orderBy, skip, limit)(pos)
}
