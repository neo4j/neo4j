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
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.ParameterName
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.ParserSupport.NotAntlr
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTString

import java.nio.charset.StandardCharsets

class AdministrationAndSchemaCommandParserTestBase extends AstParsingTestBase with LegacyAstParsingTestSupport {

  protected def assertAst(expected: ast.Statement, comparePosition: Boolean = true): Unit = {
    if (comparePosition) parses[Statements](NotAntlr).toAstPositioned(Statements(Seq(expected)))
    else parses[Statements](NotAntlr).toAst(Statements(Seq(expected)))
  }

  implicit val stringConvertor: String => Either[String, Parameter] = s => Left(s)
  implicit val namespacedNameConvertor: String => DatabaseName = s => NamespacedName(s)(pos)

  val propSeq: Seq[String] = Seq("prop")
  val accessString = "access"
  val actionString = "action"
  val grantedString: StringLiteral = literalString("GRANTED")
  val noneString: StringLiteral = literalString("none")
  val literalEmpty: Either[String, Parameter] = literal("")
  val literalUser: Either[String, Parameter] = literal("user")
  val literalUser1: Either[String, Parameter] = literal("user1")
  def literalFoo[T](implicit convertor: String => T): T = literal("foo")
  val literalFColonOo: Either[String, Parameter] = literal("f:oo")
  val literalBar: Either[String, Parameter] = literal("bar")
  val literalRole: Either[String, Parameter] = literal("role")
  val literalRColonOle: Either[String, Parameter] = literal("r:ole")
  val literalRole1: Either[String, Parameter] = literal("role1")
  val literalRole2: Either[String, Parameter] = literal("role2")
  val paramUser: Either[String, Parameter] = stringParam("user")
  val paramFoo: Either[String, Parameter] = stringParam("foo")
  val namespacedParamFoo: ParameterName = stringParamName("foo")
  val paramRole: Either[String, Parameter] = stringParam("role")
  val paramRole1: Either[String, Parameter] = stringParam("role1")
  val paramRole2: Either[String, Parameter] = stringParam("role2")
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

  def stringParam(name: String): Either[String, Parameter] = Right(parameter(name, CTString))
  def stringParamName(name: String): ParameterName = ParameterName(parameter(name, CTString))(pos)

  def namespacedName(nameParts: String*): NamespacedName =
    if (nameParts.size == 1) NamespacedName(nameParts.head)(_)
    else NamespacedName(nameParts.tail.toList, Some(nameParts.head))(_)

  def toUtf8Bytes(pw: String): Array[Byte] = pw.getBytes(StandardCharsets.UTF_8)

  def pw(password: String): InputPosition => SensitiveStringLiteral =
    SensitiveStringLiteral(toUtf8Bytes(password))(_)

  def pwParam(name: String): Parameter = parameter(name, CTString)

  def commandResultItem(original: String, alias: Option[String] = None): ast.CommandResultItem =
    ast.CommandResultItem(original, alias.map(varFor).getOrElse(varFor(original)))(pos)

  def withFromYield(
    returnItems: ast.ReturnItems,
    orderBy: Option[ast.OrderBy] = None,
    skip: Option[ast.Skip] = None,
    limit: Option[ast.Limit] = None,
    where: Option[ast.Where] = None
  ): ast.With =
    ast.With(distinct = false, returnItems, orderBy, skip, limit, where = where, withType = ast.ParsedAsYield)(pos)

  type Immutable = Boolean

  def immutableOrEmpty(immutable: Immutable): String = if (immutable) " IMMUTABLE" else ""

  type resourcePrivilegeFunc = (
    ast.PrivilegeType,
    ast.ActionResource,
    List[ast.GraphPrivilegeQualifier],
    Seq[Either[String, Parameter]],
    Immutable
  ) => InputPosition => ast.Statement

  type noResourcePrivilegeFunc =
    (
      ast.PrivilegeType,
      List[ast.GraphPrivilegeQualifier],
      Seq[Either[String, Parameter]],
      Immutable
    ) => InputPosition => ast.Statement

  type databasePrivilegeFunc =
    (
      ast.DatabaseAction,
      ast.DatabaseScope,
      Seq[Either[String, Parameter]],
      Immutable
    ) => InputPosition => ast.Statement

  type loadPrivilegeFunc =
    (
      ast.LoadActions,
      ast.LoadPrivilegeQualifier,
      Seq[Either[String, Parameter]],
      Immutable
    ) => InputPosition => ast.Statement

  type transactionPrivilegeFunc = (
    ast.DatabaseAction,
    ast.DatabaseScope,
    List[ast.DatabasePrivilegeQualifier],
    Seq[Either[String, Parameter]],
    Immutable
  ) => InputPosition => ast.Statement

  type dbmsPrivilegeFunc = (ast.DbmsAction, Seq[Either[String, Parameter]], Immutable) => InputPosition => ast.Statement

  type executeProcedurePrivilegeFunc =
    (
      ast.DbmsAction,
      List[ast.ProcedurePrivilegeQualifier],
      Seq[Either[String, Parameter]],
      Immutable
    ) => InputPosition => ast.Statement

  type executeFunctionPrivilegeFunc =
    (
      ast.DbmsAction,
      List[ast.FunctionPrivilegeQualifier],
      Seq[Either[String, Parameter]],
      Immutable
    ) => InputPosition => ast.Statement

  type settingPrivilegeFunc =
    (
      ast.DbmsAction,
      List[ast.SettingPrivilegeQualifier],
      Seq[Either[String, Parameter]],
      Immutable
    ) => InputPosition => ast.Statement

  def grantGraphPrivilege(
    p: ast.PrivilegeType,
    a: ast.ActionResource,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.GrantPrivilege(p, i, Some(a), q, r)

  def grantGraphPrivilege(
    p: ast.PrivilegeType,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.GrantPrivilege(p, i, None, q, r)

  def grantDatabasePrivilege(
    d: ast.DatabaseAction,
    s: ast.DatabaseScope,
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.GrantPrivilege.databaseAction(d, i, s, r)

  def grantTransactionPrivilege(
    d: ast.DatabaseAction,
    s: ast.DatabaseScope,
    q: List[ast.DatabasePrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.GrantPrivilege.databaseAction(d, i, s, r, q)

  def grantDbmsPrivilege(
    a: ast.DbmsAction,
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.GrantPrivilege.dbmsAction(a, i, r)

  def grantExecuteProcedurePrivilege(
    a: ast.DbmsAction,
    q: List[ast.ProcedurePrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.GrantPrivilege.dbmsAction(a, i, r, q)

  def grantExecuteFunctionPrivilege(
    a: ast.DbmsAction,
    q: List[ast.FunctionPrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.GrantPrivilege.dbmsAction(a, i, r, q)

  def grantShowSettingPrivilege(
    a: ast.DbmsAction,
    q: List[ast.SettingPrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.GrantPrivilege.dbmsAction(a, i, r, q)

  def denyGraphPrivilege(
    p: ast.PrivilegeType,
    a: ast.ActionResource,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.DenyPrivilege(p, i, Some(a), q, r)

  def denyGraphPrivilege(
    p: ast.PrivilegeType,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.DenyPrivilege(p, i, None, q, r)

  def denyDatabasePrivilege(
    d: ast.DatabaseAction,
    s: ast.DatabaseScope,
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.DenyPrivilege.databaseAction(d, i, s, r)

  def denyTransactionPrivilege(
    d: ast.DatabaseAction,
    s: ast.DatabaseScope,
    q: List[ast.DatabasePrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.DenyPrivilege.databaseAction(d, i, s, r, q)

  def denyDbmsPrivilege(
    a: ast.DbmsAction,
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.DenyPrivilege.dbmsAction(a, i, r)

  def denyExecuteProcedurePrivilege(
    a: ast.DbmsAction,
    q: List[ast.ProcedurePrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.DenyPrivilege.dbmsAction(a, i, r, q)

  def denyExecuteFunctionPrivilege(
    a: ast.DbmsAction,
    q: List[ast.FunctionPrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.DenyPrivilege.dbmsAction(a, i, r, q)

  def denyShowSettingPrivilege(
    a: ast.DbmsAction,
    q: List[ast.SettingPrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.DenyPrivilege.dbmsAction(a, i, r, q)

  def revokeGrantGraphPrivilege(
    p: ast.PrivilegeType,
    a: ast.ActionResource,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege(p, i, Some(a), q, r, ast.RevokeGrantType()(pos))

  def revokeGrantGraphPrivilege(
    p: ast.PrivilegeType,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege(p, i, None, q, r, ast.RevokeGrantType()(pos))

  def revokeGrantDatabasePrivilege(
    d: ast.DatabaseAction,
    s: ast.DatabaseScope,
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege.databaseAction(d, i, s, r, ast.RevokeGrantType()(pos))

  def revokeGrantTransactionPrivilege(
    d: ast.DatabaseAction,
    s: ast.DatabaseScope,
    q: List[ast.DatabasePrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege.databaseAction(d, i, s, r, ast.RevokeGrantType()(pos), q)

  def revokeGrantDbmsPrivilege(
    a: ast.DbmsAction,
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, i, r, ast.RevokeGrantType()(pos))

  def revokeGrantExecuteProcedurePrivilege(
    a: ast.DbmsAction,
    q: List[ast.ProcedurePrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, i, r, ast.RevokeGrantType()(pos), q)

  def revokeGrantExecuteFunctionPrivilege(
    a: ast.DbmsAction,
    q: List[ast.FunctionPrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, i, r, ast.RevokeGrantType()(pos), q)

  def revokeGrantShowSettingPrivilege(
    a: ast.DbmsAction,
    q: List[ast.SettingPrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, i, r, ast.RevokeGrantType()(pos), q)

  def revokeDenyGraphPrivilege(
    p: ast.PrivilegeType,
    a: ast.ActionResource,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege(p, i, Some(a), q, r, ast.RevokeDenyType()(pos))

  def revokeDenyGraphPrivilege(
    p: ast.PrivilegeType,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege(p, i, None, q, r, ast.RevokeDenyType()(pos))

  def revokeDenyDatabasePrivilege(
    d: ast.DatabaseAction,
    s: ast.DatabaseScope,
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege.databaseAction(d, i, s, r, ast.RevokeDenyType()(pos))

  def revokeDenyTransactionPrivilege(
    d: ast.DatabaseAction,
    s: ast.DatabaseScope,
    q: List[ast.DatabasePrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege.databaseAction(d, i, s, r, ast.RevokeDenyType()(pos), q)

  def revokeDenyDbmsPrivilege(
    a: ast.DbmsAction,
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, i, r, ast.RevokeDenyType()(pos))

  def revokeDenyExecuteProcedurePrivilege(
    a: ast.DbmsAction,
    q: List[ast.ProcedurePrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, i, r, ast.RevokeDenyType()(pos), q)

  def revokeDenyExecuteFunctionPrivilege(
    a: ast.DbmsAction,
    q: List[ast.FunctionPrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, i, r, ast.RevokeDenyType()(pos), q)

  def revokeDenyShowSettingPrivilege(
    a: ast.DbmsAction,
    q: List[ast.SettingPrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, i, r, ast.RevokeDenyType()(pos), q)

  def revokeGraphPrivilege(
    p: ast.PrivilegeType,
    a: ast.ActionResource,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege(p, i, Some(a), q, r, ast.RevokeBothType()(pos))

  def revokeGraphPrivilege(
    p: ast.PrivilegeType,
    q: List[ast.PrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege(p, i, None, q, r, ast.RevokeBothType()(pos))

  def revokeDatabasePrivilege(
    d: ast.DatabaseAction,
    s: ast.DatabaseScope,
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege.databaseAction(d, i, s, r, ast.RevokeBothType()(pos))

  def revokeTransactionPrivilege(
    d: ast.DatabaseAction,
    s: ast.DatabaseScope,
    q: List[ast.DatabasePrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege.databaseAction(d, i, s, r, ast.RevokeBothType()(pos), q)

  def revokeDbmsPrivilege(
    a: ast.DbmsAction,
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, i, r, ast.RevokeBothType()(pos))

  def revokeExecuteProcedurePrivilege(
    a: ast.DbmsAction,
    q: List[ast.ProcedurePrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, i, r, ast.RevokeBothType()(pos), q)

  def revokeExecuteFunctionPrivilege(
    a: ast.DbmsAction,
    q: List[ast.FunctionPrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, i, r, ast.RevokeBothType()(pos), q)

  def revokeShowSettingPrivilege(
    a: ast.DbmsAction,
    q: List[ast.SettingPrivilegeQualifier],
    r: Seq[Either[String, Parameter]],
    i: Immutable
  ): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, i, r, ast.RevokeBothType()(pos), q)

  def returnClause(
    returnItems: ast.ReturnItems,
    orderBy: Option[ast.OrderBy] = None,
    limit: Option[ast.Limit] = None,
    distinct: Boolean = false,
    skip: Option[ast.Skip] = None
  ): ast.Return =
    ast.Return(distinct, returnItems, orderBy, skip, limit)(pos)
}
