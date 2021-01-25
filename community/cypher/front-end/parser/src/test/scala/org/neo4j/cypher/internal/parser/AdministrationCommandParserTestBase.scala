/*
 * Copyright (c) "Neo4j"
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

import java.nio.charset.StandardCharsets

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.ActionResource
import org.neo4j.cypher.internal.ast.AdminAction
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.DatabaseAction
import org.neo4j.cypher.internal.ast.DatabasePrivilegeQualifier
import org.neo4j.cypher.internal.ast.DatabaseScope
import org.neo4j.cypher.internal.ast.ElementQualifier
import org.neo4j.cypher.internal.ast.FunctionPrivilegeQualifier
import org.neo4j.cypher.internal.ast.GraphPrivilegeQualifier
import org.neo4j.cypher.internal.ast.GraphScope
import org.neo4j.cypher.internal.ast.LabelQualifier
import org.neo4j.cypher.internal.ast.NamedGraphScope
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
import org.neo4j.cypher.internal.ast.PrivilegeType
import org.neo4j.cypher.internal.ast.ProcedurePrivilegeQualifier
import org.neo4j.cypher.internal.ast.RelationshipQualifier
import org.neo4j.cypher.internal.ast.RevokeBothType
import org.neo4j.cypher.internal.ast.RevokeDenyType
import org.neo4j.cypher.internal.ast.RevokeGrantType
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTString
import org.parboiled.scala.Rule1

class AdministrationCommandParserTestBase
  extends ParserAstTest[ast.Statement] with Statement with AstConstructionTestSupport {
  val propSeq = Seq("prop")
  val accessString = "access"
  val actionString = "action"
  val grantedString: StringLiteral = literalString("GRANTED")
  val noneString: StringLiteral = literalString("none")
  val literalEmpty: Either[String, Parameter] = literal("")
  val literalUser: Either[String, Parameter] = literal("user")
  val literalUser1: Either[String, Parameter] = literal("user1")
  val literalFoo: Either[String, Parameter] = literal("foo")
  val literalFColonOo: Either[String, Parameter] = literal("f:oo")
  val literalBar: Either[String, Parameter] = literal("bar")
  val literalRole: Either[String, Parameter] = literal("role")
  val literalRColonOle: Either[String, Parameter] = literal("r:ole")
  val literalRole1: Either[String, Parameter] = literal("role1")
  val literalRole2: Either[String, Parameter] = literal("role2")
  val paramUser: Either[String, Parameter] = param("user")
  val paramFoo: Either[String, Parameter] = param("foo")
  val paramRole: Either[String, Parameter] = param("role")
  val paramRole1: Either[String, Parameter] = param("role1")
  val paramRole2: Either[String, Parameter] = param("role2")
  val accessVar: Variable = varFor(accessString)
  val labelQualifierA: InputPosition => LabelQualifier = ast.LabelQualifier("A")(_)
  val labelQualifierB: InputPosition => LabelQualifier = ast.LabelQualifier("B")(_)
  val relQualifierA: InputPosition => RelationshipQualifier = ast.RelationshipQualifier("A")(_)
  val relQualifierB: InputPosition => RelationshipQualifier = ast.RelationshipQualifier("B")(_)
  val elemQualifierA: InputPosition => ElementQualifier = ast.ElementQualifier("A")(_)
  val elemQualifierB: InputPosition => ElementQualifier = ast.ElementQualifier("B")(_)
  val graphScopeFoo: InputPosition => NamedGraphScope = ast.NamedGraphScope(literalFoo)(_)
  val graphScopeParamFoo: InputPosition => NamedGraphScope = ast.NamedGraphScope(paramFoo)(_)
  val graphScopeBaz: InputPosition => NamedGraphScope = ast.NamedGraphScope(literal("baz"))(_)

  implicit val parser: Rule1[ast.Statement] = Statement

  def literal(name: String): Either[String, Parameter] = Left(name)

  def param(name: String): Either[String, Parameter] = Right(expressions.Parameter(name, CTString)(_))

  def toUtf8Bytes(pw: String): Array[Byte] = pw.getBytes(StandardCharsets.UTF_8)

  def pw(password: String): InputPosition => SensitiveStringLiteral = expressions.SensitiveStringLiteral(toUtf8Bytes(password))(_)

  def pwParam(name: String): Parameter = expressions.Parameter(name, CTString)(_)

  type resourcePrivilegeFunc = (PrivilegeType, ActionResource, List[GraphScope], List[GraphPrivilegeQualifier], Seq[Either[String, Parameter]]) => InputPosition => ast.Statement
  type noResourcePrivilegeFunc = (PrivilegeType, List[GraphScope], List[GraphPrivilegeQualifier], Seq[Either[String, Parameter]]) => InputPosition => ast.Statement
  type databasePrivilegeFunc = (DatabaseAction, List[DatabaseScope], Seq[Either[String, Parameter]]) => InputPosition => ast.Statement
  type transactionPrivilegeFunc = (DatabaseAction, List[DatabaseScope], List[DatabasePrivilegeQualifier], Seq[Either[String, Parameter]]) => InputPosition => ast.Statement
  type dbmsPrivilegeFunc = (AdminAction, Seq[Either[String, Parameter]]) => InputPosition => ast.Statement
  type executeProcedurePrivilegeFunc = (AdminAction, List[ProcedurePrivilegeQualifier], Seq[Either[String, Parameter]]) => InputPosition => ast.Statement
  type executeFunctionPrivilegeFunc = (AdminAction, List[FunctionPrivilegeQualifier], Seq[Either[String, Parameter]]) => InputPosition => ast.Statement

  def grantGraphPrivilege(p: PrivilegeType, a: ActionResource, s: List[GraphScope], q: List[PrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.GrantPrivilege(p, Some(a), s, q, r)

  def grantGraphPrivilege(p: PrivilegeType, s: List[GraphScope], q: List[PrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.GrantPrivilege(p, None, s, q, r)

  def grantDatabasePrivilege(d: DatabaseAction, s: List[DatabaseScope], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.GrantPrivilege.databaseAction(d, s, r)

  def grantTransactionPrivilege(d: DatabaseAction, s: List[DatabaseScope], q: List[DatabasePrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.GrantPrivilege.databaseAction(d, s, r, q)

  def grantDbmsPrivilege(a: AdminAction, r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.GrantPrivilege.dbmsAction(a, r)

  def grantExecuteProcedurePrivilege(a: AdminAction, q: List[ProcedurePrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.GrantPrivilege.dbmsAction(a, r, q)

  def grantExecuteFunctionPrivilege(a: AdminAction, q: List[FunctionPrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.GrantPrivilege.dbmsAction(a, r, q)

  def denyGraphPrivilege(p: PrivilegeType, a: ActionResource, s: List[GraphScope], q: List[PrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.DenyPrivilege(p, Some(a), s, q, r)

  def denyGraphPrivilege(p: PrivilegeType, s: List[GraphScope], q: List[PrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.DenyPrivilege(p, None, s, q, r)

  def denyDatabasePrivilege(d: DatabaseAction, s: List[DatabaseScope], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.DenyPrivilege.databaseAction(d, s, r)

  def denyTransactionPrivilege(d: DatabaseAction, s: List[DatabaseScope], q: List[DatabasePrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.DenyPrivilege.databaseAction(d, s, r, q)

  def denyDbmsPrivilege(a: AdminAction, r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.DenyPrivilege.dbmsAction(a, r)

  def denyExecuteProcedurePrivilege(a: AdminAction, q: List[ProcedurePrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.DenyPrivilege.dbmsAction(a, r, q)

  def denyExecuteFunctionPrivilege(a: AdminAction, q: List[FunctionPrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.DenyPrivilege.dbmsAction(a, r, q)

  def revokeGrantGraphPrivilege(p: PrivilegeType, a: ActionResource, s: List[GraphScope], q: List[PrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege(p, Some(a), s, q, r, RevokeGrantType()(pos))

  def revokeGrantGraphPrivilege(p: PrivilegeType, s: List[GraphScope], q: List[PrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege(p, None, s, q, r, RevokeGrantType()(pos))

  def revokeGrantDatabasePrivilege(d: DatabaseAction, s: List[DatabaseScope], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.databaseAction(d, s, r, RevokeGrantType()(pos))

  def revokeGrantTransactionPrivilege(d: DatabaseAction, s: List[DatabaseScope], q: List[DatabasePrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.databaseAction(d, s, r, RevokeGrantType()(pos), q)

  def revokeGrantDbmsPrivilege(a: AdminAction, r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, r, RevokeGrantType()(pos))

  def revokeGrantExecuteProcedurePrivilege(a: AdminAction, q: List[ProcedurePrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, r, RevokeGrantType()(pos), q)

  def revokeGrantExecuteFunctionPrivilege(a: AdminAction, q: List[FunctionPrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, r, RevokeGrantType()(pos), q)

  def revokeDenyGraphPrivilege(p: PrivilegeType, a: ActionResource, s: List[GraphScope], q: List[PrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege(p, Some(a), s, q, r, RevokeDenyType()(pos))

  def revokeDenyGraphPrivilege(p: PrivilegeType, s: List[GraphScope], q: List[PrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege(p, None, s, q, r, RevokeDenyType()(pos))

  def revokeDenyDatabasePrivilege(d: DatabaseAction, s: List[DatabaseScope], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.databaseAction(d, s, r, RevokeDenyType()(pos))

  def revokeDenyTransactionPrivilege(d: DatabaseAction, s: List[DatabaseScope], q: List[DatabasePrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.databaseAction(d, s, r, RevokeDenyType()(pos), q)

  def revokeDenyDbmsPrivilege(a: AdminAction, r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, r, RevokeDenyType()(pos))

  def revokeDenyExecuteProcedurePrivilege(a: AdminAction, q: List[ProcedurePrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, r, RevokeDenyType()(pos), q)

  def revokeDenyExecuteFunctionPrivilege(a: AdminAction, q: List[FunctionPrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, r, RevokeDenyType()(pos), q)

  def revokeGraphPrivilege(p: PrivilegeType, a: ActionResource, s: List[GraphScope], q: List[PrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege(p, Some(a), s, q, r, RevokeBothType()(pos))

  def revokeGraphPrivilege(p: PrivilegeType, s: List[GraphScope], q: List[PrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege(p, None, s, q, r, RevokeBothType()(pos))

  def revokeDatabasePrivilege(d: DatabaseAction, s: List[DatabaseScope], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.databaseAction(d, s, r, RevokeBothType()(pos))

  def revokeTransactionPrivilege(d: DatabaseAction, s: List[DatabaseScope], q: List[DatabasePrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.databaseAction(d, s, r, RevokeBothType()(pos), q)

  def revokeDbmsPrivilege(a: AdminAction, r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, r, RevokeBothType()(pos))

  def revokeExecuteProcedurePrivilege(a: AdminAction, q: List[ProcedurePrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, r, RevokeBothType()(pos), q)

  def revokeExecuteFunctionPrivilege(a: AdminAction, q: List[FunctionPrivilegeQualifier], r: Seq[Either[String, Parameter]]): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, r, RevokeBothType()(pos), q)

  def variableReturnItem(text: String): ast.UnaliasedReturnItem = returnItem(varFor(text), text)

  def returnItem(expr: Expression, text: String): ast.UnaliasedReturnItem = ast.UnaliasedReturnItem(expr, text)(pos)

  def aliasedReturnItem(variable: Variable): ast.AliasedReturnItem = ast.AliasedReturnItem(variable)

  def returnAllItems: ast.ReturnItems = ast.ReturnItems(includeExisting = true, Seq.empty)(pos)

  def returnItems(items: ast.ReturnItem*): ast.ReturnItems = ast.ReturnItems(includeExisting = false, items)(pos)

  def where(expr: Expression): ast.Where = ast.Where(expr)(pos)

  def skip(value: Long): ast.Skip = ast.Skip(literalInt(value))(pos)

  def limit(value: Long): ast.Limit = ast.Limit(literalInt(value))(pos)

  def yieldClause(returnItems: ast.ReturnItems,
                  orderBy: Option[ast.OrderBy] = None,
                  skip: Option[ast.Skip] = None,
                  limit: Option[ast.Limit] = None,
                  where: Option[ast.Where] = None): ast.Yield =
    ast.Yield(returnItems, orderBy, skip, limit, where)(pos)

  // Can't use the `return_` methods in `AstConstructionTestSupport`
  // since that results in `Cannot resolve overloaded method 'return_'` for unknown reasons
  def returnClause(returnItems: ast.ReturnItems,
                   orderBy: Option[ast.OrderBy] = None,
                   limit: Option[ast.Limit] = None,
                   distinct: Boolean = false): ast.Return =
    ast.Return(distinct, returnItems, orderBy, None, limit)(pos)
}
