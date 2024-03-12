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
package org.neo4j.cypher.internal.cst.factory.neo4j.ast

import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.IndefiniteWait
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.ast.ParameterName
import org.neo4j.cypher.internal.ast.SetHomeDatabaseAction
import org.neo4j.cypher.internal.ast.StatementWithGraph
import org.neo4j.cypher.internal.ast.TimeoutAfter
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astOpt
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astPairs
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.lastChild
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.nodeChild
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.pos
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.SensitiveParameter
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.parser.CypherParser
import org.neo4j.cypher.internal.parser.CypherParserListener
import org.neo4j.cypher.internal.util.symbols.CTString

import java.nio.charset.StandardCharsets

import scala.collection.immutable.ArraySeq

trait DdlBuilder extends CypherParserListener {

  override def exitCommandOptions(ctx: CypherParser.CommandOptionsContext): Unit = {
    val map = ctx.mapOrParameter().ast[Either[Map[String, Expression], Parameter]]()
    ctx.ast = map match {
      case Left(m)  => OptionsMap(m)
      case Right(p) => OptionsParam(p)
    }
  }

  final override def exitMapOrParameter(
    ctx: CypherParser.MapOrParameterContext
  ): Unit = {
    val map = ctx.map()
    ctx.ast = if (map != null) {
      Left[Map[String, Expression], Parameter](map.ast[MapExpression]().items.map(x => (x._1.name, x._2)).toMap)
    } else {
      Right[Map[String, Expression], Parameter](ctx.parameter().ast())
    }
  }

  final override def exitCommand(
    ctx: CypherParser.CommandContext
  ): Unit = {
    ctx.ast = lastChild[AstRuleCtx](ctx) match {
      case c: CypherParser.CreateCommandContext =>
        c.ast[StatementWithGraph].withGraph(astOpt[UseGraph](ctx.useClause()))
      case _ => null
    }

  }

  final override def exitDropCommand(
    ctx: CypherParser.DropCommandContext
  ): Unit = {}

  final override def exitAlterCommand(
    ctx: CypherParser.AlterCommandContext
  ): Unit = {}

  final override def exitShowCommand(
    ctx: CypherParser.ShowCommandContext
  ): Unit = {}

  final override def exitTerminateCommand(
    ctx: CypherParser.TerminateCommandContext
  ): Unit = {}

  final override def exitShowAllCommand(
    ctx: CypherParser.ShowAllCommandContext
  ): Unit = {}

  final override def exitShowNodeCommand(
    ctx: CypherParser.ShowNodeCommandContext
  ): Unit = {}

  final override def exitShowRelationshipCommand(
    ctx: CypherParser.ShowRelationshipCommandContext
  ): Unit = {}

  final override def exitShowRelCommand(
    ctx: CypherParser.ShowRelCommandContext
  ): Unit = {}

  final override def exitShowPropertyCommand(
    ctx: CypherParser.ShowPropertyCommandContext
  ): Unit = {}

  final override def exitComposableCommandClauses(
    ctx: CypherParser.ComposableCommandClausesContext
  ): Unit = {}

  final override def exitRenameCommand(
    ctx: CypherParser.RenameCommandContext
  ): Unit = {}

  final override def exitGrantCommand(
    ctx: CypherParser.GrantCommandContext
  ): Unit = {}

  final override def exitRevokeCommand(
    ctx: CypherParser.RevokeCommandContext
  ): Unit = {}

  final override def exitEnableServerCommand(
    ctx: CypherParser.EnableServerCommandContext
  ): Unit = {}

  final override def exitAllocationCommand(
    ctx: CypherParser.AllocationCommandContext
  ): Unit = {}

  final override def exitCreateDatabase(
    ctx: CypherParser.CreateDatabaseContext
  ): Unit = {}

  final override def exitCreateCompositeDatabase(
    ctx: CypherParser.CreateCompositeDatabaseContext
  ): Unit = {}

  final override def exitDropDatabase(
    ctx: CypherParser.DropDatabaseContext
  ): Unit = {}

  final override def exitAlterDatabase(
    ctx: CypherParser.AlterDatabaseContext
  ): Unit = {}

  final override def exitStartDatabase(
    ctx: CypherParser.StartDatabaseContext
  ): Unit = {}

  final override def exitStopDatabase(
    ctx: CypherParser.StopDatabaseContext
  ): Unit = {}

  final override def exitWaitClause(
    ctx: CypherParser.WaitClauseContext
  ): Unit = {
    ctx.ast = nodeChild(ctx, 0).getSymbol.getType match {
      case CypherParser.NOWAIT => NoWait
      case CypherParser.WAIT => ctx.UNSIGNED_DECIMAL_INTEGER() match {
          case null    => IndefiniteWait
          case seconds => TimeoutAfter(seconds.getText.toLong)
        }
    }
  }

  final override def exitShowDatabase(
    ctx: CypherParser.ShowDatabaseContext
  ): Unit = {}

  final override def exitDatabaseScope(
    ctx: CypherParser.DatabaseScopeContext
  ): Unit = {}

  final override def exitGraphScope(
    ctx: CypherParser.GraphScopeContext
  ): Unit = {}

  final override def exitCreateAlias(
    ctx: CypherParser.CreateAliasContext
  ): Unit = {}

  final override def exitDropAlias(
    ctx: CypherParser.DropAliasContext
  ): Unit = {}

  final override def exitAlterAlias(
    ctx: CypherParser.AlterAliasContext
  ): Unit = {}

  final override def exitShowAliases(
    ctx: CypherParser.ShowAliasesContext
  ): Unit = {}

  final override def exitSymbolicAliasNameList(
    ctx: CypherParser.SymbolicAliasNameListContext
  ): Unit = {}

  final override def exitSymbolicAliasNameOrParameter(
    ctx: CypherParser.SymbolicAliasNameOrParameterContext
  ): Unit = {
    val symbAliasName = ctx.symbolicAliasName()
    ctx.ast =
      if (symbAliasName != null) {
        val s = symbAliasName.ast[ArraySeq[String]]().toList
        NamespacedName(s)(pos(ctx))
      } else
        ParameterName(ctx.parameter().ast())(pos(ctx))
  }

  final override def exitCommandNodePattern(
    ctx: CypherParser.CommandNodePatternContext
  ): Unit = {}

  final override def exitCommandRelPattern(
    ctx: CypherParser.CommandRelPatternContext
  ): Unit = {}

  final override def exitDropConstraint(
    ctx: CypherParser.DropConstraintContext
  ): Unit = {}

  final override def exitDropConstraintNodeCheck(
    ctx: CypherParser.DropConstraintNodeCheckContext
  ): Unit = {}

  final override def exitDropIndex(
    ctx: CypherParser.DropIndexContext
  ): Unit = {}

  final override def exitPropertyList(
    ctx: CypherParser.PropertyListContext
  ): Unit = {
    ctx.ast = astPairs[Expression, PropertyKeyName](ctx.variable(), ctx.property()).map {
      case (e, p) => Property(e, p)(pos(ctx))
    }
  }

  final override def exitAlterServer(
    ctx: CypherParser.AlterServerContext
  ): Unit = {}

  final override def exitRenameServer(
    ctx: CypherParser.RenameServerContext
  ): Unit = {}

  final override def exitDropServer(
    ctx: CypherParser.DropServerContext
  ): Unit = {}

  final override def exitShowServers(
    ctx: CypherParser.ShowServersContext
  ): Unit = {}

  final override def exitDeallocateDatabaseFromServers(
    ctx: CypherParser.DeallocateDatabaseFromServersContext
  ): Unit = {}

  final override def exitReallocateDatabases(
    ctx: CypherParser.ReallocateDatabasesContext
  ): Unit = {}

  final override def exitCreateRole(
    ctx: CypherParser.CreateRoleContext
  ): Unit = {}

  final override def exitDropRole(
    ctx: CypherParser.DropRoleContext
  ): Unit = {}

  final override def exitRenameRole(
    ctx: CypherParser.RenameRoleContext
  ): Unit = {}

  final override def exitShowRoles(
    ctx: CypherParser.ShowRolesContext
  ): Unit = {}

  final override def exitGrantRole(
    ctx: CypherParser.GrantRoleContext
  ): Unit = {}

  final override def exitRevokeRole(
    ctx: CypherParser.RevokeRoleContext
  ): Unit = {}

  final override def exitCreateUser(
    ctx: CypherParser.CreateUserContext
  ): Unit = {}

  final override def exitDropUser(
    ctx: CypherParser.DropUserContext
  ): Unit = {}

  final override def exitRenameUser(
    ctx: CypherParser.RenameUserContext
  ): Unit = {}

  final override def exitAlterCurrentUser(
    ctx: CypherParser.AlterCurrentUserContext
  ): Unit = {}

  final override def exitAlterUser(
    ctx: CypherParser.AlterUserContext
  ): Unit = {}

  final override def exitSetPassword(
    ctx: CypherParser.SetPasswordContext
  ): Unit = {}

  final override def exitPasswordExpression(
    ctx: CypherParser.PasswordExpressionContext
  ): Unit = {
    val str = ctx.stringLiteral()
    ctx.ast = if (str != null) {
      val pass = str.ast[StringLiteral]()
      SensitiveStringLiteral(pass.value.getBytes(StandardCharsets.UTF_8))(pass.position, pass.endPosition)
    } else {
      val pass = ctx.parameter().ast[Parameter]()
      new ExplicitParameter(pass.name, CTString)(pass.position) with SensitiveParameter
    }
  }

  final override def exitPasswordChangeRequired(
    ctx: CypherParser.PasswordChangeRequiredContext
  ): Unit = {
    ctx.ast = ctx.NOT() == null
  }

  final override def exitUserStatus(
    ctx: CypherParser.UserStatusContext
  ): Unit = {
    ctx.ast = ctx.SUSPENDED() != null
  }

  final override def exitHomeDatabase(
    ctx: CypherParser.HomeDatabaseContext
  ): Unit = {
    val dbName = ctx.symbolicAliasNameOrParameter().ast[DatabaseName]()
    ctx.ast = SetHomeDatabaseAction(dbName)
  }

  final override def exitShowUsers(
    ctx: CypherParser.ShowUsersContext
  ): Unit = {}

  final override def exitShowCurrentUser(
    ctx: CypherParser.ShowCurrentUserContext
  ): Unit = {}

  final override def exitShowSupportedPrivileges(
    ctx: CypherParser.ShowSupportedPrivilegesContext
  ): Unit = {}

  final override def exitShowPrivileges(
    ctx: CypherParser.ShowPrivilegesContext
  ): Unit = {}

  final override def exitShowRolePrivileges(
    ctx: CypherParser.ShowRolePrivilegesContext
  ): Unit = {}

  final override def exitShowUserPrivileges(
    ctx: CypherParser.ShowUserPrivilegesContext
  ): Unit = {}

  final override def exitGrantRoleManagement(
    ctx: CypherParser.GrantRoleManagementContext
  ): Unit = {}

  final override def exitRevokeRoleManagement(
    ctx: CypherParser.RevokeRoleManagementContext
  ): Unit = {}

  final override def exitRoleManagementPrivilege(
    ctx: CypherParser.RoleManagementPrivilegeContext
  ): Unit = {}

  final override def exitGrantPrivilege(
    ctx: CypherParser.GrantPrivilegeContext
  ): Unit = {}

  final override def exitDenyPrivilege(
    ctx: CypherParser.DenyPrivilegeContext
  ): Unit = {}

  final override def exitRevokePrivilege(
    ctx: CypherParser.RevokePrivilegeContext
  ): Unit = {}

  final override def exitPrivilege(
    ctx: CypherParser.PrivilegeContext
  ): Unit = {}

  final override def exitAllPrivilege(
    ctx: CypherParser.AllPrivilegeContext
  ): Unit = {}

  final override def exitAllPrivilegeType(
    ctx: CypherParser.AllPrivilegeTypeContext
  ): Unit = {}

  final override def exitAllPrivilegeTarget(
    ctx: CypherParser.AllPrivilegeTargetContext
  ): Unit = {}

  final override def exitCreatePrivilege(
    ctx: CypherParser.CreatePrivilegeContext
  ): Unit = {}

  final override def exitDropPrivilege(
    ctx: CypherParser.DropPrivilegeContext
  ): Unit = {}

  final override def exitLoadPrivilege(
    ctx: CypherParser.LoadPrivilegeContext
  ): Unit = {}

  final override def exitShowPrivilege(
    ctx: CypherParser.ShowPrivilegeContext
  ): Unit = {}

  final override def exitSetPrivilege(
    ctx: CypherParser.SetPrivilegeContext
  ): Unit = {}

  final override def exitRemovePrivilege(
    ctx: CypherParser.RemovePrivilegeContext
  ): Unit = {}

  final override def exitWritePrivilege(
    ctx: CypherParser.WritePrivilegeContext
  ): Unit = {}

  final override def exitDatabasePrivilege(
    ctx: CypherParser.DatabasePrivilegeContext
  ): Unit = {}

  final override def exitDbmsPrivilege(
    ctx: CypherParser.DbmsPrivilegeContext
  ): Unit = {}

  final override def exitExecuteFunctionQualifier(
    ctx: CypherParser.ExecuteFunctionQualifierContext
  ): Unit = {}

  final override def exitExecuteProcedureQualifier(
    ctx: CypherParser.ExecuteProcedureQualifierContext
  ): Unit = {}

  final override def exitSettingQualifier(
    ctx: CypherParser.SettingQualifierContext
  ): Unit = {}

  final override def exitQualifiedGraphPrivilegesWithProperty(
    ctx: CypherParser.QualifiedGraphPrivilegesWithPropertyContext
  ): Unit = {}

  final override def exitQualifiedGraphPrivileges(
    ctx: CypherParser.QualifiedGraphPrivilegesContext
  ): Unit = {}

  final override def exitGlobs(
    ctx: CypherParser.GlobsContext
  ): Unit = {}

  final override def exitGlob(
    ctx: CypherParser.GlobContext
  ): Unit = {}

  final override def exitGlobRecursive(
    ctx: CypherParser.GlobRecursiveContext
  ): Unit = {}

  final override def exitGlobPart(
    ctx: CypherParser.GlobPartContext
  ): Unit = {}

  final override def exitLabelResource(
    ctx: CypherParser.LabelResourceContext
  ): Unit = {}

  final override def exitPropertyResource(
    ctx: CypherParser.PropertyResourceContext
  ): Unit = {}

  final override def exitGraphQualifier(
    ctx: CypherParser.GraphQualifierContext
  ): Unit = {}

  final override def exitSymbolicNameOrStringParameterList(
    ctx: CypherParser.SymbolicNameOrStringParameterListContext
  ): Unit = {}

  final override def exitCommandNameExpression(
    ctx: CypherParser.CommandNameExpressionContext
  ): Unit = {
    ctx.ast = if (ctx.symbolicNameString() != null) {
      StringLiteral(ctx.symbolicNameString().ast[String]())(
        pos(ctx.symbolicNameString.start),
        pos(ctx.symbolicNameString.stop)
      )
    } else {
      ctx.parameter().ast[Parameter]()
    }
  }

  final override def exitSymbolicNameOrStringParameter(
    ctx: CypherParser.SymbolicNameOrStringParameterContext
  ): Unit = {
    ctx.ast = if (ctx.symbolicNameString() != null) {
      Left(ctx.symbolicNameString().ast[String]())
    } else {
      Right(ctx.parameter().ast[Parameter]())
    }
  }

  final override def exitStringList(
    ctx: CypherParser.StringListContext
  ): Unit = {}

  final override def exitStringOrParameter(
    ctx: CypherParser.StringOrParameterContext
  ): Unit = {
    ctx.ast = if (ctx.stringLiteral() != null) {
      Left(ctx.stringLiteral().ast[StringLiteral]().value)
    } else {
      Right(ctx.parameter().ast[Parameter]())
    }
  }

}
