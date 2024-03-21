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

import org.neo4j.cypher.internal.ast.AlterDatabase
import org.neo4j.cypher.internal.ast.AlterLocalDatabaseAlias
import org.neo4j.cypher.internal.ast.AlterRemoteDatabaseAlias
import org.neo4j.cypher.internal.ast.AlterServer
import org.neo4j.cypher.internal.ast.AlterUser
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.DeallocateServers
import org.neo4j.cypher.internal.ast.DestroyData
import org.neo4j.cypher.internal.ast.DropConstraintOnName
import org.neo4j.cypher.internal.ast.DropDatabase
import org.neo4j.cypher.internal.ast.DropDatabaseAlias
import org.neo4j.cypher.internal.ast.DropIndex
import org.neo4j.cypher.internal.ast.DropIndexOnName
import org.neo4j.cypher.internal.ast.DropNodeKeyConstraint
import org.neo4j.cypher.internal.ast.DropNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.DropPropertyUniquenessConstraint
import org.neo4j.cypher.internal.ast.DropRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.DropRole
import org.neo4j.cypher.internal.ast.DropServer
import org.neo4j.cypher.internal.ast.DropUser
import org.neo4j.cypher.internal.ast.DumpData
import org.neo4j.cypher.internal.ast.EnableServer
import org.neo4j.cypher.internal.ast.HomeDatabaseAction
import org.neo4j.cypher.internal.ast.IndefiniteWait
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.ast.ParameterName
import org.neo4j.cypher.internal.ast.ReadOnlyAccess
import org.neo4j.cypher.internal.ast.ReadWriteAccess
import org.neo4j.cypher.internal.ast.ReallocateDatabases
import org.neo4j.cypher.internal.ast.RemoveHomeDatabaseAction
import org.neo4j.cypher.internal.ast.RenameRole
import org.neo4j.cypher.internal.ast.RenameServer
import org.neo4j.cypher.internal.ast.RenameUser
import org.neo4j.cypher.internal.ast.SchemaCommand
import org.neo4j.cypher.internal.ast.SetHomeDatabaseAction
import org.neo4j.cypher.internal.ast.SetOwnPassword
import org.neo4j.cypher.internal.ast.StartDatabase
import org.neo4j.cypher.internal.ast.StatementWithGraph
import org.neo4j.cypher.internal.ast.StopDatabase
import org.neo4j.cypher.internal.ast.TimeoutAfter
import org.neo4j.cypher.internal.ast.Topology
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.UserOptions
import org.neo4j.cypher.internal.ast.WaitUntilComplete
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astOpt
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astOptFromList
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astPairs
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.astSeq
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.ctxChild
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.lastChild
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.nodeChild
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.Util.pos
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SensitiveParameter
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.parser.AstRuleCtx
import org.neo4j.cypher.internal.parser.CypherParser
import org.neo4j.cypher.internal.parser.CypherParserListener
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.exceptions.SyntaxException

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
      case c: CypherParser.DropCommandContext =>
        c.ast[StatementWithGraph].withGraph(astOpt[UseGraph](ctx.useClause()))
      case c: CypherParser.AlterCommandContext =>
        c.ast[StatementWithGraph].withGraph(astOpt[UseGraph](ctx.useClause()))
      case c: CypherParser.StartDatabaseContext =>
        c.ast[StatementWithGraph].withGraph(astOpt[UseGraph](ctx.useClause()))
      case c: CypherParser.StopDatabaseContext =>
        c.ast[StatementWithGraph].withGraph(astOpt[UseGraph](ctx.useClause()))
      case c: CypherParser.RenameCommandContext =>
        c.ast[StatementWithGraph].withGraph(astOpt[UseGraph](ctx.useClause()))
      case c: CypherParser.EnableServerCommandContext =>
        c.ast[StatementWithGraph].withGraph(astOpt[UseGraph](ctx.useClause()))
      case c: CypherParser.AllocationCommandContext =>
        c.ast[StatementWithGraph].withGraph(astOpt[UseGraph](ctx.useClause()))
      case _ => null
    }
  }

  final override def exitDropCommand(
    ctx: CypherParser.DropCommandContext
  ): Unit = {
    val cPos = pos(ctx)
    ctx.ast = ctxChild(ctx, 1) match {
      case c: CypherParser.DropAliasContext =>
        DropDatabaseAlias(c.symbolicAliasNameOrParameter().ast[DatabaseName](), c.EXISTS() != null)(cPos)
      case c: CypherParser.DropConstraintContext => dropConstraintBuilder(c, cPos)
      case c: CypherParser.DropDatabaseContext =>
        val databaseName = c.symbolicAliasNameOrParameter().ast[DatabaseName]()
        val additionalAction = if (c.DUMP() != null) DumpData else DestroyData
        val waitUntilComplete = astOpt[WaitUntilComplete](c.waitClause(), NoWait)
        DropDatabase(databaseName, c.EXISTS() != null, c.COMPOSITE() != null, additionalAction, waitUntilComplete)(cPos)
      case c: CypherParser.DropIndexContext =>
        val indexName = c.symbolicNameOrStringParameter()
        if (indexName != null) {
          DropIndexOnName(indexName.ast[Either[String, Parameter]](), c.EXISTS() != null)(cPos)
        } else {
          val labelName = c.labelType().ast[LabelName]
          val properties = c.nonEmptyNameList().ast[ArraySeq[PropertyKeyName]].toList
          DropIndex(labelName, properties)(cPos)
        }
      case c: CypherParser.DropRoleContext =>
        DropRole(c.commandNameExpression().ast(), c.EXISTS() != null)(cPos)
      case c: CypherParser.DropServerContext =>
        DropServer(c.stringOrParameter().ast[Either[String, Parameter]])(cPos)
      case c: CypherParser.DropUserContext =>
        DropUser(c.commandNameExpression().ast(), c.EXISTS() != null)(cPos)
      case _ => throw new IllegalStateException()
    }
  }

  private def dropConstraintBuilder(ctx: CypherParser.DropConstraintContext, pos: InputPosition): SchemaCommand = {
    val constraintName = ctx.symbolicNameOrStringParameter()
    if (constraintName != null) {
      DropConstraintOnName(constraintName.ast(), ctx.EXISTS() != null)(pos)
    } else {
      val properties = ctx.propertyList().ast[ArraySeq[Property]]()
      val nodePattern = ctx.commandNodePattern()
      val isNode = nodePattern != null
      if (isNode) {
        val variable = nodePattern.variable().ast[Variable]()
        val label = nodePattern.labelType.ast[LabelName]()
        if (ctx.EXISTS() != null) {
          DropNodePropertyExistenceConstraint(variable, label, properties(0))(pos)
        } else if (ctx.UNIQUE() != null) {
          DropPropertyUniquenessConstraint(variable, label, properties)(pos)
        } else if (ctx.KEY() != null) {
          DropNodeKeyConstraint(variable, label, properties)(pos)
        } else {
          throw new SyntaxException("Unsupported drop constraint command: Please delete the constraint by name instead")
        }
      } else {
        if (ctx.EXISTS() != null) {
          val relPattern = ctx.commandRelPattern()
          val variable = relPattern.variable().ast[Variable]()
          val relType = relPattern.relType().ast[RelTypeName]()
          DropRelationshipPropertyExistenceConstraint(variable, relType, properties(0))(pos)
        } else {
          throw new SyntaxException("Unsupported drop constraint command: Please delete the constraint by name instead")
        }
      }
    }
  }

  final override def exitAlterCommand(
    ctx: CypherParser.AlterCommandContext
  ): Unit = {
    val p = pos(ctx)
    ctx.ast = ctxChild(ctx, 1) match {
      case c: CypherParser.AlterAliasContext =>
        val aliasName = c.symbolicAliasNameOrParameter().ast[DatabaseName]()
        val aliasTargetCtx = c.alterAliasTarget()
        val (targetName, url) = {
          if (aliasTargetCtx.isEmpty) (None, None)
          else
            (
              Some(aliasTargetCtx.get(0).symbolicAliasNameOrParameter().ast[DatabaseName]()),
              astOpt[Either[String, Parameter]](aliasTargetCtx.get(0).stringOrParameter())
            )
        }
        val username = astOptFromList[Expression](c.alterAliasUser(), None)
        val password = astOptFromList[Expression](c.alterAliasPassword(), None)
        val driverSettings = astOptFromList[Either[Map[String, Expression], Parameter]](c.alterAliasDriver(), None)
        val properties = astOptFromList[Either[Map[String, Expression], Parameter]](c.alterAliasProperties(), None)
        if (url.isEmpty && username.isEmpty && password.isEmpty && driverSettings.isEmpty) {
          AlterLocalDatabaseAlias(aliasName, targetName, c.EXISTS() != null, properties)(p)
        } else {
          AlterRemoteDatabaseAlias(
            aliasName,
            targetName,
            c.EXISTS() != null,
            url,
            username,
            password,
            driverSettings,
            properties
          )(p)
        }
      case c: CypherParser.AlterCurrentUserContext =>
        SetOwnPassword(c.passwordExpression(1).ast[Expression](), c.passwordExpression(0).ast[Expression]())(p)
      case c: CypherParser.AlterDatabaseContext =>
        val dbName = c.symbolicAliasNameOrParameter().ast[DatabaseName]()
        val waitUntilComplete = astOpt[WaitUntilComplete](c.waitClause(), NoWait)
        if (!c.REMOVE().isEmpty) {
          val optionsToRemove = Set.from(astSeq[String](c.symbolicNameString()))
          AlterDatabase(dbName, c.EXISTS() != null, None, None, NoOptions, optionsToRemove, waitUntilComplete)(p)
        } else {
          val access = astOptFromList(c.alterDatabaseAccess(), None)
          val topology = astOptFromList(c.alterDatabaseTopology(), None)
          val options = if (c.alterDatabaseOption().isEmpty) NoOptions
          else OptionsMap(astSeq[Map[String, Expression]](c.alterDatabaseOption()).reduce(_ ++ _))
          AlterDatabase(dbName, c.EXISTS() != null, access, topology, options, Set.empty, waitUntilComplete)(p)
        }
      case c: CypherParser.AlterServerContext =>
        AlterServer(c.stringOrParameter().ast[Either[String, Parameter]](), c.commandOptions().ast())(p)
      case c: CypherParser.AlterUserContext =>
        val username = c.commandNameExpression().ast[Expression]()
        val passCtx = c.password()
        val (isEncrypted, initPass) = if (!passCtx.isEmpty)
          (Some(passCtx.get(0).ENCRYPTED() != null), Some(passCtx.get(0).passwordExpression().ast[Expression]()))
        else (None, None)
        val passwordReq =
          if (passCtx.isEmpty && c.passwordChangeRequired().isEmpty) None
          else if (!c.passwordChangeRequired().isEmpty) Some(c.passwordChangeRequired().get(0).ast[Boolean]())
          else if (!passCtx.isEmpty) {
            if (passCtx.get(0).passwordChangeRequired() != null)
              Some(passCtx.get(0).passwordChangeRequired().ast[Boolean]())
            else None
          } else None
        val suspended = astOptFromList[Boolean](c.userStatus(), None)
        val homeDatabaseAction = if (c.REMOVE() != null) Some(RemoveHomeDatabaseAction)
        else astOptFromList[HomeDatabaseAction](c.homeDatabase(), None)
        val userOptions = UserOptions(passwordReq, suspended, homeDatabaseAction)
        AlterUser(username, isEncrypted, initPass, userOptions, c.EXISTS() != null)(p)
      case _ => throw new IllegalStateException()
    }
  }

  override def exitPassword(ctx: CypherParser.PasswordContext): Unit = {}

  override def exitAlterAliasTarget(ctx: CypherParser.AlterAliasTargetContext): Unit = {
    val target = ctx.symbolicAliasNameOrParameter().ast[DatabaseName]()
    val url = astOpt[Either[String, Parameter]](ctx.stringOrParameter())
    ctx.ast = (target, url)
  }

  override def exitAlterAliasUser(ctx: CypherParser.AlterAliasUserContext): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  override def exitAlterAliasPassword(ctx: CypherParser.AlterAliasPasswordContext): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  override def exitAlterAliasDriver(ctx: CypherParser.AlterAliasDriverContext): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  override def exitAlterAliasProperties(ctx: CypherParser.AlterAliasPropertiesContext): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  final override def exitAlterDatabaseAccess(ctx: CypherParser.AlterDatabaseAccessContext): Unit = {
    ctx.ast = if (ctx.ONLY() != null) {
      ReadOnlyAccess
    } else {
      ReadWriteAccess
    }
  }

  final override def exitAlterDatabaseTopology(ctx: CypherParser.AlterDatabaseTopologyContext): Unit = {
    ctx.ast =
      if (ctx.TOPOLOGY() != null) {
        val pT = astOptFromList[Int](ctx.primaryTopology(), None)
        val sT = astOptFromList[Int](ctx.secondaryTopology(), None)
        Topology(pT, sT)
      } else None
  }

  final override def exitAlterDatabaseOption(ctx: CypherParser.AlterDatabaseOptionContext): Unit = {
    ctx.ast = Map((ctx.symbolicNameString().ast[String], ctx.expression().ast[Expression]))
  }

  final override def exitShowCommand(
    ctx: CypherParser.ShowCommandContext
  ): Unit = {}

  final override def exitTerminateCommand(
    ctx: CypherParser.TerminateCommandContext
  ): Unit = {}

  override def exitTransactionClauses(
    ctx: CypherParser.TransactionClausesContext
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
  ): Unit = {
    ctx.ast = ctxChild(ctx, 1) match {
      case c: CypherParser.RenameRoleContext =>
        val names = c.commandNameExpression()
        RenameRole(names.get(0).ast(), names.get(1).ast(), c.EXISTS() != null)(pos(ctx))
      case c: CypherParser.RenameServerContext =>
        val names = c.stringOrParameter()
        RenameServer(
          names.get(0).ast[Either[String, Parameter]](),
          names.get(1).ast[Either[String, Parameter]]()
        )(pos(ctx))
      case c: CypherParser.RenameUserContext =>
        val names = c.commandNameExpression()
        RenameUser(names.get(0).ast(), names.get(1).ast(), c.EXISTS() != null)(pos(ctx))
      case _ => throw new IllegalStateException()
    }
  }

  final override def exitGrantCommand(
    ctx: CypherParser.GrantCommandContext
  ): Unit = {}

  final override def exitRevokeCommand(
    ctx: CypherParser.RevokeCommandContext
  ): Unit = {}

  final override def exitEnableServerCommand(
    ctx: CypherParser.EnableServerCommandContext
  ): Unit = {
    ctx.ast = EnableServer(ctx.stringOrParameter().ast(), astOpt[Options](ctx.commandOptions(), NoOptions))(pos(ctx))
  }

  final override def exitAllocationCommand(
    ctx: CypherParser.AllocationCommandContext
  ): Unit = {
    val dryRun = ctx.DRYRUN() != null
    ctx.ast = if (ctx.reallocateDatabases() != null) {
      ReallocateDatabases(dryRun)(pos(ctx.reallocateDatabases()))
    } else {
      DeallocateServers(
        dryRun,
        astSeq[Either[String, Parameter]](ctx.deallocateDatabaseFromServers().stringOrParameter())
      )(pos(ctx.deallocateDatabaseFromServers()))
    }
  }

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
  ): Unit = {
    ctx.ast = StartDatabase(
      ctx.symbolicAliasNameOrParameter().ast(),
      astOpt[WaitUntilComplete](ctx.waitClause(), NoWait)
    )(pos(ctx))
  }

  final override def exitStopDatabase(
    ctx: CypherParser.StopDatabaseContext
  ): Unit = {
    ctx.ast = StopDatabase(
      ctx.symbolicAliasNameOrParameter().ast(),
      astOpt[WaitUntilComplete](ctx.waitClause(), NoWait)
    )(pos(ctx))
  }

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
