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

package org.neo4j.cypher.internal.parser.v5.ast.factory

import org.neo4j.cypher.internal.ast.AdministrationCommand.NATIVE_AUTH
import org.neo4j.cypher.internal.ast.AllDatabasesScope
import org.neo4j.cypher.internal.ast.AllGraphsScope
import org.neo4j.cypher.internal.ast.AlterDatabase
import org.neo4j.cypher.internal.ast.AlterLocalDatabaseAlias
import org.neo4j.cypher.internal.ast.AlterRemoteDatabaseAlias
import org.neo4j.cypher.internal.ast.AlterServer
import org.neo4j.cypher.internal.ast.AlterUser
import org.neo4j.cypher.internal.ast.Auth
import org.neo4j.cypher.internal.ast.AuthAttribute
import org.neo4j.cypher.internal.ast.AuthId
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.DeallocateServers
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.DefaultGraphScope
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
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.IndefiniteWait
import org.neo4j.cypher.internal.ast.NamedDatabasesScope
import org.neo4j.cypher.internal.ast.NamedGraphsScope
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.ast.ParameterName
import org.neo4j.cypher.internal.ast.Password
import org.neo4j.cypher.internal.ast.PasswordChange
import org.neo4j.cypher.internal.ast.ReadOnlyAccess
import org.neo4j.cypher.internal.ast.ReadWriteAccess
import org.neo4j.cypher.internal.ast.ReallocateDatabases
import org.neo4j.cypher.internal.ast.RemoveAuth
import org.neo4j.cypher.internal.ast.RemoveHomeDatabaseAction
import org.neo4j.cypher.internal.ast.RenameRole
import org.neo4j.cypher.internal.ast.RenameServer
import org.neo4j.cypher.internal.ast.RenameUser
import org.neo4j.cypher.internal.ast.SetHomeDatabaseAction
import org.neo4j.cypher.internal.ast.SetOwnPassword
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.StartDatabase
import org.neo4j.cypher.internal.ast.StatementWithGraph
import org.neo4j.cypher.internal.ast.StopDatabase
import org.neo4j.cypher.internal.ast.TimeoutAfter
import org.neo4j.cypher.internal.ast.Topology
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.UserOptions
import org.neo4j.cypher.internal.ast.WaitUntilComplete
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.ListLiteral
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
import org.neo4j.cypher.internal.parser.ast.util.Util.astOpt
import org.neo4j.cypher.internal.parser.ast.util.Util.astOptFromList
import org.neo4j.cypher.internal.parser.ast.util.Util.astPairs
import org.neo4j.cypher.internal.parser.ast.util.Util.astSeq
import org.neo4j.cypher.internal.parser.ast.util.Util.ctxChild
import org.neo4j.cypher.internal.parser.ast.util.Util.lastChild
import org.neo4j.cypher.internal.parser.ast.util.Util.nodeChild
import org.neo4j.cypher.internal.parser.ast.util.Util.pos
import org.neo4j.cypher.internal.parser.ast.util.Util.rangePos
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser
import org.neo4j.cypher.internal.parser.v5.Cypher5ParserListener
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.exceptions.SyntaxException

import java.nio.charset.StandardCharsets

import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters.ListHasAsScala

trait DdlBuilder extends Cypher5ParserListener {

  override def exitCommandOptions(ctx: Cypher5Parser.CommandOptionsContext): Unit = {
    val map = ctx.mapOrParameter().ast[Either[Map[String, Expression], Parameter]]()
    ctx.ast = map match {
      case Left(m)  => OptionsMap(m)
      case Right(p) => OptionsParam(p)
    }
  }

  final override def exitMapOrParameter(
    ctx: Cypher5Parser.MapOrParameterContext
  ): Unit = {
    val map = ctx.map()
    ctx.ast = if (map != null) {
      Left[Map[String, Expression], Parameter](map.ast[MapExpression]().items.map(x => (x._1.name, x._2)).toMap)
    } else {
      Right[Map[String, Expression], Parameter](ctx.parameter().ast())
    }
  }

  final override def exitCommand(
    ctx: Cypher5Parser.CommandContext
  ): Unit = {
    val useCtx = ctx.useClause()
    ctx.ast = lastChild[AstRuleCtx](ctx) match {
      case c: Cypher5Parser.ShowCommandContext => c.ast match {
          case sQ: SingleQuery if useCtx != null => SingleQuery(useCtx.ast[UseGraph]() +: sQ.clauses)(pos(ctx))
          case command: StatementWithGraph if useCtx != null => command.withGraph(Some(useCtx.ast()))
          case a                                             => a
        }
      case c: Cypher5Parser.TerminateCommandContext =>
        if (useCtx != null) SingleQuery(useCtx.ast[UseGraph]() +: c.ast[Seq[Clause]]())(pos(ctx))
        else SingleQuery(c.ast[Seq[Clause]]())(pos(ctx))
      case c => c.ast[StatementWithGraph].withGraph(astOpt[UseGraph](useCtx))
    }
  }

  final override def exitDropCommand(
    ctx: Cypher5Parser.DropCommandContext
  ): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  final override def exitAlterCommand(
    ctx: Cypher5Parser.AlterCommandContext
  ): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  final override def exitTerminateCommand(
    ctx: Cypher5Parser.TerminateCommandContext
  ): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  final override def exitRenameCommand(
    ctx: Cypher5Parser.RenameCommandContext
  ): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  final override def exitEnableServerCommand(
    ctx: Cypher5Parser.EnableServerCommandContext
  ): Unit = {
    ctx.ast = EnableServer(ctx.stringOrParameter().ast(), astOpt[Options](ctx.commandOptions(), NoOptions))(pos(ctx))
  }

  final override def exitAllocationCommand(
    ctx: Cypher5Parser.AllocationCommandContext
  ): Unit = {
    val dryRun = ctx.DRYRUN() != null
    ctx.ast = if (ctx.reallocateDatabases() != null) {
      ReallocateDatabases(dryRun)(pos(ctx.reallocateDatabases()))
    } else {
      DeallocateServers(
        dryRun,
        ctx.deallocateDatabaseFromServers().ast()
      )(pos(ctx.deallocateDatabaseFromServers()))
    }
  }

  final override def exitDropDatabase(
    ctx: Cypher5Parser.DropDatabaseContext
  ): Unit = {
    val additionalAction = if (ctx.DUMP() != null) DumpData else DestroyData
    ctx.ast = DropDatabase(
      ctx.symbolicAliasNameOrParameter().ast[DatabaseName](),
      ctx.EXISTS() != null,
      ctx.COMPOSITE() != null,
      additionalAction,
      astOpt[WaitUntilComplete](ctx.waitClause(), NoWait)
    )(pos(ctx.getParent))
  }

  final override def exitAlterDatabase(
    ctx: Cypher5Parser.AlterDatabaseContext
  ): Unit = {
    val dbName = ctx.symbolicAliasNameOrParameter().ast[DatabaseName]()
    val waitUntilComplete = astOpt[WaitUntilComplete](ctx.waitClause(), NoWait)
    ctx.ast = if (!ctx.REMOVE().isEmpty) {
      val optionsToRemove = Set.from(astSeq[String](ctx.symbolicNameString()))
      AlterDatabase(dbName, ctx.EXISTS() != null, None, None, NoOptions, optionsToRemove, waitUntilComplete)(
        pos(ctx.getParent)
      )
    } else {
      val access = astOptFromList(ctx.alterDatabaseAccess(), None)
      val topology = astOptFromList(ctx.alterDatabaseTopology(), None)
      val options =
        if (ctx.alterDatabaseOption().isEmpty) NoOptions
        else OptionsMap(astSeq[Map[String, Expression]](ctx.alterDatabaseOption()).reduce(_ ++ _))
      AlterDatabase(dbName, ctx.EXISTS() != null, access, topology, options, Set.empty, waitUntilComplete)(
        pos(ctx.getParent)
      )
    }
  }

  final override def exitAlterDatabaseAccess(ctx: Cypher5Parser.AlterDatabaseAccessContext): Unit = {
    ctx.ast = if (ctx.ONLY() != null) {
      ReadOnlyAccess
    } else {
      ReadWriteAccess
    }
  }

  final override def exitAlterDatabaseTopology(ctx: Cypher5Parser.AlterDatabaseTopologyContext): Unit = {
    ctx.ast =
      if (ctx.TOPOLOGY() != null) {
        val pT = astOptFromList[Int](ctx.primaryTopology(), None)
        val sT = astOptFromList[Int](ctx.secondaryTopology(), None)
        Topology(pT, sT)
      } else None
  }

  final override def exitAlterDatabaseOption(ctx: Cypher5Parser.AlterDatabaseOptionContext): Unit = {
    ctx.ast = Map((ctx.symbolicNameString().ast[String], ctx.expression().ast[Expression]))
  }

  final override def exitStartDatabase(
    ctx: Cypher5Parser.StartDatabaseContext
  ): Unit = {
    ctx.ast = StartDatabase(
      ctx.symbolicAliasNameOrParameter().ast(),
      astOpt[WaitUntilComplete](ctx.waitClause(), NoWait)
    )(pos(ctx))
  }

  final override def exitStopDatabase(
    ctx: Cypher5Parser.StopDatabaseContext
  ): Unit = {
    ctx.ast = StopDatabase(
      ctx.symbolicAliasNameOrParameter().ast(),
      astOpt[WaitUntilComplete](ctx.waitClause(), NoWait)
    )(pos(ctx))
  }

  final override def exitWaitClause(
    ctx: Cypher5Parser.WaitClauseContext
  ): Unit = {
    ctx.ast = nodeChild(ctx, 0).getSymbol.getType match {
      case Cypher5Parser.NOWAIT => NoWait
      case Cypher5Parser.WAIT => ctx.UNSIGNED_DECIMAL_INTEGER() match {
          case null    => IndefiniteWait
          case seconds => TimeoutAfter(seconds.getText.toLong)
        }
    }
  }

  final override def exitDatabaseScope(
    ctx: Cypher5Parser.DatabaseScopeContext
  ): Unit = {
    ctx.ast = if (ctx.DEFAULT() != null) {
      DefaultDatabaseScope()(pos(ctx))
    } else if (ctx.HOME() != null) {
      HomeDatabaseScope()(pos(ctx))
    } else if (ctx.TIMES() != null) {
      AllDatabasesScope()(pos(ctx))
    } else {
      NamedDatabasesScope(ctx.symbolicAliasNameList().ast())(pos(ctx))
    }
  }

  final override def exitGraphScope(
    ctx: Cypher5Parser.GraphScopeContext
  ): Unit = {
    ctx.ast = if (ctx.DEFAULT() != null) {
      DefaultGraphScope()(pos(ctx))
    } else if (ctx.HOME() != null) {
      HomeGraphScope()(pos(ctx))
    } else if (ctx.TIMES() != null) {
      AllGraphsScope()(pos(ctx))
    } else {
      NamedGraphsScope(ctx.symbolicAliasNameList().ast())(pos(ctx))
    }
  }

  final override def exitDropAlias(
    ctx: Cypher5Parser.DropAliasContext
  ): Unit = {
    ctx.ast = DropDatabaseAlias(ctx.symbolicAliasNameOrParameter().ast[DatabaseName](), ctx.EXISTS() != null)(pos(
      ctx.getParent
    ))
  }

  final override def exitAlterAlias(
    ctx: Cypher5Parser.AlterAliasContext
  ): Unit = {
    val aliasName = ctx.symbolicAliasNameOrParameter().ast[DatabaseName]()
    val aliasTargetCtx = ctx.alterAliasTarget()
    val (targetName, url) = {
      if (aliasTargetCtx.isEmpty) (None, None)
      else
        (
          Some(aliasTargetCtx.get(0).symbolicAliasNameOrParameter().ast[DatabaseName]()),
          astOpt[Either[String, Parameter]](aliasTargetCtx.get(0).stringOrParameter())
        )
    }
    val username = astOptFromList[Expression](ctx.alterAliasUser(), None)
    val password = astOptFromList[Expression](ctx.alterAliasPassword(), None)
    val driverSettings = astOptFromList[Either[Map[String, Expression], Parameter]](ctx.alterAliasDriver(), None)
    val properties = astOptFromList[Either[Map[String, Expression], Parameter]](ctx.alterAliasProperties(), None)
    ctx.ast = if (url.isEmpty && username.isEmpty && password.isEmpty && driverSettings.isEmpty) {
      AlterLocalDatabaseAlias(aliasName, targetName, ctx.EXISTS() != null, properties)(pos(ctx.getParent))
    } else {
      AlterRemoteDatabaseAlias(
        aliasName,
        targetName,
        ctx.EXISTS() != null,
        url,
        username,
        password,
        driverSettings,
        properties
      )(pos(ctx.getParent))
    }
  }

  override def exitAlterAliasTarget(ctx: Cypher5Parser.AlterAliasTargetContext): Unit = {
    val target = ctx.symbolicAliasNameOrParameter().ast[DatabaseName]()
    val url = astOpt[Either[String, Parameter]](ctx.stringOrParameter())
    ctx.ast = (target, url)
  }

  override def exitAlterAliasUser(ctx: Cypher5Parser.AlterAliasUserContext): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  override def exitAlterAliasPassword(ctx: Cypher5Parser.AlterAliasPasswordContext): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  override def exitAlterAliasDriver(ctx: Cypher5Parser.AlterAliasDriverContext): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  override def exitAlterAliasProperties(ctx: Cypher5Parser.AlterAliasPropertiesContext): Unit = {
    ctx.ast = ctxChild(ctx, 1).ast
  }

  final override def exitSymbolicAliasNameList(
    ctx: Cypher5Parser.SymbolicAliasNameListContext
  ): Unit = {
    ctx.ast = astSeq[DatabaseName](ctx.symbolicAliasNameOrParameter())
  }

  final override def exitSymbolicAliasNameOrParameter(
    ctx: Cypher5Parser.SymbolicAliasNameOrParameterContext
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
    ctx: Cypher5Parser.CommandNodePatternContext
  ): Unit = {
    ctx.ast = (
      ctx.variable().ast[Variable](),
      ctx.labelType.ast[LabelName]()
    )
  }

  final override def exitCommandRelPattern(
    ctx: Cypher5Parser.CommandRelPatternContext
  ): Unit = {
    ctx.ast = (
      ctx.variable().ast[Variable](),
      ctx.relType().ast[RelTypeName]()
    )
  }

  final override def exitDropConstraint(
    ctx: Cypher5Parser.DropConstraintContext
  ): Unit = {
    val p = pos(ctx.getParent)
    val constraintName = ctx.symbolicNameOrStringParameter()
    ctx.ast = if (constraintName != null) {
      DropConstraintOnName(constraintName.ast(), ctx.EXISTS() != null)(p)
    } else {
      val properties = ctx.propertyList().ast[ArraySeq[Property]]()
      val nodePattern = ctx.commandNodePattern()
      val isNode = nodePattern != null
      if (isNode) {
        val (variable, label) = nodePattern.ast[(Variable, LabelName)]()
        if (ctx.EXISTS() != null) {
          DropNodePropertyExistenceConstraint(variable, label, properties(0))(p)
        } else if (ctx.UNIQUE() != null) {
          DropPropertyUniquenessConstraint(variable, label, properties)(p)
        } else if (ctx.KEY() != null) {
          DropNodeKeyConstraint(variable, label, properties)(p)
        } else {
          throw new SyntaxException("Unsupported drop constraint command: Please delete the constraint by name instead")
        }
      } else {
        if (ctx.EXISTS() != null) {
          val (variable, relType) = ctx.commandRelPattern().ast[(Variable, RelTypeName)]()
          DropRelationshipPropertyExistenceConstraint(variable, relType, properties(0))(p)
        } else {
          throw new SyntaxException("Unsupported drop constraint command: Please delete the constraint by name instead")
        }
      }
    }
  }

  final override def exitDropIndex(
    ctx: Cypher5Parser.DropIndexContext
  ): Unit = {
    val indexName = ctx.symbolicNameOrStringParameter()
    ctx.ast = if (indexName != null) {
      DropIndexOnName(
        indexName.ast[Either[String, Parameter]](),
        ctx.EXISTS() != null
      )(pos(ctx.getParent))
    } else {
      DropIndex(
        ctx.labelType().ast[LabelName],
        ctx.nonEmptyNameList().ast[ArraySeq[PropertyKeyName]].toList
      )(pos(ctx.getParent))
    }
  }

  final override def exitPropertyList(
    ctx: Cypher5Parser.PropertyListContext
  ): Unit = {
    val enclosed = ctx.enclosedPropertyList()
    ctx.ast =
      if (enclosed != null) enclosed.ast[Seq[Property]]()
      else ArraySeq(Property(ctx.variable().ast[Expression], ctx.property().ast[PropertyKeyName])(pos(ctx)))
  }

  final override def exitEnclosedPropertyList(
    ctx: Cypher5Parser.EnclosedPropertyListContext
  ): Unit = {
    ctx.ast = astPairs[Expression, PropertyKeyName](ctx.variable(), ctx.property())
      .map { case (e, p) => Property(e, p)(e.position) }
  }

  final override def exitAlterServer(
    ctx: Cypher5Parser.AlterServerContext
  ): Unit = {
    ctx.ast = AlterServer(
      ctx.stringOrParameter().ast[Either[String, Parameter]](),
      ctx.commandOptions().ast()
    )(pos(ctx.getParent))
  }

  final override def exitRenameServer(
    ctx: Cypher5Parser.RenameServerContext
  ): Unit = {
    val names = ctx.stringOrParameter()
    ctx.ast = RenameServer(
      names.get(0).ast[Either[String, Parameter]](),
      names.get(1).ast[Either[String, Parameter]]()
    )(pos(ctx.getParent))
  }

  final override def exitDropServer(
    ctx: Cypher5Parser.DropServerContext
  ): Unit = {
    ctx.ast = DropServer(ctx.stringOrParameter().ast[Either[String, Parameter]])(pos(ctx.getParent))
  }

  final override def exitDeallocateDatabaseFromServers(
    ctx: Cypher5Parser.DeallocateDatabaseFromServersContext
  ): Unit = {
    ctx.ast = astSeq[Either[String, Parameter]](ctx.stringOrParameter())
  }

  final override def exitReallocateDatabases(
    ctx: Cypher5Parser.ReallocateDatabasesContext
  ): Unit = {}

  final override def exitDropRole(
    ctx: Cypher5Parser.DropRoleContext
  ): Unit = {
    ctx.ast = DropRole(ctx.commandNameExpression().ast(), ctx.EXISTS() != null)(pos(ctx.getParent))
  }

  final override def exitRenameRole(
    ctx: Cypher5Parser.RenameRoleContext
  ): Unit = {
    val names = ctx.commandNameExpression()
    ctx.ast = RenameRole(names.get(0).ast(), names.get(1).ast(), ctx.EXISTS() != null)(pos(ctx.getParent))
  }

  final override def exitDropUser(
    ctx: Cypher5Parser.DropUserContext
  ): Unit = {
    ctx.ast = DropUser(ctx.commandNameExpression().ast(), ctx.EXISTS() != null)(pos(ctx.getParent))
  }

  final override def exitRenameUser(
    ctx: Cypher5Parser.RenameUserContext
  ): Unit = {
    val names = ctx.commandNameExpression()
    ctx.ast = RenameUser(names.get(0).ast(), names.get(1).ast(), ctx.EXISTS() != null)(pos(ctx.getParent))
  }

  final override def exitAlterCurrentUser(
    ctx: Cypher5Parser.AlterCurrentUserContext
  ): Unit = {
    ctx.ast = SetOwnPassword(
      ctx.passwordExpression(1).ast[Expression](),
      ctx.passwordExpression(0).ast[Expression]()
    )(pos(ctx.getParent))
  }

  final override def exitAlterUser(
    ctx: Cypher5Parser.AlterUserContext
  ): Unit = {
    val username = ctx.commandNameExpression().ast[Expression]()
    val nativePassAttributes = ctx.password().asScala.toList
      .map(_.ast[(Password, Option[PasswordChange])]())
      .foldLeft(List.empty[AuthAttribute]) { case (acc, (password, change)) => (acc :+ password) ++ change }
    val nativeAuthAttr = ctx.passwordChangeRequired().asScala.toList
      .map(c => PasswordChange(c.ast[Boolean]())(pos(c)))
      .foldLeft(nativePassAttributes) { case (acc, changeReq) => acc :+ changeReq }
      .sortBy(_.position)
    val suspended = astOptFromList[Boolean](ctx.userStatus(), None)
    val removeHome = if (!ctx.HOME().isEmpty) Some(RemoveHomeDatabaseAction) else None
    val homeDatabaseAction = astOptFromList[HomeDatabaseAction](ctx.homeDatabase(), removeHome)
    val userOptions = UserOptions(suspended, homeDatabaseAction)
    val nativeAuth =
      if (nativeAuthAttr.nonEmpty) Some(Auth(NATIVE_AUTH, nativeAuthAttr)(nativeAuthAttr.head.position)) else None
    val removeAuth = RemoveAuth(!ctx.ALL().isEmpty, ctx.removeNamedProvider().asScala.toList.map(_.ast[Expression]()))
    val setAuth = ctx.setAuthClause().asScala.toList.map(_.ast[Auth]())
    ctx.ast =
      AlterUser(username, userOptions, ctx.EXISTS() != null, setAuth, nativeAuth, removeAuth)(pos(ctx.getParent))
  }

  override def exitRemoveNamedProvider(ctx: Cypher5Parser.RemoveNamedProviderContext): Unit = {
    ctx.ast = if (ctx.stringLiteral() != null) ctx.stringLiteral().ast[StringLiteral]()
    else if (ctx.stringListLiteral() != null) ctx.stringListLiteral().ast[ListLiteral]()
    else ctx.parameter().ast[Parameter]()
  }

  override def exitSetAuthClause(ctx: Cypher5Parser.SetAuthClauseContext): Unit = {
    val provider = ctx.stringLiteral().ast[StringLiteral]()
    val attributes = astSeq[AuthAttribute](ctx.userAuthAttribute()).toList
    ctx.ast = Auth(provider.value, attributes)(pos(ctx))
  }

  override def exitUserAuthAttribute(ctx: Cypher5Parser.UserAuthAttributeContext): Unit = {
    ctx.ast = if (ctx.ID() != null) {
      AuthId(ctx.stringOrParameterExpression().ast())(pos(ctx.ID()))
    } else if (ctx.passwordOnly() != null) {
      ctx.passwordOnly().ast()
    } else {
      PasswordChange(ctx.passwordChangeRequired().ast[Boolean]())(pos(ctx.passwordChangeRequired()))
    }
  }

  override def exitPasswordOnly(ctx: Cypher5Parser.PasswordOnlyContext): Unit = {
    ctx.ast = Password(ctx.passwordExpression().ast[Expression](), ctx.ENCRYPTED() != null)(pos(ctx))
  }

  override def exitPassword(ctx: Cypher5Parser.PasswordContext): Unit = {
    val password = Password(ctx.passwordExpression().ast[Expression](), ctx.ENCRYPTED() != null)(pos(ctx))
    val passwordReq =
      if (ctx.passwordChangeRequired() != null)
        Some(PasswordChange(ctx.passwordChangeRequired().ast[Boolean]())(pos(ctx.passwordChangeRequired())))
      else None
    ctx.ast = (password, passwordReq)
  }

  final override def exitPasswordExpression(
    ctx: Cypher5Parser.PasswordExpressionContext
  ): Unit = {
    val str = ctx.stringLiteral()
    ctx.ast = if (str != null) {
      val pass = str.ast[StringLiteral]()
      SensitiveStringLiteral(pass.value.getBytes(StandardCharsets.UTF_8))(pass.position)
    } else {
      val pass = ctx.parameter().ast[Parameter]()
      new ExplicitParameter(pass.name, CTString)(pass.position) with SensitiveParameter
    }
  }

  final override def exitPasswordChangeRequired(
    ctx: Cypher5Parser.PasswordChangeRequiredContext
  ): Unit = {
    ctx.ast = ctx.NOT() == null
  }

  final override def exitUserStatus(
    ctx: Cypher5Parser.UserStatusContext
  ): Unit = {
    ctx.ast = ctx.SUSPENDED() != null
  }

  final override def exitHomeDatabase(
    ctx: Cypher5Parser.HomeDatabaseContext
  ): Unit = {
    val dbName = ctx.symbolicAliasNameOrParameter().ast[DatabaseName]()
    ctx.ast = SetHomeDatabaseAction(dbName)
  }

  final override def exitSymbolicNameOrStringParameterList(
    ctx: Cypher5Parser.SymbolicNameOrStringParameterListContext
  ): Unit = {
    ctx.ast = astSeq[Expression](ctx.commandNameExpression())
  }

  final override def exitCommandNameExpression(
    ctx: Cypher5Parser.CommandNameExpressionContext
  ): Unit = {
    val name = ctx.symbolicNameString()
    ctx.ast = if (name != null) {
      StringLiteral(ctx.symbolicNameString().ast[String]())(rangePos(name))
    } else {
      ctx.parameter().ast[Parameter]()
    }
  }

  final override def exitSymbolicNameOrStringParameter(
    ctx: Cypher5Parser.SymbolicNameOrStringParameterContext
  ): Unit = {
    ctx.ast = if (ctx.symbolicNameString() != null) {
      Left(ctx.symbolicNameString().ast[String]())
    } else {
      Right(ctx.parameter().ast[Parameter]())
    }
  }

  final override def exitStringOrParameterExpression(
    ctx: Cypher5Parser.StringOrParameterExpressionContext
  ): Unit = {
    ctx.ast = if (ctx.stringLiteral() != null) {
      ctx.stringLiteral().ast[StringLiteral]()
    } else {
      ctx.parameter().ast[Parameter]()
    }
  }

  final override def exitStringOrParameter(
    ctx: Cypher5Parser.StringOrParameterContext
  ): Unit = {
    ctx.ast = if (ctx.stringLiteral() != null) {
      Left(ctx.stringLiteral().ast[StringLiteral]().value)
    } else {
      Right(ctx.parameter().ast[Parameter]())
    }
  }

}
