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

package org.neo4j.cypher.internal.parser.v5.ast.factory.ast

import org.antlr.v4.runtime.tree.ParseTree
import org.antlr.v4.runtime.tree.RuleNode
import org.antlr.v4.runtime.tree.TerminalNode
import org.neo4j.cypher.internal.ast.AccessDatabaseAction
import org.neo4j.cypher.internal.ast.ActionResource
import org.neo4j.cypher.internal.ast.AllAliasManagementActions
import org.neo4j.cypher.internal.ast.AllConstraintActions
import org.neo4j.cypher.internal.ast.AllDatabaseAction
import org.neo4j.cypher.internal.ast.AllDatabaseManagementActions
import org.neo4j.cypher.internal.ast.AllDatabasesQualifier
import org.neo4j.cypher.internal.ast.AllDatabasesScope
import org.neo4j.cypher.internal.ast.AllDbmsAction
import org.neo4j.cypher.internal.ast.AllGraphAction
import org.neo4j.cypher.internal.ast.AllGraphsScope
import org.neo4j.cypher.internal.ast.AllIndexActions
import org.neo4j.cypher.internal.ast.AllLabelResource
import org.neo4j.cypher.internal.ast.AllPrivilegeActions
import org.neo4j.cypher.internal.ast.AllPropertyResource
import org.neo4j.cypher.internal.ast.AllQualifier
import org.neo4j.cypher.internal.ast.AllRoleActions
import org.neo4j.cypher.internal.ast.AllTokenActions
import org.neo4j.cypher.internal.ast.AllTransactionActions
import org.neo4j.cypher.internal.ast.AllUserActions
import org.neo4j.cypher.internal.ast.AlterAliasAction
import org.neo4j.cypher.internal.ast.AlterDatabaseAction
import org.neo4j.cypher.internal.ast.AlterUserAction
import org.neo4j.cypher.internal.ast.AssignPrivilegeAction
import org.neo4j.cypher.internal.ast.AssignRoleAction
import org.neo4j.cypher.internal.ast.CompositeDatabaseManagementActions
import org.neo4j.cypher.internal.ast.CreateAliasAction
import org.neo4j.cypher.internal.ast.CreateCompositeDatabaseAction
import org.neo4j.cypher.internal.ast.CreateConstraintAction
import org.neo4j.cypher.internal.ast.CreateDatabaseAction
import org.neo4j.cypher.internal.ast.CreateElementAction
import org.neo4j.cypher.internal.ast.CreateIndexAction
import org.neo4j.cypher.internal.ast.CreateNodeLabelAction
import org.neo4j.cypher.internal.ast.CreatePropertyKeyAction
import org.neo4j.cypher.internal.ast.CreateRelationshipTypeAction
import org.neo4j.cypher.internal.ast.CreateRoleAction
import org.neo4j.cypher.internal.ast.CreateUserAction
import org.neo4j.cypher.internal.ast.DatabaseAction
import org.neo4j.cypher.internal.ast.DatabasePrivilege
import org.neo4j.cypher.internal.ast.DatabasePrivilegeQualifier
import org.neo4j.cypher.internal.ast.DatabaseScope
import org.neo4j.cypher.internal.ast.DbmsAction
import org.neo4j.cypher.internal.ast.DbmsPrivilege
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.DefaultGraphScope
import org.neo4j.cypher.internal.ast.DeleteElementAction
import org.neo4j.cypher.internal.ast.DenyPrivilege
import org.neo4j.cypher.internal.ast.DropAliasAction
import org.neo4j.cypher.internal.ast.DropCompositeDatabaseAction
import org.neo4j.cypher.internal.ast.DropConstraintAction
import org.neo4j.cypher.internal.ast.DropDatabaseAction
import org.neo4j.cypher.internal.ast.DropIndexAction
import org.neo4j.cypher.internal.ast.DropRoleAction
import org.neo4j.cypher.internal.ast.DropUserAction
import org.neo4j.cypher.internal.ast.ElementQualifier
import org.neo4j.cypher.internal.ast.ElementsAllQualifier
import org.neo4j.cypher.internal.ast.ExecuteAdminProcedureAction
import org.neo4j.cypher.internal.ast.ExecuteBoostedFunctionAction
import org.neo4j.cypher.internal.ast.ExecuteBoostedProcedureAction
import org.neo4j.cypher.internal.ast.ExecuteFunctionAction
import org.neo4j.cypher.internal.ast.ExecuteProcedureAction
import org.neo4j.cypher.internal.ast.FileResource
import org.neo4j.cypher.internal.ast.FunctionQualifier
import org.neo4j.cypher.internal.ast.GrantPrivilege
import org.neo4j.cypher.internal.ast.GrantRolesToUsers
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.GraphPrivilegeQualifier
import org.neo4j.cypher.internal.ast.GraphScope
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.ImpersonateUserAction
import org.neo4j.cypher.internal.ast.LabelAllQualifier
import org.neo4j.cypher.internal.ast.LabelQualifier
import org.neo4j.cypher.internal.ast.LabelResource
import org.neo4j.cypher.internal.ast.LabelsResource
import org.neo4j.cypher.internal.ast.LoadAllDataAction
import org.neo4j.cypher.internal.ast.LoadAllQualifier
import org.neo4j.cypher.internal.ast.LoadCidrAction
import org.neo4j.cypher.internal.ast.LoadCidrQualifier
import org.neo4j.cypher.internal.ast.LoadPrivilege
import org.neo4j.cypher.internal.ast.LoadUrlAction
import org.neo4j.cypher.internal.ast.LoadUrlQualifier
import org.neo4j.cypher.internal.ast.MatchAction
import org.neo4j.cypher.internal.ast.MergeAdminAction
import org.neo4j.cypher.internal.ast.NamedDatabasesScope
import org.neo4j.cypher.internal.ast.NamedGraphsScope
import org.neo4j.cypher.internal.ast.PatternQualifier
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
import org.neo4j.cypher.internal.ast.PrivilegeType
import org.neo4j.cypher.internal.ast.ProcedureQualifier
import org.neo4j.cypher.internal.ast.PropertiesResource
import org.neo4j.cypher.internal.ast.ReadAction
import org.neo4j.cypher.internal.ast.RelationshipAllQualifier
import org.neo4j.cypher.internal.ast.RelationshipQualifier
import org.neo4j.cypher.internal.ast.RemoveLabelAction
import org.neo4j.cypher.internal.ast.RemovePrivilegeAction
import org.neo4j.cypher.internal.ast.RemoveRoleAction
import org.neo4j.cypher.internal.ast.RenameRoleAction
import org.neo4j.cypher.internal.ast.RenameUserAction
import org.neo4j.cypher.internal.ast.RevokeBothType
import org.neo4j.cypher.internal.ast.RevokeDenyType
import org.neo4j.cypher.internal.ast.RevokeGrantType
import org.neo4j.cypher.internal.ast.RevokePrivilege
import org.neo4j.cypher.internal.ast.RevokeRolesFromUsers
import org.neo4j.cypher.internal.ast.ServerManagementAction
import org.neo4j.cypher.internal.ast.SetDatabaseAccessAction
import org.neo4j.cypher.internal.ast.SetLabelAction
import org.neo4j.cypher.internal.ast.SetPasswordsAction
import org.neo4j.cypher.internal.ast.SetPropertyAction
import org.neo4j.cypher.internal.ast.SetUserHomeDatabaseAction
import org.neo4j.cypher.internal.ast.SetUserStatusAction
import org.neo4j.cypher.internal.ast.SettingQualifier
import org.neo4j.cypher.internal.ast.ShowAliasAction
import org.neo4j.cypher.internal.ast.ShowConstraintAction
import org.neo4j.cypher.internal.ast.ShowIndexAction
import org.neo4j.cypher.internal.ast.ShowPrivilegeAction
import org.neo4j.cypher.internal.ast.ShowRoleAction
import org.neo4j.cypher.internal.ast.ShowServerAction
import org.neo4j.cypher.internal.ast.ShowSettingAction
import org.neo4j.cypher.internal.ast.ShowTransactionAction
import org.neo4j.cypher.internal.ast.ShowUserAction
import org.neo4j.cypher.internal.ast.StartDatabaseAction
import org.neo4j.cypher.internal.ast.StopDatabaseAction
import org.neo4j.cypher.internal.ast.TerminateTransactionAction
import org.neo4j.cypher.internal.ast.TraverseAction
import org.neo4j.cypher.internal.ast.UserAllQualifier
import org.neo4j.cypher.internal.ast.UserQualifier
import org.neo4j.cypher.internal.ast.WriteAction
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.parser.ast.util.Util.astOpt
import org.neo4j.cypher.internal.parser.ast.util.Util.astSeq
import org.neo4j.cypher.internal.parser.ast.util.Util.child
import org.neo4j.cypher.internal.parser.ast.util.Util.ctxChild
import org.neo4j.cypher.internal.parser.ast.util.Util.nodeChild
import org.neo4j.cypher.internal.parser.ast.util.Util.pos
import org.neo4j.cypher.internal.parser.v5.CypherParser
import org.neo4j.cypher.internal.parser.v5.CypherParserListener
import org.neo4j.cypher.internal.util.InputPosition

import scala.collection.immutable.ArraySeq

trait DdlPrivilegeBuilder extends CypherParserListener {

  final override def exitGrantCommand(
    ctx: CypherParser.GrantCommandContext
  ): Unit = {
    val p = pos(ctx)
    ctx.ast = if (ctx.privilege() != null) {
      val (privilegeType, resource, qualifier) =
        ctx.privilege().ast[(PrivilegeType, Option[ActionResource], List[PrivilegeQualifier])]
      val roleNames = ctx.roleNames.ast[ArraySeq[Expression]]()
      GrantPrivilege(privilegeType, ctx.IMMUTABLE() != null, resource, qualifier, roleNames)(p)
    } else {
      val (rolenames, usernames) =
        ctx.grantRole().ast[(Seq[Expression], Seq[Expression])]()
      GrantRolesToUsers(rolenames, usernames)(p)
    }
  }

  final override def exitGrantRole(
    ctx: CypherParser.GrantRoleContext
  ): Unit = {
    ctx.ast = (
      ctx.roleNames.ast[Seq[Expression]](),
      ctx.userNames.ast[Seq[Expression]]()
    )
  }

  final override def exitDenyCommand(
    ctx: CypherParser.DenyCommandContext
  ): Unit = {
    val p = pos(ctx)
    val (privilegeType, resource, qualifier) =
      ctx.privilege().ast[(PrivilegeType, Option[ActionResource], List[PrivilegeQualifier])]
    val roleNames = ctx.roleNames.ast[ArraySeq[Expression]]()
    ctx.ast = DenyPrivilege(privilegeType, ctx.IMMUTABLE() != null, resource, qualifier, roleNames)(p)
  }

  final override def exitRevokeCommand(
    ctx: CypherParser.RevokeCommandContext
  ): Unit = {
    val p = pos(ctx)
    ctx.ast = if (ctx.privilege() != null) {
      val (privilegeType, resource, qualifier) =
        ctx.privilege().ast[(PrivilegeType, Option[ActionResource], List[PrivilegeQualifier])]
      val roleNames = ctx.roleNames.ast[ArraySeq[Expression]]()
      val revokeType =
        if (ctx.DENY() != null) RevokeDenyType()(pos(ctx.DENY()))
        else if (ctx.GRANT() != null) RevokeGrantType()(pos(ctx.GRANT()))
        else RevokeBothType()(p)
      RevokePrivilege(privilegeType, ctx.IMMUTABLE() != null, resource, qualifier, roleNames, revokeType)(p)
    } else {
      val (rolenames, usernames) =
        ctx.revokeRole().ast[(Seq[Expression], Seq[Expression])]()
      RevokeRolesFromUsers(rolenames, usernames)(p)
    }
  }

  final override def exitRevokeRole(
    ctx: CypherParser.RevokeRoleContext
  ): Unit = {
    ctx.ast = (
      ctx.roleNames.ast[Seq[Either[String, Parameter]]](),
      ctx.userNames.ast[Seq[Either[String, Parameter]]]()
    )
  }

  final override def exitPrivilege(
    ctx: CypherParser.PrivilegeContext
  ): Unit = {
    ctx.ast = ctxChild(ctx, 0).ast
  }

  final override def exitAllPrivilege(
    ctx: CypherParser.AllPrivilegeContext
  ): Unit = {
    ctx.ast = ctx.allPrivilegeTarget() match {
      case c: CypherParser.DefaultTargetContext =>
        if (c.DATABASE() != null) {
          val scope = if (c.DEFAULT() != null) DefaultDatabaseScope()(pos(ctx)) else HomeDatabaseScope()(pos(ctx))
          allDbQualifier(DatabasePrivilege(AllDatabaseAction, scope)(pos(ctx)), None)
        } else {
          val scope = if (c.DEFAULT() != null) DefaultGraphScope()(pos(ctx)) else HomeGraphScope()(pos(ctx))
          allQualifier(GraphPrivilege(AllGraphAction, scope)(pos(ctx)), None)
        }
      case c: CypherParser.DatabaseVariableTargetContext =>
        val scope =
          if (c.TIMES() != null) AllDatabasesScope()(pos(ctx))
          else NamedDatabasesScope(c.symbolicAliasNameList().ast())(pos(ctx))
        allDbQualifier(DatabasePrivilege(AllDatabaseAction, scope)(pos(ctx)), None)
      case c: CypherParser.GraphVariableTargetContext =>
        val scope =
          if (c.TIMES() != null) AllGraphsScope()(pos(ctx))
          else NamedGraphsScope(c.symbolicAliasNameList().ast())(pos(ctx))
        allQualifier(GraphPrivilege(AllGraphAction, scope)(pos(ctx)), None)
      case _: CypherParser.DBMSTargetContext =>
        allQualifier(DbmsPrivilege(AllDbmsAction)(pos(ctx)), None)
      case _ => throw new IllegalStateException("Unexpected privilege all command")
    }
  }

  final override def exitAllPrivilegeTarget(
    ctx: CypherParser.AllPrivilegeTargetContext
  ): Unit = {}

  final override def exitAllPrivilegeType(
    ctx: CypherParser.AllPrivilegeTypeContext
  ): Unit = {}

  final override def exitCreatePrivilege(
    ctx: CypherParser.CreatePrivilegeContext
  ): Unit = {

    ctx.ast = if (ctx.databaseScope() != null) {
      val scope = ctx.databaseScope().ast[DatabaseScope]()
      val action = ctx.createPrivilegeForDatabase().ast[DatabaseAction]()
      allDbQualifier(DatabasePrivilege(action, scope)(pos(ctx)), None)
    } else if (ctx.actionForDBMS() != null) {
      val action = ctx.actionForDBMS().ast[DbmsAction]()
      allQualifier(DbmsPrivilege(action)(pos(ctx)), None)
    } else {
      val scope = ctx.graphScope().ast[GraphScope]()
      val qualifier = ctx.graphQualifier().ast[List[GraphPrivilegeQualifier]]()
      (GraphPrivilege(CreateElementAction, scope)(pos(ctx)), None, qualifier)
    }
  }

  final override def exitCreatePrivilegeForDatabase(
    ctx: CypherParser.CreatePrivilegeForDatabaseContext
  ): Unit = {
    ctx.ast = ctxChild(ctx, 0).ast
  }

  override def exitActionForDBMS(ctx: CypherParser.ActionForDBMSContext): Unit = {
    val isCreate = ctx.parent.getRuleIndex == CypherParser.RULE_createPrivilege
    ctx.ast = nodeChild(ctx, 0).getSymbol.getType match {
      case CypherParser.ALIAS     => if (isCreate) CreateAliasAction else DropAliasAction
      case CypherParser.COMPOSITE => if (isCreate) CreateCompositeDatabaseAction else DropCompositeDatabaseAction
      case CypherParser.DATABASE  => if (isCreate) CreateDatabaseAction else DropDatabaseAction
      case CypherParser.ROLE      => if (isCreate) CreateRoleAction else DropRoleAction
      case CypherParser.USER      => if (isCreate) CreateUserAction else DropUserAction
      case _                      => throw new IllegalStateException("Unexpected DBMS token")
    }
  }

  final override def exitDatabasePrivilege(
    ctx: CypherParser.DatabasePrivilegeContext
  ): Unit = {
    val (action, qualifier) =
      child[ParseTree](ctx, 0) match {
        case _: CypherParser.ConstraintTokenContext => withQualifier(AllConstraintActions)
        case _: CypherParser.IndexTokenContext      => withQualifier(AllIndexActions)
        case c: TerminalNode =>
          c.getSymbol.getType match {
            case CypherParser.ACCESS => withQualifier(AccessDatabaseAction)
            case CypherParser.NAME   => withQualifier(AllTokenActions)
            case CypherParser.START  => withQualifier(StartDatabaseAction)
            case CypherParser.STOP   => withQualifier(StopDatabaseAction)
            case CypherParser.TERMINATE =>
              (
                TerminateTransactionAction,
                astOpt[List[DatabasePrivilegeQualifier]](
                  ctx.userQualifier(),
                  List(UserAllQualifier()(InputPosition.NONE))
                )
              )
            case CypherParser.TRANSACTION =>
              (
                AllTransactionActions,
                astOpt[List[DatabasePrivilegeQualifier]](
                  ctx.userQualifier(),
                  List(UserAllQualifier()(InputPosition.NONE))
                )
              )
            case _ => throw new IllegalStateException()
          }
        case _ => throw new IllegalStateException("Unexpected action for Database Privilege")
      }
    val scope = ctx.databaseScope().ast[DatabaseScope]()
    ctx.ast = (
      DatabasePrivilege(action, scope)(pos(ctx)),
      None,
      qualifier
    )
  }

  final override def exitDbmsPrivilege(
    ctx: CypherParser.DbmsPrivilegeContext
  ): Unit = {
    val (action, qualifier) =
      child[ParseTree](ctx, 0) match {
        case _: CypherParser.DbmsPrivilegeExecuteContext =>
          ctx.dbmsPrivilegeExecute().ast[(DbmsAction, List[PrivilegeQualifier])]
        case c: TerminalNode => c.getSymbol.getType match {
            case CypherParser.ALIAS => withQualifier(AllAliasManagementActions)
            case CypherParser.ALTER => nodeChild(ctx, 1).getSymbol.getType match {
                case CypherParser.ALIAS    => withQualifier(AlterAliasAction)
                case CypherParser.DATABASE => withQualifier(AlterDatabaseAction)
                case CypherParser.USER     => withQualifier(AlterUserAction)
                case _                     => throw new IllegalStateException()
              }
            case CypherParser.ASSIGN => nodeChild(ctx, 1).getSymbol.getType match {
                case CypherParser.PRIVILEGE => withQualifier(AssignPrivilegeAction)
                case CypherParser.ROLE      => withQualifier(AssignRoleAction)
                case _                      => throw new IllegalStateException()
              }
            case CypherParser.COMPOSITE => withQualifier(CompositeDatabaseManagementActions)
            case CypherParser.DATABASE  => withQualifier(AllDatabaseManagementActions)
            case CypherParser.IMPERSONATE =>
              (
                ImpersonateUserAction,
                astOpt[List[DatabasePrivilegeQualifier]](
                  ctx.userQualifier(),
                  List(UserAllQualifier()(InputPosition.NONE))
                )
              )
            case CypherParser.PRIVILEGE => withQualifier(AllPrivilegeActions)
            case CypherParser.RENAME => nodeChild(ctx, 1).getSymbol.getType match {
                case CypherParser.ROLE => withQualifier(RenameRoleAction)
                case CypherParser.USER => withQualifier(RenameUserAction)
                case _                 => throw new IllegalStateException()
              }
            case CypherParser.ROLE   => withQualifier(AllRoleActions)
            case CypherParser.SERVER => withQualifier(ServerManagementAction)
            case CypherParser.USER   => withQualifier(AllUserActions)
            case _                   => throw new IllegalStateException()
          }
        case _ => throw new IllegalStateException()
      }
    ctx.ast = (DbmsPrivilege(action)(pos(ctx)), None, qualifier)
  }

  override def exitDbmsPrivilegeExecute(ctx: CypherParser.DbmsPrivilegeExecuteContext): Unit = {
    ctx.ast = if (ctx.adminToken() != null) {
      withQualifier(ExecuteAdminProcedureAction)
    } else if (ctx.procedureToken() != null) {
      val qualifier = ctx.executeProcedureQualifier().ast[List[PrivilegeQualifier]]()
      if (ctx.BOOSTED() != null) (ExecuteBoostedProcedureAction, qualifier) else (ExecuteProcedureAction, qualifier)
    } else {
      val qualifier = ctx.executeFunctionQualifier().ast[List[PrivilegeQualifier]]()
      if (ctx.BOOSTED() != null) (ExecuteBoostedFunctionAction, qualifier) else (ExecuteFunctionAction, qualifier)
    }
  }

  final override def exitDropPrivilege(
    ctx: CypherParser.DropPrivilegeContext
  ): Unit = {
    ctx.ast = if (ctx.databaseScope() != null) {
      val action = if (ctx.indexToken() != null) DropIndexAction else DropConstraintAction
      val scope = ctx.databaseScope().ast[DatabaseScope]()
      allDbQualifier(DatabasePrivilege(action, scope)(pos(ctx)), None)
    } else {
      allQualifier(DbmsPrivilege(ctx.actionForDBMS().ast())(pos(ctx)), None)
    }
  }

  final override def exitLoadPrivilege(
    ctx: CypherParser.LoadPrivilegeContext
  ): Unit = {
    ctx.ast = if (ctx.ALL() != null) {
      (
        LoadPrivilege(LoadAllDataAction)(pos(ctx)),
        Some(FileResource()(pos(ctx))),
        List(LoadAllQualifier()(pos(ctx)))
      )
    } else if (ctx.URL() != null) {
      (
        LoadPrivilege(LoadUrlAction)(pos(ctx)),
        Some(FileResource()(pos(ctx))),
        List(LoadUrlQualifier(ctx.stringOrParameter().ast())(pos(ctx)))
      )
    } else {
      (
        LoadPrivilege(LoadCidrAction)(pos(ctx)),
        Some(FileResource()(pos(ctx))),
        List(LoadCidrQualifier(ctx.stringOrParameter().ast())(pos(ctx)))
      )
    }
  }

  final override def exitQualifiedGraphPrivileges(
    ctx: CypherParser.QualifiedGraphPrivilegesContext
  ): Unit = {
    val (action, resource) = {
      if (ctx.DELETE() != null) {
        (DeleteElementAction, None)
      } else (MergeAdminAction, ctx.propertiesResource().ast[Option[PropertiesResource]]())
    }
    val scope = ctx.graphScope().ast[GraphScope]()
    val qualifier = ctx.graphQualifier().ast[List[GraphPrivilegeQualifier]]()
    ctx.ast = (
      GraphPrivilege(action, scope)(pos(ctx)),
      resource,
      qualifier
    )
  }

  final override def exitQualifiedGraphPrivilegesWithProperty(
    ctx: CypherParser.QualifiedGraphPrivilegesWithPropertyContext
  ): Unit = {
    val (action, resource) = {
      if (ctx.TRAVERSE() != null) {
        (TraverseAction, None)
      } else {
        val action = if (ctx.READ != null) ReadAction else MatchAction
        (action, ctx.propertiesResource().ast[Option[PropertiesResource]]())
      }
    }
    val scope = ctx.graphScope().ast[GraphScope]()
    val qualifier = ctx.graphQualifier().ast[List[GraphPrivilegeQualifier]]()
    ctx.ast = (
      GraphPrivilege(action, scope)(pos(ctx)),
      resource,
      qualifier
    )
  }

  final override def exitShowPrivilege(
    ctx: CypherParser.ShowPrivilegeContext
  ): Unit = {
    val p = pos(ctx)
    ctx.ast = if (ctx.databaseScope() != null) {
      val (action, qualifier): (DatabaseAction, List[DatabasePrivilegeQualifier]) = ctxChild(ctx, 1) match {
        case _: CypherParser.ConstraintTokenContext => withQualifier(ShowConstraintAction)
        case _: CypherParser.IndexTokenContext      => withQualifier(ShowIndexAction)
        case _: CypherParser.TransactionTokenContext =>
          (
            ShowTransactionAction,
            astOpt[List[DatabasePrivilegeQualifier]](ctx.userQualifier(), List(UserAllQualifier()(InputPosition.NONE)))
          )
        case _ => throw new IllegalStateException()
      }
      val scope = ctx.databaseScope().ast[DatabaseScope]()
      (DatabasePrivilege(action, scope)(p), None, qualifier)
    } else {
      val (action, qualifier): (DbmsAction, List[PrivilegeQualifier]) = ctx.getChild(1) match {
        case t: TerminalNode => t.getSymbol.getType match {
            case CypherParser.ALIAS                         => withQualifier(ShowAliasAction)
            case CypherParser.PRIVILEGE                     => withQualifier(ShowPrivilegeAction)
            case CypherParser.ROLE                          => withQualifier(ShowRoleAction)
            case CypherParser.SERVER | CypherParser.SERVERS => withQualifier(ShowServerAction)
            case CypherParser.USER                          => withQualifier(ShowUserAction)
            case _                                          => throw new IllegalStateException()
          }
        case r: RuleNode if r.getRuleContext.getRuleIndex == CypherParser.RULE_settingToken =>
          (ShowSettingAction, ctx.settingQualifier().ast[List[SettingQualifier]]())
        case _ => throw new IllegalStateException()
      }
      (DbmsPrivilege(action)(p), None, qualifier)
    }
  }

  final override def exitSetPrivilege(
    ctx: CypherParser.SetPrivilegeContext
  ): Unit = {
    val p = pos(ctx)
    ctx.ast = if (ctx.DBMS() != null) {
      val action = if (ctx.passwordToken() != null) SetPasswordsAction
      else if (ctx.STATUS() != null) SetUserStatusAction
      else if (ctx.HOME() != null) SetUserHomeDatabaseAction
      else SetDatabaseAccessAction
      allQualifier(DbmsPrivilege(action)(p), None)
    } else {
      val scope = ctx.graphScope().ast[GraphScope]()
      if (ctx.LABEL() != null) {
        val resource = ctx.labelsResource().ast[Option[LabelsResource]]()
        labelAllQualifier(GraphPrivilege(SetLabelAction, scope)(p), resource, p)
      } else {
        val resource = ctx.propertiesResource().ast[Option[PropertiesResource]]()
        (
          GraphPrivilege(SetPropertyAction, scope)(p),
          resource,
          ctx.graphQualifier().ast[List[GraphPrivilegeQualifier]]()
        )
      }
    }
  }

  final override def exitRemovePrivilege(
    ctx: CypherParser.RemovePrivilegeContext
  ): Unit = {
    val p = pos(ctx)
    ctx.ast = if (ctx.DBMS() != null) {
      val action = if (ctx.PRIVILEGE() != null) RemovePrivilegeAction else RemoveRoleAction
      allQualifier(DbmsPrivilege(action)(p), None)
    } else {
      val scope = ctx.graphScope().ast[GraphScope]()
      labelAllQualifier(
        GraphPrivilege(RemoveLabelAction, scope)(p),
        ctx.labelsResource().ast[Option[LabelResource]](),
        p
      )
    }
  }

  final override def exitWritePrivilege(
    ctx: CypherParser.WritePrivilegeContext
  ): Unit = {
    val scope = ctx.graphScope().ast[GraphScope]()
    ctx.ast = (GraphPrivilege(WriteAction, scope)(pos(ctx)), None, List(ElementsAllQualifier()(pos(ctx))))
  }

  final override def exitNonEmptyStringList(ctx: CypherParser.NonEmptyStringListContext): Unit = {
    ctx.ast = astSeq[String](ctx.symbolicNameString())
  }

  // RESOURCES

  final override def exitLabelsResource(
    ctx: CypherParser.LabelsResourceContext
  ): Unit = {
    ctx.ast = if (ctx.TIMES() != null) {
      Some(AllLabelResource()(pos(ctx)))
    } else {
      Some(LabelsResource(ctx.nonEmptyStringList().ast[ArraySeq[String]]())(pos(ctx)))
    }
  }

  final override def exitPropertiesResource(
    ctx: CypherParser.PropertiesResourceContext
  ): Unit = {
    ctx.ast = if (ctx.TIMES() != null) {
      Some(AllPropertyResource()(pos(ctx)))
    } else {
      Some(PropertiesResource(ctx.nonEmptyStringList().ast[ArraySeq[String]]())(pos(ctx)))
    }
  }

  // QUALIFIER CONTEXTS AND HELP METHODS

  final override def exitExecuteFunctionQualifier(
    ctx: CypherParser.ExecuteFunctionQualifierContext
  ): Unit = {
    ctx.ast = ctx.globs().ast[Seq[String]].map(FunctionQualifier(_)(pos(ctx))).toList
  }

  final override def exitExecuteProcedureQualifier(
    ctx: CypherParser.ExecuteProcedureQualifierContext
  ): Unit = {
    ctx.ast = ctx.globs().ast[Seq[String]].map(ProcedureQualifier(_)(pos(ctx))).toList
  }

  final override def exitGlobs(
    ctx: CypherParser.GlobsContext
  ): Unit = {
    ctx.ast = astSeq[String](ctx.glob())
  }

  final override def exitGlob(
    ctx: CypherParser.GlobContext
  ): Unit = {
    val str = astOpt[String](ctx.escapedSymbolicNameString(), "")
    val glob = astOpt[String](ctx.globRecursive(), "")
    ctx.ast = str + glob
  }

  final override def exitGlobRecursive(
    ctx: CypherParser.GlobRecursiveContext
  ): Unit = {
    ctx.ast = ctx.globPart().ast[String]() + astOpt[String](ctx.globRecursive(), "")
  }

  final override def exitGlobPart(
    ctx: CypherParser.GlobPartContext
  ): Unit = {
    ctx.ast = if (ctx.DOT() != null) "." + astOpt[String](ctx.escapedSymbolicNameString(), "")
    else if (ctx.QUESTION() != null) "?"
    else if (ctx.TIMES() != null) "*"
    else ctx.unescapedSymbolicNameString().ast[String]()
  }

  final override def exitGraphQualifier(
    ctx: CypherParser.GraphQualifierContext
  ): Unit = {
    val token = ctx.graphQualifierToken()
    ctx.ast = if (token != null) {
      val isAll = ctx.TIMES() != null
      val strings = ctx.nonEmptyStringList()
      val graphToken = token.ast[GraphToken]
      graphToken match {
        case RelGraphToken =>
          if (isAll) List(RelationshipAllQualifier()(pos(ctx)))
          else strings.ast[Seq[String]]().map(a => RelationshipQualifier(a)(pos(ctx))).toList
        case NodeGraphToken =>
          if (isAll) List(LabelAllQualifier()(pos(ctx)))
          else strings.ast[Seq[String]]().map(a => LabelQualifier(a)(pos(ctx))).toList
        case ElementGraphToken =>
          if (isAll) List(ElementsAllQualifier()(pos(ctx)))
          else strings.ast[Seq[String]].map(a => ElementQualifier(a)(pos(ctx))).toList
      }
    } else if (ctx.FOR() != null) {
      val variable = astOpt[Variable](ctx.variable())
      val qualifiers = if (!ctx.symbolicNameString().isEmpty) {
        astSeq[String](ctx.symbolicNameString()).map(a => LabelQualifier(a)(pos(ctx))).toList
      } else List(LabelAllQualifier()(pos(ctx)))
      List(PatternQualifier(qualifiers, variable, astOpt[Expression](ctx.expression(), ctx.map.ast[Expression]())))
    } else List(ElementsAllQualifier()(pos(ctx)))
  }

  sealed private trait GraphToken
  final private case object RelGraphToken extends GraphToken
  final private case object NodeGraphToken extends GraphToken
  final private case object ElementGraphToken extends GraphToken

  override def exitGraphQualifierToken(ctx: CypherParser.GraphQualifierTokenContext): Unit = {
    ctx.ast = ctxChild(ctx, 0) match {
      case _: CypherParser.RelTokenContext     => RelGraphToken
      case _: CypherParser.NodeTokenContext    => NodeGraphToken
      case _: CypherParser.ElementTokenContext => ElementGraphToken
      case _                                   => throw new IllegalStateException("Unexpected token in Graph Qualifier")
    }
  }

  final override def exitSettingQualifier(
    ctx: CypherParser.SettingQualifierContext
  ): Unit = {
    ctx.ast = ctx.globs().ast[ArraySeq[String]]().map(SettingQualifier(_)(pos(ctx))).toList
  }

  override def exitUserQualifier(ctx: CypherParser.UserQualifierContext): Unit = {
    ctx.ast = if (ctx.userNames != null) {
      ctx.userNames.ast[ArraySeq[Expression]]().map(
        UserQualifier(_)(InputPosition.NONE)
      ).toList
    } else List(UserAllQualifier()(InputPosition.NONE))
  }

  private def withQualifier(
    action: DatabaseAction
  ): (DatabaseAction, List[DatabasePrivilegeQualifier]) = {
    (action, List(AllDatabasesQualifier()(InputPosition.NONE)))
  }

  private def withQualifier(
    action: DbmsAction
  ): (DbmsAction, List[GraphPrivilegeQualifier]) = {
    (action, List(AllQualifier()(InputPosition.NONE)))
  }

  private def allDbQualifier(
    privilege: PrivilegeType,
    resource: Option[ActionResource]
  ): (PrivilegeType, Option[ActionResource], List[DatabasePrivilegeQualifier]) = {
    (privilege, resource, List(AllDatabasesQualifier()(InputPosition.NONE)))
  }

  private def allQualifier(
    privilege: PrivilegeType,
    resource: Option[ActionResource]
  ): (PrivilegeType, Option[ActionResource], List[GraphPrivilegeQualifier]) = {
    (privilege, resource, List(AllQualifier()(InputPosition.NONE)))
  }

  private def labelAllQualifier(
    privilege: PrivilegeType,
    resource: Option[ActionResource],
    pos: InputPosition
  ): (PrivilegeType, Option[ActionResource], List[GraphPrivilegeQualifier]) = {
    (privilege, resource, List(LabelAllQualifier()(pos)))
  }

  override def exitRoleNames(ctx: CypherParser.RoleNamesContext): Unit = {
    ctx.ast = ctx.symbolicNameOrStringParameterList().ast[ArraySeq[Expression]]()
  }

  override def exitUserNames(ctx: CypherParser.UserNamesContext): Unit = {
    ctx.ast = ctx.symbolicNameOrStringParameterList().ast[ArraySeq[Expression]]()
  }

  // TOKEN CONTEXTS
  override def exitAdminToken(ctx: CypherParser.AdminTokenContext): Unit = {}

  override def exitConstraintToken(ctx: CypherParser.ConstraintTokenContext): Unit = {
    ctx.ast = CreateConstraintAction
  }

  override def exitCreateNodePrivilegeToken(ctx: CypherParser.CreateNodePrivilegeTokenContext): Unit = {
    ctx.ast = CreateNodeLabelAction
  }

  override def exitCreatePropertyPrivilegeToken(ctx: CypherParser.CreatePropertyPrivilegeTokenContext): Unit = {
    ctx.ast = CreatePropertyKeyAction
  }

  override def exitCreateRelPrivilegeToken(ctx: CypherParser.CreateRelPrivilegeTokenContext): Unit = {
    ctx.ast = CreateRelationshipTypeAction
  }
  override def exitElementToken(ctx: CypherParser.ElementTokenContext): Unit = {}
  override def exitIndexToken(ctx: CypherParser.IndexTokenContext): Unit = { ctx.ast = CreateIndexAction }
  override def exitNodeToken(ctx: CypherParser.NodeTokenContext): Unit = {}
  override def exitPasswordToken(ctx: CypherParser.PasswordTokenContext): Unit = {}
  override def exitPrivilegeToken(ctx: CypherParser.PrivilegeTokenContext): Unit = {}
  override def exitProcedureToken(ctx: CypherParser.ProcedureTokenContext): Unit = {}
  override def exitRelToken(ctx: CypherParser.RelTokenContext): Unit = {}
  override def exitRoleToken(ctx: CypherParser.RoleTokenContext): Unit = {}
  override def exitTransactionToken(ctx: CypherParser.TransactionTokenContext): Unit = {}
  override def exitFunctionToken(ctx: CypherParser.FunctionTokenContext): Unit = {}
  override def exitAscToken(ctx: CypherParser.AscTokenContext): Unit = {}
  override def exitDescToken(ctx: CypherParser.DescTokenContext): Unit = {}
  override def exitSettingToken(ctx: CypherParser.SettingTokenContext): Unit = {}
  override def exitPrimaryToken(ctx: CypherParser.PrimaryTokenContext): Unit = {}
  override def exitSecondaryToken(ctx: CypherParser.SecondaryTokenContext): Unit = {}
  override def exitSecondsToken(ctx: CypherParser.SecondsTokenContext): Unit = {}
  override def exitGroupToken(ctx: CypherParser.GroupTokenContext): Unit = {}
  override def exitPathToken(ctx: CypherParser.PathTokenContext): Unit = {}

}
