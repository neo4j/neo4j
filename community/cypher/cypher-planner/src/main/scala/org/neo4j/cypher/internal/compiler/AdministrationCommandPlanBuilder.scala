/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.internal.ast.AlterDatabase
import org.neo4j.cypher.internal.ast.AlterDatabaseAction
import org.neo4j.cypher.internal.ast.AlterDatabaseAlias
import org.neo4j.cypher.internal.ast.AlterUser
import org.neo4j.cypher.internal.ast.AssignPrivilegeAction
import org.neo4j.cypher.internal.ast.AssignRoleAction
import org.neo4j.cypher.internal.ast.CallClause
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.ClauseAllowedOnSystem
import org.neo4j.cypher.internal.ast.CommandClauseAllowedOnSystem
import org.neo4j.cypher.internal.ast.CreateDatabase
import org.neo4j.cypher.internal.ast.CreateDatabaseAction
import org.neo4j.cypher.internal.ast.CreateDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateRole
import org.neo4j.cypher.internal.ast.CreateRoleAction
import org.neo4j.cypher.internal.ast.CreateUser
import org.neo4j.cypher.internal.ast.CreateUserAction
import org.neo4j.cypher.internal.ast.DatabasePrivilege
import org.neo4j.cypher.internal.ast.DatabaseScope
import org.neo4j.cypher.internal.ast.DbmsPrivilege
import org.neo4j.cypher.internal.ast.DenyPrivilege
import org.neo4j.cypher.internal.ast.DestroyData
import org.neo4j.cypher.internal.ast.DropDatabase
import org.neo4j.cypher.internal.ast.DropDatabaseAction
import org.neo4j.cypher.internal.ast.DropDatabaseAlias
import org.neo4j.cypher.internal.ast.DropRole
import org.neo4j.cypher.internal.ast.DropRoleAction
import org.neo4j.cypher.internal.ast.DropUser
import org.neo4j.cypher.internal.ast.DropUserAction
import org.neo4j.cypher.internal.ast.GrantPrivilege
import org.neo4j.cypher.internal.ast.GrantRolesToUsers
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.GraphScope
import org.neo4j.cypher.internal.ast.IfExistsDo
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.NoResource
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.RemovePrivilegeAction
import org.neo4j.cypher.internal.ast.RemoveRoleAction
import org.neo4j.cypher.internal.ast.RenameRole
import org.neo4j.cypher.internal.ast.RenameRoleAction
import org.neo4j.cypher.internal.ast.RenameUser
import org.neo4j.cypher.internal.ast.RenameUserAction
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.RevokeBothType
import org.neo4j.cypher.internal.ast.RevokeDenyType
import org.neo4j.cypher.internal.ast.RevokeGrantType
import org.neo4j.cypher.internal.ast.RevokePrivilege
import org.neo4j.cypher.internal.ast.RevokeRolesFromUsers
import org.neo4j.cypher.internal.ast.RevokeType
import org.neo4j.cypher.internal.ast.SetDatabaseAccessAction
import org.neo4j.cypher.internal.ast.SetOwnPassword
import org.neo4j.cypher.internal.ast.SetPasswordsAction
import org.neo4j.cypher.internal.ast.SetUserHomeDatabaseAction
import org.neo4j.cypher.internal.ast.SetUserStatusAction
import org.neo4j.cypher.internal.ast.ShowCurrentUser
import org.neo4j.cypher.internal.ast.ShowDatabase
import org.neo4j.cypher.internal.ast.ShowPrivilegeAction
import org.neo4j.cypher.internal.ast.ShowPrivilegeCommands
import org.neo4j.cypher.internal.ast.ShowPrivileges
import org.neo4j.cypher.internal.ast.ShowRoleAction
import org.neo4j.cypher.internal.ast.ShowRoles
import org.neo4j.cypher.internal.ast.ShowUserAction
import org.neo4j.cypher.internal.ast.ShowUserPrivileges
import org.neo4j.cypher.internal.ast.ShowUsers
import org.neo4j.cypher.internal.ast.ShowUsersPrivileges
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.StartDatabase
import org.neo4j.cypher.internal.ast.StartDatabaseAction
import org.neo4j.cypher.internal.ast.StopDatabase
import org.neo4j.cypher.internal.ast.StopDatabaseAction
import org.neo4j.cypher.internal.ast.WaitUntilComplete
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.planner.spi.AdministrationPlannerName
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.dbms.database.TopologyGraphDbmsModel.DATABASE_NAME
import org.neo4j.dbms.database.TopologyGraphDbmsModel.DATABASE_NAME_LABEL_DESCRIPTION
import org.neo4j.dbms.database.TopologyGraphDbmsModel.PRIMARY_PROPERTY
import org.neo4j.kernel.database.NormalizedDatabaseName

/**
 * This planner takes on queries that run at the DBMS level for multi-database administration.
 *
 * Take on administrative queries that require no planning such as multi-database administration commands
 */
//noinspection DuplicatedCode
case object AdministrationCommandPlanBuilder extends Phase[PlannerContext, BaseState, LogicalPlanState] {

  val prettifier: Prettifier = Prettifier(ExpressionStringifier())

  private val systemDbProcedureRules = "The system database supports a restricted set of Cypher clauses. " +
    "The supported clause structure for procedure calls is: CALL, YIELD, RETURN. YIELD and RETURN clauses are optional. " +
    "The order of the clauses is fix and each can only occur once."

  override def phase: CompilationPhase = PIPE_BUILDING

  override def postConditions: Set[StepSequencer.Condition] = Set.empty

  override def process(from: BaseState, context: PlannerContext): LogicalPlanState = {
    implicit val idGen: SequentialIdGen = new SequentialIdGen()
    def planRevokes(source: plans.PrivilegePlan,
                    revokeType: RevokeType,
                    planRevoke: (plans.PrivilegePlan, String) => plans.PrivilegePlan): plans.PrivilegePlan = revokeType match {
      case t: RevokeBothType =>
        val revokeGrant = planRevoke(source, RevokeGrantType()(t.position).relType)
        planRevoke(revokeGrant, RevokeDenyType()(t.position).relType)
      case t => planRevoke(source, t.relType)
    }

    def getSourceForCreateRole(roleName: Either[String, Parameter], ifExistsDo: IfExistsDo): plans.SecurityAdministrationLogicalPlan = ifExistsDo match {
      case IfExistsReplace => plans.DropRole(plans.AssertAllowedDbmsActions(None, Seq(DropRoleAction, CreateRoleAction)), roleName)
      case IfExistsDoNothing => plans.DoNothingIfExists(plans.AssertAllowedDbmsActions(CreateRoleAction), "Role", roleName)
      case _ => plans.AssertAllowedDbmsActions(CreateRoleAction)
    }

    def wrapInWait(logicalPlan: plans.DatabaseAdministrationLogicalPlan, databaseName: Either[String,Parameter],
                   waitUntilComplete: WaitUntilComplete): plans.DatabaseAdministrationLogicalPlan = waitUntilComplete match {
      case NoWait => logicalPlan
      case _ =>  plans.WaitForCompletion(logicalPlan, databaseName, waitUntilComplete)
    }

    def planSystemProcedureCall(resolved: ResolvedCall, returns: Option[Return]): LogicalPlan = {
      val SemanticCheckResult(_, errors) = resolved.semanticCheck(SemanticState.clean)
      errors.foreach { error => throw context.cypherExceptionFactory.syntaxException(error.msg, error.position) }
      val signature = resolved.signature
      val checkCredentialsExpired = !signature.allowExpiredCredentials
      plans.SystemProcedureCall(signature.name.toString, resolved, returns, context.params, checkCredentialsExpired)
    }

    val maybeLogicalPlan: Option[plans.LogicalPlan] = from.statement() match {
      // SHOW USERS
      case su: ShowUsers => Some(plans.ShowUsers(plans.AssertAllowedDbmsActions(ShowUserAction), su.defaultColumnNames, su.yields, su.returns))

      // SHOW CURRENT USER
      case su: ShowCurrentUser => Some(plans.ShowCurrentUser(su.defaultColumnNames, su.yields, su.returns))

      // CREATE [OR REPLACE] USER foo [IF NOT EXISTS] WITH [PLAINTEXT | ENCRYPTED] PASSWORD password
      case c@CreateUser(userName, isEncryptedPassword, initialPassword, userOptions, ifExistsDo) =>
        val source = ifExistsDo match {
          case IfExistsReplace => plans.DropUser(plans.AssertNotCurrentUser(plans.AssertAllowedDbmsActions(None, Seq(DropUserAction, CreateUserAction)), userName, "replace", "Deleting yourself is not allowed"), userName)
          case IfExistsDoNothing => plans.DoNothingIfExists(plans.AssertAllowedDbmsActions(CreateUserAction), "User", userName)
          case _ => plans.AssertAllowedDbmsActions(CreateUserAction)
        }
        Some(plans.LogSystemCommand(
          plans.CreateUser(source, userName, isEncryptedPassword, initialPassword, userOptions.requirePasswordChange.getOrElse(true), userOptions.suspended, userOptions.homeDatabase),
          prettifier.asString(c)))

      // RENAME USER foo [IF EXISTS] TO bar
      case c@RenameUser(fromUserName, toUserName, ifExists) =>
        val assertAllowed = plans.AssertAllowedDbmsActions(RenameUserAction)
        val source = if (ifExists) plans.DoNothingIfNotExists(assertAllowed, "User", fromUserName, "rename") else assertAllowed
        Some(plans.LogSystemCommand(plans.RenameUser(source, fromUserName, toUserName), prettifier.asString(c)))

      // DROP USER foo [IF EXISTS]
      case c@DropUser(userName, ifExists) =>
        val assertAllowed = plans.AssertNotCurrentUser(plans.AssertAllowedDbmsActions(DropUserAction), userName, "delete", "Deleting yourself is not allowed")
        val source = if (ifExists) plans.DoNothingIfNotExists(assertAllowed, "User", userName, "delete")
        else plans.EnsureNodeExists(assertAllowed, "User", userName, labelDescription = "User", action = "delete")
        Some(plans.LogSystemCommand(plans.DropUser(source, userName), prettifier.asString(c)))

      // ALTER USER foo
      case c@AlterUser(userName, isEncryptedPassword, initialPassword, userOptions, ifExists) =>
        val dbmsActions = Vector((initialPassword, SetPasswordsAction),
                               (userOptions.requirePasswordChange, SetPasswordsAction),
                               (userOptions.suspended, SetUserStatusAction),
                               (userOptions.homeDatabase, SetUserHomeDatabaseAction)).collect{case (Some(_), action) => action}.distinct
        if (dbmsActions.isEmpty) throw new IllegalStateException("Alter user has nothing to do")

        val assertAllowed = plans.AssertAllowedDbmsActions(None, dbmsActions)

        val assertionSubPlan =
          if(userOptions.suspended.isDefined) plans.AssertNotCurrentUser(assertAllowed, userName, "alter", "Changing your own activation status is not allowed")
          else assertAllowed
        val ifExistsSubPlan = if (ifExists) plans.DoNothingIfNotExists(assertionSubPlan, "User", userName, "alter") else assertionSubPlan
        Some(plans.LogSystemCommand(
          plans.AlterUser(ifExistsSubPlan, userName, isEncryptedPassword, initialPassword, userOptions.requirePasswordChange, userOptions.suspended, userOptions.homeDatabase),
          prettifier.asString(c)))

      // ALTER CURRENT USER SET PASSWORD FROM currentPassword TO newPassword
      case c: SetOwnPassword =>
        Some(plans.LogSystemCommand(
          plans.SetOwnPassword(c.newPassword, c.currentPassword),
          prettifier.asString(c)))

      // SHOW [ ALL | POPULATED ] ROLES [WITH USERS]
      case sr: ShowRoles =>
        val assertAllowed =
          if (sr.withUsers) plans.AssertAllowedDbmsActions(None, Seq(ShowRoleAction, ShowUserAction))
          else plans.AssertAllowedDbmsActions(ShowRoleAction)
        Some(plans.ShowRoles(assertAllowed, withUsers = sr.withUsers, showAll = sr.showAll, sr.defaultColumnNames, sr.yields, sr.returns ))

      // CREATE [OR REPLACE] ROLE foo [IF NOT EXISTS]
      case c@CreateRole(roleName, None, ifExistsDo) =>
        val source = getSourceForCreateRole(roleName, ifExistsDo)
        Some(plans.LogSystemCommand(plans.CreateRole(source, roleName), prettifier.asString(c)))

      // CREATE [OR REPLACE] ROLE foo [IF NOT EXISTS] AS COPY OF bar
      case c@CreateRole(roleName, Some(fromName), ifExistsDo) =>
        val source = getSourceForCreateRole(roleName, ifExistsDo)
        Some(plans.LogSystemCommand(
          plans.CopyRolePrivileges(
            plans.CopyRolePrivileges(
              plans.CreateRole(
                plans.RequireRole(source, fromName), roleName),
              roleName, fromName, "GRANTED"),
            roleName, fromName, "DENIED"),
          prettifier.asString(c)))

      // RENAME ROLE foo [IF EXISTS] TO bar
      case c@RenameRole(fromRoleName, toRoleName, ifExists) =>
        val assertAllowed = plans.AssertAllowedDbmsActions(RenameRoleAction)
        val source = if (ifExists) plans.DoNothingIfNotExists(assertAllowed, "Role", fromRoleName, "rename") else assertAllowed
        Some(plans.LogSystemCommand(plans.RenameRole(source, fromRoleName, toRoleName), prettifier.asString(c)))

      // DROP ROLE foo [IF EXISTS]
      case c@DropRole(roleName, ifExists) =>
        val assertAllowed = plans.AssertAllowedDbmsActions(DropRoleAction)
        val source = if (ifExists) plans.DoNothingIfNotExists(assertAllowed, "Role", roleName, "delete")
        else plans.EnsureNodeExists(assertAllowed, "Role", roleName, labelDescription = "Role", action = "delete")
        Some(plans.LogSystemCommand(plans.DropRole(source, roleName), prettifier.asString(c)))

      // GRANT roles TO users
      case c: GrantRolesToUsers =>
        val plan = (for (userName <- c.userNames; roleName <- c.roleNames) yield {
          roleName -> userName
        }).foldLeft(plans.AssertAllowedDbmsActions(AssignRoleAction).asInstanceOf[plans.SecurityAdministrationLogicalPlan]) {
          case (source, (roleName, userName)) => plans.GrantRoleToUser(source, roleName, userName)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE roles FROM users
      case c: RevokeRolesFromUsers =>
        val plan = (for (userName <- c.userNames; roleName <- c.roleNames) yield {
          roleName -> userName
        }).foldLeft(plans.AssertAllowedDbmsActions(RemoveRoleAction).asInstanceOf[plans.SecurityAdministrationLogicalPlan]) {
          case (source, (userName, roleName)) => plans.RevokeRoleFromUser(source, userName, roleName)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // GRANT _ ON DBMS TO role
      case c@GrantPrivilege(DbmsPrivilege(action), _, qualifiers, roleNames) =>
        val plan = (for (roleName <- roleNames; qualifier <- qualifiers; simpleQualifier <- qualifier.simplify) yield {
          (roleName, simpleQualifier)
        }).foldLeft(plans.AssertAllowedDbmsActions(AssignPrivilegeAction).asInstanceOf[plans.PrivilegePlan]) {
          case (source, (roleName, simpleQualifier)) => plans.GrantDbmsAction(source, action, simpleQualifier, roleName)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // DENY _ ON DBMS TO role
      case c@DenyPrivilege(DbmsPrivilege(action), _, qualifiers, roleNames) =>
        val plan = (for (roleName <- roleNames; qualifier <- qualifiers; simpleQualifier <- qualifier.simplify) yield {
          (roleName, simpleQualifier)
        }).foldLeft(plans.AssertAllowedDbmsActions(AssignPrivilegeAction).asInstanceOf[plans.PrivilegePlan]) {
          case (source, (roleName, simpleQualifier)) => plans.DenyDbmsAction(source, action, simpleQualifier, roleName)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE _ ON DBMS FROM role
      case c@RevokePrivilege(DbmsPrivilege(action), _, qualifiers, roleNames, revokeType) =>
        val plan = (for (roleName <- roleNames; qualifier <- qualifiers; simpleQualifier <- qualifier.simplify) yield {
          (roleName, simpleQualifier)
        }).foldLeft(plans.AssertAllowedDbmsActions(RemovePrivilegeAction).asInstanceOf[plans.PrivilegePlan]) {
          case (previous, (roleName, simpleQualifier)) => planRevokes(previous, revokeType, (s, r) => plans.RevokeDbmsAction(s, action, simpleQualifier, roleName, r))
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // GRANT _ ON DATABASE foo TO role
      case c@GrantPrivilege(DatabasePrivilege(action, dbScopes), _, qualifiers, roleNames) =>
        val plan = (for (dbScope <- dbScopes; roleName <- roleNames; qualifier <- qualifiers; simpleQualifiers <- qualifier.simplify) yield {
          (roleName, simpleQualifiers, dbScope)
        }).foldLeft(plans.AssertAllowedDbmsActions(AssignPrivilegeAction).asInstanceOf[plans.PrivilegePlan]) {
          case (source, (role, qualifier, dbScope: DatabaseScope)) =>
            plans.GrantDatabaseAction(source, action, dbScope, qualifier, role)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // DENY _ ON DATABASE foo TO role
      case c@DenyPrivilege(DatabasePrivilege(action, dbScopes), _, qualifiers, roleNames) =>
        val plan = (for (dbScope <- dbScopes; roleName <- roleNames; qualifier <- qualifiers; simpleQualifiers <- qualifier.simplify) yield {
          (roleName, simpleQualifiers,  dbScope)
        }).foldLeft(plans.AssertAllowedDbmsActions(AssignPrivilegeAction).asInstanceOf[plans.PrivilegePlan]) {
         case (source, (role, qualifier, dbScope:DatabaseScope)) =>
            plans.DenyDatabaseAction(source, action, dbScope, qualifier, role)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE _ ON DATABASE foo FROM role
      case c@RevokePrivilege(DatabasePrivilege(action, dbScopes), _, qualifiers, roleNames, revokeType) =>
        val plan = (for (dbScope <- dbScopes; roleName <- roleNames; qualifier <- qualifiers; simpleQualifiers <- qualifier.simplify) yield {
          (roleName, simpleQualifiers, dbScope)
        }).foldLeft(plans.AssertAllowedDbmsActions(RemovePrivilegeAction).asInstanceOf[plans.PrivilegePlan]) {
          case (plan, (role, qualifier, dbScope: DatabaseScope)) =>
            planRevokes(plan, revokeType, (s, r) => plans.RevokeDatabaseAction(s, action, dbScope, qualifier, role, r))
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // GRANT _ ON GRAPH foo _ TO role
      case c@GrantPrivilege(GraphPrivilege(action, graphScopes), optionalResource, qualifiers, roleNames) =>
        val resources = optionalResource.getOrElse(NoResource()(InputPosition.NONE))
        val plan = (for (graphScope <- graphScopes; roleName <- roleNames; qualifier <- qualifiers; simpleQualifiers <- qualifier.simplify; resource <- resources.simplify) yield {
          (roleName, simpleQualifiers, resource, graphScope)
        }).foldLeft(plans.AssertAllowedDbmsActions(AssignPrivilegeAction).asInstanceOf[plans.PrivilegePlan]) {
          case (source, (roleName, segment, resource, graphScope: GraphScope)) => plans.GrantGraphAction(source, action, resource, graphScope, segment, roleName)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // DENY _ ON GRAPH foo _ TO role
      case c@DenyPrivilege(GraphPrivilege(action, graphScopes), optionalResource, qualifiers, roleNames) =>
        val resources = optionalResource.getOrElse(NoResource()(InputPosition.NONE))
        val plan = (for (graphScope <- graphScopes; roleName <- roleNames; qualifier <- qualifiers; simpleQualifiers <- qualifier.simplify; resource <- resources.simplify) yield {
          (roleName, simpleQualifiers, resource, graphScope)
        }).foldLeft(plans.AssertAllowedDbmsActions(AssignPrivilegeAction).asInstanceOf[plans.PrivilegePlan]) {
          case (source, (roleName, segment, resource, graphScope: GraphScope)) => plans.DenyGraphAction(source, action, resource, graphScope, segment, roleName)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE _ ON GRAPH foo _ FROM role
      case c@RevokePrivilege(GraphPrivilege(action, graphScopes), optionalResource, qualifiers, roleNames, revokeType) =>
        val resources = optionalResource.getOrElse(NoResource()(InputPosition.NONE))
        val plan = (for (graphScope <- graphScopes; roleName <- roleNames; qualifier <- qualifiers; simpleQualifiers <- qualifier.simplify; resource <- resources.simplify) yield {
          (roleName, simpleQualifiers, resource, graphScope)
        }).foldLeft(plans.AssertAllowedDbmsActions(RemovePrivilegeAction).asInstanceOf[plans.PrivilegePlan]) {
          case (source, (roleName, segment, resource, graphScope: GraphScope)) =>
            planRevokes(source, revokeType, (s, r) => plans.RevokeGraphAction(s, action, resource, graphScope, segment, roleName, r))
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // SHOW [ALL | ROLE role | ROLES role1, role2 | USER [user] | USERS user1, user2] PRIVILEGES
      case sp: ShowPrivileges =>
        val (newScope, source) = sp.scope match {
          // SHOW USER [user] PRIVILEGES
          case scope: ShowUserPrivileges =>
            val user = scope.user
            val source = if (user.isDefined) Some(plans.AssertAllowedDbmsActionsOrSelf(user.get, Seq(ShowPrivilegeAction, ShowUserAction))) else None
            (scope, source)
          // SHOW USERS user1, user2 PRIVILEGES
          case scope: ShowUsersPrivileges =>
            val users = scope.users
            if (users.size > 1) (scope, Some(plans.AssertAllowedDbmsActions(None, Seq(ShowPrivilegeAction, ShowUserAction))))
            else (ShowUserPrivileges(Some(users.head))(scope.position), Some(plans.AssertAllowedDbmsActionsOrSelf(users.head, Seq(ShowPrivilegeAction, ShowUserAction))))
          // SHOW [ALL | ROLE role | ROLES role1, role2] PRIVILEGES
          case scope =>
            (scope, Some(plans.AssertAllowedDbmsActions(ShowPrivilegeAction)))
        }
        Some(plans.ShowPrivileges(source, newScope, sp.defaultColumnNames, sp.yields, sp.returns))

      // SHOW [ALL | ROLE role | ROLES role1, role2 | USER [user] | USERS user1, user2] PRIVILEGES AS [REVOKE] COMMAND
      case sp: ShowPrivilegeCommands =>
        val (newScope, source) = sp.scope match {
          // SHOW USER [user] PRIVILEGES
          case scope: ShowUserPrivileges =>
            val user = scope.user
            val source = if (user.isDefined) Some(plans.AssertAllowedDbmsActionsOrSelf(user.get, Seq(ShowPrivilegeAction, ShowUserAction))) else None
            (scope, source)
          // SHOW USERS user1, user2 PRIVILEGES
          case scope: ShowUsersPrivileges =>
            val users = scope.users
            if (users.size > 1) (scope, Some(plans.AssertAllowedDbmsActions(None, Seq(ShowPrivilegeAction, ShowUserAction))))
            else (ShowUserPrivileges(Some(users.head))(scope.position), Some(plans.AssertAllowedDbmsActionsOrSelf(users.head, Seq(ShowPrivilegeAction, ShowUserAction))))
          // SHOW [ALL | ROLE role | ROLES role1, role2] PRIVILEGES
          case scope =>
            (scope, Some(plans.AssertAllowedDbmsActions(ShowPrivilegeAction)))
        }
        Some(plans.ShowPrivilegeCommands(source, newScope, sp.asRevoke, sp.defaultColumnNames, sp.yields, sp.returns))

      // SHOW DATABASES | SHOW DEFAULT DATABASE | SHOW DATABASE foo
      case sd: ShowDatabase =>
        Some(plans.ShowDatabase(sd.scope, sd.defaultColumns.useAllColumns, sd.defaultColumnNames, sd.yields, sd.returns))

      // CREATE [OR REPLACE] DATABASE foo [IF NOT EXISTS]
      case c@CreateDatabase(dbName, ifExistsDo, options, waitUntilComplete) =>
        (ifExistsDo match {
          case IfExistsReplace =>
            Some(plans.AssertNotBlocked(CreateDatabaseAction))
              .map(p => plans.AssertAllowedDbmsActions(Some(p), Seq(DropDatabaseAction, CreateDatabaseAction)))
              .map(plans.EnsureDatabaseHasNoAliases(_, dbName))
              .map(plans.DropDatabase(_, dbName, DestroyData))
          case IfExistsDoNothing =>
            Some(plans.AssertNotBlocked(CreateDatabaseAction))
              .map(plans.AssertAllowedDbmsActions(_, CreateDatabaseAction))
              .map(plans.DoNothingIfDatabaseExists(_, dbName, s => new NormalizedDatabaseName(s).name()))
          case _ =>
            Some(plans.AssertNotBlocked(CreateDatabaseAction))
              .map(plans.AssertAllowedDbmsActions(_, CreateDatabaseAction))
        }).map(plans.CreateDatabase(_, dbName, options))
          .map(plans.EnsureValidNumberOfDatabases(_))
          .map(wrapInWait(_, dbName, waitUntilComplete))
          .map(plans.LogSystemCommand(_, prettifier.asString(c)))

      // DROP DATABASE foo [IF EXISTS] [DESTROY | DUMP DATA]
      case c@DropDatabase(dbName, ifExists, additionalAction, waitUntilComplete) =>
        Some(plans.AssertAllowedDbmsActions(plans.AssertNotBlocked(DropDatabaseAction),DropDatabaseAction))
          .map(assertAllowed =>
            if (ifExists)
              plans.DoNothingIfDatabaseNotExists(assertAllowed, dbName, "delete", s => new NormalizedDatabaseName(s).name())
            else assertAllowed)
          .map(plans.EnsureDatabaseHasNoAliases(_, dbName))
          .map(plans.EnsureValidNonSystemDatabase(_, dbName, "delete"))
          .map(plans.DropDatabase(_, dbName, additionalAction))
          .map(wrapInWait(_, dbName, waitUntilComplete))
          .map(plans.LogSystemCommand(_, prettifier.asString(c)))

      // ALTER DATABASE foo [IF EXISTS] SET ACCESS {READ ONLY | READ WRITE}
      case c@AlterDatabase(dbName, ifExists, access) =>
        val assertAllowed = plans.AssertAllowedDbmsActions(plans.AssertNotBlocked(AlterDatabaseAction),SetDatabaseAccessAction)
        val source = if (ifExists) plans.DoNothingIfDatabaseNotExists(assertAllowed, dbName, "alter", s => new NormalizedDatabaseName(s).name()) else assertAllowed
        val plan = plans.AlterDatabase(plans.EnsureValidNonSystemDatabase(source, dbName, "alter"), dbName, access)
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // START DATABASE foo
      case c@StartDatabase(dbName, waitUntilComplete) =>
        val assertAllowed = plans.AssertAllowedDatabaseAction(StartDatabaseAction, dbName, Some(plans.AssertNotBlocked(StartDatabaseAction)))
        val plan = wrapInWait(plans.StartDatabase(assertAllowed, dbName), dbName, waitUntilComplete)
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // STOP DATABASE foo
      case c@StopDatabase(dbName, waitUntilComplete) =>
        val assertAllowed = plans.AssertAllowedDatabaseAction(StopDatabaseAction, dbName, Some(plans.AssertNotBlocked(StopDatabaseAction)))
        val plan = wrapInWait(plans.StopDatabase(plans.EnsureValidNonSystemDatabase(assertAllowed, dbName, "stop"), dbName), dbName, waitUntilComplete)
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // CREATE DATABASE ALIAS
      case c@CreateDatabaseAlias(aliasName, targetName, ifExistsDo) =>
        val (source, replace) = ifExistsDo match {
          case IfExistsReplace => (plans.DropDatabaseAlias(plans.AssertAllowedDbmsActions(None, Seq(CreateDatabaseAction, DropDatabaseAction)), aliasName), true)
          case IfExistsDoNothing => (plans.DoNothingIfDatabaseExists(plans.AssertAllowedDbmsActions(None, Seq(CreateDatabaseAction)),
            aliasName), false)
          case _ => (plans.AssertAllowedDbmsActions(None, Seq(CreateDatabaseAction)), false)
        }
        Some(plans.LogSystemCommand(plans.CreateDatabaseAlias(plans.EnsureValidNonSystemDatabase(source, targetName, "create", Some(aliasName)), aliasName, targetName, replace), prettifier.asString(c)))

      // DROP DATABASE ALIAS foo [IF EXISTS]
      case c@DropDatabaseAlias(aliasName, ifExists) =>
        val assertAllowed = plans.AssertAllowedDbmsActions(None, Seq(DropDatabaseAction))
        val source =
          if (ifExists) plans.DoNothingIfDatabaseNotExists(assertAllowed, aliasName, "delete")
          else plans.EnsureNodeExists(assertAllowed, DATABASE_NAME, aliasName,
            new NormalizedDatabaseName(_).name(), node => s"WHERE $node.$PRIMARY_PROPERTY = false", DATABASE_NAME_LABEL_DESCRIPTION, "delete")

        Some(plans.LogSystemCommand(plans.DropDatabaseAlias(source, aliasName), prettifier.asString(c)))

      // ALTER DATABASE ALIAS foo
      case c@AlterDatabaseAlias(aliasName, targetName, ifExists) =>
        val assertAllowed = plans.AssertAllowedDbmsActions(None, Seq(AlterDatabaseAction))
        val source = if (ifExists) plans.DoNothingIfDatabaseNotExists(assertAllowed, aliasName, "alter")
        else plans.EnsureNodeExists(assertAllowed, DATABASE_NAME, aliasName, new NormalizedDatabaseName(_).name(),
          node => s"WHERE $node.$PRIMARY_PROPERTY = false", DATABASE_NAME_LABEL_DESCRIPTION, "alter")
        Some(plans.LogSystemCommand(plans.AlterDatabaseAlias(
          plans.EnsureValidNonSystemDatabase(source, targetName, "alter", Some(aliasName)), aliasName, targetName),
          prettifier.asString(c)))

      // Global call: CALL foo.bar.baz("arg1", 2) // only if system procedure is allowed!
      case Query(None, SingleQuery(Seq(resolved@plans.ResolvedCall(signature, _, _, _, _, _),returns@Return(_,_,_,_,_,_)))) if signature.systemProcedure =>
        Some(planSystemProcedureCall(resolved, Some(returns)))

      case Query(None, SingleQuery(Seq(resolved@plans.ResolvedCall(signature, _, _, _, _, _)))) if signature.systemProcedure =>
        Some(planSystemProcedureCall(resolved, None))

      // Non-administration commands that are allowed on system database, e.g. SHOW PROCEDURES YIELD ...
      // Currently doesn't allow WITH, is this a problem for rewrites?
      case q@Query(None, SingleQuery(clauses))
        if clauses.exists(_.isInstanceOf[CommandClauseAllowedOnSystem]) && clauses.forall(_.isInstanceOf[ClauseAllowedOnSystem]) =>
        Some(plans.AllowedNonAdministrationCommands(q))

      case q =>
        val unsupportedClauses = q.treeFold(List.empty[String]) {
          case _: CallClause => acc => SkipChildren(acc)
          case _: Return => acc => SkipChildren(acc)
          case c: Clause => acc => SkipChildren(acc :+ c.name)
        }
        if (unsupportedClauses.nonEmpty) {
          throw new RuntimeException(s"The following unsupported clauses were used: ${unsupportedClauses.sorted.mkString(", ")}. \n" + systemDbProcedureRules)
        }

        val callCount = q.treeCount {
          case _: CallClause => true
        }
        if (callCount > 1) {
          throw new RuntimeException(s"The given query uses $callCount CALL clauses (${callCount - 1} too many). \n" + systemDbProcedureRules)
        }

        None  // this means we will throw the general UnsupportedSystemCommand
    }

    val planState = LogicalPlanState(from)

    if (maybeLogicalPlan.isDefined)
      planState.copy(maybeLogicalPlan = maybeLogicalPlan, plannerName = AdministrationPlannerName)
    else planState
  }

}

case object UnsupportedSystemCommand extends Phase[PlannerContext, BaseState, LogicalPlanState] {
  override def phase: CompilationPhase = PIPE_BUILDING

  override def postConditions: Set[StepSequencer.Condition] = Set.empty

  override def process(from: BaseState, context: PlannerContext): LogicalPlanState = throw new RuntimeException(s"Not a recognised system command or procedure. " +
    s"This Cypher command can only be executed in a user database: ${from.queryText}")
}
