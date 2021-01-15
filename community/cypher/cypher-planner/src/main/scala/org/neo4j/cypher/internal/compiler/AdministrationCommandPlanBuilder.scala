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

import org.neo4j.configuration.helpers.NormalizedDatabaseName
import org.neo4j.cypher.internal.ast.AlterRole
import org.neo4j.cypher.internal.ast.AlterRoleAction
import org.neo4j.cypher.internal.ast.AlterUser
import org.neo4j.cypher.internal.ast.AssignPrivilegeAction
import org.neo4j.cypher.internal.ast.AssignRoleAction
import org.neo4j.cypher.internal.ast.CreateDatabase
import org.neo4j.cypher.internal.ast.CreateDatabaseAction
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
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.RevokeBothType
import org.neo4j.cypher.internal.ast.RevokeDenyType
import org.neo4j.cypher.internal.ast.RevokeGrantType
import org.neo4j.cypher.internal.ast.RevokePrivilege
import org.neo4j.cypher.internal.ast.RevokeRolesFromUsers
import org.neo4j.cypher.internal.ast.RevokeType
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
import org.neo4j.cypher.internal.logical.plans.DatabaseAdministrationLogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.PrivilegePlan
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.logical.plans.SecurityAdministrationLogicalPlan
import org.neo4j.cypher.internal.planner.spi.AdministrationPlannerName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen

/**
 * This planner takes on queries that run at the DBMS level for multi-database administration.
 *
 * Take on administrative queries that require no planning such as multi-database administration commands
 */
//noinspection DuplicatedCode
case object AdministrationCommandPlanBuilder extends Phase[PlannerContext, BaseState, LogicalPlanState] {

  val prettifier: Prettifier = Prettifier(ExpressionStringifier())

  override def phase: CompilationPhase = PIPE_BUILDING

  override def postConditions: Set[StepSequencer.Condition] = Set.empty

  override def process(from: BaseState, context: PlannerContext): LogicalPlanState = {
    implicit val idGen: SequentialIdGen = new SequentialIdGen()
    def planRevokes(source: PrivilegePlan,
                    revokeType: RevokeType,
                    planRevoke: (PrivilegePlan, String) => PrivilegePlan): PrivilegePlan = revokeType match {
      case t: RevokeBothType =>
        val revokeGrant = planRevoke(source, RevokeGrantType()(t.position).relType)
        planRevoke(revokeGrant, RevokeDenyType()(t.position).relType)
      case t => planRevoke(source, t.relType)
    }

    def getSourceForCreateRole(roleName: Either[String, Parameter], ifExistsDo: IfExistsDo): SecurityAdministrationLogicalPlan = ifExistsDo match {
      case IfExistsReplace => plans.DropRole(plans.AssertDbmsAdmin(None, Seq(DropRoleAction, CreateRoleAction)), roleName)
      case IfExistsDoNothing => plans.DoNothingIfExists(plans.AssertDbmsAdmin(CreateRoleAction), "Role", roleName)
      case _ => plans.AssertDbmsAdmin(CreateRoleAction)
    }

    def wrapInWait(logicalPlan: DatabaseAdministrationLogicalPlan, databaseName: Either[String,Parameter],
                   waitUntilComplete: WaitUntilComplete): DatabaseAdministrationLogicalPlan = waitUntilComplete match {
      case NoWait => logicalPlan
      case _ =>  plans.WaitForCompletion(logicalPlan, databaseName, waitUntilComplete)
    }

    val maybeLogicalPlan: Option[LogicalPlan] = from.statement() match {
      // SHOW USERS
      case su: ShowUsers => Some(plans.ShowUsers(plans.AssertDbmsAdmin(ShowUserAction), su.defaultColumnNames, su.yields, su.returns))

      // SHOW CURRENT USER
      case su: ShowCurrentUser => Some(plans.ShowCurrentUser(su.defaultColumnNames, su.yields, su.returns))

      // CREATE [OR REPLACE] USER foo [IF NOT EXISTS] WITH [PLAINTEXT | ENCRYPTED] PASSWORD password
      case c@CreateUser(userName, isEncryptedPassword, initialPassword, userOptions, ifExistsDo) =>
        val source = ifExistsDo match {
          case IfExistsReplace => plans.DropUser(plans.AssertNotCurrentUser(plans.AssertDbmsAdmin(None, Seq(DropUserAction, CreateUserAction)), userName, "replace", "Deleting yourself is not allowed"), userName)
          case IfExistsDoNothing => plans.DoNothingIfExists(plans.AssertDbmsAdmin(CreateUserAction), "User", userName)
          case _ => plans.AssertDbmsAdmin(CreateUserAction)
        }
        Some(plans.LogSystemCommand(
          plans.CreateUser(source, userName, isEncryptedPassword, initialPassword, userOptions.requirePasswordChange.getOrElse(true), userOptions.suspended, userOptions.homeDatabase),
          prettifier.asString(c)))

      // DROP USER foo [IF EXISTS]
      case c@DropUser(userName, ifExists) =>
        val admin = plans.AssertNotCurrentUser(plans.AssertDbmsAdmin(DropUserAction), userName, "delete", "Deleting yourself is not allowed")
        val source = if (ifExists) plans.DoNothingIfNotExists(admin, "User", userName, "delete") else plans.EnsureNodeExists(admin, "User", userName)
        Some(plans.LogSystemCommand(plans.DropUser(source, userName), prettifier.asString(c)))

      // ALTER USER foo
      case c@AlterUser(userName, isEncryptedPassword, initialPassword, userOptions, ifExists) =>
        val adminActions = Vector((initialPassword, SetPasswordsAction),
                               (userOptions.requirePasswordChange, SetPasswordsAction),
                               (userOptions.suspended, SetUserStatusAction),
                               (userOptions.homeDatabase, SetUserHomeDatabaseAction)).collect{case (Some(_), action) => action}.distinct
        if (adminActions.isEmpty) throw new IllegalStateException("Alter user has nothing to do")

        val admin = plans.AssertDbmsAdmin(None, adminActions)

        val assertionSubPlan =
          if(userOptions.suspended.isDefined) plans.AssertNotCurrentUser(admin, userName, "alter", "Changing your own activation status is not allowed")
          else admin
        val ifExistsSubPlan = if (ifExists) plans.DoNothingIfNotExists(assertionSubPlan, "User", userName, "alter") else assertionSubPlan
        Some(plans.LogSystemCommand(
          plans.AlterUser(ifExistsSubPlan, userName, isEncryptedPassword, initialPassword, userOptions.requirePasswordChange, userOptions.suspended, userOptions.homeDatabase),
          prettifier.asString(c)))

      // ALTER CURRENT USER SET PASSWORD FROM currentPassword TO newPassword
      case c@SetOwnPassword(newPassword, currentPassword) =>
        Some(plans.LogSystemCommand(
          plans.SetOwnPassword(newPassword, currentPassword),
          prettifier.asString(c)))

      // SHOW [ ALL | POPULATED ] ROLES
      case sr @ ShowRoles(false, showAll, _,_) =>
        Some(plans.ShowRoles(plans.AssertDbmsAdmin(ShowRoleAction), withUsers = false, showAll = showAll, sr.defaultColumnNames, sr.yields, sr.returns ))

      // SHOW [ ALL | POPULATED ] ROLES WITH USERS
      case sr @ ShowRoles(true, showAll, _,_) =>
        Some(plans.ShowRoles(plans.AssertDbmsAdmin(None, Seq(ShowRoleAction, ShowUserAction)), withUsers = true, showAll = showAll,
          sr.defaultColumnNames, sr.yields, sr.returns ))

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

      // ALTER ROLE foo SET NAME bar
      case c@AlterRole(fromRoleName, toRoleName) =>
        val source = plans.AssertDbmsAdmin(AlterRoleAction)
        Some(plans.LogSystemCommand(plans.AlterRole(source, fromRoleName, toRoleName), prettifier.asString(c)))

      // DROP ROLE foo [IF EXISTS]
      case c@DropRole(roleName, ifExists) =>
        val admin = plans.AssertDbmsAdmin(DropRoleAction)
        val source = if (ifExists) plans.DoNothingIfNotExists(admin, "Role", roleName, "delete") else plans.EnsureNodeExists(admin, "Role", roleName)
        Some(plans.LogSystemCommand(plans.DropRole(source, roleName), prettifier.asString(c)))

      // GRANT roles TO users
      case c@GrantRolesToUsers(roleNames, userNames) =>
        val plan = (for (userName <- userNames; roleName <- roleNames) yield {
          roleName -> userName
        }).foldLeft(plans.AssertDbmsAdmin(AssignRoleAction).asInstanceOf[SecurityAdministrationLogicalPlan]) {
          case (source, (roleName, userName)) => plans.GrantRoleToUser(source, roleName, userName)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE roles FROM users
      case c@RevokeRolesFromUsers(roleNames, userNames) =>
        val plan = (for (userName <- userNames; roleName <- roleNames) yield {
          roleName -> userName
        }).foldLeft(plans.AssertDbmsAdmin(RemoveRoleAction).asInstanceOf[SecurityAdministrationLogicalPlan]) {
          case (source, (userName, roleName)) => plans.RevokeRoleFromUser(source, userName, roleName)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // GRANT _ ON DBMS TO role
      case c@GrantPrivilege(DbmsPrivilege(action), _, qualifiers, roleNames) =>
        val plan = (for (roleName <- roleNames; qualifier <- qualifiers; simpleQualifier <- qualifier.simplify) yield {
          (roleName, simpleQualifier)
        }).foldLeft(plans.AssertDbmsAdmin(AssignPrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (source, (roleName, simpleQualifier)) => plans.GrantDbmsAction(source, action, simpleQualifier, roleName)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // DENY _ ON DBMS TO role
      case c@DenyPrivilege(DbmsPrivilege(action), _, qualifiers, roleNames) =>
        val plan = (for (roleName <- roleNames; qualifier <- qualifiers; simpleQualifier <- qualifier.simplify) yield {
          (roleName, simpleQualifier)
        }).foldLeft(plans.AssertDbmsAdmin(AssignPrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (source, (roleName, simpleQualifier)) => plans.DenyDbmsAction(source, action, simpleQualifier, roleName)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE _ ON DBMS FROM role
      case c@RevokePrivilege(DbmsPrivilege(action), _, qualifiers, roleNames, revokeType) =>
        val plan = (for (roleName <- roleNames; qualifier <- qualifiers; simpleQualifier <- qualifier.simplify) yield {
          (roleName, simpleQualifier)
        }).foldLeft(plans.AssertDbmsAdmin(RemovePrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (previous, (roleName, simpleQualifier)) => planRevokes(previous, revokeType, (s, r) => plans.RevokeDbmsAction(s, action, simpleQualifier, roleName, r))
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // GRANT _ ON DATABASE foo TO role
      case c@GrantPrivilege(DatabasePrivilege(action, dbScopes), _, qualifiers, roleNames) =>
        val plan = (for (dbScope <- dbScopes; roleName <- roleNames; qualifier <- qualifiers; simpleQualifiers <- qualifier.simplify) yield {
          (roleName, simpleQualifiers, dbScope)
        }).foldLeft(plans.AssertDbmsAdmin(AssignPrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (source, (role, qualifier, dbScope: DatabaseScope)) =>
            plans.GrantDatabaseAction(source, action, dbScope, qualifier, role)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // DENY _ ON DATABASE foo TO role
      case c@DenyPrivilege(DatabasePrivilege(action, dbScopes), _, qualifiers, roleNames) =>
        val plan = (for (dbScope <- dbScopes; roleName <- roleNames; qualifier <- qualifiers; simpleQualifiers <- qualifier.simplify) yield {
          (roleName, simpleQualifiers,  dbScope)
        }).foldLeft(plans.AssertDbmsAdmin(AssignPrivilegeAction).asInstanceOf[PrivilegePlan]) {
         case (source, (role, qualifier, dbScope:DatabaseScope)) =>
            plans.DenyDatabaseAction(source, action, dbScope, qualifier, role)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE _ ON DATABASE foo FROM role
      case c@RevokePrivilege(DatabasePrivilege(action, dbScopes), _, qualifiers, roleNames, revokeType) =>
        val plan = (for (dbScope <- dbScopes; roleName <- roleNames; qualifier <- qualifiers; simpleQualifiers <- qualifier.simplify) yield {
          (roleName, simpleQualifiers, dbScope)
        }).foldLeft(plans.AssertDbmsAdmin(RemovePrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (plan, (role, qualifier, dbScope: DatabaseScope)) =>
            planRevokes(plan, revokeType, (s, r) => plans.RevokeDatabaseAction(s, action, dbScope, qualifier, role, r))
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // GRANT _ ON GRAPH foo _ TO role
      case c@GrantPrivilege(GraphPrivilege(action, graphScopes), optionalResource, qualifiers, roleNames) =>
        val resources = optionalResource.getOrElse(NoResource()(InputPosition.NONE))
        val plan = (for (graphScope <- graphScopes; roleName <- roleNames; qualifier <- qualifiers; simpleQualifiers <- qualifier.simplify; resource <- resources.simplify) yield {
          (roleName, simpleQualifiers, resource, graphScope)
        }).foldLeft(plans.AssertDbmsAdmin(AssignPrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (source, (roleName, segment, resource, graphScope: GraphScope)) => plans.GrantGraphAction(source, action, resource, graphScope, segment, roleName)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // DENY _ ON GRAPH foo _ TO role
      case c@DenyPrivilege(GraphPrivilege(action, graphScopes), optionalResource, qualifiers, roleNames) =>
        val resources = optionalResource.getOrElse(NoResource()(InputPosition.NONE))
        val plan = (for (graphScope <- graphScopes; roleName <- roleNames; qualifier <- qualifiers; simpleQualifiers <- qualifier.simplify; resource <- resources.simplify) yield {
          (roleName, simpleQualifiers, resource, graphScope)
        }).foldLeft(plans.AssertDbmsAdmin(AssignPrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (source, (roleName, segment, resource, graphScope: GraphScope)) => plans.DenyGraphAction(source, action, resource, graphScope, segment, roleName)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE _ ON GRAPH foo _ FROM role
      case c@RevokePrivilege(GraphPrivilege(action, graphScopes), optionalResource, qualifiers, roleNames, revokeType) =>
        val resources = optionalResource.getOrElse(NoResource()(InputPosition.NONE))
        val plan = (for (graphScope <- graphScopes; roleName <- roleNames; qualifier <- qualifiers; simpleQualifiers <- qualifier.simplify; resource <- resources.simplify) yield {
          (roleName, simpleQualifiers, resource, graphScope)
        }).foldLeft(plans.AssertDbmsAdmin(RemovePrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (source, (roleName, segment, resource, graphScope: GraphScope)) =>
            planRevokes(source, revokeType, (s, r) => plans.RevokeGraphAction(s, action, resource, graphScope, segment, roleName, r))
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // SHOW USER user PRIVILEGES
      case sp @ ShowPrivileges(scope: ShowUserPrivileges, _, _) =>
        val user = scope.user
        val source = if (user.isDefined) Some(plans.AssertDbmsAdminOrSelf(user.get, Seq(ShowPrivilegeAction, ShowUserAction))) else None
        Some(plans.ShowPrivileges(source, scope, sp.defaultColumnNames, sp.yields, sp.returns))

      // SHOW USERS user1, user2 PRIVILEGES
      case sp @ ShowPrivileges(scope: ShowUsersPrivileges, _, _) =>
        val (newScope, source) = {
          val users = scope.users
          if (users.size > 1) (scope, Some(plans.AssertDbmsAdmin(None, Seq(ShowPrivilegeAction, ShowUserAction))))
          else (ShowUserPrivileges(Some(users.head))(scope.position), Some(plans.AssertDbmsAdminOrSelf(users.head, Seq(ShowPrivilegeAction, ShowUserAction))))
        }
        Some(plans.ShowPrivileges(source, newScope, sp.defaultColumnNames, sp.yields, sp.returns))

      // SHOW [ALL | ROLE role | ROLES role1, role2] PRIVILEGES
      case sp @ ShowPrivileges(scope, _, _) =>
        Some(plans.ShowPrivileges(Some(plans.AssertDbmsAdmin(ShowPrivilegeAction)), scope, sp.defaultColumnNames, sp.yields, sp.returns))

      // SHOW USER user PRIVILEGES AS [REVOKE] COMMAND
      case sp @ ShowPrivilegeCommands(scope: ShowUserPrivileges, asRevoke, _,_) =>
        val user = scope.user
        val source = if (user.isDefined) Some(plans.AssertDbmsAdminOrSelf(user.get, Seq(ShowPrivilegeAction, ShowUserAction))) else None
        Some(plans.ShowPrivilegeCommands(source, scope, asRevoke, sp.defaultColumnNames, sp.yields, sp.returns))

      // SHOW USERS user1, user2 PRIVILEGES AS [REVOKE] COMMAND
      case sp @ ShowPrivilegeCommands(scope: ShowUsersPrivileges, asRevoke, _, _) =>
        val (newScope, source) = {
          val users = scope.users
          if (users.size > 1) (scope, Some(plans.AssertDbmsAdmin(None, Seq(ShowPrivilegeAction, ShowUserAction))))
          else (ShowUserPrivileges(Some(users.head))(scope.position), Some(plans.AssertDbmsAdminOrSelf(users.head, Seq(ShowPrivilegeAction, ShowUserAction))))
        }
        Some(plans.ShowPrivilegeCommands(source, newScope, asRevoke, sp.defaultColumnNames, sp.yields, sp.returns))

      // SHOW [ALL | ROLE role | ROLES role1, role2] PRIVILEGES AS [REVOKE] COMMAND
      case sp @ ShowPrivilegeCommands(scope, asRevoke, _, returns) =>
        Some(plans.ShowPrivilegeCommands(Some(plans.AssertDbmsAdmin(ShowPrivilegeAction)), scope, asRevoke, sp.defaultColumnNames, sp.yields, sp.returns))

      // SHOW DATABASES | SHOW DEFAULT DATABASE | SHOW DATABASE foo
      case sd @ ShowDatabase(scope, _,_) =>
        Some(plans.ShowDatabase(scope, sd.defaultColumnNames, sd.yields, sd.returns))

      // CREATE [OR REPLACE] DATABASE foo [IF NOT EXISTS]
      case CreateDatabase(dbName, ifExistsDo, waitUntilComplete) =>
        val source = ifExistsDo match {
          case IfExistsReplace =>
            plans.DropDatabase(plans.AssertDbmsAdmin(Some(plans.AssertNotBlocked(CreateDatabaseAction)), Seq(DropDatabaseAction, CreateDatabaseAction)),
              dbName, DestroyData)

          case IfExistsDoNothing =>
            plans.DoNothingIfExists(plans.AssertDbmsAdmin(plans.AssertNotBlocked(CreateDatabaseAction), CreateDatabaseAction),
              "Database", dbName, s => new NormalizedDatabaseName(s).name())

          case _ => plans.AssertDbmsAdmin(plans.AssertNotBlocked(CreateDatabaseAction), CreateDatabaseAction)
        }
        Some(wrapInWait(plans.EnsureValidNumberOfDatabases(plans.CreateDatabase(source, dbName)), dbName, waitUntilComplete))

      // DROP DATABASE foo [IF EXISTS] [DESTROY | DUMP DATA]
      case DropDatabase(dbName, ifExists, additionalAction, waitUntilComplete) =>
        val checkAllowed = plans.AssertDbmsAdmin(plans.AssertNotBlocked(DropDatabaseAction),DropDatabaseAction)
        val source = if (ifExists) plans.DoNothingIfNotExists(checkAllowed, "Database", dbName, "delete", s => new NormalizedDatabaseName(s).name()) else checkAllowed
        Some(wrapInWait(plans.DropDatabase(plans.EnsureValidNonSystemDatabase(source, dbName, "delete"), dbName, additionalAction), dbName, waitUntilComplete))

      // START DATABASE foo
      case StartDatabase(dbName, waitUntilComplete) =>
        val checkAllowed = plans.AssertDatabaseAdmin(StartDatabaseAction, dbName, Some(plans.AssertNotBlocked(StartDatabaseAction)))
        Some(wrapInWait(plans.StartDatabase(checkAllowed, dbName), dbName, waitUntilComplete))

      // STOP DATABASE foo
      case StopDatabase(dbName, waitUntilComplete) =>
        val checkAllowed = plans.AssertDatabaseAdmin(StopDatabaseAction, dbName, Some(plans.AssertNotBlocked(StopDatabaseAction)))
        Some(wrapInWait(plans.StopDatabase(
          plans.EnsureValidNonSystemDatabase(checkAllowed, dbName, "stop"), dbName), dbName, waitUntilComplete))

      // Global call: CALL foo.bar.baz("arg1", 2) // only if system procedure is allowed!
      case Query(None, SingleQuery(Seq(resolved@ResolvedCall(signature, _, _, _, _, _),returns@Return(_,_,_,_,_,_)))) if signature.systemProcedure =>
        val SemanticCheckResult(_, errors) = resolved.semanticCheck(SemanticState.clean)
        errors.foreach { error => throw context.cypherExceptionFactory.syntaxException(error.msg, error.position) }
        Some(plans.SystemProcedureCall(signature.name.toString, resolved, returns, context.params, checkCredentialsExpired = !signature.allowExpiredCredentials))

      case _ => None
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
