/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.cypher.internal.ast.IfExistsDo
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.MatchPrivilege
import org.neo4j.cypher.internal.ast.NoResource
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.ReadPrivilege
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
import org.neo4j.cypher.internal.ast.SetUserStatusAction
import org.neo4j.cypher.internal.ast.ShowDatabase
import org.neo4j.cypher.internal.ast.ShowDatabases
import org.neo4j.cypher.internal.ast.ShowDefaultDatabase
import org.neo4j.cypher.internal.ast.ShowPrivilegeAction
import org.neo4j.cypher.internal.ast.ShowPrivileges
import org.neo4j.cypher.internal.ast.ShowRoleAction
import org.neo4j.cypher.internal.ast.ShowRoles
import org.neo4j.cypher.internal.ast.ShowUserAction
import org.neo4j.cypher.internal.ast.ShowUserPrivileges
import org.neo4j.cypher.internal.ast.ShowUsers
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.StartDatabase
import org.neo4j.cypher.internal.ast.StartDatabaseAction
import org.neo4j.cypher.internal.ast.StopDatabase
import org.neo4j.cypher.internal.ast.StopDatabaseAction
import org.neo4j.cypher.internal.ast.TraversePrivilege
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
import org.neo4j.cypher.internal.frontend.phases.Condition
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.PrivilegePlan
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.logical.plans.SecurityAdministrationLogicalPlan
import org.neo4j.cypher.internal.planner.spi.AdministrationPlannerName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen

/**
 * This planner takes on queries that run at the DBMS level for multi-database administration
 */
//noinspection DuplicatedCode
case object AdministrationCommandPlanBuilder extends Phase[PlannerContext, BaseState, LogicalPlanState] {

  val prettifier: Prettifier = Prettifier(ExpressionStringifier())

  override def phase: CompilationPhase = PIPE_BUILDING

  override def description = "take on administrative queries that require no planning such as multi-database administration commands"

  override def postConditions: Set[Condition] = Set.empty

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
      case _: IfExistsReplace => plans.DropRole(plans.AssertDbmsAdmin(Seq(DropRoleAction, CreateRoleAction)), roleName)
      case _: IfExistsDoNothing => plans.DoNothingIfExists(plans.AssertDbmsAdmin(CreateRoleAction), "Role", roleName)
      case _ => plans.AssertDbmsAdmin(CreateRoleAction)
    }

    val maybeLogicalPlan: Option[LogicalPlan] = from.statement() match {
      // SHOW USERS
      case su @ ShowUsers(yields, where, returns) => Some(plans.ShowUsers(plans.AssertDbmsAdmin(ShowUserAction), su.returnColumnNames, yields, where, returns))

      // CREATE [OR REPLACE] USER foo [IF NOT EXISTS] WITH PASSWORD password
      case c@CreateUser(userName, initialPassword, requirePasswordChange, suspended, ifExistsDo) =>
        val source = ifExistsDo match {
          case _: IfExistsReplace => plans.DropUser(plans.AssertNotCurrentUser(plans.AssertDbmsAdmin(Seq(DropUserAction, CreateUserAction)), userName, "replace", "Deleting yourself is not allowed"), userName)
          case _: IfExistsDoNothing => plans.DoNothingIfExists(plans.AssertDbmsAdmin(CreateUserAction), "User", userName)
          case _ => plans.AssertDbmsAdmin(CreateUserAction)
        }
        Some(plans.LogSystemCommand(
          plans.CreateUser(source, userName, initialPassword, requirePasswordChange, suspended),
          prettifier.asString(c)))

      // DROP USER foo [IF EXISTS]
      case c@DropUser(userName, ifExists) =>
        val admin = plans.AssertNotCurrentUser(plans.AssertDbmsAdmin(DropUserAction), userName, "delete", "Deleting yourself is not allowed")
        val source = if (ifExists) plans.DoNothingIfNotExists(admin, "User", userName) else plans.EnsureNodeExists(admin, "User", userName)
        Some(plans.LogSystemCommand(plans.DropUser(source, userName), prettifier.asString(c)))

      // ALTER USER foo
      case c@AlterUser(userName, initialPassword, requirePasswordChange, suspended) =>
        val isSetPassword = initialPassword.isDefined || requirePasswordChange.isDefined
        val admin = (isSetPassword, suspended) match {
          case (true, Some(_)) => plans.AssertDbmsAdmin(Seq(SetPasswordsAction, SetUserStatusAction))
          case (true, None) => plans.AssertDbmsAdmin(SetPasswordsAction)
          case (false, Some(_)) => plans.AssertDbmsAdmin(SetUserStatusAction)
        }
        val assertionSubPlan =
          if(suspended.isDefined) plans.AssertNotCurrentUser(admin, userName, "alter", "Changing your own activation status is not allowed")
          else admin
        Some(plans.LogSystemCommand(
          plans.AlterUser(assertionSubPlan, userName, initialPassword, requirePasswordChange, suspended),
          prettifier.asString(c)))

      // ALTER CURRENT USER SET PASSWORD FROM currentPassword TO newPassword
      case c@SetOwnPassword(newPassword, currentPassword) =>
        Some(plans.LogSystemCommand(
          plans.SetOwnPassword(newPassword, currentPassword),
          prettifier.asString(c)))

      // SHOW [ ALL | POPULATED ] ROLES
      case sr @ ShowRoles(false, showAll, yields, where, returns) =>
        Some(plans.ShowRoles(plans.AssertDbmsAdmin(ShowRoleAction), withUsers = false, showAll = showAll, sr.returnColumnNames, yields, where, returns ))

      // SHOW [ ALL | POPULATED ] ROLES WITH USERS
      case sr @ ShowRoles(true, showAll, yields, where, returns) =>
        Some(plans.ShowRoles(plans.AssertDbmsAdmin(Seq(ShowRoleAction, ShowUserAction)), withUsers = true, showAll = showAll,
          sr.returnColumnNames, yields, where, returns ))

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

      // DROP ROLE foo [IF EXISTS]
      case c@DropRole(roleName, ifExists) =>
        val admin = plans.AssertDbmsAdmin(DropRoleAction)
        val source = if (ifExists) plans.DoNothingIfNotExists(admin, "Role", roleName) else plans.EnsureNodeExists(admin, "Role", roleName)
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
      case c@GrantPrivilege(DbmsPrivilege(action), _, _, _, roleNames) =>
        val plan = roleNames.foldLeft(plans.AssertDbmsAdmin(AssignPrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (source, roleName) => plans.GrantDbmsAction(source, action, roleName)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // DENY _ ON DBMS TO role
      case c@DenyPrivilege(DbmsPrivilege(action), _, _, _, roleNames) =>
        val plan = roleNames.foldLeft(plans.AssertDbmsAdmin(AssignPrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (source, roleName) => plans.DenyDbmsAction(source, action, roleName)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE _ ON DBMS FROM role
      case c@RevokePrivilege(DbmsPrivilege(action), _, _, _, roleNames, revokeType) =>
        val plan = roleNames.foldLeft(plans.AssertDbmsAdmin(RemovePrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (previous, roleName) => planRevokes(previous, revokeType, (s, r) => plans.RevokeDbmsAction(s, action, roleName, r))
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // GRANT _ ON DATABASE foo TO role
      case c@GrantPrivilege(DatabasePrivilege(action), _, graphScopes, qualifiers, roleNames) =>
        val plan = (for (graphScope <- graphScopes; roleName <- roleNames; qualifier <- qualifiers.simplify) yield {
          (roleName, qualifier, graphScope)
        }).foldLeft(plans.AssertDbmsAdmin(AssignPrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (source, (role, qualifier, graphScope)) =>
            plans.GrantDatabaseAction(source, action, graphScope, qualifier, role)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // DENY _ ON DATABASE foo TO role
      case c@DenyPrivilege(DatabasePrivilege(action), _, graphScopes, qualifiers, roleNames) =>
        val plan = (for (graphScope <- graphScopes; roleName <- roleNames; qualifier <- qualifiers.simplify) yield {
          (roleName, qualifier,  graphScope)
        }).foldLeft(plans.AssertDbmsAdmin(AssignPrivilegeAction).asInstanceOf[PrivilegePlan]) {
         case (source, (role, qualifier, graphScope)) =>
            plans.DenyDatabaseAction(source, action, graphScope, qualifier, role)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE _ ON DATABASE foo FROM role
      case c@RevokePrivilege(DatabasePrivilege(action), _, graphScopes, qualifiers, roleNames, revokeType) =>
        val plan = (for (graphScope <- graphScopes; roleName <- roleNames; qualifier <- qualifiers.simplify) yield {
          (roleName, qualifier, graphScope)
        }).foldLeft(plans.AssertDbmsAdmin(RemovePrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (plan, (role, qualifier, graphScope)) =>
            planRevokes(plan, revokeType, (s, r) => plans.RevokeDatabaseAction(s, action, graphScope, qualifier, role, r))
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // GRANT TRAVERSE ON GRAPH foo ELEMENTS A (*) TO role
      case c@GrantPrivilege(TraversePrivilege(), _, graphScopes, segments, roleNames) =>
        val plan = (for (graphScope <- graphScopes; roleName <- roleNames; segment <- segments.simplify) yield {
          (roleName, segment, graphScope)
        }).foldLeft(plans.AssertDbmsAdmin(AssignPrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (source, (roleName, segment, graphScope)) => plans.GrantTraverse(source, graphScope, segment, roleName)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // DENY TRAVERSE ON GRAPH foo ELEMENTS A (*) TO role
      case c@DenyPrivilege(TraversePrivilege(), _, graphScopes, segments, roleNames) =>
        val plan = (for (graphScope <- graphScopes; roleName <- roleNames; segment <- segments.simplify) yield {
          (roleName, segment, graphScope)
        }).foldLeft(plans.AssertDbmsAdmin(AssignPrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (source, (roleName, segment, graphScope)) => plans.DenyTraverse(source, graphScope, segment, roleName)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE TRAVERSE ON GRAPH foo ELEMENTS A (*) FROM role
      case c@RevokePrivilege(TraversePrivilege(), _, graphScopes, segments, roleNames, revokeType) =>
        val plan = (for (graphScope <- graphScopes; roleName <- roleNames; segment <- segments.simplify) yield {
          (roleName, segment, graphScope)
        }).foldLeft(plans.AssertDbmsAdmin(RemovePrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (source, (roleName, segment, graphScope)) => planRevokes(source, revokeType, (s, r) => plans.RevokeTraverse(s, graphScope, segment, roleName, r))
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // GRANT _ ON GRAPH foo _ TO role
      case c@GrantPrivilege(GraphPrivilege(action), optionalResource, graphScopes, segments, roleNames) =>
        val resources = optionalResource.getOrElse(NoResource()(InputPosition.NONE))
        val plan = (for (graphScope <- graphScopes; roleName <- roleNames; segment <- segments.simplify; resource <- resources.simplify) yield {
          (roleName, segment, resource, graphScope)
        }).foldLeft(plans.AssertDbmsAdmin(AssignPrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (source, (roleName, segment, resource, graphScope)) => plans.GrantGraphAction(source, action, resource, graphScope, segment, roleName)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // DENY _ ON GRAPH foo _ TO role
      case c@DenyPrivilege(GraphPrivilege(action), optionalResource, graphScopes, segments, roleNames) =>
        val resources = optionalResource.getOrElse(NoResource()(InputPosition.NONE))
        val plan = (for (graphScope <- graphScopes; roleName <- roleNames; segment <- segments.simplify; resource <- resources.simplify) yield {
          (roleName, segment, resource, graphScope)
        }).foldLeft(plans.AssertDbmsAdmin(AssignPrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (source, (roleName, segment, resource, graphScope)) => plans.DenyGraphAction(source, action, resource, graphScope, segment, roleName)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE _ ON GRAPH foo _ FROM role
      case c@RevokePrivilege(GraphPrivilege(action), optionalResource, graphScopes, segments, roleNames, revokeType) =>
        val resources = optionalResource.getOrElse(NoResource()(InputPosition.NONE))
        val plan = (for (graphScope <- graphScopes; roleName <- roleNames; segment <- segments.simplify; resource <- resources.simplify) yield {
          (roleName, segment, resource, graphScope)
        }).foldLeft(plans.AssertDbmsAdmin(RemovePrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (source, (roleName, segment, resource, graphScope)) =>
            planRevokes(source, revokeType, (s, r) => plans.RevokeGraphAction(s, action, resource, graphScope, segment, roleName, r))
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // GRANT READ {prop} ON GRAPH foo ELEMENTS A (*) TO role
      case c@GrantPrivilege(ReadPrivilege(), Some(resources), graphScopes, segments, roleNames) =>
        val plan = (for (graphScope <- graphScopes; roleName <- roleNames; segment <- segments.simplify; resource <- resources.simplify) yield {
          (roleName, segment, resource, graphScope)
        }).foldLeft(plans.AssertDbmsAdmin(AssignPrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (source, (roleName, segment, resource, graphScope)) => plans.GrantRead(source, resource, graphScope, segment, roleName)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // DENY READ {prop} ON GRAPH foo ELEMENTS A (*) TO role
      case c@DenyPrivilege(ReadPrivilege(), Some(resources), graphScopes, segments, roleNames) =>
        val plan = (for (graphScope <- graphScopes; roleName <- roleNames; segment <- segments.simplify; resource <- resources.simplify) yield {
          (roleName, segment, resource, graphScope)
        }).foldLeft(plans.AssertDbmsAdmin(AssignPrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (source, (roleName, segment, resource, graphScope)) => plans.DenyRead(source, resource, graphScope, segment, roleName)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE READ {prop} ON GRAPH foo ELEMENTS A (*) FROM role
      case c@RevokePrivilege(ReadPrivilege(), Some(resources), graphScopes, segments, roleNames, revokeType) =>
        val plan = (for (graphScope <- graphScopes; roleName <- roleNames; segment <- segments.simplify; resource <- resources.simplify) yield {
          (roleName, segment, resource, graphScope)
        }).foldLeft(plans.AssertDbmsAdmin(RemovePrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (source, (roleName, segment, resource, graphScope)) => planRevokes(source, revokeType, (s,r) => plans.RevokeRead(s, resource, graphScope, segment, roleName, r))
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // GRANT MATCH {prop} ON GRAPH foo ELEMENTS A (*) TO role
      case c@GrantPrivilege(MatchPrivilege(), Some(resources), graphScopes, segments, roleNames) =>
        val plan = (for (graphScope <- graphScopes; roleName <- roleNames; segment <- segments.simplify; resource <- resources.simplify) yield {
          (roleName, segment, resource, graphScope)
        }).foldLeft(plans.AssertDbmsAdmin(AssignPrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (source, (roleName, segment, resource, graphScope)) => plans.GrantMatch(source, resource, graphScope, segment, roleName)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // DENY MATCH {prop} ON GRAPH foo ELEMENTS A (*) TO role
      case c@DenyPrivilege(MatchPrivilege(), Some(resources), graphScopes, segments, roleNames) =>
        val plan = (for (graphScope <- graphScopes; roleName <- roleNames; segment <- segments.simplify; resource <- resources.simplify) yield {
          (roleName, segment, resource, graphScope)
        }).foldLeft(plans.AssertDbmsAdmin(AssignPrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (source, (roleName, segment, resource, graphScope)) => plans.DenyMatch(source, resource, graphScope, segment, roleName)
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE MATCH {prop} ON GRAPH foo ELEMENTS A (*) FROM role
      case c@RevokePrivilege(MatchPrivilege(), Some(resources), graphScopes, segments, roleNames, revokeType) =>
        val plan = (for (graphScope <- graphScopes; roleName <- roleNames; segment <- segments.simplify; resource <- resources.simplify) yield {
          (roleName, segment, resource, graphScope)
        }).foldLeft(plans.AssertDbmsAdmin(RemovePrivilegeAction).asInstanceOf[PrivilegePlan]) {
          case (source, (roleName, segment, resource, graphScope)) => planRevokes(source, revokeType, (s,r) => plans.RevokeMatch(s, resource, graphScope, segment, roleName, r))
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // SHOW USER user PRIVILEGES
      case sp @ ShowPrivileges(scope: ShowUserPrivileges, where, yields, returns) =>
        val user = scope.user
        val source = if (user.isDefined) Some(plans.AssertDbmsAdminOrSelf(user.get, Seq(ShowPrivilegeAction, ShowUserAction))) else None
        Some(plans.ShowPrivileges(source, scope, sp.returnColumnNames, where, yields, returns))

      // SHOW [ALL | ROLE role] PRIVILEGES
      case sp @ ShowPrivileges(scope, where, yields, returns) =>
        Some(plans.ShowPrivileges(Some(plans.AssertDbmsAdmin(ShowPrivilegeAction)), scope, sp.returnColumnNames, where, yields, returns))

      // SHOW DATABASES
      case sd @ ShowDatabases(yields, where, returns) =>
        Some(plans.ShowDatabases(sd.returnColumnNames, yields, where, returns))

      // SHOW DEFAULT DATABASE
      case sd @ ShowDefaultDatabase(yields, where, returns) =>
        Some(plans.ShowDefaultDatabase(sd.returnColumnNames, yields, where, returns))

      // SHOW DATABASE foo
      case sd @ ShowDatabase(dbName, yields, where, returns) =>
        Some(plans.ShowDatabase(dbName, sd.returnColumnNames, yields, where, returns))

      // CREATE [OR REPLACE] DATABASE foo [IF NOT EXISTS]
      case CreateDatabase(dbName, ifExistsDo) =>
        val source = ifExistsDo match {
          case _: IfExistsReplace => plans.DropDatabase(plans.AssertDbmsAdmin(Seq(DropDatabaseAction, CreateDatabaseAction)), dbName, DestroyData)
          case _: IfExistsDoNothing => plans.DoNothingIfExists(plans.AssertDbmsAdmin(CreateDatabaseAction), "Database", dbName, s => new NormalizedDatabaseName(s).name())
          case _ => plans.AssertDbmsAdmin(CreateDatabaseAction)
        }
        Some(plans.EnsureValidNumberOfDatabases(plans.CreateDatabase(source, dbName)))

      // DROP DATABASE foo [IF EXISTS] [DESTROY | DUMP DATA]
      case DropDatabase(dbName, ifExists, additionalAction) =>
        val admin = plans.AssertDbmsAdmin(DropDatabaseAction)
        val source = if (ifExists) plans.DoNothingIfNotExists(admin, "Database", dbName, s => new NormalizedDatabaseName(s).name()) else admin
        Some(plans.DropDatabase(plans.EnsureValidNonSystemDatabase(source, dbName, "delete"), dbName, additionalAction))

      // START DATABASE foo
      case StartDatabase(dbName) =>
        Some(plans.StartDatabase(plans.AssertDatabaseAdmin(StartDatabaseAction, dbName), dbName))

      // STOP DATABASE foo
      case StopDatabase(dbName) =>
        Some(plans.StopDatabase(
          plans.EnsureValidNonSystemDatabase(
            plans.AssertDatabaseAdmin(StopDatabaseAction, dbName), dbName, "stop"), dbName))

      // Global call: CALL foo.bar.baz("arg1", 2) // only if system procedure is allowed!
      case Query(None, SingleQuery(Seq(resolved@ResolvedCall(signature, _, _, _, _),Return(_,_,_,_,_,_)))) if signature.systemProcedure =>
        val checkCredentialsExpired = signature.name match {
          // TODO this is a hot fix to get browser to get password change required error
          // It should be changed so that only a few key procedures are allowed to be run with password change required
          case QualifiedName(Seq("db"), "indexes") => true
          case _ => false
        }
        val SemanticCheckResult(_, errors) = resolved.semanticCheck(SemanticState.clean)
        errors.foreach { error => throw context.cypherExceptionFactory.syntaxException(error.msg, error.position) }
        Some(plans.SystemProcedureCall(signature.name.toString, resolved, context.params, checkCredentialsExpired))

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

  override def description: String = "Unsupported system command"

  override def postConditions: Set[Condition] = Set.empty

  override def process(from: BaseState, context: PlannerContext): LogicalPlanState = throw new RuntimeException(s"Not a recognised system command or procedure. " +
    s"This Cypher command can only be executed in a user database: ${from.queryText}")
}
