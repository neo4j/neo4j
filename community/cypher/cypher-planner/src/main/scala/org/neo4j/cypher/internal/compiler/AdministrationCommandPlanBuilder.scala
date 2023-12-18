/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler

import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.internal.ast.AddedInRewrite
import org.neo4j.cypher.internal.ast.AllDatabasesScope
import org.neo4j.cypher.internal.ast.AllGraphsScope
import org.neo4j.cypher.internal.ast.AlterAliasAction
import org.neo4j.cypher.internal.ast.AlterDatabase
import org.neo4j.cypher.internal.ast.AlterDatabaseAction
import org.neo4j.cypher.internal.ast.AlterLocalDatabaseAlias
import org.neo4j.cypher.internal.ast.AlterRemoteDatabaseAlias
import org.neo4j.cypher.internal.ast.AlterServer
import org.neo4j.cypher.internal.ast.AlterUser
import org.neo4j.cypher.internal.ast.AssignImmutablePrivilegeAction
import org.neo4j.cypher.internal.ast.AssignPrivilegeAction
import org.neo4j.cypher.internal.ast.AssignRoleAction
import org.neo4j.cypher.internal.ast.CallClause
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.ClauseAllowedOnSystem
import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.CommandClause
import org.neo4j.cypher.internal.ast.CommandClauseAllowedOnSystem
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.CreateAliasAction
import org.neo4j.cypher.internal.ast.CreateCompositeDatabase
import org.neo4j.cypher.internal.ast.CreateCompositeDatabaseAction
import org.neo4j.cypher.internal.ast.CreateDatabase
import org.neo4j.cypher.internal.ast.CreateDatabaseAction
import org.neo4j.cypher.internal.ast.CreateLocalDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateRemoteDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateRole
import org.neo4j.cypher.internal.ast.CreateRoleAction
import org.neo4j.cypher.internal.ast.CreateUser
import org.neo4j.cypher.internal.ast.CreateUserAction
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.DatabasePrivilege
import org.neo4j.cypher.internal.ast.DatabaseScope
import org.neo4j.cypher.internal.ast.DbmsAction
import org.neo4j.cypher.internal.ast.DbmsPrivilege
import org.neo4j.cypher.internal.ast.DeallocateServers
import org.neo4j.cypher.internal.ast.DenyPrivilege
import org.neo4j.cypher.internal.ast.DestroyData
import org.neo4j.cypher.internal.ast.DropAliasAction
import org.neo4j.cypher.internal.ast.DropCompositeDatabaseAction
import org.neo4j.cypher.internal.ast.DropDatabase
import org.neo4j.cypher.internal.ast.DropDatabaseAction
import org.neo4j.cypher.internal.ast.DropDatabaseAlias
import org.neo4j.cypher.internal.ast.DropRole
import org.neo4j.cypher.internal.ast.DropRoleAction
import org.neo4j.cypher.internal.ast.DropServer
import org.neo4j.cypher.internal.ast.DropUser
import org.neo4j.cypher.internal.ast.DropUserAction
import org.neo4j.cypher.internal.ast.EnableServer
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.GrantPrivilege
import org.neo4j.cypher.internal.ast.GrantRolesToUsers
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.GraphScope
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.IfExistsDo
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.LoadPrivilege
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NoResource
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.ParsedAsYield
import org.neo4j.cypher.internal.ast.ReallocateDatabases
import org.neo4j.cypher.internal.ast.RemovePrivilegeAction
import org.neo4j.cypher.internal.ast.RemoveRoleAction
import org.neo4j.cypher.internal.ast.RenameRole
import org.neo4j.cypher.internal.ast.RenameRoleAction
import org.neo4j.cypher.internal.ast.RenameServer
import org.neo4j.cypher.internal.ast.RenameUser
import org.neo4j.cypher.internal.ast.RenameUserAction
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.RevokeBothType
import org.neo4j.cypher.internal.ast.RevokeDenyType
import org.neo4j.cypher.internal.ast.RevokeGrantType
import org.neo4j.cypher.internal.ast.RevokePrivilege
import org.neo4j.cypher.internal.ast.RevokeRolesFromUsers
import org.neo4j.cypher.internal.ast.RevokeType
import org.neo4j.cypher.internal.ast.ServerManagementAction
import org.neo4j.cypher.internal.ast.SetDatabaseAccessAction
import org.neo4j.cypher.internal.ast.SetOwnPassword
import org.neo4j.cypher.internal.ast.SetPasswordsAction
import org.neo4j.cypher.internal.ast.SetUserHomeDatabaseAction
import org.neo4j.cypher.internal.ast.SetUserStatusAction
import org.neo4j.cypher.internal.ast.ShowAliasAction
import org.neo4j.cypher.internal.ast.ShowAliases
import org.neo4j.cypher.internal.ast.ShowCurrentUser
import org.neo4j.cypher.internal.ast.ShowDatabase
import org.neo4j.cypher.internal.ast.ShowPrivilegeAction
import org.neo4j.cypher.internal.ast.ShowPrivilegeCommands
import org.neo4j.cypher.internal.ast.ShowPrivileges
import org.neo4j.cypher.internal.ast.ShowRoleAction
import org.neo4j.cypher.internal.ast.ShowRoles
import org.neo4j.cypher.internal.ast.ShowServerAction
import org.neo4j.cypher.internal.ast.ShowServers
import org.neo4j.cypher.internal.ast.ShowSupportedPrivilegeCommand
import org.neo4j.cypher.internal.ast.ShowUserAction
import org.neo4j.cypher.internal.ast.ShowUserPrivileges
import org.neo4j.cypher.internal.ast.ShowUsers
import org.neo4j.cypher.internal.ast.ShowUsersPrivileges
import org.neo4j.cypher.internal.ast.SingleNamedDatabaseScope
import org.neo4j.cypher.internal.ast.SingleNamedGraphScope
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.StartDatabase
import org.neo4j.cypher.internal.ast.StartDatabaseAction
import org.neo4j.cypher.internal.ast.StopDatabase
import org.neo4j.cypher.internal.ast.StopDatabaseAction
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.WaitUntilComplete
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.SubqueryExpression
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.DatabaseTypeFilter.Alias
import org.neo4j.cypher.internal.logical.plans.DatabaseTypeFilter.CompositeDatabase
import org.neo4j.cypher.internal.logical.plans.DatabaseTypeFilter.DatabaseOrLocalAlias
import org.neo4j.cypher.internal.logical.plans.DenyLoadAction
import org.neo4j.cypher.internal.logical.plans.GrantLoadAction
import org.neo4j.cypher.internal.planner.spi.AdministrationPlannerName
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.PRIMARY_PROPERTY
import org.neo4j.exceptions.InvalidSemanticsException

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
    "The order of the clauses is fixed and each can only occur once."

  override def phase: CompilationPhase = PIPE_BUILDING

  override def postConditions: Set[StepSequencer.Condition] = Set.empty

  override def process(from: BaseState, context: PlannerContext): LogicalPlanState = {
    implicit val idGen: SequentialIdGen = new SequentialIdGen()
    def planRevokes(
      source: plans.PrivilegePlan,
      revokeType: RevokeType,
      planRevoke: (plans.PrivilegePlan, RevokeType) => plans.PrivilegePlan
    ): plans.PrivilegePlan = revokeType match {
      case t: RevokeBothType =>
        val revokeGrant = planRevoke(source, RevokeGrantType()(t.position))
        planRevoke(revokeGrant, RevokeDenyType()(t.position))
      case t => planRevoke(source, t)
    }

    def getSourceForCreateRole(
      roleName: Either[String, Parameter],
      ifExistsDo: IfExistsDo
    ): plans.SecurityAdministrationLogicalPlan = ifExistsDo match {
      case IfExistsReplace =>
        plans.DropRole(plans.AssertAllowedDbmsActions(None, Seq(DropRoleAction, CreateRoleAction)), roleName)
      case IfExistsDoNothing =>
        plans.DoNothingIfExists(plans.AssertAllowedDbmsActions(CreateRoleAction), "Role", roleName)
      case _ => plans.AssertAllowedDbmsActions(CreateRoleAction)
    }

    def wrapInWait(
      logicalPlan: plans.DatabaseAdministrationLogicalPlan,
      databaseName: DatabaseName,
      waitUntilComplete: WaitUntilComplete
    ): plans.DatabaseAdministrationLogicalPlan = waitUntilComplete match {
      case NoWait => logicalPlan
      case _      => plans.WaitForCompletion(logicalPlan, databaseName, waitUntilComplete)
    }

    def planSystemProcedureCall(resolved: ResolvedCall, returns: Option[Return]): plans.LogicalPlan = {
      val SemanticCheckResult(_, errors) = resolved.semanticCheck.run(SemanticState.clean, SemanticCheckContext.default)
      errors.foreach { error => throw context.cypherExceptionFactory.syntaxException(error.msg, error.position) }
      val signature = resolved.signature
      val checkCredentialsExpired = !signature.allowExpiredCredentials
      plans.SystemProcedureCall(signature.name.toString, resolved, returns, context.params, checkCredentialsExpired)
    }

    // Check for non-administration commands that are allowed on system database, e.g. SHOW PROCEDURES YIELD ...
    // Currently doesn't allow WITH except when it is used instead of YIELD
    def checkClausesAllowedOnSystem(clauses: Seq[Clause]) =
      clauses.exists(_.isInstanceOf[CommandClauseAllowedOnSystem]) && clauses.forall {
        case w: With => w.withType == ParsedAsYield || w.withType == AddedInRewrite
        case c       => c.isInstanceOf[ClauseAllowedOnSystem]
      }

    // Return non-administration commands that are not allowed on system database, e.g. SHOW CONSTRAINTS YIELD ...
    // only needed for better error messages
    def getCommandClausesNotAllowedOnSystem(clauses: Seq[Clause]) =
      clauses.filter(clause => clause.isInstanceOf[CommandClause] && !clause.isInstanceOf[ClauseAllowedOnSystem])

    def assignPrivilegeAction(immutable: Boolean) =
      if (immutable) AssignImmutablePrivilegeAction else AssignPrivilegeAction

    val mapDatabaseScope: PartialFunction[DatabaseScope, plans.PrivilegeCommandScope] = {
      case SingleNamedDatabaseScope(db) => plans.NamedScope(db)
      case AllDatabasesScope()          => plans.AllScope
      case HomeDatabaseScope()          => plans.HomeScope
    }

    val mapGraphScope: PartialFunction[GraphScope, plans.PrivilegeCommandScope] = {
      case SingleNamedGraphScope(graph) => plans.NamedScope(graph)
      case AllGraphsScope()             => plans.AllScope
      case HomeGraphScope()             => plans.HomeScope
    }

    val maybeLogicalPlan: Option[plans.LogicalPlan] = from.statement() match {
      // SHOW USERS
      case su: ShowUsers => Some(plans.ShowUsers(
          plans.AssertAllowedDbmsActions(ShowUserAction),
          su.defaultColumnNames.map(varFor),
          su.yields,
          su.returns
        ))

      // SHOW CURRENT USER
      case su: ShowCurrentUser => Some(plans.ShowCurrentUser(su.defaultColumnNames.map(varFor), su.yields, su.returns))

      // CREATE [OR REPLACE] USER foo [IF NOT EXISTS] WITH [PLAINTEXT | ENCRYPTED] PASSWORD password
      case c @ CreateUser(userName, isEncryptedPassword, initialPassword, userOptions, ifExistsDo) =>
        val source = ifExistsDo match {
          case IfExistsReplace => plans.DropUser(
              plans.AssertNotCurrentUser(
                plans.AssertAllowedDbmsActions(None, Seq(DropUserAction, CreateUserAction)),
                userName,
                "replace",
                "Deleting yourself is not allowed"
              ),
              userName
            )
          case IfExistsDoNothing =>
            plans.DoNothingIfExists(plans.AssertAllowedDbmsActions(CreateUserAction), "User", userName)
          case _ => plans.AssertAllowedDbmsActions(CreateUserAction)
        }
        Some(plans.LogSystemCommand(
          plans.CreateUser(
            source,
            userName,
            isEncryptedPassword,
            initialPassword,
            userOptions.requirePasswordChange.getOrElse(true),
            userOptions.suspended,
            userOptions.homeDatabase
          ),
          prettifier.asString(c)
        ))

      // RENAME USER foo [IF EXISTS] TO bar
      case c @ RenameUser(fromUserName, toUserName, ifExists) =>
        val assertAllowed = plans.AssertAllowedDbmsActions(RenameUserAction)
        val source =
          if (ifExists) plans.DoNothingIfNotExists(assertAllowed, "User", fromUserName, "rename") else assertAllowed
        Some(plans.LogSystemCommand(plans.RenameUser(source, fromUserName, toUserName), prettifier.asString(c)))

      // DROP USER foo [IF EXISTS]
      case c @ DropUser(userName, ifExists) =>
        val assertAllowed = plans.AssertNotCurrentUser(
          plans.AssertAllowedDbmsActions(DropUserAction),
          userName,
          "delete",
          "Deleting yourself is not allowed"
        )
        val source =
          if (ifExists) plans.DoNothingIfNotExists(assertAllowed, "User", userName, "delete")
          else plans.EnsureNodeExists(assertAllowed, "User", userName, labelDescription = "User", action = "delete")
        Some(plans.LogSystemCommand(plans.DropUser(source, userName), prettifier.asString(c)))

      // ALTER USER foo
      case c @ AlterUser(userName, isEncryptedPassword, initialPassword, userOptions, ifExists) =>
        val dbmsActions = Vector(
          (initialPassword, SetPasswordsAction),
          (userOptions.requirePasswordChange, SetPasswordsAction),
          (userOptions.suspended, SetUserStatusAction),
          (userOptions.homeDatabase, SetUserHomeDatabaseAction)
        ).collect { case (Some(_), action) => action }.distinct
        if (dbmsActions.isEmpty) throw new IllegalStateException("Alter user has nothing to do")

        val assertAllowed = plans.AssertAllowedDbmsActions(None, dbmsActions)

        val assertionSubPlan =
          if (userOptions.suspended.isDefined) plans.AssertNotCurrentUser(
            assertAllowed,
            userName,
            "alter",
            "Changing your own activation status is not allowed"
          )
          else assertAllowed
        val ifExistsSubPlan =
          if (ifExists) plans.DoNothingIfNotExists(assertionSubPlan, "User", userName, "alter") else assertionSubPlan
        Some(plans.LogSystemCommand(
          plans.AlterUser(
            ifExistsSubPlan,
            userName,
            isEncryptedPassword,
            initialPassword,
            userOptions.requirePasswordChange,
            userOptions.suspended,
            userOptions.homeDatabase
          ),
          prettifier.asString(c)
        ))

      // ALTER CURRENT USER SET PASSWORD FROM currentPassword TO newPassword
      case c: SetOwnPassword =>
        Some(plans.LogSystemCommand(
          plans.SetOwnPassword(c.newPassword, c.currentPassword),
          prettifier.asString(c)
        ))

      // SHOW [ ALL | POPULATED ] ROLES [WITH USERS]
      case sr: ShowRoles =>
        val assertAllowed =
          if (sr.withUsers) plans.AssertAllowedDbmsActions(None, Seq(ShowRoleAction, ShowUserAction))
          else plans.AssertAllowedDbmsActions(ShowRoleAction)
        Some(plans.ShowRoles(
          assertAllowed,
          withUsers = sr.withUsers,
          showAll = sr.showAll,
          sr.defaultColumnNames.map(varFor),
          sr.yields,
          sr.returns
        ))

      // CREATE [OR REPLACE] ROLE foo [IF NOT EXISTS]
      case c @ CreateRole(roleName, None, ifExistsDo) =>
        val source = getSourceForCreateRole(roleName, ifExistsDo)
        Some(plans.LogSystemCommand(plans.CreateRole(source, roleName), prettifier.asString(c)))

      // CREATE [OR REPLACE] ROLE foo [IF NOT EXISTS] AS COPY OF bar
      case c @ CreateRole(roleName, Some(fromName), ifExistsDo) =>
        val source = getSourceForCreateRole(roleName, ifExistsDo)
        Some(plans.LogSystemCommand(
          plans.CopyRolePrivileges(
            plans.CopyRolePrivileges(
              plans.CreateRole(
                plans.AssertAllRolePrivilegesCanBeCopied(
                  plans.RequireRole(source, fromName),
                  fromName
                ),
                roleName
              ),
              roleName,
              fromName,
              "GRANTED"
            ),
            roleName,
            fromName,
            "DENIED"
          ),
          prettifier.asString(c)
        ))

      // RENAME ROLE foo [IF EXISTS] TO bar
      case c @ RenameRole(fromRoleName, toRoleName, ifExists) =>
        val assertAllowed = plans.AssertAllowedDbmsActions(RenameRoleAction)
        val source =
          if (ifExists) plans.DoNothingIfNotExists(assertAllowed, "Role", fromRoleName, "rename") else assertAllowed
        Some(plans.LogSystemCommand(plans.RenameRole(source, fromRoleName, toRoleName), prettifier.asString(c)))

      // DROP ROLE foo [IF EXISTS]
      case c @ DropRole(roleName, ifExists) =>
        val assertAllowed = plans.AssertAllowedDbmsActions(DropRoleAction)
        val source =
          if (ifExists) plans.DoNothingIfNotExists(assertAllowed, "Role", roleName, "delete")
          else plans.EnsureNodeExists(assertAllowed, "Role", roleName, labelDescription = "Role", action = "delete")
        Some(plans.LogSystemCommand(plans.DropRole(source, roleName), prettifier.asString(c)))

      // GRANT roles TO users
      case c: GrantRolesToUsers =>
        val plan = (for (userName <- c.userNames; roleName <- c.roleNames) yield {
          roleName -> userName
        }).foldLeft(
          plans.AssertAllowedDbmsActions(AssignRoleAction).asInstanceOf[plans.SecurityAdministrationLogicalPlan]
        ) {
          case (source, (roleName, userName)) =>
            val subCommand = c.copy(
              roleNames = List(roleName),
              userNames = List(userName)
            )(c.position)
            plans.GrantRoleToUser(source, roleName, userName, prettifier.asString(subCommand))
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE roles FROM users
      case c: RevokeRolesFromUsers =>
        val plan = (for (userName <- c.userNames; roleName <- c.roleNames) yield {
          roleName -> userName
        }).foldLeft(
          plans.AssertAllowedDbmsActions(RemoveRoleAction).asInstanceOf[plans.SecurityAdministrationLogicalPlan]
        ) {
          case (source, (roleName, userName)) =>
            val subCommand = c.copy(
              roleNames = List(roleName),
              userNames = List(userName)
            )(c.position)
            plans.RevokeRoleFromUser(source, roleName, userName, prettifier.asString(subCommand))
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // GRANT _ ON DBMS TO role
      case c @ GrantPrivilege(DbmsPrivilege(action), immutable, _, qualifiers, roleNames) =>
        val plan = (for (roleName <- roleNames; qualifier <- qualifiers; simpleQualifier <- qualifier.simplify) yield {
          (roleName, simpleQualifier)
        }).foldLeft(
          plans.AssertAllowedDbmsActions(
            plans.AssertDbmsActionIsAssignable(None, action), // this is the privilege being granted
            assignPrivilegeAction(immutable) // this is the action of assigning a privilege
          ).asInstanceOf[plans.PrivilegePlan]
        ) {
          case (source, (roleName, simpleQualifier)) =>
            val subCommand = c.copy(
              qualifier = List(simpleQualifier),
              roleNames = List(roleName)
            )(c.position)
            plans.GrantDbmsAction(source, action, simpleQualifier, roleName, immutable, prettifier.asString(subCommand))
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // DENY _ ON DBMS TO role
      case c @ DenyPrivilege(DbmsPrivilege(action), immutable, _, qualifiers, roleNames) =>
        val plan = (for (roleName <- roleNames; qualifier <- qualifiers; simpleQualifier <- qualifier.simplify) yield {
          (roleName, simpleQualifier)
        }).foldLeft(
          plans.AssertAllowedDbmsActions(
            plans.AssertDbmsActionIsAssignable(None, action), // this is the privilege being granted
            assignPrivilegeAction(immutable) // this is the action of assigning a privilege
          ).asInstanceOf[plans.PrivilegePlan]
        ) {
          case (source, (roleName, simpleQualifier)) =>
            val subCommand = c.copy(
              qualifier = List(simpleQualifier),
              roleNames = List(roleName)
            )(c.position)
            plans.DenyDbmsAction(source, action, simpleQualifier, roleName, immutable, prettifier.asString(subCommand))
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE _ ON DBMS FROM role
      case c @ RevokePrivilege(DbmsPrivilege(action), immutableOnly, _, qualifiers, roleNames, revokeType) =>
        val plan = (for (roleName <- roleNames; qualifier <- qualifiers; simpleQualifier <- qualifier.simplify) yield {
          (roleName, simpleQualifier)
        }).foldLeft(
          plans.AssertAllowedDbmsActions(RemovePrivilegeAction)
            .asInstanceOf[plans.PrivilegePlan]
        ) {
          // recursively build privilege plan using `AssertDbmsPrivilegeCanBeMutated` as the innermost plan.
          // use `planRevokes` to expand plans which are revoking BOTH (i.e. GRANT and DENY).
          case (previous, (roleName, simpleQualifier)) =>
            planRevokes(
              previous,
              revokeType,
              (s, r) => {
                val subCommand =
                  c.copy(qualifier = List(simpleQualifier), roleNames = List(roleName), revokeType = r)(c.position)
                plans.RevokeDbmsAction(
                  planRevokes(
                    s,
                    revokeType,
                    (s, r) => plans.AssertDbmsPrivilegeCanBeMutated(s, action, simpleQualifier, roleName, r.relType)
                  ),
                  action,
                  simpleQualifier,
                  roleName,
                  r.relType,
                  immutableOnly,
                  prettifier.asString(subCommand)
                )
              }
            )
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // GRANT _ ON DATABASE foo TO role
      case c @ GrantPrivilege(privilege @ DatabasePrivilege(action, dbScopes), immutable, _, qualifiers, roleNames) =>
        val plan = (for (
          dbScope <- dbScopes.simplify; roleName <- roleNames; qualifier <- qualifiers;
          simpleQualifiers <- qualifier.simplify
        ) yield {
          (roleName, simpleQualifiers, dbScope, mapDatabaseScope(dbScope))
        }).foldLeft(
          plans.AssertAllowedDbmsActions(assignPrivilegeAction(immutable)).asInstanceOf[plans.PrivilegePlan]
        ) {
          case (source, (role, qualifier, dbScope, runtimeScope)) =>
            val subCommand = c.copy(
              privilege = privilege.copy(scope = dbScope)(privilege.position),
              qualifier = List(qualifier),
              roleNames = List(role)
            )(c.position)
            plans.GrantDatabaseAction(
              source,
              action,
              runtimeScope,
              qualifier,
              role,
              immutable,
              prettifier.asString(subCommand)
            )
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // DENY _ ON DATABASE foo TO role
      case c @ DenyPrivilege(privilege @ DatabasePrivilege(action, dbScopes), immutable, _, qualifiers, roleNames) =>
        val plan = (for (
          dbScope <- dbScopes.simplify; roleName <- roleNames; qualifier <- qualifiers;
          simpleQualifiers <- qualifier.simplify
        ) yield {
          (roleName, simpleQualifiers, dbScope, mapDatabaseScope(dbScope))
        }).foldLeft(
          plans.AssertAllowedDbmsActions(assignPrivilegeAction(immutable)).asInstanceOf[plans.PrivilegePlan]
        ) {
          case (source, (role, qualifier, dbScope, runtimeScope)) =>
            val subCommand = c.copy(
              privilege = privilege.copy(scope = dbScope)(privilege.position),
              qualifier = List(qualifier),
              roleNames = List(role)
            )(c.position)
            plans.DenyDatabaseAction(
              source,
              action,
              runtimeScope,
              qualifier,
              role,
              immutable,
              prettifier.asString(subCommand)
            )
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE _ ON DATABASE foo FROM role
      case c @ RevokePrivilege(
          privilege @ DatabasePrivilege(action, dbScopes),
          immutableOnly,
          _,
          qualifiers,
          roleNames,
          revokeType
        ) =>
        val plan = (for (
          dbScope <- dbScopes.simplify; roleName <- roleNames; qualifier <- qualifiers;
          simpleQualifiers <- qualifier.simplify
        ) yield {
          (roleName, simpleQualifiers, dbScope, mapDatabaseScope(dbScope))
        }).foldLeft(plans.AssertAllowedDbmsActions(RemovePrivilegeAction).asInstanceOf[plans.PrivilegePlan]) {
          case (plan, (role, qualifier, dbScope, runtimeScope)) =>
            planRevokes(
              plan,
              revokeType,
              (s, r) => {
                val subCommand = c.copy(
                  privilege = privilege.copy(scope = dbScope)(privilege.position),
                  qualifier = List(qualifier),
                  roleNames = List(role),
                  revokeType = r
                )(c.position)
                plans.RevokeDatabaseAction(
                  planRevokes(
                    s,
                    revokeType,
                    (s, r) =>
                      plans.AssertDatabasePrivilegeCanBeMutated(s, action, runtimeScope, qualifier, role, r.relType)
                  ),
                  action,
                  runtimeScope,
                  qualifier,
                  role,
                  r.relType,
                  immutableOnly,
                  prettifier.asString(subCommand)
                )
              }
            )
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // GRANT _ ON GRAPH foo _ TO role
      case c @ GrantPrivilege(
          privilege @ GraphPrivilege(action, graphScopes),
          immutable,
          optionalResource,
          qualifiers,
          roleNames
        ) =>
        val resources = optionalResource.getOrElse(NoResource()(InputPosition.NONE))
        val plan = (for (
          graphScope <- graphScopes.simplify; roleName <- roleNames; qualifier <- qualifiers;
          simpleQualifier <- qualifier.simplify; resource <- resources.simplify
        ) yield {
          (roleName, simpleQualifier, resource, graphScope, mapGraphScope(graphScope))
        }).foldLeft(
          plans.AssertAllowedDbmsActions(assignPrivilegeAction(immutable)).asInstanceOf[plans.PrivilegePlan]
        ) {
          case (source, (roleName, simpleQualifier, resource, graphScope, runtimeScope)) =>
            val subCommand = c.copy(
              privilege = privilege.copy(scope = graphScope)(privilege.position),
              qualifier = List(simpleQualifier),
              roleNames = List(roleName)
            )(c.position)
            plans.GrantGraphAction(
              source,
              action,
              resource,
              runtimeScope,
              simpleQualifier,
              roleName,
              immutable,
              prettifier.asString(subCommand)
            )
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // DENY _ ON GRAPH foo _ TO role
      case c @ DenyPrivilege(
          privilege @ GraphPrivilege(action, graphScopes),
          immutable,
          optionalResource,
          qualifiers,
          roleNames
        ) =>
        val resources = optionalResource.getOrElse(NoResource()(InputPosition.NONE))
        val plan = (for (
          graphScope <- graphScopes.simplify; roleName <- roleNames; qualifier <- qualifiers;
          simpleQualifier <- qualifier.simplify; resource <- resources.simplify
        ) yield {
          (roleName, simpleQualifier, resource, graphScope, mapGraphScope(graphScope))
        }).foldLeft(
          plans.AssertAllowedDbmsActions(assignPrivilegeAction(immutable)).asInstanceOf[plans.PrivilegePlan]
        ) {
          case (source, (roleName, simpleQualifier, resource, graphScope, runtimeScope)) =>
            val subCommand = c.copy(
              privilege = privilege.copy(scope = graphScope)(privilege.position),
              qualifier = List(simpleQualifier),
              roleNames = List(roleName)
            )(c.position)
            plans.DenyGraphAction(
              source,
              action,
              resource,
              runtimeScope,
              simpleQualifier,
              roleName,
              immutable,
              prettifier.asString(subCommand)
            )
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE _ ON GRAPH foo _ FROM role
      case c @ RevokePrivilege(
          privilege @ GraphPrivilege(action, graphScopes),
          immutableOnly,
          optionalResource,
          qualifiers,
          roleNames,
          revokeType
        ) =>
        val resources = optionalResource.getOrElse(NoResource()(InputPosition.NONE))
        val plan = (for (
          graphScope <- graphScopes.simplify; roleName <- roleNames; qualifier <- qualifiers;
          simpleQualifiers <- qualifier.simplify; resource <- resources.simplify
        ) yield {
          (roleName, simpleQualifiers, resource, graphScope, mapGraphScope(graphScope))
        }).foldLeft(plans.AssertAllowedDbmsActions(RemovePrivilegeAction).asInstanceOf[plans.PrivilegePlan]) {
          case (source, (roleName, segment, resource, graphScope, runtimeScope)) =>
            planRevokes(
              source,
              revokeType,
              (s, r) => {
                val subCommand = c.copy(
                  privilege = privilege.copy(scope = graphScope)(privilege.position),
                  qualifier = List(segment),
                  resource = if (resource.isInstanceOf[NoResource]) None else Some(resource),
                  roleNames = List(roleName),
                  revokeType = r
                )(c.position)
                plans.RevokeGraphAction(
                  planRevokes(
                    s,
                    revokeType,
                    (s, r) =>
                      plans.AssertGraphPrivilegeCanBeMutated(
                        s,
                        action,
                        resource,
                        runtimeScope,
                        segment,
                        roleName,
                        r.relType
                      )
                  ),
                  action,
                  resource,
                  runtimeScope,
                  segment,
                  roleName,
                  r.relType,
                  immutableOnly,
                  prettifier.asString(subCommand)
                )
              }
            )
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // LOAD privileges
      case g @ GrantPrivilege(
          privilege @ LoadPrivilege(action),
          immutable,
          optionalResource,
          qualifiers,
          roleNames
        ) =>
        val resources = optionalResource.getOrElse(NoResource()(InputPosition.NONE))
        val plan = (for (
          roleName <- roleNames; qualifier <- qualifiers;
          simpleQualifiers <- qualifier.simplify; resource <- resources.simplify
        ) yield {
          (roleName, simpleQualifiers, resource)
        }).foldLeft(
          plans.AssertAllowedDbmsActions(assignPrivilegeAction(immutable)).asInstanceOf[plans.PrivilegePlan]
        ) { case (source, (roleName, qualifier, resource)) =>
          val subCommand = g.copy(
            privilege = privilege,
            qualifier = List(qualifier),
            roleNames = List(roleName)
          )(g.position)
          GrantLoadAction(source, action, resource, qualifier, roleName, immutable, prettifier.asString(subCommand))
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(g)))

      case d @ DenyPrivilege(
          privilege @ LoadPrivilege(action),
          immutable,
          optionalResource,
          qualifiers,
          roleNames
        ) =>
        val resources = optionalResource.getOrElse(NoResource()(InputPosition.NONE))
        val plan = (for (
          roleName <- roleNames; qualifier <- qualifiers;
          simpleQualifiers <- qualifier.simplify; resource <- resources.simplify
        ) yield {
          (roleName, simpleQualifiers, resource)
        }).foldLeft(
          plans.AssertAllowedDbmsActions(assignPrivilegeAction(immutable)).asInstanceOf[plans.PrivilegePlan]
        ) { case (source, (roleName, qualifier, resource)) =>
          val subCommand = d.copy(
            privilege = privilege,
            qualifier = List(qualifier),
            roleNames = List(roleName)
          )(d.position)
          DenyLoadAction(source, action, resource, qualifier, roleName, immutable, prettifier.asString(subCommand))
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(d)))

      case rp @ RevokePrivilege(
          privilege @ LoadPrivilege(action),
          immutableOnly,
          optionalResource,
          qualifiers,
          roleNames,
          revokeType
        ) =>
        val resources = optionalResource.getOrElse(NoResource()(InputPosition.NONE))
        val plan = (for (
          roleName <- roleNames; qualifier <- qualifiers;
          simpleQualifiers <- qualifier.simplify; resource <- resources.simplify
        ) yield {
          (roleName, simpleQualifiers, resource)
        }).foldLeft(
          plans.AssertAllowedDbmsActions(RemovePrivilegeAction).asInstanceOf[plans.PrivilegePlan]
        ) { case (source, (roleName, qualifier, resource)) =>
          planRevokes(
            source,
            revokeType,
            (s, r) => {
              val subCommand = rp.copy(
                privilege = privilege,
                qualifier = List(qualifier),
                roleNames = List(roleName),
                revokeType = r
              )(rp.position)
              plans.RevokeLoadAction(
                planRevokes(
                  s,
                  revokeType,
                  (s, r) =>
                    plans.AssertLoadPrivilegeCanBeMutated(
                      s,
                      action,
                      resource,
                      qualifier,
                      roleName,
                      r.relType
                    )
                ),
                action,
                resource,
                qualifier,
                roleName,
                r.relType,
                immutableOnly,
                prettifier.asString(subCommand)
              )
            }
          )
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(rp)))

      // SHOW [ALL | ROLE role | ROLES role1, role2 | USER [user] | USERS user1, user2] PRIVILEGES
      case sp: ShowPrivileges =>
        val (newScope, source) = sp.scope match {
          // SHOW USER [user] PRIVILEGES
          case scope: ShowUserPrivileges =>
            val user = scope.user
            val source =
              if (user.isDefined)
                Some(plans.AssertAllowedDbmsActionsOrSelf(user.get, Seq(ShowPrivilegeAction, ShowUserAction)))
              else None
            (scope, source)
          // SHOW USERS user1, user2 PRIVILEGES
          case scope: ShowUsersPrivileges =>
            val users = scope.users
            if (users.size > 1)
              (scope, Some(plans.AssertAllowedDbmsActions(None, Seq(ShowPrivilegeAction, ShowUserAction))))
            else (
              ShowUserPrivileges(Some(users.head))(scope.position),
              Some(plans.AssertAllowedDbmsActionsOrSelf(users.head, Seq(ShowPrivilegeAction, ShowUserAction)))
            )
          // SHOW [ALL | ROLE role | ROLES role1, role2] PRIVILEGES
          case scope =>
            (scope, Some(plans.AssertAllowedDbmsActions(ShowPrivilegeAction)))
        }
        Some(plans.ShowPrivileges(source, newScope, sp.defaultColumnNames.map(varFor), sp.yields, sp.returns))

      // SHOW [ALL | ROLE role | ROLES role1, role2 | USER [user] | USERS user1, user2] PRIVILEGES AS [REVOKE] COMMAND
      case sp: ShowPrivilegeCommands =>
        val (newScope, source) = sp.scope match {
          // SHOW USER [user] PRIVILEGES
          case scope: ShowUserPrivileges =>
            val user = scope.user
            val source =
              if (user.isDefined)
                Some(plans.AssertAllowedDbmsActionsOrSelf(user.get, Seq(ShowPrivilegeAction, ShowUserAction)))
              else None
            (scope, source)
          // SHOW USERS user1, user2 PRIVILEGES
          case scope: ShowUsersPrivileges =>
            val users = scope.users
            if (users.size > 1)
              (scope, Some(plans.AssertAllowedDbmsActions(None, Seq(ShowPrivilegeAction, ShowUserAction))))
            else (
              ShowUserPrivileges(Some(users.head))(scope.position),
              Some(plans.AssertAllowedDbmsActionsOrSelf(users.head, Seq(ShowPrivilegeAction, ShowUserAction)))
            )
          // SHOW [ALL | ROLE role | ROLES role1, role2] PRIVILEGES
          case scope =>
            (scope, Some(plans.AssertAllowedDbmsActions(ShowPrivilegeAction)))
        }
        Some(plans.ShowPrivilegeCommands(
          source,
          newScope,
          sp.asRevoke,
          sp.defaultColumnNames.map(varFor),
          sp.yields,
          sp.returns
        ))

      case c: ShowSupportedPrivilegeCommand =>
        Some(plans.ShowSupportedPrivileges(c.defaultColumnNames.map(varFor), c.yields, c.returns))

      // SHOW DATABASES | SHOW DEFAULT DATABASE | SHOW DATABASE foo
      case sd: ShowDatabase =>
        Some(plans.ShowDatabase(
          sd.scope,
          sd.defaultColumns.useAllColumns,
          sd.defaultColumnNames.map(varFor),
          sd.yields,
          sd.returns
        ))

      // CREATE [OR REPLACE] DATABASE foo [IF NOT EXISTS]
      case c @ CreateDatabase(dbName, ifExistsDo, options, waitUntilComplete, topology) =>
        Some(plans.AssertManagementActionNotBlocked(CreateDatabaseAction))
          .map(plans.AssertAllowedDbmsActions(_, CreateDatabaseAction))
          .flatMap(canCreateCheck =>
            ifExistsDo match {
              case IfExistsReplace =>
                Some(plans.AssertCanDropDatabase(
                  canCreateCheck,
                  dbName,
                  DropDatabaseAction
                ))
                  .map(plans.EnsureDatabaseSafeToDelete(_, dbName))
                  .map(plans.DropDatabase(_, dbName, DestroyData, forceComposite = false))
              case IfExistsDoNothing =>
                Some(canCreateCheck)
                  .map(plans.DoNothingIfDatabaseExists(
                    _,
                    dbName
                  ))
              case _ =>
                Some(canCreateCheck)
            }
          ).map(plans.EnsureNameIsNotAmbiguous(_, dbName.asLegacyName, isComposite = false))
          .map(plans.CreateDatabase(_, dbName.asLegacyName, options, ifExistsDo, isComposite = false, topology))
          .map(plans.EnsureValidNumberOfDatabases(_))
          .map(wrapInWait(_, dbName, waitUntilComplete))
          .map(plans.LogSystemCommand(_, prettifier.asString(c)))

      case c @ CreateCompositeDatabase(dbName, ifExistsDo, options, waitUntilComplete) =>
        Some(plans.AssertManagementActionNotBlocked(CreateCompositeDatabaseAction))
          .map(plans.AssertAllowedDbmsActions(_, CreateCompositeDatabaseAction))
          .flatMap(canCreateCheck =>
            ifExistsDo match {
              case IfExistsReplace =>
                Some(plans.AssertCanDropDatabase(
                  canCreateCheck,
                  dbName,
                  DropCompositeDatabaseAction
                ))
                  .map(plans.EnsureDatabaseSafeToDelete(_, dbName))
                  .map(plans.DropDatabase(_, dbName, DestroyData, forceComposite = false))
              case IfExistsDoNothing =>
                Some(canCreateCheck)
                  .map(plans.DoNothingIfDatabaseExists(
                    _,
                    dbName
                  ))
              case _ =>
                Some(canCreateCheck)
            }
          ).map(plans.EnsureNameIsNotAmbiguous(_, dbName.asLegacyName, isComposite = true))
          .map(plans.CreateDatabase(_, dbName.asLegacyName, options, ifExistsDo, isComposite = true, topology = None))
          .map(plans.EnsureValidNumberOfDatabases(_))
          .map(wrapInWait(_, dbName, waitUntilComplete))
          .map(plans.LogSystemCommand(_, prettifier.asString(c)))

      // DROP [COMPOSITE] DATABASE foo [IF EXISTS] [DESTROY | DUMP DATA]
      case c @ DropDatabase(dbName, ifExists, composite, additionalAction, waitUntilComplete) =>
        val action = if (composite) DropCompositeDatabaseAction else DropDatabaseAction
        val assertNotBlockedPlan = plans.AssertManagementActionNotBlocked(action)
        Some(
          if (composite) plans.AssertAllowedDbmsActions(assertNotBlockedPlan, DropCompositeDatabaseAction)
          else plans.AssertCanDropDatabase(assertNotBlockedPlan, dbName, DropDatabaseAction)
        ).map(assertAllowed =>
          if (ifExists)
            plans.DoNothingIfDatabaseNotExists(
              assertAllowed,
              dbName,
              "delete",
              if (composite) CompositeDatabase else DatabaseOrLocalAlias
            )
          else assertAllowed
        )
          .map(plans.EnsureDatabaseSafeToDelete(_, dbName))
          .map(plans.EnsureValidNonSystemDatabase(_, dbName, "delete"))
          .map(plans.DropDatabase(_, dbName, additionalAction, composite))
          .map(wrapInWait(_, dbName, waitUntilComplete))
          .map(plans.LogSystemCommand(_, prettifier.asString(c)))

      // ALTER DATABASE foo [IF EXISTS] [SET ACCESS {READ ONLY | READ WRITE}] [SET TOPOLOGY n PRIMARY [m SECONDARY]] [SET OPTION key value] [REMOVE OPTION key]
      case c @ AlterDatabase(dbName, ifExists, access, topology, options, optionsToRemove, waitUntilComplete) =>
        // For a set of (predicate -> privilege); If the predicate is true, add the privilege to the set of required privileges
        val requiredPrivilegedActions: Seq[DbmsAction] = Seq(
          // ALTER DATABASE foo SET TOPOLOGY requires 'ALTER DATABASE' privileges:
          topology.nonEmpty -> AlterDatabaseAction,
          // ALTER DATABASE foo SET OPTION ... requires 'ALTER DATABASE' privileges:
          (options != NoOptions) -> AlterDatabaseAction,
          // ALTER DATABASE foo REMOVE OPTION ... requires 'ALTER DATABASE' privileges:
          optionsToRemove.nonEmpty -> AlterDatabaseAction,
          // ALTER DATABASE foo SET ACCESS ... requires 'SET DATABASE ACCESS' privileges:
          access.nonEmpty -> SetDatabaseAccessAction
        ).filter(_._1)
          .map(_._2)
          .distinct

        Some(plans.AssertManagementActionNotBlocked(AlterDatabaseAction))
          // AssertManagementActionNotBlocked doesn't know about SetDatabaseAccessAction,
          // pass AlterDatabaseAction no matter what requiredPrivilegedActions we need
          .map(s => plans.AssertAllowedDbmsActions(Some(s), requiredPrivilegedActions))
          .map(assertAllowed =>
            if (ifExists) plans.DoNothingIfDatabaseNotExists(
              assertAllowed,
              dbName,
              "alter",
              DatabaseOrLocalAlias
            )
            else assertAllowed
          )
          .map(plans.EnsureValidNonSystemDatabase(_, dbName, "alter"))
          .map(plans.AlterDatabase(_, dbName, access, topology, options, optionsToRemove))
          .map(wrapInWait(_, dbName, waitUntilComplete))
          .map(plans.LogSystemCommand(_, prettifier.asString(c)))

      // START DATABASE foo
      case c @ StartDatabase(dbName, waitUntilComplete) =>
        val assertAllowed = plans.AssertAllowedDatabaseAction(
          StartDatabaseAction,
          dbName,
          Some(plans.AssertManagementActionNotBlocked(StartDatabaseAction))
        )
        val plan = wrapInWait(plans.StartDatabase(assertAllowed, dbName), dbName, waitUntilComplete)
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // STOP DATABASE foo
      case c @ StopDatabase(dbName, waitUntilComplete) =>
        val assertAllowed = plans.AssertAllowedDatabaseAction(
          StopDatabaseAction,
          dbName,
          Some(plans.AssertManagementActionNotBlocked(StopDatabaseAction))
        )
        val plan = wrapInWait(
          plans.StopDatabase(plans.EnsureValidNonSystemDatabase(assertAllowed, dbName, "stop"), dbName),
          dbName,
          waitUntilComplete
        )
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // CREATE DATABASE ALIAS
      case c @ CreateLocalDatabaseAlias(aliasName, targetName, ifExistsDo, properties) =>
        val (source, replace) = ifExistsDo match {
          case IfExistsReplace => (
              plans.DropDatabaseAlias(
                plans.AssertAllowedDbmsActions(None, Seq(CreateAliasAction, DropAliasAction)),
                aliasName
              ),
              true
            )
          case IfExistsDoNothing => (
              plans.DoNothingIfDatabaseExists(
                plans.AssertAllowedDbmsActions(None, Seq(CreateAliasAction)),
                aliasName
              ),
              false
            )
          case _ => (plans.AssertAllowedDbmsActions(None, Seq(CreateAliasAction)), false)
        }
        val ensureValidDatabase = plans.EnsureValidNonSystemDatabase(source, targetName, "create", Some(aliasName))
        val aliasCommand =
          plans.CreateLocalDatabaseAlias(ensureValidDatabase, aliasName, targetName, properties, replace)
        Some(plans.LogSystemCommand(aliasCommand, prettifier.asString(c)))

      // CREATE DATABASE ALIAS name AT
      case c @ CreateRemoteDatabaseAlias(
          aliasName,
          targetName,
          ifExistsDo,
          url,
          username,
          password,
          driverSettings,
          properties
        ) =>
        val assertAllowed =
          plans.AssertAllowedDbmsActions(
            Some(plans.AssertNotBlockedRemoteAliasManagement()),
            Seq(CreateAliasAction)
          )

        val (source, replace) = ifExistsDo match {
          case IfExistsReplace =>
            (
              plans.DropDatabaseAlias(
                plans.AssertAllowedDbmsActions(Some(assertAllowed), Seq(DropAliasAction)),
                aliasName
              ),
              true
            )
          case IfExistsDoNothing => (plans.DoNothingIfDatabaseExists(assertAllowed, aliasName), false)
          case _                 => (assertAllowed, false)
        }
        val aliasCommand = plans.CreateRemoteDatabaseAlias(
          source,
          aliasName,
          targetName,
          replace,
          url,
          username,
          password,
          driverSettings,
          properties
        )
        Some(plans.LogSystemCommand(aliasCommand, prettifier.asString(c)))

      // DROP DATABASE ALIAS foo [IF EXISTS]
      case c @ DropDatabaseAlias(aliasName, ifExists) =>
        val assertAllowed = plans.AssertAllowedDbmsActions(
          Some(plans.AssertNotBlockedDropAlias(aliasName)),
          Seq(DropAliasAction)
        )
        val source =
          if (ifExists) plans.DoNothingIfDatabaseNotExists(assertAllowed, aliasName, "delete", Alias)
          else plans.EnsureDatabaseNodeExists(
            assertAllowed,
            aliasName,
            node => s"WHERE $node.$PRIMARY_PROPERTY = false",
            "delete"
          )

        Some(plans.LogSystemCommand(plans.DropDatabaseAlias(source, aliasName), prettifier.asString(c)))

      // ALTER DATABASE ALIAS foo (local)
      case c @ AlterLocalDatabaseAlias(aliasName, targetName, ifExists, properties) =>
        val assertAllowedLocal = plans.AssertAllowedDbmsActions(None, Seq(AlterAliasAction))

        val source =
          if (ifExists)
            plans.DoNothingIfDatabaseNotExists(assertAllowedLocal, aliasName, "alter", Alias)
          else plans.EnsureDatabaseNodeExists(
            assertAllowedLocal,
            aliasName,
            node => s"WHERE $node.$PRIMARY_PROPERTY = false",
            "alter"
          )

        val aliasCommand = plans.AlterLocalDatabaseAlias(
          targetName.map(plans.EnsureValidNonSystemDatabase(source, _, "alter", Some(aliasName))),
          aliasName,
          targetName,
          properties
        )
        Some(plans.LogSystemCommand(aliasCommand, prettifier.asString(c)))

      // ALTER DATABASE ALIAS foo (remote)
      case c @ AlterRemoteDatabaseAlias(
          aliasName,
          targetName,
          ifExists,
          url,
          username,
          password,
          driverSettings,
          properties
        ) =>
        val assertAllowedRemote = plans.AssertAllowedDbmsActions(
          Some(plans.AssertNotBlockedRemoteAliasManagement()),
          Seq(AlterAliasAction)
        )

        val source =
          if (ifExists) plans.DoNothingIfDatabaseNotExists(assertAllowedRemote, aliasName, "alter", Alias)
          else plans.EnsureDatabaseNodeExists(
            assertAllowedRemote,
            aliasName,
            node => s"WHERE $node.$PRIMARY_PROPERTY = false",
            "alter"
          )

        val aliasCommand =
          plans.AlterRemoteDatabaseAlias(
            source,
            aliasName,
            targetName,
            url,
            username,
            password,
            driverSettings,
            properties
          )
        Some(plans.LogSystemCommand(aliasCommand, prettifier.asString(c)))

      case showAliases: ShowAliases =>
        Some(plans.AssertAllowedDbmsActions(None, Seq(ShowAliasAction)))
          .map(plans.ShowAliases(
            _,
            showAliases.aliasName,
            showAliases.defaultColumns.useAllColumns,
            showAliases.defaultColumnNames.map(varFor),
            showAliases.yields,
            showAliases.returns
          ))

      case c @ EnableServer(name, options) =>
        val checkBlocked = plans.AssertManagementActionNotBlocked(ServerManagementAction)
        val assertAllowed = plans.AssertAllowedDbmsActions(checkBlocked, ServerManagementAction)
        Some(plans.LogSystemCommand(plans.EnableServer(assertAllowed, name, options), prettifier.asString(c)))

      case c @ AlterServer(name, options) =>
        val checkBlocked = plans.AssertManagementActionNotBlocked(ServerManagementAction)
        val assertAllowed = plans.AssertAllowedDbmsActions(checkBlocked, ServerManagementAction)
        Some(plans.LogSystemCommand(plans.AlterServer(assertAllowed, name, options), prettifier.asString(c)))

      case c @ RenameServer(name, newName) =>
        val checkBlocked = plans.AssertManagementActionNotBlocked(ServerManagementAction)
        val assertAllowed = plans.AssertAllowedDbmsActions(checkBlocked, ServerManagementAction)
        Some(plans.LogSystemCommand(plans.RenameServer(assertAllowed, name, newName), prettifier.asString(c)))

      case c @ DropServer(name) =>
        val checkBlocked = plans.AssertManagementActionNotBlocked(ServerManagementAction)
        val assertAllowed = plans.AssertAllowedDbmsActions(checkBlocked, ServerManagementAction)
        Some(plans.LogSystemCommand(plans.DropServer(assertAllowed, name), prettifier.asString(c)))

      case showServers: ShowServers =>
        val assertAllowed = plans.AssertAllowedDbmsActions(ShowServerAction)
        Some(plans.ShowServers(
          assertAllowed,
          showServers.defaultColumns.useAllColumns,
          showServers.defaultColumnNames.map(varFor),
          showServers.yields,
          showServers.returns
        ))

      case c @ DeallocateServers(dryRun, names) =>
        val checkBlocked = plans.AssertManagementActionNotBlocked(ServerManagementAction)
        val assertAllowed = plans.AssertAllowedDbmsActions(checkBlocked, ServerManagementAction)
        Some(plans.LogSystemCommand(plans.DeallocateServer(assertAllowed, dryRun, names), prettifier.asString(c)))

      case c @ ReallocateDatabases(dryRun) =>
        val checkBlocked = plans.AssertManagementActionNotBlocked(ServerManagementAction)
        Some(plans.LogSystemCommand(
          plans.ReallocateDatabases(plans.AssertAllowedDbmsActions(checkBlocked, ServerManagementAction), dryRun),
          prettifier.asString(c)
        ))

      // Global call: CALL foo.bar.baz("arg1", 2) // only if system procedure is allowed!
      case SingleQuery(Seq(
          resolved @ ResolvedCall(signature, _, _, _, _, _),
          returns @ Return(_, _, _, _, _, _, _)
        )) if signature.systemProcedure =>
        Some(planSystemProcedureCall(resolved, Some(returns)))

      case SingleQuery(Seq(
          UseGraph(Variable(SYSTEM_DATABASE_NAME)),
          resolved @ ResolvedCall(signature, _, _, _, _, _),
          returns @ Return(_, _, _, _, _, _, _)
        )) if signature.systemProcedure =>
        Some(planSystemProcedureCall(resolved, Some(returns)))

      case SingleQuery(Seq(resolved @ ResolvedCall(signature, _, _, _, _, _))) if signature.systemProcedure =>
        Some(planSystemProcedureCall(resolved, None))

      case SingleQuery(
          Seq(UseGraph(Variable(SYSTEM_DATABASE_NAME)), resolved @ ResolvedCall(signature, _, _, _, _, _))
        ) if signature.systemProcedure =>
        Some(planSystemProcedureCall(resolved, None))

      // Non-administration commands that are allowed on system database, e.g. SHOW PROCEDURES YIELD ...
      case q @ SingleQuery(clauses) if checkClausesAllowedOnSystem(clauses) =>
        q.folder.treeExists {
          case p: PatternExpression => throw context.cypherExceptionFactory.syntaxException(
              "You cannot include a pattern expression on a system database",
              p.position
            )
          case p: PatternComprehension => throw context.cypherExceptionFactory.syntaxException(
              "You cannot include a pattern comprehension on a system database",
              p.position
            )
          case c: CollectExpression => throw context.cypherExceptionFactory.syntaxException(
              "You cannot include a COLLECT expression on a system database",
              c.position
            )
          case c: CountExpression => throw context.cypherExceptionFactory.syntaxException(
              "You cannot include a COUNT expression on a system database",
              c.position
            )
          case c: ExistsExpression => throw context.cypherExceptionFactory.syntaxException(
              "You cannot include an EXISTS expression on a system database",
              c.position
            )
          case c: SubqueryExpression => throw context.cypherExceptionFactory.syntaxException(
              "You cannot include a subquery expression on a system database",
              c.position
            )
        }

        Some(plans.AllowedNonAdministrationCommands(q))

      case q =>
        // Check for non-administration commands that are not allowed on system database, e.g. SHOW CONSTRAINTS YIELD ...
        // To get a better error than the procedure error below
        val unsupportedCommandClauses = q match {
          case SingleQuery(clauses) =>
            getCommandClausesNotAllowedOnSystem(clauses).map(_.name).distinct
          case _ => List.empty
        }
        if (unsupportedCommandClauses.nonEmpty) {
          throw new InvalidSemanticsException(
            s"The following commands are not allowed on a system database: ${unsupportedCommandClauses.sorted.mkString(", ")}."
          )
        }

        val unsupportedClauses = q.folder.treeFold(List.empty[String]) {
          case _: UseGraph   => acc => SkipChildren(acc)
          case _: CallClause => acc => SkipChildren(acc)
          case _: Return     => acc => SkipChildren(acc)
          case c: Clause     => acc => SkipChildren(acc :+ c.name)
        }
        if (unsupportedClauses.nonEmpty) {
          throw new InvalidSemanticsException(
            s"The following unsupported clauses were used: ${unsupportedClauses.sorted.mkString(", ")}. \n" + systemDbProcedureRules
          )
        }

        val callCount = q.folder.treeCount {
          case _: CallClause => ()
        }
        if (callCount > 1) {
          throw new InvalidSemanticsException(
            s"The given query uses $callCount CALL clauses (${callCount - 1} too many). \n" + systemDbProcedureRules
          )
        }

        None // this means we will throw the general UnsupportedSystemCommand
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

  override def process(from: BaseState, context: PlannerContext): LogicalPlanState =
    throw new InvalidSemanticsException(s"Not a recognised system command or procedure. " +
      s"This Cypher command can only be executed in a user database: ${from.queryText}")
}
