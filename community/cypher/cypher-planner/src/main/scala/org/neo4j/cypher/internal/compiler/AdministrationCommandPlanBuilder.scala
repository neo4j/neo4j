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
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AddedInRewriteProcCall
import org.neo4j.cypher.internal.ast.AddedInRewriteShowCommands
import org.neo4j.cypher.internal.ast.AllDatabasesScope
import org.neo4j.cypher.internal.ast.AllGraphsScope
import org.neo4j.cypher.internal.ast.AlterAliasAction
import org.neo4j.cypher.internal.ast.AlterCompositeDatabaseAction
import org.neo4j.cypher.internal.ast.AlterDatabase
import org.neo4j.cypher.internal.ast.AlterDatabaseOptionsAction
import org.neo4j.cypher.internal.ast.AlterDatabaseTopologyAction
import org.neo4j.cypher.internal.ast.AlterLocalDatabaseAlias
import org.neo4j.cypher.internal.ast.AlterRemoteDatabaseAlias
import org.neo4j.cypher.internal.ast.AlterServer
import org.neo4j.cypher.internal.ast.AlterUser
import org.neo4j.cypher.internal.ast.AssignPrivilegeAction
import org.neo4j.cypher.internal.ast.AssignRoleAction
import org.neo4j.cypher.internal.ast.CallClause
import org.neo4j.cypher.internal.ast.CascadeAliases
import org.neo4j.cypher.internal.ast.CatalogName
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
import org.neo4j.cypher.internal.ast.DatabaseAndDbmsAction
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.DatabasePrivilege
import org.neo4j.cypher.internal.ast.DatabaseScope
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
import org.neo4j.cypher.internal.ast.GraphDirectReference
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.GraphScope
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.IfExistsDo
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.LoadPrivilege
import org.neo4j.cypher.internal.ast.NextStatement
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
import org.neo4j.cypher.internal.ast.Restrict
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.RevokeBothType
import org.neo4j.cypher.internal.ast.RevokeDenyType
import org.neo4j.cypher.internal.ast.RevokeGrantType
import org.neo4j.cypher.internal.ast.RevokePrivilege
import org.neo4j.cypher.internal.ast.RevokeRolesFromUsers
import org.neo4j.cypher.internal.ast.RevokeType
import org.neo4j.cypher.internal.ast.ServerManagementAction
import org.neo4j.cypher.internal.ast.SetAuthAction
import org.neo4j.cypher.internal.ast.SetDatabaseAccessAction
import org.neo4j.cypher.internal.ast.SetDatabaseDefaultLanguageAction
import org.neo4j.cypher.internal.ast.SetOwnPassword
import org.neo4j.cypher.internal.ast.SetPasswordsAction
import org.neo4j.cypher.internal.ast.SetUserHomeDatabaseAction
import org.neo4j.cypher.internal.ast.SetUserStatusAction
import org.neo4j.cypher.internal.ast.ShardDefinition
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
import org.neo4j.cypher.internal.ast.TopLevelBraces
import org.neo4j.cypher.internal.ast.Union
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.WaitUntilComplete
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.SubqueryExpression
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.AssertRoleCanBeDropped
import org.neo4j.cypher.internal.logical.plans.AssertRoleCanBeRenamed
import org.neo4j.cypher.internal.logical.plans.DatabaseTypeFilter.Alias
import org.neo4j.cypher.internal.logical.plans.DatabaseTypeFilter.CompositeDatabase
import org.neo4j.cypher.internal.logical.plans.DatabaseTypeFilter.DatabaseOrLocalAlias
import org.neo4j.cypher.internal.logical.plans.DenyLoadAction
import org.neo4j.cypher.internal.logical.plans.GrantLoadAction
import org.neo4j.cypher.internal.logical.plans.PrivilegePlan
import org.neo4j.cypher.internal.planner.spi.AdministrationPlannerName
import org.neo4j.cypher.internal.util.EmptyErrorMessageProvider
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.PRIMARY_PROPERTY
import org.neo4j.exceptions.InvalidSemanticsException
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.graphdb.security.AuthorizationViolationException

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

  // Automatically convert AST form into Planner form
  implicit private val expressionToEitherStringParam: PartialFunction[Expression, Either[String, Parameter]] = {
    case StringLiteral(str) => Left(str)
    case p: Parameter       => Right(p)
  }

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
      createImmutable: Boolean,
      ifExistsDo: IfExistsDo
    ): plans.SecurityAdministrationLogicalPlan = {
      val canCreateImmutableCheck =
        if (createImmutable)
          Some(plans.AssertSecurityDisabled(() => AuthorizationViolationException.creatingImmutableRoles()))
        else None
      ifExistsDo match {
        case IfExistsReplace =>
          plans.DropRole(
            plans.AssertRoleCanBeReplaced(
              plans.AssertAllowedDbmsActions(canCreateImmutableCheck, Seq(DropRoleAction, CreateRoleAction)),
              roleName
            ),
            roleName
          )
        case IfExistsDoNothing =>
          plans.DoNothingIfExists(
            plans.AssertAllowedDbmsActions(canCreateImmutableCheck, Seq(CreateRoleAction)),
            s"CREATE${Prettifier.maybeImmutable(createImmutable)} ROLE",
            plans.RoleEntity,
            roleName
          )
        case _ => plans.AssertAllowedDbmsActions(canCreateImmutableCheck, Seq(CreateRoleAction))
      }
    }

    def wrapInWait(
      logicalPlan: plans.DatabaseAdministrationLogicalPlan,
      databaseName: DatabaseName,
      waitUntilComplete: WaitUntilComplete
    ): plans.DatabaseAdministrationLogicalPlan = waitUntilComplete match {
      case _: NoWait => logicalPlan
      case _         => plans.WaitForCompletion(logicalPlan, databaseName, waitUntilComplete)
    }

    def planSystemProcedureCall(
      language: CypherVersion,
      resolved: ResolvedCall,
      returns: Option[Return]
    ): plans.LogicalPlan = {
      val semanticContext = SemanticCheckContext(language, EmptyErrorMessageProvider)
      resolved.semanticCheck.run(SemanticState.clean, semanticContext).errors.foreach { error =>
        if (error.gqlStatusObject != null) {
          throw context.cypherExceptionFactory.syntaxException(error.gqlStatusObject, error.msg, error.position)
        }
        // This case can be removed once all semantic errors have been ported to GQLSTATUS
        throw context.cypherExceptionFactory.syntaxException(GqlHelper.getDefaultObject, error.msg, error.position)
      }
      val signature = resolved.signature
      val checkCredentialsExpired = !signature.allowExpiredCredentials
      plans.SystemProcedureCall(signature.name.toString, resolved, returns, context.params, checkCredentialsExpired)
    }

    // Check for non-administration commands that are allowed on system database, e.g. SHOW PROCEDURES YIELD ...
    // Currently doesn't allow WITH except when it is used instead of YIELD
    def checkClausesAllowedOnSystem(clauses: Seq[Clause]) =
      clauses.exists(_.isInstanceOf[CommandClauseAllowedOnSystem]) && clauses.forall {
        case w: With => w.withType == ParsedAsYield || w.withType == AddedInRewriteShowCommands
        case c       => c.isInstanceOf[ClauseAllowedOnSystem]
      }

    // Return non-administration commands that are not allowed on system database, e.g. SHOW CONSTRAINTS YIELD ...
    // only needed for better error messages
    def getCommandClausesNotAllowedOnSystem(clauses: Seq[Clause]) =
      clauses.filter(clause => clause.isInstanceOf[CommandClause] && !clause.isInstanceOf[ClauseAllowedOnSystem])

    def checkCanMutateImmutablePlan(
      immutable: Boolean,
      onViolation: () => AuthorizationViolationException
    ): Option[PrivilegePlan] =
      if (immutable) Some(plans.AssertSecurityDisabled(onViolation)) else None

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
          su.withAuth,
          su.defaultColumnNames.map(varFor),
          su.yields,
          su.returns
        ))

      // SHOW CURRENT USER
      case su: ShowCurrentUser => Some(plans.ShowCurrentUser(su.defaultColumnNames.map(varFor), su.yields, su.returns))

      // CREATE [OR REPLACE] USER foo [IF NOT EXISTS] WITH [PLAINTEXT | ENCRYPTED] PASSWORD password
      case c @ CreateUser(userName, userOptions, ifExistsDo, externalAuths, nativeAuth) =>
        val source = ifExistsDo match {
          case IfExistsReplace => plans.DropUser(
              plans.AssertNotCurrentUser(
                plans.AssertAllowedDbmsActions(None, Seq(DropUserAction, CreateUserAction)),
                userName,
                "replace",
                "Deleting yourself is not allowed",
                ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N99)
                  .build()
              ),
              userName
            )
          case IfExistsDoNothing =>
            plans.DoNothingIfExists(
              plans.AssertAllowedDbmsActions(CreateUserAction),
              "CREATE USER",
              plans.UserEntity,
              userName
            )
          case _ => plans.AssertAllowedDbmsActions(CreateUserAction)
        }
        Some(plans.LogSystemCommand(
          plans.CreateUser(
            source,
            userName,
            userOptions.suspended,
            userOptions.homeDatabase,
            externalAuths,
            nativeAuth
          ),
          prettifier.asString(c)
        ))

      // RENAME USER foo [IF EXISTS] TO bar
      case c @ RenameUser(fromUserName, toUserName, ifExists) =>
        val assertAllowed = plans.AssertAllowedDbmsActions(RenameUserAction)
        val source =
          if (ifExists)
            plans.DoNothingIfNotExists(assertAllowed, "RENAME USER", plans.UserEntity, fromUserName, "rename")
          else assertAllowed
        Some(plans.LogSystemCommand(plans.RenameUser(source, fromUserName, toUserName), prettifier.asString(c)))

      // DROP USER foo [IF EXISTS]
      case c @ DropUser(userName, ifExists) =>
        val assertAllowed = plans.AssertNotCurrentUser(
          plans.AssertAllowedDbmsActions(DropUserAction),
          userName,
          "delete",
          "Deleting yourself is not allowed",
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N99)
            .build()
        )
        val source =
          if (ifExists) plans.DoNothingIfNotExists(assertAllowed, "DROP USER", plans.UserEntity, userName, "delete")
          else plans.EnsureNodeExists(
            assertAllowed,
            "DROP USER",
            plans.UserEntity,
            userName,
            labelDescription = "User",
            action = "delete"
          )
        Some(plans.LogSystemCommand(plans.DropUser(source, userName), prettifier.asString(c)))

      // ALTER USER foo
      case c @ AlterUser(userName, userOptions, ifExists, externalAuths, nativeAuth, removeAuth) =>
        val dbmsActions = Vector(
          (nativeAuth.nonEmpty, SetPasswordsAction),
          (externalAuths.nonEmpty || removeAuth.nonEmpty, SetAuthAction),
          (userOptions.suspended.nonEmpty, SetUserStatusAction),
          (userOptions.homeDatabase.nonEmpty, SetUserHomeDatabaseAction)
        ).collect { case (true, action) => action }

        if (dbmsActions.isEmpty) throw new IllegalStateException("Alter user has nothing to do")

        val assertAllowed = plans.AssertAllowedDbmsActions(None, dbmsActions)

        val assertionSubPlan =
          if (userOptions.suspended.isDefined) plans.AssertNotCurrentUser(
            assertAllowed,
            userName,
            "alter",
            "Changing your own activation status is not allowed",
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N98)
              .build()
          )
          else assertAllowed
        val ifExistsSubPlan =
          if (ifExists) plans.DoNothingIfNotExists(assertionSubPlan, "ALTER USER", plans.UserEntity, userName, "alter")
          else assertionSubPlan
        Some(plans.LogSystemCommand(
          plans.AlterUser(
            ifExistsSubPlan,
            userName,
            userOptions.suspended,
            userOptions.homeDatabase,
            nativeAuth,
            externalAuths,
            removeAuth
          ),
          prettifier.asString(c)
        ))

      // ALTER CURRENT USER SET PASSWORD FROM currentPassword TO newPassword
      case c: SetOwnPassword =>
        Some(plans.LogSystemCommand(
          plans.SetOwnPassword(plans.CheckNativeAuthentication(), c.newPassword, c.currentPassword),
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
      case c @ CreateRole(roleName, immutable, None, ifExistsDo) =>
        val source = getSourceForCreateRole(roleName, immutable, ifExistsDo)
        Some(plans.LogSystemCommand(plans.CreateRole(source, roleName, immutable), prettifier.asString(c)))

      // CREATE [OR REPLACE] ROLE foo [IF NOT EXISTS] AS COPY OF bar
      case c @ CreateRole(roleName, immutable, Some(fromName), ifExistsDo) =>
        val source = getSourceForCreateRole(roleName, immutable, ifExistsDo)
        Some(plans.LogSystemCommand(
          plans.CopyRolePrivileges(
            plans.CopyRolePrivileges(
              plans.CreateRole(
                if (immutable)
                  plans.AssertRolePrivilegesAreAllImmutable(plans.RequireRole(source, fromName), fromName)
                else
                  plans.AssertRolePrivilegesCanAllBeCopied(plans.RequireRole(source, fromName), fromName),
                roleName,
                immutable
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
          if (ifExists)
            plans.DoNothingIfNotExists(assertAllowed, "RENAME ROLE", plans.RoleEntity, fromRoleName, "rename")
          else assertAllowed
        Some(plans.LogSystemCommand(
          plans.RenameRole(AssertRoleCanBeRenamed(source, fromRoleName), fromRoleName, toRoleName),
          prettifier.asString(c)
        ))

      // DROP ROLE foo [IF EXISTS]
      case c @ DropRole(roleName, ifExists) =>
        val assertAllowed = plans.AssertAllowedDbmsActions(DropRoleAction)
        val source =
          if (ifExists) plans.DoNothingIfNotExists(assertAllowed, "DROP ROLE", plans.RoleEntity, roleName, "delete")
          else plans.EnsureNodeExists(
            assertAllowed,
            "DROP ROLE",
            plans.RoleEntity,
            roleName,
            labelDescription = "Role",
            action = "delete"
          )
        Some(plans.LogSystemCommand(
          plans.DropRole(AssertRoleCanBeDropped(source, roleName), roleName),
          prettifier.asString(c)
        ))

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
            plans.GrantRoleToUser(
              source,
              roleName,
              userName,
              prettifier.asString(subCommand)
            )
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
            plans.RevokeRoleFromUser(
              source,
              roleName,
              userName,
              prettifier.asString(subCommand)
            )
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // GRANT _ ON DBMS TO role
      case c @ GrantPrivilege(DbmsPrivilege(action), immutable, _, qualifiers, roleNames) =>
        val plan = (for (roleName <- roleNames; qualifier <- qualifiers; simpleQualifier <- qualifier.simplify) yield {
          (roleName, simpleQualifier)
        }).foldLeft(
          plans.AssertAllowedDbmsActions(
            checkCanMutateImmutablePlan(immutable, () => AuthorizationViolationException.grantingImmutablePrivileges()),
            Seq(AssignPrivilegeAction)
          ).asInstanceOf[plans.PrivilegePlan]
        ) {
          case (source, (roleName, simpleQualifier)) =>
            val subCommand = c.copy(
              qualifier = List(simpleQualifier),
              roleNames = List(roleName)
            )(c.position)
            val roleCheck =
              if (immutable) source else plans.AssertMutablePrivilegesCanBeAssignedToRole(source, roleName)
            plans.GrantDbmsAction(
              roleCheck,
              action,
              simpleQualifier,
              roleName,
              immutable,
              prettifier.asString(subCommand)
            )
        }
        Some(plans.LogSystemCommand(plan, prettifier.asString(c)))

      // DENY _ ON DBMS TO role
      case c @ DenyPrivilege(DbmsPrivilege(action), immutable, _, qualifiers, roleNames) =>
        val plan = (for (roleName <- roleNames; qualifier <- qualifiers; simpleQualifier <- qualifier.simplify) yield {
          (roleName, simpleQualifier)
        }).foldLeft(
          plans.AssertAllowedDbmsActions(
            checkCanMutateImmutablePlan(immutable, () => AuthorizationViolationException.denyingImmutablePrivileges()),
            Seq(AssignPrivilegeAction)
          ).asInstanceOf[plans.PrivilegePlan]
        ) {
          case (source, (roleName, simpleQualifier)) =>
            val subCommand = c.copy(
              qualifier = List(simpleQualifier),
              roleNames = List(roleName)
            )(c.position)
            val roleCheck =
              if (immutable) source else plans.AssertMutablePrivilegesCanBeAssignedToRole(source, roleName)
            plans.DenyDbmsAction(
              roleCheck,
              action,
              simpleQualifier,
              roleName,
              immutable,
              prettifier.asString(subCommand)
            )
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
          plans.AssertAllowedDbmsActions(
            checkCanMutateImmutablePlan(immutable, () => AuthorizationViolationException.grantingImmutablePrivileges()),
            Seq(AssignPrivilegeAction)
          ).asInstanceOf[plans.PrivilegePlan]
        ) {
          case (source, (role, qualifier, dbScope, runtimeScope)) =>
            val subCommand = c.copy(
              privilege = privilege.copy(scope = dbScope)(privilege.position),
              qualifier = List(qualifier),
              roleNames = List(role)
            )(c.position)
            val roleCheck = if (immutable) source else plans.AssertMutablePrivilegesCanBeAssignedToRole(source, role)
            plans.GrantDatabaseAction(
              roleCheck,
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
          plans.AssertAllowedDbmsActions(
            checkCanMutateImmutablePlan(immutable, () => AuthorizationViolationException.denyingImmutablePrivileges()),
            Seq(AssignPrivilegeAction)
          ).asInstanceOf[plans.PrivilegePlan]
        ) {
          case (source, (role, qualifier, dbScope, runtimeScope)) =>
            val subCommand = c.copy(
              privilege = privilege.copy(scope = dbScope)(privilege.position),
              qualifier = List(qualifier),
              roleNames = List(role)
            )(c.position)
            val roleCheck = if (immutable) source else plans.AssertMutablePrivilegesCanBeAssignedToRole(source, role)
            plans.DenyDatabaseAction(
              roleCheck,
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
          plans.AssertAllowedDbmsActions(
            checkCanMutateImmutablePlan(immutable, () => AuthorizationViolationException.grantingImmutablePrivileges()),
            Seq(AssignPrivilegeAction)
          ).asInstanceOf[plans.PrivilegePlan]
        ) {
          case (source, (roleName, simpleQualifier, resource, graphScope, runtimeScope)) =>
            val subCommand = c.copy(
              privilege = privilege.copy(scope = graphScope)(privilege.position),
              qualifier = List(simpleQualifier),
              roleNames = List(roleName)
            )(c.position)
            val roleCheck =
              if (immutable) source else plans.AssertMutablePrivilegesCanBeAssignedToRole(source, roleName)
            plans.GrantGraphAction(
              roleCheck,
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
          plans.AssertAllowedDbmsActions(
            checkCanMutateImmutablePlan(immutable, () => AuthorizationViolationException.denyingImmutablePrivileges()),
            Seq(AssignPrivilegeAction)
          ).asInstanceOf[plans.PrivilegePlan]
        ) {
          case (source, (roleName, simpleQualifier, resource, graphScope, runtimeScope)) =>
            val subCommand = c.copy(
              privilege = privilege.copy(scope = graphScope)(privilege.position),
              qualifier = List(simpleQualifier),
              roleNames = List(roleName)
            )(c.position)
            val roleCheck =
              if (immutable) source else plans.AssertMutablePrivilegesCanBeAssignedToRole(source, roleName)
            plans.DenyGraphAction(
              roleCheck,
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
          plans.AssertAllowedDbmsActions(
            checkCanMutateImmutablePlan(immutable, () => AuthorizationViolationException.grantingImmutablePrivileges()),
            Seq(AssignPrivilegeAction)
          ).asInstanceOf[plans.PrivilegePlan]
        ) { case (source, (roleName, qualifier, resource)) =>
          val subCommand = g.copy(
            privilege = privilege,
            qualifier = List(qualifier),
            roleNames = List(roleName)
          )(g.position)
          val roleCheck = if (immutable) source else plans.AssertMutablePrivilegesCanBeAssignedToRole(source, roleName)
          GrantLoadAction(roleCheck, action, resource, qualifier, roleName, immutable, prettifier.asString(subCommand))
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
          plans.AssertAllowedDbmsActions(
            checkCanMutateImmutablePlan(immutable, () => AuthorizationViolationException.denyingImmutablePrivileges()),
            Seq(AssignPrivilegeAction)
          ).asInstanceOf[plans.PrivilegePlan]
        ) { case (source, (roleName, qualifier, resource)) =>
          val subCommand = d.copy(
            privilege = privilege,
            qualifier = List(qualifier),
            roleNames = List(roleName)
          )(d.position)
          val roleCheck = if (immutable) source else plans.AssertMutablePrivilegesCanBeAssignedToRole(source, roleName)
          DenyLoadAction(roleCheck, action, resource, qualifier, roleName, immutable, prettifier.asString(subCommand))
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

      // CREATE [OR REPLACE] DATABASE foo [IF NOT EXISTS] (with shards)
      case c @ CreateDatabase(dbName, ifExistsDo, options, waitUntilComplete, None, cypherVersion, Some(shardDef)) =>
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
                  .map(plans.AssertNotShardedDatabase(_, dbName, "DROP DATABASE", "delete"))
                  .map(plans.EnsureDatabaseSafeToDelete(_, dbName, Restrict))
                  .map(plans.DropDatabase(_, dbName, DestroyData, forceComposite = false, Restrict))
              case IfExistsDoNothing =>
                Some(canCreateCheck)
                  .map(plans.DoNothingIfDatabaseExists(
                    _,
                    "CREATE DATABASE",
                    dbName
                  ))
              case _ =>
                Some(canCreateCheck)
            }
          ).map(plans.EnsureNameIsNotAmbiguous(_, dbName.asLegacyName, isComposite = false))
          .map(plans.CreateShardedDatabase(_, dbName.asLegacyName, options, ifExistsDo, shardDef, cypherVersion))
          .map(plans.EnsureValidNumberOfDatabases(_))
          .map(wrapInWait(_, dbName, waitUntilComplete))
          .map(plans.LogSystemCommand(_, prettifier.asString(c)))

      // CREATE [OR REPLACE] DATABASE foo [IF NOT EXISTS]
      case c @ CreateDatabase(dbName, ifExistsDo, options, waitUntilComplete, topology, cypherVersion, None) =>
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
                  .map(plans.AssertNotShardedDatabase(_, dbName, "DROP DATABASE", "delete"))
                  .map(plans.EnsureDatabaseSafeToDelete(_, dbName, Restrict))
                  .map(plans.DropDatabase(_, dbName, DestroyData, forceComposite = false, Restrict))
              case IfExistsDoNothing =>
                Some(canCreateCheck)
                  .map(plans.DoNothingIfDatabaseExists(
                    _,
                    "CREATE DATABASE",
                    dbName
                  ))
              case _ =>
                Some(canCreateCheck)
            }
          ).map(plans.EnsureNameIsNotAmbiguous(_, dbName.asLegacyName, isComposite = false))
          .map(plans.CreateDatabase(
            _,
            dbName.asLegacyName,
            options,
            ifExistsDo,
            isComposite = false,
            topology,
            cypherVersion
          ))
          .map(plans.EnsureValidNumberOfDatabases(_))
          .map(wrapInWait(_, dbName, waitUntilComplete))
          .map(plans.LogSystemCommand(_, prettifier.asString(c)))

      case c @ CreateCompositeDatabase(dbName, ifExistsDo, options, waitUntilComplete, cypherVersion) =>
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
                  .map(plans.EnsureDatabaseSafeToDelete(_, dbName, Restrict))
                  .map(plans.DropDatabase(_, dbName, DestroyData, forceComposite = false, Restrict))
              case IfExistsDoNothing =>
                Some(canCreateCheck)
                  .map(plans.DoNothingIfDatabaseExists(
                    _,
                    "CREATE COMPOSITE DATABASE",
                    dbName
                  ))
              case _ =>
                Some(canCreateCheck)
            }
          ).map(plans.EnsureNameIsNotAmbiguous(_, dbName.asLegacyName, isComposite = true))
          .map(plans.CreateDatabase(
            _,
            dbName.asLegacyName,
            options,
            ifExistsDo,
            isComposite = true,
            topology = None,
            cypherVersion
          ))
          .map(plans.EnsureValidNumberOfDatabases(_))
          .map(wrapInWait(_, dbName, waitUntilComplete))
          .map(plans.LogSystemCommand(_, prettifier.asString(c)))

      // DROP [COMPOSITE] DATABASE foo [IF EXISTS] [DESTROY | DUMP DATA]
      case c @ DropDatabase(dbName, ifExists, composite, aliasAction, additionalAction, waitUntilComplete) =>
        val action = if (composite) DropCompositeDatabaseAction else DropDatabaseAction
        val assertNotBlockedPlan = plans.AssertManagementActionNotBlocked(action)
        Some(
          if (composite) plans.AssertAllowedDbmsActions(assertNotBlockedPlan, DropCompositeDatabaseAction)
          else plans.AssertCanDropDatabase(assertNotBlockedPlan, dbName, DropDatabaseAction)
        ).map(assertAllowed =>
          if (aliasAction == CascadeAliases)
            plans.AssertAllowedDbmsActions(
              Some(assertAllowed),
              Seq(DropAliasAction)
            )
          else assertAllowed
        ).map(assertAllowed =>
          if (ifExists)
            plans.DoNothingIfDatabaseNotExists(
              assertAllowed,
              "DROP DATABASE",
              dbName,
              "delete",
              if (composite) CompositeDatabase else DatabaseOrLocalAlias
            )
          else assertAllowed
        )
          .map(plans.EnsureDatabaseSafeToDelete(_, dbName, aliasAction))
          .map(plans.EnsureValidNonSystemDatabase(_, "DROP DATABASE", dbName, "delete"))
          .map(plans.AssertNotInvalidActionOnShard(_, dbName, "DROP DATABASE", "delete"))
          .map(plans.DropDatabase(_, dbName, additionalAction, composite, aliasAction))
          .map(wrapInWait(_, dbName, waitUntilComplete))
          .map(plans.LogSystemCommand(_, prettifier.asString(c)))

      // ALTER DATABASE foo [IF EXISTS] ...
      case c @ AlterDatabase(
          dbName,
          ifExists,
          access,
          topology,
          options,
          optionsToRemove,
          waitUntilComplete,
          cypherVersion,
          shardDefinition,
          replicas
        ) =>
        val cypherVersionForPrivileges = context.cypherVersion == CypherVersion.Cypher5
        // Composite databases currently don't have any sub-privileges, just ALTER privilege
        val requiredPrivilegeActionsForCompositeDatabases: Seq[DatabaseAndDbmsAction] =
          Seq(AlterCompositeDatabaseAction(cypherVersionForPrivileges))
        // For a set of (predicate -> privilege); If the predicate is true, add the privilege to the set of required privileges
        val requiredPrivilegedActionsForDatabases: Seq[DatabaseAndDbmsAction] = Seq(
          // ALTER DATABASE foo SET TOPOLOGY requires internal AlterDatabaseTopology privilege which can be granted by 'ALTER DATABASE':
          topology.nonEmpty -> AlterDatabaseTopologyAction,
          replicas.nonEmpty -> AlterDatabaseTopologyAction,
          shardDefinition.nonEmpty -> AlterDatabaseTopologyAction,
          // ALTER DATABASE foo SET OPTION ... requires internal AlterDatabaseOptions privilege which can be granted by 'ALTER DATABASE':
          (options != NoOptions) -> AlterDatabaseOptionsAction,
          // ALTER DATABASE foo REMOVE OPTION ... requires internal AlterDatabaseOptions privilege which can be granted by 'ALTER DATABASE':
          optionsToRemove.nonEmpty -> AlterDatabaseOptionsAction,
          // ALTER DATABASE foo SET ACCESS ... requires 'SET DATABASE ACCESS' privileges:
          access.nonEmpty -> SetDatabaseAccessAction,
          // ALTER DATABASE foo SET DEFAULT LANGUAGE ... requires 'SET DATABASE DEFAULT LANGUAGE' privileges:
          cypherVersion.nonEmpty -> SetDatabaseDefaultLanguageAction
        ).filter(_._1)
          .map(_._2(cypherVersionForPrivileges))
          .distinct
        val alterIsValidOnSystem =
          requiredPrivilegedActionsForDatabases == Seq(SetDatabaseDefaultLanguageAction(cypherVersionForPrivileges))

        Some(plans.AssertManagementActionNotBlocked(requiredPrivilegedActionsForDatabases))
          .map(s =>
            plans.AssertCanAlterDatabase(
              s,
              dbName,
              requiredPrivilegeActionsForCompositeDatabases,
              requiredPrivilegedActionsForDatabases
            )
          )
          .map(assertAllowed =>
            if (ifExists) plans.DoNothingIfDatabaseNotExists(
              assertAllowed,
              "ALTER DATABASE",
              dbName,
              "alter",
              DatabaseOrLocalAlias
            )
            else assertAllowed
          )
          .map(source =>
            if (!alterIsValidOnSystem) plans.EnsureValidNonSystemDatabase(source, "ALTER DATABASE", dbName, "alter")
            else source
          )
          .map(s =>
            if (access.nonEmpty) {
              val action = "SET ACCESS"
              plans.AssertNotPropertyShard(
                plans.AssertNotGraphShard(s, dbName, action, "alter"),
                dbName,
                action,
                "alter"
              )
            } else s
          )
          .map(s =>
            if (cypherVersion.nonEmpty) {
              val action = "SET DEFAULT LANGUAGE"
              plans.AssertNotPropertyShard(
                plans.AssertNotGraphShard(s, dbName, action, "alter"),
                dbName,
                action,
                "alter"
              )
            } else s
          )
          .map(s =>
            if (options != NoOptions) {
              val action = "SET OPTION"
              plans.AssertNotGraphShard(
                plans.AssertNotPropertyShard(s, dbName, action, "alter"),
                dbName,
                action,
                "alter"
              )
            } else s
          )
          .map(s =>
            if (optionsToRemove.nonEmpty) {
              val action = "REMOVE OPTION"
              plans.AssertNotGraphShard(
                plans.AssertNotPropertyShard(s, dbName, action, "alter"),
                dbName,
                action,
                "alter"
              )
            } else s
          )
          .map(s =>
            if (topology.nonEmpty) {
              // allowed on graph shard and non-sharded database
              val action = "SET TOPOLOGY ... PRIMARY / SECONDARY"
              val ps =
                plans.AssertNotVirtualSpd(
                  plans.AssertNotPropertyShard(s, dbName, action, "alter"),
                  dbName,
                  action,
                  "alter"
                )
              plans.AlterDatabase(ps, dbName, access, topology, options, cypherVersion, optionsToRemove, None)
            } else if (shardDefinition.nonEmpty) {
              // allowed on sharded database
              val action = shardDefinition match {
                case Some(ShardDefinition(_, Some(_), _)) => "SET GRAPH SHARD"
                case _                                    => "SET PROPERTY SHARD"
              }
              val ps = plans.AssertNotPropertyShard(
                plans.AssertNotGraphShard(plans.AssertNotStandard(s, dbName, action, "alter"), dbName, action, "alter"),
                dbName,
                action,
                "alter"
              )
              plans.AlterShardedDatabase(ps, dbName, access, options, cypherVersion, shardDefinition)
            } else if (replicas.nonEmpty) {
              // allowed on property shard
              val action = "SET TOPOLOGY ... REPLICAS"
              val ps = plans.AssertNotVirtualSpd(
                plans.AssertNotGraphShard(plans.AssertNotStandard(s, dbName, action, "alter"), dbName, action, "alter"),
                dbName,
                action,
                "alter"
              )
              plans.AlterDatabase(ps, dbName, access, None, options, cypherVersion, optionsToRemove, replicas)
            } else plans.AlterDatabase(s, dbName, access, None, options, cypherVersion, optionsToRemove, None)
          )
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
          plans.StopDatabase(
            plans.EnsureValidNonSystemDatabase(assertAllowed, "STOP DATABASE", dbName, "stop"),
            dbName
          ),
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
                "CREATE DATABASE ALIAS",
                aliasName
              ),
              false
            )
          case _ => (plans.AssertAllowedDbmsActions(None, Seq(CreateAliasAction)), false)
        }
        val ensureValidDatabase =
          plans.EnsureValidNonSystemDatabase(source, "CREATE DATABASE ALIAS", targetName, "create", Some(aliasName))
        val assertNotInvalidActionOnShard =
          plans.AssertNotInvalidActionOnShard(ensureValidDatabase, targetName, "CREATE ALIAS", "create")
        val aliasCommand =
          plans.CreateLocalDatabaseAlias(assertNotInvalidActionOnShard, aliasName, targetName, properties, replace)
        Some(plans.LogSystemCommand(aliasCommand, prettifier.asString(c)))

      // CREATE DATABASE ALIAS name AT
      case c @ CreateRemoteDatabaseAlias(
          aliasName,
          targetName,
          ifExistsDo,
          url,
          remoteAliasCredentials,
          driverSettings,
          properties,
          defaultLanguage
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
          case IfExistsDoNothing =>
            (plans.DoNothingIfDatabaseExists(assertAllowed, "CREATE DATABASE ALIAS", aliasName), false)
          case _ => (assertAllowed, false)
        }
        val aliasCommand = plans.CreateRemoteDatabaseAlias(
          source,
          aliasName,
          targetName,
          replace,
          url,
          remoteAliasCredentials,
          driverSettings,
          properties,
          defaultLanguage
        )
        Some(plans.LogSystemCommand(aliasCommand, prettifier.asString(c)))

      // DROP DATABASE ALIAS foo [IF EXISTS]
      case c @ DropDatabaseAlias(aliasName, ifExists) =>
        val assertAllowed = plans.AssertAllowedDbmsActions(
          Some(plans.AssertNotBlockedDropAlias(aliasName)),
          Seq(DropAliasAction)
        )
        val source =
          if (ifExists)
            plans.DoNothingIfDatabaseNotExists(assertAllowed, "DROP DATABASE ALIAS", aliasName, "delete", Alias)
          else plans.EnsureDatabaseNodeExists(
            assertAllowed,
            "DROP DATABASE ALIAS",
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
            plans.DoNothingIfDatabaseNotExists(assertAllowedLocal, "ALTER DATABASE ALIAS", aliasName, "alter", Alias)
          else plans.EnsureDatabaseNodeExists(
            assertAllowedLocal,
            "ALTER DATABASE ALIAS",
            aliasName,
            node => s"WHERE $node.$PRIMARY_PROPERTY = false",
            "alter"
          )

        val aliasCommand = plans.AlterLocalDatabaseAlias(
          targetName.map(plans.EnsureValidNonSystemDatabase(
            source,
            "ALTER DATABASE ALIAS",
            _,
            "alter",
            Some(aliasName)
          )).getOrElse(source),
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
          properties,
          defaultLanguage
        ) =>
        val assertAllowedRemote = plans.AssertAllowedDbmsActions(
          Some(plans.AssertNotBlockedRemoteAliasManagement()),
          Seq(AlterAliasAction)
        )

        val source =
          if (ifExists)
            plans.DoNothingIfDatabaseNotExists(assertAllowedRemote, "ALTER DATABASE ALIAS", aliasName, "alter", Alias)
          else plans.EnsureDatabaseNodeExists(
            assertAllowedRemote,
            "ALTER DATABASE ALIAS",
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
            username.map(expressionToEitherStringParam),
            password,
            driverSettings,
            properties,
            defaultLanguage
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
          resolved @ ResolvedCall(signature, _, _, _, _, _, _),
          returns @ Return(_, _, _, _, _, _, _, _)
        )) if signature.systemProcedure =>
        Some(planSystemProcedureCall(context.cypherVersion, resolved, Some(returns)))

      case SingleQuery(Seq(
          UseGraph(GraphDirectReference(CatalogName(List(SYSTEM_DATABASE_NAME), _))),
          resolved @ ResolvedCall(signature, _, _, _, _, _, _),
          returns @ Return(_, _, _, _, _, _, _, _)
        )) if signature.systemProcedure =>
        Some(planSystemProcedureCall(context.cypherVersion, resolved, Some(returns)))

      case SingleQuery(Seq(resolved @ ResolvedCall(signature, _, _, _, _, _, _))) if signature.systemProcedure =>
        Some(planSystemProcedureCall(context.cypherVersion, resolved, None))

      case SingleQuery(
          Seq(
            UseGraph(GraphDirectReference(CatalogName(List(SYSTEM_DATABASE_NAME), _))),
            resolved @ ResolvedCall(signature, _, _, _, _, _, _)
          )
        ) if signature.systemProcedure =>
        Some(planSystemProcedureCall(context.cypherVersion, resolved, None))

      // Non-administration commands that are allowed on system database, e.g. SHOW PROCEDURES YIELD ...
      case q @ SingleQuery(clauses) if checkClausesAllowedOnSystem(clauses) =>
        q.folder.treeExists {
          case p: PatternExpression => throw context.cypherExceptionFactory.unsupportedRequestOnSystemDatabaseException(
              "Pattern expression",
              "You cannot include a pattern expression on a system database",
              p.position
            )
          case p: PatternComprehension =>
            throw context.cypherExceptionFactory.unsupportedRequestOnSystemDatabaseException(
              "Pattern comprehension",
              "You cannot include a pattern comprehension on a system database",
              p.position
            )
          case c: CollectExpression => throw context.cypherExceptionFactory.unsupportedRequestOnSystemDatabaseException(
              "COLLECT expression",
              "You cannot include a COLLECT expression on a system database",
              c.position
            )
          case c: CountExpression => throw context.cypherExceptionFactory.unsupportedRequestOnSystemDatabaseException(
              "COUNT expression",
              "You cannot include a COUNT expression on a system database",
              c.position
            )
          case c: ExistsExpression => throw context.cypherExceptionFactory.unsupportedRequestOnSystemDatabaseException(
              "EXISTS expression",
              "You cannot include an EXISTS expression on a system database",
              c.position
            )
          case c: SubqueryExpression =>
            throw context.cypherExceptionFactory.unsupportedRequestOnSystemDatabaseException(
              "Subquery expression",
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
          throw InvalidSemanticsException.unsupportedRequestOnSystemDatabase(
            unsupportedCommandClauses.sorted.mkString(", "),
            s"The following commands are not allowed on a system database: ${unsupportedCommandClauses.sorted.mkString(", ")}.",
            false
          )
        }

        val unsupportedClauses = q.folder.treeFold(List.empty[String]) {
          case _: Union         => acc => TraverseChildren(acc :+ "UNION")
          case _: NextStatement =>
            // might have been re-written away but let's keep the check for if not
            acc => TraverseChildren(acc :+ "NEXT")
          case _: TopLevelBraces =>
            // might have been re-written away but let's keep the check for if not
            acc => TraverseChildren(acc :+ TopLevelBraces.name)
          case _: UseGraph   => acc => SkipChildren(acc)
          case _: CallClause => acc => SkipChildren(acc)
          case _: Return     => acc => SkipChildren(acc)
          case w: With if w.withType == AddedInRewriteProcCall =>
            acc =>
              val name = w.where.map(_ => "WHERE").getOrElse(w.name)
              SkipChildren(acc :+ name)
          case c: Clause => acc => SkipChildren(acc :+ c.name)
        }.distinct
        if (unsupportedClauses.nonEmpty) {
          throw InvalidSemanticsException.unsupportedRequestOnSystemDatabase(
            unsupportedClauses.sorted.mkString(", "),
            s"The following unsupported clauses were used: ${unsupportedClauses.sorted.mkString(", ")}. \n" + systemDbProcedureRules,
            true
          )
        }

        val callCount = q.folder.treeCount {
          case _: CallClause => ()
        }
        if (callCount > 1) {
          throw InvalidSemanticsException.unsupportedRequestOnSystemDatabase(
            "More than one CALL clause",
            s"The given query uses $callCount CALL clauses (${callCount - 1} too many). \n" + systemDbProcedureRules,
            true
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

  override def process(from: BaseState, context: PlannerContext): LogicalPlanState = {
    throw InvalidSemanticsException.unsupportedRequestOnSystemDatabase(
      from.queryText,
      s"Not a recognised system command or procedure. " +
        s"This Cypher command can only be executed in a user database: ${from.queryText}",
      false
    )
  }
}
