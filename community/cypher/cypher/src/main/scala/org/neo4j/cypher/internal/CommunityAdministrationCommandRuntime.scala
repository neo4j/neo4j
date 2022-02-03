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
package org.neo4j.cypher.internal

import org.neo4j.common.DependencyResolver
import org.neo4j.cypher.internal.AdministrationCommandRuntime.makeRenameExecutionPlan
import org.neo4j.cypher.internal.AdministrationCommandRuntime.runtimeStringValue
import org.neo4j.cypher.internal.administration.AlterUserExecutionPlanner
import org.neo4j.cypher.internal.administration.CommunityExtendedDatabaseInfoMapper
import org.neo4j.cypher.internal.administration.CreateUserExecutionPlanner
import org.neo4j.cypher.internal.administration.DoNothingExecutionPlanner
import org.neo4j.cypher.internal.administration.DropUserExecutionPlanner
import org.neo4j.cypher.internal.administration.EnsureNodeExistsExecutionPlanner
import org.neo4j.cypher.internal.administration.SetOwnPasswordExecutionPlanner
import org.neo4j.cypher.internal.administration.ShowDatabasesExecutionPlanner
import org.neo4j.cypher.internal.administration.ShowUsersExecutionPlanner
import org.neo4j.cypher.internal.administration.SystemProcedureCallPlanner
import org.neo4j.cypher.internal.ast.AdministrationAction
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.DbmsAction
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.StartDatabaseAction
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.StopDatabaseAction
import org.neo4j.cypher.internal.ast.TerminateTransactionsClause
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.logical.plans.AllowedNonAdministrationCommands
import org.neo4j.cypher.internal.logical.plans.AlterUser
import org.neo4j.cypher.internal.logical.plans.AssertAllowedDatabaseAction
import org.neo4j.cypher.internal.logical.plans.AssertAllowedDbmsActions
import org.neo4j.cypher.internal.logical.plans.AssertAllowedDbmsActionsOrSelf
import org.neo4j.cypher.internal.logical.plans.AssertNotCurrentUser
import org.neo4j.cypher.internal.logical.plans.CreateUser
import org.neo4j.cypher.internal.logical.plans.DoNothingIfDatabaseExists
import org.neo4j.cypher.internal.logical.plans.DoNothingIfDatabaseNotExists
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExists
import org.neo4j.cypher.internal.logical.plans.DoNothingIfNotExists
import org.neo4j.cypher.internal.logical.plans.DropUser
import org.neo4j.cypher.internal.logical.plans.EnsureNodeExists
import org.neo4j.cypher.internal.logical.plans.LogSystemCommand
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NameValidator
import org.neo4j.cypher.internal.logical.plans.RenameUser
import org.neo4j.cypher.internal.logical.plans.SetOwnPassword
import org.neo4j.cypher.internal.logical.plans.ShowCurrentUser
import org.neo4j.cypher.internal.logical.plans.ShowDatabase
import org.neo4j.cypher.internal.logical.plans.ShowUsers
import org.neo4j.cypher.internal.logical.plans.SystemProcedureCall
import org.neo4j.cypher.internal.procs.ActionMapper
import org.neo4j.cypher.internal.procs.AuthorizationPredicateExecutionPlan
import org.neo4j.cypher.internal.procs.PredicateExecutionPlan
import org.neo4j.cypher.internal.procs.SystemCommandExecutionPlan
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.rendering.QueryRenderer
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.internal.kernel.api.security.AccessMode
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.AdminActionOnResource.DatabaseScope
import org.neo4j.internal.kernel.api.security.PermissionState
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.kernel.api.security.Segment
import org.neo4j.kernel.database.DefaultDatabaseResolver
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue

import scala.annotation.tailrec

/**
 * This runtime takes on queries that work on the system database, such as multidatabase and security administration commands.
 * The planning requirements for these are much simpler than normal Cypher commands, and as such the runtime stack is also different.
 */
case class CommunityAdministrationCommandRuntime(normalExecutionEngine: ExecutionEngine, resolver: DependencyResolver,
                                                 extraLogicalToExecutable: PartialFunction[LogicalPlan, AdministrationCommandRuntimeContext => ExecutionPlan] = CommunityAdministrationCommandRuntime.emptyLogicalToExecutable
                                                ) extends AdministrationCommandRuntime {
  override def name: String = "community administration-commands"

  private lazy val securityAuthorizationHandler = new SecurityAuthorizationHandler(resolver.resolveDependency(classOf[AbstractSecurityLog]))

  private lazy val defaultDatabaseResolver = resolver.resolveDependency(classOf[DefaultDatabaseResolver])

  def throwCantCompile(unknownPlan: LogicalPlan): Nothing = {
    throw new CantCompileQueryException(
      s"Plan is not a recognized database administration command in community edition: ${unknownPlan.getClass.getSimpleName}")
  }

  override def compileToExecutable(state: LogicalQuery, context: RuntimeContext): ExecutionPlan = {
    // Either the logical plan is a command that the partial function logicalToExecutable provides/understands OR we throw an error
    logicalToExecutable.applyOrElse(state.logicalPlan, throwCantCompile).apply(AdministrationCommandRuntimeContext(context))
  }

  // When the community commands are run within enterprise, this allows the enterprise commands to be chained
  private def fullLogicalToExecutable = extraLogicalToExecutable orElse logicalToExecutable

  val checkShowUserPrivilegesText: String = "Try executing SHOW USER PRIVILEGES to determine the missing or denied privileges. " +
    "In case of missing privileges, they need to be granted (See GRANT). In case of denied privileges, they need to be revoked (See REVOKE) and granted."

  def prettifyActionName (actions: AdministrationAction*) : String = {
    actions.map{
      case StartDatabaseAction => "START DATABASE"
      case StopDatabaseAction => "STOP DATABASE"
      case a => a.name
    }.sorted.mkString(" and/or ")
  }

  private def adminActionErrorMessage(permissionState: PermissionState, actions: Seq[AdministrationAction]) = permissionState match {
    case PermissionState.EXPLICIT_DENY => "Permission denied for " + prettifyActionName(actions:_*) + ". " + checkShowUserPrivilegesText
    case PermissionState.NOT_GRANTED => "Permission has not been granted for " + prettifyActionName(actions:_*) + ". " + checkShowUserPrivilegesText
    case PermissionState.EXPLICIT_GRANT => ""
  }

  private def checkAdminRightsForDBMSOrSelf (user: Either[String, Parameter], actions: Seq[DbmsAction]): AdministrationCommandRuntimeContext => ExecutionPlan = _ =>  {
    def checkActions(securityContext: SecurityContext): Seq[(DbmsAction, PermissionState)] = actions.map( action =>
      (action, securityContext.allowsAdminAction(new AdminActionOnResource(ActionMapper.asKernelAction(action), DatabaseScope.ALL, Segment.ALL)))
    )
    AuthorizationPredicateExecutionPlan(securityAuthorizationHandler, (params, securityContext) => {
      if (securityContext.subject().hasUsername(runtimeStringValue(user, params))) Seq((null, PermissionState.EXPLICIT_GRANT))
      else checkActions(securityContext)
    }, violationMessage = adminActionErrorMessage)
  }

  def logicalToExecutable: PartialFunction[LogicalPlan, AdministrationCommandRuntimeContext => ExecutionPlan] = {
    // Check Admin Rights for DBMS commands
    case AssertAllowedDbmsActions(maybeSource, actions) => context =>
      AuthorizationPredicateExecutionPlan(
        securityAuthorizationHandler,
        (_, securityContext) => actions.map { action =>
          (action, securityContext.allowsAdminAction(new AdminActionOnResource(ActionMapper.asKernelAction(action), DatabaseScope.ALL, Segment.ALL)))
        },
        violationMessage = adminActionErrorMessage,
        source = maybeSource match {
          case Some(source) => Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
          case _            => None
        }
      )

    // Check Admin Rights for DBMS commands or self
    case AssertAllowedDbmsActionsOrSelf(user, actions) => context => checkAdminRightsForDBMSOrSelf(user, actions)(context)

    // Check that the specified user is not the logged in user (eg. for some CREATE/DROP/ALTER USER commands)
    case AssertNotCurrentUser(source, userName, verb, violationMessage) => context =>
      new PredicateExecutionPlan((params, sc) => !sc.subject().hasUsername(runtimeStringValue(userName, params)),
        onViolation = (_, sc) => new InvalidArgumentException(s"Failed to $verb the specified user '${sc.subject().executingUser()}': $violationMessage."),
        source = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
      )

    // Check Admin Rights for some Database commands
    case AssertAllowedDatabaseAction(action, database, maybeSource) => context =>
      AuthorizationPredicateExecutionPlan(securityAuthorizationHandler, (params, securityContext) =>
        Seq((action, securityContext.allowsAdminAction(
          new AdminActionOnResource(ActionMapper.asKernelAction(action), new DatabaseScope(runtimeStringValue(database, params)), Segment.ALL)))),
        violationMessage = adminActionErrorMessage,
        source = maybeSource match {
          case Some(source) => Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
          case _ => None
        }
      )

    // SHOW USERS
    case ShowUsers(source, symbols, yields, returns) => context =>
      val sourcePlan: Option[ExecutionPlan] = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
      ShowUsersExecutionPlanner(normalExecutionEngine, securityAuthorizationHandler).planShowUsers(symbols, yields, returns, sourcePlan)

    // SHOW CURRENT USER
    case ShowCurrentUser(symbols, yields, returns) => _ =>
      ShowUsersExecutionPlanner(normalExecutionEngine, securityAuthorizationHandler).planShowCurrentUser(symbols, yields, returns)

    // CREATE [OR REPLACE] USER foo [IF NOT EXISTS] SET [PLAINTEXT | ENCRYPTED] PASSWORD 'password'
    // CREATE [OR REPLACE] USER foo [IF NOT EXISTS] SET [PLAINTEXT | ENCRYPTED] PASSWORD $password
    case createUser: CreateUser => context =>
      val sourcePlan: Option[ExecutionPlan] = Some(fullLogicalToExecutable.applyOrElse(createUser.source, throwCantCompile).apply(context))
      CreateUserExecutionPlanner(normalExecutionEngine, securityAuthorizationHandler).planCreateUser(createUser, sourcePlan)

    // RENAME USER
    case RenameUser(source, fromUserName, toUserName) => context =>
      val sourcePlan: Option[ExecutionPlan] = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
      makeRenameExecutionPlan("User", fromUserName, toUserName,
        params => {
          val toName = runtimeStringValue(toUserName, params)
          NameValidator.assertValidUsername(toName)
        }
      )(sourcePlan, normalExecutionEngine, securityAuthorizationHandler)

    // ALTER USER foo [SET [PLAINTEXT | ENCRYPTED] PASSWORD pw] [CHANGE [NOT] REQUIRED]
    case alterUser: AlterUser => context =>
      val sourcePlan: Option[ExecutionPlan] = Some(fullLogicalToExecutable.applyOrElse(alterUser.source, throwCantCompile).apply(context))
      AlterUserExecutionPlanner(normalExecutionEngine, securityAuthorizationHandler).planAlterUser(alterUser, sourcePlan)

    // DROP USER foo [IF EXISTS]
    case DropUser(source, userName) => context =>
      val sourcePlan: Option[ExecutionPlan] = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
      DropUserExecutionPlanner(normalExecutionEngine, securityAuthorizationHandler).planDropUser(userName, sourcePlan)

    // ALTER CURRENT USER SET PASSWORD FROM 'currentPassword' TO 'newPassword'
    // ALTER CURRENT USER SET PASSWORD FROM 'currentPassword' TO $newPassword
    // ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO 'newPassword'
    // ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword
    case SetOwnPassword(newPassword, currentPassword) => _ =>
      SetOwnPasswordExecutionPlanner(normalExecutionEngine, securityAuthorizationHandler).planSetOwnPassword(newPassword, currentPassword)

    // SHOW DATABASES | SHOW DEFAULT DATABASE | SHOW HOME DATABASE | SHOW DATABASE foo
    case ShowDatabase(scope, verbose, symbols, yields, returns) => _ =>
      ShowDatabasesExecutionPlanner(resolver, defaultDatabaseResolver, normalExecutionEngine, securityAuthorizationHandler)(CommunityExtendedDatabaseInfoMapper)
      .planShowDatabases(scope, verbose, symbols, yields, returns)

    case DoNothingIfNotExists(source, label, name, operation, valueMapper) => context =>
      val sourcePlan: Option[ExecutionPlan] = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
      DoNothingExecutionPlanner(normalExecutionEngine, securityAuthorizationHandler).planDoNothingIfNotExists(label, name, valueMapper, operation, sourcePlan)

    case DoNothingIfExists(source, label, name, valueMapper) => context =>
      val sourcePlan: Option[ExecutionPlan] = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
      DoNothingExecutionPlanner(normalExecutionEngine, securityAuthorizationHandler).planDoNothingIfExists(label, name, valueMapper, sourcePlan)

    case DoNothingIfDatabaseNotExists(source, name, operation, valueMapper) => context =>
      val sourcePlan: Option[ExecutionPlan] = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
      DoNothingExecutionPlanner(normalExecutionEngine, securityAuthorizationHandler).planDoNothingIfDatabaseNotExists(name, valueMapper, operation, sourcePlan)

    case DoNothingIfDatabaseExists(source, name, valueMapper) => context =>
      val sourcePlan: Option[ExecutionPlan] = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
      DoNothingExecutionPlanner(normalExecutionEngine, securityAuthorizationHandler).planDoNothingIfDatabaseExists(name, valueMapper, sourcePlan)

    // Ensure that the role or user exists before being dropped
    case EnsureNodeExists(source, label, name, valueMapper, extraFilter, labelDescription, action) => context =>
      val sourcePlan: Option[ExecutionPlan] = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
      EnsureNodeExistsExecutionPlanner(normalExecutionEngine, securityAuthorizationHandler)
        .planEnsureNodeExists(label, name, valueMapper, extraFilter, labelDescription, action, sourcePlan)

    // SUPPORT PROCEDURES (need to be cleared before here)
    case SystemProcedureCall(_, call, returns, _, checkCredentialsExpired) => _ =>
      SystemProcedureCallPlanner(normalExecutionEngine, securityAuthorizationHandler).planSystemProcedureCall(call, returns, checkCredentialsExpired)

    // Non-administration commands that are allowed on system database, e.g. SHOW PROCEDURES
    case AllowedNonAdministrationCommands(statement) => _ =>
      val updatedStatement = statement.rewrite(new Rewriter {
        override def apply(v: AnyRef): AnyRef = instance(v)

        private val instance = bottomUp(Rewriter.lift {
          case s@SingleQuery(clauses) => s.copy(clauses = rewriteClauses(clauses.toList, List()))(s.position)
        })

        @tailrec
        // Remove internally added YIELD and RETURN for TERMINATE TRANSACTION
        // as the command does not allow those clauses and otherwise will fail to parse
        // No risk of throwing away any WHERE clauses, as those are not allowed either
        private def rewriteClauses(clauses: List[Clause], rewrittenClause: List[Clause]): List[Clause] = clauses match {
          // Terminate transaction command with only a YIELD (don't think this case exists but to be on the safe side)
          case (terminateClause: TerminateTransactionsClause) :: (_: Yield) :: Nil =>
            rewrittenClause :+ terminateClause
          // Terminate transaction command with YIELD and RETURN
          case (terminateClause: TerminateTransactionsClause) :: (_: Yield) :: (_: Return) :: Nil =>
            rewrittenClause :+ terminateClause
          case c :: cs => rewriteClauses(cs, rewrittenClause :+ c)
          case Nil => rewrittenClause
        }
      }).asInstanceOf[Statement]

      SystemCommandExecutionPlan("AllowedNonAdministrationCommand",
        normalExecutionEngine,
        securityAuthorizationHandler,
        QueryRenderer.render(updatedStatement),
        MapValue.EMPTY,
        // If we have a non admin command executing in the system database, forbid it to make reads / writes
        // from the system graph. This is to prevent queries such as SHOW PROCEDURES YIELD * RETURN ()--()
        // from leaking nodes from the system graph: the ()--() would return empty results
        modeConverter = s => s.withMode(AccessMode.Static.ACCESS)
      )

    // Ignore the log command in community
    case LogSystemCommand(source, _) => context =>
      fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context)
  }

  override def isApplicableAdministrationCommand(logicalPlanArg: LogicalPlan): Boolean = {
    val logicalPlan = logicalPlanArg match {
      // Ignore the log command in community
      case LogSystemCommand(source, _) => source
      case plan => plan
    }
    logicalToExecutable.isDefinedAt(logicalPlan)
  }
}

object DatabaseStatus extends Enumeration {
  type Status = TextValue

  val Online: TextValue = Values.utf8Value("online")
  val Offline: TextValue = Values.utf8Value("offline")
}

object CommunityAdministrationCommandRuntime {
  def emptyLogicalToExecutable: PartialFunction[LogicalPlan, AdministrationCommandRuntimeContext => ExecutionPlan] =
    new PartialFunction[LogicalPlan, AdministrationCommandRuntimeContext => ExecutionPlan] {
      override def isDefinedAt(x: LogicalPlan): Boolean = false

      override def apply(v1: LogicalPlan): AdministrationCommandRuntimeContext => ExecutionPlan = ???
    }
}
