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
package org.neo4j.cypher.internal

import org.neo4j.common.DependencyResolver
import org.neo4j.configuration.Config
import org.neo4j.cypher.internal.AdministrationCommandRuntime.checkNamespaceExists
import org.neo4j.cypher.internal.AdministrationCommandRuntime.getDatabaseNameFields
import org.neo4j.cypher.internal.AdministrationCommandRuntime.internalKey
import org.neo4j.cypher.internal.AdministrationCommandRuntime.makeRenameExecutionPlan
import org.neo4j.cypher.internal.AdministrationCommandRuntime.runtimeStringValue
import org.neo4j.cypher.internal.administration.AlterUserExecutionPlanner
import org.neo4j.cypher.internal.administration.CommunityAlterDatabaseExecutionPlanner
import org.neo4j.cypher.internal.administration.CreateUserExecutionPlanner
import org.neo4j.cypher.internal.administration.DoNothingExecutionPlanner
import org.neo4j.cypher.internal.administration.DropUserExecutionPlanner
import org.neo4j.cypher.internal.administration.EnsureNodeExistsExecutionPlanner
import org.neo4j.cypher.internal.administration.SetOwnPasswordExecutionPlanner
import org.neo4j.cypher.internal.administration.ShowDatabasesExecutionPlanner
import org.neo4j.cypher.internal.administration.ShowUsersExecutionPlanner
import org.neo4j.cypher.internal.administration.SystemProcedureCallPlanner
import org.neo4j.cypher.internal.ast.AdministrationAction
import org.neo4j.cypher.internal.ast.DbmsAction
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.StartDatabaseAction
import org.neo4j.cypher.internal.ast.StopDatabaseAction
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.logical.plans.AllowedNonAdministrationCommands
import org.neo4j.cypher.internal.logical.plans.AlterDatabase
import org.neo4j.cypher.internal.logical.plans.AlterUser
import org.neo4j.cypher.internal.logical.plans.AssertAllowedDatabaseAction
import org.neo4j.cypher.internal.logical.plans.AssertAllowedDbmsActions
import org.neo4j.cypher.internal.logical.plans.AssertAllowedDbmsActionsOrSelf
import org.neo4j.cypher.internal.logical.plans.AssertCanAlterDatabase
import org.neo4j.cypher.internal.logical.plans.AssertManagementActionNotBlocked
import org.neo4j.cypher.internal.logical.plans.AssertNotCurrentUser
import org.neo4j.cypher.internal.logical.plans.AssertNotGraphShard
import org.neo4j.cypher.internal.logical.plans.AssertNotPropertyShard
import org.neo4j.cypher.internal.logical.plans.AssertNotVirtualSpd
import org.neo4j.cypher.internal.logical.plans.CheckNativeAuthentication
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
import org.neo4j.cypher.internal.logical.plans.PrivilegePlan
import org.neo4j.cypher.internal.logical.plans.RenameUser
import org.neo4j.cypher.internal.logical.plans.SetOwnPassword
import org.neo4j.cypher.internal.logical.plans.ShowCurrentUser
import org.neo4j.cypher.internal.logical.plans.ShowDatabase
import org.neo4j.cypher.internal.logical.plans.ShowUsers
import org.neo4j.cypher.internal.logical.plans.SystemProcedureCall
import org.neo4j.cypher.internal.procs.ActionMapper
import org.neo4j.cypher.internal.procs.AuthorizationAndPredicateExecutionPlan
import org.neo4j.cypher.internal.procs.Continue
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.PredicateExecutionPlan
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.SystemCommandExecutionPlan
import org.neo4j.cypher.internal.procs.ThrowException
import org.neo4j.cypher.internal.procs.UpdatingSystemCommandExecutionPlan
import org.neo4j.cypher.rendering.QueryRenderer
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_NAME_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.GRAPH_SHARD
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.NAMESPACE_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.PROPERTY_SHARD
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.SPD
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.TARGETS
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.exceptions.InvalidSemanticsException
import org.neo4j.exceptions.Neo4jException
import org.neo4j.gqlstatus.PrivilegeGqlCodeEntity
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.AdminActionOnResource.DatabaseScope
import org.neo4j.internal.kernel.api.security.PermissionState
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.kernel.api.security.Segment
import org.neo4j.internal.kernel.api.security.StaticAccessMode
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode
import org.neo4j.kernel.impl.query.TransactionalContext.DatabaseMode
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

/**
 * This runtime takes on queries that work on the system database, such as multidatabase and security administration commands.
 * The planning requirements for these are much simpler than normal Cypher commands, and as such the runtime stack is also different.
 */
case class CommunityAdministrationCommandRuntime(
  normalExecutionEngine: ExecutionEngine,
  resolver: DependencyResolver,
  extraLogicalToExecutable: PartialFunction[LogicalPlan, AdministrationCommandRuntimeContext => ExecutionPlan] =
    CommunityAdministrationCommandRuntime.emptyLogicalToExecutable
) extends AdministrationCommandRuntime {
  override def name: String = "community administration-commands"

  private lazy val securityAuthorizationHandler =
    new SecurityAuthorizationHandler(resolver.resolveDependency(classOf[AbstractSecurityLog]))
  private val config: Config = resolver.resolveDependency(classOf[Config])

  private lazy val userSecurity: UserSecurityGraphComponent =
    resolver.resolveDependency(classOf[UserSecurityGraphComponent])

  def throwCantCompile(unknownPlan: LogicalPlan): Nothing = {
    throw CantCompileQueryException.planUnsupportedInCommunityEdition(unknownPlan.getClass.getSimpleName)
  }

  override def compileToExecutable(
    state: LogicalQuery,
    context: RuntimeContext,
    databaseMode: DatabaseMode
  ): ExecutionPlan = {
    // Either the logical plan is a command that the partial function logicalToExecutable provides/understands OR we throw an error
    logicalToExecutable.applyOrElse(state.logicalPlan, throwCantCompile).apply(
      AdministrationCommandRuntimeContext(context)
    )
  }

  // When the community commands are run within enterprise, this allows the enterprise commands to be chained
  private def fullLogicalToExecutable = extraLogicalToExecutable orElse logicalToExecutable

  private val checkShowUserPrivilegesText: String =
    "Try executing SHOW USER PRIVILEGES to determine the missing or denied privileges. " +
      "In case of missing privileges, they need to be granted (See GRANT). In case of denied privileges, they need to be revoked (See REVOKE) and granted."

  private def prettifyActionName(actions: AdministrationAction*): String = {
    actions.map {
      case StartDatabaseAction => "START DATABASE"
      case StopDatabaseAction  => "STOP DATABASE"
      case a                   => a.name
    }.distinct.sorted.mkString(" and/or ")
  }

  def adminActionErrorMessage(
    permissionState: PermissionState,
    actions: Seq[AdministrationAction]
  ): String = {
    permissionState match {
      case PermissionState.EXPLICIT_DENY =>
        s"Permission denied for ${prettifyActionName(actions: _*)}. $checkShowUserPrivilegesText"
      case PermissionState.NOT_GRANTED =>
        s"Permission has not been granted for ${prettifyActionName(actions: _*)}. $checkShowUserPrivilegesText"
      case PermissionState.EXPLICIT_GRANT => ""
    }
  }

  private def getSource(
    maybeSource: Option[PrivilegePlan],
    context: AdministrationCommandRuntimeContext
  ): Option[ExecutionPlan] =
    maybeSource match {
      case Some(source) => Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
      case _            => None
    }

  def checkActions(
    actions: Seq[DbmsAction],
    securityContext: SecurityContext
  ): Seq[(DbmsAction, PermissionState)] =
    actions.map { action =>
      (
        action,
        securityContext.allowsAdminAction(new AdminActionOnResource(
          ActionMapper.asKernelAction(action),
          DatabaseScope.ALL,
          Segment.ALL
        ))
      )
    }

  private def checkAdminRightsForDBMSOrSelf(
    user: Either[String, Parameter],
    actions: Seq[DbmsAction]
  ): AdministrationCommandRuntimeContext => ExecutionPlan = _ => {
    AuthorizationAndPredicateExecutionPlan(
      securityAuthorizationHandler,
      (params, securityContext) => {
        if (securityContext.subject().hasUsername(runtimeStringValue(user, params)))
          Seq((null, PermissionState.EXPLICIT_GRANT))
        else checkActions(actions, securityContext)
      },
      violationMessage = adminActionErrorMessage
    )
  }

  def logicalToExecutable: PartialFunction[LogicalPlan, AdministrationCommandRuntimeContext => ExecutionPlan] = {
    // Check Admin Rights for DBMS commands
    case AssertAllowedDbmsActions(maybeSource, actions) => context =>
        AuthorizationAndPredicateExecutionPlan(
          securityAuthorizationHandler,
          (_, securityContext) => checkActions(actions, securityContext),
          violationMessage = adminActionErrorMessage,
          source = getSource(maybeSource, context)
        )

    // Check Admin Rights for DBMS commands or self
    case AssertAllowedDbmsActionsOrSelf(user, actions) =>
      context => checkAdminRightsForDBMSOrSelf(user, actions)(context)

    // Check rights for ALTER DATABASE, does the same as AssertAllowedDbmsActions
    // using the non-composite privileges, since community doesn't have composite databases
    case AssertCanAlterDatabase(source, database, _, actions) => context =>
        AuthorizationAndPredicateExecutionPlan(
          securityAuthorizationHandler,
          (params, securityContext) =>
            actions.map(action =>
              (
                action,
                securityContext.allowsAdminAction(
                  new AdminActionOnResource(
                    ActionMapper.asKernelAction(action),
                    new DatabaseScope(runtimeStringValue(database, params)),
                    Segment.ALL
                  )
                )
              )
            ),
          violationMessage = adminActionErrorMessage,
          source = getSource(Some(source), context)
        )

    // Check that the specified user is not the logged in user (eg. for some CREATE/DROP/ALTER USER commands)
    case AssertNotCurrentUser(source, userName, verb, violationMessage, errorGqlStatusObject) => context =>
        PredicateExecutionPlan(
          (params, sc) => !sc.subject().hasUsername(runtimeStringValue(userName, params)),
          onViolation = (_, _, sc) =>
            new InvalidArgumentException(
              errorGqlStatusObject,
              s"Failed to $verb the specified user '${sc.subject().executingUser()}': $violationMessage."
            ),
          source = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
        )

    // Check Admin Rights for some Database commands
    case AssertAllowedDatabaseAction(action, database, maybeSource) => context =>
        AuthorizationAndPredicateExecutionPlan(
          securityAuthorizationHandler,
          (params, securityContext) =>
            Seq((
              action,
              securityContext.allowsAdminAction(
                new AdminActionOnResource(
                  ActionMapper.asKernelAction(action),
                  new DatabaseScope(runtimeStringValue(database, params)),
                  Segment.ALL
                )
              )
            )),
          violationMessage = adminActionErrorMessage,
          source = getSource(maybeSource, context)
        )

    // SHOW USERS
    case ShowUsers(source, withAuth, symbols, yields, returns) => context =>
        val sourcePlan: Option[ExecutionPlan] =
          Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
        ShowUsersExecutionPlanner(normalExecutionEngine, securityAuthorizationHandler).planShowUsers(
          symbols,
          withAuth,
          yields,
          returns,
          sourcePlan,
          context
        )

    // SHOW CURRENT USER
    case ShowCurrentUser(symbols, yields, returns) => context =>
        ShowUsersExecutionPlanner(normalExecutionEngine, securityAuthorizationHandler).planShowCurrentUser(
          symbols,
          yields,
          returns,
          context
        )

    // CREATE [OR REPLACE] USER foo [IF NOT EXISTS] SET [PLAINTEXT | ENCRYPTED] PASSWORD 'password'
    // CREATE [OR REPLACE] USER foo [IF NOT EXISTS] SET [PLAINTEXT | ENCRYPTED] PASSWORD $password
    case createUser: CreateUser => context =>
        val sourcePlan: Option[ExecutionPlan] =
          Some(fullLogicalToExecutable.applyOrElse(createUser.source, throwCantCompile).apply(context))
        CreateUserExecutionPlanner(
          normalExecutionEngine,
          securityAuthorizationHandler,
          config
        ).planCreateUser(
          createUser,
          sourcePlan
        )

    // RENAME USER
    case RenameUser(source, fromUserName, toUserName) => context =>
        val sourcePlan: Option[ExecutionPlan] =
          Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
        makeRenameExecutionPlan(
          PrivilegeGqlCodeEntity.USER,
          USER_NAME_PROPERTY,
          fromUserName,
          toUserName,
          params => {
            val toName = runtimeStringValue(toUserName, params)
            NameValidator.assertValidUsername(toName)
          }
        )(sourcePlan, normalExecutionEngine, securityAuthorizationHandler)

    // ALTER USER foo [SET [PLAINTEXT | ENCRYPTED] PASSWORD pw] [CHANGE [NOT] REQUIRED]
    case alterUser: AlterUser => context =>
        val sourcePlan: Option[ExecutionPlan] =
          Some(fullLogicalToExecutable.applyOrElse(alterUser.source, throwCantCompile).apply(context))
        AlterUserExecutionPlanner(
          normalExecutionEngine,
          securityAuthorizationHandler,
          userSecurity,
          config
        ).planAlterUser(
          alterUser,
          sourcePlan
        )

    // DROP USER foo [IF EXISTS]
    case DropUser(source, userName) => context =>
        val sourcePlan: Option[ExecutionPlan] =
          Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
        DropUserExecutionPlanner(normalExecutionEngine, securityAuthorizationHandler).planDropUser(userName, sourcePlan)

    // ALTER CURRENT USER SET PASSWORD FROM 'currentPassword' TO 'newPassword'
    // ALTER CURRENT USER SET PASSWORD FROM 'currentPassword' TO $newPassword
    // ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO 'newPassword'
    // ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword
    case SetOwnPassword(source, newPassword, currentPassword) => context =>
        val sourcePlan: Option[ExecutionPlan] =
          Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
        SetOwnPasswordExecutionPlanner(normalExecutionEngine, securityAuthorizationHandler, config).planSetOwnPassword(
          newPassword,
          currentPassword,
          sourcePlan
        )

    // ALTER DATABASE SET DEFAULT LANGUAGE
    case AlterDatabase(source, databaseName, None, None, NoOptions, Some(defaultLanguageVersion), optionsToRemove, None)
      if optionsToRemove.isEmpty =>
      context => {
        val sourcePlan: Option[ExecutionPlan] =
          Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
        CommunityAlterDatabaseExecutionPlanner(
          normalExecutionEngine,
          securityAuthorizationHandler
        ).planAlterDatabase(databaseName, defaultLanguageVersion, sourcePlan, context)
      }

    case AssertNotVirtualSpd(source, databaseName, action, actionVerb) => context =>
        val sourcePlan: Option[ExecutionPlan] =
          Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
        val nameFields = getDatabaseNameFields("databaseName", databaseName)

        val parameterTransformer = ParameterTransformer()
          .convert(nameFields.nameConverter)
          .validate(checkNamespaceExists(nameFields, context))

        UpdatingSystemCommandExecutionPlan(
          "AssertNotSpd",
          normalExecutionEngine,
          securityAuthorizationHandler,
          query =
            s"""
               |MATCH (n:$DATABASE_NAME)-[:$TARGETS]->(d:$SPD)
               |  WHERE n.$DATABASE_NAME_PROPERTY = $$`${nameFields.nameKey}`
               |  AND n.$NAMESPACE_PROPERTY = $$`${nameFields.namespaceKey}`
               |RETURN d.$DATABASE_NAME_PROPERTY AS dbName""".stripMargin,
          VirtualValues.map(
            Array(nameFields.nameKey, nameFields.namespaceKey),
            Array(nameFields.nameValue, nameFields.namespaceValue)
          ),
          queryHandler = QueryHandler.handleResult((_, _, _) =>
            ThrowException(
              InvalidSemanticsException.invalidAlterShardedTarget(
                action
              )
            )
          ).handleError((error, params) =>
            (error, error.getCause) match {
              case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
                DatabaseAdministrationOnFollowerException.notALeader(
                  action,
                  s"Failed to $actionVerb the specified database '${runtimeStringValue(databaseName, params)}'",
                  error
                )
              case _ => error
            }
          ),
          source = sourcePlan,
          parameterTransformer = parameterTransformer
        )

    case AssertNotGraphShard(source, databaseName, action, actionVerb) => context =>
        val sourcePlan: Option[ExecutionPlan] =
          Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
        val nameFields = getDatabaseNameFields("databaseName", databaseName)

        val parameterTransformer = ParameterTransformer()
          .convert(nameFields.nameConverter)
          .validate(checkNamespaceExists(nameFields, context))

        UpdatingSystemCommandExecutionPlan(
          "AssertNotGraphShard",
          normalExecutionEngine,
          securityAuthorizationHandler,
          query =
            s"""
               |MATCH (n:$DATABASE_NAME)-[:$TARGETS]->(d:$GRAPH_SHARD)
               |  WHERE n.$DATABASE_NAME_PROPERTY = $$`${nameFields.nameKey}`
               |  AND n.$NAMESPACE_PROPERTY = $$`${nameFields.namespaceKey}`
               |RETURN d.$DATABASE_NAME_PROPERTY AS dbName""".stripMargin,
          VirtualValues.map(
            Array(nameFields.nameKey, nameFields.namespaceKey),
            Array(nameFields.nameValue, nameFields.namespaceValue)
          ),
          queryHandler = QueryHandler.handleResult((_, _, _) =>
            ThrowException(
              InvalidSemanticsException.invalidAlterGraphShardTarget(
                action
              )
            )
          ).handleError((error, params) =>
            (error, error.getCause) match {
              case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
                DatabaseAdministrationOnFollowerException.notALeader(
                  action,
                  s"Failed to $actionVerb the specified database '${runtimeStringValue(databaseName, params)}'",
                  error
                )
              case _ => error
            }
          ),
          source = sourcePlan,
          parameterTransformer = parameterTransformer
        )

    case AssertNotPropertyShard(source, databaseName, action, actionVerb) => context =>
        val sourcePlan: Option[ExecutionPlan] =
          Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
        val nameFields = getDatabaseNameFields("databaseName", databaseName)

        val parameterTransformer = ParameterTransformer()
          .convert(nameFields.nameConverter)
          .validate(checkNamespaceExists(nameFields, context))

        UpdatingSystemCommandExecutionPlan(
          "AssertNotPropertyShard",
          normalExecutionEngine,
          securityAuthorizationHandler,
          query =
            s"""
               |MATCH (n:$DATABASE_NAME)-[:$TARGETS]->(d:$PROPERTY_SHARD)
               |  WHERE n.$DATABASE_NAME_PROPERTY = $$`${nameFields.nameKey}`
               |  AND n.$NAMESPACE_PROPERTY = $$`${nameFields.namespaceKey}`
               |RETURN d.$DATABASE_NAME_PROPERTY AS dbName""".stripMargin,
          VirtualValues.map(
            Array(nameFields.nameKey, nameFields.namespaceKey),
            Array(nameFields.nameValue, nameFields.namespaceValue)
          ),
          queryHandler = QueryHandler.handleResult((_, _, _) =>
            ThrowException(
              InvalidSemanticsException.invalidAlterShardTarget(
                action
              )
            )
          ).handleError((error, params) =>
            (error, error.getCause) match {
              case (e: HasStatus, _) if e.status() == Status.Cluster.NotALeader =>
                DatabaseAdministrationOnFollowerException.notALeader(
                  action,
                  s"Failed to $actionVerb the specified database '${runtimeStringValue(databaseName, params)}'",
                  error
                )
              case _ => error
            }
          ),
          source = sourcePlan,
          parameterTransformer = parameterTransformer
        )

    // This is no-op in community
    case AssertManagementActionNotBlocked(_) => _ =>
        PredicateExecutionPlan(
          (_, _) => true,
          None,
          onViolation = (_, _, _) => new RuntimeException()
        )

    // SHOW DATABASES | SHOW DEFAULT DATABASE | SHOW HOME DATABASE | SHOW DATABASE foo
    case ShowDatabase(scope, verbose, symbols, yields, returns) => context =>
        ShowDatabasesExecutionPlanner(
          resolver,
          normalExecutionEngine,
          securityAuthorizationHandler
        )
          .planShowDatabases(scope, verbose, symbols, yields, returns, context)

    case DoNothingIfNotExists(source, command, entity, name, operation, valueMapper) => context =>
        val sourcePlan: Option[ExecutionPlan] =
          Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
        DoNothingExecutionPlanner(normalExecutionEngine, securityAuthorizationHandler).planDoNothingIfNotExists(
          command,
          entity,
          name,
          valueMapper,
          operation,
          sourcePlan
        )

    case DoNothingIfExists(source, command, entity, name, valueMapper) => context =>
        val sourcePlan: Option[ExecutionPlan] =
          Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
        DoNothingExecutionPlanner(normalExecutionEngine, securityAuthorizationHandler).planDoNothingIfExists(
          command,
          entity,
          name,
          valueMapper,
          sourcePlan
        )

    case DoNothingIfDatabaseNotExists(source, command, name, operation, databaseTypeFilter) => context =>
        val sourcePlan: Option[ExecutionPlan] =
          Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
        DoNothingExecutionPlanner(normalExecutionEngine, securityAuthorizationHandler).planDoNothingIfDatabaseNotExists(
          command,
          name,
          operation,
          sourcePlan,
          databaseTypeFilter,
          context
        )

    case DoNothingIfDatabaseExists(source, command, name, databaseTypeFilter) => context =>
        val sourcePlan: Option[ExecutionPlan] =
          Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
        DoNothingExecutionPlanner(normalExecutionEngine, securityAuthorizationHandler).planDoNothingIfDatabaseExists(
          command,
          name,
          sourcePlan,
          databaseTypeFilter,
          context
        )

    // Ensure that the role or user exists before being dropped
    case EnsureNodeExists(source, command, entity, name, valueMapper, extraFilter, labelDescription, action) =>
      context =>
        val sourcePlan: Option[ExecutionPlan] =
          Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
        EnsureNodeExistsExecutionPlanner(normalExecutionEngine, securityAuthorizationHandler)
          .planEnsureNodeExists(command, entity, name, valueMapper, extraFilter, labelDescription, action, sourcePlan)

    // SUPPORT PROCEDURES (need to be cleared before here)
    case SystemProcedureCall(_, call, returns, _, checkCredentialsExpired) => context =>
        SystemProcedureCallPlanner(normalExecutionEngine, securityAuthorizationHandler).planSystemProcedureCall(
          context.runtimeContext.cypherVersion,
          call,
          returns,
          checkCredentialsExpired
        )

    case CheckNativeAuthentication() => _ =>
        val usernameKey = internalKey("username")
        val nativeAuth = internalKey("nativelyAuthenticated")

        def currentUser(p: MapValue): String = p.get(usernameKey).asInstanceOf[TextValue].stringValue()

        UpdatingSystemCommandExecutionPlan(
          "CheckNativeAuthentication",
          normalExecutionEngine,
          securityAuthorizationHandler,
          s"RETURN $$`$nativeAuth` AS nativelyAuthenticated",
          MapValue.EMPTY,
          QueryHandler
            .handleError {
              case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
                DatabaseAdministrationOnFollowerException.notALeader(
                  "ALTER CURRENT USER SET PASSWORD",
                  s"User '${currentUser(p)}' failed to alter their own password",
                  error
                )
              case (error: Neo4jException, _) => error
              case (error, p) =>
                CypherExecutionException.alterOwnPassword(currentUser(p), error)
            }
            .handleResult((_, value, _) => {
              if (value eq BooleanValue.TRUE) Continue
              else ThrowException(AuthorizationViolationException.alterCurrentUserNotAllowed())
            }),
          parameterTransformer = ParameterTransformer((_, securityContext, _) =>
            VirtualValues.map(
              Array(nativeAuth, usernameKey),
              Array(
                Values.booleanValue(securityContext.nativelyAuthenticated()),
                Values.utf8Value(securityContext.subject().executingUser())
              )
            )
          ),
          checkCredentialsExpired = false
        )

    // Non-administration commands that are allowed on system database, e.g. SHOW PROCEDURES
    case AllowedNonAdministrationCommands(statement) => context =>
        SystemCommandExecutionPlan(
          "AllowedNonAdministrationCommand",
          normalExecutionEngine,
          securityAuthorizationHandler,
          QueryRenderer.render(statement),
          MapValue.EMPTY,
          // If we have a non admin command executing in the system database, forbid it to make reads / writes
          // from the system graph. This is to prevent queries such as SHOW PROCEDURES YIELD * RETURN ()--()
          // from leaking nodes from the system graph: the ()--() would return empty results
          modeConverter = s => s.withMode(new RestrictedAccessMode(s.mode(), StaticAccessMode.ACCESS)),
          // While running against system will override most pre-parser options.
          // However, we shouldn't override the Cypher version,
          // so let's prepend the inner query with the relevant Cypher version.
          cypherVersion = Some(context.runtimeContext.cypherVersion)
        )

    // Ignore the log command in community
    case LogSystemCommand(source, _) => context =>
        fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context)
  }

  override def isApplicableAdministrationCommand(logicalPlanArg: LogicalPlan): Boolean = {
    val logicalPlan = logicalPlanArg match {
      // Ignore the log command in community
      case LogSystemCommand(source, _) => source
      case plan                        => plan
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
