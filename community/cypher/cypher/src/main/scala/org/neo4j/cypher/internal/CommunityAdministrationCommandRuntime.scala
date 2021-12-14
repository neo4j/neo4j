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
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.helpers.NormalizedDatabaseName
import org.neo4j.cypher.internal.ast.AdministrationAction
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.NamedDatabaseScope
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.StartDatabaseAction
import org.neo4j.cypher.internal.ast.StopDatabaseAction
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.expressions.ImplicitProcedureArgument
import org.neo4j.cypher.internal.logical.plans.AllowedNonAdministrationCommands
import org.neo4j.cypher.internal.logical.plans.AlterUser
import org.neo4j.cypher.internal.logical.plans.AssertAllowedDatabaseAction
import org.neo4j.cypher.internal.logical.plans.AssertAllowedDbmsActions
import org.neo4j.cypher.internal.logical.plans.AssertAllowedDbmsActionsOrSelf
import org.neo4j.cypher.internal.logical.plans.AssertNotCurrentUser
import org.neo4j.cypher.internal.logical.plans.CreateUser
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
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.SystemCommandExecutionPlan
import org.neo4j.cypher.internal.procs.UpdatingSystemCommandExecutionPlan
import org.neo4j.cypher.internal.security.SystemGraphCredential
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.rendering.QueryRenderer
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.exceptions.Neo4jException
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType.withName
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.internal.kernel.api.security.AccessMode
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.AdminActionOnResource.DatabaseScope
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.kernel.api.security.Segment
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.kernel.database.DefaultDatabaseResolver
import org.neo4j.kernel.impl.api.security.OverriddenAccessMode
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.storable.ByteArray
import org.neo4j.values.storable.StringArray
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.VirtualValues
import scala.collection.JavaConverters.asScalaIteratorConverter

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

  def logicalToExecutable: PartialFunction[LogicalPlan, AdministrationCommandRuntimeContext => ExecutionPlan] = {
    // Check Admin Rights for DBMS commands
    case AssertAllowedDbmsActions(maybeSource, actions) => context =>
      AuthorizationPredicateExecutionPlan(
        securityAuthorizationHandler,
        (_, securityContext) => actions.forall { action =>
        securityContext.allowsAdminAction(new AdminActionOnResource(ActionMapper.asKernelAction(action), DatabaseScope.ALL, Segment.ALL))
      },
        violationMessage = "Permission denied for " + prettifyActionName(actions: _*) + ". " + checkShowUserPrivilegesText, //sorting is important to keep error messages stable
        source = maybeSource match {
          case Some(source) => Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
          case _            => None
        }
      )

    // Check Admin Rights for DBMS commands or self
    case AssertAllowedDbmsActionsOrSelf(user, actions) => _ =>
      AuthorizationPredicateExecutionPlan(securityAuthorizationHandler,
        (params, securityContext) => securityContext.subject().hasUsername(runtimeStringValue(user, params)) || actions.forall { action =>
        securityContext.allowsAdminAction(new AdminActionOnResource(ActionMapper.asKernelAction(action), DatabaseScope.ALL, Segment.ALL))
      }, violationMessage = "Permission denied for " + prettifyActionName(actions:_*) + ". " + checkShowUserPrivilegesText)  //sorting is important to keep error messages stable

    // Check that the specified user is not the logged in user (eg. for some CREATE/DROP/ALTER USER commands)
    case AssertNotCurrentUser(source, userName, verb, violationMessage) => context =>
      new PredicateExecutionPlan((params, sc) => !sc.subject().hasUsername(runtimeStringValue(userName, params)),
        onViolation = (_, sc) => new InvalidArgumentException(s"Failed to $verb the specified user '${sc.subject().username()}': $violationMessage."),
        source = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
      )

    // Check Admin Rights for some Database commands
    case AssertAllowedDatabaseAction(action, database, maybeSource) => context =>
      AuthorizationPredicateExecutionPlan(securityAuthorizationHandler,
        (params, securityContext) => securityContext.allowsAdminAction(new AdminActionOnResource(ActionMapper.asKernelAction(action),
          new DatabaseScope(runtimeStringValue(database, params)), Segment.ALL)),
        violationMessage = "Permission denied for " + prettifyActionName(action) + ". " + checkShowUserPrivilegesText,
        source = maybeSource match {
          case Some(source) => Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
          case _            => None
        }
      )

    // SHOW USERS
    case ShowUsers(source, symbols, yields, returns) => context =>
      SystemCommandExecutionPlan("ShowUsers",
        normalExecutionEngine,
        securityAuthorizationHandler,
        s"""MATCH (u:User)
           |WITH u.name as user, null as roles, u.passwordChangeRequired AS passwordChangeRequired, null as suspended, null as home
           |${AdministrationShowCommandUtils.generateReturnClause(symbols, yields, returns, Seq("user"))}
           |""".stripMargin,
        VirtualValues.EMPTY_MAP,
        source = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))
      )

    // SHOW CURRENT USER
    case ShowCurrentUser(symbols, yields, returns) => _ =>
      val currentUserKey = internalKey("currentUser")
      SystemCommandExecutionPlan("ShowCurrentUser",
        normalExecutionEngine,
        securityAuthorizationHandler,
        s"""MATCH (u:User)
           |WITH u.name as user, null as roles, u.passwordChangeRequired AS passwordChangeRequired, null as suspended, null as home
           |WHERE user = $$`$currentUserKey`
           |${AdministrationShowCommandUtils.generateReturnClause(symbols, yields, returns, Seq("user"))}
           |""".stripMargin,
        VirtualValues.EMPTY_MAP,
        parameterGenerator = (_, securityContext) => VirtualValues.map(
          Array(currentUserKey),
          Array(Values.utf8Value(securityContext.subject().username()))),
      )

    // CREATE [OR REPLACE] USER foo [IF NOT EXISTS] SET [PLAINTEXT | ENCRYPTED] PASSWORD 'password'
    // CREATE [OR REPLACE] USER foo [IF NOT EXISTS] SET [PLAINTEXT | ENCRYPTED] PASSWORD $password
    case CreateUser(source, userName, isEncryptedPassword, password, requirePasswordChange, suspendedOptional, defaultDatabase) => context =>
      val sourcePlan: Option[ExecutionPlan] = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))

      def failWithError(command: String) : PredicateExecutionPlan = {
        new PredicateExecutionPlan((_, _) => false, sourcePlan, (params, _) => {
          val user = runtimeStringValue(userName, params)
          throw new CantCompileQueryException(s"Failed to create the specified user '$user': '$command' is not available in community edition.")
        })
      }

      if (suspendedOptional.isDefined) { // Users are always active in community
        failWithError("SET STATUS")
      } else if (defaultDatabase.isDefined) { // There is only one database in community
        failWithError("HOME DATABASE")
      }
      else {
        makeCreateUserExecutionPlan(
          userName, isEncryptedPassword, password, requirePasswordChange, suspended = false, defaultDatabase = None
        )(sourcePlan, normalExecutionEngine, securityAuthorizationHandler)
      }

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
    case AlterUser(source, userName, isEncryptedPassword, password, requirePasswordChange, suspended, defaultDatabase) => context =>
      val sourcePlan: Option[ExecutionPlan] = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context))

      def failWithError(command: String) : PredicateExecutionPlan = {
        new PredicateExecutionPlan((_, _) => false, sourcePlan, (params, _) => {
          val user = runtimeStringValue(userName, params)
          throw new CantCompileQueryException(s"Failed to alter the specified user '$user': '$command' is not available in community edition.")
        })
      }

      if (suspended.isDefined) { // Users are always active in community
        failWithError("SET STATUS")
      } else if (defaultDatabase.isDefined ) {
        failWithError("HOME DATABASE")
      } else {
        makeAlterUserExecutionPlan(
          userName, isEncryptedPassword, password, requirePasswordChange, suspended = None, defaultDatabase = None
        )(sourcePlan, normalExecutionEngine, securityAuthorizationHandler)
      }

    // DROP USER foo [IF EXISTS]
    case DropUser(source, userName) => context =>
      val userNameFields = getNameFields("username", userName)
      UpdatingSystemCommandExecutionPlan("DropUser",
        normalExecutionEngine,
        securityAuthorizationHandler,
        s"""MATCH (user:User {name: $$`${userNameFields.nameKey}`}) DETACH DELETE user
          |RETURN 1 AS ignore""".stripMargin,
        VirtualValues.map(Array(userNameFields.nameKey), Array(userNameFields.nameValue)),
        QueryHandler
          .handleError {
            case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to delete the specified user '${runtimeStringValue(userName, p)}': $followerError", error)
            case (error, p) => new IllegalStateException(s"Failed to delete the specified user '${runtimeStringValue(userName, p)}'.", error)
          },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context)),
        parameterConverter = userNameFields.nameConverter
      )

    // ALTER CURRENT USER SET PASSWORD FROM 'currentPassword' TO 'newPassword'
    // ALTER CURRENT USER SET PASSWORD FROM 'currentPassword' TO $newPassword
    // ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO 'newPassword'
    // ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword
    case SetOwnPassword(newPassword, currentPassword) => _ =>
      val usernameKey = internalKey("username")
      val newPw = getPasswordExpression(None, newPassword, isEncryptedPassword = false)
      val (currentKeyBytes, currentValueBytes, currentConverterBytes) = getPasswordFieldsCurrent(currentPassword)
      def currentUser(p: MapValue): String = p.get(usernameKey).asInstanceOf[TextValue].stringValue()
      val query =
        s"""MATCH (user:User {name: $$`$usernameKey`})
          |WITH user, user.credentials AS oldCredentials
          |SET user.credentials = $$`${newPw.key}`
          |SET user.passwordChangeRequired = false
          |RETURN oldCredentials""".stripMargin

      UpdatingSystemCommandExecutionPlan("AlterCurrentUserSetPassword", normalExecutionEngine,
        securityAuthorizationHandler,
        query,
        VirtualValues.map(Array(newPw.key, newPw.bytesKey, currentKeyBytes), Array(newPw.value, newPw.bytesValue, currentValueBytes)),
        QueryHandler
          .handleError {
            case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"User '${currentUser(p)}' failed to alter their own password: $followerError", error)
            case (error: Neo4jException, _) => error
            case (error, p) => new IllegalStateException(s"User '${currentUser(p)}' failed to alter their own password.", error)
          }
          .handleResult((_, value, p) => {
            val oldCredentials = SystemGraphCredential.deserialize(value.asInstanceOf[TextValue].stringValue(), secureHasher)
            val newValue = p.get(newPw.bytesKey).asInstanceOf[ByteArray].asObject()
            val currentValue = p.get(currentKeyBytes).asInstanceOf[ByteArray].asObject()
            if (!oldCredentials.matchesPassword(currentValue))
              Some(new InvalidArgumentException(s"User '${currentUser(p)}' failed to alter their own password: Invalid principal or credentials."))
            else if (oldCredentials.matchesPassword(newValue))
              Some(new InvalidArgumentException(s"User '${currentUser(p)}' failed to alter their own password: Old password and new password cannot be the same."))
            else
              None
          })
          .handleNoResult( p => {
            if (currentUser(p).isEmpty) // This is true if the securityContext is AUTH_DISABLED (both for community and enterprise)
              Some(new IllegalStateException("User failed to alter their own password: Command not available with auth disabled."))
            else // The 'current user' doesn't exist in the system graph
              Some(new IllegalStateException(s"User '${currentUser(p)}' failed to alter their own password: User does not exist."))
          }),
        checkCredentialsExpired = false,
        finallyFunction = p => {
          p.get(newPw.bytesKey).asInstanceOf[ByteArray].zero()
          p.get(currentKeyBytes).asInstanceOf[ByteArray].zero()
        },
        parameterGenerator = (_, securityContext) => VirtualValues.map(Array(usernameKey), Array(Values.utf8Value(securityContext.subject().username()))),
        parameterConverter = (tx, m) => newPw.mapValueConverter(tx, currentConverterBytes(m))
      )

    // SHOW HOME DATABASES
    case ShowDatabase(scope: HomeDatabaseScope, symbols, yields, returns) => _ =>
      val usernameKey = internalKey("username")

      val paramGenerator: (Transaction, SecurityContext) => MapValue = (_, securityContext) => {
        val username = Option(securityContext.subject().username()) match {
          case None       => Values.NO_VALUE
          case Some("")   => Values.NO_VALUE
          case Some(user) => Values.stringValue(user)
        }
        VirtualValues.map(Array(internalKey("username")), Array(username))
      }

      val returnClause = AdministrationShowCommandUtils.generateReturnClause(symbols, yields, returns, Seq("name"))
      val query =
        s"""
          |OPTIONAL MATCH (default: Database {default: true})
          |OPTIONAL MATCH (user:User {name: $$`$usernameKey`})
          |WITH coalesce(user.homeDatabase, default.name) AS homeDatabase
          |MATCH (d:Database) WHERE d.name = homeDatabase
          |CALL dbms.database.state(d.name) yield status, error, address, role
          |WITH d.name as name, address, role, d.status as requestedStatus, status as currentStatus, error
          |$returnClause""".stripMargin
      SystemCommandExecutionPlan(scope.showCommandName,
        normalExecutionEngine,
        securityAuthorizationHandler,
        query,
        VirtualValues.EMPTY_MAP,
        parameterGenerator = paramGenerator)

    // SHOW DATABASES | SHOW DEFAULT DATABASE | SHOW DATABASE foo
    case ShowDatabase(scope, symbols, yields, returns) => _ =>
      val usernameKey = internalKey("username")
      val paramGenerator: (Transaction, SecurityContext) => MapValue = (tx, securityContext) => generateShowAccessibleDatabasesParameter(tx, securityContext)
      val (extraFilter, params, paramConverter) = scope match {
        // show default database
        case _: DefaultDatabaseScope => (s"WHERE default = true", VirtualValues.EMPTY_MAP, IdentityConverter)
        // show database name
        case NamedDatabaseScope(p) =>
          val nameFields = getNameFields("databaseName", p, valueMapper = s => new NormalizedDatabaseName(s).name())
          val combinedConverter: (Transaction, MapValue) => MapValue = (tx, m) => {
            val normalizedName = new NormalizedDatabaseName(runtimeStringValue(p, m)).name()
            val filteredDatabases = m.get(accessibleDbsKey).asInstanceOf[StringArray].asObjectCopy().filter(normalizedName.equals)
            nameFields.nameConverter(tx, m.updatedWith(accessibleDbsKey, Values.stringArray(filteredDatabases:_*)))
          }
          (s"WHERE name = $$`${nameFields.nameKey}`", VirtualValues.map(Array(nameFields.nameKey), Array(nameFields.nameValue)), combinedConverter)
        // show all databases
        case _ => ("", VirtualValues.EMPTY_MAP, IdentityConverter)
      }
      val returnClause = AdministrationShowCommandUtils.generateReturnClause(symbols, yields, returns, Seq("name"))

      val query = s"""// First resolve which database is the home database
                     |OPTIONAL MATCH (default: Database {default: true})
                     |OPTIONAL MATCH (user:User {name: $$`$usernameKey`})
                     |WITH coalesce(user.homeDatabase, default.name) as homeDbName
                     |
                     |MATCH (d: Database)
                     |WHERE d.name IN $$`$accessibleDbsKey`
                     |CALL dbms.database.state(d.name) yield status, error, address, role
                     |WITH d.name as name, address, role, d.status as requestedStatus, status as currentStatus, error,
                     |d.default as default, coalesce(d.name = homeDbName, false) as home
                     |$extraFilter
                     |$returnClause""".stripMargin
      SystemCommandExecutionPlan(scope.showCommandName,
        normalExecutionEngine,
        securityAuthorizationHandler,
        query,
        params,
        parameterGenerator = paramGenerator,
        parameterConverter = paramConverter)

    case DoNothingIfNotExists(source, label, name, operation, valueMapper) => context =>
      val nameFields = getNameFields("name", name, valueMapper = valueMapper)
      UpdatingSystemCommandExecutionPlan("DoNothingIfNotExists",
        normalExecutionEngine,
        securityAuthorizationHandler,
        s"""
           |MATCH (node:$label {name: $$`${nameFields.nameKey}`})
           |RETURN node.name AS name
        """.stripMargin, VirtualValues.map(Array(nameFields.nameKey), Array(nameFields.nameValue)),
        QueryHandler
          .ignoreNoResult()
          .handleError {
            case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to $operation the specified ${label.toLowerCase} '${runtimeStringValue(name, p)}': $followerError", error)
            case (error, p) => new IllegalStateException(s"Failed to $operation the specified ${label.toLowerCase} '${runtimeStringValue(name, p)}'.", error) // should not get here but need a default case
          },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context)),
        parameterConverter = nameFields.nameConverter
      )

    case DoNothingIfExists(source, label, name, valueMapper) => context =>
      val nameFields = getNameFields("name", name, valueMapper = valueMapper)
      UpdatingSystemCommandExecutionPlan("DoNothingIfExists",
        normalExecutionEngine,
        securityAuthorizationHandler,
        s"""
           |MATCH (node:$label {name: $$`${nameFields.nameKey}`})
           |RETURN node.name AS name
        """.stripMargin, VirtualValues.map(Array(nameFields.nameKey), Array(nameFields.nameValue)),
        QueryHandler
          .ignoreOnResult()
          .handleError {
            case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to create the specified ${label.toLowerCase} '${runtimeStringValue(name, p)}': $followerError", error)
            case (error, p) => new IllegalStateException(s"Failed to create the specified ${label.toLowerCase} '${runtimeStringValue(name, p)}'.", error) // should not get here but need a default case
          },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context)),
        parameterConverter = nameFields.nameConverter
      )

    // Ensure that the role or user exists before being dropped
    case EnsureNodeExists(source, label, name, valueMapper) => context =>
      val nameFields = getNameFields("name", name, valueMapper = valueMapper)
      UpdatingSystemCommandExecutionPlan("EnsureNodeExists",
        normalExecutionEngine,
        securityAuthorizationHandler,
        s"""MATCH (node:$label {name: $$`${nameFields.nameKey}`})
           |RETURN node""".stripMargin,
        VirtualValues.map(Array(nameFields.nameKey), Array(nameFields.nameValue)),
        QueryHandler
          .handleNoResult(p => Some(new InvalidArgumentException(s"Failed to delete the specified ${label.toLowerCase} '${runtimeStringValue(name, p)}': $label does not exist.")))
          .handleError {
            case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to delete the specified ${label.toLowerCase} '${runtimeStringValue(name, p)}': $followerError", error)
            case (error, p) => new IllegalStateException(s"Failed to delete the specified ${label.toLowerCase} '${runtimeStringValue(name, p)}'.", error) // should not get here but need a default case
          },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context)),
        parameterConverter = nameFields.nameConverter
      )

    // SUPPORT PROCEDURES (need to be cleared before here)
    case SystemProcedureCall(_, call, returns, _, checkCredentialsExpired) => context =>
      val queryString = returns match {
        case Return(_, ReturnItems(_, items, _), _, _, _, _) if items.nonEmpty => QueryRenderer.render(Seq(call, returns))
        case _ => QueryRenderer.render(Seq(call))
      }

      def addParameterDefaults(params: MapValue): MapValue = {
        val builder = call.treeFold(new MapValueBuilder()) {
          case ImplicitProcedureArgument(name, _, defaultValue) => acc =>
            acc.add(name, ValueUtils.of(defaultValue))
            TraverseChildren(acc)
        }
        val defaults = builder.build()
        defaults.updatedWith(params)
      }

      SystemCommandExecutionPlan("SystemProcedure",
        normalExecutionEngine,
        securityAuthorizationHandler,
        queryString,
        MapValue.EMPTY,
        checkCredentialsExpired = checkCredentialsExpired,
        parameterConverter = (_, params) => addParameterDefaults(params),
        modeConverter = s => s.withMode(new OverriddenAccessMode(s.mode(), AccessMode.Static.READ)),
      )

    // Non-administration commands that are allowed on system database, e.g. SHOW PROCEDURES
    case AllowedNonAdministrationCommands(statement) => _ =>
      val queryString = QueryRenderer.render(statement)
      SystemCommandExecutionPlan("AllowedNonAdministrationCommand",
        normalExecutionEngine,
        securityAuthorizationHandler,
        queryString,
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

  private val accessibleDbsKey = internalKey("accessibleDbs")

  private def generateShowAccessibleDatabasesParameter(transaction: Transaction, securityContext: SecurityContext): MapValue = {
    def accessForDatabase(database: Node, roles: java.util.Set[String]): Option[Boolean] = {
      //(:Role)-[p]->(:Privilege {action: 'access'})-[s:SCOPE]->()-[f:FOR]->(d:Database)
      var result: Seq[Boolean] = Seq.empty
      database.getRelationships(Direction.INCOMING, withName("FOR")).forEach { f =>
        f.getStartNode.getRelationships(Direction.INCOMING, withName("SCOPE")).forEach { s =>
          val privilegeNode = s.getStartNode
          if (privilegeNode.getProperty("action").equals("access")) {
            privilegeNode.getRelationships(Direction.INCOMING).forEach { p =>
              val roleName = p.getStartNode.getProperty("name")
              if (roles.contains(roleName)) {
                p.getType.name() match {
                  case "DENIED" => result = result :+ false
                  case "GRANTED" => result = result :+ true
                  case _ =>
                }
              }
            }
          }
        }
      }
      result.reduceOption(_ && _)
    }

    val allowsDatabaseManagement: Boolean =
      securityContext.allowsAdminAction(new AdminActionOnResource(PrivilegeAction.CREATE_DATABASE, DatabaseScope.ALL, Segment.ALL)) ||
      securityContext.allowsAdminAction(new AdminActionOnResource(PrivilegeAction.DROP_DATABASE, DatabaseScope.ALL, Segment.ALL))
    val roles = securityContext.mode().roles()

    val allDatabaseNode = transaction.findNode(Label.label("DatabaseAll"), "name", "*")
    val allDatabaseAccess = if (allDatabaseNode != null) accessForDatabase(allDatabaseNode, roles) else None
    val defaultDatabaseNode = transaction.findNode(Label.label("DatabaseDefault"), "name", "DEFAULT")
    val defaultDatabaseAccess = if (defaultDatabaseNode != null) accessForDatabase(defaultDatabaseNode, roles) else None
    val defaultDatabaseName = defaultDatabaseResolver.defaultDatabase(securityContext.subject().username())

    val accessibleDatabases = transaction.findNodes(Label.label("Database")).asScala.foldLeft[Seq[String]](Seq.empty) { (acc, dbNode) =>
      val dbName = dbNode.getProperty("name").toString
      val isDefault = dbName.equals(defaultDatabaseName)
      if (dbName.equals(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)) {
        acc :+ dbName
      } else if (allowsDatabaseManagement) {
        acc :+ dbName
      } else {
        (accessForDatabase(dbNode, roles), allDatabaseAccess, defaultDatabaseAccess, isDefault) match {
          // denied
          case (Some(false), _, _, _) => acc
          case (_, Some(false), _, _) => acc
          case (_, _, Some(false), true) => acc

          // granted
          case (Some(true), _, _, _) => acc :+ dbName
          case (_, Some(true), _, _) => acc :+ dbName
          case (_, _, Some(true), true) => acc :+ dbName

          // no privilege
          case _ => acc
        }
      }
    }

    val username = Option(securityContext.subject().username()) match {
      case None => Values.NO_VALUE
      case Some("") => Values.NO_VALUE
      case Some(user) => Values.stringValue(user)
    }
    VirtualValues.map(Array(accessibleDbsKey, internalKey("username")),
      Array(Values.stringArray(accessibleDatabases: _*), username))
  }

  override def isApplicableAdministrationCommand(logicalPlanState: LogicalPlanState): Boolean = {
    val logicalPlan = logicalPlanState.maybeLogicalPlan.get match {
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
