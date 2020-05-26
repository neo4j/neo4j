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
package org.neo4j.cypher.internal

import org.neo4j.common.DependencyResolver
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.helpers.NormalizedDatabaseName
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.logical.plans.AlterUser
import org.neo4j.cypher.internal.logical.plans.AssertDatabaseAdmin
import org.neo4j.cypher.internal.logical.plans.AssertDbmsAdmin
import org.neo4j.cypher.internal.logical.plans.AssertDbmsAdminOrSelf
import org.neo4j.cypher.internal.logical.plans.AssertNotCurrentUser
import org.neo4j.cypher.internal.logical.plans.CreateUser
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExists
import org.neo4j.cypher.internal.logical.plans.DoNothingIfNotExists
import org.neo4j.cypher.internal.logical.plans.DropUser
import org.neo4j.cypher.internal.logical.plans.EnsureNodeExists
import org.neo4j.cypher.internal.logical.plans.LogSystemCommand
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.SetOwnPassword
import org.neo4j.cypher.internal.logical.plans.ShowDatabase
import org.neo4j.cypher.internal.logical.plans.ShowDatabases
import org.neo4j.cypher.internal.logical.plans.ShowDefaultDatabase
import org.neo4j.cypher.internal.logical.plans.ShowUsers
import org.neo4j.cypher.internal.logical.plans.SystemProcedureCall
import org.neo4j.cypher.internal.procs.ActionMapper
import org.neo4j.cypher.internal.procs.AuthorizationPredicateExecutionPlan
import org.neo4j.cypher.internal.procs.PredicateExecutionPlan
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.SystemCommandExecutionPlan
import org.neo4j.cypher.internal.procs.UpdatingSystemCommandExecutionPlan
import org.neo4j.cypher.internal.runtime.ParameterMapping
import org.neo4j.cypher.internal.runtime.slottedParameters
import org.neo4j.cypher.internal.security.SystemGraphCredential
import org.neo4j.cypher.rendering.QueryRenderer
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.exceptions.Neo4jException
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType.withName
import org.neo4j.graphdb.Transaction
import org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.AdminActionOnResource.DatabaseScope
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.kernel.api.security.Segment
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus
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
                                                 extraLogicalToExecutable: PartialFunction[LogicalPlan, (RuntimeContext, ParameterMapping) => ExecutionPlan] = CommunityAdministrationCommandRuntime.emptyLogicalToExecutable
                                                ) extends AdministrationCommandRuntime {
  override def name: String = "community administration-commands"

  def throwCantCompile(unknownPlan: LogicalPlan): Nothing = {
    throw new CantCompileQueryException(
      s"Plan is not a recognized database administration command in community edition: ${unknownPlan.getClass.getSimpleName}")
  }

  override def compileToExecutable(state: LogicalQuery, context: RuntimeContext): ExecutionPlan = {

    val (planWithSlottedParameters, parameterMapping) = slottedParameters(state.logicalPlan)

    // Either the logical plan is a command that the partial function logicalToExecutable provides/understands OR we throw an error
    logicalToExecutable.applyOrElse(planWithSlottedParameters, throwCantCompile).apply(context, parameterMapping)
  }

  // When the community commands are run within enterprise, this allows the enterprise commands to be chained
  private def fullLogicalToExecutable = extraLogicalToExecutable orElse logicalToExecutable

  def logicalToExecutable: PartialFunction[LogicalPlan, (RuntimeContext, ParameterMapping) => ExecutionPlan] = {

    // Check Admin Rights for DBMS commands
    case AssertDbmsAdmin(actions) => (_, _) =>
      AuthorizationPredicateExecutionPlan((_, securityContext) => actions.forall { action =>
        securityContext.allowsAdminAction(new AdminActionOnResource(ActionMapper.asKernelAction(action), DatabaseScope.ALL, Segment.ALL))
      }, violationMessage = PERMISSION_DENIED)

    // Check Admin Rights for DBMS commands or self
    case AssertDbmsAdminOrSelf(user, actions) => (_, _) =>
      AuthorizationPredicateExecutionPlan((params, securityContext) => securityContext.subject().hasUsername(runtimeValue(user, params)) || actions.forall { action =>
        securityContext.allowsAdminAction(new AdminActionOnResource(ActionMapper.asKernelAction(action), DatabaseScope.ALL, Segment.ALL))
      }, violationMessage = PERMISSION_DENIED)

    // Check that the specified user is not the logged in user (eg. for some CREATE/DROP/ALTER USER commands)
    case AssertNotCurrentUser(source, userName, verb, violationMessage) => (context, parameterMapping) =>
      new PredicateExecutionPlan((params, sc) => !sc.subject().hasUsername(runtimeValue(userName, params)),
        onViolation = (_, sc) => new InvalidArgumentsException(s"Failed to $verb the specified user '${sc.subject().username()}': $violationMessage."),
        source = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping))
      )

    // Check Admin Rights for some Database commands
    case AssertDatabaseAdmin(action, database) => (_, _) =>
      AuthorizationPredicateExecutionPlan((params, securityContext) =>
        securityContext.allowsAdminAction(new AdminActionOnResource(ActionMapper.asKernelAction(action), new DatabaseScope(runtimeValue(database, params)), Segment.ALL)),
        violationMessage = PERMISSION_DENIED
      )

    // SHOW USERS
    case ShowUsers(source, symbols, yields, where, returns) => (context, parameterMapping) =>
      SystemCommandExecutionPlan("ShowUsers", normalExecutionEngine,
        s"""MATCH (u:User)
          |WITH u.name as user, null as roles, u.passwordChangeRequired AS passwordChangeRequired, null as suspended
          |${AdministrationShowCommandUtils.generateWhereClause(where)}
          |${AdministrationShowCommandUtils.generateReturnClause(symbols, yields, returns, Seq("user"))}
          |""".stripMargin,
        VirtualValues.EMPTY_MAP,
        source = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping))
      )

    // CREATE [OR REPLACE] USER foo [IF NOT EXISTS] SET PASSWORD 'password'
    // CREATE [OR REPLACE] USER foo [IF NOT EXISTS] SET PASSWORD $password
    case CreateUser(source, userName, password, requirePasswordChange, suspendedOptional) => (context, parameterMapping) =>
      val sourcePlan: Option[ExecutionPlan] = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping))
      if (suspendedOptional.isDefined) { // Users are always active in community
        new PredicateExecutionPlan((_, _) => false, sourcePlan, (params, _) => {
          val user = runtimeValue(userName, params)
          throw new CantCompileQueryException(s"Failed to create the specified user '$user': 'SET STATUS' is not available in community edition.")
        })
      }
      else {
        makeCreateUserExecutionPlan(userName, password, requirePasswordChange, suspended = false)(sourcePlan, normalExecutionEngine)
      }

    // ALTER USER foo [SET PASSWORD pw] [CHANGE [NOT] REQUIRED]
    case AlterUser(source, userName, password, requirePasswordChange, suspended) => (context, parameterMapping) =>
      val sourcePlan: Option[ExecutionPlan] = Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping))
      if (suspended.isDefined) { // Users are always active in community
        new PredicateExecutionPlan((_, _) => false, sourcePlan, (params, _) => {
          val user = runtimeValue(userName, params)
          throw new CantCompileQueryException(s"Failed to alter the specified user '$user': 'SET STATUS' is not available in community edition.")
        })
      } else {
        makeAlterUserExecutionPlan(userName, password, requirePasswordChange, suspended = None)(sourcePlan, normalExecutionEngine)
      }

    // DROP USER foo [IF EXISTS]
    case DropUser(source, userName) => (context, parameterMapping) =>
      val (userNameKey, userNameValue, userNameConverter) = getNameFields("username", userName)
      UpdatingSystemCommandExecutionPlan("DropUser", normalExecutionEngine,
        s"""MATCH (user:User {name: $$`$userNameKey`}) DETACH DELETE user
          |RETURN 1 AS ignore""".stripMargin,
        VirtualValues.map(Array(userNameKey), Array(userNameValue)),
        QueryHandler
          .handleError {
            case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to delete the specified user '${runtimeValue(userName, p)}': $followerError", error)
            case (error, p) => new IllegalStateException(s"Failed to delete the specified user '${runtimeValue(userName, p)}'.", error)
          },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)),
        parameterConverter = userNameConverter
      )

    // ALTER CURRENT USER SET PASSWORD FROM 'currentPassword' TO 'newPassword'
    // ALTER CURRENT USER SET PASSWORD FROM 'currentPassword' TO $newPassword
    // ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO 'newPassword'
    // ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO $newPassword
    case SetOwnPassword(newPassword, currentPassword) => (_, _) =>
      val usernameKey = internalKey("username")
      val newPw = getPasswordExpression(newPassword)
      val (currentKeyBytes, currentValueBytes, currentConverterBytes) = getPasswordFieldsCurrent(currentPassword)
      def currentUser(p: MapValue): String = p.get(usernameKey).asInstanceOf[TextValue].stringValue()
      val query =
        s"""MATCH (user:User {name: $$`$usernameKey`})
          |WITH user, user.credentials AS oldCredentials
          |SET user.credentials = $$`${newPw.key}`
          |SET user.passwordChangeRequired = false
          |RETURN oldCredentials""".stripMargin

      UpdatingSystemCommandExecutionPlan("AlterCurrentUserSetPassword", normalExecutionEngine, query,
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
              Some(new InvalidArgumentsException(s"User '${currentUser(p)}' failed to alter their own password: Invalid principal or credentials."))
            else if (oldCredentials.matchesPassword(newValue))
              Some(new InvalidArgumentsException(s"User '${currentUser(p)}' failed to alter their own password: Old password and new password cannot be the same."))
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

    // SHOW DATABASES
    case ShowDatabases(symbols, yields, where, returns) => (_, _) =>
      val (query, _, generator, _) = makeShowDatabasesQuery(symbols, yields, where, returns)
      SystemCommandExecutionPlan("ShowDatabases", normalExecutionEngine, query, VirtualValues.EMPTY_MAP, parameterGenerator = generator)

    // SHOW DEFAULT DATABASE
    case ShowDefaultDatabase(symbols, yields, where, returns) => (_, _) =>
      val (query, _, generator, _) = makeShowDatabasesQuery(symbols, yields, where, returns,isDefault = true)
      SystemCommandExecutionPlan("ShowDefaultDatabase", normalExecutionEngine, query, VirtualValues.EMPTY_MAP, parameterGenerator = generator)

    // SHOW DATABASE foo
    case ShowDatabase(databaseName,symbols, yields, where, returns) => (_, _) =>
      val (query, params, generator, converter) = makeShowDatabasesQuery(symbols, yields, where, returns, dbName = Some(databaseName))
      SystemCommandExecutionPlan("ShowDatabase", normalExecutionEngine, query, params, parameterGenerator = generator, parameterConverter = converter)

    case DoNothingIfNotExists(source, label, name, valueMapper) => (context, parameterMapping) =>
      val (nameKey, nameValue, nameConverter) = getNameFields("name", name, valueMapper = valueMapper)
      UpdatingSystemCommandExecutionPlan("DoNothingIfNotExists", normalExecutionEngine,
        s"""
           |MATCH (node:$label {name: $$`$nameKey`})
           |RETURN node.name AS name
        """.stripMargin, VirtualValues.map(Array(nameKey), Array(nameValue)),
        QueryHandler
          .ignoreNoResult()
          .handleError {
            case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to delete the specified ${label.toLowerCase} '${runtimeValue(name, p)}': $followerError", error)
            case (error, p) => new IllegalStateException(s"Failed to delete the specified ${label.toLowerCase} '${runtimeValue(name, p)}'.", error) // should not get here but need a default case
          },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)),
        parameterConverter = nameConverter
      )

    case DoNothingIfExists(source, label, name, valueMapper) => (context, parameterMapping) =>
      val (nameKey, nameValue, nameConverter) = getNameFields("name", name, valueMapper = valueMapper)
      UpdatingSystemCommandExecutionPlan("DoNothingIfExists", normalExecutionEngine,
        s"""
           |MATCH (node:$label {name: $$`$nameKey`})
           |RETURN node.name AS name
        """.stripMargin, VirtualValues.map(Array(nameKey), Array(nameValue)),
        QueryHandler
          .ignoreOnResult()
          .handleError {
            case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to create the specified ${label.toLowerCase} '${runtimeValue(name, p)}': $followerError", error)
            case (error, p) => new IllegalStateException(s"Failed to create the specified ${label.toLowerCase} '${runtimeValue(name, p)}'.", error) // should not get here but need a default case
          },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)),
        parameterConverter = nameConverter
      )

    // Ensure that the role or user exists before being dropped
    case EnsureNodeExists(source, label, name, valueMapper) => (context, parameterMapping) =>
      val (nameKey, nameValue, nameConverter) = getNameFields("name", name, valueMapper = valueMapper)
      UpdatingSystemCommandExecutionPlan("EnsureNodeExists", normalExecutionEngine,
        s"""MATCH (node:$label {name: $$`$nameKey`})
           |RETURN node""".stripMargin,
        VirtualValues.map(Array(nameKey), Array(nameValue)),
        QueryHandler
          .handleNoResult(p => Some(new InvalidArgumentsException(s"Failed to delete the specified ${label.toLowerCase} '${runtimeValue(name, p)}': $label does not exist.")))
          .handleError {
            case (error: HasStatus, p) if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to delete the specified ${label.toLowerCase} '${runtimeValue(name, p)}': $followerError", error)
            case (error, p) => new IllegalStateException(s"Failed to delete the specified ${label.toLowerCase} '${runtimeValue(name, p)}'.", error) // should not get here but need a default case
          },
        Some(fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)),
        parameterConverter = nameConverter
      )

    // SUPPORT PROCEDURES (need to be cleared before here)
    case SystemProcedureCall(_, call, _, checkCredentialsExpired) => (_, parameterMapping) =>
      val queryString = QueryRenderer.render(Seq(call))

      def addParameterDefaults(transaction: Transaction, params: MapValue): MapValue = {
        val builder = new MapValueBuilder()
        parameterMapping.foreach((name, value) =>
          value.default.foreach(builder.add(name, _))
        )
        val defaults = builder.build()
        defaults.updatedWith(params)
      }

      SystemCommandExecutionPlan("SystemProcedure", normalExecutionEngine, queryString, MapValue.EMPTY,
        checkCredentialsExpired = checkCredentialsExpired,
        parameterConverter = addParameterDefaults
      )

    // Ignore the log command in community
    case LogSystemCommand(source, _) => (context, parameterMapping) =>
      fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping)
  }

  private val accessibleDbsKey = internalKey("accessibleDbs")

  private def makeShowDatabasesQuery(symbols: List[String], yields: Option[Return], where: Option[Where], returns: Option[Return],
                                     isDefault: Boolean = false, dbName: Option[Either[String, Parameter]] = None): (String, MapValue, (Transaction, SecurityContext) => MapValue, (Transaction, MapValue) => MapValue) = {
    val paramGenerator: (Transaction, SecurityContext) => MapValue = (tx, securityContext) => generateShowAccessibleDatabasesParameter(tx, securityContext, isDefault)
    val (extraFilter, params, paramConverter) = (isDefault, dbName) match {
      // show default database
      case (true, _) => ("AND d.default = true", VirtualValues.EMPTY_MAP, IdentityConverter)
      // show database name
      case (_, Some(p)) =>
        val (key, value, converter) = getNameFields("databaseName", p, valueMapper = s => new NormalizedDatabaseName(s).name())
        val combinedConverter: (Transaction, MapValue) => MapValue = (tx, m) => {
          val normalizedName = new NormalizedDatabaseName(runtimeValue(p, m)).name()
          val filteredDatabases = m.get(accessibleDbsKey).asInstanceOf[StringArray].asObjectCopy().filter(normalizedName.equals)
          converter(tx, m.updatedWith(accessibleDbsKey, Values.stringArray(filteredDatabases:_*)))
        }
        (s"AND d.name = $$`$key`", VirtualValues.map(Array(key), Array(value)), combinedConverter)
      // show all databases
      case _ => ("", VirtualValues.EMPTY_MAP, IdentityConverter)
    }
    val returnClause = AdministrationShowCommandUtils.generateReturnClause(symbols, yields, returns, Seq("name"))
    val filtering = AdministrationShowCommandUtils.generateWhereClause(where)

    val query = s"""
       |MATCH (d: Database)
       |WHERE d.name IN $$`$accessibleDbsKey` $extraFilter
       |CALL dbms.database.state(d.name) yield status, error, address, role
       |WITH d.name as name, address, role, d.status as requestedStatus, status as currentStatus, error, d.default as default
       |$filtering
       |$returnClause
    """.stripMargin
    (query, params, paramGenerator, paramConverter)
  }

  private def generateShowAccessibleDatabasesParameter(transaction: Transaction, securityContext: SecurityContext, isDefault: Boolean = false): MapValue = {
    // TODO isDefault is not used, should it be?
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

    val accessibleDatabases = transaction.findNodes(Label.label("Database")).asScala.foldLeft[Seq[String]](Seq.empty) { (acc, dbNode) =>
      val dbName = dbNode.getProperty("name").toString
      val isDefault = Boolean.unbox(dbNode.getProperty("default"))
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

    VirtualValues.map(Array(accessibleDbsKey), Array(Values.stringArray(accessibleDatabases: _*)))
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
  def emptyLogicalToExecutable: PartialFunction[LogicalPlan, (RuntimeContext, ParameterMapping) => ExecutionPlan] =
    new PartialFunction[LogicalPlan, (RuntimeContext, ParameterMapping) => ExecutionPlan] {
      override def isDefinedAt(x: LogicalPlan): Boolean = false

      override def apply(v1: LogicalPlan): (RuntimeContext, ParameterMapping) => ExecutionPlan = ???
    }
}
