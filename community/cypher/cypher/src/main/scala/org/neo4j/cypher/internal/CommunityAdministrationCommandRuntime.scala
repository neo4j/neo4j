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

import java.util

import org.neo4j.common.DependencyResolver
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.procs._
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.security.SecureHasher
import org.neo4j.cypher.internal.security.SystemGraphCredential
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
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
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException
import org.neo4j.kernel.api.security.AuthManager
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import scala.collection.JavaConverters._

/**
  * This runtime takes on queries that require no planning, such as multidatabase administration commands
  */
case class CommunityAdministrationCommandRuntime(normalExecutionEngine: ExecutionEngine, resolver: DependencyResolver,
                                                 extraLogicalToExecutable: PartialFunction[LogicalPlan, (RuntimeContext, ParameterMapping, SecurityContext) => ExecutionPlan] = CommunityAdministrationCommandRuntime.emptyLogicalToExecutable
                                                ) extends AdministrationCommandRuntime {
  override def name: String = "community administration-commands"

  def throwCantCompile(unknownPlan: LogicalPlan): Nothing = {
    throw new CantCompileQueryException(
      s"Plan is not a recognized database administration command in community edition: ${unknownPlan.getClass.getSimpleName}")
  }

  override def compileToExecutable(state: LogicalQuery, context: RuntimeContext, securityContext: SecurityContext): ExecutionPlan = {

    val (planWithSlottedParameters, parameterMapping) = slottedParameters(state.logicalPlan)

    // Either the logical plan is a command that the partial function logicalToExecutable provides/understands OR we throw an error
    logicalToExecutable.applyOrElse(planWithSlottedParameters, throwCantCompile).apply(context, parameterMapping, securityContext)
  }

  private lazy val authManager = {
    resolver.resolveDependency(classOf[AuthManager])
  }
  private val secureHasher = new SecureHasher

  // When the community commands are run within enterprise, this allows the enterprise commands to be chained
  private def fullLogicalToExecutable = extraLogicalToExecutable orElse logicalToExecutable

  def logicalToExecutable: PartialFunction[LogicalPlan, (RuntimeContext, ParameterMapping, SecurityContext) => ExecutionPlan] = {

    // Check Admin Rights for DBMS commands
    case AssertDbmsAdmin(actions@_*) => (_, _, securityContext) =>
      AuthorizationPredicateExecutionPlan(() => actions.forall { action =>
        securityContext.allowsAdminAction(new AdminActionOnResource(AdminActionMapper.asKernelAction(action), DatabaseScope.ALL))
      }, violationMessage = PERMISSION_DENIED)

    // Check that the specified user is not the logged in user (eg. for some ALTER USER commands)
    case AssertNotCurrentUser(source, userName, violationMessage) => (context, parameterMapping, securityContext) =>
      AuthorizationPredicateExecutionPlan(() => !securityContext.subject().hasUsername(userName),
        violationMessage = violationMessage,
        source = source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, securityContext))
      )

    // Check Admin Rights for some Database commands
    case AssertDatabaseAdmin(action, database) => (_, _, securityContext) =>
      AuthorizationPredicateExecutionPlan(() =>
        securityContext.allowsAdminAction(new AdminActionOnResource(AdminActionMapper.asKernelAction(action), new DatabaseScope(database.name()))),
        violationMessage = PERMISSION_DENIED
      )

    // SHOW USERS
    case ShowUsers(source) => (context, parameterMapping, securityContext) =>
      SystemCommandExecutionPlan("ShowUsers", normalExecutionEngine,
        """MATCH (u:User)
          |RETURN u.name as user, u.passwordChangeRequired AS passwordChangeRequired""".stripMargin,
        VirtualValues.EMPTY_MAP,
        source = source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, securityContext))
      )

    // CREATE [OR REPLACE] USER foo [IF NOT EXISTS] SET PASSWORD password
    case CreateUser(source, userName, Some(initialPassword), None, requirePasswordChange, suspendedOptional) => (context, parameterMapping, securityContext) =>
      if (suspendedOptional.isDefined) // Users are always active in community
        throw new CantCompileQueryException(s"Failed to create the specified user '$userName': 'SET STATUS' is not available in community edition.")

      try {
        validatePassword(initialPassword)
        UpdatingSystemCommandExecutionPlan("CreateUser", normalExecutionEngine,
          // NOTE: If username already exists we will violate a constraint
          """CREATE (u:User {name: $name, credentials: $credentials, passwordChangeRequired: $passwordChangeRequired, suspended: false})
            |RETURN u.name""".stripMargin,
          VirtualValues.map(
            Array("name", "credentials", "passwordChangeRequired"),
            Array(
              Values.stringValue(userName),
              Values.stringValue(SystemGraphCredential.createCredentialForPassword(initialPassword, secureHasher).serialize()),
              Values.booleanValue(requirePasswordChange))),
          QueryHandler
            .handleNoResult(() => Some(new IllegalStateException(s"Failed to create the specified user '$userName'.")))
            .handleError(e => e.getCause match {
              case _: UniquePropertyValueValidationException =>
                new InvalidArgumentsException(s"Failed to create the specified user '$userName': User already exists.", e)
              case _ => new IllegalStateException(s"Failed to create the specified user '$userName'.", e)
            }),
          source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, securityContext))
        )
      } finally {
        // Clear password
        if (initialPassword != null) util.Arrays.fill(initialPassword, 0.toByte)
      }

    // CREATE [OR REPLACE] USER foo [IF NOT EXISTS] SET PASSWORD $password
    case CreateUser(_, userName, _, Some(_), _, _) =>
      throw new IllegalStateException(s"Failed to create the specified user '$userName': Did not resolve parameters correctly.")

    // CREATE [OR REPLACE] USER foo [IF NOT EXISTS] SET PASSWORD
    case CreateUser(_, userName, _, _, _, _) =>
      throw new IllegalStateException(s"Failed to create the specified user '$userName': Password not correctly supplied.")

    // DROP USER foo [IF EXISTS]
    case DropUser(source, userName) => (context, parameterMapping, securityContext) =>
      if (securityContext.subject().hasUsername(userName)) throw new InvalidArgumentsException(s"Failed to delete the specified user '$userName': Deleting yourself is not allowed.")
      UpdatingSystemCommandExecutionPlan("DropUser", normalExecutionEngine,
        """MATCH (user:User {name: $name}) DETACH DELETE user
          |RETURN 1 AS ignore""".stripMargin,
        VirtualValues.map(Array("name"), Array(Values.stringValue(userName))),
        QueryHandler
          .handleError {
            case error: HasStatus if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to delete the specified user '$userName': $followerError", error)
            case error => new IllegalStateException(s"Failed to delete the specified user '$userName'.", error)
          },
        source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, securityContext))
      )

    // ALTER CURRENT USER SET PASSWORD FROM 'currentPassword' TO 'newPassword'
    case SetOwnPassword(Some(newPassword), None, Some(currentPassword), None) => (_, _, securityContext) =>
      val query =
        """MATCH (user:User {name: $name})
          |WITH user, user.credentials AS oldCredentials
          |SET user.credentials = $credentials
          |SET user.passwordChangeRequired = false
          |RETURN oldCredentials""".stripMargin
      val currentUser = securityContext.subject().username()

      UpdatingSystemCommandExecutionPlan("AlterCurrentUserSetPassword", normalExecutionEngine, query,
        VirtualValues.map(Array("name", "credentials"),
          Array(Values.stringValue(currentUser), Values.stringValue(SystemGraphCredential.createCredentialForPassword(validatePassword(newPassword), secureHasher).serialize()))),
        QueryHandler
          .handleError {
            case error: HasStatus if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"User '$currentUser' failed to alter their own password: $followerError", error)
            case error => new IllegalStateException(s"User '$currentUser' failed to alter their own password.", error)
          }
          .handleResult((_, value) => {
            val oldCredentials = SystemGraphCredential.deserialize(value.asInstanceOf[TextValue].stringValue(), secureHasher)
            if (!oldCredentials.matchesPassword(currentPassword))
              Some(new InvalidArgumentsException(s"User '$currentUser' failed to alter their own password: Invalid principal or credentials."))
            else if (oldCredentials.matchesPassword(newPassword))
              Some(new InvalidArgumentsException(s"User '$currentUser' failed to alter their own password: Old password and new password cannot be the same."))
            else
              None
          })
          .handleNoResult( () => {
            if (currentUser.isEmpty) // This is true if the securityContext is AUTH_DISABLED (both for community and enterprise)
              Some(new IllegalStateException("User failed to alter their own password: Command not available with auth disabled."))
            else // The 'current user' doesn't exist in the system graph
              Some(new IllegalStateException(s"User '$currentUser' failed to alter their own password: User does not exist."))
          }),
        checkCredentialsExpired = false
      )

    // ALTER CURRENT USER SET PASSWORD FROM currentPassword TO $newPassword
    case SetOwnPassword(_, Some(_), _, _) => (_, _, securityContext) =>
      val currentUser = securityContext.subject().username()
      throw new IllegalStateException(s"User '$currentUser' failed to alter their own password: Did not resolve parameters correctly.")

    // ALTER CURRENT USER SET PASSWORD FROM $currentPassword TO newPassword
    case SetOwnPassword(_, _, _, Some(_)) => (_, _, securityContext) =>
      val currentUser = securityContext.subject().username()
      throw new IllegalStateException(s"User '$currentUser' failed to alter their own password: Did not resolve parameters correctly.")

    // ALTER CURRENT USER SET PASSWORD FROM currentPassword TO newPassword
    case SetOwnPassword(_, _, _, _) => (_, _, securityContext) =>
      val currentUser = securityContext.subject().username()
      throw new IllegalStateException(s"User '$currentUser' failed to alter their own password: Password not correctly supplied.")

    // SHOW DATABASES
    case ShowDatabases() => (_, _, securityContext) =>
      val (query, generator) = makeShowDatabasesQuery(securityContext)
      SystemCommandExecutionPlan("ShowDatabases", normalExecutionEngine, query, VirtualValues.EMPTY_MAP, parameterGenerator = generator)

    // SHOW DEFAULT DATABASE
    case ShowDefaultDatabase() => (_, _, securityContext) =>
      val (query, generator) = makeShowDatabasesQuery(securityContext, isDefault = true)
      SystemCommandExecutionPlan("ShowDefaultDatabase", normalExecutionEngine, query, VirtualValues.EMPTY_MAP, parameterGenerator = generator)

    // SHOW DATABASE foo
    case ShowDatabase(normalizedName) => (_, _, securityContext) =>
      val (query, generator) = makeShowDatabasesQuery(securityContext, dbName = Some(normalizedName.name))
      SystemCommandExecutionPlan("ShowDatabase", normalExecutionEngine, query,
        VirtualValues.map(Array("name"), Array(Values.stringValue(normalizedName.name))), parameterGenerator = generator)

    case DoNothingIfNotExists(source, label, name) => (context, parameterMapping, securityContext) =>
      UpdatingSystemCommandExecutionPlan("DoNothingIfNotExists", normalExecutionEngine,
        s"""
           |MATCH (node:$label {name: $$name})
           |RETURN node.name AS name
        """.stripMargin, VirtualValues.map(Array("name"), Array(Values.stringValue(name))),
        QueryHandler
          .ignoreNoResult()
          .handleError {
            case error: HasStatus if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to delete the specified ${label.toLowerCase} '$name': $followerError", error)
            case error => new IllegalStateException(s"Failed to delete the specified ${label.toLowerCase} '$name'.", error) // should not get here but need a default case
          },
        source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, securityContext))
      )

    case DoNothingIfExists(source, label, name) => (context, parameterMapping, securityContext) =>
      UpdatingSystemCommandExecutionPlan("DoNothingIfExists", normalExecutionEngine,
        s"""
           |MATCH (node:$label {name: $$name})
           |RETURN node.name AS name
        """.stripMargin, VirtualValues.map(Array("name"), Array(Values.stringValue(name))),
        QueryHandler
          .ignoreOnResult()
          .handleError {
            case error: HasStatus if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to create the specified ${label.toLowerCase} '$name': $followerError", error)
            case error => new IllegalStateException(s"Failed to create the specified ${label.toLowerCase} '$name'.", error) // should not get here but need a default case
          },
        source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, securityContext))
      )

    // Ensure that the role or user exists before being dropped
    case EnsureNodeExists(source, label, name) => (context, parameterMapping, securityContext) =>
      UpdatingSystemCommandExecutionPlan("EnsureNodeExists", normalExecutionEngine,
        s"""MATCH (node:$label {name: $$name})
           |RETURN node""".stripMargin,
        VirtualValues.map(Array("name"), Array(Values.stringValue(name))),
        QueryHandler
          .handleNoResult(() => Some(new InvalidArgumentsException(s"Failed to delete the specified ${label.toLowerCase} '$name': $label does not exist.")))
          .handleError {
            case error: HasStatus if error.status() == Status.Cluster.NotALeader =>
              new DatabaseAdministrationOnFollowerException(s"Failed to delete the specified ${label.toLowerCase} '$name': $followerError", error)
            case error => new IllegalStateException(s"Failed to delete the specified ${label.toLowerCase} '$name'.", error) // should not get here but need a default case
          },
        source.map(fullLogicalToExecutable.applyOrElse(_, throwCantCompile).apply(context, parameterMapping, securityContext))
      )

    // SUPPORT PROCEDURES (need to be cleared before here)
    case SystemProcedureCall(_, queryString, params, checkCredentialsExpired) => (_, _, _) =>
      SystemCommandExecutionPlan("SystemProcedure", normalExecutionEngine, queryString, params, checkCredentialsExpired = checkCredentialsExpired)

    // Ignore the log command in community
    case LogSystemCommand(source, _) => (context, parameterMapping, securityContext) =>
      fullLogicalToExecutable.applyOrElse(source, throwCantCompile).apply(context, parameterMapping, securityContext)
  }

  private def makeShowDatabasesQuery(securityContext: SecurityContext, isDefault: Boolean = false, dbName: Option[String] = None): (String, Transaction => MapValue) = {
    def allowsDatabaseManagement(securityContext: SecurityContext): Boolean =
      securityContext.allowsAdminAction(new AdminActionOnResource(PrivilegeAction.CREATE_DATABASE, DatabaseScope.ALL)) ||
      securityContext.allowsAdminAction(new AdminActionOnResource(PrivilegeAction.DROP_DATABASE, DatabaseScope.ALL))

    val defaultColumn = if (isDefault) "" else ", d.default as default"

    val paramGenerator: Transaction => MapValue = tx => generateShowAccessibleDatabasesParameter(tx, securityContext, isDefault, dbName)
    val noGen: Transaction => MapValue = _ => MapValue.EMPTY

    val (filter, generator) = (allowsDatabaseManagement(securityContext), isDefault, dbName) match {
      // show default database
      case (false, true, _) => ("WHERE d.name IN $accessibleDbs AND d.default = true", paramGenerator)
      case (true, true, _) => ("WHERE d.default = true", noGen)
      // show database name
      case (false, _, Some(_)) => ("WHERE d.name IN $accessibleDbs AND d.name = $name", paramGenerator)
      case (true, _, Some(_)) => ("WHERE d.name = $name", noGen)
      // show all databases
      case (false, false, None) => ("WHERE d.name IN $accessibleDbs", paramGenerator)
      case _ => ("", noGen)
    }
    val query = s"""
       |MATCH (d: Database)
       |$filter
       |CALL dbms.database.state(d.name) yield status, error, address, role
       |WITH d, status as currentStatus, error, address, role
       |RETURN d.name as name, address, role, d.status as requestedStatus, currentStatus, error $defaultColumn
       |ORDER BY name
    """.stripMargin
    (query, generator)
  }

  private def generateShowAccessibleDatabasesParameter(transaction: Transaction, securityContext: SecurityContext, isDefault: Boolean = false, dbName: Option[String] = None ): MapValue = {
    def accessForDatabase(database: Node, roles: util.Set[String]): Option[Boolean] = {
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

    val roles = securityContext.mode().roles()

    val allDatabaseNode = transaction.findNode(Label.label("DatabaseAll"), "name", "*")
    val allDatabaseAccess = accessForDatabase(allDatabaseNode, roles)

    val accessibleDatabases = transaction.findNodes(Label.label("Database")).asScala.foldLeft[Seq[String]](Seq.empty) { (acc, dbNode) =>
      val dbName = dbNode.getProperty("name").toString
      if (dbName.equals(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)) {
        acc :+ dbName
      } else {
        (accessForDatabase(dbNode, roles), allDatabaseAccess) match {
          // denied
          case (Some(false), _) => acc
          case (_, Some(false)) => acc

          // granted
          case (_, Some(true)) => acc :+ dbName
          case (Some(true), _) => acc :+ dbName

          // no privilege
          case _ => acc
        }
      }
    }

    val filteredDatabases = dbName match {
      case Some(name) => accessibleDatabases.filter(db => name.equals(db))
      case _ => accessibleDatabases
    }
    VirtualValues.map(Array("accessibleDbs"), Array(Values.stringArray(filteredDatabases: _*)))
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

  val Online: TextValue = Values.stringValue("online")
  val Offline: TextValue = Values.stringValue("offline")
}

object CommunityAdministrationCommandRuntime {
  def emptyLogicalToExecutable: PartialFunction[LogicalPlan, (RuntimeContext, ParameterMapping, SecurityContext) => ExecutionPlan] =
    new PartialFunction[LogicalPlan, (RuntimeContext, ParameterMapping, SecurityContext) => ExecutionPlan] {
      override def isDefinedAt(x: LogicalPlan): Boolean = false

      override def apply(v1: LogicalPlan): (RuntimeContext, ParameterMapping, SecurityContext) => ExecutionPlan = ???
    }
}
