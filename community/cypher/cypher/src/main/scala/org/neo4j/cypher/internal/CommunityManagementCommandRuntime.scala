/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.planner.CantCompileQueryException
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.procs.{QueryHandler, SystemCommandExecutionPlan, UpdatingSystemCommandExecutionPlan}
import org.neo4j.cypher.internal.runtime._
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.api.security.AuthManager
import org.neo4j.string.UTF8
import org.neo4j.values.storable.{TextValue, Values}
import org.neo4j.values.virtual.VirtualValues

/**
  * This runtime takes on queries that require no planning, such as multidatabase management commands
  */
case class CommunityManagementCommandRuntime(normalExecutionEngine: ExecutionEngine, resolver: DependencyResolver) extends ManagementCommandRuntime {
  override def name: String = "community management-commands"

  override def compileToExecutable(state: LogicalQuery, context: RuntimeContext, username: String): ExecutionPlan = {

    def throwCantCompile(unknownPlan: LogicalPlan): Nothing = {
      throw new CantCompileQueryException(
        s"Plan is not a recognized database administration command in community edition: ${unknownPlan.getClass.getSimpleName}")
    }

    val (planWithSlottedParameters, parameterMapping) = slottedParameters(state.logicalPlan)

    // Either the logical plan is a command that the partial function logicalToExecutable provides/understands OR we throw an error
    logicalToExecutable.applyOrElse(planWithSlottedParameters, throwCantCompile).apply(context, parameterMapping, username)
  }

  private lazy val authManager = {
    resolver.resolveDependency(classOf[AuthManager])
  }

  val logicalToExecutable: PartialFunction[LogicalPlan, (RuntimeContext, ParameterMapping, String) => ExecutionPlan] = {

    // SHOW USERS
    case ShowUsers() => (_, _, _) =>
      SystemCommandExecutionPlan("ShowUsers", normalExecutionEngine,
        """MATCH (u:User)
          |RETURN u.name as user, u.passwordChangeRequired AS passwordChangeRequired""".stripMargin,
        VirtualValues.EMPTY_MAP
      )

    // CREATE USER foo WITH PASSWORD password
    case CreateUser(userName, Some(initialStringPassword), None, requirePasswordChange, suspendedOptional) => (_, _, _) =>
      if(suspendedOptional.isDefined)  // Users are always active in community
        throw new CantCompileQueryException("'SET STATUS' is not available in community edition.")

      // TODO: Move the conversion to byte[] earlier in the stack (during or after parsing)
      val initialPassword = UTF8.encode(initialStringPassword)
      try {
        validatePassword(initialPassword)

        // NOTE: If username already exists we will violate a constraint
        UpdatingSystemCommandExecutionPlan("CreateUser", normalExecutionEngine,
          """CREATE (u:User {name: $name, credentials: $credentials, passwordChangeRequired: $passwordChangeRequired, suspended: false})
            |RETURN u.name""".stripMargin,
          VirtualValues.map(
            Array("name", "credentials", "passwordChangeRequired"),
            Array(
              Values.stringValue(userName),
              Values.stringValue(authManager.createCredentialForPassword(initialPassword).serialize()),
              Values.booleanValue(requirePasswordChange))),
          QueryHandler
            .handleNoResult(() => Some(new InvalidArgumentsException(s"Failed to create user '$userName'.")))
            .handleError(e => new InvalidArgumentsException(s"The specified user '$userName' already exists.", e))
        )
      } finally {
        // Clear password
        if (initialPassword != null) util.Arrays.fill(initialPassword, 0.toByte)
      }

    // CREATE USER foo WITH PASSWORD $password
    case CreateUser(_, _, Some(_), _, _) =>
      throw new IllegalStateException("Did not resolve parameters correctly.")

    // CREATE USER foo WITH PASSWORD
    case CreateUser(_, _, _, _, _) =>
      throw new IllegalStateException("Password not correctly supplied.")

    // DROP USER foo
    case DropUser(userName) => (_, _, currentUser) =>
      if (userName.equals(currentUser)) throw new InvalidArgumentsException(s"Deleting yourself (user '$userName') is not allowed.")
      UpdatingSystemCommandExecutionPlan("DropUser", normalExecutionEngine,
        """MATCH (user:User {name: $name}) DETACH DELETE user
          |RETURN user""".stripMargin,
        VirtualValues.map(Array("name"), Array(Values.stringValue(userName))),
        QueryHandler
          .handleNoResult(() => Some(new InvalidArgumentsException(s"User '$userName' does not exist.")))
          .handleError(e => new InvalidArgumentsException(s"Failed to delete the specified user '$userName'.", e))
      )

    // SHOW DEFAULT DATABASE
    case ShowDefaultDatabase() => (_, _, _) =>
      SystemCommandExecutionPlan("ShowDefaultDatabase", normalExecutionEngine,
        "MATCH (d:Database {default: true}) RETURN d.name as name", VirtualValues.EMPTY_MAP)

    // SUPPORT PROCEDURES (need to be cleared before here)
    case SystemProcedureCall(queryString, params) => (_, _, _) =>
      SystemCommandExecutionPlan("SystemProcedure", normalExecutionEngine, queryString, params)
  }

  override def isApplicableManagementCommand(logicalPlanState: LogicalPlanState): Boolean = logicalToExecutable.isDefinedAt(logicalPlanState.maybeLogicalPlan.get)
}

object DatabaseStatus extends Enumeration {
  type Status = TextValue

  val Online: TextValue = Values.stringValue("online")
  val Offline: TextValue = Values.stringValue("offline")
}
