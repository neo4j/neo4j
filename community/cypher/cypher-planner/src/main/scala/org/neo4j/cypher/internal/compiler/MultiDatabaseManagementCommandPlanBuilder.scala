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
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.internal.compiler.phases.{LogicalPlanState, PlannerContext}
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.ProcedurePlannerName
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.v4_0.frontend.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.v4_0.frontend.phases._
import org.neo4j.cypher.internal.v4_0.util.attribution.SequentialIdGen
import org.neo4j.kernel.database.DatabaseId

/**
  * This planner takes on queries that run at the DBMS level for multi-database management
  */
case object MultiDatabaseManagementCommandPlanBuilder extends Phase[PlannerContext, BaseState, LogicalPlanState] {

  override def phase: CompilationPhase = PIPE_BUILDING

  override def description = "take on administrative queries that require no planning such as multi-database management commands"

  override def postConditions: Set[Condition] = Set.empty

  override def process(from: BaseState, context: PlannerContext): LogicalPlanState = {
    implicit val idGen: SequentialIdGen = new SequentialIdGen()
    val maybeLogicalPlan: Option[LogicalPlan] = from.statement() match {
      // SHOW USERS
      case _: ShowUsers =>
        Some(plans.ShowUsers())

      // CREATE USER foo
      case CreateUser(userName, initialStringPassword, initialParameterPassword, requirePasswordChange, suspended) =>
        NameValidator.assertValidUsername(userName)
        Some(plans.CreateUser(userName, initialStringPassword, initialParameterPassword, requirePasswordChange, suspended))

      // DROP USER foo
      case DropUser(userName) =>
        Some(plans.DropUser(userName))

      // ALTER USER foo
      case AlterUser(userName, initialStringPassword, initialParameterPassword, requirePasswordChange, suspended) =>
        Some(plans.AlterUser(userName, initialStringPassword, initialParameterPassword, requirePasswordChange, suspended))

      // SHOW [ ALL | POPULATED ] ROLES [ WITH USERS ]
      case ShowRoles(withUsers, showAll) =>
        Some(plans.ShowRoles(withUsers, showAll))

      // CREATE ROLE foo
      case CreateRole(roleName, None) =>
        NameValidator.assertValidRoleName(roleName)
        Some(plans.CreateRole(None, roleName))

      // CREATE ROLE foo AS COPY OF bar
      case CreateRole(roleName, Some(fromName)) =>
        NameValidator.assertValidRoleName(roleName)
        Some(plans.CopyRolePrivileges(
          Some(plans.CopyRolePrivileges(
            Some(plans.CreateRole(
              Some(plans.RequireRole(None, fromName)), roleName)
            ), roleName, fromName, "GRANTED")
          ), roleName, fromName, "DENIED")
        )

      // DROP ROLE foo
      case DropRole(roleName) =>
        Some(plans.DropRole(roleName))

      // GRANT roles TO users
      case GrantRolesToUsers(roleNames, userNames) =>
        (for (userName <- userNames; roleName <- roleNames) yield {
          roleName -> userName
        }).foldLeft(Option.empty[plans.GrantRoleToUser]) {
          case (source, (userName, roleName)) => Some(plans.GrantRoleToUser(source, userName, roleName))
        }

      // REVOKE roles FROM users
      case RevokeRolesFromUsers(roleNames, userNames) =>
        (for (userName <- userNames; roleName <- roleNames) yield {
          roleName -> userName
        }).foldLeft(Option.empty[plans.RevokeRoleFromUser]) {
          case (source, (userName, roleName)) => Some(plans.RevokeRoleFromUser(source, userName, roleName))
        }

      // GRANT TRAVERSE ON GRAPH foo NODES A (*) TO role
      case GrantPrivilege(TraversePrivilege(), _, database, labels, roleNames) =>
        (for (roleName <- roleNames; label <- labels.simplify) yield {
          roleName -> label
        }).foldLeft(Option.empty[plans.GrantTraverse]) {
          case (source, (roleName, label)) => Some(plans.GrantTraverse(source, database, label, roleName))
        }

      // REVOKE TRAVERSE ON GRAPH foo NODES A (*) FROM role
      case RevokePrivilege(TraversePrivilege(), _, database, labels, roleNames) =>
        (for (roleName <- roleNames; label <- labels.simplify) yield {
          roleName -> label
        }).foldLeft(Option.empty[plans.RevokeTraverse]) {
          case (source, (roleName, label)) => Some(plans.RevokeTraverse(source, database, label, roleName))
        }

      // GRANT READ (prop) ON GRAPH foo NODES A (*) TO role
      // GRANT MATCH (prop) ON GRAPH foo NODES A (*) TO role
      case GrantPrivilege(privilege, resources, database, labels, roleNames) =>
        val combos = for (roleName <- roleNames; label <- labels.simplify; resource <- resources.simplify) yield {
          roleName -> (label, resource)
        }
        val plan = privilege match {
          case ReadPrivilege() => Option.empty[plans.PrivilegePlan]
          case MatchPrivilege() => combos.foldLeft(Option.empty[plans.PrivilegePlan]) {
            case (source, (roleName, (label, _))) => Some(plans.GrantTraverse(source, database, label, roleName))
          }
        }
        combos.foldLeft(plan) {
          case (source, (roleName, (label, resource))) => Some(plans.GrantRead(source, resource, database, label, roleName))
        }

      // REVOKE READ (prop) ON GRAPH foo NODES A (*) FROM role
      // REVOKE MATCH (prop) ON GRAPH foo NODES A (*) FROM role
      case RevokePrivilege(_, resources, database, labels, roleNames) =>
        (for (roleName <- roleNames; label <- labels.simplify; resource <- resources.simplify) yield {
          roleName -> (label, resource)
        }).foldLeft(Option.empty[plans.RevokeRead]) {
          case (source, (roleName, (label, resource))) => Some(plans.RevokeRead(source, resource, database, label, roleName))
        }

      // SHOW [ALL | USER user | ROLE role] PRIVILEGES
      case ShowPrivileges(scope) =>
        Some(plans.ShowPrivileges(scope))

      // SHOW DATABASES
      case _: ShowDatabases =>
        Some(plans.ShowDatabases())

      // SHOW DATABASE foo
      case ShowDatabase(dbName) =>
        Some(plans.ShowDatabase(new DatabaseId(dbName)))

      // CREATE DATABASE foo
      case CreateDatabase(dbName) =>
        val dbId = new DatabaseId(dbName)
        NameValidator.assertValidDatabaseName(dbId.name())
        Some(plans.CreateDatabase(dbId))

      // DROP DATABASE foo
      case DropDatabase(dbName) =>
        val dbId = new DatabaseId(dbName)
        Some(plans.DropDatabase(
          Some(plans.EnsureValidNonDefaultDatabase(dbId, "drop")),
          dbId))

      // START DATABASE foo
      case StartDatabase(dbName) =>
        Some(plans.StartDatabase(new DatabaseId(dbName)))

      // STOP DATABASE foo
      case StopDatabase(dbName) =>
        val dbId = new DatabaseId(dbName)
        Some(plans.StopDatabase(
          Some(plans.EnsureValidNonDefaultDatabase(dbId, "stop")),
          dbId))

      case _ => None
    }

    val planState = LogicalPlanState(from)

    if (maybeLogicalPlan.isDefined)
      planState.copy(maybeLogicalPlan = maybeLogicalPlan, plannerName = ProcedurePlannerName)
    else planState
  }
}

case object UnsupportedSystemCommand extends Phase[PlannerContext, BaseState, LogicalPlanState] {
  override def phase: CompilationPhase = PIPE_BUILDING

  override def description: String = "Unsupported system command"

  override def postConditions: Set[Condition] = Set.empty

  override def process(from: BaseState, context: PlannerContext): LogicalPlanState = throw new RuntimeException(s"Not a recognised system command: ${from.queryText}")
}
