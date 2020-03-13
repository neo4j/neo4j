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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.configuration.helpers.NormalizedDatabaseName
import org.neo4j.cypher.internal.ir.{LazyMode, StrictnessMode}
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.expressions.Parameter
import org.neo4j.cypher.internal.v4_0.util.attribution.IdGen
import org.neo4j.exceptions.{DatabaseAdministrationException, SecurityAdministrationException}

abstract class MultiDatabaseLogicalPlan(source: Option[MultiDatabaseLogicalPlan] = None)(implicit idGen: IdGen) extends LogicalPlan(idGen) {
  override def lhs: Option[LogicalPlan] = source

  override def rhs: Option[LogicalPlan] = None

  override val availableSymbols: Set[String] = Set.empty

  override def strictness: StrictnessMode = LazyMode

  def invalid(message: String): RuntimeException
}

abstract class DatabaseAdministrationLogicalPlan(source: Option[MultiDatabaseLogicalPlan] = None)(implicit idGen: IdGen) extends MultiDatabaseLogicalPlan(source) {
  override def invalid(message: String): DatabaseAdministrationException = new DatabaseAdministrationException(message)
}

abstract class SecurityAdministrationLogicalPlan(source: Option[MultiDatabaseLogicalPlan] = None)(implicit idGen: IdGen) extends MultiDatabaseLogicalPlan(source) {
  override def invalid(message: String): SecurityAdministrationException = new SecurityAdministrationException(message)
}

// Security administration commands
case class ShowUsers(source: Option[PrivilegePlan])(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)
case class CreateUser(source: Option[SecurityAdministrationLogicalPlan], userName: String, initialStringPassword: Option[Array[Byte]], initialParameterPassword: Option[Parameter],
                      requirePasswordChange: Boolean, suspended: Option[Boolean])(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)
case class DropUser(source: Option[SecurityAdministrationLogicalPlan], userName: String)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)
case class AlterUser(source: Option[PrivilegePlan], userName: String, initialStringPassword: Option[Array[Byte]], initialParameterPassword: Option[Parameter],
                     requirePasswordChange: Option[Boolean], suspended: Option[Boolean])(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)
case class SetOwnPassword(newStringPassword: Option[Array[Byte]], newParameterPassword: Option[Parameter],
                          currentStringPassword: Option[Array[Byte]], currentParameterPassword: Option[Parameter])(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan
case class ShowRoles(source: Option[PrivilegePlan], withUsers: Boolean, showAll: Boolean)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)
case class CreateRole(source: Option[SecurityAdministrationLogicalPlan], roleName: String)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)
case class DropRole(source: Option[SecurityAdministrationLogicalPlan], roleName: String)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)
case class GrantRoleToUser(source: Option[SecurityAdministrationLogicalPlan], roleName: String, userName: String)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)
case class RevokeRoleFromUser(source: Option[SecurityAdministrationLogicalPlan], roleName: String, userNames: String)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)
case class RequireRole(source: Option[SecurityAdministrationLogicalPlan], name: String)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)
case class CopyRolePrivileges(source: Option[SecurityAdministrationLogicalPlan], to: String, from: String, grantDeny: String)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)

abstract class PrivilegePlan(source: Option[PrivilegePlan] = None)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)

case class CheckFrozenRole(source: Option[PrivilegePlan], roleName: String)(implicit idGen: IdGen) extends PrivilegePlan(source)

case class AssertDbmsAdmin(actions: AdminAction*)(implicit idGen: IdGen) extends PrivilegePlan
case class AssertDatabaseAdmin(action: AdminAction, database: NormalizedDatabaseName)(implicit idGen: IdGen) extends PrivilegePlan
case class AssertNotCurrentUser(source: Option[PrivilegePlan], userName: String, violationMessage: String)(implicit idGen: IdGen) extends PrivilegePlan(source)
case class AssertValidRevoke(source: Option[PrivilegePlan], action: AdminAction, scope: GraphScope, roleName: String, revokeType: RevokeType)(implicit idGen: IdGen) extends PrivilegePlan(source)

case class GrantDbmsAction(source: Option[PrivilegePlan], action: AdminAction, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan(source)
case class DenyDbmsAction(source: Option[PrivilegePlan], action: AdminAction, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan(source)
case class RevokeDbmsAction(source: Option[PrivilegePlan], action: AdminAction, roleName: String, revokeType: String)(implicit idGen: IdGen) extends PrivilegePlan(source)

case class GrantDatabaseAction(source: Option[PrivilegePlan], action: AdminAction, database: GraphScope, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan(source)
case class DenyDatabaseAction(source: Option[PrivilegePlan], action: AdminAction, database: GraphScope, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan(source)
case class RevokeDatabaseAction(source: Option[PrivilegePlan], action: AdminAction, database: GraphScope, roleName: String, revokeType: String)(implicit idGen: IdGen) extends PrivilegePlan(source)

case class GrantTraverse(source: Option[PrivilegePlan], database: GraphScope, qualifier: PrivilegeQualifier, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan(source)
case class DenyTraverse(source: Option[PrivilegePlan], database: GraphScope, qualifier: PrivilegeQualifier, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan(source)
case class RevokeTraverse(source: Option[PrivilegePlan], database: GraphScope, qualifier: PrivilegeQualifier, roleName: String, revokeType: String)(implicit idGen: IdGen) extends PrivilegePlan(source)

case class GrantRead(source: Option[PrivilegePlan], resource: ActionResource, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan(source)
case class DenyRead(source: Option[PrivilegePlan], resource: ActionResource, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan(source)
case class RevokeRead(source: Option[PrivilegePlan], resource: ActionResource, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String, revokeType: String)(implicit idGen: IdGen) extends PrivilegePlan(source)

case class GrantWrite(source: Option[PrivilegePlan], resource: ActionResource, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan(source)
case class DenyWrite(source: Option[PrivilegePlan], resource: ActionResource, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan(source)
case class RevokeWrite(source: Option[PrivilegePlan], resource: ActionResource, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String, revokeType: String)(implicit idGen: IdGen) extends PrivilegePlan(source)

case class ShowPrivileges(source: Option[PrivilegePlan], scope: ShowPrivilegeScope)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)

case class LogSystemCommand(source: MultiDatabaseLogicalPlan, command: String)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))
case class DoNothingIfNotExists(source: Option[PrivilegePlan], label: String, name: String)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)
case class DoNothingIfExists(source: Option[PrivilegePlan], label: String, name: String)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)
case class EnsureNodeExists(source: Option[PrivilegePlan], label: String, name: String)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)

// Database administration commands
case class ShowDatabases()(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan
case class ShowDefaultDatabase()(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan
case class ShowDatabase(normalizedName: NormalizedDatabaseName)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan
case class CreateDatabase(source: Option[MultiDatabaseLogicalPlan], normalizedName: NormalizedDatabaseName)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(source)
case class DropDatabase(source: Option[MultiDatabaseLogicalPlan], normalizedName: NormalizedDatabaseName)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(source)
case class StartDatabase(source: Option[MultiDatabaseLogicalPlan], normalizedName: NormalizedDatabaseName)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(source)
case class StopDatabase(source: Option[MultiDatabaseLogicalPlan], normalizedName: NormalizedDatabaseName)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(source)
case class EnsureValidNonSystemDatabase(source: Option[SecurityAdministrationLogicalPlan], normalizedName: NormalizedDatabaseName, action: String)(implicit idGen: IdGen)
  extends DatabaseAdministrationLogicalPlan(source)
case class EnsureValidNumberOfDatabases(source: Option[CreateDatabase])(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(source)
