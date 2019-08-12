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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.configuration.helpers.NormalizedDatabaseName
import org.neo4j.cypher.{DatabaseAdministrationException, SecurityAdministrationException}
import org.neo4j.cypher.internal.ir.{LazyMode, StrictnessMode}
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.expressions.Parameter
import org.neo4j.cypher.internal.v4_0.util.attribution.IdGen

abstract class MultiDatabaseLogicalPlan(source: Option[MultiDatabaseLogicalPlan] = None)(implicit idGen: IdGen) extends LogicalPlan(idGen) {
  override def lhs: Option[LogicalPlan] = source

  override def rhs: Option[LogicalPlan] = None

  override val availableSymbols: Set[String] = Set.empty

  override def strictness: StrictnessMode = LazyMode

  def invalid(message: String): RuntimeException
}

abstract class DatabaseAdministrationLogicalPlan(implicit idGen: IdGen) extends MultiDatabaseLogicalPlan {
  override def invalid(message: String): DatabaseAdministrationException = new DatabaseAdministrationException(message)
}

abstract class SecurityAdministrationLogicalPlan(source: Option[MultiDatabaseLogicalPlan] = None)(implicit idGen: IdGen) extends MultiDatabaseLogicalPlan(source) {
  override def invalid(message: String): SecurityAdministrationException = new SecurityAdministrationException(message)
}

// Security administration commands
case class ShowUsers()(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan
case class CreateUser(userName: String, initialStringPassword: Option[Array[Byte]], initialParameterPassword: Option[Parameter],
                      requirePasswordChange: Boolean, suspended: Option[Boolean])(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan
case class DropUser(userName: String)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan
case class AlterUser(userName: String, initialStringPassword: Option[Array[Byte]], initialParameterPassword: Option[Parameter],
                     requirePasswordChange: Option[Boolean], suspended: Option[Boolean])(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan
case class SetOwnPassword(newStringPassword: Option[Array[Byte]], newParameterPassword: Option[Parameter],
                          currentStringPassword: Option[Array[Byte]], currentParameterPassword: Option[Parameter])(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan
case class ShowRoles(withUsers: Boolean, showAll: Boolean)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan
case class CreateRole(source: Option[SecurityAdministrationLogicalPlan], roleName: String)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)
case class DropRole(roleName: String)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan
case class GrantRoleToUser(source: Option[GrantRoleToUser], roleName: String, userName: String)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)
case class RevokeRoleFromUser(source: Option[RevokeRoleFromUser], roleName: String, userNames: String)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)
case class RequireRole(source: Option[SecurityAdministrationLogicalPlan], name: String)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)
case class CopyRolePrivileges(source: Option[SecurityAdministrationLogicalPlan], to: String, from: String, grantDeny: String)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)

abstract class PrivilegePlan()(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan
case class GrantTraverse(source: Option[PrivilegePlan], database: GraphScope, qualifier: PrivilegeQualifier, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan
case class DenyTraverse(source: Option[PrivilegePlan], database: GraphScope, qualifier: PrivilegeQualifier, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan
case class RevokeTraverse(source: Option[PrivilegePlan], database: GraphScope, qualifier: PrivilegeQualifier, roleName: String, revokeType: RevokeType)(implicit idGen: IdGen) extends PrivilegePlan

case class GrantRead(source: Option[PrivilegePlan], resource: ActionResource, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan
case class DenyRead(source: Option[PrivilegePlan], resource: ActionResource, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan
case class RevokeRead(source: Option[PrivilegePlan], resource: ActionResource, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String, revokeType: RevokeType)(implicit idGen: IdGen) extends PrivilegePlan

case class GrantWrite(source: Option[PrivilegePlan], resource: ActionResource, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan
case class DenyWrite(source: Option[PrivilegePlan], resource: ActionResource, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan
case class RevokeWrite(source: Option[PrivilegePlan], resource: ActionResource, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String, revokeType: RevokeType)(implicit idGen: IdGen) extends PrivilegePlan

case class ShowPrivileges(scope: ShowPrivilegeScope)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan

case class LogSystemCommand(source: LogicalPlan, command: String)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan

// Database administration commands
case class ShowDatabases()(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan
case class ShowDefaultDatabase()(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan
case class ShowDatabase(normalizedName: NormalizedDatabaseName)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan
case class CreateDatabase(normalizedName: NormalizedDatabaseName)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan
case class DropDatabase(source: Option[EnsureValidNonSystemDatabase], normalizedName: NormalizedDatabaseName)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan
case class StartDatabase(normalizedName: NormalizedDatabaseName)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan
case class StopDatabase(source: Option[EnsureValidNonSystemDatabase], normalizedName: NormalizedDatabaseName)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan
case class EnsureValidNonSystemDatabase(normalizedName: NormalizedDatabaseName, action: String)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan
