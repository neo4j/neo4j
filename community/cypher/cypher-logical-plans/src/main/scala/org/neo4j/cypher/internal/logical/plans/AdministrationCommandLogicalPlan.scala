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
import org.neo4j.cypher.internal.ast.ActionResource
import org.neo4j.cypher.internal.ast.AdminAction
import org.neo4j.cypher.internal.ast.GraphScope
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
import org.neo4j.cypher.internal.ast.ShowPrivilegeScope
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.ir.LazyMode
import org.neo4j.cypher.internal.ir.StrictnessMode
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.exceptions.DatabaseAdministrationException
import org.neo4j.exceptions.SecurityAdministrationException

abstract class AdministrationCommandLogicalPlan(source: Option[AdministrationCommandLogicalPlan] = None)(implicit idGen: IdGen) extends LogicalPlan(idGen) {
  override def lhs: Option[LogicalPlan] = source

  override def rhs: Option[LogicalPlan] = None

  override val availableSymbols: Set[String] = Set.empty

  override def strictness: StrictnessMode = LazyMode

  def invalid(message: String): RuntimeException
}

abstract class DatabaseAdministrationLogicalPlan(source: Option[AdministrationCommandLogicalPlan] = None)(implicit idGen: IdGen) extends AdministrationCommandLogicalPlan(source) {
  override def invalid(message: String): DatabaseAdministrationException = new DatabaseAdministrationException(message)
}

abstract class SecurityAdministrationLogicalPlan(source: Option[AdministrationCommandLogicalPlan] = None)(implicit idGen: IdGen) extends AdministrationCommandLogicalPlan(source) {
  override def invalid(message: String): SecurityAdministrationException = new SecurityAdministrationException(message)
}

// Security administration commands
case class ShowUsers(source: PrivilegePlan)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))
case class CreateUser(source: SecurityAdministrationLogicalPlan, userName: Either[String, Parameter], initialPassword: Either[Array[Byte], Parameter],
                      requirePasswordChange: Boolean, suspended: Option[Boolean])(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))
case class DropUser(source: SecurityAdministrationLogicalPlan, userName: Either[String, Parameter])(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))
case class AlterUser(source: PrivilegePlan, userName: Either[String, Parameter], initialPassword: Option[Either[Array[Byte], Parameter]],
                     requirePasswordChange: Option[Boolean], suspended: Option[Boolean])(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))
case class SetOwnPassword(newPassword: Either[Array[Byte], Parameter], currentPassword: Either[Array[Byte], Parameter])
                         (implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan
case class ShowRoles(source: PrivilegePlan, withUsers: Boolean, showAll: Boolean)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))
case class CreateRole(source: SecurityAdministrationLogicalPlan, roleName: Either[String, Parameter])(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))
case class DropRole(source: SecurityAdministrationLogicalPlan, roleName: Either[String, Parameter])(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))
case class GrantRoleToUser(source: SecurityAdministrationLogicalPlan, roleName: Either[String, Parameter], userName: Either[String, Parameter])(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))
case class RevokeRoleFromUser(source: SecurityAdministrationLogicalPlan, roleName: Either[String, Parameter], userNames: Either[String, Parameter])(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))
case class RequireRole(source: SecurityAdministrationLogicalPlan, name: Either[String, Parameter])(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))
case class CopyRolePrivileges(source: SecurityAdministrationLogicalPlan, to: Either[String, Parameter], from: Either[String, Parameter], grantDeny: String)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

abstract class PrivilegePlan(source: Option[PrivilegePlan] = None)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)

object AssertDbmsAdmin {
  def apply(action: AdminAction)(implicit idGen: IdGen): AssertDbmsAdmin = AssertDbmsAdmin(Seq(action))(idGen)
}
object AssertDbmsAdminOrSelf {
  def apply(user: Either[String, Parameter], action: AdminAction)(implicit idGen: IdGen): AssertDbmsAdminOrSelf = AssertDbmsAdminOrSelf(user, Seq(action))(idGen)
}
case class AssertDbmsAdmin(actions: Seq[AdminAction])(implicit idGen: IdGen) extends PrivilegePlan
case class AssertDbmsAdminOrSelf(user: Either[String, Parameter], actions: Seq[AdminAction])(implicit idGen: IdGen) extends PrivilegePlan
case class AssertDatabaseAdmin(action: AdminAction, database: NormalizedDatabaseName)(implicit idGen: IdGen) extends PrivilegePlan
case class AssertNotCurrentUser(source: PrivilegePlan, userName: Either[String, Parameter], verb: String, violationMessage: String)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class GrantDbmsAction(source: PrivilegePlan, action: AdminAction, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))
case class DenyDbmsAction(source: PrivilegePlan, action: AdminAction, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))
case class RevokeDbmsAction(source: PrivilegePlan, action: AdminAction, roleName: String, revokeType: String)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class GrantDatabaseAction(source: PrivilegePlan, action: AdminAction, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))
case class DenyDatabaseAction(source: PrivilegePlan, action: AdminAction, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))
case class RevokeDatabaseAction(source: PrivilegePlan, action: AdminAction, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String, revokeType: String)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class GrantTraverse(source: PrivilegePlan, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))
case class DenyTraverse(source: PrivilegePlan, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))
case class RevokeTraverse(source: PrivilegePlan, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String, revokeType: String)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class GrantRead(source: PrivilegePlan, resource: ActionResource, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))
case class DenyRead(source: PrivilegePlan, resource: ActionResource, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))
case class RevokeRead(source: PrivilegePlan, resource: ActionResource, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String, revokeType: String)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class GrantMatch(source: PrivilegePlan, resource: ActionResource, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))
case class DenyMatch(source: PrivilegePlan, resource: ActionResource, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))
case class RevokeMatch(source: PrivilegePlan, resource: ActionResource, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String, revokeType: String)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class GrantWrite(source: PrivilegePlan, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))
case class DenyWrite(source: PrivilegePlan, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))
case class RevokeWrite(source: PrivilegePlan, database: GraphScope, qualifier: PrivilegeQualifier, roleName: String, revokeType: String)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class ShowPrivileges(source: Option[PrivilegePlan], scope: ShowPrivilegeScope)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)

case class LogSystemCommand(source: AdministrationCommandLogicalPlan, command: String)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))
case class DoNothingIfNotExists(source: PrivilegePlan, label: String, name: Either[String, Parameter])(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))
case class DoNothingIfExists(source: PrivilegePlan, label: String, name: Either[String, Parameter])(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))
case class EnsureNodeExists(source: PrivilegePlan, label: String, name: Either[String, Parameter])(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

// Database administration commands
case class ShowDatabases()(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan
case class ShowDefaultDatabase()(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan
case class ShowDatabase(normalizedName: NormalizedDatabaseName)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan
case class CreateDatabase(source: AdministrationCommandLogicalPlan, normalizedName: NormalizedDatabaseName)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))
case class DropDatabase(source: AdministrationCommandLogicalPlan, normalizedName: NormalizedDatabaseName)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))
case class StartDatabase(source: AdministrationCommandLogicalPlan, normalizedName: NormalizedDatabaseName)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))
case class StopDatabase(source: AdministrationCommandLogicalPlan, normalizedName: NormalizedDatabaseName)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))
case class EnsureValidNonSystemDatabase(source: SecurityAdministrationLogicalPlan, normalizedName: NormalizedDatabaseName, action: String)(implicit idGen: IdGen)
  extends DatabaseAdministrationLogicalPlan(Some(source))
case class EnsureValidNumberOfDatabases(source: CreateDatabase)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))
