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

import org.neo4j.cypher.internal.ast.ActionResource
import org.neo4j.cypher.internal.ast.AdminAction
import org.neo4j.cypher.internal.ast.DatabaseScope
import org.neo4j.cypher.internal.ast.DropDatabaseAdditionalAction
import org.neo4j.cypher.internal.ast.GraphAction
import org.neo4j.cypher.internal.ast.GraphScope
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ShowPrivilegeScope
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.ir.LazyMode
import org.neo4j.cypher.internal.ir.StrictnessMode
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.exceptions.DatabaseAdministrationException
import org.neo4j.exceptions.SecurityAdministrationException

abstract class AdministrationCommandLogicalPlan(source: Option[AdministrationCommandLogicalPlan] = None)(implicit idGen: IdGen) extends LogicalPlan(idGen) {
  override def lhs: Option[LogicalPlan] = source

  override def rhs: Option[LogicalPlan] = None

  val returnColumns: List[String] = List.empty

  override val availableSymbols: Set[String] = returnColumns.toSet

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
case class ShowUsers(source: PrivilegePlan, override val returnColumns: List[String], yields: Option[Return], where: Option[Where], returns: Option[Return])
                    (implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class CreateUser(source: SecurityAdministrationLogicalPlan, userName: Either[String, Parameter], initialPassword: Expression,
                      requirePasswordChange: Boolean, suspended: Option[Boolean])(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))
case class DropUser(source: SecurityAdministrationLogicalPlan, userName: Either[String, Parameter])(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))
case class AlterUser(source: PrivilegePlan, userName: Either[String, Parameter], initialPassword: Option[Expression],
                     requirePasswordChange: Option[Boolean], suspended: Option[Boolean])(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))
case class SetOwnPassword(newPassword: Expression, currentPassword: Expression)
                         (implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan

case class ShowRoles(source: PrivilegePlan,
                     withUsers: Boolean,
                     showAll: Boolean,
                     override val returnColumns: List[String],
                     yields: Option[Return],
                     where: Option[Where],
                     returns: Option[Return])(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

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
case class AssertDatabaseAdmin(action: AdminAction, database: Either[String, Parameter])(implicit idGen: IdGen) extends PrivilegePlan
case class AssertNotCurrentUser(source: PrivilegePlan, userName: Either[String, Parameter], verb: String, violationMessage: String)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class GrantDbmsAction(source: PrivilegePlan, action: AdminAction, roleName: Either[String, Parameter])(implicit idGen: IdGen) extends PrivilegePlan(Some(source))
case class DenyDbmsAction(source: PrivilegePlan, action: AdminAction, roleName: Either[String, Parameter])(implicit idGen: IdGen) extends PrivilegePlan(Some(source))
case class RevokeDbmsAction(source: PrivilegePlan, action: AdminAction, roleName: Either[String, Parameter], revokeType: String)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class GrantDatabaseAction(source: PrivilegePlan, action: AdminAction, database: DatabaseScope, qualifier: PrivilegeQualifier, roleName: Either[String, Parameter])(implicit idGen: IdGen) extends PrivilegePlan(Some(source))
case class DenyDatabaseAction(source: PrivilegePlan, action: AdminAction, database: DatabaseScope, qualifier: PrivilegeQualifier, roleName: Either[String, Parameter])(implicit idGen: IdGen) extends PrivilegePlan(Some(source))
case class RevokeDatabaseAction(source: PrivilegePlan, action: AdminAction, database: DatabaseScope, qualifier: PrivilegeQualifier, roleName: Either[String, Parameter], revokeType: String)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class GrantGraphAction(source: PrivilegePlan, action: GraphAction, resource: ActionResource, graph: GraphScope, qualifier: PrivilegeQualifier, roleName: Either[String, Parameter])(implicit idGen: IdGen) extends PrivilegePlan(Some(source))
case class DenyGraphAction(source: PrivilegePlan, action: GraphAction, resource: ActionResource, graph: GraphScope, qualifier: PrivilegeQualifier, roleName: Either[String, Parameter])(implicit idGen: IdGen) extends PrivilegePlan(Some(source))
case class RevokeGraphAction(source: PrivilegePlan, action: GraphAction, resoure: ActionResource, graph: GraphScope, qualifier: PrivilegeQualifier, roleName: Either[String, Parameter], revokeType: String)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class ShowPrivileges(source: Option[PrivilegePlan],
                          scope: ShowPrivilegeScope,
                          override val returnColumns: List[String],
                          yields: Option[Return],
                          where: Option[Where],
                          returns: Option[Return])(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)

case class LogSystemCommand(source: AdministrationCommandLogicalPlan, command: String)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))
case class DoNothingIfNotExists(source: PrivilegePlan, label: String, name: Either[String, Parameter], valueMapper: String => String = s => s)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))
case class DoNothingIfExists(source: PrivilegePlan, label: String, name: Either[String, Parameter], valueMapper: String => String = s => s)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))
case class EnsureNodeExists(source: PrivilegePlan, label: String, name: Either[String, Parameter], valueMapper: String => String = s => s)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

// Database administration commands
case class ShowDatabase(scope: DatabaseScope,
                        override val returnColumns: List[String],
                        yields: Option[Return],
                        where: Option[Where],
                        returns: Option[Return])(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan

case class CreateDatabase(source: AdministrationCommandLogicalPlan, databaseName: Either[String, Parameter])(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))
case class DropDatabase(source: AdministrationCommandLogicalPlan, databaseName: Either[String, Parameter], additionalAction: DropDatabaseAdditionalAction)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))
case class StartDatabase(source: AdministrationCommandLogicalPlan, databaseName: Either[String, Parameter])(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))
case class StopDatabase(source: AdministrationCommandLogicalPlan, databaseName: Either[String, Parameter])(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))
case class EnsureValidNonSystemDatabase(source: SecurityAdministrationLogicalPlan, databaseName: Either[String, Parameter], action: String)(implicit idGen: IdGen)
  extends DatabaseAdministrationLogicalPlan(Some(source))
case class EnsureValidNumberOfDatabases(source: CreateDatabase)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))
