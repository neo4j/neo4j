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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ast.Access
import org.neo4j.cypher.internal.ast.ActionResource
import org.neo4j.cypher.internal.ast.AdministrationAction
import org.neo4j.cypher.internal.ast.DatabaseAction
import org.neo4j.cypher.internal.ast.DatabaseScope
import org.neo4j.cypher.internal.ast.DbmsAction
import org.neo4j.cypher.internal.ast.DropDatabaseAdditionalAction
import org.neo4j.cypher.internal.ast.GraphAction
import org.neo4j.cypher.internal.ast.GraphScope
import org.neo4j.cypher.internal.ast.HomeDatabaseAction
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ShowPrivilegeScope
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.WaitUntilComplete
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.exceptions.DatabaseAdministrationException
import org.neo4j.exceptions.SecurityAdministrationException

abstract class AdministrationCommandLogicalPlan(source: Option[AdministrationCommandLogicalPlan] = None)(implicit
idGen: IdGen) extends LogicalPlan(idGen) {
  override def lhs: Option[LogicalPlan] = source

  override def rhs: Option[LogicalPlan] = None

  val returnColumns: List[String] = List.empty

  override val availableSymbols: Set[String] = returnColumns.toSet

  def invalid(message: String): RuntimeException
}

abstract class DatabaseAdministrationLogicalPlan(source: Option[AdministrationCommandLogicalPlan] = None)(implicit
idGen: IdGen) extends AdministrationCommandLogicalPlan(source) {
  override def invalid(message: String): DatabaseAdministrationException = new DatabaseAdministrationException(message)
}

abstract class SecurityAdministrationLogicalPlan(source: Option[AdministrationCommandLogicalPlan] = None)(implicit
idGen: IdGen) extends AdministrationCommandLogicalPlan(source) {
  override def invalid(message: String): SecurityAdministrationException = new SecurityAdministrationException(message)
}

// Non-administration commands that are allowed on system database, e.g. SHOW PROCEDURES
case class AllowedNonAdministrationCommands(statement: Statement)(implicit idGen: IdGen)
    extends DatabaseAdministrationLogicalPlan

// Security administration commands
case class ShowUsers(
  source: PrivilegePlan,
  override val returnColumns: List[String],
  yields: Option[Yield],
  returns: Option[Return]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class ShowCurrentUser(override val returnColumns: List[String], yields: Option[Yield], returns: Option[Return])(
  implicit idGen: IdGen
) extends SecurityAdministrationLogicalPlan(None)

case class CreateUser(
  source: SecurityAdministrationLogicalPlan,
  userName: Either[String, Parameter],
  isEncryptedPassword: Boolean,
  initialPassword: Expression,
  requirePasswordChange: Boolean,
  suspended: Option[Boolean],
  defaultDatabase: Option[HomeDatabaseAction]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class RenameUser(
  source: SecurityAdministrationLogicalPlan,
  fromUserName: Either[String, Parameter],
  toUserName: Either[String, Parameter]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class DropUser(source: SecurityAdministrationLogicalPlan, userName: Either[String, Parameter])(implicit
idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class AlterUser(
  source: SecurityAdministrationLogicalPlan,
  userName: Either[String, Parameter],
  isEncryptedPassword: Option[Boolean],
  initialPassword: Option[Expression],
  requirePasswordChange: Option[Boolean],
  suspended: Option[Boolean],
  defaultDatabase: Option[HomeDatabaseAction]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class SetOwnPassword(newPassword: Expression, currentPassword: Expression)(implicit idGen: IdGen)
    extends SecurityAdministrationLogicalPlan

case class ShowRoles(
  source: PrivilegePlan,
  withUsers: Boolean,
  showAll: Boolean,
  override val returnColumns: List[String],
  yields: Option[Yield],
  returns: Option[Return]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class CreateRole(source: SecurityAdministrationLogicalPlan, roleName: Either[String, Parameter])(implicit
idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class RenameRole(
  source: SecurityAdministrationLogicalPlan,
  fromRoleName: Either[String, Parameter],
  toRoleName: Either[String, Parameter]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class DropRole(source: SecurityAdministrationLogicalPlan, roleName: Either[String, Parameter])(implicit
idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class GrantRoleToUser(
  source: SecurityAdministrationLogicalPlan,
  roleName: Either[String, Parameter],
  userName: Either[String, Parameter]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class RevokeRoleFromUser(
  source: SecurityAdministrationLogicalPlan,
  roleName: Either[String, Parameter],
  userNames: Either[String, Parameter]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class RequireRole(source: SecurityAdministrationLogicalPlan, name: Either[String, Parameter])(implicit
idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class CopyRolePrivileges(
  source: SecurityAdministrationLogicalPlan,
  to: Either[String, Parameter],
  from: Either[String, Parameter],
  grantDeny: String
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

abstract class PrivilegePlan(source: Option[PrivilegePlan] = None)(implicit idGen: IdGen)
    extends SecurityAdministrationLogicalPlan(source)

object AssertAllowedDbmsActions {

  def apply(maybeSource: PrivilegePlan, action: DbmsAction)(implicit idGen: IdGen): AssertAllowedDbmsActions =
    AssertAllowedDbmsActions(Some(maybeSource), Seq(action))(idGen)

  def apply(action: DbmsAction)(implicit idGen: IdGen): AssertAllowedDbmsActions =
    AssertAllowedDbmsActions(None, Seq(action))(idGen)
}

object AssertAllowedDbmsActionsOrSelf {

  def apply(user: Either[String, Parameter], action: DbmsAction)(implicit
  idGen: IdGen): AssertAllowedDbmsActionsOrSelf = AssertAllowedDbmsActionsOrSelf(user, Seq(action))(idGen)
}

case class AssertAllowedDbmsActions(maybeSource: Option[PrivilegePlan], actions: Seq[DbmsAction])(implicit idGen: IdGen)
    extends PrivilegePlan(maybeSource)

case class AssertAllowedDbmsActionsOrSelf(user: Either[String, Parameter], actions: Seq[DbmsAction])(implicit
idGen: IdGen) extends PrivilegePlan

case class AssertAllowedDatabaseAction(
  action: DatabaseAction,
  database: Either[String, Parameter],
  maybeSource: Option[PrivilegePlan]
)(implicit idGen: IdGen) extends PrivilegePlan(maybeSource)

case class AssertNotCurrentUser(
  source: PrivilegePlan,
  userName: Either[String, Parameter],
  verb: String,
  violationMessage: String
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))
case class AssertNotBlocked(action: AdministrationAction)(implicit idGen: IdGen) extends PrivilegePlan

case class GrantDbmsAction(
  source: PrivilegePlan,
  action: DbmsAction,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter]
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class DenyDbmsAction(
  source: PrivilegePlan,
  action: DbmsAction,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter]
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class RevokeDbmsAction(
  source: PrivilegePlan,
  action: DbmsAction,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter],
  revokeType: String
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class GrantDatabaseAction(
  source: PrivilegePlan,
  action: DatabaseAction,
  database: DatabaseScope,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter]
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class DenyDatabaseAction(
  source: PrivilegePlan,
  action: DatabaseAction,
  database: DatabaseScope,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter]
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class RevokeDatabaseAction(
  source: PrivilegePlan,
  action: DatabaseAction,
  database: DatabaseScope,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter],
  revokeType: String
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class GrantGraphAction(
  source: PrivilegePlan,
  action: GraphAction,
  resource: ActionResource,
  graph: GraphScope,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter]
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class DenyGraphAction(
  source: PrivilegePlan,
  action: GraphAction,
  resource: ActionResource,
  graph: GraphScope,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter]
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class RevokeGraphAction(
  source: PrivilegePlan,
  action: GraphAction,
  resoure: ActionResource,
  graph: GraphScope,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter],
  revokeType: String
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class ShowPrivileges(
  source: Option[PrivilegePlan],
  scope: ShowPrivilegeScope,
  override val returnColumns: List[String],
  yields: Option[Yield],
  returns: Option[Return]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)

case class ShowPrivilegeCommands(
  source: Option[PrivilegePlan],
  scope: ShowPrivilegeScope,
  asRevoke: Boolean,
  override val returnColumns: List[String],
  yields: Option[Yield],
  returns: Option[Return]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)

case class LogSystemCommand(source: AdministrationCommandLogicalPlan, command: String)(implicit idGen: IdGen)
    extends SecurityAdministrationLogicalPlan(Some(source))

case class DoNothingIfNotExists(
  source: PrivilegePlan,
  label: String,
  name: Either[String, Parameter],
  operation: String,
  valueMapper: String => String = s => s
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class DoNothingIfExists(
  source: PrivilegePlan,
  label: String,
  name: Either[String, Parameter],
  valueMapper: String => String = s => s
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class DoNothingIfDatabaseNotExists(
  source: PrivilegePlan,
  name: Either[String, Parameter],
  operation: String,
  valueMapper: String => String = s => s
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class DoNothingIfDatabaseExists(
  source: PrivilegePlan,
  name: Either[String, Parameter],
  valueMapper: String => String = s => s
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class EnsureNodeExists(
  source: PrivilegePlan,
  label: String,
  name: Either[String, Parameter],
  valueMapper: String => String = s => s,
  extraFilter: String => String = s => "",
  labelDescription: String,
  action: String
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

// Database administration commands
case class ShowDatabase(
  scope: DatabaseScope,
  verbose: Boolean,
  override val returnColumns: List[String],
  yields: Option[Yield],
  returns: Option[Return]
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan

case class CreateDatabase(
  source: AdministrationCommandLogicalPlan,
  databaseName: Either[String, Parameter],
  options: Options
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class DropDatabase(
  source: AdministrationCommandLogicalPlan,
  databaseName: Either[String, Parameter],
  additionalAction: DropDatabaseAdditionalAction
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class AlterDatabase(
  source: AdministrationCommandLogicalPlan,
  databaseName: Either[String, Parameter],
  access: Access
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class StartDatabase(source: AdministrationCommandLogicalPlan, databaseName: Either[String, Parameter])(implicit
idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class StopDatabase(source: AdministrationCommandLogicalPlan, databaseName: Either[String, Parameter])(implicit
idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class CreateDatabaseAlias(
  source: AdministrationCommandLogicalPlan,
  aliasName: Either[String, Parameter],
  targetName: Either[String, Parameter],
  replace: Boolean
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class DropDatabaseAlias(source: AdministrationCommandLogicalPlan, aliasName: Either[String, Parameter])(implicit
idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class AlterDatabaseAlias(
  source: AdministrationCommandLogicalPlan,
  aliasName: Either[String, Parameter],
  targetName: Either[String, Parameter]
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class EnsureValidNonSystemDatabase(
  source: AdministrationCommandLogicalPlan,
  databaseName: Either[String, Parameter],
  action: String,
  aliasName: Option[Either[String, Parameter]] = None
)(implicit idGen: IdGen)
    extends DatabaseAdministrationLogicalPlan(Some(source))

case class EnsureDatabaseHasNoAliases(
  source: AdministrationCommandLogicalPlan,
  databaseName: Either[String, Parameter]
)(implicit idGen: IdGen)
    extends DatabaseAdministrationLogicalPlan(Some(source))

case class EnsureValidNumberOfDatabases(source: CreateDatabase)(implicit idGen: IdGen)
    extends DatabaseAdministrationLogicalPlan(Some(source))

case class WaitForCompletion(
  source: AdministrationCommandLogicalPlan,
  databaseName: Either[String, Parameter],
  waitForCompletion: WaitUntilComplete
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))
