/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ast.Access
import org.neo4j.cypher.internal.ast.ActionResource
import org.neo4j.cypher.internal.ast.AdministrationAction
import org.neo4j.cypher.internal.ast.DataExchangeAction
import org.neo4j.cypher.internal.ast.DatabaseAction
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.DatabaseScope
import org.neo4j.cypher.internal.ast.DbmsAction
import org.neo4j.cypher.internal.ast.DropDatabaseAdditionalAction
import org.neo4j.cypher.internal.ast.ExternalAuth
import org.neo4j.cypher.internal.ast.GraphAction
import org.neo4j.cypher.internal.ast.HomeDatabaseAction
import org.neo4j.cypher.internal.ast.IfExistsDo
import org.neo4j.cypher.internal.ast.NativeAuth
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
import org.neo4j.cypher.internal.ast.RemoveAuth
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ShowPrivilegeScope
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Topology
import org.neo4j.cypher.internal.ast.WaitUntilComplete
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.logical.plans.DatabaseTypeFilter.All
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.exceptions.DatabaseAdministrationException
import org.neo4j.exceptions.NotSystemDatabaseException
import org.neo4j.exceptions.SecurityAdministrationException

abstract class AdministrationCommandLogicalPlan(
  source: Option[AdministrationCommandLogicalPlan] = None
)(implicit idGen: IdGen) extends LogicalPlanExtension(idGen) {
  override def lhs: Option[LogicalPlan] = source

  override def rhs: Option[LogicalPlan] = None

  val returnColumns: List[LogicalVariable] = List.empty

  override val availableSymbols: Set[LogicalVariable] = returnColumns.toSet

  def invalid(message: String): RuntimeException
}

abstract class DatabaseAdministrationLogicalPlan(source: Option[AdministrationCommandLogicalPlan] = None)(implicit
idGen: IdGen) extends AdministrationCommandLogicalPlan(source) {
  override def invalid(message: String): DatabaseAdministrationException = new NotSystemDatabaseException(message)
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
  override val returnColumns: List[LogicalVariable],
  yields: Option[Yield],
  returns: Option[Return]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class ShowCurrentUser(
  override val returnColumns: List[LogicalVariable],
  yields: Option[Yield],
  returns: Option[Return]
)(
  implicit idGen: IdGen
) extends SecurityAdministrationLogicalPlan(None)

case class CreateUser(
  source: SecurityAdministrationLogicalPlan,
  userName: Either[String, Parameter],
  suspended: Option[Boolean],
  defaultDatabase: Option[HomeDatabaseAction],
  externalAuths: Seq[ExternalAuth],
  nativeAuth: Option[NativeAuth]
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
  suspended: Option[Boolean],
  defaultDatabase: Option[HomeDatabaseAction],
  nativeAuth: Option[NativeAuth],
  externalAuths: Seq[ExternalAuth],
  removeAuth: RemoveAuth
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class SetOwnPassword(newPassword: Expression, currentPassword: Expression)(implicit idGen: IdGen)
    extends SecurityAdministrationLogicalPlan

case class ShowRoles(
  source: PrivilegePlan,
  withUsers: Boolean,
  showAll: Boolean,
  override val returnColumns: List[LogicalVariable],
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
  userName: Either[String, Parameter],
  command: String
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class RevokeRoleFromUser(
  source: SecurityAdministrationLogicalPlan,
  roleName: Either[String, Parameter],
  userName: Either[String, Parameter],
  command: String
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class RequireRole(source: SecurityAdministrationLogicalPlan, name: Either[String, Parameter])(implicit
idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class CopyRolePrivileges(
  source: SecurityAdministrationLogicalPlan,
  to: Either[String, Parameter],
  from: Either[String, Parameter],
  grantDeny: String
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class AssertAllRolePrivilegesCanBeCopied(
  source: SecurityAdministrationLogicalPlan,
  roleName: Either[String, Parameter]
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

case class AssertCanDropDatabase(
  source: PrivilegePlan,
  namespacedName: DatabaseName,
  defaultAction: DbmsAction
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class AssertAllowedDbmsActionsOrSelf(user: Either[String, Parameter], actions: Seq[DbmsAction])(implicit
idGen: IdGen) extends PrivilegePlan

case class AssertAllowedDatabaseAction(
  action: DatabaseAction,
  database: DatabaseName,
  maybeSource: Option[PrivilegePlan]
)(implicit idGen: IdGen) extends PrivilegePlan(maybeSource)

case class AssertNotCurrentUser(
  source: PrivilegePlan,
  userName: Either[String, Parameter],
  verb: String,
  violationMessage: String
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class AssertManagementActionNotBlocked(action: AdministrationAction)(implicit idGen: IdGen) extends PrivilegePlan
case class AssertNotBlockedRemoteAliasManagement()(implicit idGen: IdGen) extends PrivilegePlan

case class AssertNotBlockedDropAlias(aliasName: DatabaseName)(implicit idGen: IdGen)
    extends PrivilegePlan

case class GrantDbmsAction(
  source: PrivilegePlan,
  action: DbmsAction,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter],
  immutable: Boolean,
  command: String
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class DenyDbmsAction(
  source: PrivilegePlan,
  action: DbmsAction,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter],
  immutable: Boolean,
  command: String
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class RevokeDbmsAction(
  source: PrivilegePlan,
  action: DbmsAction,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter],
  revokeType: String,
  immutableOnly: Boolean,
  command: String
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class AssertDbmsPrivilegeCanBeMutated(
  source: PrivilegePlan,
  action: DbmsAction,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter],
  revokeType: String
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class GrantDatabaseAction(
  source: PrivilegePlan,
  action: DatabaseAction,
  database: PrivilegeCommandScope,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter],
  immutable: Boolean,
  command: String
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class DenyDatabaseAction(
  source: PrivilegePlan,
  action: DatabaseAction,
  database: PrivilegeCommandScope,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter],
  immutable: Boolean,
  command: String
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class RevokeDatabaseAction(
  source: PrivilegePlan,
  action: DatabaseAction,
  database: PrivilegeCommandScope,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter],
  revokeType: String,
  immutableOnly: Boolean,
  command: String
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class AssertDatabasePrivilegeCanBeMutated(
  source: PrivilegePlan,
  action: DatabaseAction,
  database: PrivilegeCommandScope,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter],
  revokeType: String
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class GrantGraphAction(
  source: PrivilegePlan,
  action: GraphAction,
  resource: ActionResource,
  graph: PrivilegeCommandScope,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter],
  immutable: Boolean,
  command: String
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class DenyGraphAction(
  source: PrivilegePlan,
  action: GraphAction,
  resource: ActionResource,
  graph: PrivilegeCommandScope,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter],
  immutable: Boolean,
  command: String
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class RevokeGraphAction(
  source: PrivilegePlan,
  action: GraphAction,
  resource: ActionResource,
  graph: PrivilegeCommandScope,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter],
  revokeType: String,
  immutableOnly: Boolean,
  command: String
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class AssertGraphPrivilegeCanBeMutated(
  source: PrivilegePlan,
  action: GraphAction,
  resource: ActionResource,
  graph: PrivilegeCommandScope,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter],
  revokeType: String
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class GrantLoadAction(
  source: PrivilegePlan,
  action: DataExchangeAction,
  resource: ActionResource,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter],
  immutable: Boolean,
  command: String
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class DenyLoadAction(
  source: PrivilegePlan,
  action: DataExchangeAction,
  resource: ActionResource,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter],
  immutable: Boolean,
  command: String
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class RevokeLoadAction(
  source: PrivilegePlan,
  action: DataExchangeAction,
  resource: ActionResource,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter],
  revokeType: String,
  immutableOnly: Boolean,
  command: String
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class AssertLoadPrivilegeCanBeMutated(
  source: PrivilegePlan,
  action: DataExchangeAction,
  resource: ActionResource,
  qualifier: PrivilegeQualifier,
  roleName: Either[String, Parameter],
  revokeType: String
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class AssertDbmsActionIsAssignable(
  source: Option[PrivilegePlan],
  action: DbmsAction
)(implicit idGen: IdGen) extends PrivilegePlan(source)

case class ShowSupportedPrivileges(
  override val returnColumns: List[LogicalVariable],
  yields: Option[Yield],
  returns: Option[Return]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(None)

case class ShowPrivileges(
  source: Option[PrivilegePlan],
  scope: ShowPrivilegeScope,
  override val returnColumns: List[LogicalVariable],
  yields: Option[Yield],
  returns: Option[Return]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)

case class ShowPrivilegeCommands(
  source: Option[PrivilegePlan],
  scope: ShowPrivilegeScope,
  asRevoke: Boolean,
  override val returnColumns: List[LogicalVariable],
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
  name: DatabaseName,
  operation: String,
  databaseTypeFilter: DatabaseTypeFilter = All
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class DoNothingIfDatabaseExists(
  source: PrivilegePlan,
  name: DatabaseName,
  databaseTypeFilter: DatabaseTypeFilter = All
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class EnsureNodeExists(
  source: PrivilegePlan,
  label: String,
  name: Either[String, Parameter],
  valueMapper: String => String = s => s,
  extraFilter: String => String = _ => "",
  labelDescription: String,
  action: String
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class EnsureDatabaseNodeExists(
  source: PrivilegePlan,
  name: DatabaseName,
  extraFilter: String => String = _ => "",
  action: String
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

// Database administration commands
case class ShowDatabase(
  scope: DatabaseScope,
  verbose: Boolean,
  override val returnColumns: List[LogicalVariable],
  yields: Option[Yield],
  returns: Option[Return]
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan

case class CreateDatabase(
  source: AdministrationCommandLogicalPlan,
  databaseName: Either[String, Parameter],
  options: Options,
  ifExistsDo: IfExistsDo,
  isComposite: Boolean,
  topology: Option[Topology]
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class DropDatabase(
  source: AdministrationCommandLogicalPlan,
  databaseName: DatabaseName,
  additionalAction: DropDatabaseAdditionalAction,
  forceComposite: Boolean
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class AlterDatabase(
  source: AdministrationCommandLogicalPlan,
  databaseName: DatabaseName,
  access: Option[Access],
  topology: Option[Topology],
  options: Options,
  optionsToRemove: Set[String]
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class StartDatabase(source: AdministrationCommandLogicalPlan, databaseName: DatabaseName)(
  implicit idGen: IdGen
) extends DatabaseAdministrationLogicalPlan(Some(source))

case class StopDatabase(source: AdministrationCommandLogicalPlan, databaseName: DatabaseName)(
  implicit idGen: IdGen
) extends DatabaseAdministrationLogicalPlan(Some(source))

case class CreateLocalDatabaseAlias(
  source: AdministrationCommandLogicalPlan,
  aliasName: DatabaseName,
  targetName: DatabaseName,
  properties: Option[Either[Map[String, Expression], Parameter]],
  replace: Boolean
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class CreateRemoteDatabaseAlias(
  source: AdministrationCommandLogicalPlan,
  aliasName: DatabaseName,
  targetName: DatabaseName,
  replace: Boolean,
  url: Either[String, Parameter],
  username: Either[String, Parameter],
  password: Expression,
  driverSettings: Option[Either[Map[String, Expression], Parameter]],
  properties: Option[Either[Map[String, Expression], Parameter]]
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class DropDatabaseAlias(source: AdministrationCommandLogicalPlan, aliasName: DatabaseName)(
  implicit idGen: IdGen
) extends DatabaseAdministrationLogicalPlan(Some(source))

case class AlterLocalDatabaseAlias(
  source: Option[AdministrationCommandLogicalPlan],
  aliasName: DatabaseName,
  targetName: Option[DatabaseName],
  properties: Option[Either[Map[String, Expression], Parameter]]
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(source)

case class AlterRemoteDatabaseAlias(
  source: AdministrationCommandLogicalPlan,
  aliasName: DatabaseName,
  targetName: Option[DatabaseName],
  url: Option[Either[String, Parameter]],
  username: Option[Either[String, Parameter]],
  password: Option[Expression],
  driverSettings: Option[Either[Map[String, Expression], Parameter]],
  properties: Option[Either[Map[String, Expression], Parameter]]
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class ShowAliases(
  source: AdministrationCommandLogicalPlan,
  aliasName: Option[DatabaseName],
  verbose: Boolean,
  override val returnColumns: List[LogicalVariable],
  yields: Option[Yield],
  returns: Option[Return]
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan

case class EnsureValidNonSystemDatabase(
  source: AdministrationCommandLogicalPlan,
  databaseName: DatabaseName,
  action: String,
  aliasName: Option[DatabaseName] = None
)(implicit idGen: IdGen)
    extends DatabaseAdministrationLogicalPlan(Some(source))

case class EnsureDatabaseSafeToDelete(
  source: AdministrationCommandLogicalPlan,
  databaseName: DatabaseName
)(implicit idGen: IdGen)
    extends DatabaseAdministrationLogicalPlan(Some(source))

case class EnsureAliasIsNotRemote(source: AdministrationCommandLogicalPlan, aliasName: Either[String, Parameter])(
  implicit idGen: IdGen
) extends DatabaseAdministrationLogicalPlan(Some(source))

case class EnsureNameIsNotAmbiguous(
  source: AdministrationCommandLogicalPlan,
  databaseName: Either[String, Parameter],
  isComposite: Boolean
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class EnableServer(
  source: AdministrationCommandLogicalPlan,
  serverName: Either[String, Parameter],
  options: Options
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class AlterServer(
  source: AdministrationCommandLogicalPlan,
  serverName: Either[String, Parameter],
  options: Options
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class RenameServer(
  source: AdministrationCommandLogicalPlan,
  serverName: Either[String, Parameter],
  newName: Either[String, Parameter]
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class DropServer(
  source: AdministrationCommandLogicalPlan,
  serverName: Either[String, Parameter]
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class ShowServers(
  source: AdministrationCommandLogicalPlan,
  verbose: Boolean,
  override val returnColumns: List[LogicalVariable],
  yields: Option[Yield],
  returns: Option[Return]
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan

case class DeallocateServer(
  source: AdministrationCommandLogicalPlan,
  dryRun: Boolean,
  serverNames: Seq[Either[String, Parameter]]
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class ReallocateDatabases(source: AdministrationCommandLogicalPlan, dryRun: Boolean)(implicit idGen: IdGen)
    extends DatabaseAdministrationLogicalPlan(Some(source))

case class EnsureValidNumberOfDatabases(source: CreateDatabase)(implicit idGen: IdGen)
    extends DatabaseAdministrationLogicalPlan(Some(source))

case class WaitForCompletion(
  source: AdministrationCommandLogicalPlan,
  databaseName: DatabaseName,
  waitForCompletion: WaitUntilComplete
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

sealed trait DatabaseTypeFilter

object DatabaseTypeFilter {
  case object All extends DatabaseTypeFilter

  case object CompositeDatabase extends DatabaseTypeFilter

  case object DatabaseOrLocalAlias extends DatabaseTypeFilter

  case object Alias extends DatabaseTypeFilter
}
