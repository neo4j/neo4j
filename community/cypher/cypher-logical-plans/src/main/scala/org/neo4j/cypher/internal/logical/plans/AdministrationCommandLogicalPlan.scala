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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.Access
import org.neo4j.cypher.internal.ast.ActionResource
import org.neo4j.cypher.internal.ast.AdministrationAction
import org.neo4j.cypher.internal.ast.DataExchangeAction
import org.neo4j.cypher.internal.ast.DatabaseAction
import org.neo4j.cypher.internal.ast.DatabaseAndDbmsAction
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.DbmsAction
import org.neo4j.cypher.internal.ast.DropDatabaseAdditionalAction
import org.neo4j.cypher.internal.ast.DropDatabaseAliasAction
import org.neo4j.cypher.internal.ast.ExternalAuth
import org.neo4j.cypher.internal.ast.GraphAction
import org.neo4j.cypher.internal.ast.HomeDatabaseAction
import org.neo4j.cypher.internal.ast.IfExistsDo
import org.neo4j.cypher.internal.ast.NativeAuth
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
import org.neo4j.cypher.internal.ast.RemoteAliasCredentials
import org.neo4j.cypher.internal.ast.RemoveAuth
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.SetTags
import org.neo4j.cypher.internal.ast.ShardDefinition
import org.neo4j.cypher.internal.ast.ShowPrivilegeScope
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Topology
import org.neo4j.cypher.internal.ast.UserTagsAction
import org.neo4j.cypher.internal.ast.WaitUntilComplete
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.logical.plans.DatabaseTypeFilter.All
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.exceptions.InternalException
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.graphdb.security.AuthorizationViolationException

abstract class AdministrationCommandLogicalPlan(
  source: Option[AdministrationCommandLogicalPlan] = None
)(implicit idGen: IdGen) extends LogicalPlanExtension(idGen) {
  override def lhs: Option[LogicalPlan] = source

  override def rhs: Option[LogicalPlan] = None

  val returnColumns: List[LogicalVariable] = List.empty

  override val localAvailableSymbols: Set[LogicalVariable] = returnColumns.toSet

}

abstract class DatabaseAdministrationLogicalPlan(source: Option[AdministrationCommandLogicalPlan] = None)(implicit
  idGen: IdGen) extends AdministrationCommandLogicalPlan(source)

abstract class SecurityAdministrationLogicalPlan(source: Option[AdministrationCommandLogicalPlan] = None)(implicit
  idGen: IdGen) extends AdministrationCommandLogicalPlan(source)

// Non-administration commands that are allowed on system database, e.g. SHOW PROCEDURES
// The `statement` is used to run the query, it gets turned back into a query string at runtime.
// The `maybePlan` is just used to get a nicer plan description than `AdministrationCommand`.
case class AllowedNonAdministrationCommands(
  statement: Statement,
  maybePlan: Option[LogicalPlan] = None
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan {
  def addPlan(newPlan: LogicalPlan): AllowedNonAdministrationCommands = this.copy(maybePlan = Some(newPlan))(idGen)
}

// Security administration commands
case class ShowUsers(
  source: PrivilegePlan,
  withAuth: Boolean,
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
  nativeAuth: Option[NativeAuth],
  tags: Option[SetTags]
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
  removeAuth: RemoveAuth,
  tags: Seq[UserTagsAction]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source)) {

  val isTagsOnly: Boolean =
    suspended.isEmpty && defaultDatabase.isEmpty && nativeAuth.isEmpty && externalAuths.isEmpty && removeAuth.isEmpty && tags.nonEmpty
}

case class AlterUsers(
  source: SecurityAdministrationLogicalPlan,
  userNames: Seq[Either[String, Parameter]],
  ifExists: Boolean,
  tags: Seq[UserTagsAction]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class SetOwnPassword(
  source: SecurityAdministrationLogicalPlan,
  newPassword: Expression,
  currentPassword: Expression
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class CheckNativeAuthentication()(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan()

case class ShowRoles(
  source: PrivilegePlan,
  extensionType: ShowRoles.ShowRolesExtensionType,
  showAll: Boolean,
  asCommands: Boolean,
  override val returnColumns: List[LogicalVariable],
  yields: Option[Yield],
  returns: Option[Return]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

object ShowRoles {

  def apply(
    source: PrivilegePlan,
    withUsers: Boolean,
    withAuthRules: Boolean,
    showAll: Boolean,
    asCommands: Boolean,
    returnColumns: List[LogicalVariable],
    yields: Option[Yield],
    returns: Option[Return]
  )(implicit idGen: IdGen): ShowRoles = {
    val extensionType = (withUsers, withAuthRules) match {
      case (false, false) => ShowRoles.NoExtension
      case (true, false)  => ShowRoles.Users
      case (false, true)  => ShowRoles.AuthRules
      case (true, true) =>
        throw InternalException.internalError(
          this.getClass.getSimpleName,
          "Cannot show both users and auth rules in the same command"
        )
    }
    ShowRoles(source, extensionType, showAll, asCommands, returnColumns, yields, returns)(idGen)
  }

  sealed trait ShowRolesExtensionType
  case object NoExtension extends ShowRolesExtensionType
  case object Users extends ShowRolesExtensionType
  case object AuthRules extends ShowRolesExtensionType

}

case class CreateRole(
  source: SecurityAdministrationLogicalPlan,
  roleName: Either[String, Parameter],
  immutable: Boolean
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class RenameRole(
  source: SecurityAdministrationLogicalPlan,
  fromRoleName: Either[String, Parameter],
  toRoleName: Either[String, Parameter]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class DropRole(source: SecurityAdministrationLogicalPlan, roleName: Either[String, Parameter])(implicit
  idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class AssertRoleCanBeDropped(
  source: SecurityAdministrationLogicalPlan,
  roleName: Either[String, Parameter]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class AssertRoleCanBeReplaced(
  source: SecurityAdministrationLogicalPlan,
  roleName: Either[String, Parameter]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class AssertRoleCanBeRenamed(
  source: SecurityAdministrationLogicalPlan,
  roleName: Either[String, Parameter]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class AssertMutablePrivilegesCanBeAssignedToRole(
  source: PrivilegePlan,
  roleName: Either[String, Parameter]
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class ShowAuthRules(
  source: PrivilegePlan,
  asCommands: Boolean,
  override val returnColumns: List[LogicalVariable],
  yields: Option[Yield],
  returns: Option[Return]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class CreateAuthRule(
  source: SecurityAdministrationLogicalPlan,
  authRuleName: Either[String, Parameter],
  condition: Expression,
  enabled: Option[Boolean]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class AlterAuthRule(
  source: SecurityAdministrationLogicalPlan,
  authRuleName: Either[String, Parameter],
  condition: Option[Expression],
  enabled: Option[Boolean]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class RenameAuthRule(
  source: SecurityAdministrationLogicalPlan,
  fromAuthRuleName: Either[String, Parameter],
  toAuthRuleName: Either[String, Parameter]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class DropAuthRule(
  source: SecurityAdministrationLogicalPlan,
  authRuleName: Either[String, Parameter]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

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

case class GrantRoleToAuthRule(
  source: SecurityAdministrationLogicalPlan,
  roleName: Either[String, Parameter],
  ruleName: Either[String, Parameter],
  command: String
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class RevokeRoleFromAuthRule(
  source: SecurityAdministrationLogicalPlan,
  roleName: Either[String, Parameter],
  ruleName: Either[String, Parameter],
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

case class AssertRolePrivilegesCanAllBeCopied(
  source: SecurityAdministrationLogicalPlan,
  roleName: Either[String, Parameter]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class AssertRolePrivilegesAreAllImmutable(
  source: SecurityAdministrationLogicalPlan,
  roleName: Either[String, Parameter]
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class AssertSecurityDisabled(onViolation: () => AuthorizationViolationException)(implicit idGen: IdGen)
    extends PrivilegePlan(None)

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

case class AssertCanAlterDatabase(
  source: PrivilegePlan,
  namespacedName: DatabaseName,
  actionsForCompositeDatabases: Seq[DatabaseAndDbmsAction],
  actionsForDatabases: Seq[DatabaseAndDbmsAction]
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
  violationMessage: String,
  errorGqlStatusObject: ErrorGqlStatusObject
)(implicit idGen: IdGen) extends PrivilegePlan(Some(source))

case class AssertManagementActionNotBlocked(command: String, action: Seq[AdministrationAction])(implicit idGen: IdGen)
    extends PrivilegePlan

object AssertManagementActionNotBlocked {

  def apply(command: String, administrationAction: AdministrationAction)(implicit
    idGen: IdGen): AssertManagementActionNotBlocked =
    AssertManagementActionNotBlocked(command, Seq(administrationAction))
}

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

sealed trait RBACEntity
case object UserEntity extends RBACEntity
case object RoleEntity extends RBACEntity
case object AuthRuleEntity extends RBACEntity

case class DoNothingIfNotExists(
  source: PrivilegePlan,
  command: String,
  entity: RBACEntity,
  name: Either[String, Parameter],
  operation: String,
  valueMapper: String => String = s => s
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class DoNothingIfExists(
  source: PrivilegePlan,
  command: String,
  entity: RBACEntity,
  name: Either[String, Parameter],
  valueMapper: String => String = s => s
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class DoNothingIfDatabaseNotExists(
  source: PrivilegePlan,
  command: String,
  name: DatabaseName,
  operation: String,
  databaseTypeFilter: DatabaseTypeFilter = All,
  updateContextParams: Boolean = false
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class DoNothingIfDatabaseExists(
  source: PrivilegePlan,
  command: String,
  name: DatabaseName,
  databaseTypeFilter: DatabaseTypeFilter = All
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class EnsureNodeExists(
  source: PrivilegePlan,
  command: String,
  entity: RBACEntity,
  name: Either[String, Parameter],
  valueMapper: String => String = s => s,
  extraFilter: String => String = _ => "",
  labelDescription: String,
  action: String
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class EnsureDatabaseNodeExists(
  source: PrivilegePlan,
  command: String,
  name: DatabaseName,
  extraFilter: String => String = _ => "",
  action: String
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(Some(source))

case class EnsureRoleHasNoDeniedPrivileges(
  source: Option[SecurityAdministrationLogicalPlan],
  roleName: Either[String, Parameter],
  subquery: String
)(implicit idGen: IdGen) extends SecurityAdministrationLogicalPlan(source)

case class EnsureRoleNotGrantedToAnyAuthRules(
  source: Option[PrivilegePlan],
  roleName: Either[String, Parameter],
  subquery: String
)(implicit idGen: IdGen) extends PrivilegePlan(source)

// Database administration commands

abstract class CreateDatabasePlan(source: AdministrationCommandLogicalPlan)(implicit idGen: IdGen)
    extends DatabaseAdministrationLogicalPlan(Some(source)) {
  def databaseName: Either[String, Parameter]
}

sealed trait CreateDatabaseType {
  def isComposite: Boolean = this == CompositeDatabase
  def isReplica: Boolean = this == ReplicaDatabase
}

case object StandardDatabase extends CreateDatabaseType
case object CompositeDatabase extends CreateDatabaseType
case object ReplicaDatabase extends CreateDatabaseType

case class CreateShardedDatabase(
  source: AdministrationCommandLogicalPlan,
  databaseName: Either[String, Parameter],
  options: Options,
  ifExistsDo: IfExistsDo,
  shardDef: ShardDefinition,
  defaultLanguageVersion: Option[CypherVersion]
)(implicit idGen: IdGen) extends CreateDatabasePlan(source)

case class CreateDatabase(
  source: AdministrationCommandLogicalPlan,
  databaseName: Either[String, Parameter],
  options: Options,
  ifExistsDo: IfExistsDo,
  databaseType: CreateDatabaseType,
  topology: Option[Topology],
  defaultLanguageVersion: Option[CypherVersion]
)(implicit idGen: IdGen) extends CreateDatabasePlan(source)

case class DropDatabase(
  source: AdministrationCommandLogicalPlan,
  databaseName: DatabaseName,
  additionalAction: DropDatabaseAdditionalAction,
  forceComposite: Boolean,
  aliasAction: DropDatabaseAliasAction
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class AlterDatabase(
  source: AdministrationCommandLogicalPlan,
  databaseName: DatabaseName,
  access: Option[Access],
  topology: Option[Topology],
  options: Options,
  defaultLanguageVersion: Option[CypherVersion],
  optionsToRemove: Set[String],
  replicas: Option[Either[Int, Parameter]]
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class AlterShardedDatabase(
  source: AdministrationCommandLogicalPlan,
  databaseName: DatabaseName,
  access: Option[Access],
  options: Options,
  defaultLanguageVersion: Option[CypherVersion],
  shardDefinition: Option[ShardDefinition]
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
  remoteAliasCredentials: RemoteAliasCredentials,
  driverSettings: Option[Either[Map[String, Expression], Parameter]],
  properties: Option[Either[Map[String, Expression], Parameter]],
  defaultLanguageVersion: Option[CypherVersion]
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class DropDatabaseAlias(source: AdministrationCommandLogicalPlan, aliasName: DatabaseName)(
  implicit idGen: IdGen
) extends DatabaseAdministrationLogicalPlan(Some(source))

case class AlterLocalDatabaseAlias(
  source: AdministrationCommandLogicalPlan,
  aliasName: DatabaseName,
  targetName: Option[DatabaseName],
  properties: Option[Either[Map[String, Expression], Parameter]]
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class AlterRemoteDatabaseAlias(
  source: AdministrationCommandLogicalPlan,
  aliasName: DatabaseName,
  targetName: Option[DatabaseName],
  url: Option[Either[String, Parameter]],
  username: Option[Either[String, Parameter]],
  password: Option[Expression],
  driverSettings: Option[Either[Map[String, Expression], Parameter]],
  properties: Option[Either[Map[String, Expression], Parameter]],
  defaultLanguage: Option[CypherVersion]
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
  command: String,
  databaseName: DatabaseName,
  action: String,
  aliasName: Option[DatabaseName] = None
)(implicit idGen: IdGen)
    extends DatabaseAdministrationLogicalPlan(Some(source))

case class EnsureDatabaseSafeToDelete(
  source: AdministrationCommandLogicalPlan,
  databaseName: DatabaseName,
  aliasAction: DropDatabaseAliasAction
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

case class AssertNotStandard(
  source: AdministrationCommandLogicalPlan,
  name: DatabaseName,
  action: String,
  actionVerb: String
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class AssertNotVirtualSpd(
  source: AdministrationCommandLogicalPlan,
  name: DatabaseName,
  action: String,
  actionVerb: String
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class AssertNotGraphShard(
  source: AdministrationCommandLogicalPlan,
  name: DatabaseName,
  action: String,
  actionVerb: String
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class AssertNotPropertyShard(
  source: AdministrationCommandLogicalPlan,
  name: DatabaseName,
  action: String,
  actionVerb: String
)(implicit idGen: IdGen) extends DatabaseAdministrationLogicalPlan(Some(source))

case class AssertNotShardedDatabase(
  source: AdministrationCommandLogicalPlan,
  name: DatabaseName,
  action: String,
  actionVerb: String
)(implicit idGen: IdGen)
    extends DatabaseAdministrationLogicalPlan(Some(source))

case class AssertNotInvalidActionOnShard(
  source: AdministrationCommandLogicalPlan,
  name: DatabaseName,
  action: String,
  actionVerb: String
)(implicit idGen: IdGen)
    extends SecurityAdministrationLogicalPlan(Some(source))

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

case class EnsureValidNumberOfDatabases(source: CreateDatabasePlan)(implicit idGen: IdGen)
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
