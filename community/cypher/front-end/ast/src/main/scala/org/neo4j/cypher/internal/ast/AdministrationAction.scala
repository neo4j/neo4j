/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast

sealed trait AdministrationAction {
  def name: String = "<unknown>"
}

// Graph privilege actions

abstract class GraphAction(override val name: String, val planName: String) extends AdministrationAction

case object AllGraphAction extends GraphAction("ALL GRAPH PRIVILEGES", "AllGraphPrivileges")

case object TraverseAction extends GraphAction("TRAVERSE", "Traverse")

case object ReadAction extends GraphAction("READ", "Read")

case object MatchAction extends GraphAction("MATCH", "Match")

case object MergeAdminAction extends GraphAction("MERGE", "Merge")

case object CreateElementAction extends GraphAction("CREATE", "CreateElement")

case object DeleteElementAction extends GraphAction("DELETE", "DeleteElement")

case object SetLabelAction extends GraphAction("SET LABEL", "SetLabel")

case object RemoveLabelAction extends GraphAction("REMOVE LABEL", "RemoveLabel")

case object SetPropertyAction extends GraphAction("SET PROPERTY", "SetProperty")

case object WriteAction extends GraphAction("WRITE", "Write")

// Database privilege actions

abstract class DatabaseAction(override val name: String) extends AdministrationAction

case object AllDatabaseAction extends DatabaseAction("ALL DATABASE PRIVILEGES")

case object StartDatabaseAction extends DatabaseAction("START")

case object StopDatabaseAction extends DatabaseAction("STOP")

case object AccessDatabaseAction extends DatabaseAction("ACCESS")

sealed abstract class DatabaseAndDbmsAction(override val name: String) extends DatabaseAction(name) {
  def useCypher5: Boolean
}

case class AlterDatabaseAction(useCypher5: Boolean) extends DatabaseAndDbmsAction("ALTER DATABASE")

case class SetDatabaseAccessAction(useCypher5: Boolean) extends DatabaseAndDbmsAction("SET DATABASE ACCESS")

case class SetDatabaseDefaultLanguageAction(useCypher5: Boolean)
    extends DatabaseAndDbmsAction("SET DATABASE DEFAULT LANGUAGE")

/*
 * This is an internal only sub-privilege of ALTER DATABASE, so we display it to the user as ALTER DATABASE since
 * that is what they need to grant. ALTER DATABASE SET TOPOLOGY will check this.
 */
case class AlterDatabaseTopologyAction(useCypher5: Boolean) extends DatabaseAndDbmsAction("ALTER DATABASE")

/*
 * This is an internal only sub-privilege of ALTER DATABASE, so we display it to the user as ALTER DATABASE since
 * that is what they need to grant. ALTER DATABASE SET / REMOVE OPTION will check this.
 */
case class AlterDatabaseOptionsAction(useCypher5: Boolean) extends DatabaseAndDbmsAction("ALTER DATABASE")

case class AlterCompositeDatabaseAction(useCypher5: Boolean) extends DatabaseAndDbmsAction("ALTER COMPOSITE DATABASE")

abstract class IndexManagementAction(override val name: String) extends DatabaseAction(name)

case object AllIndexActions extends IndexManagementAction("INDEX MANAGEMENT")

case object CreateIndexAction extends IndexManagementAction("CREATE INDEX")

case object DropIndexAction extends IndexManagementAction("DROP INDEX")

case object ShowIndexAction extends IndexManagementAction("SHOW INDEX")

abstract class ConstraintManagementAction(override val name: String) extends DatabaseAction(name)

case object AllConstraintActions extends ConstraintManagementAction("CONSTRAINT MANAGEMENT")

case object CreateConstraintAction extends ConstraintManagementAction("CREATE CONSTRAINT")

case object DropConstraintAction extends ConstraintManagementAction("DROP CONSTRAINT")

case object ShowConstraintAction extends ConstraintManagementAction("SHOW CONSTRAINT")

abstract class NameManagementAction(override val name: String) extends DatabaseAction(name)

case object AllTokenActions extends NameManagementAction("NAME MANAGEMENT")

case object CreateNodeLabelAction extends NameManagementAction("CREATE NEW NODE LABEL")

case object CreateRelationshipTypeAction extends NameManagementAction("CREATE NEW RELATIONSHIP TYPE")

case object CreatePropertyKeyAction extends NameManagementAction("CREATE NEW PROPERTY NAME")

abstract class TransactionManagementAction(override val name: String) extends DatabaseAction(name)

case object AllTransactionActions extends TransactionManagementAction("TRANSACTION MANAGEMENT")

case object ShowTransactionAction extends TransactionManagementAction("SHOW TRANSACTION")

case object TerminateTransactionAction extends TransactionManagementAction("TERMINATE TRANSACTION")

// DBMS privilege actions

abstract class DbmsAction(override val name: String) extends AdministrationAction

case object AllDbmsAction extends DbmsAction("ALL DBMS PRIVILEGES")

case object ServerManagementAction extends DbmsAction("SERVER MANAGEMENT")

case object ShowServerAction extends DbmsAction("SHOW SERVERS")

case object ImpersonateUserAction extends DbmsAction("IMPERSONATE")

case object ExecuteProcedureAction extends DbmsAction("EXECUTE PROCEDURE")

case object ExecuteBoostedProcedureAction extends DbmsAction("EXECUTE BOOSTED PROCEDURE")

case object ExecuteAdminProcedureAction extends DbmsAction("EXECUTE ADMIN PROCEDURES")

case object ExecuteFunctionAction extends DbmsAction("EXECUTE USER DEFINED FUNCTION")

case object ExecuteBoostedFunctionAction extends DbmsAction("EXECUTE BOOSTED USER DEFINED FUNCTION")

case object ShowSettingAction extends DbmsAction("SHOW SETTING")

abstract class SecretManagementAction(override val name: String) extends DbmsAction(name)

case object AllSecretManagementActions extends SecretManagementAction("SECRETS MANAGEMENT")

case object ReadSecretsAction extends SecretManagementAction("READ SECRETS")

case object WriteSecretsAction extends SecretManagementAction("WRITE SECRETS")

case object ShowSecretsAction extends SecretManagementAction("SHOW SECRETS")

abstract class UserManagementAction(override val name: String) extends DbmsAction(name)

case object AllUserActions extends UserManagementAction("USER MANAGEMENT")

case object ShowUserAction extends UserManagementAction("SHOW USER")

case object CreateUserAction extends UserManagementAction("CREATE USER")

case object DropUserAction extends UserManagementAction("DROP USER")

case object RenameUserAction extends RoleManagementAction("RENAME USER")

case object AlterUserAction extends UserManagementAction("ALTER USER")

case object SetUserStatusAction extends UserManagementAction("SET USER STATUS")

case object SetPasswordsAction extends UserManagementAction("SET PASSWORDS")

case object SetAuthAction extends UserManagementAction("SET AUTH")

case object SetUserHomeDatabaseAction extends UserManagementAction("SET USER HOME DATABASE")

abstract class UserMetadataManagementAction(override val name: String) extends DbmsAction(name)

case object AllUserMetadataActions extends UserMetadataManagementAction("USER METADATA MANAGEMENT")

case object ShowUserMetadataAction extends UserMetadataManagementAction("SHOW USER METADATA")

case object SetUserMetadataAction extends UserMetadataManagementAction("SET USER METADATA")

abstract class RoleManagementAction(override val name: String) extends DbmsAction(name)

case object AllRoleActions extends RoleManagementAction("ROLE MANAGEMENT")

case object ShowRoleAction extends RoleManagementAction("SHOW ROLE")

case object CreateRoleAction extends RoleManagementAction("CREATE ROLE")

case object DropRoleAction extends RoleManagementAction("DROP ROLE")

case object RenameRoleAction extends RoleManagementAction("RENAME ROLE")

case object AssignRoleAction extends RoleManagementAction("ASSIGN ROLE")

case object RemoveRoleAction extends RoleManagementAction("REMOVE ROLE")

abstract class AuthRuleManagementAction(override val name: String) extends DbmsAction(name)

case object ShowAuthRuleAction extends AuthRuleManagementAction("SHOW AUTH RULE")

case object CreateAuthRuleAction extends AuthRuleManagementAction("CREATE AUTH RULE")

case object DropAuthRuleAction extends AuthRuleManagementAction("DROP AUTH RULE")

case object AlterAuthRuleAction extends AuthRuleManagementAction("ALTER AUTH RULE")

case object RenameAuthRuleAction extends AuthRuleManagementAction("RENAME AUTH RULE")

case object AllAuthRuleActions extends AuthRuleManagementAction("AUTH RULE MANAGEMENT")

abstract class DatabaseManagementAction(override val name: String) extends DbmsAction(name)

case object AllDatabaseManagementActions extends DatabaseManagementAction("DATABASE MANAGEMENT")

case object CreateDatabaseAction extends DatabaseManagementAction("CREATE DATABASE")

case object DropDatabaseAction extends DatabaseManagementAction("DROP DATABASE")

case object CompositeDatabaseManagementActions extends DatabaseManagementAction("COMPOSITE DATABASE MANAGEMENT")

case object CreateCompositeDatabaseAction extends DatabaseManagementAction("CREATE COMPOSITE DATABASE")

case object DropCompositeDatabaseAction extends DatabaseManagementAction("DROP COMPOSITE DATABASE")

abstract class AliasManagementAction(override val name: String) extends DbmsAction(name)

case object AllAliasManagementActions extends AliasManagementAction("ALIAS MANAGEMENT")

case object CreateAliasAction extends AliasManagementAction("CREATE ALIAS")

case object DropAliasAction extends AliasManagementAction("DROP ALIAS")

case object AlterAliasAction extends AliasManagementAction("ALTER ALIAS")

case object ShowAliasAction extends AliasManagementAction("SHOW ALIAS")

abstract class PrivilegeManagementAction(override val name: String) extends DbmsAction(name)

case object AllPrivilegeActions extends PrivilegeManagementAction("PRIVILEGE MANAGEMENT")

case object ShowPrivilegeAction extends PrivilegeManagementAction("SHOW PRIVILEGE")

case object AssignPrivilegeAction extends PrivilegeManagementAction("ASSIGN PRIVILEGE")

case object RemovePrivilegeAction extends PrivilegeManagementAction("REMOVE PRIVILEGE")

// Load privilege actions

sealed trait DataExchangeAction extends AdministrationAction

sealed trait LoadActions extends DataExchangeAction {
  override val name: String = "LOAD"
}

case object LoadAllDataAction extends LoadActions

case object LoadCidrAction extends LoadActions

case object LoadUrlAction extends LoadActions
