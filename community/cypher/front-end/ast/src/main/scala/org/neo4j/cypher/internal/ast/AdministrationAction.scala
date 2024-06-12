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

abstract class RoleManagementAction(override val name: String) extends DbmsAction(name)

case object AllRoleActions extends RoleManagementAction("ROLE MANAGEMENT")

case object ShowRoleAction extends RoleManagementAction("SHOW ROLE")

case object CreateRoleAction extends RoleManagementAction("CREATE ROLE")

case object DropRoleAction extends RoleManagementAction("DROP ROLE")

case object RenameRoleAction extends RoleManagementAction("RENAME ROLE")

case object AssignRoleAction extends RoleManagementAction("ASSIGN ROLE")

case object RemoveRoleAction extends RoleManagementAction("REMOVE ROLE")

abstract class DatabaseManagementAction(override val name: String) extends DbmsAction(name)

case object AllDatabaseManagementActions extends DatabaseManagementAction("DATABASE MANAGEMENT")

case object CreateDatabaseAction extends DatabaseManagementAction("CREATE DATABASE")

case object DropDatabaseAction extends DatabaseManagementAction("DROP DATABASE")

case object AlterDatabaseAction extends DatabaseManagementAction("ALTER DATABASE")

case object SetDatabaseAccessAction extends DatabaseManagementAction("SET DATABASE ACCESS")

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

sealed trait UnassignableAction

case object AssignImmutablePrivilegeAction extends PrivilegeManagementAction("ASSIGN IMMUTABLE PRIVILEGE")
    with UnassignableAction

case object RemoveImmutablePrivilegeAction extends PrivilegeManagementAction("REMOVE IMMUTABLE PRIVILEGE")
    with UnassignableAction

// Load privilege actions

sealed trait DataExchangeAction extends AdministrationAction

sealed trait LoadActions extends DataExchangeAction {
  override val name: String = "LOAD"
}

case object LoadAllDataAction extends LoadActions

case object LoadCidrAction extends LoadActions

case object LoadUrlAction extends LoadActions
