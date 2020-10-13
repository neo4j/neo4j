/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.parser

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.IndefiniteWait
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.TimeoutAfter
import org.neo4j.cypher.internal.ast.WaitUntilComplete
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.util.InputPosition
import org.parboiled.scala.Parser
import org.parboiled.scala.Rule0
import org.parboiled.scala.Rule1
import org.parboiled.scala.Rule2
import org.parboiled.scala.Rule3
import org.parboiled.scala.Rule4
import org.parboiled.scala.group

//noinspection ConvertibleToMethodValue
// Can't convert since that breaks parsing
trait Statement extends Parser
  with GraphSelection
  with Query
  with SchemaCommand
  with Base {

  def Statement: Rule1[ast.Statement] = AdministrationCommand | MultiGraphCommand | SchemaCommand | Query

  def MultiGraphCommand: Rule1[ast.MultiGraphDDL] = rule("Multi graph DDL statement") {
    CreateGraph | DropGraph | CreateView | DropView
  }

  def AdministrationCommand: Rule1[ast.AdministrationCommand] = rule("Administration statement") {
    optional(UseGraph) ~~ (MultiDatabaseAdministrationCommand | PrivilegeAdministrationCommand | UserAndRoleAdministrationCommand) ~~> ((use, command) => command.withGraph(use))
  }

  def MultiDatabaseAdministrationCommand: Rule1[ast.AdministrationCommand] = rule("MultiDatabase administration statement") {
    optional(keyword("CATALOG")) ~~ (ShowDatabase | CreateDatabase | DropDatabase | StartDatabase | StopDatabase)
  }

  def UserAndRoleAdministrationCommand: Rule1[ast.AdministrationCommand] = rule("Security role and user administration statement") {
    optional(keyword("CATALOG")) ~~ (ShowRoles | CreateRole | DropRole | ShowUsers | ShowCurrentUser | CreateUser | DropUser | AlterUser | SetOwnPassword)
  }

  def PrivilegeAdministrationCommand: Rule1[ast.AdministrationCommand] = rule("Security privilege administration statement") {
    optional(keyword("CATALOG")) ~~ (ShowPrivileges | GrantCommand | DenyCommand | RevokeCommand)
  }

  def GrantCommand: Rule1[ast.AdministrationCommand] = rule("Security privilege grant statement") {
      GrantRole | GrantDatabasePrivilege | GrantGraphPrivilege | GrantDbmsPrivilege
  }

  def DenyCommand: Rule1[ast.AdministrationCommand] = rule("Security privilege deny statement") {
    DenyDatabasePrivilege | DenyGraphPrivilege | DenyDbmsPrivilege
  }

  def RevokeCommand: Rule1[ast.AdministrationCommand] = rule("Security privilege revoke statement") {
    RevokeRole | RevokeGraphPrivilege | RevokeDatabasePrivilege | RevokeDbmsPrivilege
  }

  // User management commands

  def ShowUsers: Rule1[ast.ShowUsers] = rule("SHOW USERS") {
    keyword("SHOW USERS") ~~ optional(ShowCommandClauses) ~~>> (ast.ShowUsers(_))
  }

  def ShowCurrentUser: Rule1[ast.ShowCurrentUser] = rule("SHOW CURRENT USER") {
    keyword("SHOW CURRENT USER") ~~ optional(ShowCommandClauses) ~~>> (ast.ShowCurrentUser(_))
  }

  def CreateUser: Rule1[ast.CreateUser] = rule("CREATE USER") {
    // CREATE [OR REPLACE] USER username [IF NOT EXISTS] SET [PLAINTEXT | ENCRYPTED] PASSWORD stringLiteralPassword optionalStatus
    group(CreateUserStart ~~ SetPassword ~~ SensitiveStringLiteral ~~ OptionalStatus) ~~>> ((userName, ifExistsDo, isEncryptedPassword, initialPassword, suspended) =>
      ast.CreateUser(userName, isEncryptedPassword, initialPassword, requirePasswordChange = true, suspended, ifExistsDo)) |
    // CREATE [OR REPLACE] USER username [IF NOT EXISTS] SET [PLAINTEXT | ENCRYPTED] PASSWORD stringLiteralPassword optionalRequirePasswordChange optionalStatus
    group(CreateUserStart ~~ SetPassword ~~ SensitiveStringLiteral ~~ OptionalRequirePasswordChange ~~ OptionalStatus) ~~>>
      ((userName, ifExistsDo, isEncryptedPassword, initialPassword, requirePasswordChange, suspended) =>
      ast.CreateUser(userName, isEncryptedPassword, initialPassword, requirePasswordChange.getOrElse(true), suspended, ifExistsDo)) |
    //
    // CREATE [OR REPLACE] USER username [IF NOT EXISTS] SET [PLAINTEXT | ENCRYPTED] PASSWORD parameterPassword optionalStatus
    group(CreateUserStart ~~ SetPassword ~~ SensitiveStringParameter ~~ OptionalStatus) ~~>> ((userName, ifExistsDo, isEncryptedPassword, initialPassword, suspended) =>
      ast.CreateUser(userName, isEncryptedPassword, initialPassword, requirePasswordChange = true, suspended, ifExistsDo)) |
    // CREATE [OR REPLACE] USER username [IF NOT EXISTS] SET [PLAINTEXT | ENCRYPTED] PASSWORD parameterPassword optionalRequirePasswordChange optionalStatus
    group(CreateUserStart ~~ SetPassword ~~ SensitiveStringParameter ~~ OptionalRequirePasswordChange ~~ OptionalStatus) ~~>>
      ((userName, ifExistsDo, isEncryptedPassword, initialPassword, requirePasswordChange, suspended) =>
      ast.CreateUser(userName, isEncryptedPassword, initialPassword, requirePasswordChange.getOrElse(true), suspended, ifExistsDo))
  }

  def CreateUserStart: Rule2[Either[String, Parameter], ast.IfExistsDo] = {
    // returns: userName, IfExistsDo
    group(keyword("CREATE OR REPLACE USER") ~~ SymbolicNameOrStringParameter ~~ keyword("IF NOT EXISTS") ~> (_ => ast.IfExistsInvalidSyntax)) |
    group(keyword("CREATE OR REPLACE USER") ~~ SymbolicNameOrStringParameter ~> (_ => ast.IfExistsReplace)) |
    group(keyword("CREATE USER") ~~ SymbolicNameOrStringParameter ~~ keyword("IF NOT EXISTS") ~> (_ => ast.IfExistsDoNothing)) |
    group(keyword("CREATE USER") ~~ SymbolicNameOrStringParameter ~> (_ => ast.IfExistsThrowError))
  }

  def DropUser: Rule1[ast.DropUser] = rule("DROP USER") {
    group(keyword("DROP USER") ~~ SymbolicNameOrStringParameter ~~ keyword("IF EXISTS")) ~~>> (ast.DropUser(_, ifExists = true)) |
    group(keyword("DROP USER") ~~ SymbolicNameOrStringParameter) ~~>> (ast.DropUser(_, ifExists = false))
  }

  def AlterUser: Rule1[ast.AlterUser] = rule("ALTER USER") {
    // ALTER USER username SET [PLAINTEXT | ENCRYPTED] PASSWORD stringLiteralPassword optionalStatus
    group(keyword("ALTER USER") ~~ SymbolicNameOrStringParameter ~~ SetPassword ~~ SensitiveStringLiteral ~~ OptionalStatus) ~~>>
      ((userName, isEncryptedPassword, initialPassword, suspended) => ast.AlterUser(userName, Some(isEncryptedPassword), Some(initialPassword), None, suspended)) |
    // ALTER USER username SET [PLAINTEXT | ENCRYPTED] PASSWORD stringLiteralPassword optionalRequirePasswordChange optionalStatus
    group(keyword("ALTER USER") ~~ SymbolicNameOrStringParameter ~~ SetPassword ~~ SensitiveStringLiteral ~~ OptionalRequirePasswordChange ~~ OptionalStatus) ~~>>
      ((userName, isEncryptedPassword, initialPassword, requirePasswordChange, suspended) => ast.AlterUser(userName, Some(isEncryptedPassword), Some(initialPassword), requirePasswordChange, suspended)) |
    //
    // ALTER USER username SET [PLAINTEXT | ENCRYPTED] PASSWORD parameterPassword optionalStatus
    group(keyword("ALTER USER") ~~ SymbolicNameOrStringParameter ~~ SetPassword ~~ SensitiveStringParameter ~~ OptionalStatus) ~~>>
      ((userName, isEncryptedPassword, initialPassword, suspended) => ast.AlterUser(userName, Some(isEncryptedPassword), Some(initialPassword), None, suspended)) |
    // ALTER USER username SET [PLAINTEXT | ENCRYPTED] PASSWORD parameterPassword optionalRequirePasswordChange optionalStatus
    group(keyword("ALTER USER") ~~ SymbolicNameOrStringParameter ~~ SetPassword ~~ SensitiveStringParameter ~~ OptionalRequirePasswordChange ~~ OptionalStatus) ~~>>
      ((userName, isEncryptedPassword, initialPassword, requirePasswordChange, suspended) => ast.AlterUser(userName, Some(isEncryptedPassword), Some(initialPassword), requirePasswordChange, suspended)) |
    //
    // ALTER USER username setRequirePasswordChange optionalStatus
    group(keyword("ALTER USER") ~~ SymbolicNameOrStringParameter ~~ SetRequirePasswordChange ~~ OptionalStatus) ~~>>
      ((userName, requirePasswordChange, suspended) => ast.AlterUser(userName, None, None, Some(requirePasswordChange), suspended)) |
    //
    // ALTER USER username setStatus
    group(keyword("ALTER USER") ~~ SymbolicNameOrStringParameter ~~ SetStatus) ~~>>
      ((userName, suspended) => ast.AlterUser(userName, None, None, None, Some(suspended)))
  }

  def SetOwnPassword: Rule1[ast.SetOwnPassword] = rule("ALTER CURRENT USER SET PASSWORD") {
    // ALTER CURRENT USER SET PASSWORD FROM stringLiteralPassword TO stringLiteralPassword
    group(keyword("ALTER CURRENT USER SET PASSWORD FROM") ~~ SensitiveStringLiteral ~~ keyword("TO") ~~ SensitiveStringLiteral) ~~>>
      ((currentPassword, newPassword) => ast.SetOwnPassword(newPassword, currentPassword)) |
    // ALTER CURRENT USER SET PASSWORD FROM stringLiteralPassword TO parameterPassword
    group(keyword("ALTER CURRENT USER SET PASSWORD FROM") ~~ SensitiveStringLiteral ~~ keyword("TO") ~~ SensitiveStringParameter) ~~>>
      ((currentPassword, newPassword) => ast.SetOwnPassword(newPassword, currentPassword)) |
    // ALTER CURRENT USER SET PASSWORD FROM parameterPassword TO stringLiteralPassword
    group(keyword("ALTER CURRENT USER SET PASSWORD FROM") ~~ SensitiveStringParameter ~~ keyword("TO") ~~ SensitiveStringLiteral) ~~>>
      ((currentPassword, newPassword) => ast.SetOwnPassword(newPassword, currentPassword)) |
    // ALTER CURRENT USER SET PASSWORD FROM parameterPassword TO parameterPassword
    group(keyword("ALTER CURRENT USER SET PASSWORD FROM") ~~ SensitiveStringParameter ~~ keyword("TO") ~~ SensitiveStringParameter) ~~>>
      ((currentPassword, newPassword) => ast.SetOwnPassword(newPassword, currentPassword))
  }

  def SetPassword: Rule1[Boolean] = rule("set encrypted or plaintext password") {
    // returns: isEncryptedPassword
    group(keyword("SET") ~~ optional(keyword("PLAINTEXT")) ~~ (keyword("PASSWORD")) ~> (_ => false)) |
      group(keyword("SET ENCRYPTED PASSWORD") ~> (_ => true))
  }

  def OptionalRequirePasswordChange: Rule1[Option[Boolean]] = {
    group(optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE NOT REQUIRED")) ~>>> (_ => _ => Some(false)) |
    group(optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE REQUIRED")) ~>>> (_ => _ => Some(true)) |
    keyword("") ~>>> (_ => _ => None) // no password mode change
  }

  def OptionalStatus: Rule1[Option[Boolean]] = {
    keyword("SET STATUS SUSPENDED") ~>>> (_ => _ => Some(true)) |
    keyword("SET STATUS ACTIVE") ~>>> (_ => _ => Some(false)) |
    keyword("") ~>>> (_ => _ => None) // no status change
  }

  //noinspection MutatorLikeMethodIsParameterless
  def SetStatus: Rule1[Boolean] = {
    keyword("SET STATUS SUSPENDED") ~>>> (_ => _ => true) |
    keyword("SET STATUS ACTIVE") ~>>> (_ => _ => false)
  }

  //noinspection MutatorLikeMethodIsParameterless
  def SetRequirePasswordChange: Rule1[Boolean] = {
    keyword("SET PASSWORD CHANGE NOT REQUIRED") ~>>> (_ => _ => false) |
    keyword("SET PASSWORD CHANGE REQUIRED") ~>>> (_ => _ => true)
  }

  // Role management commands

  def ShowRoles: Rule1[ast.ShowRoles] = rule("SHOW ROLES") {
    //SHOW [ ALL | POPULATED ] ROLES WITH USERS
    group(ShowAllRoles ~~ keyword("WITH USERS") ~~ optional(ShowCommandClauses)) ~~>> (ast.ShowRoles(withUsers = true, _, _)) |
    // SHOW [ ALL | POPULATED ] ROLES
    group(ShowAllRoles ~~ optional(ShowCommandClauses)) ~~>> (ast.ShowRoles(withUsers = false, _, _))
  }

  private def ShowAllRoles: Rule1[Boolean] = rule("return true for SHOW ALL ROLES, false for SHOW POPULATED ROLES") {
    keyword("SHOW POPULATED ROLES") ~~~> (_ => false) |
    group(keyword("SHOW") ~~ optional(keyword("ALL")) ~~ keyword("ROLES")) ~~~> (_ => true)
  }

  def CreateRole: Rule1[ast.CreateRole] = rule("CREATE ROLE") {
    group(CreateRoleStart ~~ optional(keyword("AS COPY OF") ~~ SymbolicNameOrStringParameter)) ~~>> ((roleName, ifExistsDo, from) => ast.CreateRole(roleName, from, ifExistsDo))
  }

  private def CreateRoleStart: Rule2[Either[String, Parameter], ast.IfExistsDo] = {
    // returns: roleName, IfExistsDo
    group(keyword("CREATE OR REPLACE ROLE") ~~ SymbolicNameOrStringParameter ~~ keyword("IF NOT EXISTS") ~> (_ => ast.IfExistsInvalidSyntax)) |
    group(keyword("CREATE OR REPLACE ROLE") ~~ SymbolicNameOrStringParameter ~> (_ => ast.IfExistsReplace)) |
    group(keyword("CREATE ROLE") ~~ SymbolicNameOrStringParameter ~~ keyword("IF NOT EXISTS") ~> (_ => ast.IfExistsDoNothing)) |
    group(keyword("CREATE ROLE") ~~ SymbolicNameOrStringParameter ~> (_ => ast.IfExistsThrowError))
  }

  def DropRole: Rule1[ast.DropRole] = rule("DROP ROLE") {
    group(keyword("DROP ROLE") ~~ SymbolicNameOrStringParameter ~~ keyword("IF EXISTS")) ~~>> (ast.DropRole(_, ifExists = true)) |
    group(keyword("DROP ROLE") ~~ SymbolicNameOrStringParameter) ~~>> (ast.DropRole(_, ifExists = false))
  }

  def GrantRole: Rule1[ast.GrantRolesToUsers] = rule("GRANT ROLE") {
    group(keyword("GRANT") ~~ RoleKeyword ~~ SymbolicNameOrStringParameterList ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>> (ast.GrantRolesToUsers(_, _))
  }

  def RevokeRole: Rule1[ast.RevokeRolesFromUsers] = rule("REVOKE ROLE") {
    group(keyword("REVOKE") ~~ RoleKeyword ~~ SymbolicNameOrStringParameterList ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>> (ast.RevokeRolesFromUsers(_, _))
  }

  // Privilege commands
  def ShowPrivileges: Rule1[ast.ReadAdministrationCommand] = rule("SHOW PRIVILEGES") {
    group(keyword("SHOW") ~~ ScopeForShowPrivileges ~~ asCommand ~~ optional(ShowCommandClauses) ~~>> ((scope, revoke, yld) => ast.ShowPrivilegeCommands(scope, revoke, yld))) |
    group(keyword("SHOW") ~~ ScopeForShowPrivileges ~~ optional(ShowCommandClauses) ~~>> ((scope, yld) => ast.ShowPrivileges(scope, yld)))
  }

  private def ScopeForShowPrivileges: Rule1[ast.ShowPrivilegeScope] = rule("show privilege scope") {
    group(RoleKeyword ~~ SymbolicNameOrStringParameterList ~~ PrivilegeKeyword) ~~>> (ast.ShowRolesPrivileges(_)) |
    group(UserKeyword ~~ SymbolicNameOrStringParameterList ~~ PrivilegeKeyword) ~~>> (ast.ShowUsersPrivileges(_)) |
    group(UserKeyword ~~ PrivilegeKeyword) ~~~> ast.ShowUserPrivileges(None) |
    optional(keyword("ALL")) ~~ PrivilegeKeyword ~~~> ast.ShowAllPrivileges()
  }

  private def asCommand: Rule1[Boolean] = rule("AS COMMANDS") {
    group(keyword("AS") ~~ CommandKeyword) ~> (_ => false) |
    group(keyword("AS REVOKE") ~~ CommandKeyword) ~> (_ => true)
  }

  //` ... ON DBMS TO role`
  def GrantDbmsPrivilege: Rule1[ast.GrantPrivilege] = rule("GRANT dbms privileges") {
    group(keyword("GRANT") ~~ DbmsAction ~~ keyword("ON DBMS TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((dbmsAction, grantees) => pos => ast.GrantPrivilege.dbmsAction(dbmsAction, grantees, List(ast.AllQualifier()(pos)))(pos)) |
    group(keyword("GRANT") ~~ QualifiedDbmsAction ~~ keyword("ON DBMS TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((qualifier, dbmsAction, grantees) => ast.GrantPrivilege.dbmsAction( dbmsAction, grantees, qualifier ))
  }

  def DenyDbmsPrivilege: Rule1[ast.DenyPrivilege] = rule("DENY dbms privileges") {
    group(keyword("DENY") ~~ DbmsAction ~~ keyword("ON DBMS TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((dbmsAction, grantees) => ast.DenyPrivilege.dbmsAction( dbmsAction, grantees )) |
    group(keyword("DENY") ~~ QualifiedDbmsAction ~~ keyword("ON DBMS TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((qualifier, dbmsAction, grantees) => ast.DenyPrivilege.dbmsAction( dbmsAction, grantees, qualifier ))
  }

  def RevokeDbmsPrivilege: Rule1[ast.RevokePrivilege] = rule("REVOKE dbms privileges") {
    group(RevokeType ~~ DbmsAction ~~ keyword("ON DBMS FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((revokeType, dbmsAction, grantees) => ast.RevokePrivilege.dbmsAction(dbmsAction, grantees, revokeType)) |
    group(RevokeType ~~ QualifiedDbmsAction ~~ keyword("ON DBMS FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((revokeType, qualifier, dbmsAction, grantees) => ast.RevokePrivilege.dbmsAction(dbmsAction, grantees, revokeType, qualifier))
  }

  //` ... ON DATABASE foo TO role`
  def GrantDatabasePrivilege: Rule1[ast.GrantPrivilege] = rule("GRANT database privileges") {
    group(keyword("GRANT") ~~ QualifiedDatabaseAction ~~ Database ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((qualifier, databaseAction, scope, grantees) => ast.GrantPrivilege.databaseAction( databaseAction, scope, grantees, qualifier)) |
    group(keyword("GRANT") ~~ DatabaseAction ~~ Database ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((databaseAction, scope, grantees) => ast.GrantPrivilege.databaseAction( databaseAction, scope, grantees))
  }

  def DenyDatabasePrivilege: Rule1[ast.DenyPrivilege] = rule("DENY database privileges") {
    group(keyword("DENY") ~~ QualifiedDatabaseAction ~~ Database ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((qualifier, databaseAction, scope, grantees) => ast.DenyPrivilege.databaseAction( databaseAction, scope, grantees, qualifier)) |
    group(keyword("DENY") ~~ DatabaseAction ~~ Database ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((databaseAction, scope, grantees) => ast.DenyPrivilege.databaseAction( databaseAction, scope, grantees))
  }

  def RevokeDatabasePrivilege: Rule1[ast.RevokePrivilege] = rule("REVOKE database privileges") {
    group(RevokeType ~~ QualifiedDatabaseAction ~~ Database ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((revokeType, qualifier, databaseAction, scope, grantees) => ast.RevokePrivilege.databaseAction(databaseAction, scope, grantees, revokeType, qualifier)) |
    group(RevokeType ~~ DatabaseAction ~~ Database ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((revokeType, databaseAction, scope, grantees) => ast.RevokePrivilege.databaseAction(databaseAction, scope, grantees, revokeType))
  }

   //` ... ON GRAPH foo TO role`
  def GrantGraphPrivilege: Rule1[ast.GrantPrivilege] = rule("GRANT graph privileges") {
    group(keyword("GRANT") ~~ GraphAction ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, action, qualifier, roles) => ast.GrantPrivilege.graphAction(action, None, graphScope, qualifier, roles)) |
    group(keyword("GRANT") ~~ QualifiedGraphAction ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, qualifier, action, roles) => ast.GrantPrivilege.graphAction(action, None, graphScope, qualifier, roles)) |
    group(keyword("GRANT") ~~ GraphActionWithResource ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((resource, graphScope, action, roles) => ast.GrantPrivilege.graphAction(action, Some(resource), graphScope, List(ast.LabelAllQualifier()(InputPosition.NONE)), roles)) |
    group(keyword("GRANT") ~~ QualifiedGraphActionWithResource ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((resource, graphScope, qualifier, action, roles) => ast.GrantPrivilege.graphAction(action, Some(resource), graphScope, qualifier, roles))
  }

  def DenyGraphPrivilege: Rule1[ast.DenyPrivilege] = rule("DENY graph privileges") {
    group(keyword("DENY") ~~ GraphAction ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, action, qualifier, roles) => ast.DenyPrivilege.graphAction(action, None, graphScope, qualifier, roles)) |
    group(keyword("DENY") ~~ QualifiedGraphAction ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, qualifier, action, roles) => ast.DenyPrivilege.graphAction(action, None, graphScope, qualifier, roles)) |
    group(keyword("DENY") ~~ GraphActionWithResource ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((resource, graphScope, action, roles) => ast.DenyPrivilege.graphAction(action, Some(resource), graphScope, List(ast.LabelAllQualifier()(InputPosition.NONE)), roles)) |
    group(keyword("DENY") ~~ QualifiedGraphActionWithResource ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((resource, graphScope, qualifier, action, roles) => ast.DenyPrivilege.graphAction(action, Some(resource), graphScope, qualifier, roles))
  }

  def RevokeGraphPrivilege: Rule1[ast.RevokePrivilege] = rule("REVOKE graph privileges") {
    group(RevokeType ~~ GraphAction ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((revokeType, graphScope, action, qualifier, roles) => ast.RevokePrivilege.graphAction(action, None, graphScope, qualifier, roles, revokeType)) |
    group(RevokeType ~~ QualifiedGraphAction ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((revokeType, graphScope, qualifier, action, roles) => ast.RevokePrivilege.graphAction(action, None, graphScope, qualifier, roles, revokeType)) |
    group(RevokeType ~~ GraphActionWithResource ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((revokeType, resource, graphScope, action, roles) => ast.RevokePrivilege.graphAction(action, Some(resource), graphScope, List(ast.LabelAllQualifier()(InputPosition.NONE)), roles, revokeType)) |
    group(RevokeType ~~ QualifiedGraphActionWithResource ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((revokeType, resource, graphScope, qualifier, action, roles) => ast.RevokePrivilege.graphAction(action, Some(resource), graphScope, qualifier, roles, revokeType))
  }

  // Help methods for grant/deny/revoke

  private def RevokeType: Rule1[ast.RevokeType] = rule("revoke type") {
    keyword("REVOKE GRANT") ~~~> ast.RevokeGrantType() |
    keyword("REVOKE DENY") ~~~> ast.RevokeDenyType() |
    keyword("REVOKE") ~~~> ast.RevokeBothType()
  }

  // Dbms specific

  private def DbmsAction: Rule1[ast.AdminAction] = rule("dbms action") {
    keyword("CREATE ROLE") ~~~> (_ => ast.CreateRoleAction) |
    keyword("DROP ROLE") ~~~> (_ => ast.DropRoleAction) |
    keyword("ASSIGN ROLE") ~~~> (_ => ast.AssignRoleAction) |
    keyword("REMOVE ROLE") ~~~> (_ => ast.RemoveRoleAction) |
    keyword("SHOW ROLE") ~~~> (_ => ast.ShowRoleAction) |
    keyword("ROLE MANAGEMENT") ~~~> (_ => ast.AllRoleActions) |
    keyword("CREATE USER") ~~~> (_ => ast.CreateUserAction) |
    keyword("DROP USER") ~~~> (_ => ast.DropUserAction) |
    keyword("SHOW USER") ~~~> (_ => ast.ShowUserAction) |
    keyword("SET USER STATUS") ~~~> (_ => ast.SetUserStatusAction) |
    keyword("SET") ~~ PasswordKeyword ~~~> (_ => ast.SetPasswordsAction) |
    keyword("ALTER USER") ~~~> (_ => ast.AlterUserAction) |
    keyword("USER MANAGEMENT") ~~~> (_ => ast.AllUserActions) |
    keyword("CREATE DATABASE") ~~~> (_ => ast.CreateDatabaseAction) |
    keyword("DROP DATABASE") ~~~> (_ => ast.DropDatabaseAction) |
    keyword("DATABASE MANAGEMENT") ~~~> (_ => ast.AllDatabaseManagementActions) |
    keyword("SHOW PRIVILEGE") ~~~> (_ => ast.ShowPrivilegeAction) |
    keyword("ASSIGN PRIVILEGE") ~~~> (_ => ast.AssignPrivilegeAction) |
    keyword("REMOVE PRIVILEGE") ~~~> (_ => ast.RemovePrivilegeAction) |
    keyword("PRIVILEGE MANAGEMENT") ~~~> (_ => ast.AllPrivilegeActions) |
    group(keyword("ALL") ~~ optional(optional(keyword("DBMS")) ~~ keyword("PRIVILEGES")))~~~> (_ => ast.AllDbmsAction) |
    group(keyword("EXECUTE") ~~ AdminKeyword ~~ keyword("PROCEDURES")) ~> (_ => ast.ExecuteAdminProcedureAction)
  }

  private def QualifiedDbmsAction: Rule2[List[ast.PrivilegeQualifier], ast.AdminAction] = rule("qualified dbms action") {
    keyword("EXECUTE") ~~ Procedure ~> (_ => ast.ExecuteProcedureAction) |
    keyword("EXECUTE BOOSTED") ~~ Procedure ~> (_ => ast.ExecuteBoostedProcedureAction) |
    keyword("EXECUTE") ~~ Function ~> (_ => ast.ExecuteFunctionAction) |
    keyword("EXECUTE BOOSTED") ~~ Function ~> (_ => ast.ExecuteBoostedFunctionAction)
  }

  private def Procedure: Rule1[List[ast.PrivilegeQualifier]] = ProcedureKeyword ~~ ProcedureIdentifier

  private def ProcedureIdentifier: Rule1[List[ast.PrivilegeQualifier]] = rule("procedure identifier") {
    oneOrMore(group(GlobbedNamespace ~ GlobbedProcedureName), separator = CommaSep) ~~>>
      { procedures => pos => procedures.map(p => ast.ProcedureQualifier(p._1, p._2)(pos)) }
  }

  private def Function: Rule1[List[ast.PrivilegeQualifier]] =
    group(optional(keyword("USER") ~~ optional(keyword("DEFINED"))) ~~ FunctionKeyword) ~~ FunctionIdentifier

  private def FunctionIdentifier: Rule1[List[ast.PrivilegeQualifier]] = rule("function identifier") {
    oneOrMore(group(GlobbedNamespace ~ GlobbedFunctionName), separator = CommaSep) ~~>>
      { functions => pos => functions.map(p => ast.FunctionQualifier(p._1, p._2)(pos)) }
  }

  // Database specific

  private def Database: Rule1[List[ast.DatabaseScope]] = rule("on a database") {
    keyword("ON DEFAULT DATABASE") ~~~> (pos => List(ast.DefaultDatabaseScope()(pos))) |
    group(keyword("ON") ~~ DatabaseKeyword) ~~ group(
      (SymbolicDatabaseNameOrStringParameterList ~~>> (params => pos => params.map(ast.NamedDatabaseScope(_)(pos)))) |
      (keyword("*") ~~~> (pos => List(ast.AllDatabasesScope()(pos))))
    )
  }

  private def DatabaseAction: Rule1[ast.DatabaseAction] = rule("database action") {
    keyword("ACCESS") ~~~> (_ => ast.AccessDatabaseAction) |
    keyword("START") ~~~> (_ => ast.StartDatabaseAction) |
    keyword("STOP") ~~~> (_ => ast.StopDatabaseAction) |
    group(keyword("CREATE") ~~ IndexKeyword) ~~~> (_ => ast.CreateIndexAction) |
    group(keyword("DROP") ~~ IndexKeyword) ~~~> (_ => ast.DropIndexAction) |
    group(IndexKeyword ~~ optional(keyword("MANAGEMENT"))) ~~~> (_ => ast.AllIndexActions) |
    group(keyword("CREATE") ~~ ConstraintKeyword) ~~~> (_ => ast.CreateConstraintAction) |
    group(keyword("DROP") ~~ ConstraintKeyword) ~~~> (_ => ast.DropConstraintAction) |
    group(ConstraintKeyword ~~ optional(keyword("MANAGEMENT"))) ~~~> (_ => ast.AllConstraintActions) |
    group(keyword("CREATE NEW") ~~ optional(keyword("NODE")) ~~ LabelKeyword) ~~~> (_ => ast.CreateNodeLabelAction) |
    group(keyword("CREATE NEW") ~~ optional(keyword("RELATIONSHIP")) ~~ TypeKeyword) ~~~> (_ => ast.CreateRelationshipTypeAction) |
    group(keyword("CREATE NEW") ~~ optional(keyword("PROPERTY")) ~~ NameKeyword) ~~~> (_ => ast.CreatePropertyKeyAction) |
    group(keyword("NAME") ~~ optional(keyword("MANAGEMENT"))) ~~~> (_ => ast.AllTokenActions) |
    group(keyword("ALL") ~~ optional(optional(keyword("DATABASE")) ~~ keyword("PRIVILEGES"))) ~~~> (_ => ast.AllDatabaseAction)
  }

  private def QualifiedDatabaseAction: Rule2[List[ast.DatabasePrivilegeQualifier], ast.DatabaseAction] = rule("qualified database action") {
    group(keyword("SHOW") ~~ TransactionKeyword ~~ UserQualifier ~> (_ => ast.ShowTransactionAction)) |
    group(keyword("SHOW") ~~ TransactionKeyword ~> (_ => List(ast.UserAllQualifier()(InputPosition.NONE))) ~> (_ => ast.ShowTransactionAction)) |
    group(keyword("TERMINATE") ~~ TransactionKeyword ~~ UserQualifier ~> (_ => ast.TerminateTransactionAction)) |
    group(keyword("TERMINATE") ~~ TransactionKeyword ~> (_ => List(ast.UserAllQualifier()(InputPosition.NONE))) ~> (_ => ast.TerminateTransactionAction)) |
    group(keyword("TRANSACTION") ~~ optional(keyword("MANAGEMENT")) ~~ UserQualifier ~> (_ => ast.AllTransactionActions)) |
    group(keyword("TRANSACTION") ~~ optional(keyword("MANAGEMENT")) ~> (_ => List(ast.UserAllQualifier()(InputPosition.NONE))) ~> (_ => ast.AllTransactionActions))
  }

  private def UserQualifier: Rule1[List[ast.DatabasePrivilegeQualifier]] = rule("(usernameList)") {
    group("(" ~~ SymbolicNameOrStringParameterList ~~ ")") ~~>> { userName => pos => userName.map(ast.UserQualifier(_)(pos)) } |
    group("(" ~~ "*" ~~ ")") ~~~> { pos => List(ast.UserAllQualifier()(pos)) }
  }

  // Graph specific

  private def Graph: Rule1[List[ast.GraphScope]] = rule("on a graph") {
    keyword("ON DEFAULT GRAPH") ~~~> (pos => List(ast.DefaultGraphScope()(pos))) |
    group(keyword("ON") ~~ GraphKeyword) ~~ group(
      (SymbolicDatabaseNameOrStringParameterList ~~>> (names => ipp => names.map(ast.NamedGraphScope(_)(ipp)))) |
      keyword("*") ~~~> (ipp => List(ast.AllGraphsScope()(ipp)))
    )
  }

  private def GraphAction: Rule3[List[ast.GraphScope], ast.GraphAction, List[ast.GraphPrivilegeQualifier]] = rule("graph action") {
    group(keyword("ALL") ~~ optional(optional(keyword("GRAPH")) ~~ keyword("PRIVILEGES"))) ~~ Graph ~>
      (_ => ast.AllGraphAction) ~> (_ => List(ast.AllQualifier()(InputPosition.NONE))) |
    group(keyword("WRITE") ~~ Graph) ~> (_ => ast.WriteAction) ~> (_ => List(ast.ElementsAllQualifier()(InputPosition.NONE)))
  }

  private def QualifiedGraphAction: Rule3[List[ast.GraphScope], List[ast.GraphPrivilegeQualifier], ast.GraphAction] = rule("qualified graph action") {
    group(keyword("CREATE") ~~ Graph ~~ ScopeQualifier ~> (_ => ast.CreateElementAction)) |
    group(keyword("DELETE") ~~ Graph ~~ ScopeQualifier ~> (_ => ast.DeleteElementAction)) |
    group(keyword("TRAVERSE") ~~ Graph ~~ ScopeQualifierWithProperty ~> (_ => ast.TraverseAction))
  }

  private def GraphActionWithResource: Rule3[ast.ActionResource, List[ast.GraphScope], ast.GraphAction] = rule("graph action with resource") {
    group(keyword("SET LABEL") ~~ LabelResource ~~ Graph ~> (_ => ast.SetLabelAction)) |
    group(keyword("REMOVE LABEL") ~~ LabelResource ~~ Graph ~> (_ => ast.RemoveLabelAction))
  }

  private def QualifiedGraphActionWithResource: Rule4[ast.ActionResource, List[ast.GraphScope], List[ast.GraphPrivilegeQualifier], ast.GraphAction] = rule("qualified graph action with resource") {
    group(keyword("MERGE") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifier ~> (_ => ast.MergeAdminAction)) |
    group(keyword("SET PROPERTY") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifier ~> (_ => ast.SetPropertyAction)) |
    group(keyword("READ")  ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifierWithProperty ~> (_ => ast.ReadAction)) |
    group(keyword("MATCH")  ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifierWithProperty ~> (_ => ast.MatchAction))
  }

  private def ScopeQualifier: Rule1[List[ast.GraphPrivilegeQualifier]] = rule("which element type and associated labels/relTypes qualifier combination") {
    group(RelationshipKeyword ~~ SymbolicNamesList) ~~>> { relNames => pos => relNames.map(ast.RelationshipQualifier(_)(pos)) } |
    group(RelationshipKeyword ~~ "*") ~~~> { pos => List(ast.RelationshipAllQualifier()(pos)) } |
    group(NodeKeyword ~~ SymbolicNamesList) ~~>> { nodeName => pos => nodeName.map(ast.LabelQualifier(_)(pos)) } |
    group(NodeKeyword ~~ "*") ~~~> { pos => List(ast.LabelAllQualifier()(pos)) } |
    group(ElementKeyword ~~ SymbolicNamesList) ~~>> { elemName => pos => elemName.map(ast.ElementQualifier(_)(pos)) } |
    optional(ElementKeyword ~~ "*") ~~~> { pos => List(ast.ElementsAllQualifier()(pos)) }
  }

  private def ScopeQualifierWithProperty: Rule1[List[ast.GraphPrivilegeQualifier]] = rule("which element type and associated labels/relTypes (props) qualifier combination") {
    ScopeQualifier ~~ optional("(" ~~ "*" ~~ ")")
  }

  private def LabelResource: Rule1[ast.ActionResource] = rule("label used for set/remove label") {
    group(SymbolicNamesList  ~~>> {ast.LabelsResource(_)} |
    group(keyword("*") ~~~> {ast.AllLabelResource()}))
  }

  private def PrivilegeProperty: Rule1[ast.ActionResource] = rule("{propertyList}") {
    group("{" ~~ SymbolicNamesList ~~ "}") ~~>> {ast.PropertiesResource(_)} |
    group("{" ~~ "*" ~~ "}") ~~~> {ast.AllPropertyResource()}
  }

  // Database management commands

  def ShowDatabase: Rule1[ast.ShowDatabase] = rule("SHOW DATABASE") {
    group(keyword("SHOW") ~~ ScopeForShowDatabase) ~~ optional(ShowCommandClauses) ~~>> (ast.ShowDatabase(_,_))
  }

  private def ScopeForShowDatabase: Rule1[ast.DatabaseScope] = rule("show database scope") {
    group(keyword("DATABASE") ~~ SymbolicDatabaseNameOrStringParameter) ~~>> (ast.NamedDatabaseScope(_)) |
    keyword("DATABASES") ~~~> ast.AllDatabasesScope() |
    keyword("DEFAULT DATABASE") ~~~> ast.DefaultDatabaseScope()
  }

  def CreateDatabase: Rule1[ast.CreateDatabase] = rule("CREATE DATABASE") {
    group(keyword("CREATE OR REPLACE DATABASE") ~~ SymbolicDatabaseNameOrStringParameter ~~ keyword("IF NOT EXISTS") ~~ WaitUntilComplete) ~~>> (ast.CreateDatabase(_, ast.IfExistsInvalidSyntax, _)) |
    group(keyword("CREATE OR REPLACE DATABASE") ~~ SymbolicDatabaseNameOrStringParameter ~~ WaitUntilComplete) ~~>> (ast.CreateDatabase(_, ast.IfExistsReplace, _)) |
    group(keyword("CREATE DATABASE") ~~ SymbolicDatabaseNameOrStringParameter ~~ keyword("IF NOT EXISTS") ~~ WaitUntilComplete) ~~>> (ast.CreateDatabase(_, ast.IfExistsDoNothing, _)) |
    group(keyword("CREATE DATABASE") ~~ SymbolicDatabaseNameOrStringParameter ~~ WaitUntilComplete) ~~>> (ast.CreateDatabase(_, ast.IfExistsThrowError, _))
  }

  def DropDatabase: Rule1[ast.DropDatabase] = rule("DROP DATABASE") {
    group(keyword("DROP DATABASE") ~~ SymbolicDatabaseNameOrStringParameter ~~ keyword("IF EXISTS") ~~ DataAction ~~ WaitUntilComplete) ~~>>
      ((dbName, dataAction, wait) => ast.DropDatabase(dbName, ifExists = true, dataAction, wait)) |
    group(keyword("DROP DATABASE") ~~ SymbolicDatabaseNameOrStringParameter ~~ DataAction ~~ WaitUntilComplete) ~~>>
      ((dbName, dataAction, wait) => ast.DropDatabase(dbName, ifExists = false, dataAction, wait))
  }

  private def DataAction: Rule1[ast.DropDatabaseAdditionalAction] = rule("data action on drop database") {
    keyword("DUMP DATA") ~~~> (_ => ast.DumpData) |
    optional(keyword("DESTROY DATA")) ~~~> (_ => ast.DestroyData)
  }

  def StartDatabase: Rule1[ast.StartDatabase] = rule("START DATABASE") {
    group(keyword("START DATABASE") ~~ SymbolicDatabaseNameOrStringParameter ~~ WaitUntilComplete) ~~>> (ast.StartDatabase(_, _))
  }

  def StopDatabase: Rule1[ast.StopDatabase] = rule("STOP DATABASE") {
    group(keyword("STOP DATABASE") ~~ SymbolicDatabaseNameOrStringParameter ~~ WaitUntilComplete) ~~>> (ast.StopDatabase(_, _))
  }

  // Graph/View commands

  def CreateGraph: Rule1[ast.CreateGraph] = rule("CATALOG CREATE GRAPH") {
    group(keyword("CATALOG CREATE GRAPH") ~~ CatalogName ~~ "{" ~~
      RegularQuery ~~
      "}") ~~>> (ast.CreateGraph(_, _))
  }

  def DropGraph: Rule1[ast.DropGraph] = rule("CATALOG DROP GRAPH") {
    group(keyword("CATALOG DROP GRAPH") ~~ CatalogName) ~~>> (ast.DropGraph(_))
  }

  def CreateView: Rule1[ast.CreateView] = rule("CATALOG CREATE VIEW") {
    group((keyword("CATALOG CREATE VIEW") | keyword("CATALOG CREATE QUERY")) ~~
      CatalogName ~~ optional("(" ~~ zeroOrMore(Parameter, separator = CommaSep) ~~ ")") ~~ "{" ~~
      captureString(RegularQuery) ~~
      "}") ~~>> { case (name, params, (query, string)) => ast.CreateView(name, params.getOrElse(Seq.empty), query, string) }
  }

  def DropView: Rule1[ast.DropView] = rule("CATALOG DROP VIEW") {
    group((keyword("CATALOG DROP VIEW") | keyword("CATALOG DROP QUERY")) ~~ CatalogName) ~~>> (ast.DropView(_))
  }

  // Shared help methods

  private def ShowCommandClauses: Rule1[Either[(ast.Yield, Option[ast.Return]), ast.Where]] = rule("YIELD, WHERE") {
    (Yield ~~ optional(ReturnWithoutGraph)) ~~> ((y,r) => Left(y,r)) |
      (Where ~~>> (where => _ => Right(where)))
  }

  def SymbolicNameOrStringParameter: Rule1[Either[String, Parameter]] =
    group(SymbolicNameString) ~~>> (s => _ => Left(s)) |
    group(StringParameter) ~~>> (p => _ => Right(p))

  def SymbolicDatabaseNameOrStringParameter: Rule1[Either[String, Parameter]] =
    group(SymbolicDatabaseNameString) ~~>> (s => _ => Left(s)) |
    group(StringParameter) ~~>> (p => _ => Right(p))

  def SymbolicNameOrStringParameterList: Rule1[List[Either[String, Parameter]]] = rule("a list of symbolic names or string parameters") {
    //noinspection LanguageFeature
    (oneOrMore(WS ~~ SymbolicNameOrStringParameter ~~ WS, separator = ",") memoMismatches).suppressSubnodes
  }

  def SymbolicDatabaseNameOrStringParameterList: Rule1[List[Either[String, Parameter]]] = rule("a list of symbolic database names or string parameters") {
    //noinspection LanguageFeature
    (oneOrMore(WS ~~ SymbolicDatabaseNameOrStringParameter ~~ WS, separator = ",") memoMismatches).suppressSubnodes
  }

  private def WaitUntilComplete: Rule1[WaitUntilComplete] = rule("WAIT [n [SEC[OND[S]]]] | NOWAIT") {
    group(keyword("WAIT") ~~ UnsignedIntegerLiteral ~~ optional(keyword("SEC") | keyword("SECOND") | keyword("SECONDS")) ~~>
      (timeout => TimeoutAfter(timeout.value))) |
      keyword("WAIT") ~> (_ => IndefiniteWait) |
      optional(keyword("NOWAIT")) ~> (_ => NoWait)
  }

  // Keyword methods

  private def RoleKeyword: Rule0 = keyword("ROLES") | keyword("ROLE")

  private def UserKeyword: Rule0 = keyword("USERS") | keyword("USER")

  private def PrivilegeKeyword: Rule0 = keyword("PRIVILEGES") | keyword("PRIVILEGE")

  // Dbms specific

  private def PasswordKeyword: Rule0 = keyword("PASSWORDS") | keyword("PASSWORD")

  private def ProcedureKeyword: Rule0 = keyword("PROCEDURES") | keyword("PROCEDURE")

  private def FunctionKeyword: Rule0 = keyword("FUNCTIONS") | keyword("FUNCTION")

  private def AdminKeyword: Rule0 = keyword("ADMINISTRATOR") | keyword("ADMIN")

  private def CommandKeyword: Rule0 = keyword("COMMANDS") | keyword("COMMAND")

  // Database specific

  private def DatabaseKeyword: Rule0 = keyword("DATABASES") | keyword("DATABASE")

  private def ConstraintKeyword: Rule0 = keyword("CONSTRAINTS") | keyword("CONSTRAINT")

  private def LabelKeyword: Rule0 = keyword("LABELS") | keyword("LABEL")

  private def TypeKeyword: Rule0 = keyword("TYPES") | keyword("TYPE")

  private def NameKeyword: Rule0 = keyword("NAMES") | keyword("NAME")

  private def TransactionKeyword: Rule0 = keyword("TRANSACTIONS") | keyword("TRANSACTION")

  // Graph specific

  private def GraphKeyword: Rule0 = keyword("GRAPHS") | keyword("GRAPH")

  private def ElementKeyword: Rule0 = keyword("ELEMENTS") | keyword("ELEMENT")

  private def RelationshipKeyword: Rule0 = keyword("RELATIONSHIPS") | keyword("RELATIONSHIP")

  private def NodeKeyword: Rule0 = keyword("NODES") | keyword("NODE")

}
