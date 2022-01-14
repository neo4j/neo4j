/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.TimeoutAfter
import org.neo4j.cypher.internal.ast.WaitUntilComplete
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.util.InputPosition
import org.parboiled.scala.Parser
import org.parboiled.scala.Rule0
import org.parboiled.scala.Rule1
import org.parboiled.scala.Rule2
import org.parboiled.scala.Rule3
import org.parboiled.scala.Rule4
import org.parboiled.scala.group

import scala.language.postfixOps

//noinspection ConvertibleToMethodValue
// Can't convert since that breaks parsing
trait AdministrationCommand extends Parser
                            with GraphSelection
                            with CommandHelper
                            with Base {

  def AdministrationCommand: Rule1[ast.AdministrationCommand] = rule("Administration command") {
    optional(UseGraph) ~~ AllAdministrationCommands ~~> ((use, command) => command.withGraph(use))
  }

  private def AllAdministrationCommands: Rule1[ast.AdministrationCommand] = rule("Administration command") {
    MultiDatabaseAdministrationCommand | PrivilegeAdministrationCommand | UserAndRoleAdministrationCommand
  }

  private def MultiDatabaseAdministrationCommand: Rule1[ast.AdministrationCommand] = rule("MultiDatabase administration command") {
    ShowDatabase | CreateDatabase | DropDatabase | StartDatabase | StopDatabase
  }

  private def UserAndRoleAdministrationCommand: Rule1[ast.AdministrationCommand] = rule("Security role and user administration command") {
    ShowRoles | CreateRole | RenameRole | DropRole | ShowUsers | ShowCurrentUser | CreateUser | RenameUser | DropUser | AlterUser | SetOwnPassword
  }

  private def PrivilegeAdministrationCommand: Rule1[ast.AdministrationCommand] = rule("Security privilege administration command") {
    ShowPrivileges | GrantCommand | DenyCommand | RevokeCommand
  }

  private def GrantCommand: Rule1[ast.AdministrationCommand] = rule("Security privilege grant command") {
      GrantRole | GrantDatabasePrivilege | GrantGraphPrivilege | GrantDbmsPrivilege
  }

  private def DenyCommand: Rule1[ast.AdministrationCommand] = rule("Security privilege deny command") {
    DenyDatabasePrivilege | DenyGraphPrivilege | DenyDbmsPrivilege
  }

  private def RevokeCommand: Rule1[ast.AdministrationCommand] = rule("Security privilege revoke command") {
    RevokeRole | RevokeGraphPrivilege | RevokeDatabasePrivilege | RevokeDbmsPrivilege
  }

  // User management commands

  private def ShowUsers: Rule1[ast.ShowUsers] = rule("SHOW USERS") {
    keyword("SHOW USERS") ~~ optional(ShowCommandClauses) ~~>> (ast.ShowUsers(_))
  }

  private def ShowCurrentUser: Rule1[ast.ShowCurrentUser] = rule("SHOW CURRENT USER") {
    keyword("SHOW CURRENT USER") ~~ optional(ShowCommandClauses) ~~>> (ast.ShowCurrentUser(_))
  }

  private def CreateUser: Rule1[ast.CreateUser] = rule("CREATE USER") {
    // CREATE [OR REPLACE] USER username [IF NOT EXISTS] SET [PLAINTEXT | ENCRYPTED] PASSWORD stringLiteralPassword|parameterPassword [{SET PASSWORD CHANGE [NOT] REQUIRED} | {SET STATUS SUSPENDED|ACTIVE} | {SET HOME DATABASE name}]*
    group(CreateUserStart ~~ SetPassword ~~ optional(UserOptions)) ~~>>
      ((userName, ifExistsDo, isEncryptedPassword, initialPassword, userOptions) => {
        val createUserOptions = userOptions.map {
          // Since we in the prettifier adds `CHANGE REQUIRED` if omitted,
          // we should parse them to the same thing (as we did before adding UserOptions)
          case ast.UserOptions(None, s, d) => ast.UserOptions(Some(true), s, d)
          case u => u
        }.getOrElse(ast.UserOptions(Some(true), None, None))
        ast.CreateUser(userName, isEncryptedPassword, initialPassword, createUserOptions, ifExistsDo)
      })
  }

  private def CreateUserStart: Rule2[Either[String, Parameter], ast.IfExistsDo] = {
    // returns: userName, IfExistsDo
    group(keyword("CREATE OR REPLACE USER") ~~ SymbolicNameOrStringParameter ~~ keyword("IF NOT EXISTS") ~> (_ => ast.IfExistsInvalidSyntax)) |
    group(keyword("CREATE OR REPLACE USER") ~~ SymbolicNameOrStringParameter ~> (_ => ast.IfExistsReplace)) |
    group(keyword("CREATE USER") ~~ SymbolicNameOrStringParameter ~~ keyword("IF NOT EXISTS") ~> (_ => ast.IfExistsDoNothing)) |
    group(keyword("CREATE USER") ~~ SymbolicNameOrStringParameter ~> (_ => ast.IfExistsThrowError))
  }

  private def RenameUser: Rule1[ast.RenameUser] = rule("RENAME USER") {
    val renameUserStart = keyword("RENAME USER") ~~ SymbolicNameOrStringParameter
    group(renameUserStart ~~ keyword("IF EXISTS") ~~ keyword("TO") ~~ SymbolicNameOrStringParameter) ~~>> (ast.RenameUser(_, _, ifExists = true)) |
    group(renameUserStart ~~ keyword("TO") ~~ SymbolicNameOrStringParameter) ~~>> (ast.RenameUser(_, _, ifExists = false))
  }

  private def DropUser: Rule1[ast.DropUser] = rule("DROP USER") {
    val dropUserStart = keyword("DROP USER") ~~ SymbolicNameOrStringParameter
    group(dropUserStart ~~ keyword("IF EXISTS")) ~~>> (ast.DropUser(_, ifExists = true)) |
    group(dropUserStart) ~~>> (ast.DropUser(_, ifExists = false))
  }

  private def AlterUser: Rule1[ast.AlterUser] = rule("ALTER USER") {
    val alterUserStart = AlterUserStart
    // ALTER USER username [IF EXISTS] SET [PLAINTEXT | ENCRYPTED] PASSWORD stringLiteralPassword|parameterPassword [{SET PASSWORD CHANGE [NOT] REQUIRED} | {SET STATUS SUSPENDED|ACTIVE} | {SET HOME DATABASE name}]*
    group(alterUserStart ~~ SetPassword ~~ optional(UserOptions)) ~~>>
      ((userName, ifExists, isEncryptedPassword, initialPassword, userOptions) => ast.AlterUser(userName, Some(isEncryptedPassword), Some(initialPassword), userOptions.getOrElse(ast.UserOptions(None, None, None)), ifExists)) |
    //
    // ALTER USER username [IF EXISTS] [{SET PASSWORD CHANGE [NOT] REQUIRED} | {SET STATUS SUSPENDED|ACTIVE} | {SET HOME DATABASE name}]+
    group(alterUserStart ~~ UserOptionsWithSetPart) ~~>> ((userName, ifExists, userOptions) => ast.AlterUser(userName, None, None, userOptions, ifExists)) |
    // ALTER USER username [IF EXISTS] REMOVE HOME DATABASE
    group(alterUserStart ~~ keyword("REMOVE HOME DATABASE")) ~~>> ((userName, ifExists) => ast.AlterUser(userName, None, None, ast.UserOptions(None, None, Some(ast.RemoveHomeDatabaseAction)), ifExists))
  }

  private def AlterUserStart: Rule2[Either[String, Parameter], Boolean] = {
    // returns: userName, IfExists
    val alterUserStart = keyword("ALTER USER") ~~ SymbolicNameOrStringParameter
    group(alterUserStart ~~ keyword("IF EXISTS") ~> (_ => true)) |
    group(alterUserStart ~> (_ => false))
  }

  private def SetOwnPassword: Rule1[ast.SetOwnPassword] = rule("ALTER CURRENT USER SET PASSWORD") {
    // ALTER CURRENT USER SET PASSWORD FROM stringLiteralPassword|parameterPassword TO stringLiteralPassword|parameterPassword
    group(keyword("ALTER CURRENT USER SET PASSWORD FROM") ~~ PasswordExpression ~~ keyword("TO") ~~ PasswordExpression) ~~>>
      ((currentPassword, newPassword) => ast.SetOwnPassword(newPassword, currentPassword))
  }

  private def SetPassword: Rule2[Boolean, Expression] = rule("set encrypted or plaintext password") {
    // returns: isEncryptedPassword
    group(
      group(keyword("SET") ~~ optional(keyword("PLAINTEXT")) ~~ keyword("PASSWORD") ~> (_ => false)) |
      group(keyword("SET ENCRYPTED PASSWORD") ~> (_ => true))
    ) ~~ PasswordExpression
  }

  private def PasswordExpression: Rule1[Expression] = group(SensitiveStringLiteral | SensitiveStringParameter)

  private def UserOptions: Rule1[ast.UserOptions] =
    RequirePasswordChangeNoSetKeyword ~~ SetStatus ~~ SetHomeDatabase ~~> ((password, status, database) => ast.UserOptions(Some(password), Some(status), Some(database))) |
    RequirePasswordChangeNoSetKeyword ~~ SetHomeDatabase ~~ SetStatus ~~> ((password, database, status) => ast.UserOptions(Some(password), Some(status), Some(database))) |
    RequirePasswordChangeNoSetKeyword ~~ SetStatus                    ~~> ((password, status)           => ast.UserOptions(Some(password), Some(status), None)) |
    RequirePasswordChangeNoSetKeyword ~~ SetHomeDatabase              ~~> ((password, database)         => ast.UserOptions(Some(password), None, Some(database))) |
    RequirePasswordChangeNoSetKeyword                                 ~~> (password                     => ast.UserOptions(Some(password), None, None)) |
    UserOptionsWithSetPart

  private def UserOptionsWithSetPart: Rule1[ast.UserOptions] =
    RequirePasswordChange ~~ SetStatus ~~ SetHomeDatabase             ~~> ((password, status, database) => ast.UserOptions(Some(password), Some(status), Some(database))) |
    RequirePasswordChange ~~ SetHomeDatabase ~~ SetStatus             ~~> ((password, database, status) => ast.UserOptions(Some(password), Some(status), Some(database))) |
    SetStatus ~~ RequirePasswordChange ~~ SetHomeDatabase             ~~> ((status, password, database) => ast.UserOptions(Some(password), Some(status), Some(database))) |
    SetStatus ~~ SetHomeDatabase ~~ RequirePasswordChange             ~~> ((status, database, password) => ast.UserOptions(Some(password), Some(status), Some(database))) |
    SetHomeDatabase ~~ RequirePasswordChange ~~ SetStatus             ~~> ((database, password, status) => ast.UserOptions(Some(password), Some(status), Some(database))) |
    SetHomeDatabase ~~ SetStatus ~~ RequirePasswordChange             ~~> ((database, status, password) => ast.UserOptions(Some(password), Some(status), Some(database))) |
    RequirePasswordChange ~~ SetStatus                                ~~> ((password, status)           => ast.UserOptions(Some(password), Some(status), None)) |
    RequirePasswordChange ~~ SetHomeDatabase                          ~~> ((password, database)         => ast.UserOptions(Some(password), None, Some(database))) |
    SetStatus ~~ RequirePasswordChange                                ~~> ((status, password)           => ast.UserOptions(Some(password), Some(status), None)) |
    SetStatus ~~ SetHomeDatabase                                      ~~> ((status, database)           => ast.UserOptions(None, Some(status), Some(database))) |
    SetHomeDatabase ~~ RequirePasswordChange                          ~~> ((database, password)         => ast.UserOptions(Some(password), None, Some(database))) |
    SetHomeDatabase ~~ SetStatus                                      ~~> ((database, status)           => ast.UserOptions(None, Some(status), Some(database))) |
    RequirePasswordChange                                             ~~> (password                     => ast.UserOptions(Some(password), None, None)) |
    SetStatus                                                         ~~> (status                       => ast.UserOptions(None, Some(status), None)) |
    SetHomeDatabase                                                   ~~> (database                     => ast.UserOptions(None, None, Some(database)))

  private def RequirePasswordChangeNoSetKeyword: Rule1[Boolean] =
    keyword("CHANGE NOT REQUIRED") ~>>> (_ => _ => false) |
    keyword("CHANGE REQUIRED") ~>>> (_ => _ => true)

  //noinspection MutatorLikeMethodIsParameterless
  private def RequirePasswordChange: Rule1[Boolean] =
    keyword("SET PASSWORD CHANGE NOT REQUIRED") ~>>> (_ => _ => false) |
    keyword("SET PASSWORD CHANGE REQUIRED") ~>>> (_ => _ => true)

  private def SetStatus: Rule1[Boolean] =
    keyword("SET STATUS SUSPENDED") ~>>> (_ => _ => true) |
    keyword("SET STATUS ACTIVE") ~>>> (_ => _ => false)

  private def SetHomeDatabase: Rule1[ast.SetHomeDatabaseAction] =
    keyword("SET HOME DATABASE") ~~ SymbolicDatabaseNameOrStringParameter ~~>> (name => _ => ast.SetHomeDatabaseAction(name))

  // Role management commands

  private def ShowRoles: Rule1[ast.ShowRoles] = rule("SHOW ROLES") {
    val showAllRoles = ShowAllRoles
    //SHOW [ ALL | POPULATED ] ROLES WITH USERS
    group(showAllRoles ~~ keyword("WITH USERS") ~~ optional(ShowCommandClauses)) ~~>> (ast.ShowRoles(withUsers = true, _, _)) |
    // SHOW [ ALL | POPULATED ] ROLES
    group(showAllRoles ~~ optional(ShowCommandClauses)) ~~>> (ast.ShowRoles(withUsers = false, _, _))
  }

  private def ShowAllRoles: Rule1[Boolean] = rule("return true for SHOW ALL ROLES, false for SHOW POPULATED ROLES") {
    keyword("SHOW POPULATED ROLES") ~~~> (_ => false) |
    group(keyword("SHOW") ~~ optional(keyword("ALL")) ~~ keyword("ROLES")) ~~~> (_ => true)
  }

  private def CreateRole: Rule1[ast.CreateRole] = rule("CREATE ROLE") {
    group(CreateRoleStart ~~ optional(keyword("AS COPY OF") ~~ SymbolicNameOrStringParameter)) ~~>> ((roleName, ifExistsDo, from) => ast.CreateRole(roleName, from, ifExistsDo))
  }

  private def CreateRoleStart: Rule2[Either[String, Parameter], ast.IfExistsDo] = {
    // returns: roleName, IfExistsDo
    group(keyword("CREATE OR REPLACE ROLE") ~~ SymbolicNameOrStringParameter ~~ keyword("IF NOT EXISTS") ~> (_ => ast.IfExistsInvalidSyntax)) |
    group(keyword("CREATE OR REPLACE ROLE") ~~ SymbolicNameOrStringParameter ~> (_ => ast.IfExistsReplace)) |
    group(keyword("CREATE ROLE") ~~ SymbolicNameOrStringParameter ~~ keyword("IF NOT EXISTS") ~> (_ => ast.IfExistsDoNothing)) |
    group(keyword("CREATE ROLE") ~~ SymbolicNameOrStringParameter ~> (_ => ast.IfExistsThrowError))
  }

  private def RenameRole: Rule1[ast.RenameRole] = rule("RENAME ROLE") {
    val renameRoleStart = keyword("RENAME ROLE") ~~ SymbolicNameOrStringParameter
    group(renameRoleStart ~~ keyword("IF EXISTS") ~~ keyword("TO") ~~ SymbolicNameOrStringParameter) ~~>> (ast.RenameRole(_, _, ifExists = true)) |
    group(renameRoleStart ~~ keyword("TO") ~~ SymbolicNameOrStringParameter) ~~>> (ast.RenameRole(_, _, ifExists = false))
  }

  private def DropRole: Rule1[ast.DropRole] = rule("DROP ROLE") {
    val dropRuleStart = keyword("DROP ROLE") ~~ SymbolicNameOrStringParameter
    group(dropRuleStart ~~ keyword("IF EXISTS")) ~~>> (ast.DropRole(_, ifExists = true)) |
    group(dropRuleStart) ~~>> (ast.DropRole(_, ifExists = false))
  }

  private def GrantRole: Rule1[ast.GrantRolesToUsers] = rule("GRANT ROLE") {
    group(keyword("GRANT") ~~ RoleKeyword ~~ SymbolicNameOrStringParameterList ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>> (ast.GrantRolesToUsers(_, _))
  }

  private def RevokeRole: Rule1[ast.RevokeRolesFromUsers] = rule("REVOKE ROLE") {
    group(keyword("REVOKE") ~~ RoleKeyword ~~ SymbolicNameOrStringParameterList ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>> (ast.RevokeRolesFromUsers(_, _))
  }

  // Privilege commands

  private def ShowPrivileges: Rule1[ast.ReadAdministrationCommand] = rule("SHOW PRIVILEGES") {
    val showPrivilegesStart = keyword("SHOW") ~~ ScopeForShowPrivileges
    group(showPrivilegesStart ~~ asCommand ~~ optional(ShowCommandClauses) ~~>> ((scope, revoke, yld) => ast.ShowPrivilegeCommands(scope, revoke, yld))) |
    group(showPrivilegesStart ~~ optional(ShowCommandClauses) ~~>> ((scope, yld) => ast.ShowPrivileges(scope, yld)))
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
  private def GrantDbmsPrivilege: Rule1[ast.GrantPrivilege] = rule("GRANT dbms privileges") {
    group(keyword("GRANT") ~~ DbmsAction ~~ keyword("ON DBMS TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((dbmsAction, grantees) => pos => ast.GrantPrivilege.dbmsAction(dbmsAction, grantees, List(ast.AllQualifier()(pos)))(pos)) |
    group(keyword("GRANT") ~~ QualifiedDbmsAction ~~ keyword("ON DBMS TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((qualifier, dbmsAction, grantees) => ast.GrantPrivilege.dbmsAction( dbmsAction, grantees, qualifier ))
  }

  private def DenyDbmsPrivilege: Rule1[ast.DenyPrivilege] = rule("DENY dbms privileges") {
    group(keyword("DENY") ~~ DbmsAction ~~ keyword("ON DBMS TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((dbmsAction, grantees) => ast.DenyPrivilege.dbmsAction( dbmsAction, grantees )) |
    group(keyword("DENY") ~~ QualifiedDbmsAction ~~ keyword("ON DBMS TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((qualifier, dbmsAction, grantees) => ast.DenyPrivilege.dbmsAction( dbmsAction, grantees, qualifier ))
  }

  private def RevokeDbmsPrivilege: Rule1[ast.RevokePrivilege] = rule("REVOKE dbms privileges") {
    group(RevokeType ~~ DbmsAction ~~ keyword("ON DBMS FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((revokeType, dbmsAction, grantees) => ast.RevokePrivilege.dbmsAction(dbmsAction, grantees, revokeType)) |
    group(RevokeType ~~ QualifiedDbmsAction ~~ keyword("ON DBMS FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((revokeType, qualifier, dbmsAction, grantees) => ast.RevokePrivilege.dbmsAction(dbmsAction, grantees, revokeType, qualifier))
  }

  //` ... ON DATABASE foo TO role`
  private def GrantDatabasePrivilege: Rule1[ast.GrantPrivilege] = rule("GRANT database privileges") {
    group(keyword("GRANT") ~~ QualifiedDatabaseAction ~~ Database ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((qualifier, databaseAction, scope, grantees) => ast.GrantPrivilege.databaseAction( databaseAction, scope, grantees, qualifier)) |
    group(keyword("GRANT") ~~ DatabaseAction ~~ Database ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((databaseAction, scope, grantees) => ast.GrantPrivilege.databaseAction( databaseAction, scope, grantees))
  }

  private def DenyDatabasePrivilege: Rule1[ast.DenyPrivilege] = rule("DENY database privileges") {
    group(keyword("DENY") ~~ QualifiedDatabaseAction ~~ Database ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((qualifier, databaseAction, scope, grantees) => ast.DenyPrivilege.databaseAction( databaseAction, scope, grantees, qualifier)) |
    group(keyword("DENY") ~~ DatabaseAction ~~ Database ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((databaseAction, scope, grantees) => ast.DenyPrivilege.databaseAction( databaseAction, scope, grantees))
  }

  private def RevokeDatabasePrivilege: Rule1[ast.RevokePrivilege] = rule("REVOKE database privileges") {
    val revokeStart = RevokeType
    group(revokeStart ~~ QualifiedDatabaseAction ~~ Database ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((revokeType, qualifier, databaseAction, scope, grantees) => ast.RevokePrivilege.databaseAction(databaseAction, scope, grantees, revokeType, qualifier)) |
    group(revokeStart ~~ DatabaseAction ~~ Database ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((revokeType, databaseAction, scope, grantees) => ast.RevokePrivilege.databaseAction(databaseAction, scope, grantees, revokeType))
  }

   //` ... ON GRAPH foo TO role`
  private def GrantGraphPrivilege: Rule1[ast.GrantPrivilege] = rule("GRANT graph privileges") {
    group(keyword("GRANT") ~~ GraphAction ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, action, qualifier, roles) => ast.GrantPrivilege.graphAction(action, None, graphScope, qualifier, roles)) |
    group(keyword("GRANT") ~~ QualifiedGraphAction ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, qualifier, action, roles) => ast.GrantPrivilege.graphAction(action, None, graphScope, qualifier, roles)) |
    group(keyword("GRANT") ~~ GraphActionWithResource ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((resource, graphScope, action, roles) => ast.GrantPrivilege.graphAction(action, Some(resource), graphScope, List(ast.LabelAllQualifier()(InputPosition.NONE)), roles)) |
    group(keyword("GRANT") ~~ QualifiedGraphActionWithResource ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((resource, graphScope, qualifier, action, roles) => ast.GrantPrivilege.graphAction(action, Some(resource), graphScope, qualifier, roles))
  }

  private def DenyGraphPrivilege: Rule1[ast.DenyPrivilege] = rule("DENY graph privileges") {
    group(keyword("DENY") ~~ GraphAction ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, action, qualifier, roles) => ast.DenyPrivilege.graphAction(action, None, graphScope, qualifier, roles)) |
    group(keyword("DENY") ~~ QualifiedGraphAction ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, qualifier, action, roles) => ast.DenyPrivilege.graphAction(action, None, graphScope, qualifier, roles)) |
    group(keyword("DENY") ~~ GraphActionWithResource ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((resource, graphScope, action, roles) => ast.DenyPrivilege.graphAction(action, Some(resource), graphScope, List(ast.LabelAllQualifier()(InputPosition.NONE)), roles)) |
    group(keyword("DENY") ~~ QualifiedGraphActionWithResource ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((resource, graphScope, qualifier, action, roles) => ast.DenyPrivilege.graphAction(action, Some(resource), graphScope, qualifier, roles))
  }

  private def RevokeGraphPrivilege: Rule1[ast.RevokePrivilege] = rule("REVOKE graph privileges") {
    val revokeStart = RevokeType
    group(revokeStart ~~ GraphAction ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((revokeType, graphScope, action, qualifier, roles) => ast.RevokePrivilege.graphAction(action, None, graphScope, qualifier, roles, revokeType)) |
    group(revokeStart ~~ QualifiedGraphAction ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((revokeType, graphScope, qualifier, action, roles) => ast.RevokePrivilege.graphAction(action, None, graphScope, qualifier, roles, revokeType)) |
    group(revokeStart ~~ GraphActionWithResource ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((revokeType, resource, graphScope, action, roles) => ast.RevokePrivilege.graphAction(action, Some(resource), graphScope, List(ast.LabelAllQualifier()(InputPosition.NONE)), roles, revokeType)) |
    group(revokeStart ~~ QualifiedGraphActionWithResource ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((revokeType, resource, graphScope, qualifier, action, roles) => ast.RevokePrivilege.graphAction(action, Some(resource), graphScope, qualifier, roles, revokeType))
  }

  // Help methods for grant/deny/revoke

  private def RevokeType: Rule1[ast.RevokeType] = rule("revoke type") {
    keyword("REVOKE GRANT") ~~~> ast.RevokeGrantType() |
    keyword("REVOKE DENY") ~~~> ast.RevokeDenyType() |
    keyword("REVOKE") ~~~> ast.RevokeBothType()
  }

  // Dbms specific

  private def DbmsAction: Rule1[ast.DbmsAction] = rule("dbms action") {
    group(keyword("ALL") ~~ optional(optional(keyword("DBMS")) ~~ keyword("PRIVILEGES")))~~~> (_ => ast.AllDbmsAction) |
    keyword("ALTER USER") ~~~> (_ => ast.AlterUserAction) |
    keyword("ASSIGN PRIVILEGE") ~~~> (_ => ast.AssignPrivilegeAction) |
    keyword("ASSIGN ROLE") ~~~> (_ => ast.AssignRoleAction) |
    keyword("CREATE DATABASE") ~~~> (_ => ast.CreateDatabaseAction) |
    keyword("CREATE ROLE") ~~~> (_ => ast.CreateRoleAction) |
    keyword("CREATE USER") ~~~> (_ => ast.CreateUserAction) |
    keyword("DATABASE MANAGEMENT") ~~~> (_ => ast.AllDatabaseManagementActions) |
    keyword("DROP DATABASE") ~~~> (_ => ast.DropDatabaseAction) |
    keyword("DROP ROLE") ~~~> (_ => ast.DropRoleAction) |
    keyword("DROP USER") ~~~> (_ => ast.DropUserAction) |
    group(keyword("EXECUTE") ~~ AdminKeyword ~~ keyword("PROCEDURES")) ~> (_ => ast.ExecuteAdminProcedureAction) |
    keyword("PRIVILEGE MANAGEMENT") ~~~> (_ => ast.AllPrivilegeActions) |
    keyword("REMOVE PRIVILEGE") ~~~> (_ => ast.RemovePrivilegeAction) |
    keyword("REMOVE ROLE") ~~~> (_ => ast.RemoveRoleAction) |
    keyword("RENAME ROLE") ~~~> (_ => ast.RenameRoleAction) |
    keyword("RENAME USER") ~~~> (_ => ast.RenameUserAction) |
    keyword("ROLE MANAGEMENT") ~~~> (_ => ast.AllRoleActions) |
    keyword("SET") ~~ PasswordKeyword ~~~> (_ => ast.SetPasswordsAction) |
    keyword("SET USER STATUS") ~~~> (_ => ast.SetUserStatusAction) |
    keyword("SET USER HOME DATABASE") ~~~> (_ => ast.SetUserHomeDatabaseAction) |
    keyword("SHOW PRIVILEGE") ~~~> (_ => ast.ShowPrivilegeAction) |
    keyword("SHOW ROLE") ~~~> (_ => ast.ShowRoleAction) |
    keyword("SHOW USER") ~~~> (_ => ast.ShowUserAction) |
    keyword("USER MANAGEMENT") ~~~> (_ => ast.AllUserActions)
  }

  private def QualifiedDbmsAction: Rule2[List[ast.PrivilegeQualifier], ast.DbmsAction] = rule("qualified dbms action") {
    keyword("EXECUTE") ~~ Procedure ~> (_ => ast.ExecuteProcedureAction) |
    keyword("EXECUTE BOOSTED") ~~ Procedure ~> (_ => ast.ExecuteBoostedProcedureAction) |
    keyword("EXECUTE") ~~ Function ~> (_ => ast.ExecuteFunctionAction) |
    keyword("EXECUTE BOOSTED") ~~ Function ~> (_ => ast.ExecuteBoostedFunctionAction) |
    keyword("IMPERSONATE") ~~ UserQualifier ~> (_ => ast.ImpersonateUserAction) |
    keyword("IMPERSONATE") ~> (_ => List(ast.UserAllQualifier()(InputPosition.NONE))) ~> (_ => ast.ImpersonateUserAction)
  }

  private def Procedure: Rule1[List[ast.PrivilegeQualifier]] = ProcedureKeyword ~~ ProcedureIdentifier

  private def ProcedureIdentifier: Rule1[List[ast.PrivilegeQualifier]] = rule("procedure identifier") {
    oneOrMore(Glob | GlobWithoutNamespace, separator = CommaSep) ~~>>
      { procedures => pos => procedures.map(p => ast.ProcedureQualifier(p)(pos)) }
  }

  private def Function: Rule1[List[ast.PrivilegeQualifier]] =
    group(optional(keyword("USER") ~~ optional(keyword("DEFINED"))) ~~ FunctionKeyword) ~~ FunctionIdentifier

  private def FunctionIdentifier: Rule1[List[ast.PrivilegeQualifier]] = rule("function identifier") {
    oneOrMore(Glob | GlobWithoutNamespace, separator = CommaSep) ~~>>
      { functions => pos => functions.map(p => ast.FunctionQualifier(p)(pos)) }
  }

  // Database specific

  private def Database: Rule1[List[ast.DatabaseScope]] = rule("on a database") {
    keyword("ON DEFAULT DATABASE") ~~~> (pos => List(ast.DefaultDatabaseScope()(pos))) |
    keyword("ON HOME DATABASE") ~~~> (pos => List(ast.HomeDatabaseScope()(pos))) |
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
    group(keyword("SHOW") ~~ IndexKeyword) ~~~> (_ => ast.ShowIndexAction) |
    group(IndexKeyword ~~ optional(keyword("MANAGEMENT"))) ~~~> (_ => ast.AllIndexActions) |
    group(keyword("CREATE") ~~ ConstraintKeyword) ~~~> (_ => ast.CreateConstraintAction) |
    group(keyword("DROP") ~~ ConstraintKeyword) ~~~> (_ => ast.DropConstraintAction) |
    group(keyword("SHOW") ~~ ConstraintKeyword) ~~~> (_ => ast.ShowConstraintAction) |
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
    keyword("ON HOME GRAPH") ~~~> (pos => List(ast.HomeGraphScope()(pos))) |
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

  private def ShowDatabase: Rule1[ast.ShowDatabase] = rule("SHOW DATABASE") {
    group(keyword("SHOW") ~~ ScopeForShowDatabase) ~~ optional(ShowCommandClauses) ~~>> (ast.ShowDatabase(_,_))
  }

  private def ScopeForShowDatabase: Rule1[ast.DatabaseScope] = rule("show database scope") {
    group(keyword("DATABASE") ~~ SymbolicDatabaseNameOrStringParameter) ~~>> (ast.NamedDatabaseScope(_)) |
    keyword("DATABASES") ~~~> ast.AllDatabasesScope() |
    keyword("DEFAULT DATABASE") ~~~> ast.DefaultDatabaseScope() |
    keyword("HOME DATABASE") ~~~> ast.HomeDatabaseScope()
  }

  private def CreateDatabase: Rule1[ast.CreateDatabase] = rule("CREATE DATABASE") {
    group(keyword("CREATE OR REPLACE DATABASE") ~~ SymbolicDatabaseNameOrStringParameter ~~ keyword("IF NOT EXISTS") ~~ optional(optionsMapOrParameter)
      ~~ WaitUntilComplete) ~~>> ((dbName, options, wait) => pos => ast.CreateDatabase(dbName, ast.IfExistsInvalidSyntax, options.getOrElse(NoOptions), wait)(pos)) |
    group(keyword("CREATE OR REPLACE DATABASE") ~~ SymbolicDatabaseNameOrStringParameter ~~ optional(optionsMapOrParameter) ~~ WaitUntilComplete) ~~>>
      ((dbName, options, wait) => pos => ast.CreateDatabase(dbName, ast.IfExistsReplace, options.getOrElse(NoOptions), wait)(pos)) |
    group(keyword("CREATE DATABASE") ~~ SymbolicDatabaseNameOrStringParameter ~~ keyword("IF NOT EXISTS") ~~ optional(optionsMapOrParameter) ~~ WaitUntilComplete) ~~>>
      ((dbName, options, wait) => pos => ast.CreateDatabase(dbName, ast.IfExistsDoNothing, options.getOrElse(NoOptions), wait)(pos)) |
    group(keyword("CREATE DATABASE") ~~ SymbolicDatabaseNameOrStringParameter ~~ optional(optionsMapOrParameter) ~~ WaitUntilComplete) ~~>>
      ((dbName, options, wait) => pos => ast.CreateDatabase(dbName, ast.IfExistsThrowError, options.getOrElse(NoOptions), wait)(pos))
  }

  private def DropDatabase: Rule1[ast.DropDatabase] = rule("DROP DATABASE") {
    val dropDatabaseStart = keyword("DROP DATABASE") ~~ SymbolicDatabaseNameOrStringParameter
    group(dropDatabaseStart ~~ keyword("IF EXISTS") ~~ DataAction ~~ WaitUntilComplete) ~~>>
      ((dbName, dataAction, wait) => ast.DropDatabase(dbName, ifExists = true, dataAction, wait)) |
    group(dropDatabaseStart ~~ DataAction ~~ WaitUntilComplete) ~~>>
      ((dbName, dataAction, wait) => ast.DropDatabase(dbName, ifExists = false, dataAction, wait))
  }

  private def DataAction: Rule1[ast.DropDatabaseAdditionalAction] = rule("data action on drop database") {
    keyword("DUMP DATA") ~~~> (_ => ast.DumpData) |
    optional(keyword("DESTROY DATA")) ~~~> (_ => ast.DestroyData)
  }

  private def StartDatabase: Rule1[ast.StartDatabase] = rule("START DATABASE") {
    group(keyword("START DATABASE") ~~ SymbolicDatabaseNameOrStringParameter ~~ WaitUntilComplete) ~~>> (ast.StartDatabase(_, _))
  }

  private def StopDatabase: Rule1[ast.StopDatabase] = rule("STOP DATABASE") {
    group(keyword("STOP DATABASE") ~~ SymbolicDatabaseNameOrStringParameter ~~ WaitUntilComplete) ~~>> (ast.StopDatabase(_, _))
  }

  // Shared help methods

  private def SymbolicNameOrStringParameter: Rule1[Either[String, Parameter]] =
    group(SymbolicNameString) ~~>> (s => _ => Left(s)) |
    group(StringParameter) ~~>> (p => _ => Right(p))

  private def SymbolicDatabaseNameOrStringParameter: Rule1[Either[String, Parameter]] =
    group(SymbolicDatabaseNameString) ~~>> (s => _ => Left(s)) |
    group(StringParameter) ~~>> (p => _ => Right(p))

  private def SymbolicNameOrStringParameterList: Rule1[List[Either[String, Parameter]]] = rule("a list of symbolic names or string parameters") {
    //noinspection LanguageFeature
    (oneOrMore(WS ~~ SymbolicNameOrStringParameter ~~ WS, separator = ",") memoMismatches).suppressSubnodes
  }

  private def SymbolicDatabaseNameOrStringParameterList: Rule1[List[Either[String, Parameter]]] = rule("a list of symbolic database names or string parameters") {
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

  private def AdminKeyword: Rule0 = keyword("ADMINISTRATOR") | keyword("ADMIN")

  private def CommandKeyword: Rule0 = keyword("COMMANDS") | keyword("COMMAND")

  // Database specific

  private def DatabaseKeyword: Rule0 = keyword("DATABASES") | keyword("DATABASE")

  private def LabelKeyword: Rule0 = keyword("LABELS") | keyword("LABEL")

  private def TypeKeyword: Rule0 = keyword("TYPES") | keyword("TYPE")

  private def NameKeyword: Rule0 = keyword("NAMES") | keyword("NAME")

  // Graph specific

  private def GraphKeyword: Rule0 = keyword("GRAPHS") | keyword("GRAPH")

  private def ElementKeyword: Rule0 = keyword("ELEMENTS") | keyword("ELEMENT")

  private def RelationshipKeyword: Rule0 = keyword("RELATIONSHIPS") | keyword("RELATIONSHIP")

  private def NodeKeyword: Rule0 = keyword("NODES") | keyword("NODE")

}
