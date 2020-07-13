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
import org.neo4j.cypher.internal.ast.ActionResource
import org.neo4j.cypher.internal.ast.AdminAction
import org.neo4j.cypher.internal.ast.AlterUser
import org.neo4j.cypher.internal.ast.CreateDatabase
import org.neo4j.cypher.internal.ast.CreateGraph
import org.neo4j.cypher.internal.ast.CreateRole
import org.neo4j.cypher.internal.ast.CreateUser
import org.neo4j.cypher.internal.ast.CreateView
import org.neo4j.cypher.internal.ast.DatabaseAction
import org.neo4j.cypher.internal.ast.DatabasePrivilegeQualifier
import org.neo4j.cypher.internal.ast.DatabaseScope
import org.neo4j.cypher.internal.ast.DenyPrivilege
import org.neo4j.cypher.internal.ast.DestroyData
import org.neo4j.cypher.internal.ast.DropDatabase
import org.neo4j.cypher.internal.ast.DropGraph
import org.neo4j.cypher.internal.ast.DropRole
import org.neo4j.cypher.internal.ast.DropUser
import org.neo4j.cypher.internal.ast.DropView
import org.neo4j.cypher.internal.ast.DumpData
import org.neo4j.cypher.internal.ast.GrantPrivilege
import org.neo4j.cypher.internal.ast.GrantRolesToUsers
import org.neo4j.cypher.internal.ast.GraphAction
import org.neo4j.cypher.internal.ast.GraphPrivilegeQualifier
import org.neo4j.cypher.internal.ast.GraphScope
import org.neo4j.cypher.internal.ast.IfExistsDo
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsInvalidSyntax
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.RevokeBothType
import org.neo4j.cypher.internal.ast.RevokeDenyType
import org.neo4j.cypher.internal.ast.RevokeGrantType
import org.neo4j.cypher.internal.ast.RevokePrivilege
import org.neo4j.cypher.internal.ast.RevokeRolesFromUsers
import org.neo4j.cypher.internal.ast.SetOwnPassword
import org.neo4j.cypher.internal.ast.ShowDatabase
import org.neo4j.cypher.internal.ast.ShowPrivilegeScope
import org.neo4j.cypher.internal.ast.ShowPrivileges
import org.neo4j.cypher.internal.ast.ShowRoles
import org.neo4j.cypher.internal.ast.ShowUsers
import org.neo4j.cypher.internal.ast.StartDatabase
import org.neo4j.cypher.internal.ast.StopDatabase
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.util.InputPosition
import org.parboiled.scala.Parser
import org.parboiled.scala.Rule0
import org.parboiled.scala.Rule1
import org.parboiled.scala.Rule2
import org.parboiled.scala.Rule3
import org.parboiled.scala.Rule4
import org.parboiled.scala.group

trait Statement extends Parser
  with GraphSelection
  with Query
  with SchemaCommand
  with Base {

  def Statement: Rule1[ast.Statement] = rule(
    AdministrationCommand | MultiGraphCommand | SchemaCommand | Query
  )

  def MultiGraphCommand: Rule1[ast.MultiGraphDDL] = rule("Multi graph DDL statement") {
    CreateGraph | DropGraph | CreateView | DropView
  }

  def AdministrationCommand: Rule1[ast.AdministrationCommand] = rule("Administration statement")(
    optional(UseGraph) ~~ (MultiDatabaseAdministrationCommand | PrivilegeAdministrationCommand | UserAndRoleAdministrationCommand) ~~> ((use, command) => command.withGraph(use))
  )

  def MultiDatabaseAdministrationCommand: Rule1[ast.AdministrationCommand] = rule("MultiDatabase administration statement") {
    optional(keyword("CATALOG")) ~~ (ShowDatabase | CreateDatabase | DropDatabase | StartDatabase | StopDatabase)
  }

  def UserAndRoleAdministrationCommand: Rule1[ast.AdministrationCommand] = rule("Security role and user administration statement") {
    optional(keyword("CATALOG")) ~~ (ShowRoles | CreateRole | DropRole | ShowUsers | CreateUser | DropUser | AlterUser | SetOwnPassword)
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
    RevokeRole | RevokeGraphPrivilege | RevokeGrantDatabasePrivilege | RevokeDenyDatabasePrivilege | RevokeDatabasePrivilege |
    RevokeGrantDbmsPrivilege | RevokeDenyDbmsPrivilege | RevokeDbmsPrivilege
  }

  def ShowUsers: Rule1[ShowUsers] = rule("CATALOG SHOW USERS") {
    keyword("SHOW USERS") ~~ ShowCommandClauses ~~>> (ast.ShowUsers(_,_,_)_)
  }

  def CreateUser: Rule1[CreateUser] = rule("CATALOG CREATE USER") {
    // CREATE [OR REPLACE] USER username [IF NOT EXISTS] SET PASSWORD stringLiteralPassword optionalStatus
    group(createUserStart ~~ keyword("SET PASSWORD") ~~ SensitiveStringLiteral ~~ optionalStatus) ~~>> ((userName, ifExistsDo, initialPassword, suspended) =>
      ast.CreateUser(userName, initialPassword, requirePasswordChange = true, suspended, ifExistsDo)) |
    // CREATE [OR REPLACE] USER username [IF NOT EXISTS] SET PASSWORD stringLiteralPassword optionalRequirePasswordChange optionalStatus
    group(createUserStart ~~ keyword("SET PASSWORD") ~~ SensitiveStringLiteral ~~
    optionalRequirePasswordChange ~~ optionalStatus) ~~>> ((userName, ifExistsDo, initialPassword, requirePasswordChange, suspended) =>
      ast.CreateUser(userName, initialPassword, requirePasswordChange.getOrElse(true), suspended, ifExistsDo)) |
    //
    // CREATE [OR REPLACE] USER username [IF NOT EXISTS] SET PASSWORD parameterPassword optionalStatus
    group(createUserStart ~~ keyword("SET PASSWORD") ~~ SensitiveStringParameter ~~
    optionalStatus) ~~>> ((userName, ifExistsDo, initialPassword, suspended) =>
      ast.CreateUser(userName, initialPassword, requirePasswordChange = true, suspended, ifExistsDo)) |
    // CREATE [OR REPLACE] USER username [IF NOT EXISTS] SET PASSWORD parameterPassword optionalRequirePasswordChange optionalStatus
    group(createUserStart ~~ keyword("SET PASSWORD") ~~ SensitiveStringParameter ~~
    optionalRequirePasswordChange ~~ optionalStatus) ~~>> ((userName, ifExistsDo, initialPassword, requirePasswordChange, suspended) =>
      ast.CreateUser(userName, initialPassword, requirePasswordChange.getOrElse(true), suspended, ifExistsDo))
  }

  def createUserStart: Rule2[Either[String, Parameter], IfExistsDo] = {
    // returns: userName, IfExistsDo
    group(keyword("CREATE OR REPLACE USER") ~~ SymbolicNameOrStringParameter ~~ keyword("IF NOT EXISTS") ~> (_ => IfExistsInvalidSyntax)) |
    group(keyword("CREATE OR REPLACE USER") ~~ SymbolicNameOrStringParameter ~> (_ => IfExistsReplace)) |
    group(keyword("CREATE USER") ~~ SymbolicNameOrStringParameter ~~ keyword("IF NOT EXISTS") ~> (_ => IfExistsDoNothing)) |
    group(keyword("CREATE USER") ~~ SymbolicNameOrStringParameter ~> (_ => IfExistsThrowError))
  }

  def DropUser: Rule1[DropUser] = rule("CATALOG DROP USER") {
    group(keyword("DROP USER") ~~ SymbolicNameOrStringParameter ~~ keyword("IF EXISTS")) ~~>> (ast.DropUser(_, ifExists = true)) |
    group(keyword("DROP USER") ~~ SymbolicNameOrStringParameter) ~~>> (ast.DropUser(_, ifExists = false))
  }

  def AlterUser: Rule1[AlterUser] = rule("CATALOG ALTER USER") {
    // ALTER USER username SET PASSWORD stringLiteralPassword optionalStatus
    group(keyword("ALTER USER") ~~ SymbolicNameOrStringParameter ~~ keyword("SET PASSWORD") ~~ SensitiveStringLiteral ~~
    optionalStatus) ~~>> ((userName, initialPassword, suspended) =>
      ast.AlterUser(userName, Some(initialPassword), None, suspended)) |
    // ALTER USER username SET PASSWORD stringLiteralPassword optionalRequirePasswordChange optionalStatus
    group(keyword("ALTER USER") ~~ SymbolicNameOrStringParameter ~~ keyword("SET PASSWORD") ~~ SensitiveStringLiteral ~~
    optionalRequirePasswordChange ~~ optionalStatus) ~~>> ((userName, initialPassword, requirePasswordChange, suspended) =>
      ast.AlterUser(userName, Some(initialPassword), requirePasswordChange, suspended)) |
    //
    // ALTER USER username SET PASSWORD parameterPassword optionalStatus
    group(keyword("ALTER USER") ~~ SymbolicNameOrStringParameter ~~ keyword("SET PASSWORD") ~~ SensitiveStringParameter ~~
    optionalStatus) ~~>> ((userName, initialPassword, suspended) =>
      ast.AlterUser(userName, Some(initialPassword), None, suspended)) |
    // ALTER USER username SET PASSWORD parameterPassword optionalRequirePasswordChange optionalStatus
    group(keyword("ALTER USER") ~~ SymbolicNameOrStringParameter ~~ keyword("SET PASSWORD") ~~ SensitiveStringParameter ~~
    optionalRequirePasswordChange ~~ optionalStatus) ~~>> ((userName, initialPassword, requirePasswordChange, suspended) =>
      ast.AlterUser(userName, Some(initialPassword), requirePasswordChange, suspended)) |
    //
    // ALTER USER username setRequirePasswordChange optionalStatus
    group(keyword("ALTER USER") ~~ SymbolicNameOrStringParameter ~~ setRequirePasswordChange ~~ optionalStatus) ~~>>
      ((userName, requirePasswordChange, suspended) => ast.AlterUser(userName, None, Some(requirePasswordChange), suspended)) |
    //
    // ALTER USER username setStatus
    group(keyword("ALTER USER") ~~ SymbolicNameOrStringParameter ~~ setStatus) ~~>>
      ((userName, suspended) => ast.AlterUser(userName, None, None, Some(suspended)))
  }

  def SetOwnPassword: Rule1[SetOwnPassword] = rule("CATALOG ALTER CURRENT USER SET PASSWORD") {
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

  def optionalRequirePasswordChange: Rule1[Option[Boolean]] = {
    group(optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE NOT REQUIRED")) ~>>> (_ => _ => Some(false)) |
    group(optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE REQUIRED")) ~>>> (_ => _ => Some(true)) |
    keyword("") ~>>> (_ => _ => None) // no password mode change
  }

  def optionalStatus: Rule1[Option[Boolean]] = {
    keyword("SET STATUS SUSPENDED") ~>>> (_ => _ => Some(true)) |
    keyword("SET STATUS ACTIVE") ~>>> (_ => _ => Some(false)) |
    keyword("") ~>>> (_ => _ => None) // no status change
  }

  def setStatus: Rule1[Boolean] = {
    keyword("SET STATUS SUSPENDED") ~>>> (_ => _ => true) |
    keyword("SET STATUS ACTIVE") ~>>> (_ => _ => false)
  }

  def setRequirePasswordChange: Rule1[Boolean] = {
    keyword("SET PASSWORD CHANGE NOT REQUIRED") ~>>> (_ => _ => false) |
    keyword("SET PASSWORD CHANGE REQUIRED") ~>>> (_ => _ => true)
  }

  def ShowRoles: Rule1[ShowRoles] = rule("CATALOG SHOW ROLES") {
    //SHOW [ ALL | POPULATED ] ROLES WITH USERS
    group(keyword("SHOW") ~~ keyword("POPULATED") ~~ keyword("ROLES") ~~
      keyword("WITH USERS")) ~~ ShowCommandClauses ~~>> (ast.ShowRoles(withUsers = true, showAll = false, _, _, _)) |
    group(keyword("SHOW") ~~ optional(keyword("ALL")) ~~ keyword("ROLES") ~~
      keyword("WITH USERS")) ~~ ShowCommandClauses ~~>> (ast.ShowRoles(withUsers = true, showAll = true, _, _, _)) |
    // SHOW [ ALL | POPULATED ] ROLES
    group(keyword("SHOW") ~~ keyword("POPULATED") ~~ keyword("ROLES")) ~~ ShowCommandClauses ~~>>
      (ast.ShowRoles(withUsers = false, showAll = false, _, _, _)) |
    group(keyword("SHOW") ~~ optional(keyword("ALL")) ~~ keyword("ROLES")) ~~ ShowCommandClauses ~~>>
      (ast.ShowRoles(withUsers = false, showAll = true, _, _, _))
  }

  def CreateRole: Rule1[CreateRole] = rule("CATALOG CREATE ROLE") {
    group(keyword("CREATE OR REPLACE ROLE") ~~ SymbolicNameOrStringParameter ~~ keyword("IF NOT EXISTS") ~~
      optional(keyword("AS COPY OF") ~~ SymbolicNameOrStringParameter)) ~~>> (ast.CreateRole(_, _, IfExistsInvalidSyntax)) |
    group(keyword("CREATE OR REPLACE ROLE") ~~ SymbolicNameOrStringParameter ~~
      optional(keyword("AS COPY OF") ~~ SymbolicNameOrStringParameter)) ~~>> (ast.CreateRole(_, _, IfExistsReplace)) |
    group(keyword("CREATE ROLE") ~~ SymbolicNameOrStringParameter ~~ keyword("IF NOT EXISTS") ~~
      optional(keyword("AS COPY OF") ~~ SymbolicNameOrStringParameter)) ~~>> (ast.CreateRole(_, _, IfExistsDoNothing)) |
    group(keyword("CREATE ROLE") ~~ SymbolicNameOrStringParameter ~~
      optional(keyword("AS COPY OF") ~~ SymbolicNameOrStringParameter)) ~~>> (ast.CreateRole(_, _, IfExistsThrowError))
  }

  def DropRole: Rule1[DropRole] = rule("CATALOG DROP ROLE") {
    group(keyword("DROP ROLE") ~~ SymbolicNameOrStringParameter ~~ keyword("IF EXISTS")) ~~>> (ast.DropRole(_, ifExists = true)) |
    group(keyword("DROP ROLE") ~~ SymbolicNameOrStringParameter) ~~>> (ast.DropRole(_, ifExists = false))
  }

  def GrantRole: Rule1[GrantRolesToUsers] = rule("CATALOG GRANT ROLE") {
    group(keyword("GRANT") ~~ RoleKeyword ~~ SymbolicNameOrStringParameterList ~~
      keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>> (ast.GrantRolesToUsers(_, _))
  }

  def RevokeRole: Rule1[RevokeRolesFromUsers] = rule("CATALOG REVOKE ROLE") {
    group(keyword("REVOKE") ~~ RoleKeyword ~~ SymbolicNameOrStringParameterList ~~
      keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>> (ast.RevokeRolesFromUsers(_, _))
  }

  //` ... ON DBMS TO role`
  def GrantDbmsPrivilege: Rule1[GrantPrivilege] = rule("CATALOG GRANT dbms privileges") {
    group(keyword("GRANT") ~~ DbmsAction ~~ keyword("ON DBMS TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((dbmsAction, grantees) => ast.GrantPrivilege.dbmsAction( dbmsAction, grantees ))
  }

  def DenyDbmsPrivilege: Rule1[DenyPrivilege] = rule("CATALOG DENY dbms privileges") {
    group(keyword("DENY") ~~ DbmsAction ~~ keyword("ON DBMS TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((dbmsAction, grantees) => ast.DenyPrivilege.dbmsAction( dbmsAction, grantees ))
  }

  def RevokeDbmsPrivilege: Rule1[RevokePrivilege] = rule("CATALOG REVOKE dbms privileges") {
    group(keyword("REVOKE") ~~ DbmsAction ~~ keyword("ON DBMS FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((dbmsAction, grantees) => pos => ast.RevokePrivilege.dbmsAction(dbmsAction, grantees, RevokeBothType()(pos))(pos))
  }

  def RevokeGrantDbmsPrivilege: Rule1[RevokePrivilege] = rule("CATALOG REVOKE GRANT dbms privileges") {
    group(keyword("REVOKE GRANT") ~~ DbmsAction ~~ keyword("ON DBMS FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((dbmsAction, grantees) => pos => ast.RevokePrivilege.dbmsAction(dbmsAction, grantees, RevokeGrantType()(pos))(pos))
  }

  def RevokeDenyDbmsPrivilege: Rule1[RevokePrivilege] = rule("CATALOG REVOKE DENY dbms privileges") {
    group(keyword("REVOKE DENY") ~~ DbmsAction ~~ keyword("ON DBMS FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((dbmsAction, grantees) => pos => ast.RevokePrivilege.dbmsAction(dbmsAction, grantees, RevokeDenyType()(pos))(pos))
  }

  //` ... ON DATABASE foo TO role`
  def GrantDatabasePrivilege: Rule1[GrantPrivilege] = rule("CATALOG GRANT database privileges") {
    group(keyword("GRANT") ~~ QualifiedDatabaseAction ~~ Database ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((qualifier, databaseAction, scope, grantees) => ast.GrantPrivilege.databaseAction( databaseAction, scope, grantees, qualifier)) |
    group(keyword("GRANT") ~~ DatabaseAction ~~ Database ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((databaseAction, scope, grantees) => ast.GrantPrivilege.databaseAction( databaseAction, scope, grantees))
  }

  def DenyDatabasePrivilege: Rule1[DenyPrivilege] = rule("CATALOG DENY database privileges") {
    group(keyword("DENY") ~~ QualifiedDatabaseAction ~~ Database ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((qualifier, databaseAction, scope, grantees) => ast.DenyPrivilege.databaseAction( databaseAction, scope, grantees, qualifier)) |
    group(keyword("DENY") ~~ DatabaseAction ~~ Database ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((databaseAction, scope, grantees) => ast.DenyPrivilege.databaseAction( databaseAction, scope, grantees))
  }

  def RevokeGrantDatabasePrivilege: Rule1[RevokePrivilege] = rule("CATALOG REVOKE GRANT database privileges") {
    group(keyword("REVOKE GRANT") ~~ QualifiedDatabaseAction ~~ Database ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((qualifier, databaseAction, scope, grantees) => pos => ast.RevokePrivilege.databaseAction( databaseAction, scope, grantees, RevokeGrantType()(pos), qualifier)(pos)) |
    group(keyword("REVOKE GRANT") ~~ DatabaseAction ~~ Database ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((databaseAction, scope, grantees) => pos => ast.RevokePrivilege.databaseAction( databaseAction, scope, grantees, RevokeGrantType()(pos))(pos))
  }

  def RevokeDenyDatabasePrivilege: Rule1[RevokePrivilege] = rule("CATALOG REVOKE DENY database privileges") {
    group(keyword("REVOKE DENY") ~~ QualifiedDatabaseAction ~~ Database ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((qualifier, databaseAction, scope, grantees) => pos => ast.RevokePrivilege.databaseAction( databaseAction, scope, grantees, RevokeDenyType()(pos), qualifier)(pos)) |
    group(keyword("REVOKE DENY") ~~ DatabaseAction ~~ Database ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((databaseAction, scope, grantees) => pos => ast.RevokePrivilege.databaseAction( databaseAction, scope, grantees, RevokeDenyType()(pos))(pos))
  }

  def RevokeDatabasePrivilege: Rule1[RevokePrivilege] = rule("CATALOG REVOKE database privileges") {
    group(keyword("REVOKE") ~~ QualifiedDatabaseAction ~~ Database ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((qualifier, databaseAction, scope, grantees) => pos => ast.RevokePrivilege.databaseAction( databaseAction, scope, grantees, RevokeBothType()(pos), qualifier)(pos)) |
    group(keyword("REVOKE") ~~ DatabaseAction ~~ Database ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((databaseAction, scope, grantees) => pos => ast.RevokePrivilege.databaseAction( databaseAction, scope, grantees, RevokeBothType()(pos))(pos))
  }

   //`GRANT ON GRAPH foo TO role`
  def GrantGraphPrivilege: Rule1[GrantPrivilege] = rule("CATALOG GRANT CREATE") {
    group(keyword("GRANT") ~~ GraphAction ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, action, qualifier, roles) => ast.GrantPrivilege.graphAction(action, None, graphScope, qualifier, roles)) |
    group(keyword("GRANT") ~~ QualifiedGraphAction ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, qualifier, action, roles) => ast.GrantPrivilege.graphAction(action, None, graphScope, qualifier, roles)) |
    group(keyword("GRANT") ~~ QualifiedWithPropertyGraphAction ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, qualifier, action, roles) => ast.GrantPrivilege.graphAction(action, None, graphScope, qualifier, roles)) |
    group(keyword("GRANT") ~~ GraphActionWithResource ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((resource, graphScope, action, roles) => ast.GrantPrivilege.graphAction(action, Some(resource), graphScope, List(ast.LabelAllQualifier()(InputPosition.NONE)), roles)) |
    group(keyword("GRANT") ~~ QualifiedGraphActionWithResource ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((resource, graphScope, qualifier, action, roles) => ast.GrantPrivilege.graphAction(action, Some(resource), graphScope, qualifier, roles)) |
    group(keyword("GRANT") ~~ QualifiedWithPropertyGraphActionWithResource ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((resource, graphScope, qualifier, action, roles) => ast.GrantPrivilege.graphAction(action, Some(resource), graphScope, qualifier, roles))
  }

  //`DENY ON GRAPH foo TO role`
  def DenyGraphPrivilege: Rule1[DenyPrivilege] = rule("CATALOG GRANT DELETE") {
    group(keyword("DENY") ~~ GraphAction ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, action, qualifier, roles) => ast.DenyPrivilege.graphAction(action, None, graphScope, qualifier, roles)) |
    group(keyword("DENY") ~~ QualifiedGraphAction ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, qualifier, action, roles) => ast.DenyPrivilege.graphAction(action, None, graphScope, qualifier, roles)) |
    group(keyword("DENY") ~~ QualifiedWithPropertyGraphAction ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, qualifier, action, roles) => ast.DenyPrivilege.graphAction(action, None, graphScope, qualifier, roles)) |
    group(keyword("DENY") ~~ GraphActionWithResource ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((resource, graphScope, action, roles) => ast.DenyPrivilege.graphAction(action, Some(resource), graphScope, List(ast.LabelAllQualifier()(InputPosition.NONE)), roles)) |
    group(keyword("DENY") ~~ QualifiedGraphActionWithResource ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((resource, graphScope, qualifier, action, roles) => ast.DenyPrivilege.graphAction(action, Some(resource), graphScope, qualifier, roles)) |
    group(keyword("DENY") ~~ QualifiedWithPropertyGraphActionWithResource ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((resource, graphScope, qualifier, action, roles) => ast.DenyPrivilege.graphAction(action, Some(resource), graphScope, qualifier, roles))
  }

  //noinspection DuplicatedCode
  //`REVOKE ON GRAPH foo TO role`
  def RevokeGraphPrivilege: Rule1[RevokePrivilege] = rule("CATALOG REVOKE GRANT CREATE") {
    group(keyword("REVOKE GRANT") ~~ GraphAction ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, action, qualifier, roles) => pos => ast.RevokePrivilege.graphAction(action, None, graphScope, qualifier, roles, RevokeGrantType()(pos))(pos)) |
    group(keyword("REVOKE GRANT") ~~ QualifiedGraphAction ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, qualifier, action, roles) => pos => ast.RevokePrivilege.graphAction(action, None, graphScope, qualifier, roles, RevokeGrantType()(pos))(pos)) |
    group(keyword("REVOKE GRANT") ~~ QualifiedWithPropertyGraphAction ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, qualifier, action, roles) => pos => ast.RevokePrivilege.graphAction(action, None, graphScope, qualifier, roles, RevokeGrantType()(pos))(pos)) |
    group(keyword("REVOKE GRANT") ~~ GraphActionWithResource ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((resource, graphScope, action, roles) =>  pos => ast.RevokePrivilege.graphAction(action, Some(resource), graphScope, List(ast.LabelAllQualifier()(InputPosition.NONE)), roles, RevokeGrantType()(pos))(pos)) |
    group(keyword("REVOKE GRANT") ~~ QualifiedGraphActionWithResource ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((resource, graphScope, qualifier, action, roles) => pos => ast.RevokePrivilege.graphAction(action, Some(resource), graphScope, qualifier, roles, RevokeGrantType()(pos))(pos)) |
    group(keyword("REVOKE GRANT") ~~ QualifiedWithPropertyGraphActionWithResource ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((resource, graphScope, qualifier, action, roles) => pos => ast.RevokePrivilege.graphAction(action, Some(resource), graphScope, qualifier, roles, RevokeGrantType()(pos))(pos)) |
    group(keyword("REVOKE DENY") ~~ GraphAction ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, action, qualifier, roles) => pos => ast.RevokePrivilege.graphAction(action, None, graphScope, qualifier, roles, RevokeDenyType()(pos))(pos)) |
    group(keyword("REVOKE DENY") ~~ QualifiedGraphAction ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, qualifier, action, roles) => pos => ast.RevokePrivilege.graphAction(action, None, graphScope, qualifier, roles, RevokeDenyType()(pos))(pos)) |
    group(keyword("REVOKE DENY") ~~ QualifiedWithPropertyGraphAction ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, qualifier, action, roles) => pos => ast.RevokePrivilege.graphAction(action, None, graphScope, qualifier, roles, RevokeDenyType()(pos))(pos)) |
    group(keyword("REVOKE DENY") ~~ GraphActionWithResource ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((resource, graphScope, action, roles) => pos => ast.RevokePrivilege.graphAction(action, Some(resource), graphScope, List(ast.LabelAllQualifier()(InputPosition.NONE)), roles, RevokeDenyType()(pos))(pos)) |
    group(keyword("REVOKE DENY") ~~ QualifiedGraphActionWithResource ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((resource, graphScope, qualifier, action, roles) => pos => ast.RevokePrivilege.graphAction(action, Some(resource), graphScope, qualifier, roles, RevokeDenyType()(pos))(pos)) |
    group(keyword("REVOKE DENY") ~~ QualifiedWithPropertyGraphActionWithResource ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((resource, graphScope, qualifier, action, roles) => pos => ast.RevokePrivilege.graphAction(action, Some(resource), graphScope, qualifier, roles, RevokeDenyType()(pos))(pos)) |
    group(keyword("REVOKE") ~~ GraphAction ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, action, qualifier, roles) => pos => ast.RevokePrivilege.graphAction(action, None, graphScope, qualifier, roles, RevokeBothType()(pos))(pos)) |
    group(keyword("REVOKE") ~~ QualifiedGraphAction ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, qualifier, action, roles) => pos => ast.RevokePrivilege.graphAction(action, None, graphScope, qualifier, roles, RevokeBothType()(pos))(pos)) |
    group(keyword("REVOKE") ~~ QualifiedWithPropertyGraphAction ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, qualifier, action, roles) => pos => ast.RevokePrivilege.graphAction(action, None, graphScope, qualifier, roles, RevokeBothType()(pos))(pos)) |
    group(keyword("REVOKE") ~~ GraphActionWithResource ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((resource, graphScope, action, roles) => pos => ast.RevokePrivilege.graphAction(action, Some(resource), graphScope, List(ast.LabelAllQualifier()(InputPosition.NONE)), roles, RevokeBothType()(pos))(pos)) |
    group(keyword("REVOKE") ~~ QualifiedGraphActionWithResource ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((resource, graphScope, qualifier, action, roles) => pos => ast.RevokePrivilege.graphAction(action, Some(resource), graphScope, qualifier, roles, RevokeBothType()(pos))(pos)) |
    group(keyword("REVOKE") ~~ QualifiedWithPropertyGraphActionWithResource ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((resource, graphScope, qualifier, action, roles) => pos => ast.RevokePrivilege.graphAction(action, Some(resource), graphScope, qualifier, roles, RevokeBothType()(pos))(pos))
  }

  def ShowPrivileges: Rule1[ShowPrivileges] = rule("CATALOG SHOW PRIVILEGES") {
    group(keyword("SHOW") ~~ ScopeForShowPrivileges ~~ ShowCommandClauses
      ~~>> ((scope, yld, where, rtn) => ast.ShowPrivileges(scope, yld, where, rtn)))
  }

  private def ShowCommandClauses: Rule3[Option[Return], Option[Where], Option[Return]] = rule("YIELD ... WHERE .. RETURN .. for SHOW commands") {
    optional(group(keyword("YIELD") ~~ YieldBody) ~~>> (ast.Return(distinct = false, _, _, _, _))) ~~
      optional(Where) ~> (_ => None)
  }

  private def PrivilegeProperty: Rule1[ActionResource] = rule("{propertyList}")(
    group("{" ~~ SymbolicNamesList ~~ "}") ~~>> {ast.PropertiesResource(_)} |
      group("{" ~~ "*" ~~ "}") ~~~> {ast.AllPropertyResource()}
  )

  private def LabelResource: Rule1[ActionResource] = rule("label used for set/remove label") {
    group(SymbolicNamesList  ~~>> {ast.LabelsResource(_)} |
      group(keyword("*") ~~~> {ast.AllLabelResource()}))
  }

  private def UserQualifier: Rule1[List[DatabasePrivilegeQualifier]] = rule("(usernameList)")(
    group("(" ~~ SymbolicNameOrStringParameterList ~~ ")") ~~>> {userName => pos => userName.map(ast.UserQualifier(_)(pos))} |
    group("(" ~~ "*" ~~ ")") ~~~> { pos => List(ast.UserAllQualifier()(pos))}
  )

  private def ScopeQualifierWithProperty: Rule1[List[GraphPrivilegeQualifier]] = rule("which element type and associated labels/relTypes (props) qualifier combination")(
    ScopeQualifier ~~ optional("(" ~~ "*" ~~ ")")
  )

  private def ScopeQualifier: Rule1[List[GraphPrivilegeQualifier]] = rule("which element type and associated labels/relTypes qualifier combination")(
    group(RelationshipKeyword ~~ SymbolicNamesList) ~~>> { relNames => pos => relNames.map(ast.RelationshipQualifier(_)(pos)) } |
    group(RelationshipKeyword ~~ "*") ~~~> { pos => List(ast.RelationshipAllQualifier()(pos)) } |
    group(NodeKeyword ~~ SymbolicNamesList) ~~>> { nodeName => pos => nodeName.map(ast.LabelQualifier(_)(pos)) } |
    group(NodeKeyword ~~ "*") ~~~> { pos => List(ast.LabelAllQualifier()(pos)) } |
    group(ElementKeyword ~~ SymbolicNamesList) ~~>> { elemName => pos => elemName.map(ast.ElementQualifier(_)(pos)) } |
    optional(ElementKeyword ~~ "*") ~~~> { pos => List(ast.ElementsAllQualifier()(pos)) }
  )

  private def ElementKeyword: Rule0 = keyword("ELEMENTS") | keyword("ELEMENT")

  private def RelationshipKeyword: Rule0 = keyword("RELATIONSHIPS") | keyword("RELATIONSHIP")

  private def NodeKeyword: Rule0 = keyword("NODE") | keyword("NODES")

  private def Database: Rule1[List[DatabaseScope]] = rule("on a database") {
    keyword("ON DEFAULT DATABASE") ~~~> (pos => List(ast.DefaultDatabaseScope()(pos))) |
    group(keyword("ON") ~~ (keyword("DATABASE") | keyword("DATABASES"))) ~~
      group((SymbolicNameOrStringParameterList ~~>> (params => pos => params.map(ast.NamedDatabaseScope(_)(pos)))) |
      (keyword("*") ~~~> (pos => List(ast.AllDatabasesScope()(pos)))))
  }

  private def DatabaseAction: Rule1[DatabaseAction] = rule("database action")(
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
  )

  private def QualifiedDatabaseAction: Rule2[List[DatabasePrivilegeQualifier], DatabaseAction] = rule("qualified database action")(
    group(keyword("SHOW") ~~ TransactionKeyword ~~ UserQualifier ~> (_ => ast.ShowTransactionAction)) |
    group(keyword("SHOW") ~~ TransactionKeyword ~> (_ => List(ast.UserAllQualifier()(InputPosition.NONE))) ~> (_ => ast.ShowTransactionAction)) |
    group(keyword("TERMINATE") ~~ TransactionKeyword ~~ UserQualifier ~> (_ => ast.TerminateTransactionAction)) |
    group(keyword("TERMINATE") ~~ TransactionKeyword ~> (_ => List(ast.UserAllQualifier()(InputPosition.NONE))) ~> (_ => ast.TerminateTransactionAction)) |
    group(keyword("TRANSACTION") ~~ optional(keyword("MANAGEMENT")) ~~ UserQualifier ~> (_ => ast.AllTransactionActions)) |
    group(keyword("TRANSACTION") ~~ optional(keyword("MANAGEMENT")) ~> (_ => List(ast.UserAllQualifier()(InputPosition.NONE))) ~> (_ => ast.AllTransactionActions))
  )

  private def GraphAction: Rule3[List[GraphScope], GraphAction, List[ast.GraphPrivilegeQualifier]] = rule("graph action")(
    group(keyword("ALL") ~~ optional(optional(keyword("GRAPH")) ~~ keyword("PRIVILEGES"))) ~~ Graph ~> (_ => ast.AllGraphAction) ~> (_ => List(ast.AllQualifier()(InputPosition.NONE))) |
    group(keyword("WRITE") ~~ Graph) ~> (_ => ast.WriteAction) ~> (_ => List(ast.ElementsAllQualifier()(InputPosition.NONE)))
  )

  private def QualifiedGraphAction: Rule3[List[GraphScope], List[GraphPrivilegeQualifier], GraphAction] = rule("qualified graph action")(
    group(keyword("CREATE") ~~ Graph ~~ ScopeQualifier ~> (_ => ast.CreateElementAction)) |
    group(keyword("DELETE") ~~ Graph ~~ ScopeQualifier ~> (_ => ast.DeleteElementAction))
  )

  private def QualifiedWithPropertyGraphAction: Rule3[List[GraphScope], List[GraphPrivilegeQualifier], GraphAction] = rule("qualified with property graph action")(
    group(keyword("TRAVERSE") ~~ Graph ~~ ScopeQualifierWithProperty ~> (_ => ast.TraverseAction))
  )

  private def GraphActionWithResource: Rule3[ActionResource, List[GraphScope], GraphAction] = rule("graph action with resource")(
    group(keyword("SET LABEL") ~~ LabelResource ~~ Graph ~> (_ => ast.SetLabelAction)) |
    group(keyword("REMOVE LABEL") ~~ LabelResource ~~ Graph ~> (_ => ast.RemoveLabelAction))
  )

  private def QualifiedGraphActionWithResource: Rule4[ActionResource, List[GraphScope], List[GraphPrivilegeQualifier], GraphAction] = rule("qualified graph action with resource")(
    group(keyword("MERGE") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifier ~> (_ => ast.MergeAdminAction)) |
    group(keyword("SET PROPERTY") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifier ~> (_ => ast.SetPropertyAction))
  )

  private def QualifiedWithPropertyGraphActionWithResource: Rule4[ActionResource, List[GraphScope], List[GraphPrivilegeQualifier], GraphAction] = rule("qualified with property graph action with resource")(
    group(keyword("READ")  ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifierWithProperty ~> (_ => ast.ReadAction)) |
    group(keyword("MATCH")  ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifierWithProperty ~> (_ => ast.MatchAction))
  )

  private def DbmsAction: Rule1[AdminAction] = rule("dbms action") {
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
    keyword("SET") ~~ passwordKeyword ~~~> (_ => ast.SetPasswordsAction) |
    keyword("ALTER USER") ~~~> (_ => ast.AlterUserAction) |
    keyword("USER MANAGEMENT") ~~~> (_ => ast.AllUserActions) |
    keyword("CREATE DATABASE") ~~~> (_ => ast.CreateDatabaseAction) |
    keyword("DROP DATABASE") ~~~> (_ => ast.DropDatabaseAction) |
    keyword("DATABASE MANAGEMENT") ~~~> (_ => ast.AllDatabaseManagementActions) |
    keyword("SHOW PRIVILEGE") ~~~> (_ => ast.ShowPrivilegeAction) |
    keyword("ASSIGN PRIVILEGE") ~~~> (_ => ast.AssignPrivilegeAction) |
    keyword("REMOVE PRIVILEGE") ~~~> (_ => ast.RemovePrivilegeAction) |
    keyword("PRIVILEGE MANAGEMENT") ~~~> (_ => ast.AllPrivilegeActions) |
    group(keyword("ALL") ~~ optional(optional(keyword("DBMS")) ~~ keyword("PRIVILEGES")))~~~> (_ => ast.AllDbmsAction)
  }

  private def IndexKeyword: Rule0 = keyword("INDEXES") | keyword("INDEX")

  private def ConstraintKeyword: Rule0 = keyword("CONSTRAINTS") | keyword("CONSTRAINT")

  private def LabelKeyword: Rule0 = keyword("LABELS") | keyword("LABEL")

  private def TypeKeyword: Rule0 = keyword("TYPES") | keyword("TYPE")

  private def NameKeyword: Rule0 = keyword("NAMES") | keyword("NAME")

  private def TransactionKeyword: Rule0 = keyword("TRANSACTION") | keyword("TRANSACTIONS")

  private def passwordKeyword: Rule0 = keyword("PASSWORD") | keyword("PASSWORDS")

  private def RoleKeyword: Rule0 = keyword("ROLES") | keyword("ROLE")

  private def UserKeyword: Rule0 = keyword("USERS") | keyword("USER")

  private def Graph: Rule1[List[GraphScope]] = rule("on a graph")(
    keyword("ON DEFAULT GRAPH") ~~~> (pos => List(ast.DefaultGraphScope()(pos))) |
    group(keyword("ON") ~~ (keyword("GRAPH") | keyword("GRAPHS"))) ~~
      group((SymbolicNameOrStringParameterList ~~>> (names => ipp => names.map(ast.NamedGraphScope(_)(ipp)))) |
      keyword("*") ~~~> (ipp => List(ast.AllGraphsScope()(ipp))))
  )

  private def ScopeForShowPrivileges: Rule1[ShowPrivilegeScope] = rule("show privilege scope")(
    group(RoleKeyword ~~ SymbolicNameOrStringParameterList ~~ keyword("PRIVILEGES")) ~~>> (ast.ShowRolesPrivileges(_)) |
      group(UserKeyword ~~ SymbolicNameOrStringParameterList ~~ keyword("PRIVILEGES")) ~~>> (ast.ShowUsersPrivileges(_)) |
      group(UserKeyword ~~ keyword("PRIVILEGES")) ~~~> ast.ShowUserPrivileges(None) |
      optional(keyword("ALL")) ~~ keyword("PRIVILEGES") ~~~> ast.ShowAllPrivileges()
  )

  def ShowDatabase: Rule1[ShowDatabase] = rule("CATALOG SHOW DATABASE") {
    group(keyword("SHOW") ~~ ScopeForShowDatabase) ~~ ShowCommandClauses ~~>> (ast.ShowDatabase(_,_,_,_))
  }

  private def ScopeForShowDatabase: Rule1[DatabaseScope] = rule("show database scope")(
    group(keyword("DATABASE") ~~ SymbolicNameOrStringParameter) ~~>> (ast.NamedDatabaseScope(_)) |
    keyword("DATABASES") ~~~> (ast.AllDatabasesScope()) |
    keyword("DEFAULT DATABASE") ~~~> (ast.DefaultDatabaseScope())
  )

  def CreateDatabase: Rule1[CreateDatabase] = rule("CATALOG CREATE DATABASE") {
    group(keyword("CREATE OR REPLACE DATABASE") ~~ SymbolicNameOrStringParameter ~~ keyword("IF NOT EXISTS")) ~~>> (ast.CreateDatabase(_, IfExistsInvalidSyntax)) |
    group(keyword("CREATE OR REPLACE DATABASE") ~~ SymbolicNameOrStringParameter) ~~>> (ast.CreateDatabase(_, IfExistsReplace)) |
    group(keyword("CREATE DATABASE") ~~ SymbolicNameOrStringParameter ~~ keyword("IF NOT EXISTS")) ~~>> (ast.CreateDatabase(_, IfExistsDoNothing)) |
    group(keyword("CREATE DATABASE") ~~ SymbolicNameOrStringParameter) ~~>> (ast.CreateDatabase(_, IfExistsThrowError))
  }

  def DropDatabase: Rule1[DropDatabase] = rule("CATALOG DROP DATABASE") {
    group(keyword("DROP DATABASE") ~~ SymbolicNameOrStringParameter ~~ keyword("IF EXISTS") ~~ keyword("DUMP DATA")) ~~>> (ast.DropDatabase(_, ifExists = true, DumpData)) |
    group(keyword("DROP DATABASE") ~~ SymbolicNameOrStringParameter ~~ keyword("IF EXISTS") ~~ optional(keyword("DESTROY DATA"))) ~~>> (ast.DropDatabase(_, ifExists = true, DestroyData)) |
    group(keyword("DROP DATABASE") ~~ SymbolicNameOrStringParameter ~~ keyword("DUMP DATA")) ~~>> (ast.DropDatabase(_, ifExists = false, DumpData)) |
    group(keyword("DROP DATABASE") ~~ SymbolicNameOrStringParameter ~~ optional(keyword("DESTROY DATA"))) ~~>> (ast.DropDatabase(_, ifExists = false, DestroyData))
  }

  def StartDatabase: Rule1[StartDatabase] = rule("CATALOG START DATABASE") {
    group(keyword("START DATABASE") ~~ SymbolicNameOrStringParameter) ~~>> (ast.StartDatabase(_))
  }

  def StopDatabase: Rule1[StopDatabase] = rule("CATALOG STOP DATABASE") {
    group(keyword("STOP DATABASE") ~~ SymbolicNameOrStringParameter) ~~>> (ast.StopDatabase(_))
  }

  def CreateGraph: Rule1[CreateGraph] = rule("CATALOG CREATE GRAPH") {
    group(keyword("CATALOG CREATE GRAPH") ~~ CatalogName ~~ "{" ~~
      RegularQuery ~~
      "}") ~~>> (ast.CreateGraph(_, _))
  }

  def DropGraph: Rule1[DropGraph] = rule("CATALOG DROP GRAPH") {
    group(keyword("CATALOG DROP GRAPH") ~~ CatalogName) ~~>> (ast.DropGraph(_))
  }

  def CreateView: Rule1[CreateView] = rule("CATALOG CREATE VIEW") {
    group((keyword("CATALOG CREATE VIEW") | keyword("CATALOG CREATE QUERY")) ~~
      CatalogName ~~ optional("(" ~~ zeroOrMore(Parameter, separator = CommaSep) ~~ ")") ~~ "{" ~~
      captureString(RegularQuery) ~~
      "}") ~~>> { case (name, params, (query, string)) => ast.CreateView(name, params.getOrElse(Seq.empty), query, string) }
  }

  def DropView: Rule1[DropView] = rule("CATALOG DROP VIEW") {
    group((keyword("CATALOG DROP VIEW") | keyword("CATALOG DROP QUERY")) ~~ CatalogName) ~~>> (ast.DropView(_))
  }

  def SymbolicNameOrStringParameter: Rule1[Either[String, expressions.Parameter]] =
    group(SymbolicNameString) ~~>> (s => _ => Left(s)) |
      group(StringParameter) ~~>> (p => _ => Right(p))

  def SymbolicNameOrStringParameterList: Rule1[List[Either[String, expressions.Parameter]]] =
    rule("a list of symbolic names or string parameters") {
      (oneOrMore(WS ~~ SymbolicNameOrStringParameter ~~ WS, separator = ",") memoMismatches).suppressSubnodes
    }
}
