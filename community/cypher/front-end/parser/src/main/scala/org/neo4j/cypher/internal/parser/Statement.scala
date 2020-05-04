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
import org.neo4j.cypher.internal.ast.DenyPrivilege
import org.neo4j.cypher.internal.ast.DropDatabase
import org.neo4j.cypher.internal.ast.DropGraph
import org.neo4j.cypher.internal.ast.DropRole
import org.neo4j.cypher.internal.ast.DropUser
import org.neo4j.cypher.internal.ast.DropView
import org.neo4j.cypher.internal.ast.GrantPrivilege
import org.neo4j.cypher.internal.ast.GrantRolesToUsers
import org.neo4j.cypher.internal.ast.GraphAction
import org.neo4j.cypher.internal.ast.GraphScope
import org.neo4j.cypher.internal.ast.IfExistsDo
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsInvalidSyntax
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
import org.neo4j.cypher.internal.ast.RevokePrivilege
import org.neo4j.cypher.internal.ast.RevokeRolesFromUsers
import org.neo4j.cypher.internal.ast.SetOwnPassword
import org.neo4j.cypher.internal.ast.ShowDatabase
import org.neo4j.cypher.internal.ast.ShowDatabases
import org.neo4j.cypher.internal.ast.ShowDefaultDatabase
import org.neo4j.cypher.internal.ast.ShowPrivilegeScope
import org.neo4j.cypher.internal.ast.ShowPrivileges
import org.neo4j.cypher.internal.ast.ShowRoles
import org.neo4j.cypher.internal.ast.ShowUsers
import org.neo4j.cypher.internal.ast.StartDatabase
import org.neo4j.cypher.internal.ast.StopDatabase
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.Parameter
import org.parboiled.scala.Parser
import org.parboiled.scala.Rule0
import org.parboiled.scala.Rule1
import org.parboiled.scala.Rule3
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
    optional(UseGraph) ~~ (MultiDatabaseAdministrationCommand | UserAdministrationCommand | PrivilegeAdministrationCommand) ~~> ((use, command) => command.withGraph(use))
  )

  def MultiDatabaseAdministrationCommand: Rule1[ast.AdministrationCommand] = rule("MultiDatabase administration statement") {
    optional(keyword("CATALOG")) ~~ (ShowDatabase | ShowDatabases | ShowDefaultDatabase | CreateDatabase | DropDatabase | StartDatabase | StopDatabase)
  }

  def UserAdministrationCommand: Rule1[ast.AdministrationCommand] = rule("Security role and user administration statement") {
    optional(keyword("CATALOG")) ~~ (ShowRoles | CreateRole | DropRole | ShowUsers | CreateUser | DropUser | AlterUser | SetOwnPassword)
  }

  def PrivilegeAdministrationCommand: Rule1[ast.AdministrationCommand] = rule("Security privilege administration statement") {
    optional(keyword("CATALOG")) ~~ (ShowPrivileges | GrantCommand | DenyCommand | RevokeCommand)
  }

  def GrantCommand: Rule1[ast.AdministrationCommand] = rule("Security privilege grant statement") {
      GrantRole | GrantDatabasePrivilege | GrantTraverse | GrantRead | GrantMatch |  GrantGraphPrivilege | GrantSetLabel | GrantRemoveLabel | GrantDbmsPrivilege
  }

  def DenyCommand: Rule1[ast.AdministrationCommand] = rule("Security privilege deny statement") {
    DenyDatabasePrivilege | DenyTraverse | DenyRead | DenyMatch |  DenyGraphPrivilege | DenySetLabel | DenyRemoveLabel | DenyDbmsPrivilege
  }

  def RevokeCommand: Rule1[ast.AdministrationCommand] = rule("Security privilege revoke statement") {
    RevokeRole | RevokeDatabasePrivilege | RevokeTraverse | RevokeRead | RevokeMatch | RevokeGraphPrivilege | RevokeSetLabel | RevokeRemoveLabel | RevokeGrant | RevokeDeny | RevokeDbmsPrivilege
  }

  def RevokeGrant: Rule1[ast.AdministrationCommand] = rule("Security privilege revoke grant statement") {
    RevokeGrantDatabasePrivilege | RevokeGrantTraverse | RevokeGrantRead | RevokeGrantMatch | RevokeGrantSetLabel | RevokeGrantRemoveLabel | RevokeGrantDbmsPrivilege
  }

  def RevokeDeny: Rule1[ast.AdministrationCommand] = rule("Security privilege revoke deny statement") {
    RevokeDenyDatabasePrivilege | RevokeDenyTraverse | RevokeDenyRead | RevokeDenyMatch |  RevokeDenySetLabel | RevokeDenyRemoveLabel | RevokeDenyDbmsPrivilege
  }

  def ShowUsers: Rule1[ShowUsers] = rule("CATALOG SHOW USERS") {
    keyword("SHOW USERS") ~>>> (_=> ast.ShowUsers())
  }

  def CreateUser: Rule1[CreateUser] = rule("CATALOG CREATE USER") {
    // CREATE [OR REPLACE] USER username [IF NOT EXISTS] SET PASSWORD stringLiteralPassword optionalStatus
    group(createUserStart ~~ keyword("SET PASSWORD") ~~ SensitiveStringLiteral ~~ optionalStatus) ~~>> ((userNameAndIfExistsDo, initialPassword, suspended) =>
      ast.CreateUser(userNameAndIfExistsDo._1, initialPassword, requirePasswordChange = true, suspended, userNameAndIfExistsDo._2)) |
    // CREATE [OR REPLACE] USER username [IF NOT EXISTS] SET PASSWORD stringLiteralPassword optionalRequirePasswordChange optionalStatus
    group(createUserStart ~~ keyword("SET PASSWORD") ~~ SensitiveStringLiteral ~~
    optionalRequirePasswordChange ~~ optionalStatus) ~~>> ((userNameAndIfExistsDo, initialPassword, requirePasswordChange, suspended) =>
      ast.CreateUser(userNameAndIfExistsDo._1, initialPassword, requirePasswordChange.getOrElse(true), suspended, userNameAndIfExistsDo._2)) |
    //
    // CREATE [OR REPLACE] USER username [IF NOT EXISTS] SET PASSWORD parameterPassword optionalStatus
    group(createUserStart ~~ keyword("SET PASSWORD") ~~ SensitiveStringParameter ~~
    optionalStatus) ~~>> ((userNameAndIfExistsDo, initialPassword, suspended) =>
      ast.CreateUser(userNameAndIfExistsDo._1, initialPassword, requirePasswordChange = true, suspended, userNameAndIfExistsDo._2)) |
    // CREATE [OR REPLACE] USER username [IF NOT EXISTS] SET PASSWORD parameterPassword optionalRequirePasswordChange optionalStatus
    group(createUserStart ~~ keyword("SET PASSWORD") ~~ SensitiveStringParameter ~~
    optionalRequirePasswordChange ~~ optionalStatus) ~~>> ((userNameAndIfExistsDo, initialPassword, requirePasswordChange, suspended) =>
      ast.CreateUser(userNameAndIfExistsDo._1, initialPassword, requirePasswordChange.getOrElse(true), suspended, userNameAndIfExistsDo._2))
  }

  def createUserStart: Rule1[(Either[String, Parameter], IfExistsDo)] = {
    // returns (userName, IfExistsDo)
    group(keyword("CREATE OR REPLACE USER") ~~ SymbolicNameOrStringParameter ~~ keyword("IF NOT EXISTS")) ~~> ((_, IfExistsInvalidSyntax())) |
    group(keyword("CREATE OR REPLACE USER") ~~ SymbolicNameOrStringParameter) ~~> ((_, IfExistsReplace())) |
    group(keyword("CREATE USER") ~~ SymbolicNameOrStringParameter ~~ keyword("IF NOT EXISTS")) ~~> ((_, IfExistsDoNothing())) |
    group(keyword("CREATE USER") ~~ SymbolicNameOrStringParameter) ~~> ((_, IfExistsThrowError()))
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
      keyword("WITH USERS")) ~>>> (_ => ast.ShowRoles(withUsers = true, showAll = false)) |
    group(keyword("SHOW") ~~ optional(keyword("ALL")) ~~ keyword("ROLES") ~~
      keyword("WITH USERS")) ~>>> (_ => ast.ShowRoles(withUsers = true, showAll = true)) |
    // SHOW [ ALL | POPULATED ] ROLES
    group(keyword("SHOW") ~~ keyword("POPULATED") ~~ keyword("ROLES")) ~>>>
      (_ => ast.ShowRoles(withUsers = false, showAll = false)) |
    group(keyword("SHOW") ~~ optional(keyword("ALL")) ~~ keyword("ROLES")) ~>>>
      (_ => ast.ShowRoles(withUsers = false, showAll = true))
  }

  def CreateRole: Rule1[CreateRole] = rule("CATALOG CREATE ROLE") {
    group(keyword("CREATE OR REPLACE ROLE") ~~ SymbolicNameOrStringParameter ~~ keyword("IF NOT EXISTS") ~~
      optional(keyword("AS COPY OF") ~~ SymbolicNameOrStringParameter)) ~~>> (ast.CreateRole(_, _, IfExistsInvalidSyntax())) |
    group(keyword("CREATE OR REPLACE ROLE") ~~ SymbolicNameOrStringParameter ~~
      optional(keyword("AS COPY OF") ~~ SymbolicNameOrStringParameter)) ~~>> (ast.CreateRole(_, _, IfExistsReplace())) |
    group(keyword("CREATE ROLE") ~~ SymbolicNameOrStringParameter ~~ keyword("IF NOT EXISTS") ~~
      optional(keyword("AS COPY OF") ~~ SymbolicNameOrStringParameter)) ~~>> (ast.CreateRole(_, _, IfExistsDoNothing())) |
    group(keyword("CREATE ROLE") ~~ SymbolicNameOrStringParameter ~~
      optional(keyword("AS COPY OF") ~~ SymbolicNameOrStringParameter)) ~~>> (ast.CreateRole(_, _, IfExistsThrowError()))
  }

  def DropRole: Rule1[DropRole] = rule("CATALOG DROP ROLE") {
    group(keyword("DROP ROLE") ~~ SymbolicNameOrStringParameter ~~ keyword("IF EXISTS")) ~~>> (ast.DropRole(_, ifExists = true)) |
    group(keyword("DROP ROLE") ~~ SymbolicNameOrStringParameter) ~~>> (ast.DropRole(_, ifExists = false))
  }

  def GrantRole: Rule1[GrantRolesToUsers] = rule("CATALOG GRANT ROLE") {
    group(keyword("GRANT") ~~ (keyword("ROLES") | keyword("ROLE")) ~~ SymbolicNameOrStringParameterList ~~
      keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>> (ast.GrantRolesToUsers(_, _))
  }

  def RevokeRole: Rule1[RevokeRolesFromUsers] = rule("CATALOG REVOKE ROLE") {
    group(keyword("REVOKE") ~~ (keyword("ROLES") | keyword("ROLE")) ~~ SymbolicNameOrStringParameterList ~~
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
      ((dbmsAction, grantees) => ast.RevokePrivilege.dbmsAction( dbmsAction, grantees ))
  }

  def RevokeGrantDbmsPrivilege: Rule1[RevokePrivilege] = rule("CATALOG REVOKE GRANT dbms privileges") {
    group(keyword("REVOKE GRANT") ~~ DbmsAction ~~ keyword("ON DBMS FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((dbmsAction, grantees) => ast.RevokePrivilege.grantedDbmsAction( dbmsAction, grantees ))
  }

  def RevokeDenyDbmsPrivilege: Rule1[RevokePrivilege] = rule("CATALOG REVOKE DENY dbms privileges") {
    group(keyword("REVOKE DENY") ~~ DbmsAction ~~ keyword("ON DBMS FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((dbmsAction, grantees) => ast.RevokePrivilege.deniedDbmsAction( dbmsAction, grantees ))
  }

  //` ... ON DATABASE foo TO role`
  def GrantDatabasePrivilege: Rule1[GrantPrivilege] = rule("CATALOG GRANT database privileges") {
    group(keyword("GRANT") ~~ QualifiedDatabaseAction ~~ Database ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((qualifiedAction, scope, grantees) => ast.GrantPrivilege.databaseAction( qualifiedAction._1, scope, grantees, qualifiedAction._2)) |
    group(keyword("GRANT") ~~ DatabaseAction ~~ Database ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((databaseAction, scope, grantees) => ast.GrantPrivilege.databaseAction( databaseAction, scope, grantees))
  }

  def DenyDatabasePrivilege: Rule1[DenyPrivilege] = rule("CATALOG DENY database privileges") {
    group(keyword("DENY") ~~ QualifiedDatabaseAction ~~ Database ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((qualifiedAction, scope, grantees) => ast.DenyPrivilege.databaseAction( qualifiedAction._1, scope, grantees, qualifiedAction._2)) |
    group(keyword("DENY") ~~ DatabaseAction ~~ Database ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((databaseAction, scope, grantees) => ast.DenyPrivilege.databaseAction( databaseAction, scope, grantees))
  }

  def RevokeGrantDatabasePrivilege: Rule1[RevokePrivilege] = rule("CATALOG REVOKE GRANT database privileges") {
    group(keyword("REVOKE GRANT") ~~ QualifiedDatabaseAction ~~ Database ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((qualifiedAction, scope, grantees) => ast.RevokePrivilege.databaseGrantedAction( qualifiedAction._1, scope, grantees, qualifiedAction._2)) |
    group(keyword("REVOKE GRANT") ~~ DatabaseAction ~~ Database ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((databaseAction, scope, grantees) => ast.RevokePrivilege.databaseGrantedAction( databaseAction, scope, grantees))
  }

  def RevokeDenyDatabasePrivilege: Rule1[RevokePrivilege] = rule("CATALOG REVOKE DENY database privileges") {
    group(keyword("REVOKE DENY") ~~ QualifiedDatabaseAction ~~ Database ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((qualifiedAction, scope, grantees) => ast.RevokePrivilege.databaseDeniedAction( qualifiedAction._1, scope, grantees, qualifiedAction._2)) |
    group(keyword("REVOKE DENY") ~~ DatabaseAction ~~ Database ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((databaseAction, scope, grantees) => ast.RevokePrivilege.databaseDeniedAction( databaseAction, scope, grantees))
  }

  def RevokeDatabasePrivilege: Rule1[RevokePrivilege] = rule("CATALOG REVOKE database privileges") {
    group(keyword("REVOKE") ~~ QualifiedDatabaseAction ~~ Database ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((qualifiedAction, scope, grantees) => ast.RevokePrivilege.databaseAction( qualifiedAction._1, scope, grantees, qualifiedAction._2)) |
    group(keyword("REVOKE") ~~ DatabaseAction ~~ Database ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((databaseAction, scope, grantees) => ast.RevokePrivilege.databaseAction( databaseAction, scope, grantees))
  }

  //`GRANT TRAVERSE ON GRAPH foo ELEMENTS A (*) TO role`
  def GrantTraverse: Rule1[GrantPrivilege] = rule("CATALOG GRANT TRAVERSE") {
    group(keyword("GRANT TRAVERSE") ~~ Graph ~~ ScopeQualifierWithProperty ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((scope, qualifier, grantees) => ast.GrantPrivilege.traverse(scope, qualifier, grantees))
  }

  //`DENY TRAVERSE ON GRAPH foo ELEMENTS A (*) TO role`
  def DenyTraverse: Rule1[DenyPrivilege] = rule("CATALOG DENY TRAVERSE") {
    group(keyword("DENY TRAVERSE") ~~ Graph ~~ ScopeQualifierWithProperty ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((scope, qualifier, grantees) => ast.DenyPrivilege.traverse(scope, qualifier, grantees))
  }

  //`REVOKE GRANT TRAVERSE ON GRAPH foo ELEMENTS A (*) FROM role`
  def RevokeGrantTraverse: Rule1[RevokePrivilege] = rule("CATALOG REVOKE GRANT TRAVERSE") {
    group(keyword("REVOKE GRANT TRAVERSE") ~~ Graph ~~ ScopeQualifierWithProperty ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((scope, qualifier, grantees) => ast.RevokePrivilege.grantedTraverse(scope, qualifier, grantees))
  }

  //`REVOKE DENY TRAVERSE ON GRAPH foo ELEMENTS A (*) FROM role`
  def RevokeDenyTraverse: Rule1[RevokePrivilege] = rule("CATALOG REVOKE DENY TRAVERSE") {
    group(keyword("REVOKE DENY TRAVERSE") ~~ Graph ~~ ScopeQualifierWithProperty ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((scope, qualifier, grantees) => ast.RevokePrivilege.deniedTraverse(scope, qualifier, grantees))
  }

  //`REVOKE TRAVERSE ON GRAPH foo ELEMENTS A (*) FROM role`
  def RevokeTraverse: Rule1[RevokePrivilege] = rule("CATALOG REVOKE TRAVERSE") {
    group(keyword("REVOKE TRAVERSE") ~~ Graph ~~ ScopeQualifierWithProperty ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((scope, qualifier, grantees) => ast.RevokePrivilege.traverse(scope, qualifier, grantees))
  }

  //`GRANT READ {a} ON GRAPH foo ELEMENTS A (*) TO role`
  def GrantRead: Rule1[GrantPrivilege] = rule("CATALOG GRANT READ") {
    group(keyword("GRANT READ") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifierWithProperty ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((prop, scope, qualifier, grantees) => ast.GrantPrivilege.read(prop, scope, qualifier, grantees))
  }

  //`DENY READ {a} ON GRAPH foo ELEMENTS A (*) TO role`
  def DenyRead: Rule1[DenyPrivilege] = rule("CATALOG DENY READ") {
    group(keyword("DENY READ") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifierWithProperty ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((prop, scope, qualifier, grantees) => ast.DenyPrivilege.read(prop, scope, qualifier, grantees))
  }

  //`REVOKE GRANT READ {a} ON GRAPH foo ELEMENTS A (*) FROM role`
  def RevokeGrantRead: Rule1[RevokePrivilege] = rule("CATALOG REVOKE GRANT READ") {
    group(keyword("REVOKE GRANT READ") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifierWithProperty ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((prop, scope, qualifier, grantees) => ast.RevokePrivilege.grantedRead(prop, scope, qualifier, grantees))
  }

  //`REVOKE DENY READ {a} ON GRAPH foo ELEMENTS A (*) FROM role`
  def RevokeDenyRead: Rule1[RevokePrivilege] = rule("CATALOG REVOKE DENY READ") {
    group(keyword("REVOKE DENY READ") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifierWithProperty ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((prop, scope, qualifier, grantees) => ast.RevokePrivilege.deniedRead(prop, scope, qualifier, grantees))
  }

  //`REVOKE READ {a} ON GRAPH foo ELEMENTS A (*) FROM role`
  def RevokeRead: Rule1[RevokePrivilege] = rule("CATALOG REVOKE READ") {
    group(keyword("REVOKE READ") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifierWithProperty ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((prop, scope, qualifier, grantees) => ast.RevokePrivilege.read(prop, scope, qualifier, grantees))
  }

  //`GRANT MATCH {a} ON GRAPH foo ELEMENTS A (*) TO role`
  def GrantMatch: Rule1[GrantPrivilege] = rule("CATALOG GRANT MATCH") {
    group(keyword("GRANT MATCH") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifierWithProperty ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((prop, scope, qualifier, grantees) => ast.GrantPrivilege.asMatch(prop, scope, qualifier, grantees))
  }

  //`DENY MATCH {a} ON GRAPH foo ELEMENTS A (*) TO role`
  def DenyMatch: Rule1[DenyPrivilege] = rule("CATALOG DENY MATCH") {
    group(keyword("DENY MATCH") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifierWithProperty ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((prop, scope, qualifier, grantees) => ast.DenyPrivilege.asMatch(prop, scope, qualifier, grantees))
  }

  //`REVOKE GRANT MATCH {a} ON GRAPH foo ELEMENTS A (*) FROM role`
  def RevokeGrantMatch: Rule1[RevokePrivilege] = rule("CATALOG REVOKE GRANT MATCH") {
    group(keyword("REVOKE GRANT MATCH") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifierWithProperty ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((prop, scope, qualifier, grantees) => ast.RevokePrivilege.grantedAsMatch(prop, scope, qualifier, grantees))
  }

  //`REVOKE DENY MATCH {a} ON GRAPH foo ELEMENTS A (*) FROM role`
  def RevokeDenyMatch: Rule1[RevokePrivilege] = rule("CATALOG REVOKE DENY MATCH") {
    group(keyword("REVOKE DENY MATCH") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifierWithProperty ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((prop, scope, qualifier, grantees) => ast.RevokePrivilege.deniedAsMatch(prop, scope, qualifier, grantees))
  }

  //`REVOKE MATCH {a} ON GRAPH foo ELEMENTS A (*) FROM role`
  def RevokeMatch: Rule1[RevokePrivilege] = rule("CATALOG REVOKE MATCH") {
    group(keyword("REVOKE MATCH") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifierWithProperty ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((prop, scope, qualifier, grantees) => ast.RevokePrivilege.asMatch(prop, scope, qualifier, grantees))
  }

   //`GRANT ON GRAPH foo TO role`
  def GrantGraphPrivilege: Rule1[GrantPrivilege] = rule("CATALOG GRANT CREATE") {
    group(keyword("GRANT") ~~ QualifiedGraphAction ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, qualifier, action, roles) => ast.GrantPrivilege.graphAction(action, None, graphScope, qualifier, roles))
  }

  //`DENY ON GRAPH foo TO role`
  def DenyGraphPrivilege: Rule1[DenyPrivilege] = rule("CATALOG GRANT DELETE") {
    group(keyword("DENY") ~~ QualifiedGraphAction ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, qualifier, action, roles) => ast.DenyPrivilege.graphAction(action, None, graphScope, qualifier, roles))
  }

  //`REVOKE ON GRAPH foo TO role`
  def RevokeGraphPrivilege: Rule1[RevokePrivilege] = rule("CATALOG REVOKE GRANT CREATE") {
    group(keyword("REVOKE GRANT") ~~ QualifiedGraphAction ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, qualifier, action, roles) => ast.RevokePrivilege.grantedGraphAction(action, None, graphScope, qualifier, roles)) |
    group(keyword("REVOKE DENY") ~~ QualifiedGraphAction ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, qualifier, action, roles) => ast.RevokePrivilege.deniedGraphAction(action, None, graphScope, qualifier, roles)) |
    group(keyword("REVOKE") ~~ QualifiedGraphAction ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((graphScope, qualifier, action, roles) => ast.RevokePrivilege.graphAction(action, None, graphScope, qualifier, roles))
  }

  //`GRANT SET LABEL * ON GRAPH foo TO role`
  def GrantSetLabel: Rule1[GrantPrivilege] = rule("CATALOG GRANT SET LABEL") {
    group(keyword("GRANT SET LABEL") ~~ LabelQualifier ~~ Graph ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((labels, graphs, roles) => ast.GrantPrivilege.setLabel(graphs, labels, roles))
  }

  //`DENY SET LABEL * ON GRAPH foo TO role`
  def DenySetLabel: Rule1[DenyPrivilege] = rule("CATALOG DENY SET LABEL") {
    group(keyword("DENY SET LABEL") ~~ LabelQualifier ~~ Graph ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((labels, graphs, roles) => ast.DenyPrivilege.setLabel(graphs, labels, roles))
  }

  //`REVOKE GRANT SET LABEL * ON GRAPH foo FROM role`
  def RevokeGrantSetLabel: Rule1[RevokePrivilege] = rule("CATALOG REVOKE GRANT SET LABEL") {
    group(keyword("REVOKE GRANT SET LABEL") ~~ LabelQualifier ~~ Graph ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((labels, graphs, roles) => ast.RevokePrivilege.grantedSetLabel(graphs, labels, roles))
  }

  //`REVOKE DENY SET LABEL * ON GRAPH foo FROM role`
  def RevokeDenySetLabel: Rule1[RevokePrivilege] = rule("CATALOG REVOKE DENY SET LABEL") {
    group(keyword("REVOKE DENY SET LABEL") ~~ LabelQualifier ~~ Graph ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((labels, graphs, roles) => ast.RevokePrivilege.deniedSetLabel(graphs, labels, roles))
  }

  //`REVOKE SET LABEL * ON GRAPH foo FROM role`
  def RevokeSetLabel: Rule1[RevokePrivilege] = rule("CATALOG REVOKE SET LABEL") {
    group(keyword("REVOKE SET LABEL") ~~ LabelQualifier ~~ Graph ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((labels, graphs, roles) => ast.RevokePrivilege.setLabel(graphs, labels, roles))
  }

    //`GRANT REMOVE LABEL * ON GRAPH foo TO role`
  def GrantRemoveLabel: Rule1[GrantPrivilege] = rule("CATALOG GRANT REMOVE LABEL") {
    group(keyword("GRANT REMOVE LABEL") ~~ LabelQualifier ~~ Graph ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((labels, graphs, roles) => ast.GrantPrivilege.removeLabel(graphs, labels, roles))
  }

  //`DENY REMOVE LABEL * ON GRAPH foo TO role`
  def DenyRemoveLabel: Rule1[DenyPrivilege] = rule("CATALOG DENY REMOVE LABEL") {
    group(keyword("DENY REMOVE LABEL") ~~ LabelQualifier ~~ Graph ~~ keyword("TO") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((labels, graphs, roles) => ast.DenyPrivilege.removeLabel(graphs, labels, roles))
  }

  //`REVOKE GRANT REMOVE LABEL * ON GRAPH foo FROM role`
  def RevokeGrantRemoveLabel: Rule1[RevokePrivilege] = rule("CATALOG REVOKE GRANT REMOVE LABEL") {
    group(keyword("REVOKE GRANT REMOVE LABEL") ~~ LabelQualifier ~~ Graph ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((labels, graphs, roles) => ast.RevokePrivilege.grantedRemoveLabel(graphs, labels, roles))
  }

  //`REVOKE DENY REMOVE LABEL * ON GRAPH foo FROM role`
  def RevokeDenyRemoveLabel: Rule1[RevokePrivilege] = rule("CATALOG REVOKE DENY REMOVE LABEL") {
    group(keyword("REVOKE DENY REMOVE LABEL") ~~ LabelQualifier ~~ Graph ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((labels, graphs, roles) => ast.RevokePrivilege.deniedRemoveLabel(graphs, labels, roles))
  }

  //`REVOKE REMOVE LABEL * ON GRAPH foo FROM role`
  def RevokeRemoveLabel: Rule1[RevokePrivilege] = rule("CATALOG REVOKE REMOVE LABEL") {
    group(keyword("REVOKE REMOVE LABEL") ~~ LabelQualifier ~~ Graph ~~ keyword("FROM") ~~ SymbolicNameOrStringParameterList) ~~>>
      ((labels, graphs, roles) => ast.RevokePrivilege.removeLabel(graphs, labels, roles))
  }

  def LabelQualifier: Rule1[PrivilegeQualifier] = rule("label used for set/remove label") {
    group(SymbolicNamesList  ~~>> {ast.LabelsQualifier(_)} |
      group(keyword("*") ~~~> {ast.LabelAllQualifier()}))
  }

  def ShowPrivileges: Rule1[ShowPrivileges] = rule("CATALOG SHOW PRIVILEGES") {
    group(keyword("SHOW") ~~ ScopeForShowPrivileges) ~~>> (ast.ShowPrivileges(_))
  }

  private def PrivilegeProperty: Rule1[ActionResource] = rule("{propertyList}")(
    group("{" ~~ SymbolicNamesList ~~ "}") ~~>> {ast.PropertiesResource(_)} |
      group("{" ~~ "*" ~~ "}") ~~~> {ast.AllResource()}
  )

  private def UserQualifier: Rule1[PrivilegeQualifier] = rule("(usernameList)")(
    group("(" ~~ SymbolicNameOrStringParameterList ~~ ")") ~~>> {ast.UsersQualifier(_)} |
    group("(" ~~ "*" ~~ ")") ~~~> {ast.UserAllQualifier()}
  )

  private def ScopeQualifierWithProperty: Rule1[PrivilegeQualifier] = rule("which element type and associated labels/relTypes (props) qualifier combination")(
    ScopeQualifier ~~ optional("(" ~~ "*" ~~ ")")
  )

  private def ScopeQualifier: Rule1[PrivilegeQualifier] = rule("which element type and associated labels/relTypes qualifier combination")(
    group(RelationshipKeyword ~~ SymbolicNamesList) ~~>> {ast.RelationshipsQualifier(_)} |
    group(RelationshipKeyword ~~ "*") ~~~> {ast.RelationshipAllQualifier()} |
    group(NodeKeyword ~~ SymbolicNamesList) ~~>> {ast.LabelsQualifier(_)} |
    group(NodeKeyword ~~ "*") ~~~> {ast.LabelAllQualifier()} |
    group(ElementKeyword ~~ SymbolicNamesList) ~~>> {ast.ElementsQualifier(_)} |
    optional(ElementKeyword ~~ "*") ~~~> {ast.ElementsAllQualifier()}
  )

  private def ElementKeyword: Rule0 = keyword("ELEMENTS") | keyword("ELEMENT")

  private def RelationshipKeyword: Rule0 = keyword("RELATIONSHIPS") | keyword("RELATIONSHIP")

  private def NodeKeyword: Rule0 = keyword("NODE") | keyword("NODES")

  private def Database: Rule1[List[GraphScope]] = rule("on a database") {
    keyword("ON DEFAULT DATABASE") ~~~> (pos => List(ast.DefaultDatabaseScope()(pos))) |
      group(keyword("ON") ~~ (keyword("DATABASE") | keyword("DATABASES"))) ~~
        group((SymbolicNameOrStringParameterList ~~>> (params => pos => params.map(ast.NamedGraphScope(_)(pos)))) |
          (keyword("*") ~~~> (pos => List(ast.AllGraphsScope()(pos)))))
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

  private def QualifiedDatabaseAction: Rule1[(DatabaseAction, PrivilegeQualifier)] = rule("qualified database action")(
    group(keyword("SHOW") ~~ TransactionKeyword ~~ UserQualifier) ~~> ((ast.ShowTransactionAction, _)) |
    group(keyword("SHOW") ~~ TransactionKeyword) ~~~> (pos => (ast.ShowTransactionAction, ast.UserAllQualifier()(pos))) |
    group(keyword("TERMINATE") ~~ TransactionKeyword ~~ UserQualifier) ~~> ((ast.TerminateTransactionAction, _)) |
    group(keyword("TERMINATE") ~~ TransactionKeyword) ~~~> (pos => (ast.TerminateTransactionAction, ast.UserAllQualifier()(pos))) |
    group(keyword("TRANSACTION") ~~ optional(keyword("MANAGEMENT")) ~~ UserQualifier) ~~> ((ast.AllTransactionActions, _)) |
    group(keyword("TRANSACTION") ~~ optional(keyword("MANAGEMENT"))) ~~~> (pos => (ast.AllTransactionActions, ast.UserAllQualifier()(pos)))
  )

  private def QualifiedGraphAction: Rule3[List[GraphScope], PrivilegeQualifier, GraphAction] = rule("qualified graph action")(
    group(keyword("CREATE") ~~ Graph ~~ ScopeQualifier ~> (_ => ast.CreateElementAction)) |
    group(keyword("DELETE") ~~ Graph ~~ ScopeQualifier ~> (_ => ast.DeleteElementAction)) |
    group(keyword("WRITE") ~~ Graph ~~ ScopeQualifierWithProperty ~> (_ => ast.WriteAction))
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

  private def Graph: Rule1[List[GraphScope]] = rule("on a graph")(
    group(keyword("ON") ~~ (keyword("GRAPH") | keyword("GRAPHS"))) ~~
      group((SymbolicNameOrStringParameterList ~~>> (names => ipp => names.map(ast.NamedGraphScope(_)(ipp)))) |
        keyword("*") ~~~> (ipp => List(ast.AllGraphsScope()(ipp))))
  )

  private def ScopeForShowPrivileges: Rule1[ShowPrivilegeScope] = rule("show privilege scope")(
    group(keyword("ROLE") ~~ SymbolicNameOrStringParameter ~~ keyword("PRIVILEGES")) ~~>> (ast.ShowRolePrivileges(_)) |
      group(keyword("USER") ~~ optional(SymbolicNameOrStringParameter) ~~ keyword("PRIVILEGES")) ~~>> (ast.ShowUserPrivileges(_)) |
      group(keyword("USER") ~~ keyword("PRIVILEGES")) ~~~> ast.ShowUserPrivileges(None) |
      optional(keyword("ALL")) ~~ keyword("PRIVILEGES") ~~~> ast.ShowAllPrivileges()
  )

  def ShowDatabase: Rule1[ShowDatabase] = rule("CATALOG SHOW DATABASE") {
    group(keyword("SHOW DATABASE") ~~ SymbolicNameOrStringParameter) ~~>> (ast.ShowDatabase(_))
  }

  def ShowDatabases: Rule1[ShowDatabases] = rule("CATALOG SHOW DATABASES") {
    keyword("SHOW DATABASES") ~>>> (_=> ast.ShowDatabases())
  }

  def ShowDefaultDatabase: Rule1[ShowDefaultDatabase] = rule("CATALOG SHOW DEFAULT DATABASE") {
    keyword("SHOW DEFAULT DATABASE") ~>>> (_=> ast.ShowDefaultDatabase())
  }

  def CreateDatabase: Rule1[CreateDatabase] = rule("CATALOG CREATE DATABASE") {
    group(keyword("CREATE OR REPLACE DATABASE") ~~ SymbolicNameOrStringParameter ~~ keyword("IF NOT EXISTS")) ~~>> (ast.CreateDatabase(_, IfExistsInvalidSyntax())) |
    group(keyword("CREATE OR REPLACE DATABASE") ~~ SymbolicNameOrStringParameter) ~~>> (ast.CreateDatabase(_, IfExistsReplace())) |
    group(keyword("CREATE DATABASE") ~~ SymbolicNameOrStringParameter ~~ keyword("IF NOT EXISTS")) ~~>> (ast.CreateDatabase(_, IfExistsDoNothing())) |
    group(keyword("CREATE DATABASE") ~~ SymbolicNameOrStringParameter) ~~>> (ast.CreateDatabase(_, IfExistsThrowError()))
  }

  def DropDatabase: Rule1[DropDatabase] = rule("CATALOG DROP DATABASE") {
    group(keyword("DROP DATABASE") ~~ SymbolicNameOrStringParameter ~~ keyword("IF EXISTS")) ~~>> (ast.DropDatabase(_, ifExists = true)) |
    group(keyword("DROP DATABASE") ~~ SymbolicNameOrStringParameter) ~~>> (ast.DropDatabase(_, ifExists = false))
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
