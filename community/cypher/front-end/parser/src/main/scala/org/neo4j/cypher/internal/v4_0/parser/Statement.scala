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
package org.neo4j.cypher.internal.v4_0.parser

import org.neo4j.cypher.internal.v4_0.ast
import org.neo4j.cypher.internal.v4_0.ast._
import org.parboiled.scala._

trait Statement extends Parser
  with Query
  with Command
  with Base {

  def Statement: Rule1[ast.Statement] = rule(
    MultiDatabaseAdministrationCommand | CatalogCommand | UserAdministrationCommand | PrivilegeAdministrationCommand | Command | Query
  )

  def CatalogCommand: Rule1[CatalogDDL] = rule("Catalog DDL statement") {
    CreateGraph | DropGraph | CreateView | DropView
  }

  def MultiDatabaseAdministrationCommand: Rule1[CatalogDDL] = rule("MultiDatabase administration statement") {
    optional(keyword("CATALOG")) ~~ (ShowDatabase | ShowDatabases | ShowDefaultDatabase | CreateDatabase | DropDatabase | StartDatabase | StopDatabase)
  }

  def UserAdministrationCommand: Rule1[CatalogDDL] = rule("Security user administration statement") {
    optional(keyword("CATALOG")) ~~ (ShowRoles | CreateRole | DropRole | ShowUsers | CreateUser | DropUser | AlterUser | SetOwnPassword)
  }

  def PrivilegeAdministrationCommand: Rule1[CatalogDDL] = rule("Security privilege administration statement") {
    optional(keyword("CATALOG")) ~~ (ShowPrivileges | GrantCommand | DenyCommand | RevokeCommand)
  }

  def GrantCommand: Rule1[CatalogDDL] = rule("Security privilege grant statement") {
      GrantRole | GrantDatabasePrivilege | GrantTraverse | GrantRead | GrantMatch | GrantWrite | GrantDbmsPrivilege
  }

  def DenyCommand: Rule1[CatalogDDL] = rule("Security privilege deny statement") {
    DenyDatabasePrivilege | DenyTraverse | DenyRead | DenyMatch | DenyWrite | DenyDbmsPrivilege
  }

  def RevokeCommand: Rule1[CatalogDDL] = rule("Security privilege revoke statement") {
    RevokeRole | RevokeDatabasePrivilege | RevokeTraverse | RevokeRead | RevokeMatch | RevokeWrite | RevokeGrant | RevokeDeny | RevokeDbmsPrivilege
  }

  def RevokeGrant: Rule1[CatalogDDL] = rule("Security privilege revoke grant statement") {
    RevokeGrantDatabasePrivilege | RevokeGrantTraverse | RevokeGrantRead | RevokeGrantMatch | RevokeGrantWrite | RevokeGrantDbmsPrivilege
  }

  def RevokeDeny: Rule1[CatalogDDL] = rule("Security privilege revoke deny statement") {
    RevokeDenyDatabasePrivilege | RevokeDenyTraverse | RevokeDenyRead | RevokeDenyMatch | RevokeDenyWrite | RevokeDenyDbmsPrivilege
  }

  def ShowUsers: Rule1[ShowUsers] = rule("CATALOG SHOW USERS") {
    keyword("SHOW USERS") ~>>> (_=> ast.ShowUsers())
  }

  def CreateUser: Rule1[CreateUser] = rule("CATALOG CREATE USER") {
    // CREATE [OR REPLACE] USER username [IF NOT EXISTS] SET PASSWORD stringLiteralPassword optionalStatus
    group(createUserStart ~~ keyword("SET PASSWORD") ~~ StringLiteral ~~
    optionalStatus) ~~>> ((userNameAndIfExistsDo, initialPassword, suspended) =>
      ast.CreateUser(userNameAndIfExistsDo._1, Some(initialPassword.value), None, requirePasswordChange = true, suspended, userNameAndIfExistsDo._2)) |
    // CREATE [OR REPLACE] USER username [IF NOT EXISTS] SET PASSWORD stringLiteralPassword optionalRequirePasswordChange optionalStatus
    group(createUserStart ~~ keyword("SET PASSWORD") ~~ StringLiteral ~~
    optionalRequirePasswordChange ~~ optionalStatus) ~~>> ((userNameAndIfExistsDo, initialPassword, requirePasswordChange, suspended) =>
      ast.CreateUser(userNameAndIfExistsDo._1, Some(initialPassword.value), None, requirePasswordChange.getOrElse(true), suspended, userNameAndIfExistsDo._2)) |
    //
    // CREATE [OR REPLACE] USER username [IF NOT EXISTS] SET PASSWORD parameterPassword optionalStatus
    group(createUserStart ~~ keyword("SET PASSWORD") ~~ Parameter ~~
    optionalStatus) ~~>> ((userNameAndIfExistsDo, initialPassword, suspended) =>
      ast.CreateUser(userNameAndIfExistsDo._1, None, Some(initialPassword), requirePasswordChange = true, suspended, userNameAndIfExistsDo._2)) |
    // CREATE [OR REPLACE] USER username [IF NOT EXISTS] SET PASSWORD parameterPassword optionalRequirePasswordChange optionalStatus
    group(createUserStart ~~ keyword("SET PASSWORD") ~~ Parameter ~~
    optionalRequirePasswordChange ~~ optionalStatus) ~~>> ((userNameAndIfExistsDo, initialPassword, requirePasswordChange, suspended) =>
      ast.CreateUser(userNameAndIfExistsDo._1, None, Some(initialPassword), requirePasswordChange.getOrElse(true), suspended, userNameAndIfExistsDo._2))
  }

  def createUserStart: Rule1[(String, IfExistsDo)] = {
    // returns (userName, IfExistsDo)
    group(keyword("CREATE OR REPLACE USER") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS")) ~~> ((_, IfExistsInvalidSyntax())) |
    group(keyword("CREATE OR REPLACE USER") ~~ SymbolicNameString) ~~> ((_, IfExistsReplace())) |
    group(keyword("CREATE USER") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS")) ~~> ((_, IfExistsDoNothing())) |
    group(keyword("CREATE USER") ~~ SymbolicNameString) ~~> ((_, IfExistsThrowError()))
  }

  def DropUser: Rule1[DropUser] = rule("CATALOG DROP USER") {
    group(keyword("DROP USER") ~~ SymbolicNameString ~~ keyword("IF EXISTS")) ~~>> (ast.DropUser(_, ifExists = true)) |
    group(keyword("DROP USER") ~~ SymbolicNameString) ~~>> (ast.DropUser(_, ifExists = false))
  }

  def AlterUser: Rule1[AlterUser] = rule("CATALOG ALTER USER") {
    // ALTER USER username SET PASSWORD stringLiteralPassword optionalStatus
    group(keyword("ALTER USER") ~~ SymbolicNameString ~~ keyword("SET PASSWORD") ~~ StringLiteral ~~
    optionalStatus) ~~>> ((userName, initialPassword, suspended) =>
      ast.AlterUser(userName, Some(initialPassword.value), None, None, suspended)) |
    // ALTER USER username SET PASSWORD stringLiteralPassword optionalRequirePasswordChange optionalStatus
    group(keyword("ALTER USER") ~~ SymbolicNameString ~~ keyword("SET PASSWORD") ~~ StringLiteral ~~
    optionalRequirePasswordChange ~~ optionalStatus) ~~>> ((userName, initialPassword, requirePasswordChange, suspended) =>
      ast.AlterUser(userName, Some(initialPassword.value), None, requirePasswordChange, suspended)) |
    //
    // ALTER USER username SET PASSWORD parameterPassword optionalStatus
    group(keyword("ALTER USER") ~~ SymbolicNameString ~~ keyword("SET PASSWORD") ~~ Parameter ~~
    optionalStatus) ~~>> ((userName, initialPassword, suspended) =>
      ast.AlterUser(userName, None, Some(initialPassword), None, suspended)) |
    // ALTER USER username SET PASSWORD parameterPassword optionalRequirePasswordChange optionalStatus
    group(keyword("ALTER USER") ~~ SymbolicNameString ~~ keyword("SET PASSWORD") ~~ Parameter ~~
    optionalRequirePasswordChange ~~ optionalStatus) ~~>> ((userName, initialPassword, requirePasswordChange, suspended) =>
      ast.AlterUser(userName, None, Some(initialPassword), requirePasswordChange, suspended)) |
    //
    // ALTER USER username setRequirePasswordChange optionalStatus
    group(keyword("ALTER USER") ~~ SymbolicNameString ~~ setRequirePasswordChange ~~ optionalStatus) ~~>>
      ((userName, requirePasswordChange, suspended) => ast.AlterUser(userName, None, None, Some(requirePasswordChange), suspended)) |
    //
    // ALTER USER username setStatus
    group(keyword("ALTER USER") ~~ SymbolicNameString ~~ setStatus) ~~>>
      ((userName, suspended) => ast.AlterUser(userName, None, None, None, Some(suspended)))
  }

  def SetOwnPassword: Rule1[SetOwnPassword] = rule("CATALOG ALTER CURRENT USER SET PASSWORD") {
    // ALTER CURRENT USER SET PASSWORD FROM stringLiteralPassword TO stringLiteralPassword
    group(keyword("ALTER CURRENT USER SET PASSWORD FROM") ~~ StringLiteral ~~ keyword("TO") ~~ StringLiteral) ~~>>
      ((currentPassword, newPassword) => ast.SetOwnPassword(Some(newPassword.value), None, Some(currentPassword.value), None)) |
    // ALTER CURRENT USER SET PASSWORD FROM stringLiteralPassword TO parameterPassword
    group(keyword("ALTER CURRENT USER SET PASSWORD FROM") ~~ StringLiteral ~~ keyword("TO") ~~ Parameter) ~~>>
      ((currentPassword, newPassword) => ast.SetOwnPassword(None, Some(newPassword), Some(currentPassword.value), None)) |
    // ALTER CURRENT USER SET PASSWORD FROM parameterPassword TO stringLiteralPassword
    group(keyword("ALTER CURRENT USER SET PASSWORD FROM") ~~ Parameter ~~ keyword("TO") ~~ StringLiteral) ~~>>
      ((currentPassword, newPassword) => ast.SetOwnPassword(Some(newPassword.value), None, None, Some(currentPassword))) |
    // ALTER CURRENT USER SET PASSWORD FROM parameterPassword TO parameterPassword
    group(keyword("ALTER CURRENT USER SET PASSWORD FROM") ~~ Parameter ~~ keyword("TO") ~~ Parameter) ~~>>
      ((currentPassword, newPassword) => ast.SetOwnPassword(None, Some(newPassword), None, Some(currentPassword)))
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
    group(keyword("CREATE OR REPLACE ROLE") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS") ~~
      optional(keyword("AS COPY OF") ~~ SymbolicNameString)) ~~>> (ast.CreateRole(_, _, IfExistsInvalidSyntax())) |
    group(keyword("CREATE OR REPLACE ROLE") ~~ SymbolicNameString ~~
      optional(keyword("AS COPY OF") ~~ SymbolicNameString)) ~~>> (ast.CreateRole(_, _, IfExistsReplace())) |
    group(keyword("CREATE ROLE") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS") ~~
      optional(keyword("AS COPY OF") ~~ SymbolicNameString)) ~~>> (ast.CreateRole(_, _, IfExistsDoNothing())) |
    group(keyword("CREATE ROLE") ~~ SymbolicNameString ~~
      optional(keyword("AS COPY OF") ~~ SymbolicNameString)) ~~>> (ast.CreateRole(_, _, IfExistsThrowError()))
  }

  def DropRole: Rule1[DropRole] = rule("CATALOG DROP ROLE") {
    group(keyword("DROP ROLE") ~~ SymbolicNameString ~~ keyword("IF EXISTS")) ~~>> (ast.DropRole(_, ifExists = true)) |
    group(keyword("DROP ROLE") ~~ SymbolicNameString) ~~>> (ast.DropRole(_, ifExists = false))
  }

  def GrantRole: Rule1[GrantRolesToUsers] = rule("CATALOG GRANT ROLE") {
    group(keyword("GRANT") ~~ (keyword("ROLES") | keyword("ROLE")) ~~ SymbolicNamesList ~~
      keyword("TO") ~~ SymbolicNamesList) ~~>> (ast.GrantRolesToUsers(_, _))
  }

  def RevokeRole: Rule1[RevokeRolesFromUsers] = rule("CATALOG REVOKE ROLE") {
    group(keyword("REVOKE") ~~ (keyword("ROLES") | keyword("ROLE")) ~~ SymbolicNamesList ~~
      keyword("FROM") ~~ SymbolicNamesList) ~~>> (ast.RevokeRolesFromUsers(_, _))
  }

  //`GRANT ROLE MANAGEMENT ON DBMS TO role`
  def GrantDbmsPrivilege: Rule1[GrantPrivilege] = rule("CATALOG GRANT dbms privileges") {
    group(keyword("GRANT") ~~ DbmsAction ~~ keyword("ON DBMS TO") ~~ SymbolicNamesList) ~~>>
      ((dbmsAction, grantees) => ast.GrantPrivilege.dbmsAction( dbmsAction, grantees ))
  }

  def DenyDbmsPrivilege: Rule1[DenyPrivilege] = rule("CATALOG DENY dbms privileges") {
    group(keyword("DENY") ~~ DbmsAction ~~ keyword("ON DBMS TO") ~~ SymbolicNamesList) ~~>>
      ((dbmsAction, grantees) => ast.DenyPrivilege.dbmsAction( dbmsAction, grantees ))
  }

  def RevokeDbmsPrivilege: Rule1[RevokePrivilege] = rule("CATALOG REVOKE dbms privileges") {
    group(keyword("REVOKE") ~~ DbmsAction ~~ keyword("ON DBMS FROM") ~~ SymbolicNamesList) ~~>>
      ((dbmsAction, grantees) => ast.RevokePrivilege.dbmsAction( dbmsAction, grantees ))
  }

  def RevokeGrantDbmsPrivilege: Rule1[RevokePrivilege] = rule("CATALOG REVOKE GRANT dbms privileges") {
    group(keyword("REVOKE GRANT") ~~ DbmsAction ~~ keyword("ON DBMS FROM") ~~ SymbolicNamesList) ~~>>
      ((dbmsAction, grantees) => ast.RevokePrivilege.grantedDbmsAction( dbmsAction, grantees ))
  }

  def RevokeDenyDbmsPrivilege: Rule1[RevokePrivilege] = rule("CATALOG REVOKE DENY dbms privileges") {
    group(keyword("REVOKE DENY") ~~ DbmsAction ~~ keyword("ON DBMS FROM") ~~ SymbolicNamesList) ~~>>
      ((dbmsAction, grantees) => ast.RevokePrivilege.deniedDbmsAction( dbmsAction, grantees ))
  }

  //`GRANT ACCESS/START/STOP ON DATABASE foo TO role`
  //`GRANT CREATE INDEX/DROP INDEX/INDEX MANAGEMENT ON DATABASE foo TO role`
  //`GRANT CREATE CONSTRAINT/DROP CONSTRAINT/CONSTRAINT MANAGEMENT ON DATABASE foo TO role`
  //`GRANT CREATE NEW NODE LABEL/RELATIONSHIP TYPE/PROPERTY NAME ON DATABASE foo TO role`
  def GrantDatabasePrivilege: Rule1[GrantPrivilege] = rule("CATALOG GRANT Database & Schema privileges") {
    group(keyword("GRANT") ~~ DatabaseAction ~~ Database ~~ keyword("TO") ~~ SymbolicNamesList) ~~>>
      ((databaseAction, scope, grantees) => ast.GrantPrivilege.databaseAction( databaseAction, scope, grantees))
  }

  //`DENY ACCESS/START/STOP ON DATABASE foo TO role`
  //`DENY CREATE INDEX/DROP INDEX/INDEX MANAGEMENT ON DATABASE foo TO role`
  //`DENY CREATE CONSTRAINT/DROP CONSTRAINT/CONSTRAINT MANAGEMENT ON DATABASE foo TO role`
  //`DENY CREATE NEW NODE LABEL/RELATIONSHIP TYPE/PROPERTY NAME ON DATABASE foo TO role`
  def DenyDatabasePrivilege: Rule1[DenyPrivilege] = rule("CATALOG DENY Database & Schema privileges") {
    group(keyword("DENY") ~~ DatabaseAction ~~ Database ~~ keyword("TO") ~~ SymbolicNamesList) ~~>>
      ((databaseAction, scope, grantees) => ast.DenyPrivilege.databaseAction( databaseAction, scope, grantees))
  }

  //`REVOKE GRANT ACCESS/START/STOP ON DATABASE foo FROM role`
  //`REVOKE GRANT CREATE INDEX/DROP INDEX/INDEX MANAGEMENT ON DATABASE foo TO role`
  //`REVOKE GRANT CREATE CONSTRAINT/DROP CONSTRAINT/CONSTRAINT MANAGEMENT ON DATABASE foo TO role`
  //`REVOKE GRANT CREATE NEW NODE LABEL/RELATIONSHIP TYPE/PROPERTY NAME ON DATABASE foo TO role`
  def RevokeGrantDatabasePrivilege: Rule1[RevokePrivilege] = rule("CATALOG REVOKE GRANT Database & Schema privileges") {
    group(keyword("REVOKE GRANT") ~~ DatabaseAction ~~ Database ~~ keyword("FROM") ~~ SymbolicNamesList) ~~>>
      ((databaseAction, scope, grantees) => ast.RevokePrivilege.databaseGrantedAction( databaseAction, scope, grantees))
  }

  //`REVOKE DENY ACCESS/START/STOP ON DATABASE foo FROM role`
  //`REVOKE DENY CREATE INDEX/DROP INDEX/INDEX MANAGEMENT ON DATABASE foo TO role`
  //`REVOKE DENY CREATE CONSTRAINT/DROP CONSTRAINT/CONSTRAINT MANAGEMENT ON DATABASE foo TO role`
  //`REVOKE DENY CREATE NEW NODE LABEL/RELATIONSHIP TYPE/PROPERTY NAME ON DATABASE foo TO role`
  def RevokeDenyDatabasePrivilege: Rule1[RevokePrivilege] = rule("CATALOG REVOKE DENY Database & Schema privileges") {
    group(keyword("REVOKE DENY") ~~ DatabaseAction ~~ Database ~~ keyword("FROM") ~~ SymbolicNamesList) ~~>>
      ((databaseAction, scope, grantees) => ast.RevokePrivilege.databaseDeniedAction( databaseAction, scope, grantees))
  }

  //`REVOKE ACCESS/START/STOP ON DATABASE foo FROM role`
  //`REVOKE CREATE INDEX/DROP INDEX/INDEX MANAGEMENT ON DATABASE foo TO role`
  //`REVOKE CREATE CONSTRAINT/DROP CONSTRAINT/CONSTRAINT MANAGEMENT ON DATABASE foo TO role`
  //`REVOKE CREATE NEW NODE LABEL/RELATIONSHIP TYPE/PROPERTY NAME ON DATABASE foo TO role`
  def RevokeDatabasePrivilege: Rule1[RevokePrivilege] = rule("CATALOG REVOKE Database & Schema privileges") {
    group(keyword("REVOKE") ~~ DatabaseAction ~~ Database ~~ keyword("FROM") ~~ SymbolicNamesList) ~~>>
      ((databaseAction, scope, grantees) => ast.RevokePrivilege.databaseAction( databaseAction, scope, grantees))
  }

  //`GRANT TRAVERSE ON GRAPH foo ELEMENTS A (*) TO role`
  def GrantTraverse: Rule1[GrantPrivilege] = rule("CATALOG GRANT TRAVERSE") {
    group(keyword("GRANT TRAVERSE") ~~ Graph ~~ ScopeQualifier ~~ keyword("TO") ~~ SymbolicNamesList) ~~>>
      ((scope, qualifier, grantees) => ast.GrantPrivilege.traverse(scope, qualifier, grantees))
  }

  //`DENY TRAVERSE ON GRAPH foo ELEMENTS A (*) TO role`
  def DenyTraverse: Rule1[DenyPrivilege] = rule("CATALOG DENY TRAVERSE") {
    group(keyword("DENY TRAVERSE") ~~ Graph ~~ ScopeQualifier ~~ keyword("TO") ~~ SymbolicNamesList) ~~>>
      ((scope, qualifier, grantees) => ast.DenyPrivilege.traverse(scope, qualifier, grantees))
  }

  //`REVOKE GRANT TRAVERSE ON GRAPH foo ELEMENTS A (*) FROM role`
  def RevokeGrantTraverse: Rule1[RevokePrivilege] = rule("CATALOG REVOKE GRANT TRAVERSE") {
    group(keyword("REVOKE GRANT TRAVERSE") ~~ Graph ~~ ScopeQualifier ~~ keyword("FROM") ~~ SymbolicNamesList) ~~>>
      ((scope, qualifier, grantees) => ast.RevokePrivilege.grantedTraverse(scope, qualifier, grantees))
  }

  //`REVOKE DENY TRAVERSE ON GRAPH foo ELEMENTS A (*) FROM role`
  def RevokeDenyTraverse: Rule1[RevokePrivilege] = rule("CATALOG REVOKE DENY TRAVERSE") {
    group(keyword("REVOKE DENY TRAVERSE") ~~ Graph ~~ ScopeQualifier ~~ keyword("FROM") ~~ SymbolicNamesList) ~~>>
      ((scope, qualifier, grantees) => ast.RevokePrivilege.deniedTraverse(scope, qualifier, grantees))
  }

  //`REVOKE TRAVERSE ON GRAPH foo ELEMENTS A (*) FROM role`
  def RevokeTraverse: Rule1[RevokePrivilege] = rule("CATALOG REVOKE TRAVERSE") {
    group(keyword("REVOKE TRAVERSE") ~~ Graph ~~ ScopeQualifier ~~ keyword("FROM") ~~ SymbolicNamesList) ~~>>
      ((scope, qualifier, grantees) => ast.RevokePrivilege.traverse(scope, qualifier, grantees))
  }

  //`GRANT READ {a} ON GRAPH foo ELEMENTS A (*) TO role`
  def GrantRead: Rule1[GrantPrivilege] = rule("CATALOG GRANT READ") {
    group(keyword("GRANT READ") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifier ~~ keyword("TO") ~~ SymbolicNamesList) ~~>>
      ((prop, scope, qualifier, grantees) => ast.GrantPrivilege.read(prop, scope, qualifier, grantees))
  }

  //`DENY READ {a} ON GRAPH foo ELEMENTS A (*) TO role`
  def DenyRead: Rule1[DenyPrivilege] = rule("CATALOG DENY READ") {
    group(keyword("DENY READ") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifier ~~ keyword("TO") ~~ SymbolicNamesList) ~~>>
      ((prop, scope, qualifier, grantees) => ast.DenyPrivilege.read(prop, scope, qualifier, grantees))
  }

  //`REVOKE GRANT READ {a} ON GRAPH foo ELEMENTS A (*) FROM role`
  def RevokeGrantRead: Rule1[RevokePrivilege] = rule("CATALOG REVOKE GRANT READ") {
    group(keyword("REVOKE GRANT READ") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifier ~~ keyword("FROM") ~~ SymbolicNamesList) ~~>>
      ((prop, scope, qualifier, grantees) => ast.RevokePrivilege.grantedRead(prop, scope, qualifier, grantees))
  }

  //`REVOKE DENY READ {a} ON GRAPH foo ELEMENTS A (*) FROM role`
  def RevokeDenyRead: Rule1[RevokePrivilege] = rule("CATALOG REVOKE DENY READ") {
    group(keyword("REVOKE DENY READ") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifier ~~ keyword("FROM") ~~ SymbolicNamesList) ~~>>
      ((prop, scope, qualifier, grantees) => ast.RevokePrivilege.deniedRead(prop, scope, qualifier, grantees))
  }

  //`REVOKE READ {a} ON GRAPH foo ELEMENTS A (*) FROM role`
  def RevokeRead: Rule1[RevokePrivilege] = rule("CATALOG REVOKE READ") {
    group(keyword("REVOKE READ") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifier ~~ keyword("FROM") ~~ SymbolicNamesList) ~~>>
      ((prop, scope, qualifier, grantees) => ast.RevokePrivilege.read(prop, scope, qualifier, grantees))
  }

  //`GRANT MATCH {a} ON GRAPH foo ELEMENTS A (*) TO role`
  def GrantMatch: Rule1[GrantPrivilege] = rule("CATALOG GRANT MATCH") {
    group(keyword("GRANT MATCH") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifier ~~ keyword("TO") ~~ SymbolicNamesList) ~~>>
      ((prop, scope, qualifier, grantees) => ast.GrantPrivilege.asMatch(prop, scope, qualifier, grantees))
  }

  //`DENY MATCH {a} ON GRAPH foo ELEMENTS A (*) TO role`
  def DenyMatch: Rule1[DenyPrivilege] = rule("CATALOG DENY MATCH") {
    group(keyword("DENY MATCH") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifier ~~ keyword("TO") ~~ SymbolicNamesList) ~~>>
      ((prop, scope, qualifier, grantees) => ast.DenyPrivilege.asMatch(prop, scope, qualifier, grantees))
  }

  //`REVOKE GRANT MATCH {a} ON GRAPH foo ELEMENTS A (*) FROM role`
  def RevokeGrantMatch: Rule1[RevokePrivilege] = rule("CATALOG REVOKE GRANT MATCH") {
    group(keyword("REVOKE GRANT MATCH") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifier ~~ keyword("FROM") ~~ SymbolicNamesList) ~~>>
      ((prop, scope, qualifier, grantees) => ast.RevokePrivilege.grantedAsMatch(prop, scope, qualifier, grantees))
  }

  //`REVOKE DENY MATCH {a} ON GRAPH foo ELEMENTS A (*) FROM role`
  def RevokeDenyMatch: Rule1[RevokePrivilege] = rule("CATALOG REVOKE DENY MATCH") {
    group(keyword("REVOKE DENY MATCH") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifier ~~ keyword("FROM") ~~ SymbolicNamesList) ~~>>
      ((prop, scope, qualifier, grantees) => ast.RevokePrivilege.deniedAsMatch(prop, scope, qualifier, grantees))
  }

  //`REVOKE MATCH {a} ON GRAPH foo ELEMENTS A (*) FROM role`
  def RevokeMatch: Rule1[RevokePrivilege] = rule("CATALOG REVOKE MATCH") {
    group(keyword("REVOKE MATCH") ~~ PrivilegeProperty ~~ Graph ~~ ScopeQualifier ~~ keyword("FROM") ~~ SymbolicNamesList) ~~>>
      ((prop, scope, qualifier, grantees) => ast.RevokePrivilege.asMatch(prop, scope, qualifier, grantees))
  }

  //`GRANT WRITE ON GRAPH foo * (*) TO role`
  def GrantWrite: Rule1[GrantPrivilege] = rule("CATALOG GRANT WRITE") {
    group(keyword("GRANT WRITE") ~~ Graph ~~ ScopeQualifier ~~ keyword("TO") ~~ SymbolicNamesList) ~~>>
      ((scope, qualifier, grantees) => ast.GrantPrivilege.write(scope, qualifier, grantees))
  }

  //`DENY WRITE ON GRAPH foo * (*) TO role`
  def DenyWrite: Rule1[DenyPrivilege] = rule("CATALOG DENY WRITE") {
    group(keyword("DENY WRITE") ~~ Graph ~~ ScopeQualifier ~~ keyword("TO") ~~ SymbolicNamesList) ~~>>
      ((scope, qualifier, grantees) => ast.DenyPrivilege.write(scope, qualifier, grantees))
  }

  //`REVOKE GRANT WRITE ON GRAPH foo * (*) FROM role`
  def RevokeGrantWrite: Rule1[RevokePrivilege] = rule("CATALOG REVOKE GRANT WRITE") {
    group(keyword("REVOKE GRANT WRITE") ~~ Graph ~~ ScopeQualifier ~~ keyword("FROM") ~~ SymbolicNamesList) ~~>>
      ((scope, qualifier, grantees) => ast.RevokePrivilege.grantedWrite(scope, qualifier, grantees))
  }

  //`REVOKE DENY WRITE ON GRAPH foo * (*) FROM role`
  def RevokeDenyWrite: Rule1[RevokePrivilege] = rule("CATALOG REVOKE DENY WRITE") {
    group(keyword("REVOKE DENY WRITE") ~~ Graph ~~ ScopeQualifier ~~ keyword("FROM") ~~ SymbolicNamesList) ~~>>
      ((scope, qualifier, grantees) => ast.RevokePrivilege.deniedWrite(scope, qualifier, grantees))
  }

  //`REVOKE WRITE ON GRAPH foo * (*) FROM role`
  def RevokeWrite: Rule1[RevokePrivilege] = rule("CATALOG REVOKE WRITE") {
    group(keyword("REVOKE WRITE") ~~ Graph ~~ ScopeQualifier ~~ keyword("FROM") ~~ SymbolicNamesList) ~~>>
      ((scope, qualifier, grantees) => ast.RevokePrivilege.write(scope, qualifier, grantees))
  }

  def ShowPrivileges: Rule1[ShowPrivileges] = rule("CATALOG SHOW PRIVILEGES") {
    group(keyword("SHOW") ~~ ScopeForShowPrivileges ~~ keyword("PRIVILEGES")) ~~>> (ast.ShowPrivileges(_))
  }

  private def PrivilegeProperty: Rule1[ActionResource] = rule("a property")(
    group("{" ~~ SymbolicNamesList ~~ "}") ~~>> {ast.PropertiesResource(_)} |
      group("{" ~~ "*" ~~ "}") ~~~> {ast.AllResource()}
  )

  private def ScopeQualifier: Rule1[PrivilegeQualifier] = rule("which element type and associated labels/relTypes (props) qualifier combination")(
    group(RelationshipKeyword ~~ SymbolicNamesList ~~ optional("(" ~~ "*" ~~ ")")) ~~>> {ast.RelationshipsQualifier(_)} |
    group(RelationshipKeyword ~~ "*" ~~ optional("(" ~~ "*" ~~ ")")) ~~~> {ast.RelationshipAllQualifier()} |
    group(NodeKeyword ~~ SymbolicNamesList ~~ optional("(" ~~ "*" ~~ ")")) ~~>> {ast.LabelsQualifier(_)} |
    group(NodeKeyword ~~ "*" ~~ optional("(" ~~ "*" ~~ ")")) ~~~> {ast.LabelAllQualifier()} |
    group(ElementKeyword ~~ SymbolicNamesList ~~ optional("(" ~~ "*" ~~ ")")) ~~>> {ast.ElementsQualifier(_)} |
    optional(ElementKeyword ~~ "*" ~~ optional("(" ~~ "*" ~~ ")")) ~~~> {ast.AllQualifier()}
  )

  private def ElementKeyword: Rule0 = keyword("ELEMENTS") | keyword("ELEMENT")

  private def RelationshipKeyword: Rule0 = keyword("RELATIONSHIPS") | keyword("RELATIONSHIP")

  private def NodeKeyword: Rule0 = keyword("NODE") | keyword("NODES")

  private def Database: Rule1[GraphScope] = rule("on a database")(
    group(keyword("ON") ~~ (keyword("DATABASE") | keyword("DATABASES"))) ~~
      (group(SymbolicNameString) ~~>> (ast.NamedGraphScope(_)) |
        keyword("*") ~~~> ast.AllGraphsScope())
  )

  private def DatabaseAction: Rule1[DatabaseAction] = rule("access/start/stop a database and index, constraint and token management")(
    keyword("ACCESS") ~~~> (_ => ast.AccessDatabaseAction) |
      keyword("START") ~~~> (_ => ast.StartDatabaseAction) |
      keyword("STOP") ~~~> (_ => ast.StopDatabaseAction) |
      group(keyword("CREATE") ~~ IndexKeyword) ~~~> (_ => ast.CreateIndexAction) |
      group(keyword("DROP") ~~ IndexKeyword) ~~~> (_ => ast.DropIndexAction) |
      group(IndexKeyword ~~ optional(keyword("MANAGEMENT"))) ~~~> (_ => ast.IndexManagementAction) |
      group(keyword("CREATE") ~~ ConstraintKeyword) ~~~> (_ => ast.CreateConstraintAction) |
      group(keyword("DROP") ~~ ConstraintKeyword) ~~~> (_ => ast.DropConstraintAction) |
      group(ConstraintKeyword ~~ optional(keyword("MANAGEMENT"))) ~~~> (_ => ast.ConstraintManagementAction) |
      group(keyword("CREATE NEW") ~~ optional(keyword("NODE")) ~~ LabelKeyword) ~~~> (_ => ast.CreateNodeLabelAction) |
      group(keyword("CREATE NEW") ~~ optional(keyword("RELATIONSHIP")) ~~ TypeKeyword) ~~~> (_ => ast.CreateRelationshipTypeAction) |
      group(keyword("CREATE NEW") ~~ optional(keyword("PROPERTY")) ~~ NameKeyword) ~~~> (_ => ast.CreatePropertyKeyAction) |
      group(keyword("NAME") ~~ optional(keyword("MANAGEMENT"))) ~~~> (_ => ast.TokenManagementAction) |
      group(keyword("ALL") ~~ optional(optional(keyword("DATABASE")) ~~ keyword("PRIVILEGES"))) ~~~> (_ => ast.AllDatabaseAction)
  )

  private def DbmsAction: Rule1[AdminAction] = rule("dbms action") {
    keyword("ROLE MANAGEMENT") ~~~> (_ => ast.AllRoleActions) |
    keyword("CREATE ROLE") ~~~> (_ => ast.CreateRoleAction) |
    keyword("DROP ROLE") ~~~> (_ => ast.DropRoleAction) |
    keyword("ASSIGN ROLE") ~~~> (_ => ast.AssignRoleAction) |
    keyword("REMOVE ROLE") ~~~> (_ => ast.RemoveRoleAction) |
    keyword("SHOW ROLE") ~~~> (_ => ast.ShowRoleAction)
  }

  private def IndexKeyword: Rule0 = keyword("INDEXES") | keyword("INDEX")

  private def ConstraintKeyword: Rule0 = keyword("CONSTRAINTS") | keyword("CONSTRAINT")

  private def LabelKeyword: Rule0 = keyword("LABELS") | keyword("LABEL")

  private def TypeKeyword: Rule0 = keyword("TYPES") | keyword("TYPE")

  private def NameKeyword: Rule0 = keyword("NAMES") | keyword("NAME")

  private def Graph: Rule1[GraphScope] = rule("on a database/graph")(
    group(keyword("ON") ~~ (keyword("GRAPH") | keyword("GRAPHS"))) ~~
      (group(SymbolicNameString) ~~>> (ast.NamedGraphScope(_)) |
        keyword("*") ~~~> ast.AllGraphsScope())
  )

  private def ScopeForShowPrivileges: Rule1[ShowPrivilegeScope] = rule("a database/graph")(
    group(keyword("ROLE") ~~ SymbolicNameString) ~~>> (ast.ShowRolePrivileges(_)) |
      group(keyword("USER") ~~ SymbolicNameString) ~~>> (ast.ShowUserPrivileges(_)) |
      optional(keyword("ALL")) ~~~> (ast.ShowAllPrivileges())
  )

  def ShowDatabase: Rule1[ShowDatabase] = rule("CATALOG SHOW DATABASE") {
    group(keyword("SHOW DATABASE") ~~ SymbolicNameString) ~~>> (ast.ShowDatabase(_))
  }

  def ShowDatabases: Rule1[ShowDatabases] = rule("CATALOG SHOW DATABASES") {
    keyword("SHOW DATABASES") ~>>> (_=> ast.ShowDatabases())
  }

  def ShowDefaultDatabase: Rule1[ShowDefaultDatabase] = rule("CATALOG SHOW DEFAULT DATABASE") {
    keyword("SHOW DEFAULT DATABASE") ~>>> (_=> ast.ShowDefaultDatabase())
  }

  def CreateDatabase: Rule1[CreateDatabase] = rule("CATALOG CREATE DATABASE") {
    group(keyword("CREATE OR REPLACE DATABASE") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS")) ~~>> (ast.CreateDatabase(_, IfExistsInvalidSyntax())) |
    group(keyword("CREATE OR REPLACE DATABASE") ~~ SymbolicNameString) ~~>> (ast.CreateDatabase(_, IfExistsReplace())) |
    group(keyword("CREATE DATABASE") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS")) ~~>> (ast.CreateDatabase(_, IfExistsDoNothing())) |
    group(keyword("CREATE DATABASE") ~~ SymbolicNameString) ~~>> (ast.CreateDatabase(_, IfExistsThrowError()))
  }

  def DropDatabase: Rule1[DropDatabase] = rule("CATALOG DROP DATABASE") {
    group(keyword("DROP DATABASE") ~~ SymbolicNameString ~~ keyword("IF EXISTS")) ~~>> (ast.DropDatabase(_, ifExists = true)) |
    group(keyword("DROP DATABASE") ~~ SymbolicNameString) ~~>> (ast.DropDatabase(_, ifExists = false))
  }

  def StartDatabase: Rule1[StartDatabase] = rule("CATALOG START DATABASE") {
    group(keyword("START DATABASE") ~~ SymbolicNameString) ~~>> (ast.StartDatabase(_))
  }

  def StopDatabase: Rule1[StopDatabase] = rule("CATALOG STOP DATABASE") {
    group(keyword("STOP DATABASE") ~~ SymbolicNameString) ~~>> (ast.StopDatabase(_))
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
}
