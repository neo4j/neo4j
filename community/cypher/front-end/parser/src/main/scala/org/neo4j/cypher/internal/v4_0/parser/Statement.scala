/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
    MultiDatabaseCommand | CatalogCommand | UserManagementCommand | PrivilegeManagementCommand | Command | Query
  )

  def CatalogCommand: Rule1[CatalogDDL] = rule("Catalog DDL statement") {
    CreateGraph | DropGraph | CreateView | DropView
  }

  def MultiDatabaseCommand: Rule1[CatalogDDL] = rule("MultiDatabase DDL statement") {
    optional(keyword("CATALOG")) ~~ (ShowDatabase | ShowDatabases | CreateDatabase | DropDatabase | StartDatabase | StopDatabase)
  }

  def UserManagementCommand: Rule1[CatalogDDL] = rule("Security user management statement") {
    optional(keyword("CATALOG")) ~~ (ShowRoles | CreateRole | DropRole | ShowUsers | CreateUser | DropUser | AlterUser)
  }

  def PrivilegeManagementCommand: Rule1[CatalogDDL] = rule("Security privilege management statement") {
    optional(keyword("CATALOG")) ~~ (ShowPrivileges | GrantRole | GrantTraverse)
  }

  def ShowUsers: Rule1[ShowUsers] = rule("CATALOG SHOW USERS") {
    keyword("SHOW USERS") ~>>> (_=> ast.ShowUsers())
  }

  def CreateUser: Rule1[CreateUser] = rule("CATALOG CREATE USER") {
    // CREATE USER username SET PASSWORD stringLiteralPassword optionalStatus
    group(keyword("CREATE USER") ~~ SymbolicNameString ~~ keyword("SET PASSWORD") ~~ StringLiteral ~~
    optionalStatus) ~~>> ((userName, initialPassword, suspended) =>
      ast.CreateUser(userName, Some(initialPassword.value), None, requirePasswordChange = true, suspended.getOrElse(false))) |
    // CREATE USER username SET PASSWORD stringLiteralPassword optionalRequirePasswordChange optionalStatus
    group(keyword("CREATE USER") ~~ SymbolicNameString ~~ keyword("SET PASSWORD") ~~ StringLiteral ~~
    optionalRequirePasswordChange ~~ optionalStatus) ~~>> ((userName, initialPassword, requirePasswordChange, suspended) =>
      ast.CreateUser(userName, Some(initialPassword.value), None, requirePasswordChange.getOrElse(true), suspended.getOrElse(false))) |
    //
    // CREATE USER username SET PASSWORD parameterPassword optionalStatus
    group(keyword("CREATE USER") ~~ SymbolicNameString ~~ keyword("SET PASSWORD") ~~ Parameter ~~
    optionalStatus) ~~>> ((userName, initialPassword, suspended) =>
      ast.CreateUser(userName, None, Some(initialPassword), requirePasswordChange = true, suspended.getOrElse(false))) |
    // CREATE USER username SET PASSWORD parameterPassword optionalRequirePasswordChange optionalStatus
    group(keyword("CREATE USER") ~~ SymbolicNameString ~~ keyword("SET PASSWORD") ~~ Parameter ~~
    optionalRequirePasswordChange ~~ optionalStatus) ~~>> ((userName, initialPassword, requirePasswordChange, suspended) =>
      ast.CreateUser(userName, None, Some(initialPassword), requirePasswordChange.getOrElse(true), suspended.getOrElse(false)))
  }

  def DropUser: Rule1[DropUser] = rule("CATALOG DROP USER") {
    group(keyword("DROP USER") ~~ SymbolicNameString) ~~>> (ast.DropUser(_))
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
    group(keyword("CREATE ROLE") ~~ SymbolicNameString ~~
      optional(keyword("AS COPY OF") ~~ SymbolicNameString)) ~~>> (ast.CreateRole(_, _))
  }

  def DropRole: Rule1[DropRole] = rule("CATALOG DROP ROLE") {
    group(keyword("DROP ROLE") ~~ SymbolicNameString) ~~>> (ast.DropRole(_))
  }

  def GrantRole: Rule1[GrantRolesToUsers] = rule("CATALOG GRANT ROLE") {
    group(keyword("GRANT") ~~ (keyword("ROLES") | keyword("ROLE")) ~~ SymbolicNamesList ~~
      keyword("TO") ~~ SymbolicNamesList) ~~>> (ast.GrantRolesToUsers(_, _))
  }

  //`GRANT TRAVERSE ON DATABASE foo GRAPH NODES A (*) TO role1`
  def GrantTraverse: Rule1[GrantTraverse] = rule("CATALOG GRANT TRAVERSE") {
    group(keyword("GRANT TRAVERSE ON GRAPH * NODES * (*) TO") ~~ SymbolicNameString) ~~>> (ast.GrantTraverse(ast.AllGraphsScope(), ast.AllQualifier(), _)) |
      group(keyword("GRANT TRAVERSE ON GRAPH * NODES") ~~ SymbolicNameString ~~ keyword("(*) TO") ~~ SymbolicNameString) ~~>>
        ((label, role) => ast.GrantTraverse(ast.AllGraphsScope(), ast.LabelQualifier(label), role)) |
      group(keyword("GRANT TRAVERSE ON GRAPH") ~~ SymbolicNameString ~~ keyword("NODES") ~~ keyword("* (*) TO") ~~ SymbolicNameString) ~~>>
        ((db, role) => ast.GrantTraverse(ast.NamedGraphScope(db), ast.AllQualifier(), role)) |
      group(keyword("GRANT TRAVERSE ON GRAPH") ~~ SymbolicNameString ~~ keyword("NODES") ~~ SymbolicNameString ~~ keyword("(*) TO") ~~
        SymbolicNameString) ~~>> ((db, label, role) => ast.GrantTraverse(ast.NamedGraphScope(db), ast.LabelQualifier(label), role))
  }

  def ShowPrivileges: Rule1[ShowPrivileges] = rule("CATALOG SHOW PRIVILEGES") {
    //SHOW ALL PRIVILEGES
    group(keyword("SHOW") ~~ keyword("ALL") ~~ keyword("PRIVILEGES")) ~>>> (_ => ast.ShowPrivileges("ALL", "")) |
      //SHOW USER joe PRIVILEGES
      group(keyword("SHOW") ~~ keyword("USER") ~~ SymbolicNameString ~~ keyword("PRIVILEGES")) ~~>> (ast.ShowPrivileges("USER", _)) |
      //SHOW ROLE role1 PRIVILEGES
      group(keyword("SHOW") ~~ keyword("ROLE") ~~ SymbolicNameString ~~ keyword("PRIVILEGES")) ~~>> (ast.ShowPrivileges("ROLE", _))
  }

  def ShowDatabase: Rule1[ShowDatabase] = rule("CATALOG SHOW DATABASE") {
    group(keyword("SHOW DATABASE") ~~ SymbolicNameString) ~~>> (ast.ShowDatabase(_))
  }

  def ShowDatabases: Rule1[ShowDatabases] = rule("CATALOG SHOW DATABASES") {
    keyword("SHOW DATABASES") ~>>> (_=> ast.ShowDatabases())
  }

  def CreateDatabase: Rule1[CreateDatabase] = rule("CATALOG CREATE DATABASE") {
    group(keyword("CREATE DATABASE") ~~ SymbolicNameString) ~~>> (ast.CreateDatabase(_))
  }

  def DropDatabase: Rule1[DropDatabase] = rule("CATALOG DROP DATABASE") {
    group(keyword("DROP DATABASE") ~~ SymbolicNameString) ~~>> (ast.DropDatabase(_))
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
