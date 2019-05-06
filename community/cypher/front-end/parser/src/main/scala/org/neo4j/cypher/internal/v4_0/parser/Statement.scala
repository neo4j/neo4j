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
    MultiDatabaseCommand | CatalogCommand | SecurityCommand | Command | Query
  )

  def CatalogCommand: Rule1[CatalogDDL] = rule("Catalog DDL statement") {
    CreateGraph | DropGraph | CreateView | DropView
  }

  def MultiDatabaseCommand: Rule1[CatalogDDL] = rule("Catalog DDL statement") {
    optional(keyword("CATALOG")) ~~ (ShowDatabase | ShowDatabases | CreateDatabase | DropDatabase | StartDatabase | StopDatabase)
  }

  def SecurityCommand: Rule1[CatalogDDL] = rule("Security DDL statement") {
    optional(keyword("CATALOG")) ~~ (ShowRoles | CreateRole | DropRole | ShowUsers | CreateUser | DropUser | AlterUser)
  }

  def ShowUsers: Rule1[ShowUsers] = rule("CATALOG SHOW USERS") {
    keyword("SHOW USERS") ~>>> (_=> ast.ShowUsers())
  }

  def CreateUser: Rule1[CreateUser] = rule("CATALOG CREATE USER") {
    CreateUserWithStringPassword | CreateUserWithParameterPassword
  }

  def CreateUserWithStringPassword: Rule1[CreateUser] = rule("CATALOG CREATE USER") {
    // CREATE USER username SET PASSWORD stringLiteralPassword [ SET PASSWORD ] CHANGE NOT REQUIRED SET STATUS SUSPENDED
    group(keyword("CREATE USER") ~~ UserNameString ~~ keyword("SET PASSWORD") ~~ StringLiteral ~~
    optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE NOT REQUIRED")) ~~
    group(keyword("SET STATUS") ~~ keyword("SUSPENDED")) ~~>> ((userName, initialPassword) =>
      ast.CreateUser(userName, Some(initialPassword.value), None, requirePasswordChange = false, suspended = true)) |
    //
    // CREATE USER username SET PASSWORD stringLiteralPassword [ SET PASSWORD ] CHANGE NOT REQUIRED [ SET STATUS ACTIVE ]
    group(keyword("CREATE USER") ~~ UserNameString ~~ keyword("SET PASSWORD") ~~ StringLiteral ~~
    optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE NOT REQUIRED")) ~~
    optional(group(keyword("SET STATUS") ~~ keyword("ACTIVE"))) ~~>> ((userName, initialPassword) =>
      ast.CreateUser(userName, Some(initialPassword.value), None, requirePasswordChange = false, suspended = false)) |
    //
    // CREATE USER username SET PASSWORD stringLiteralPassword [ [ SET PASSWORD ] CHANGE REQUIRED ] SET STATUS SUSPENDED
    group(keyword("CREATE USER") ~~ UserNameString ~~ keyword("SET PASSWORD") ~~ StringLiteral ~~
    optional(optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE REQUIRED"))) ~~
    group(keyword("SET STATUS") ~~ keyword("SUSPENDED")) ~~>> ((userName, initialPassword) =>
      ast.CreateUser(userName, Some(initialPassword.value), None, requirePasswordChange = true, suspended = true)) |
    //
    // CREATE USER username SET PASSWORD stringLiteralPassword [ [ SET PASSWORD ] CHANGE REQUIRED ] [ SET STATUS ACTIVE ]
    group(keyword("CREATE USER") ~~ UserNameString ~~ keyword("SET PASSWORD") ~~ StringLiteral ~~
    optional(optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE REQUIRED"))) ~~
    optional(group(keyword("SET STATUS") ~~ keyword("ACTIVE"))) ~~>> ((userName, initialPassword) =>
      ast.CreateUser(userName, Some(initialPassword.value), None, requirePasswordChange = true, suspended = false))
  }

  def CreateUserWithParameterPassword: Rule1[CreateUser] = rule("CATALOG CREATE USER") {
    // CREATE USER username SET PASSWORD parameterPassword [ SET PASSWORD ] CHANGE NOT REQUIRED SET STATUS SUSPENDED
    group(keyword("CREATE USER") ~~ UserNameString ~~ keyword("SET PASSWORD") ~~ Parameter ~~
    optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE NOT REQUIRED")) ~~
    group(keyword("SET STATUS") ~~ keyword("SUSPENDED")) ~~>> ((userName, initialPassword) =>
      ast.CreateUser(userName, None, Some(initialPassword), requirePasswordChange = false, suspended = true)) |
    //
    // CREATE USER username SET PASSWORD parameterPassword [ SET PASSWORD ] CHANGE NOT REQUIRED [ SET STATUS ACTIVE ]
    group(keyword("CREATE USER") ~~ UserNameString ~~ keyword("SET PASSWORD") ~~ Parameter ~~
    optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE NOT REQUIRED")) ~~
    optional(group(keyword("SET STATUS") ~~ keyword("ACTIVE"))) ~~>> ((userName, initialPassword) =>
      ast.CreateUser(userName, None, Some(initialPassword), requirePasswordChange = false, suspended = false)) |
    //
    // CREATE USER username SET PASSWORD parameterPassword [ [ SET PASSWORD ] CHANGE REQUIRED ] SET STATUS SUSPENDED
    group(keyword("CREATE USER") ~~ UserNameString ~~ keyword("SET PASSWORD") ~~ Parameter ~~
    optional(optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE REQUIRED"))) ~~
    group(keyword("SET STATUS") ~~ keyword("SUSPENDED")) ~~>> ((userName, initialPassword) =>
      ast.CreateUser(userName, None, Some(initialPassword), requirePasswordChange = true, suspended = true)) |
    //
    // CREATE USER username SET PASSWORD parameterPassword [ [ SET PASSWORD ] CHANGE REQUIRED ] [ SET STATUS ACTIVE ]
    group(keyword("CREATE USER") ~~ UserNameString ~~ keyword("SET PASSWORD") ~~ Parameter ~~
    optional(optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE REQUIRED"))) ~~
    optional(group(keyword("SET STATUS") ~~ keyword("ACTIVE"))) ~~>> ((userName, initialPassword) =>
      ast.CreateUser(userName, None, Some(initialPassword), requirePasswordChange = true, suspended = false))
  }

  def DropUser: Rule1[DropUser] = rule("CATALOG DROP USER") {
    group(keyword("DROP USER") ~~ UserNameString) ~~>> (ast.DropUser(_))
  }

  def AlterUser: Rule1[AlterUser] = rule("CATALOG ALTER USER") {
    AlterUserWithStringPassword | AlterUserWithParameterPassword | AlterUserPasswordMode | AlterUserStatus
  }

  def AlterUserWithStringPassword: Rule1[AlterUser] = rule("CATALOG ALTER USER") {
    // ALTER USER username SET PASSWORD stringLiteralPassword [ SET PASSWORD ] CHANGE NOT REQUIRED SET STATUS SUSPENDED
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~ keyword("PASSWORD") ~~ StringLiteral) ~~
    optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE NOT REQUIRED") ~~ keyword("SET STATUS SUSPENDED") ~~>>
      ((userName, initialPassword) => ast.AlterUser(userName, Some(initialPassword.value), None,
        requirePasswordChange = Some(false), suspended = Some(true))) |
    //
    // ALTER USER username SET PASSWORD stringLiteralPassword [ SET PASSWORD ] CHANGE REQUIRED SET STATUS SUSPENDED
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~ keyword("PASSWORD") ~~ StringLiteral) ~~
    optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE REQUIRED") ~~ keyword("SET STATUS SUSPENDED") ~~>>
      ((userName, initialPassword) => ast.AlterUser(userName, Some(initialPassword.value), None,
        requirePasswordChange = Some(true), suspended = Some(true))) |
    //
    // ALTER USER username SET PASSWORD stringLiteralPassword SET STATUS SUSPENDED
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~ keyword("PASSWORD") ~~ StringLiteral) ~~
    keyword("SET STATUS SUSPENDED") ~~>>
      ((userName, initialPassword) => ast.AlterUser(userName, Some(initialPassword.value), None, None, suspended = Some(true))) |
    //
    //
    // ALTER USER username SET PASSWORD stringLiteralPassword SET [ SET PASSWORD ] CHANGE NOT REQUIRED STATUS ACTIVE
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~ keyword("PASSWORD") ~~ StringLiteral) ~~
    optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE NOT REQUIRED") ~~ keyword("SET STATUS ACTIVE") ~~>>
      ((userName, initialPassword) => ast.AlterUser(userName, Some(initialPassword.value), None,
        requirePasswordChange = Some(false), suspended = Some(false))) |
    //
    // ALTER USER username SET PASSWORD stringLiteralPassword [ SET PASSWORD ] CHANGE REQUIRED SET STATUS ACTIVE
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~ keyword("PASSWORD") ~~ StringLiteral) ~~
    optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE REQUIRED") ~~ keyword("SET STATUS ACTIVE") ~~>>
      ((userName, initialPassword) => ast.AlterUser(userName, Some(initialPassword.value), None,
        requirePasswordChange = Some(true), suspended = Some(false))) |
    //
    // ALTER USER username SET PASSWORD stringLiteralPassword SET STATUS ACTIVE
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~ keyword("PASSWORD") ~~ StringLiteral) ~~
    keyword("SET STATUS ACTIVE") ~~>>
      ((userName, initialPassword) => ast.AlterUser(userName, Some(initialPassword.value), None, None, suspended = Some(false))) |
    //
    //
    // ALTER USER username SET PASSWORD stringLiteralPassword [ SET PASSWORD ] CHANGE NOT REQUIRED
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~ keyword("PASSWORD") ~~ StringLiteral) ~~
    optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE NOT REQUIRED") ~~>>
      ((userName, initialPassword) => ast.AlterUser(userName, Some(initialPassword.value), None, requirePasswordChange = Some(false), None)) |
    //
    // ALTER USER username SET PASSWORD stringLiteralPassword [ SET PASSWORD ] CHANGE REQUIRED
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~ keyword("PASSWORD") ~~ StringLiteral) ~~
    optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE REQUIRED") ~~>>
      ((userName, initialPassword) => ast.AlterUser(userName, Some(initialPassword.value), None, requirePasswordChange = Some(true), None)) |
    //
    // ALTER USER username SET PASSWORD stringLiteralPassword
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~ keyword("PASSWORD") ~~ StringLiteral) ~~>>
      ((userName, initialPassword) => ast.AlterUser(userName, Some(initialPassword.value), None, None, None))
  }

  def AlterUserWithParameterPassword: Rule1[AlterUser] = rule("CATALOG ALTER USER") {
    // ALTER USER username SET PASSWORD parameterPassword SET STATUS SUSPENDED [ SET PASSWORD ] CHANGE NOT REQUIRED
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~ keyword("PASSWORD") ~~ Parameter) ~~
    optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE NOT REQUIRED") ~~ keyword("SET STATUS SUSPENDED") ~~>>
      ((userName, initialPassword) => ast.AlterUser(userName, None, Some(initialPassword),
        requirePasswordChange = Some(false), suspended = Some(true))) |
    //
    // ALTER USER username SET PASSWORD parameterPassword SET STATUS SUSPENDED [ SET PASSWORD ] CHANGE REQUIRED
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~ keyword("PASSWORD") ~~ Parameter) ~~
    optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE REQUIRED") ~~ keyword("SET STATUS SUSPENDED") ~~>>
      ((userName, initialPassword) => ast.AlterUser(userName, None, Some(initialPassword),
        requirePasswordChange = Some(true), suspended = Some(true))) |
    //
    // ALTER USER username SET PASSWORD parameterPassword SET STATUS SUSPENDED
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~ keyword("PASSWORD") ~~ Parameter) ~~
    keyword("SET STATUS SUSPENDED") ~~>>
      ((userName, initialPassword) => ast.AlterUser(userName, None, Some(initialPassword), None, suspended = Some(true))) |
    //
    //
    // ALTER USER username SET PASSWORD parameterPassword SET STATUS ACTIVE [ SET PASSWORD ] CHANGE NOT REQUIRED
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~ keyword("PASSWORD") ~~ Parameter) ~~
    optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE NOT REQUIRED") ~~ keyword("SET STATUS ACTIVE") ~~>>
      ((userName, initialPassword) => ast.AlterUser(userName, None, Some(initialPassword),
        requirePasswordChange = Some(false), suspended = Some(false))) |
    //
    // ALTER USER username SET PASSWORD parameterPassword SET STATUS ACTIVE [ SET PASSWORD ] CHANGE REQUIRED
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~ keyword("PASSWORD") ~~ Parameter) ~~
    optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE REQUIRED") ~~ keyword("SET STATUS ACTIVE") ~~>>
      ((userName, initialPassword) => ast.AlterUser(userName, None, Some(initialPassword),
        requirePasswordChange = Some(true), suspended = Some(false))) |
    //
    // ALTER USER username SET PASSWORD parameterPassword SET STATUS ACTIVE
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~ keyword("PASSWORD") ~~ Parameter) ~~
      keyword("SET STATUS ACTIVE") ~~>>
      ((userName, initialPassword) => ast.AlterUser(userName, None, Some(initialPassword), None, suspended = Some(false))) |
    //
    //
    // ALTER USER username SET PASSWORD parameterPassword [ SET PASSWORD ] CHANGE NOT REQUIRED
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~ keyword("PASSWORD") ~~ Parameter) ~~
    optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE NOT REQUIRED") ~~>>
      ((userName, initialPassword) => ast.AlterUser(userName, None, Some(initialPassword), requirePasswordChange = Some(false), None)) |
    //
    // ALTER USER username SET PASSWORD parameterPassword [ SET PASSWORD ] CHANGE REQUIRED
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~ keyword("PASSWORD") ~~ Parameter) ~~
    optional(keyword("SET PASSWORD")) ~~ keyword("CHANGE REQUIRED") ~~>>
      ((userName, initialPassword) => ast.AlterUser(userName, None, Some(initialPassword), requirePasswordChange = Some(true), None)) |
    //
    // ALTER USER username SET PASSWORD parameterPassword
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~ keyword("PASSWORD") ~~ Parameter) ~~>>
      ((userName, initialPassword) => ast.AlterUser(userName, None, Some(initialPassword), None, None))
  }

  def AlterUserPasswordMode: Rule1[AlterUser] = rule("CATALOG ALTER USER") {
    // ALTER USER username SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~
    keyword("PASSWORD CHANGE NOT REQUIRED")) ~~ keyword("SET STATUS SUSPENDED") ~~>>
      (userName => ast.AlterUser(userName, None, None, requirePasswordChange = Some(false), suspended = Some(true))) |
    //
    // ALTER USER username SET PASSWORD CHANGE NOT REQUIRED SET STATUS ACTIVE
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~
    keyword("PASSWORD CHANGE NOT REQUIRED")) ~~ keyword("SET STATUS ACTIVE") ~~>>
      (userName => ast.AlterUser(userName, None, None, requirePasswordChange = Some(false), suspended = Some(false))) |
    //
    // ALTER USER username SET PASSWORD CHANGE NOT REQUIRED
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~
    keyword("PASSWORD CHANGE NOT REQUIRED")) ~~>>
      (userName => ast.AlterUser(userName, None, None, requirePasswordChange = Some(false), None)) |
    //
    // ALTER USER username SET PASSWORD CHANGE REQUIRED SET STATUS SUSPENDED
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~
    keyword("PASSWORD CHANGE REQUIRED")) ~~ keyword("SET STATUS SUSPENDED") ~~>> (userName =>
      ast.AlterUser(userName, None, None, requirePasswordChange = Some(true), suspended = Some(true))) |
    //
    // ALTER USER username SET PASSWORD CHANGE REQUIRED SET STATUS ACTIVE
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~
    keyword("PASSWORD CHANGE REQUIRED")) ~~ keyword("SET STATUS ACTIVE") ~~>> (userName =>
      ast.AlterUser(userName, None, None, requirePasswordChange = Some(true), suspended = Some(false))) |
    //
    // ALTER USER username SET PASSWORD CHANGE REQUIRED
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~
    keyword("PASSWORD CHANGE REQUIRED")) ~~>> (userName =>
      ast.AlterUser(userName, None, None, requirePasswordChange = Some(true), None))
  }

  def AlterUserStatus: Rule1[AlterUser] = rule("CATALOG ALTER USER") {
    // ALTER USER username SET STATUS SUSPENDED
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~
    keyword("STATUS SUSPENDED")) ~~>> (userName =>
      ast.AlterUser(userName, None, None, None, suspended = Some(true))) |
    //
    // ALTER USER username SET STATUS ACTIVE
    group(keyword("ALTER USER") ~~ UserNameString ~~ keyword("SET") ~~
    keyword("STATUS ACTIVE")) ~~>> (userName =>
      ast.AlterUser(userName, None, None, None, suspended = Some(false)))
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
    group(keyword("CREATE ROLE") ~~ RoleNameString ~~
      optional(keyword("AS COPY OF") ~~ RoleNameString)) ~~>> (ast.CreateRole(_, _))
  }

  def DropRole: Rule1[DropRole] = rule("CATALOG DROP ROLE") {
    group(keyword("DROP ROLE") ~~ RoleNameString) ~~>> (ast.DropRole(_))
  }

  def ShowDatabase: Rule1[ShowDatabase] = rule("CATALOG SHOW DATABASE") {
    group(keyword("SHOW DATABASE") ~~ DatabaseNameString) ~~>> (ast.ShowDatabase(_))
  }

  def ShowDatabases: Rule1[ShowDatabases] = rule("CATALOG SHOW DATABASES") {
    keyword("SHOW DATABASES") ~>>> (_=> ast.ShowDatabases())
  }

  def CreateDatabase: Rule1[CreateDatabase] = rule("CATALOG CREATE DATABASE") {
    group(keyword("CREATE DATABASE") ~~ DatabaseNameString) ~~>> (ast.CreateDatabase(_))
  }

  def DropDatabase: Rule1[DropDatabase] = rule("CATALOG DROP DATABASE") {
    group(keyword("DROP DATABASE") ~~ DatabaseNameString) ~~>> (ast.DropDatabase(_))
  }

  def StartDatabase: Rule1[StartDatabase] = rule("CATALOG START DATABASE") {
    group(keyword("START DATABASE") ~~ DatabaseNameString) ~~>> (ast.StartDatabase(_))
  }

  def StopDatabase: Rule1[StopDatabase] = rule("CATALOG STOP DATABASE") {
    group(keyword("STOP DATABASE") ~~ DatabaseNameString) ~~>> (ast.StopDatabase(_))
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
