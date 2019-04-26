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
    CatalogCommand | SecurityCommand | Command | Query
  )

  def CatalogCommand: Rule1[CatalogDDL] = rule("Catalog DDL statement") {
    ShowDatabase | ShowDatabases | CreateDatabase | DropDatabase | StartDatabase | StopDatabase | CreateGraph | DropGraph | CreateView | DropView
  }

  def SecurityCommand: Rule1[CatalogDDL] = rule("Security DDL statement") {
    optional(keyword("CATALOG")) ~~ (ShowRoles | CreateRole | DropRole | ShowUsers | CreateUser)
  }

  def ShowUsers: Rule1[ShowUsers] = rule("CATALOG SHOW USERS") {
    keyword("SHOW USERS") ~>>> (_=> ast.ShowUsers())
  }

  def CreateUser: Rule1[CreateUser] = rule("CATALOG CREATE USER") { //TODO should SUSPENDED or ACTIVE be default?
    // CREATE USER username WITH PASSWORD stringLiteralPassword CHANGE NOT REQUIRED WITH STATUS SUSPENDED
    group(keyword("CREATE USER") ~~ UserNameString) ~~
    group(keyword("WITH") ~~ keyword("PASSWORD") ~~ StringLiteral ~~ "CHANGE" ~~ "NOT" ~~ "REQUIRED") ~~
    group(keyword("WITH") ~~ keyword("STATUS") ~~ "SUSPENDED") ~~>>
    ((userName, initialPassword) => ast.CreateUser(userName, initialPassword.value, requirePasswordChange = false, suspended = true)) |
    // CREATE USER username WITH PASSWORD stringLiteralPassword CHANGE NOT REQUIRED [ WITH STATUS ACTIVE ]
    group(keyword("CREATE USER") ~~ UserNameString) ~~
    group(keyword("WITH") ~~ keyword("PASSWORD") ~~ StringLiteral ~~ "CHANGE" ~~ "NOT" ~~ "REQUIRED") ~~
    optional(group(keyword("WITH") ~~ keyword("STATUS") ~~ "ACTIVE")) ~~>>
    ((userName, initialPassword) => ast.CreateUser(userName, initialPassword.value, requirePasswordChange = false, suspended = false)) |
    // CREATE USER username WITH PASSWORD stringLiteralPassword [ CHANGE REQUIRED ] WITH STATUS SUSPENDED
    group(keyword("CREATE USER") ~~ UserNameString) ~~
    group(keyword("WITH") ~~ keyword("PASSWORD") ~~ StringLiteral ~~ optional("CHANGE" ~~ "REQUIRED")) ~~
    group(keyword("WITH") ~~ keyword("STATUS") ~~ "SUSPENDED") ~~>>
      ((userName, initialPassword) => ast.CreateUser(userName, initialPassword.value, requirePasswordChange = true, suspended = true)) |
    // CREATE USER username WITH PASSWORD stringLiteralPassword [ CHANGE REQUIRED ] [ WITH STATUS ACTIVE ]
    group(keyword("CREATE USER") ~~ UserNameString) ~~
    group(keyword("WITH") ~~ keyword("PASSWORD") ~~ StringLiteral ~~ optional("CHANGE" ~~ "REQUIRED")) ~~
    optional(group(keyword("WITH") ~~ keyword("STATUS") ~~ "ACTIVE")) ~~>>
      ((userName, initialPassword) => ast.CreateUser(userName, initialPassword.value, requirePasswordChange = true, suspended = false))
}

  def ShowRoles: Rule1[ShowRoles] = rule("CATALOG SHOW ROLES") {
    //SHOW [ ALL | POPULATED ] ROLES WITH USERS
    group(optional(keyword("CATALOG")) ~~
      keyword("SHOW") ~~ keyword("POPULATED") ~~ keyword("ROLES") ~~
      keyword("WITH USERS")) ~>>> (_ => ast.ShowRoles(withUsers = true, showAll = false)) |
    group(optional(keyword("CATALOG")) ~~
      keyword("SHOW") ~~ optional(keyword("ALL")) ~~ keyword("ROLES") ~~
      keyword("WITH USERS")) ~>>> (_ => ast.ShowRoles(withUsers = true, showAll = true)) |
    // SHOW [ ALL | POPULATED ] ROLES
    group(optional(keyword("CATALOG")) ~~
      keyword("SHOW") ~~ keyword("POPULATED") ~~ keyword("ROLES")) ~>>>
      (_ => ast.ShowRoles(withUsers = false, showAll = false)) |
    group(optional(keyword("CATALOG")) ~~
      keyword("SHOW") ~~ optional(keyword("ALL")) ~~ keyword("ROLES")) ~>>>
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
    group(optional(keyword("CATALOG")) ~~ keyword("SHOW DATABASE") ~~ DatabaseNameString) ~~>> (ast.ShowDatabase(_))
  }

  def ShowDatabases: Rule1[ShowDatabases] = rule("CATALOG SHOW DATABASES") {
    group(optional(keyword("CATALOG")) ~~ keyword("SHOW DATABASES")) ~>>> (_=> ast.ShowDatabases())
  }

  def CreateDatabase: Rule1[CreateDatabase] = rule("CATALOG CREATE DATABASE") {
    group(optional(keyword("CATALOG")) ~~ keyword("CREATE DATABASE") ~~ DatabaseNameString) ~~>> (ast.CreateDatabase(_))
  }

  def DropDatabase: Rule1[DropDatabase] = rule("CATALOG DROP DATABASE") {
    group(optional(keyword("CATALOG")) ~~ keyword("DROP DATABASE") ~~ DatabaseNameString) ~~>> (ast.DropDatabase(_))
  }

  def StartDatabase: Rule1[StartDatabase] = rule("CATALOG START DATABASE") {
    group(optional(keyword("CATALOG")) ~~ keyword("START DATABASE") ~~ DatabaseNameString) ~~>> (ast.StartDatabase(_))
  }

  def StopDatabase: Rule1[StopDatabase] = rule("CATALOG STOP DATABASE") {
    group(optional(keyword("CATALOG")) ~~ keyword("STOP DATABASE") ~~ DatabaseNameString) ~~>> (ast.StopDatabase(_))
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
