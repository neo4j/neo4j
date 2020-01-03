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
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.parboiled.scala.Rule1

class AdministrationCommandParserTestBase
  extends ParserAstTest[ast.Statement] with Statement with AstConstructionTestSupport {

  implicit val parser: Rule1[ast.Statement] = Statement

  type privilegeFunc = (PrivilegeType, ActionResource, GraphScope, PrivilegeQualifier, Seq[String]) => InputPosition => ast.Statement
  type databasePrivilegeFunc = (DatabaseAction, GraphScope, Seq[String]) => InputPosition => ast.Statement
  type dbmsPrivilegeFunc = (AdminAction, Seq[String]) => InputPosition => ast.Statement

  def grant(p: PrivilegeType, a: ActionResource, s: GraphScope, q: PrivilegeQualifier, r: Seq[String]): InputPosition => ast.Statement =
    ast.GrantPrivilege(p, a, s, q, r)

  def grantDatabasePrivilege(d: DatabaseAction, s: GraphScope, r: Seq[String]): InputPosition => ast.Statement =
    ast.GrantPrivilege.databaseAction(d, s, r)

  def grantDbmsPrivilege(a: AdminAction, r: Seq[String]): InputPosition => ast.Statement =
    ast.GrantPrivilege.dbmsAction(a, r)

  def deny(p: PrivilegeType, a: ActionResource, s: GraphScope, q: PrivilegeQualifier, r: Seq[String]): InputPosition => ast.Statement =
    ast.DenyPrivilege(p, a, s, q, r)

  def denyDatabasePrivilege(d: DatabaseAction, s: GraphScope, r: Seq[String]): InputPosition => ast.Statement =
    ast.DenyPrivilege.databaseAction(d, s, r)

  def denyDbmsPrivilege(a: AdminAction, r: Seq[String]): InputPosition => ast.Statement =
    ast.DenyPrivilege.dbmsAction(a, r)

  def revokeGrant(p: PrivilegeType, a: ActionResource, s: GraphScope, q: PrivilegeQualifier, r: Seq[String]): InputPosition => ast.Statement =
    ast.RevokePrivilege(p, a, s, q, r, RevokeGrantType()(InputPosition.NONE))

  def revokeGrantDatabasePrivilege(d: DatabaseAction, s: GraphScope, r: Seq[String]): InputPosition => ast.Statement =
    ast.RevokePrivilege.databaseGrantedAction(d, s, r)

  def revokeGrantDbmsPrivilege(a: AdminAction, r: Seq[String]): InputPosition => ast.Statement =
    ast.RevokePrivilege.grantedDbmsAction(a, r)

  def revokeDeny(p: PrivilegeType, a: ActionResource, s: GraphScope, q: PrivilegeQualifier, r: Seq[String]): InputPosition => ast.Statement =
    ast.RevokePrivilege(p, a, s, q, r, RevokeDenyType()(InputPosition.NONE))

  def revokeDenyDatabasePrivilege(d: DatabaseAction, s: GraphScope, r: Seq[String]): InputPosition => ast.Statement =
    ast.RevokePrivilege.databaseDeniedAction(d, s, r)

  def revokeDenyDbmsPrivilege(a: AdminAction, r: Seq[String]): InputPosition => ast.Statement =
    ast.RevokePrivilege.deniedDbmsAction(a, r)

  def revokeBoth(p: PrivilegeType, a: ActionResource, s: GraphScope, q: PrivilegeQualifier, r: Seq[String]): InputPosition => ast.Statement =
    ast.RevokePrivilege(p, a, s, q, r, RevokeBothType()(InputPosition.NONE))

  def revokeDatabasePrivilege(d: DatabaseAction, s: GraphScope, r: Seq[String]): InputPosition => ast.Statement =
    ast.RevokePrivilege.databaseAction(d, s, r)

  def revokeDbmsPrivilege(a: AdminAction, r: Seq[String]): InputPosition => ast.Statement =
    ast.RevokePrivilege.dbmsAction(a, r)
}
