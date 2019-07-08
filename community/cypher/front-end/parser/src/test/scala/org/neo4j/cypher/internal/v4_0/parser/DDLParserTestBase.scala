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
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.parboiled.scala.Rule1

class DDLParserTestBase
  extends ParserAstTest[ast.Statement] with Statement with AstConstructionTestSupport {

  implicit val parser: Rule1[ast.Statement] = Statement

  type privilegeFunc = (PrivilegeType, ActionResource, GraphScope, PrivilegeQualifier, Seq[String]) => InputPosition => ast.Statement

  def grant(p: PrivilegeType, a: ActionResource, s: GraphScope, q: PrivilegeQualifier, r: Seq[String]): InputPosition => ast.Statement =
    ast.GrantPrivilege(p, a, s, q, r)

  def deny(p: PrivilegeType, a: ActionResource, s: GraphScope, q: PrivilegeQualifier, r: Seq[String]): InputPosition => ast.Statement =
    ast.DenyPrivilege(p, a, s, q, r)

  def revoke(p: PrivilegeType, a: ActionResource, s: GraphScope, q: PrivilegeQualifier, r: Seq[String]): InputPosition => ast.Statement =
    ast.RevokePrivilege(p, a, s, q, r)
}
