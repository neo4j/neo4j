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
package org.neo4j.cypher.internal.parser.privilege

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.AllGraphAction
import org.neo4j.cypher.internal.ast.PrivilegeType
import org.neo4j.cypher.internal.parser.AdministrationCommandParserTestBase
import org.neo4j.cypher.internal.util.InputPosition

class GraphPrivilegeAdministrationCommandParserTest extends AdministrationCommandParserTestBase {

  type privilegeTypeFunction = () => InputPosition => PrivilegeType

  Seq(
    ("GRANT", "TO", grant: noResourcePrivilegeFunc),
    ("DENY", "TO", deny: noResourcePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrant: noResourcePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDeny: noResourcePrivilegeFunc),
    ("REVOKE", "FROM", revokeBoth: noResourcePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: noResourcePrivilegeFunc) =>

      // All versions of ALL [[GRAPH] PRIVILEGES] should be allowed

      test(s"$verb ALL ON GRAPH foo $preposition role") {
        yields(func(ast.GraphPrivilege(AllGraphAction)(_), List(ast.NamedGraphScope(literal("foo"))(_)), ast.AllQualifier()(_), Seq(literal("role"))))
      }

      test(s"$verb ALL PRIVILEGES ON GRAPH foo $preposition role") {
        yields(func(ast.GraphPrivilege(AllGraphAction)(_), List(ast.NamedGraphScope(literal("foo"))(_)), ast.AllQualifier()(_), Seq(literal("role"))))
      }

      test(s"$verb ALL GRAPH PRIVILEGES ON GRAPH foo $preposition role") {
        yields(func(ast.GraphPrivilege(AllGraphAction)(_), List(ast.NamedGraphScope(literal("foo"))(_)), ast.AllQualifier()(_), Seq(literal("role"))))
      }

      // Multiple graphs should be allowed

      test(s"$verb ALL GRAPH PRIVILEGES ON GRAPHS * $preposition role") {
        yields(func(ast.GraphPrivilege(AllGraphAction)(_), List(ast.AllGraphsScope()(_)), ast.AllQualifier()(_), Seq(literal("role"))))
      }

      test(s"$verb ALL GRAPH PRIVILEGES ON GRAPHS foo,bar $preposition role") {
        yields(func(ast.GraphPrivilege(AllGraphAction)(_), List(ast.NamedGraphScope(literal("foo"))(_), ast.NamedGraphScope(literal("bar"))(_)), ast.AllQualifier()(_), Seq(literal("role"))))
      }

      // Multiple roles should be allowed
      test(s"$verb ALL GRAPH PRIVILEGES ON GRAPHS foo $preposition role1, role2") {
        yields(func(ast.GraphPrivilege(AllGraphAction)(_), List(ast.NamedGraphScope(literal("foo"))(_)), ast.AllQualifier()(_), Seq(literal("role1"), literal("role2"))))
      }

      // Parameter values should be allowed

      test(s"$verb ALL GRAPH PRIVILEGES ON GRAPH $$foo $preposition role") {
        yields(func(ast.GraphPrivilege(AllGraphAction)(_), List(ast.NamedGraphScope(param("foo"))(_)), ast.AllQualifier()(_), Seq(literal("role"))))
      }

      test(s"$verb ALL GRAPH PRIVILEGES ON GRAPH foo $preposition $$role") {
        yields(func(ast.GraphPrivilege(AllGraphAction)(_), List(ast.NamedGraphScope(literal("foo"))(_)), ast.AllQualifier()(_), Seq(param("role"))))
      }

      // Qualifier or resource should not be supported

      test(s"$verb ALL GRAPH PRIVILEGES ON GRAPH foo NODE A $preposition role") {
        failsToParse
      }

      test(s"$verb ALL GRAPH PRIVILEGES ON GRAPH foo ELEMENTS * $preposition role") {
        failsToParse
      }

      test(s"$verb ALL GRAPH PRIVILEGES {prop} ON GRAPH foo $preposition role") {
        failsToParse
      }

      // Invalid syntax

      test(s"$verb ALL GRAPH ON GRAPH foo $preposition role") {
        failsToParse
      }

      test(s"$verb GRAPH ON GRAPH foo $preposition role") {
        failsToParse
      }

      test(s"$verb GRAPH PRIVILEGES ON GRAPH foo $preposition role") {
        failsToParse
      }

      test(s"$verb PRIVILEGES ON GRAPH foo $preposition role") {
        failsToParse
      }

      // Database/dbms instead of graph keyword

      test(s"$verb ALL GRAPH PRIVILEGES ON DATABASES * $preposition role") {
        failsToParse
      }

      test(s"$verb ALL GRAPH PRIVILEGES ON DATABASE foo $preposition role") {
        failsToParse
      }

      test(s"$verb ALL GRAPH PRIVILEGES ON DEFAULT DATABASE $preposition role") {
        failsToParse
      }

      test(s"$verb ALL GRAPH PRIVILEGES ON DBMS $preposition role") {
        failsToParse
      }
  }

}
