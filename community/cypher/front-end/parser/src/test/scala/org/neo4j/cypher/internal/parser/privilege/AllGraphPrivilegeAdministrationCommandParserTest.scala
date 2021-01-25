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
package org.neo4j.cypher.internal.parser.privilege

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.AllGraphAction
import org.neo4j.cypher.internal.parser.AdministrationCommandParserTestBase

class AllGraphPrivilegeAdministrationCommandParserTest extends AdministrationCommandParserTestBase {

  Seq(
    ("GRANT", "TO", grantGraphPrivilege: noResourcePrivilegeFunc),
    ("DENY", "TO", denyGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE", "FROM", revokeGraphPrivilege: noResourcePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: noResourcePrivilegeFunc) =>

      // All versions of ALL [[GRAPH] PRIVILEGES] should be allowed

      test(s"$verb ALL ON GRAPH foo $preposition role") {
        yields(func(ast.GraphPrivilege(AllGraphAction)(_), List(graphScopeFoo), List(ast.AllQualifier()(_)), Seq(literalRole)))
      }

      test(s"$verb ALL PRIVILEGES ON GRAPH foo $preposition role") {
        yields(func(ast.GraphPrivilege(AllGraphAction)(_), List(graphScopeFoo), List(ast.AllQualifier()(_)), Seq(literalRole)))
      }

      test(s"$verb ALL GRAPH PRIVILEGES ON GRAPH foo $preposition role") {
        yields(func(ast.GraphPrivilege(AllGraphAction)(_), List(graphScopeFoo), List(ast.AllQualifier()(_)), Seq(literalRole)))
      }

      // Default graph should be allowed

      test(s"$verb ALL ON DEFAULT GRAPH $preposition role") {
        yields(func(ast.GraphPrivilege(AllGraphAction)(_), List(ast.DefaultGraphScope()(_)), List(ast.AllQualifier()(_)), Seq(literalRole)))
      }

      test(s"$verb ALL PRIVILEGES ON DEFAULT GRAPH $preposition role") {
        yields(func(ast.GraphPrivilege(AllGraphAction)(_), List(ast.DefaultGraphScope()(_)), List(ast.AllQualifier()(_)), Seq(literalRole)))
      }

      test(s"$verb ALL GRAPH PRIVILEGES ON DEFAULT GRAPH $preposition role") {
        yields(func(ast.GraphPrivilege(AllGraphAction)(_), List(ast.DefaultGraphScope()(_)), List(ast.AllQualifier()(_)), Seq(literalRole)))
      }

      // Multiple graphs should be allowed

      test(s"$verb ALL GRAPH PRIVILEGES ON GRAPHS * $preposition role") {
        yields(func(ast.GraphPrivilege(AllGraphAction)(_), List(ast.AllGraphsScope()(_)), List(ast.AllQualifier()(_)), Seq(literalRole)))
      }

      test(s"$verb ALL GRAPH PRIVILEGES ON GRAPHS foo,baz $preposition role") {
        yields(func(ast.GraphPrivilege(AllGraphAction)(_), List(graphScopeFoo, graphScopeBaz), List(ast.AllQualifier()(_)), Seq(literalRole)))
      }

      // Multiple roles should be allowed

      test(s"$verb ALL GRAPH PRIVILEGES ON GRAPHS foo $preposition role1, role2") {
        yields(func(ast.GraphPrivilege(AllGraphAction)(_), List(graphScopeFoo), List(ast.AllQualifier()(_)), Seq(literalRole1, literalRole2)))
      }

      // Parameter values should be allowed

      test(s"$verb ALL GRAPH PRIVILEGES ON GRAPH $$foo $preposition role") {
        yields(func(ast.GraphPrivilege(AllGraphAction)(_), List(graphScopeParamFoo), List(ast.AllQualifier()(_)), Seq(literalRole)))
      }

      test(s"$verb ALL GRAPH PRIVILEGES ON GRAPH foo $preposition $$role") {
        yields(func(ast.GraphPrivilege(AllGraphAction)(_), List(graphScopeFoo), List(ast.AllQualifier()(_)), Seq(paramRole)))
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
