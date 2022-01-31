/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.ast.factory.neo4j.privilege

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.WriteAction
import org.neo4j.cypher.internal.ast.factory.neo4j.AdministrationAndSchemaCommandParserTestBase

class WritePrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq(
    ("GRANT", "TO", grantGraphPrivilege: noResourcePrivilegeFunc),
    ("DENY", "TO", denyGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE", "FROM", revokeGraphPrivilege: noResourcePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: noResourcePrivilegeFunc) =>

      test(s"$verb WRITE ON GRAPH foo $preposition role") {
        yields(func(ast.GraphPrivilege(WriteAction, List(graphScopeFoo))(pos), List(ast.ElementsAllQualifier() _), Seq(literalRole)))
      }

      test(s"$verb WRITE ON GRAPHS foo $preposition role") {
        yields(func(ast.GraphPrivilege(WriteAction, List(graphScopeFoo))(pos), List(ast.ElementsAllQualifier() _), Seq(literalRole)))
      }

      // Multiple graphs should be allowed (with and without plural GRAPHS)

      test(s"$verb WRITE ON GRAPH * $preposition role") {
        yields(func(ast.GraphPrivilege(WriteAction, List(ast.AllGraphsScope()(_)))(pos), List(ast.ElementsAllQualifier() _), Seq(literalRole)))
      }

      test(s"$verb WRITE ON GRAPHS * $preposition role") {
        yields(func(ast.GraphPrivilege(WriteAction, List(ast.AllGraphsScope()(_)))(pos), List(ast.ElementsAllQualifier() _), Seq(literalRole)))
      }

      test(s"$verb WRITE ON GRAPH foo, baz $preposition role") {
        yields(func(ast.GraphPrivilege(WriteAction, List(graphScopeFoo, graphScopeBaz))(pos), List(ast.ElementsAllQualifier() _), List(literalRole)))
      }

      test(s"$verb WRITE ON GRAPHS foo, baz $preposition role") {
        yields(func(ast.GraphPrivilege(WriteAction, List(graphScopeFoo, graphScopeBaz))(pos), List(ast.ElementsAllQualifier() _), List(literalRole)))
      }

      // Default and home graph should parse

      test(s"$verb WRITE ON HOME GRAPH $preposition role") {
        yields(func(ast.GraphPrivilege(WriteAction, List(ast.HomeGraphScope()(_)))(pos), List(ast.ElementsAllQualifier() _), Seq(literalRole)))
      }

      test(s"$verb WRITE ON DEFAULT GRAPH $preposition role") {
        yields(func(ast.GraphPrivilege(WriteAction, List(ast.DefaultGraphScope()(_)))(pos), List(ast.ElementsAllQualifier() _), Seq(literalRole)))
      }

      // Multiple roles should be allowed

      test(s"$verb WRITE ON GRAPH foo $preposition role1, role2") {
        yields(func(ast.GraphPrivilege(WriteAction, List(graphScopeFoo))(_), List(ast.ElementsAllQualifier() _), Seq(literalRole1, literalRole2)))
      }

      // Parameters and escaped strings should be allowed

      test(s"$verb WRITE ON GRAPH $$foo $preposition role") {
        yields(func(ast.GraphPrivilege(WriteAction, List(graphScopeParamFoo))(pos), List(ast.ElementsAllQualifier() _), Seq(literalRole)))
      }

      test(s"$verb WRITE ON GRAPH `f:oo` $preposition role") {
        yields(func(ast.GraphPrivilege(WriteAction, List(ast.NamedGraphScope(literalFColonOo)(_)))(pos), List(ast.ElementsAllQualifier() _), Seq(literalRole)))
      }

      test(s"$verb WRITE ON GRAPH foo $preposition $$role") {
        yields(func(ast.GraphPrivilege(WriteAction, List(graphScopeFoo))(pos), List(ast.ElementsAllQualifier() _), Seq(paramRole)))
      }

      test(s"$verb WRITE ON GRAPH foo $preposition `r:ole`") {
        yields(func(ast.GraphPrivilege(WriteAction, List(graphScopeFoo))(pos), List(ast.ElementsAllQualifier() _), Seq(literalRColonOle)))
      }

      // Resource or qualifier should not be supported

      test(s"$verb WRITE {*} ON GRAPH foo $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE {prop} ON GRAPH foo $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE ON GRAPH foo NODE A $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE ON GRAPH foo NODES * $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE ON GRAPH foo RELATIONSHIP R $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE ON GRAPH foo RELATIONSHIPS * $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE ON GRAPH foo ELEMENT A $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE ON GRAPH foo ELEMENTS * $preposition role") {
        failsToParse
      }

      // Invalid/missing part of the command

      test(s"$verb WRITE ON GRAPH f:oo $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE ON GRAPH foo $preposition ro:le") {
        failsToParse
      }

      test(s"$verb WRITE ON GRAPH $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE ON GRAPH foo $preposition") {
        failsToParse
      }

      test(s"$verb WRITE GRAPH foo $preposition role") {
        failsToParse
      }

      // DEFAULT and HOME together with plural GRAPHS

      test(s"$verb WRITE ON HOME GRAPHS $preposition role") {
        val offset = verb.length + 15
        assertFailsWithMessage(testName, s"""Invalid input 'GRAPHS': expected "GRAPH" (line 1, column ${offset + 1} (offset: $offset))""")
      }

      test(s"$verb WRITE ON DEFAULT GRAPHS $preposition role") {
        val offset = verb.length + 18
        assertFailsWithMessage(testName, s"""Invalid input 'GRAPHS': expected "GRAPH" (line 1, column ${offset + 1} (offset: $offset))""")
      }

      // Default and home graph with named graph

      test(s"$verb WRITE ON HOME GRAPH baz $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE ON DEFAULT GRAPH baz $preposition role") {
        failsToParse
      }

      // Mix of specific graph and *

      test(s"$verb WRITE ON GRAPH foo, * $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE ON GRAPH *, foo $preposition role") {
        failsToParse
      }

      // Database instead of graph keyword

      test(s"$verb WRITE ON DATABASES * $preposition role") {
        val offset = verb.length + 10
        assertFailsWithMessage(testName, s"""Invalid input 'DATABASES': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))""")
      }

      test(s"$verb WRITE ON DATABASE foo $preposition role") {
        val offset = verb.length + 10
        assertFailsWithMessage(testName, s"""Invalid input 'DATABASE': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))""")
      }

      test(s"$verb WRITE ON HOME DATABASE $preposition role") {
        failsToParse
      }

      test(s"$verb WRITE ON DEFAULT DATABASE $preposition role") {
        failsToParse
      }
  }
}
