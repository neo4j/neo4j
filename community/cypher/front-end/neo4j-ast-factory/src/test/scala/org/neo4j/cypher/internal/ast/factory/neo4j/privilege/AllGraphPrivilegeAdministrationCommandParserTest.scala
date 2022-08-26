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
import org.neo4j.cypher.internal.ast.factory.neo4j.AdministrationAndSchemaCommandParserTestBase

class AllGraphPrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq(
    ("GRANT", "TO", grantGraphPrivilege: noResourcePrivilegeFunc),
    ("DENY", "TO", denyGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE", "FROM", revokeGraphPrivilege: noResourcePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: noResourcePrivilegeFunc) =>
      Seq[Immutable](true, false).foreach {
        immutable =>
          val immutableString = immutableOrEmpty(immutable)
          // All versions of ALL [[GRAPH] PRIVILEGES] should be allowed

          test(s"$verb$immutableString ALL ON GRAPH foo $preposition role") {
            yields(func(
              ast.GraphPrivilege(ast.AllGraphAction, List(graphScopeFoo))(_),
              List(ast.AllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString ALL PRIVILEGES ON GRAPH foo $preposition role") {
            yields(func(
              ast.GraphPrivilege(ast.AllGraphAction, List(graphScopeFoo))(_),
              List(ast.AllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON GRAPH foo $preposition role") {
            yields(func(
              ast.GraphPrivilege(ast.AllGraphAction, List(graphScopeFoo))(_),
              List(ast.AllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          // Home graph should be allowed

          test(s"$verb$immutableString ALL ON HOME GRAPH $preposition role") {
            yields(func(
              ast.GraphPrivilege(ast.AllGraphAction, List(ast.HomeGraphScope()(_)))(_),
              List(ast.AllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString ALL PRIVILEGES ON HOME GRAPH $preposition role") {
            yields(func(
              ast.GraphPrivilege(ast.AllGraphAction, List(ast.HomeGraphScope()(_)))(_),
              List(ast.AllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON HOME GRAPH $preposition role") {
            yields(func(
              ast.GraphPrivilege(ast.AllGraphAction, List(ast.HomeGraphScope()(_)))(_),
              List(ast.AllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          // Default graph should be allowed

          test(s"$verb$immutableString ALL ON DEFAULT GRAPH $preposition role") {
            yields(func(
              ast.GraphPrivilege(ast.AllGraphAction, List(ast.DefaultGraphScope()(_)))(_),
              List(ast.AllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString ALL PRIVILEGES ON DEFAULT GRAPH $preposition role") {
            yields(func(
              ast.GraphPrivilege(ast.AllGraphAction, List(ast.DefaultGraphScope()(_)))(_),
              List(ast.AllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON DEFAULT GRAPH $preposition role") {
            yields(func(
              ast.GraphPrivilege(ast.AllGraphAction, List(ast.DefaultGraphScope()(_)))(_),
              List(ast.AllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          // Multiple graphs should be allowed

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON GRAPHS * $preposition role") {
            yields(func(
              ast.GraphPrivilege(ast.AllGraphAction, List(ast.AllGraphsScope()(_)))(_),
              List(ast.AllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON GRAPHS foo,baz $preposition role") {
            yields(func(
              ast.GraphPrivilege(ast.AllGraphAction, List(graphScopeFoo, graphScopeBaz))(_),
              List(ast.AllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          // Multiple roles should be allowed

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON GRAPHS foo $preposition role1, role2") {
            yields(func(
              ast.GraphPrivilege(ast.AllGraphAction, List(graphScopeFoo))(_),
              List(ast.AllQualifier()(_)),
              Seq(literalRole1, literalRole2),
              immutable
            ))
          }

          // Parameter values should be allowed

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON GRAPH $$foo $preposition role") {
            yields(func(
              ast.GraphPrivilege(ast.AllGraphAction, List(graphScopeParamFoo))(_),
              List(ast.AllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON GRAPH foo $preposition $$role") {
            yields(func(
              ast.GraphPrivilege(ast.AllGraphAction, List(graphScopeFoo))(_),
              List(ast.AllQualifier()(_)),
              Seq(paramRole),
              immutable
            ))
          }

          // Qualifier or resource should not be supported

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON GRAPH foo NODE A $preposition role") {
            failsToParse
          }

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON GRAPH foo ELEMENTS * $preposition role") {
            failsToParse
          }

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES {prop} ON GRAPH foo $preposition role") {
            failsToParse
          }

          // Invalid syntax

          test(s"$verb$immutableString ALL GRAPH ON GRAPH foo $preposition role") {
            failsToParse
          }

          test(s"$verb$immutableString GRAPH ON GRAPH foo $preposition role") {
            val expected =
              """Invalid input 'GRAPH': expected
                |  "ACCESS"""".stripMargin
            assertFailsWithMessageStart(testName, expected)
          }

          test(s"$verb$immutableString GRAPH PRIVILEGES ON GRAPH foo $preposition role") {
            val expected =
              """Invalid input 'GRAPH': expected
                |  "ACCESS"""".stripMargin
            assertFailsWithMessageStart(testName, expected)
          }

          test(s"$verb$immutableString PRIVILEGES ON GRAPH foo $preposition role") {
            val expected =
              """Invalid input 'PRIVILEGES': expected
                |  "ACCESS"""".stripMargin
            assertFailsWithMessageStart(testName, expected)
          }

          // Database/dbms instead of graph keyword

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON DATABASES * $preposition role") {
            val offset = verb.length + immutableString.length + 25
            assertFailsWithMessage(
              testName,
              s"""Invalid input 'DATABASES': expected "GRAPH" (line 1, column ${offset + 1} (offset: $offset))"""
            )
          }

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON DATABASE foo $preposition role") {
            val offset = verb.length + immutableString.length + 25
            assertFailsWithMessage(
              testName,
              s"""Invalid input 'DATABASE': expected "GRAPH" (line 1, column ${offset + 1} (offset: $offset))"""
            )
          }

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON HOME DATABASE $preposition role") {
            val offset = verb.length + immutableString.length + 25
            assertFailsWithMessage(
              testName,
              s"""Invalid input 'HOME': expected "GRAPH" (line 1, column ${offset + 1} (offset: $offset))"""
            )
          }

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON DEFAULT DATABASE $preposition role") {
            val offset = verb.length + immutableString.length + 25
            assertFailsWithMessage(
              testName,
              s"""Invalid input 'DEFAULT': expected "GRAPH" (line 1, column ${offset + 1} (offset: $offset))"""
            )
          }

          test(s"$verb$immutableString ALL GRAPH PRIVILEGES ON DBMS $preposition role") {
            val offset = verb.length + immutableString.length + 25
            assertFailsWithMessage(
              testName,
              s"""Invalid input 'DBMS': expected "GRAPH" (line 1, column ${offset + 1} (offset: $offset))"""
            )
          }
      }
  }
}
