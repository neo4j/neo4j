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

import org.neo4j.cypher.internal.ast.factory.neo4j.ParserComparisonTestBase
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalatest.FunSuiteLike

class WritePrivilegeAdministrationCommandJavaCcParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName {

  Seq(
    ("GRANT", "TO"),
    ("DENY", "TO"),
    ("REVOKE GRANT", "FROM"),
    ("REVOKE DENY", "FROM"),
    ("REVOKE", "FROM")
  ).foreach {
    case (verb: String, preposition: String) =>

      test(s"$verb WRITE ON GRAPH foo $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb WRITE ON GRAPHS foo $preposition role") {
        assertSameAST(testName)
      }

      // Multiple graphs should be allowed (with and without plural GRAPHS)

      test(s"$verb WRITE ON GRAPH * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb WRITE ON GRAPHS * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb WRITE ON GRAPH foo, baz $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb WRITE ON GRAPHS foo, baz $preposition role") {
        assertSameAST(testName)
      }

      // Default and home graph should parse

      test(s"$verb WRITE ON HOME GRAPH $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb WRITE ON DEFAULT GRAPH $preposition role") {
        assertSameAST(testName)
      }

      // Multiple roles should be allowed

      test(s"$verb WRITE ON GRAPH foo $preposition role1, role2") {
        assertSameAST(testName)
      }

      // Parameters and escaped strings should be allowed

      test(s"$verb WRITE ON GRAPH $$foo $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb WRITE ON GRAPH `f:oo` $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb WRITE ON GRAPH foo $preposition $$role") {
        assertSameAST(testName)
      }

      test(s"$verb WRITE ON GRAPH foo $preposition `r:ole`") {
        assertSameAST(testName)
      }

      // Resource or qualifier should not be supported

      test(s"$verb WRITE {*} ON GRAPH foo $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb WRITE {prop} ON GRAPH foo $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb WRITE ON GRAPH foo NODE A $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb WRITE ON GRAPH foo NODES * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb WRITE ON GRAPH foo RELATIONSHIP R $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb WRITE ON GRAPH foo RELATIONSHIPS * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb WRITE ON GRAPH foo ELEMENT A $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb WRITE ON GRAPH foo ELEMENTS * $preposition role") {
        assertSameAST(testName)
      }

      // Invalid/missing part of the command

      test(s"$verb WRITE ON GRAPH f:oo $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb WRITE ON GRAPH foo $preposition ro:le") {
        assertSameAST(testName)
      }

      test(s"$verb WRITE ON GRAPH $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb WRITE ON GRAPH foo $preposition") {
        assertSameAST(testName)
      }

      test(s"$verb WRITE GRAPH foo $preposition role") {
        assertSameAST(testName)
      }

      // DEFAULT and HOME together with plural GRAPHS

      test(s"$verb WRITE ON HOME GRAPHS $preposition role") {
        val offset = verb.length + 15
        assertJavaCCException(testName, s"""Invalid input 'GRAPHS': expected "GRAPH" (line 1, column ${offset + 1} (offset: $offset))""")
      }

      test(s"$verb WRITE ON DEFAULT GRAPHS $preposition role") {
        val offset = verb.length + 18
        assertJavaCCException(testName, s"""Invalid input 'GRAPHS': expected "GRAPH" (line 1, column ${offset + 1} (offset: $offset))""")
      }

      // Default and home graph with named graph

      test(s"$verb WRITE ON HOME GRAPH baz $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb WRITE ON DEFAULT GRAPH baz $preposition role") {
        assertSameAST(testName)
      }

      // Mix of specific graph and *

      test(s"$verb WRITE ON GRAPH foo, * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb WRITE ON GRAPH *, foo $preposition role") {
        assertSameAST(testName)
      }

      // Database instead of graph keyword

      test(s"$verb WRITE ON DATABASES * $preposition role") {
        val offset = verb.length + 10
        assertJavaCCException(testName, s"""Invalid input 'DATABASES': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))""")
      }

      test(s"$verb WRITE ON DATABASE foo $preposition role") {
        val offset = verb.length + 10
        assertJavaCCException(testName, s"""Invalid input 'DATABASE': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))""")
      }

      test(s"$verb WRITE ON HOME DATABASE $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb WRITE ON DEFAULT DATABASE $preposition role") {
        assertSameAST(testName)
      }
  }
}
