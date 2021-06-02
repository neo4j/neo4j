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

class CreateDeletePrivilegeAdministrationCommandJavaCcParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName {

  Seq(
    ("GRANT", "TO"),
    ("DENY", "TO"),
    ("REVOKE GRANT", "FROM"),
    ("REVOKE DENY", "FROM"),
    ("REVOKE", "FROM")
  ).foreach {
    case (verb: String, preposition: String) =>

      Seq(
        "CREATE",
        "DELETE"
      ).foreach {
        createOrDelete =>

          test(s"$verb $createOrDelete ON GRAPH foo $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $createOrDelete ON GRAPH foo ELEMENTS A $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $createOrDelete ON GRAPH foo NODE A $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $createOrDelete ON GRAPH foo RELATIONSHIPS * $preposition role") {
            assertSameAST(testName)
          }

          // Home graph

          test(s"$verb $createOrDelete ON HOME GRAPH $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $createOrDelete ON HOME GRAPH $preposition role1, role2") {
            assertSameAST(testName)
          }

          test(s"$verb $createOrDelete ON HOME GRAPH $preposition $$role1, role2") {
            assertSameAST(testName)
          }

          test(s"$verb $createOrDelete ON HOME GRAPH RELATIONSHIPS * $preposition role") {
            assertSameAST(testName)
          }

          // Both Home and * should not parse
          test(s"$verb $createOrDelete ON HOME GRAPH * $preposition role") {
            assertSameAST(testName)
          }

          // Default graph

          test(s"$verb $createOrDelete ON DEFAULT GRAPH $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $createOrDelete ON DEFAULT GRAPH $preposition role1, role2") {
            assertSameAST(testName)
          }

          test(s"$verb $createOrDelete ON DEFAULT GRAPH $preposition $$role1, role2") {
            assertSameAST(testName)
          }

          test(s"$verb $createOrDelete ON DEFAULT GRAPH RELATIONSHIPS * $preposition role") {
            assertSameAST(testName)
          }

          // Both Default and * should not parse
          test(s"$verb $createOrDelete ON DEFAULT GRAPH * $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $createOrDelete ON DATABASE blah $preposition role") {
            val offset = verb.length + createOrDelete.length + 5
            assertJavaCCException(testName, s"""Invalid input 'DATABASE': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))""")
          }
      }
  }
}
