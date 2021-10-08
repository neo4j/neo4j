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

class PropertyPrivilegeAdministrationCommandJavaCcParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName {

  Seq(
    ("GRANT", "TO"),
    ("DENY", "TO"),
    ("REVOKE GRANT", "FROM"),
    ("REVOKE DENY", "FROM"),
    ("REVOKE", "FROM")
  ).foreach {
    case (verb: String, preposition: String) =>

      test(s"$verb SET PROPERTY { prop } ON GRAPH foo $preposition role") {
        assertSameAST(testName)
      }

      // Multiple properties should be allowed

      test(s"$verb SET PROPERTY { * } ON GRAPH foo $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb SET PROPERTY { prop1, prop2 } ON GRAPH foo $preposition role") {
        assertSameAST(testName)
      }

      // Home graph should be allowed

      test(s"$verb SET PROPERTY { * } ON HOME GRAPH $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb SET PROPERTY { prop } ON HOME GRAPH $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb SET PROPERTY { prop } ON HOME GRAPH NODES A,B $preposition role") {
        assertSameAST(testName)
      }

      // Default graph should be allowed

      test(s"$verb SET PROPERTY { * } ON DEFAULT GRAPH $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb SET PROPERTY { prop } ON DEFAULT GRAPH $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb SET PROPERTY { prop } ON DEFAULT GRAPH NODES A,B $preposition role") {
        assertSameAST(testName)
      }

      // Multiple graphs should be allowed

      test(s"$verb SET PROPERTY { prop } ON GRAPHS * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb SET PROPERTY { prop } ON GRAPHS foo,baz $preposition role") {
        assertSameAST(testName)
      }

      // Qualifiers

      test(s"$verb SET PROPERTY { prop } ON GRAPHS foo ELEMENTS A,B $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb SET PROPERTY { prop } ON GRAPHS foo NODES A,B $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb SET PROPERTY { prop } ON GRAPHS foo NODES * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb SET PROPERTY { prop } ON GRAPHS foo RELATIONSHIPS A,B $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb SET PROPERTY { prop } ON GRAPHS foo RELATIONSHIPS * $preposition role") {
        assertSameAST(testName)
      }

      // Multiple roles should be allowed

      test(s"$verb SET PROPERTY { prop } ON GRAPHS foo $preposition role1, role2") {
        assertSameAST(testName)
      }

      // Parameter values

      test(s"$verb SET PROPERTY { prop } ON GRAPH $$foo $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb SET PROPERTY { prop } ON GRAPH foo $preposition $$role") {
        assertSameAST(testName)
      }

      // PROPERTYS/PROPERTIES instead of PROPERTY

      test(s"$verb SET PROPERTYS { prop } ON GRAPH * $preposition role") {
        val offset = verb.length + 5
        assertJavaCCException(testName,
          s"""Invalid input 'PROPERTYS': expected
             |  "DATABASE"
             |  "LABEL"
             |  "PASSWORD"
             |  "PASSWORDS"
             |  "PROPERTY"
             |  "USER" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin)
      }

      test(s"$verb SET PROPERTIES { prop } ON GRAPH * $preposition role") {
        val offset = verb.length + 5
        assertJavaCCException(testName,
          s"""Invalid input 'PROPERTIES': expected
             |  "DATABASE"
             |  "LABEL"
             |  "PASSWORD"
             |  "PASSWORDS"
             |  "PROPERTY"
             |  "USER" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin)
      }

      // Database instead of graph keyword

      test(s"$verb SET PROPERTY { prop } ON DATABASES * $preposition role") {
        val offset = verb.length + 26
        assertJavaCCException(testName, s"""Invalid input 'DATABASES': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))""")
      }

      test(s"$verb SET PROPERTY { prop } ON DATABASE foo $preposition role") {
        val offset = verb.length + 26
        assertJavaCCException(testName, s"""Invalid input 'DATABASE': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))""")
      }

      test(s"$verb SET PROPERTY { prop } ON HOME DATABASE $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb SET PROPERTY { prop } ON DEFAULT DATABASE $preposition role") {
        assertSameAST(testName)
      }
  }
}
