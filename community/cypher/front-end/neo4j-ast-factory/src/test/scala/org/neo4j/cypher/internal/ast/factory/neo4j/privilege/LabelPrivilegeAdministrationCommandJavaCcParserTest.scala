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

class LabelPrivilegeAdministrationCommandJavaCcParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName {
  Seq(
    ("GRANT", "TO"),
    ("DENY", "TO"),
    ("REVOKE GRANT", "FROM"),
    ("REVOKE DENY", "FROM"),
    ("REVOKE", "FROM")
  ).foreach {
    case (verb: String, preposition: String) =>

      Seq(
        "SET",
        "REMOVE"
      ).foreach {
        setOrRemove =>

          test(s"$verb $setOrRemove LABEL label ON GRAPH foo $preposition role") {
            assertSameAST(testName)
          }

          // Multiple labels should be allowed

          test(s"$verb $setOrRemove LABEL * ON GRAPH foo $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $setOrRemove LABEL label1, label2 ON GRAPH foo $preposition role") {
            assertSameAST(testName)
          }

          // Multiple graphs should be allowed

          test(s"$verb $setOrRemove LABEL label ON GRAPHS * $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $setOrRemove LABEL label ON GRAPHS foo,baz $preposition role") {
            assertSameAST(testName)
          }

          // Home graph should be allowed

          test(s"$verb $setOrRemove LABEL label ON HOME GRAPH $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $setOrRemove LABEL * ON HOME GRAPH $preposition role") {
            assertSameAST(testName)
          }

          // Default graph should be allowed

          test(s"$verb $setOrRemove LABEL label ON DEFAULT GRAPH $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $setOrRemove LABEL * ON DEFAULT GRAPH $preposition role") {
            assertSameAST(testName)
          }

          // Multiple roles should be allowed

          test(s"$verb $setOrRemove LABEL label ON GRAPHS foo $preposition role1, role2") {
            assertSameAST(testName)
          }

          // Parameter values

          test(s"$verb $setOrRemove LABEL label ON GRAPH $$foo $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $setOrRemove LABEL label ON GRAPH foo $preposition $$role") {
            assertSameAST(testName)
          }

          // TODO: should this one be supported?
          test(s"$verb $setOrRemove LABEL $$label ON GRAPH foo $preposition role") {
            assertSameAST(testName)
          }

          // LABELS instead of LABEL

          test(s"$verb $setOrRemove LABELS label ON GRAPH * $preposition role") {
            assertJavaCCExceptionStart(testName, s"""Invalid input 'LABELS': expected""")
          }

          // Database instead of graph keyword

          test(s"$verb $setOrRemove LABEL label ON DATABASES * $preposition role") {
            val offset = verb.length + setOrRemove.length + 17
            assertJavaCCExceptionStart(testName, s"""Invalid input 'DATABASES': expected""")
          }

          test(s"$verb $setOrRemove LABEL label ON DATABASE foo $preposition role") {
            val offset = verb.length + setOrRemove.length + 17
            assertJavaCCExceptionStart(testName, s"""Invalid input 'DATABASE': expected""")
          }

          test(s"$verb $setOrRemove LABEL label ON HOME DATABASE $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb $setOrRemove LABEL label ON DEFAULT DATABASE $preposition role") {
            assertSameAST(testName)
          }
      }
  }
}
