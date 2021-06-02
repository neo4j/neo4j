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

class TraversePrivilegeAdministrationCommandJavaCcParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName {

  Seq(
    ("GRANT", "TO"),
    ("DENY", "TO"),
    ("REVOKE GRANT", "FROM"),
    ("REVOKE DENY", "FROM"),
    ("REVOKE", "FROM")
  ).foreach {
    case (verb: String, preposition: String) =>

      test(s"$verb TRAVERSE ON HOME GRAPH $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TRAVERSE ON HOME GRAPH NODE A $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TRAVERSE ON HOME GRAPH RELATIONSHIP * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TRAVERSE ON HOME GRAPH ELEMENT A $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TRAVERSE ON DEFAULT GRAPH $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TRAVERSE ON DEFAULT GRAPH NODE A $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TRAVERSE ON DEFAULT GRAPH RELATIONSHIP * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TRAVERSE ON DEFAULT GRAPH ELEMENT A $preposition role") {
        assertSameAST(testName)
      }

      Seq("GRAPH", "GRAPHS").foreach {
        graphKeyword =>
          test(s"$verb TRAVERSE ON $graphKeyword * $preposition $$role") {
            assertSameAST(testName)
          }

          test(s"$verb TRAVERSE ON $graphKeyword foo $preposition role") {
            assertSameAST(testName)
          }

          test(s"$verb TRAVERSE ON $graphKeyword $$foo $preposition role") {
            assertSameAST(testName)
          }

          Seq("NODE", "NODES").foreach {
            nodeKeyword =>

              test( s"validExpressions $verb $graphKeyword $nodeKeyword $preposition") {
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword * $nodeKeyword * $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword * $nodeKeyword * (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword * $nodeKeyword A $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword * $nodeKeyword A (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword `*` $nodeKeyword A $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword * $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword * (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword A $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition role1, $$role2")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword `2foo` $nodeKeyword A (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition `r:ole`")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword `A B` (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword A, B (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword A, B (*) $preposition role1, role2")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo, baz $nodeKeyword A (*) $preposition role")
              }

              test( s"traverseParsingErrors $verb $graphKeyword $nodeKeyword $preposition") {
                assertSameAST(s"$verb TRAVERSE $graphKeyword * $nodeKeyword * (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword A B (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword A (foo) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword $nodeKeyword * $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword $nodeKeyword A $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword $nodeKeyword * (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword $nodeKeyword A (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition r:ole")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword 2foo $nodeKeyword A (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword * $nodeKeyword * (*)")
              }
          }

          Seq("RELATIONSHIP", "RELATIONSHIPS").foreach {
            relTypeKeyword =>

              test( s"validExpressions $verb $graphKeyword $relTypeKeyword $preposition") {
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword * $relTypeKeyword * $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword * $relTypeKeyword * (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword * $relTypeKeyword A $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword * $relTypeKeyword A (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword `*` $relTypeKeyword A $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword * $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword * (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword A $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition $$role1, role2")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword `2foo` $relTypeKeyword A (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition `r:ole`")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword `A B` (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword A, B (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword A, B (*) $preposition role1, role2")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo, baz $relTypeKeyword A (*) $preposition role")
              }

              test( s"traverseParsingErrors$verb $graphKeyword $relTypeKeyword $preposition") {
                assertSameAST(s"$verb TRAVERSE $graphKeyword * $relTypeKeyword * (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword A B (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword A (foo) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword $relTypeKeyword * $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword $relTypeKeyword A $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword $relTypeKeyword * (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword $relTypeKeyword A (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition r:ole")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword 2foo $relTypeKeyword A (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword * $relTypeKeyword * (*)")
              }
          }

          Seq("ELEMENT", "ELEMENTS").foreach {
            elementKeyword =>

              test( s"validExpressions $verb $graphKeyword $elementKeyword $preposition") {
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword * $elementKeyword * $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword * $elementKeyword * (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword * $elementKeyword A $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword * $elementKeyword A (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword `*` $elementKeyword A $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword * $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword * (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword A $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition role1, role2")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword `2foo` $elementKeyword A (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition `r:ole`")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword `A B` (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword A, B (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword A, B (*) $preposition $$role1, $$role2")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo, baz $elementKeyword A (*) $preposition role")
              }

              test( s"traverseParsingErrors $verb $graphKeyword $elementKeyword $preposition") {
                assertSameAST(s"$verb TRAVERSE $graphKeyword * $elementKeyword * (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword A B (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword A (foo) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword $elementKeyword * $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword $elementKeyword A $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword $elementKeyword * (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword $elementKeyword A (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition r:ole")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword 2foo $elementKeyword A (*) $preposition role")
                assertSameAST(s"$verb TRAVERSE ON $graphKeyword * $elementKeyword * (*)")
              }
          }
      }

      // Mix of specific graph and *

      test(s"$verb TRAVERSE ON GRAPH foo, * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TRAVERSE ON GRAPH *, foo $preposition role") {
        assertSameAST(testName)
      }

      // Database instead of graph keyword

      test(s"$verb TRAVERSE ON DATABASES * $preposition role") {
        val offset = verb.length + 13
        assertJavaCCException(testName,
          s"""Invalid input 'DATABASES': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin)
      }

      test(s"$verb TRAVERSE ON DATABASE foo $preposition role") {
        val offset = verb.length + 13
        assertJavaCCException(testName,
          s"""Invalid input 'DATABASE': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin)
      }

      test(s"$verb TRAVERSE ON HOME DATABASE $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb TRAVERSE ON DEFAULT DATABASE $preposition role") {
        assertSameAST(testName)
      }
  }
}
