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

class ReadMatchPrivilegeAdministrationCommandJavaCcParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName {

  // Granting/denying/revoking read and match to/from role

  Seq(
    ("READ", "GRANT", "TO"),
    ("READ", "DENY", "TO"),
    ("READ", "REVOKE GRANT", "FROM"),
    ("READ", "REVOKE DENY", "FROM"),
    ("READ", "REVOKE", "FROM"),
    ("MATCH", "GRANT", "TO"),
    ("MATCH", "DENY", "TO"),
    ("MATCH", "REVOKE GRANT", "FROM"),
    ("MATCH", "REVOKE DENY", "FROM"),
    ("MATCH", "REVOKE", "FROM")
  ).foreach {
    case (action: String, verb: String, preposition: String) =>

      test(s"$verb $action { prop } ON HOME GRAPH $preposition role"){
        assertSameAST(testName)
      }

      test(s"$verb $action { prop } ON HOME GRAPH NODE A $preposition role"){
        assertSameAST(testName)
      }

      test(s"$verb $action { prop } ON DEFAULT GRAPH $preposition role"){
        assertSameAST(testName)
      }

      test(s"$verb $action { prop } ON DEFAULT GRAPH NODE A $preposition role"){
        assertSameAST(testName)
      }

      Seq("GRAPH", "GRAPHS").foreach {
        graphKeyword =>

          Seq("NODE", "NODES").foreach {
            nodeKeyword =>

              Seq(
                ("*", "*"),
                ("*", "foo"),
                ("bar", "*"),
                ("bar", "foo"),
                ("foo, bar", "*"),
                ("foo, bar", "foo")
              ).foreach {
                case (properties: String, graphName: String) =>
                  test( s"validExpressions $verb $action {$properties} $graphKeyword $graphName $nodeKeyword $preposition") {
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $nodeKeyword * $preposition $$role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $nodeKeyword * (*) $preposition role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $nodeKeyword A $preposition role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $nodeKeyword A (*) $preposition role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $nodeKeyword `A B` (*) $preposition role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $nodeKeyword A, B (*) $preposition role1, $$role2")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $nodeKeyword * $preposition `r:ole`")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $nodeKeyword `:A` (*) $preposition role")
                  }

                  test( s"failToParse $verb $action {$properties} $graphKeyword $graphName $nodeKeyword $preposition") {
                    assertSameAST(s"$verb $action {$properties} $graphKeyword $graphName $nodeKeyword * (*) $preposition role")
                    assertSameAST(s"$verb $action {$properties} $graphKeyword $graphName $nodeKeyword A $preposition role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $nodeKeyword * (*)")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $nodeKeyword A B (*) $preposition role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $nodeKeyword A (foo) $preposition role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $nodeKeyword * $preposition r:ole")
                  }
              }

              test( s"validExpressions $verb $action $graphKeyword $nodeKeyword $preposition") {
                assertSameAST(s"$verb $action {*} ON $graphKeyword `f:oo` $nodeKeyword * $preposition role")
                assertSameAST(s"$verb $action {bar} ON $graphKeyword `f:oo` $nodeKeyword * $preposition role")
                assertSameAST(s"$verb $action {`b:ar`} ON $graphKeyword foo $nodeKeyword * $preposition role")
                assertSameAST(s"$verb $action {*} ON $graphKeyword foo, baz $nodeKeyword A (*) $preposition role")
                assertSameAST(s"$verb $action {bar} ON $graphKeyword foo, baz $nodeKeyword A (*) $preposition role")
              }

              test( s"parsingFailures $verb $action $graphKeyword $nodeKeyword $preposition") {
                // Invalid graph name
                assertSameAST(s"$verb $action {*} ON $graphKeyword f:oo $nodeKeyword * $preposition role")
                assertSameAST(s"$verb $action {bar} ON $graphKeyword f:oo $nodeKeyword * $preposition role")
                // mixing specific graph and *
                assertSameAST(s"$verb $action {*} ON $graphKeyword foo, * $nodeKeyword * $preposition role")
                assertSameAST(s"$verb $action {*} ON $graphKeyword *, foo $nodeKeyword * $preposition role")
                // invalid property definition
                assertSameAST(s"$verb $action {b:ar} ON $graphKeyword foo $nodeKeyword * $preposition role")
                // missing graph name
                assertSameAST(s"$verb $action {*} ON $graphKeyword $nodeKeyword * $preposition role")
                assertSameAST(s"$verb $action {*} ON $graphKeyword $nodeKeyword * (*) $preposition role")
                assertSameAST(s"$verb $action {*} ON $graphKeyword $nodeKeyword A $preposition role")
                assertSameAST(s"$verb $action {*} ON $graphKeyword $nodeKeyword A (*) $preposition role")
                assertSameAST(s"$verb $action {bar} ON $graphKeyword $nodeKeyword * $preposition role")
                assertSameAST(s"$verb $action {bar} ON $graphKeyword $nodeKeyword * (*) $preposition role")
                assertSameAST(s"$verb $action {bar} ON $graphKeyword $nodeKeyword A $preposition role")
                assertSameAST(s"$verb $action {bar} ON $graphKeyword $nodeKeyword A (*) $preposition role")
                // missing property definition
                assertSameAST(s"$verb $action ON $graphKeyword * $nodeKeyword * $preposition role")
                assertSameAST(s"$verb $action ON $graphKeyword * $nodeKeyword * (*) $preposition role")
                assertSameAST(s"$verb $action ON $graphKeyword * $nodeKeyword A $preposition role")
                assertSameAST(s"$verb $action ON $graphKeyword * $nodeKeyword A (*) $preposition role")
                assertSameAST(s"$verb $action ON $graphKeyword foo $nodeKeyword * $preposition role")
                assertSameAST(s"$verb $action ON $graphKeyword foo $nodeKeyword * (*) $preposition role")
                assertSameAST(s"$verb $action ON $graphKeyword foo $nodeKeyword A $preposition role")
                assertSameAST(s"$verb $action ON $graphKeyword foo $nodeKeyword A (*) $preposition role")
                // missing property list
                assertSameAST(s"$verb $action {} ON $graphKeyword * $nodeKeyword * $preposition role")
                assertSameAST(s"$verb $action {} ON $graphKeyword * $nodeKeyword * (*) $preposition role")
                assertSameAST(s"$verb $action {} ON $graphKeyword * $nodeKeyword A $preposition role")
                assertSameAST(s"$verb $action {} ON $graphKeyword * $nodeKeyword A (*) $preposition role")
                assertSameAST(s"$verb $action {} ON $graphKeyword foo $nodeKeyword * $preposition role")
                assertSameAST(s"$verb $action {} ON $graphKeyword foo $nodeKeyword * (*) $preposition role")
                assertSameAST(s"$verb $action {} ON $graphKeyword foo $nodeKeyword A $preposition role")
                assertSameAST(s"$verb $action {} ON $graphKeyword foo $nodeKeyword A (*) $preposition role")
              }
          }

          Seq("RELATIONSHIP", "RELATIONSHIPS").foreach {
            relTypeKeyword =>

              Seq(
                ("*", "*"),
                ("*", "foo"),
                ("bar", "*"),
                ("bar", "foo"),
                ("foo, bar", "*"),
                ("foo, bar", "foo")
              ).foreach {
                case (properties: String, graphName: String) =>

                  test( s"validExpressions $verb $action {$properties} $graphKeyword $graphName $relTypeKeyword $preposition") {
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $relTypeKeyword * $preposition role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $relTypeKeyword * (*) $preposition $$role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $relTypeKeyword A $preposition role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $relTypeKeyword A (*) $preposition role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $relTypeKeyword `A B` (*) $preposition role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $relTypeKeyword A, B (*) $preposition $$role1, role2")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $relTypeKeyword * $preposition `r:ole`")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $relTypeKeyword `:A` (*) $preposition role")
                  }

                  test( s"parsingFailures $verb $action {$properties} $graphKeyword $graphName $relTypeKeyword $preposition") {
                    assertSameAST(s"$verb $action {$properties} $graphKeyword $graphName $relTypeKeyword * (*) $preposition role")
                    assertSameAST(s"$verb $action {$properties} $graphKeyword $graphName $relTypeKeyword A $preposition role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $relTypeKeyword * (*)")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $relTypeKeyword A B (*) $preposition role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $relTypeKeyword A (foo) $preposition role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $relTypeKeyword * $preposition r:ole")
                  }
              }

              test( s"validExpressions $verb $action $graphKeyword $relTypeKeyword $preposition") {
                assertSameAST(s"$verb $action {*} ON $graphKeyword `f:oo` $relTypeKeyword * $preposition role")
                assertSameAST(s"$verb $action {bar} ON $graphKeyword `f:oo` $relTypeKeyword * $preposition role")
                assertSameAST(s"$verb $action {`b:ar`} ON $graphKeyword foo $relTypeKeyword * $preposition role")
                assertSameAST(s"$verb $action {*} ON $graphKeyword foo, baz $relTypeKeyword A (*) $preposition role")
                assertSameAST(s"$verb $action {bar} ON $graphKeyword foo, baz $relTypeKeyword A (*) $preposition role")
              }

              test( s"parsingFailures$verb $action $graphKeyword $relTypeKeyword $preposition") {
                // Invalid graph name
                assertSameAST(s"$verb $action {*} ON $graphKeyword f:oo $relTypeKeyword * $preposition role")
                assertSameAST(s"$verb $action {bar} ON $graphKeyword f:oo $relTypeKeyword * $preposition role")
                // invalid property definition
                assertSameAST(s"$verb $action {b:ar} ON $graphKeyword foo $relTypeKeyword * $preposition role")
                // missing graph name
                assertSameAST(s"$verb $action {*} ON $graphKeyword $relTypeKeyword * $preposition role")
                assertSameAST(s"$verb $action {*} ON $graphKeyword $relTypeKeyword * (*) $preposition role")
                assertSameAST(s"$verb $action {*} ON $graphKeyword $relTypeKeyword A $preposition role")
                assertSameAST(s"$verb $action {*} ON $graphKeyword $relTypeKeyword A (*) $preposition role")
                assertSameAST(s"$verb $action {bar} ON $graphKeyword $relTypeKeyword * $preposition role")
                assertSameAST(s"$verb $action {bar} ON $graphKeyword $relTypeKeyword * (*) $preposition role")
                assertSameAST(s"$verb $action {bar} ON $graphKeyword $relTypeKeyword A $preposition role")
                assertSameAST(s"$verb $action {bar} ON $graphKeyword $relTypeKeyword A (*) $preposition role")
                // missing property definition
                assertSameAST(s"$verb $action ON $graphKeyword * $relTypeKeyword * $preposition role")
                assertSameAST(s"$verb $action ON $graphKeyword * $relTypeKeyword * (*) $preposition role")
                assertSameAST(s"$verb $action ON $graphKeyword * $relTypeKeyword A $preposition role")
                assertSameAST(s"$verb $action ON $graphKeyword * $relTypeKeyword A (*) $preposition role")
                assertSameAST(s"$verb $action ON $graphKeyword foo $relTypeKeyword * $preposition role")
                assertSameAST(s"$verb $action ON $graphKeyword foo $relTypeKeyword * (*) $preposition role")
                assertSameAST(s"$verb $action ON $graphKeyword foo $relTypeKeyword A $preposition role")
                assertSameAST(s"$verb $action ON $graphKeyword foo $relTypeKeyword A (*) $preposition role")
                // missing property list
                assertSameAST(s"$verb $action {} ON $graphKeyword * $relTypeKeyword * $preposition role")
                assertSameAST(s"$verb $action {} ON $graphKeyword * $relTypeKeyword * (*) $preposition role")
                assertSameAST(s"$verb $action {} ON $graphKeyword * $relTypeKeyword A $preposition role")
                assertSameAST(s"$verb $action {} ON $graphKeyword * $relTypeKeyword A (*) $preposition role")
                assertSameAST(s"$verb $action {} ON $graphKeyword foo $relTypeKeyword * $preposition role")
                assertSameAST(s"$verb $action {} ON $graphKeyword foo $relTypeKeyword * (*) $preposition role")
                assertSameAST(s"$verb $action {} ON $graphKeyword foo $relTypeKeyword A $preposition role")
                assertSameAST(s"$verb $action {} ON $graphKeyword foo $relTypeKeyword A (*) $preposition role")
              }
          }

          Seq("ELEMENT", "ELEMENTS").foreach {
            elementKeyword =>

              Seq(
                ("*", "*"),
                ("*", "foo"),
                ("bar", "*"),
                ("bar", "foo"),
                ("foo, bar", "*"),
                ("foo, bar", "foo")
              ).foreach {
                case (properties: String, graphName: String) =>
                  test( s"validExpressions $verb $action {$properties} $graphKeyword $graphName $elementKeyword $preposition") {
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $elementKeyword * $preposition role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $elementKeyword * (*) $preposition role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $elementKeyword A $preposition $$role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $elementKeyword A (*) $preposition role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $elementKeyword `A B` (*) $preposition role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $elementKeyword A, B (*) $preposition $$role1, $$role2")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $elementKeyword * $preposition `r:ole`")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $elementKeyword `:A` (*) $preposition role")
                  }

                  test( s"parsingFailures$verb $action {$properties} $graphKeyword $graphName $elementKeyword $preposition") {
                    assertSameAST(s"$verb $action {$properties} $graphKeyword $graphName $elementKeyword * (*) $preposition role")
                    assertSameAST(s"$verb $action {$properties} $graphKeyword $graphName $elementKeyword A $preposition role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $elementKeyword * (*)")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $elementKeyword A B (*) $preposition role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $elementKeyword A (foo) $preposition role")
                    assertSameAST(s"$verb $action {$properties} ON $graphKeyword $graphName $elementKeyword * $preposition r:ole")
                  }
              }

              test( s"validExpressions $verb $action $graphKeyword $elementKeyword $preposition") {
                assertSameAST(s"$verb $action {*} ON $graphKeyword `f:oo` $elementKeyword * $preposition role")
                assertSameAST(s"$verb $action {bar} ON $graphKeyword `f:oo` $elementKeyword * $preposition role")
                assertSameAST(s"$verb $action {`b:ar`} ON $graphKeyword foo $elementKeyword * $preposition role")
                assertSameAST(s"$verb $action {*} ON $graphKeyword foo, baz $elementKeyword A (*) $preposition role")
                assertSameAST(s"$verb $action {bar} ON $graphKeyword foo, baz $elementKeyword A (*) $preposition role")
              }

              test( s"parsingFailures $verb $action $graphKeyword $elementKeyword $preposition") {
                // Invalid graph name
                assertSameAST(s"$verb $action {*} ON $graphKeyword f:oo $elementKeyword * $preposition role")
                assertSameAST(s"$verb $action {bar} ON $graphKeyword f:oo $elementKeyword * $preposition role")
                // invalid property definition
                assertSameAST(s"$verb $action {b:ar} ON $graphKeyword foo $elementKeyword * $preposition role")
                // missing graph name
                assertSameAST(s"$verb $action {*} ON $graphKeyword $elementKeyword * $preposition role")
                assertSameAST(s"$verb $action {*} ON $graphKeyword $elementKeyword * (*) $preposition role")
                assertSameAST(s"$verb $action {*} ON $graphKeyword $elementKeyword A $preposition role")
                assertSameAST(s"$verb $action {*} ON $graphKeyword $elementKeyword A (*) $preposition role")
                assertSameAST(s"$verb $action {bar} ON $graphKeyword $elementKeyword * $preposition role")
                assertSameAST(s"$verb $action {bar} ON $graphKeyword $elementKeyword * (*) $preposition role")
                assertSameAST(s"$verb $action {bar} ON $graphKeyword $elementKeyword A $preposition role")
                assertSameAST(s"$verb $action {bar} ON $graphKeyword $elementKeyword A (*) $preposition role")
                // missing property definition
                assertSameAST(s"$verb $action ON $graphKeyword * $elementKeyword * $preposition role")
                assertSameAST(s"$verb $action ON $graphKeyword * $elementKeyword * (*) $preposition role")
                assertSameAST(s"$verb $action ON $graphKeyword * $elementKeyword A $preposition role")
                assertSameAST(s"$verb $action ON $graphKeyword * $elementKeyword A (*) $preposition role")
                assertSameAST(s"$verb $action ON $graphKeyword foo $elementKeyword * $preposition role")
                assertSameAST(s"$verb $action ON $graphKeyword foo $elementKeyword * (*) $preposition role")
                assertSameAST(s"$verb $action ON $graphKeyword foo $elementKeyword A $preposition role")
                assertSameAST(s"$verb $action ON $graphKeyword foo $elementKeyword A (*) $preposition role")
                // missing property list
                assertSameAST(s"$verb $action {} ON $graphKeyword * $elementKeyword * $preposition role")
                assertSameAST(s"$verb $action {} ON $graphKeyword * $elementKeyword * (*) $preposition role")
                assertSameAST(s"$verb $action {} ON $graphKeyword * $elementKeyword A $preposition role")
                assertSameAST(s"$verb $action {} ON $graphKeyword * $elementKeyword A (*) $preposition role")
                assertSameAST(s"$verb $action {} ON $graphKeyword foo $elementKeyword * $preposition role")
                assertSameAST(s"$verb $action {} ON $graphKeyword foo $elementKeyword * (*) $preposition role")
                assertSameAST(s"$verb $action {} ON $graphKeyword foo $elementKeyword A $preposition role")
                assertSameAST(s"$verb $action {} ON $graphKeyword foo $elementKeyword A (*) $preposition role")
              }
          }

          // Needs to be separate loop to avoid duplicate tests since the test does not have any segment keyword
          Seq(
            ("*", "*"),
            ("*", "foo"),
            ("*", "$foo"),
            ("bar", "*"),
            ("bar", "foo"),
            ("foo, bar", "*"),
            ("foo, bar", "foo")
          ).foreach {
            case (properties: String, graphName: String) =>
              test(s"$verb $action {$properties} ON $graphKeyword $graphName $preposition role") {
                assertSameAST(testName)
              }
          }
      }

      // Database instead of graph keyword

      test(s"$verb $action ON DATABASES * $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb $action ON DATABASE foo $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb $action ON HOME DATABASE $preposition role") {
        assertSameAST(testName)
      }

      test(s"$verb $action ON DEFAULT DATABASE $preposition role") {
        assertSameAST(testName)
      }
  }
}
