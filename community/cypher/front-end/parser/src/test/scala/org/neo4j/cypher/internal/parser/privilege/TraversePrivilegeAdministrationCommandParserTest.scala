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
import org.neo4j.cypher.internal.parser.AdministrationCommandParserTestBase

class TraversePrivilegeAdministrationCommandParserTest extends AdministrationCommandParserTestBase {

  Seq(
    ("GRANT", "TO", grantGraphPrivilege: noResourcePrivilegeFunc),
    ("DENY", "TO", denyGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE", "FROM", revokeGraphPrivilege: noResourcePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: noResourcePrivilegeFunc) =>

      test(s"$verb TRAVERSE ON DEFAULT GRAPH $preposition role") {
        yields(func(ast.GraphPrivilege(ast.TraverseAction, List(ast.DefaultGraphScope()(_)))(pos), List(ast.ElementsAllQualifier() _), Seq(literalRole)))
      }

      test(s"$verb TRAVERSE ON DEFAULT GRAPH NODE A $preposition role") {
        yields(func(ast.GraphPrivilege(ast.TraverseAction, List(ast.DefaultGraphScope()(_)))(pos), List(labelQualifierA), Seq(literalRole)))
      }

      test(s"$verb TRAVERSE ON DEFAULT GRAPH RELATIONSHIP * $preposition role") {
        yields(func(ast.GraphPrivilege(ast.TraverseAction, List(ast.DefaultGraphScope()(_)))(pos), List(ast.RelationshipAllQualifier() _), Seq(literalRole)))
      }

      test(s"$verb TRAVERSE ON DEFAULT GRAPH ELEMENT A $preposition role") {
        yields(func(ast.GraphPrivilege(ast.TraverseAction, List(ast.DefaultGraphScope()(_)))(pos), List(elemQualifierA), Seq(literalRole)))
      }

      Seq("GRAPH", "GRAPHS").foreach {
        graphKeyword =>
          test(s"$verb TRAVERSE ON $graphKeyword * $preposition $$role") {
            yields(func(ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos), List(ast.ElementsAllQualifier() _), Seq(paramRole)))
          }

          test(s"$verb TRAVERSE ON $graphKeyword foo $preposition role") {
            yields(func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(ast.ElementsAllQualifier() _), Seq(literalRole)))
          }

          test(s"$verb TRAVERSE ON $graphKeyword $$foo $preposition role") {
            yields(func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeParamFoo))(pos), List(ast.ElementsAllQualifier() _), Seq(literalRole)))
          }

          Seq("NODE", "NODES").foreach {
            nodeKeyword =>

              test( s"validExpressions $verb $graphKeyword $nodeKeyword $preposition") {
                parsing(s"$verb TRAVERSE ON $graphKeyword * $nodeKeyword * $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos), List(ast.LabelAllQualifier() _), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword * $nodeKeyword * (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos), List(ast.LabelAllQualifier() _), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword * $nodeKeyword A $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos), List(labelQualifierA), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword * $nodeKeyword A (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos), List(labelQualifierA), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword `*` $nodeKeyword A $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(ast.NamedGraphScope(literal("*")) _))(pos), List(labelQualifierA), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword * $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(ast.LabelAllQualifier() _), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword * (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(ast.LabelAllQualifier() _), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword A $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(labelQualifierA), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(labelQualifierA), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition role1, $$role2") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(labelQualifierA), Seq(literalRole1, paramRole2))
                parsing(s"$verb TRAVERSE ON $graphKeyword `2foo` $nodeKeyword A (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(ast.NamedGraphScope(literal("2foo")) _))(pos), List(labelQualifierA), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition `r:ole`") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(labelQualifierA), Seq(literalRColonOle))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword `A B` (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(ast.LabelQualifier("A B") _), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword A, B (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(labelQualifierA, labelQualifierB), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword A, B (*) $preposition role1, role2") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(labelQualifierA, labelQualifierB), Seq(literalRole1, literalRole2))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo, baz $nodeKeyword A (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo, graphScopeBaz))(pos), List(labelQualifierA), Seq(literalRole))
              }

              test( s"traverseParsingErrors $verb $graphKeyword $nodeKeyword $preposition") {
                assertFails(s"$verb TRAVERSE $graphKeyword * $nodeKeyword * (*) $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword A B (*) $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword A (foo) $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword $nodeKeyword * $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword $nodeKeyword A $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword $nodeKeyword * (*) $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword $nodeKeyword A (*) $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition r:ole")
                assertFails(s"$verb TRAVERSE ON $graphKeyword 2foo $nodeKeyword A (*) $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword * $nodeKeyword * (*)")
              }
          }

          Seq("RELATIONSHIP", "RELATIONSHIPS").foreach {
            relTypeKeyword =>

              test( s"validExpressions $verb $graphKeyword $relTypeKeyword $preposition") {
                parsing(s"$verb TRAVERSE ON $graphKeyword * $relTypeKeyword * $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos), List(ast.RelationshipAllQualifier() _), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword * $relTypeKeyword * (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos), List(ast.RelationshipAllQualifier() _), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword * $relTypeKeyword A $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos), List(relQualifierA), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword * $relTypeKeyword A (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos), List(relQualifierA), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword `*` $relTypeKeyword A $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(ast.NamedGraphScope(literal("*")) _))(pos), List(relQualifierA), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword * $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(ast.RelationshipAllQualifier() _), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword * (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(ast.RelationshipAllQualifier() _), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword A $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(relQualifierA), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(relQualifierA), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition $$role1, role2") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(relQualifierA), Seq(paramRole1, literalRole2))
                parsing(s"$verb TRAVERSE ON $graphKeyword `2foo` $relTypeKeyword A (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(ast.NamedGraphScope(literal("2foo")) _))(pos), List(relQualifierA), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition `r:ole`") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(relQualifierA), Seq(literalRColonOle))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword `A B` (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(ast.RelationshipQualifier("A B") _), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword A, B (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(relQualifierA, relQualifierB), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword A, B (*) $preposition role1, role2") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(relQualifierA, relQualifierB), Seq(literalRole1, literalRole2))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo, baz $relTypeKeyword A (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo, graphScopeBaz))(pos), List(relQualifierA), Seq(literalRole))
              }

              test( s"traverseParsingErrors$verb $graphKeyword $relTypeKeyword $preposition") {
                assertFails(s"$verb TRAVERSE $graphKeyword * $relTypeKeyword * (*) $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword A B (*) $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword A (foo) $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword $relTypeKeyword * $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword $relTypeKeyword A $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword $relTypeKeyword * (*) $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword $relTypeKeyword A (*) $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition r:ole")
                assertFails(s"$verb TRAVERSE ON $graphKeyword 2foo $relTypeKeyword A (*) $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword * $relTypeKeyword * (*)")
              }
          }

          Seq("ELEMENT", "ELEMENTS").foreach {
            elementKeyword =>

              test( s"validExpressions $verb $graphKeyword $elementKeyword $preposition") {
                parsing(s"$verb TRAVERSE ON $graphKeyword * $elementKeyword * $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos), List(ast.ElementsAllQualifier() _), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword * $elementKeyword * (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos), List(ast.ElementsAllQualifier() _), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword * $elementKeyword A $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos), List(elemQualifierA), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword * $elementKeyword A (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(ast.AllGraphsScope() _))(pos), List(elemQualifierA), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword `*` $elementKeyword A $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(ast.NamedGraphScope(literal("*")) _))(pos), List(elemQualifierA), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword * $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(ast.ElementsAllQualifier() _), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword * (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(ast.ElementsAllQualifier() _), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword A $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(elemQualifierA), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(elemQualifierA), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition role1, role2") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(elemQualifierA), Seq(literalRole1, literalRole2))
                parsing(s"$verb TRAVERSE ON $graphKeyword `2foo` $elementKeyword A (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(ast.NamedGraphScope(literal("2foo")) _))(pos), List(elemQualifierA), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition `r:ole`") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(elemQualifierA), Seq(literalRColonOle))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword `A B` (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(ast.ElementQualifier("A B") _), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword A, B (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(elemQualifierA, elemQualifierB), Seq(literalRole))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword A, B (*) $preposition $$role1, $$role2") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo))(pos), List(elemQualifierA, elemQualifierB), Seq(paramRole1, paramRole2))
                parsing(s"$verb TRAVERSE ON $graphKeyword foo, baz $elementKeyword A (*) $preposition role")
                  func(ast.GraphPrivilege(ast.TraverseAction, List(graphScopeFoo, graphScopeBaz))(pos), List(elemQualifierA), Seq(paramRole))
              }

              test( s"traverseParsingErrors $verb $graphKeyword $elementKeyword $preposition") {
                assertFails(s"$verb TRAVERSE $graphKeyword * $elementKeyword * (*) $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword A B (*) $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword A (foo) $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword $elementKeyword * $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword $elementKeyword A $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword $elementKeyword * (*) $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword $elementKeyword A (*) $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition r:ole")
                assertFails(s"$verb TRAVERSE ON $graphKeyword 2foo $elementKeyword A (*) $preposition role")
                assertFails(s"$verb TRAVERSE ON $graphKeyword * $elementKeyword * (*)")
              }
          }
      }

      // Mix of specific graph and *

      test(s"$verb TRAVERSE ON GRAPH foo, * $preposition role") {
        failsToParse
      }

      test(s"$verb TRAVERSE ON GRAPH *, foo $preposition role") {
        failsToParse
      }

      // Database instead of graph keyword

      test(s"$verb TRAVERSE ON DATABASES * $preposition role") {
        failsToParse
      }

      test(s"$verb TRAVERSE ON DATABASE foo $preposition role") {
        failsToParse
      }

      test(s"$verb TRAVERSE ON DEFAULT DATABASE $preposition role") {
        failsToParse
      }
  }
}
