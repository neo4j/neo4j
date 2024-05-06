/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.ast.factory.neo4j.privilege

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.neo4j.AdministrationAndSchemaCommandParserTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Antlr
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.JavaCc
import org.neo4j.exceptions.SyntaxException

class TraversePrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

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
          test(s"$verb$immutableString TRAVERSE ON HOME GRAPH $preposition role") {
            parsesTo[Statements](func(
              ast.GraphPrivilege(ast.TraverseAction, ast.HomeGraphScope()(_))(pos),
              List(ast.ElementsAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRAVERSE ON HOME GRAPH NODE A $preposition role") {
            parsesTo[Statements](func(
              ast.GraphPrivilege(ast.TraverseAction, ast.HomeGraphScope()(_))(pos),
              List(labelQualifierA),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRAVERSE ON HOME GRAPH RELATIONSHIP * $preposition role") {
            parsesTo[Statements](func(
              ast.GraphPrivilege(ast.TraverseAction, ast.HomeGraphScope()(_))(pos),
              List(ast.RelationshipAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRAVERSE ON HOME GRAPH ELEMENT A $preposition role") {
            parsesTo[Statements](func(
              ast.GraphPrivilege(ast.TraverseAction, ast.HomeGraphScope()(_))(pos),
              List(elemQualifierA),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRAVERSE ON DEFAULT GRAPH $preposition role") {
            parsesTo[Statements](func(
              ast.GraphPrivilege(ast.TraverseAction, ast.DefaultGraphScope()(_))(pos),
              List(ast.ElementsAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRAVERSE ON DEFAULT GRAPH NODE A $preposition role") {
            parsesTo[Statements](func(
              ast.GraphPrivilege(ast.TraverseAction, ast.DefaultGraphScope()(_))(pos),
              List(labelQualifierA),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRAVERSE ON DEFAULT GRAPH RELATIONSHIP * $preposition role") {
            parsesTo[Statements](func(
              ast.GraphPrivilege(ast.TraverseAction, ast.DefaultGraphScope()(_))(pos),
              List(ast.RelationshipAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRAVERSE ON DEFAULT GRAPH ELEMENT A $preposition role") {
            parsesTo[Statements](func(
              ast.GraphPrivilege(ast.TraverseAction, ast.DefaultGraphScope()(_))(pos),
              List(elemQualifierA),
              Seq(literalRole),
              immutable
            )(pos))
          }

          Seq("GRAPH", "GRAPHS").foreach {
            graphKeyword =>
              test(s"$verb$immutableString TRAVERSE ON $graphKeyword * $preposition $$role") {
                parsesTo[Statements](func(
                  ast.GraphPrivilege(ast.TraverseAction, ast.AllGraphsScope() _)(pos),
                  List(ast.ElementsAllQualifier() _),
                  Seq(paramRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString TRAVERSE ON $graphKeyword foo $preposition role") {
                parsesTo[Statements](func(
                  ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                  List(ast.ElementsAllQualifier() _),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString TRAVERSE ON $graphKeyword $$foo $preposition role") {
                parsesTo[Statements](func(
                  ast.GraphPrivilege(ast.TraverseAction, graphScopeParamFoo)(pos),
                  List(ast.ElementsAllQualifier() _),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              Seq("NODE", "NODES").foreach {
                nodeKeyword =>
                  test(s"validExpressions $verb$immutableString $graphKeyword $nodeKeyword $preposition") {
                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $nodeKeyword * $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, ast.AllGraphsScope() _)(pos),
                          List(ast.LabelAllQualifier() _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $nodeKeyword * (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, ast.AllGraphsScope() _)(pos),
                          List(ast.LabelAllQualifier() _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $nodeKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, ast.AllGraphsScope() _)(pos),
                          List(labelQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $nodeKeyword A (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, ast.AllGraphsScope() _)(pos),
                          List(labelQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword `*` $nodeKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, ast.NamedGraphsScope(Seq(literal("*"))) _)(pos),
                          List(labelQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword * $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(ast.LabelAllQualifier() _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword * (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(ast.LabelAllQualifier() _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(labelQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(labelQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition role1, $$role2" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(labelQualifierA),
                          Seq(literalRole1, paramRole2),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword `2foo` $nodeKeyword A (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, ast.NamedGraphsScope(Seq(literal("2foo"))) _)(pos),
                          List(labelQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition `r:ole`" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(labelQualifierA),
                          Seq(literalRColonOle),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword `A B` (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(ast.LabelQualifier("A B") _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A, B (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(labelQualifierA, labelQualifierB),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A, B (*) $preposition role1, role2" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(labelQualifierA, labelQualifierB),
                          Seq(literalRole1, literalRole2),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo, baz $nodeKeyword A (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFooBaz)(pos),
                          List(labelQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                  }

                  test(s"traverseParsingErrors $verb$immutableString $graphKeyword $nodeKeyword $preposition") {
                    s"$verb$immutableString TRAVERSE $graphKeyword * $nodeKeyword * (*) $preposition role" should
                      notParse[Statements]
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A B (*) $preposition role" should
                      notParse[Statements]
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A (foo) $preposition role" should
                      notParse[Statements]
                    s"$verb$immutableString TRAVERSE ON $graphKeyword $nodeKeyword * $preposition role" should
                      notParse[Statements]
                    s"$verb$immutableString TRAVERSE ON $graphKeyword $nodeKeyword A $preposition role" should
                      notParse[Statements]
                    s"$verb$immutableString TRAVERSE ON $graphKeyword $nodeKeyword * (*) $preposition role" should
                      notParse[Statements]
                    s"$verb$immutableString TRAVERSE ON $graphKeyword $nodeKeyword A (*) $preposition role" should
                      notParse[Statements]
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition r:ole" should
                      notParse[Statements]
                    s"$verb$immutableString TRAVERSE ON $graphKeyword 2foo $nodeKeyword A (*) $preposition role" should
                      notParse[Statements]
                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $nodeKeyword * (*)" should notParse[Statements]
                  }
              }

              Seq("RELATIONSHIP", "RELATIONSHIPS").foreach {
                relTypeKeyword =>
                  test(s"validExpressions $verb$immutableString $graphKeyword $relTypeKeyword $preposition") {
                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $relTypeKeyword * $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, ast.AllGraphsScope() _)(pos),
                          List(ast.RelationshipAllQualifier() _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $relTypeKeyword * (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, ast.AllGraphsScope() _)(pos),
                          List(ast.RelationshipAllQualifier() _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $relTypeKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, ast.AllGraphsScope() _)(pos),
                          List(relQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $relTypeKeyword A (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, ast.AllGraphsScope() _)(pos),
                          List(relQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword `*` $relTypeKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, ast.NamedGraphsScope(Seq(literal("*"))) _)(pos),
                          List(relQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword * $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(ast.RelationshipAllQualifier() _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword * (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(ast.RelationshipAllQualifier() _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(relQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(relQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition $$role1, role2" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(relQualifierA),
                          Seq(paramRole1, literalRole2),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword `2foo` $relTypeKeyword A (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, ast.NamedGraphsScope(Seq(literal("2foo"))) _)(pos),
                          List(relQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition `r:ole`" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(relQualifierA),
                          Seq(literalRColonOle),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword `A B` (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(ast.RelationshipQualifier("A B") _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A, B (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(relQualifierA, relQualifierB),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A, B (*) $preposition role1, role2" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(relQualifierA, relQualifierB),
                          Seq(literalRole1, literalRole2),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo, baz $relTypeKeyword A (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFooBaz)(pos),
                          List(relQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                  }

                  test(s"traverseParsingErrors$verb$immutableString $graphKeyword $relTypeKeyword $preposition") {

                    s"$verb$immutableString TRAVERSE $graphKeyword * $relTypeKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A B (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A (foo) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword $relTypeKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword $relTypeKeyword A $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword $relTypeKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword $relTypeKeyword A (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition r:ole" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword 2foo $relTypeKeyword A (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $relTypeKeyword * (*)" should
                      notParse[Statements]
                  }
              }

              Seq("ELEMENT", "ELEMENTS").foreach {
                elementKeyword =>
                  test(s"validExpressions $verb$immutableString $graphKeyword $elementKeyword $preposition") {
                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $elementKeyword * $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, ast.AllGraphsScope() _)(pos),
                          List(ast.ElementsAllQualifier() _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $elementKeyword * (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, ast.AllGraphsScope() _)(pos),
                          List(ast.ElementsAllQualifier() _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $elementKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, ast.AllGraphsScope() _)(pos),
                          List(elemQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $elementKeyword A (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, ast.AllGraphsScope() _)(pos),
                          List(elemQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword `*` $elementKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, ast.NamedGraphsScope(Seq(literal("*"))) _)(pos),
                          List(elemQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword * $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(ast.ElementsAllQualifier() _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword * (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(ast.ElementsAllQualifier() _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(elemQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(elemQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition role1, role2" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(elemQualifierA),
                          Seq(literalRole1, literalRole2),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword `2foo` $elementKeyword A (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, ast.NamedGraphsScope(Seq(literal("2foo"))) _)(pos),
                          List(elemQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition `r:ole`" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(elemQualifierA),
                          Seq(literalRColonOle),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword `A B` (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(ast.ElementQualifier("A B") _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A, B (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(elemQualifierA, elemQualifierB),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A, B (*) $preposition $$role1, $$role2" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFoo)(pos),
                          List(elemQualifierA, elemQualifierB),
                          Seq(paramRole1, paramRole2),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo, baz $elementKeyword A (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          ast.GraphPrivilege(ast.TraverseAction, graphScopeFooBaz)(pos),
                          List(elemQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                  }

                  test(s"traverseParsingErrors $verb$immutableString $graphKeyword $elementKeyword $preposition") {

                    s"$verb$immutableString TRAVERSE $graphKeyword * $elementKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A B (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A (foo) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword $elementKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword $elementKeyword A $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword $elementKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword $elementKeyword A (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition r:ole" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword 2foo $elementKeyword A (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $elementKeyword * (*)" should
                      notParse[Statements]
                  }
              }
          }

          // Mix of specific graph and *

          test(s"$verb$immutableString TRAVERSE ON GRAPH foo, * $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString TRAVERSE ON GRAPH *, foo $preposition role") {
            failsParsing[Statements]
          }

          // Database instead of graph keyword

          test(s"$verb$immutableString TRAVERSE ON DATABASES * $preposition role") {
            val offset = verb.length + immutableString.length + 13
            failsParsing[Statements]
              .parseIn(JavaCc)(_.withMessage(
                s"""Invalid input 'DATABASES': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
              ))
              .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                s"""Invalid input 'DATABASES': expected 'GRAPH', 'DEFAULT GRAPH', 'HOME GRAPH' or 'GRAPHS' (line 1, column ${offset + 1} (offset: $offset))"""
              ))
          }

          test(s"$verb$immutableString TRAVERSE ON DATABASE foo $preposition role") {
            val offset = verb.length + immutableString.length + 13
            failsParsing[Statements]
              .parseIn(JavaCc)(_.withMessage(
                s"""Invalid input 'DATABASE': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
              ))
              .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                s"""Invalid input 'DATABASE': expected 'GRAPH', 'DEFAULT GRAPH', 'HOME GRAPH' or 'GRAPHS' (line 1, column ${offset + 1} (offset: $offset))"""
              ))
          }

          test(s"$verb$immutableString TRAVERSE ON HOME DATABASE $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString TRAVERSE ON DEFAULT DATABASE $preposition role") {
            failsParsing[Statements]
          }
      }
  }
}
