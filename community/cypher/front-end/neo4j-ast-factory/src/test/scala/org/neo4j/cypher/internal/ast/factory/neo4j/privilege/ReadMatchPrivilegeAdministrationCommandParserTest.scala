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

class ReadMatchPrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {
  // Granting/denying/revoking read and match to/from role

  Seq(
    (ast.ReadAction, "GRANT", "TO", grantGraphPrivilege: resourcePrivilegeFunc),
    (ast.ReadAction, "DENY", "TO", denyGraphPrivilege: resourcePrivilegeFunc),
    (ast.ReadAction, "REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: resourcePrivilegeFunc),
    (ast.ReadAction, "REVOKE DENY", "FROM", revokeDenyGraphPrivilege: resourcePrivilegeFunc),
    (ast.ReadAction, "REVOKE", "FROM", revokeGraphPrivilege: resourcePrivilegeFunc),
    (ast.MatchAction, "GRANT", "TO", grantGraphPrivilege: resourcePrivilegeFunc),
    (ast.MatchAction, "DENY", "TO", denyGraphPrivilege: resourcePrivilegeFunc),
    (ast.MatchAction, "REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: resourcePrivilegeFunc),
    (ast.MatchAction, "REVOKE DENY", "FROM", revokeDenyGraphPrivilege: resourcePrivilegeFunc),
    (ast.MatchAction, "REVOKE", "FROM", revokeGraphPrivilege: resourcePrivilegeFunc)
  ).foreach {
    case (action: ast.GraphAction, verb: String, preposition: String, func: resourcePrivilegeFunc) =>
      Seq[Immutable](true, false).foreach {
        immutable =>
          val immutableString = immutableOrEmpty(immutable)
          test(s"$verb$immutableString ${action.name} { prop } ON HOME GRAPH $preposition role") {
            yields[Statements](func(
              ast.GraphPrivilege(action, ast.HomeGraphScope()(pos))(pos),
              ast.PropertiesResource(propSeq)(pos),
              List(ast.ElementsAllQualifier() _),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString ${action.name} { prop } ON HOME GRAPH NODE A $preposition role") {
            yields[Statements](func(
              ast.GraphPrivilege(action, ast.HomeGraphScope()(pos))(pos),
              ast.PropertiesResource(propSeq)(pos),
              List(labelQualifierA),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString ${action.name} { prop } ON DEFAULT GRAPH $preposition role") {
            yields[Statements](func(
              ast.GraphPrivilege(action, ast.DefaultGraphScope()(pos))(pos),
              ast.PropertiesResource(propSeq)(pos),
              List(ast.ElementsAllQualifier() _),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString ${action.name} { prop } ON DEFAULT GRAPH NODE A $preposition role") {
            yields[Statements](func(
              ast.GraphPrivilege(action, ast.DefaultGraphScope()(pos))(pos),
              ast.PropertiesResource(propSeq)(pos),
              List(labelQualifierA),
              Seq(literalRole),
              immutable
            ))
          }

          Seq("GRAPH", "GRAPHS").foreach {
            graphKeyword =>
              Seq("NODE", "NODES").foreach {
                nodeKeyword =>
                  Seq(
                    ("*", ast.AllPropertyResource()(pos), "*", ast.AllGraphsScope()(pos)),
                    ("*", ast.AllPropertyResource()(pos), "foo", graphScopeFoo(pos)),
                    ("bar", ast.PropertiesResource(Seq("bar"))(pos), "*", ast.AllGraphsScope()(pos)),
                    ("bar", ast.PropertiesResource(Seq("bar"))(pos), "foo", graphScopeFoo(pos)),
                    ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "*", ast.AllGraphsScope()(pos)),
                    ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "foo", graphScopeFoo(pos))
                  ).foreach {
                    case (
                        properties: String,
                        resource: ast.ActionResource,
                        graphName: String,
                        graphScope: ast.GraphScope
                      ) =>
                      test(
                        s"validExpressions $verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $nodeKeyword $preposition"
                      ) {
                        parsing[Statements](
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword * $preposition $$role"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(ast.LabelAllQualifier() _),
                            Seq(paramRole),
                            immutable
                          )
                        parsing(
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword * (*) $preposition role"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(ast.LabelAllQualifier() _),
                            Seq(literalRole),
                            immutable
                          )
                        parsing(
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword A $preposition role"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(labelQualifierA),
                            Seq(literalRole),
                            immutable
                          )
                        parsing(
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword A (*) $preposition role"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(labelQualifierA),
                            Seq(literalRole),
                            immutable
                          )
                        parsing(
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword `A B` (*) $preposition role"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(ast.LabelQualifier("A B") _),
                            Seq(literalRole),
                            immutable
                          )
                        parsing(
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword A, B (*) $preposition role1, $$role2"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(labelQualifierA, labelQualifierB),
                            Seq(literalRole1, paramRole2),
                            immutable
                          )
                        parsing(
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword * $preposition `r:ole`"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(ast.LabelAllQualifier() _),
                            Seq(literalRColonOle),
                            immutable
                          )
                        parsing(
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword `:A` (*) $preposition role"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(ast.LabelQualifier(":A") _),
                            Seq(literalRole),
                            immutable
                          )
                      }

                      test(
                        s"failToParse $verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $nodeKeyword $preposition"
                      ) {
                        assertFails[Statements](
                          s"$verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $nodeKeyword * (*) $preposition role"
                        )
                        assertFails[Statements](
                          s"$verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $nodeKeyword A $preposition role"
                        )
                        assertFails[Statements](
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword * (*)"
                        )
                        assertFails[Statements](
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword A B (*) $preposition role"
                        )
                        assertFails[Statements](
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword A (foo) $preposition role"
                        )
                        assertFails[Statements](
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword * $preposition r:ole"
                        )
                      }
                  }

                  test(
                    s"validExpressions $verb$immutableString ${action.name} $graphKeyword $nodeKeyword $preposition"
                  ) {
                    parsing(
                      s"$verb$immutableString ${action.name} {*} ON $graphKeyword `f:oo` $nodeKeyword * $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(action, ast.NamedGraphsScope(Seq(namespacedName("f:oo"))) _)(pos),
                        ast.AllPropertyResource() _,
                        List(ast.LabelAllQualifier() _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString ${action.name} {bar} ON $graphKeyword `f:oo` $nodeKeyword * $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(action, ast.NamedGraphsScope(Seq(namespacedName("f:oo"))) _)(pos),
                        ast.PropertiesResource(Seq("bar")) _,
                        List(ast.LabelAllQualifier() _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString ${action.name} {`b:ar`} ON $graphKeyword foo $nodeKeyword * $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(action, graphScopeFoo)(pos),
                        ast.PropertiesResource(Seq("b:ar")) _,
                        List(ast.LabelAllQualifier() _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString ${action.name} {*} ON $graphKeyword foo, baz $nodeKeyword A (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(action, graphScopeFooBaz)(pos),
                        ast.AllPropertyResource() _,
                        List(labelQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString ${action.name} {bar} ON $graphKeyword foo, baz $nodeKeyword A (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(action, graphScopeFooBaz)(pos),
                        ast.PropertiesResource(Seq("bar")) _,
                        List(labelQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                  }

                  test(
                    s"parsingFailures $verb$immutableString ${action.name} $graphKeyword $nodeKeyword $preposition"
                  ) {
                    // Invalid graph name
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {*} ON $graphKeyword f:oo $nodeKeyword * $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {bar} ON $graphKeyword f:oo $nodeKeyword * $preposition role"
                    )
                    // mixing specific graph and *
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {*} ON $graphKeyword foo, * $nodeKeyword * $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {*} ON $graphKeyword *, foo $nodeKeyword * $preposition role"
                    )
                    // invalid property definition
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {b:ar} ON $graphKeyword foo $nodeKeyword * $preposition role"
                    )
                    // missing graph name
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {*} ON $graphKeyword $nodeKeyword * $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {*} ON $graphKeyword $nodeKeyword * (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {*} ON $graphKeyword $nodeKeyword A $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {*} ON $graphKeyword $nodeKeyword A (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $nodeKeyword * $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $nodeKeyword * (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $nodeKeyword A $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $nodeKeyword A (*) $preposition role"
                    )
                    // missing property definition
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword * $nodeKeyword * $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword * $nodeKeyword * (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword * $nodeKeyword A $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword * $nodeKeyword A (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword foo $nodeKeyword * $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword foo $nodeKeyword * (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword foo $nodeKeyword A $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword foo $nodeKeyword A (*) $preposition role"
                    )
                    // missing property list
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword * $nodeKeyword * $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword * $nodeKeyword * (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword * $nodeKeyword A $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword * $nodeKeyword A (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $nodeKeyword * $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $nodeKeyword * (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $nodeKeyword A $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $nodeKeyword A (*) $preposition role"
                    )
                  }
              }

              Seq("RELATIONSHIP", "RELATIONSHIPS").foreach {
                relTypeKeyword =>
                  Seq(
                    ("*", ast.AllPropertyResource()(pos), "*", ast.AllGraphsScope()(pos)),
                    ("*", ast.AllPropertyResource()(pos), "foo", graphScopeFoo(pos)),
                    ("bar", ast.PropertiesResource(Seq("bar"))(pos), "*", ast.AllGraphsScope()(pos)),
                    ("bar", ast.PropertiesResource(Seq("bar"))(pos), "foo", graphScopeFoo(pos)),
                    ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "*", ast.AllGraphsScope()(pos)),
                    ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "foo", graphScopeFoo(pos))
                  ).foreach {
                    case (
                        properties: String,
                        resource: ast.ActionResource,
                        graphName: String,
                        graphScope: ast.GraphScope
                      ) =>
                      test(
                        s"validExpressions $verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $relTypeKeyword $preposition"
                      ) {
                        parsing(
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword * $preposition role"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(ast.RelationshipAllQualifier() _),
                            Seq(literalRole),
                            immutable
                          )
                        parsing(
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword * (*) $preposition $$role"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(ast.RelationshipAllQualifier() _),
                            Seq(paramRole),
                            immutable
                          )
                        parsing(
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword A $preposition role"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(relQualifierA),
                            Seq(literalRole),
                            immutable
                          )
                        parsing(
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword A (*) $preposition role"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(relQualifierA),
                            Seq(literalRole),
                            immutable
                          )
                        parsing(
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword `A B` (*) $preposition role"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(ast.RelationshipQualifier("A B") _),
                            Seq(literalRole),
                            immutable
                          )
                        parsing(
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword A, B (*) $preposition $$role1, role2"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(relQualifierA, relQualifierB),
                            Seq(paramRole1, literalRole2),
                            immutable
                          )
                        parsing(
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword * $preposition `r:ole`"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(ast.RelationshipAllQualifier() _),
                            Seq(literalRColonOle),
                            immutable
                          )
                        parsing(
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword `:A` (*) $preposition role"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(ast.RelationshipQualifier(":A") _),
                            Seq(literalRole),
                            immutable
                          )
                      }

                      test(
                        s"parsingFailures $verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $relTypeKeyword $preposition"
                      ) {
                        assertFails[Statements](
                          s"$verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $relTypeKeyword * (*) $preposition role"
                        )
                        assertFails[Statements](
                          s"$verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $relTypeKeyword A $preposition role"
                        )
                        assertFails[Statements](
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword * (*)"
                        )
                        assertFails[Statements](
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword A B (*) $preposition role"
                        )
                        assertFails[Statements](
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword A (foo) $preposition role"
                        )
                        assertFails[Statements](
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword * $preposition r:ole"
                        )
                      }
                  }

                  test(
                    s"validExpressions $verb$immutableString ${action.name} $graphKeyword $relTypeKeyword $preposition"
                  ) {
                    parsing(
                      s"$verb$immutableString ${action.name} {*} ON $graphKeyword `f:oo` $relTypeKeyword * $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(action, ast.NamedGraphsScope(Seq(namespacedName("f:oo"))) _)(pos),
                        ast.AllPropertyResource() _,
                        List(ast.RelationshipAllQualifier() _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString ${action.name} {bar} ON $graphKeyword `f:oo` $relTypeKeyword * $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(action, ast.NamedGraphsScope(Seq(namespacedName("f:oo"))) _)(pos),
                        ast.PropertiesResource(Seq("bar")) _,
                        List(ast.RelationshipAllQualifier() _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString ${action.name} {`b:ar`} ON $graphKeyword foo $relTypeKeyword * $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(action, graphScopeFoo)(pos),
                        ast.PropertiesResource(Seq("b:ar")) _,
                        List(ast.RelationshipAllQualifier() _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString ${action.name} {*} ON $graphKeyword foo, baz $relTypeKeyword A (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(action, graphScopeFooBaz)(pos),
                        ast.AllPropertyResource() _,
                        List(relQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString ${action.name} {bar} ON $graphKeyword foo, baz $relTypeKeyword A (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(action, graphScopeFooBaz)(pos),
                        ast.PropertiesResource(Seq("bar")) _,
                        List(relQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                  }

                  test(
                    s"parsingFailures$verb$immutableString ${action.name} $graphKeyword $relTypeKeyword $preposition"
                  ) {
                    // Invalid graph name
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {*} ON $graphKeyword f:oo $relTypeKeyword * $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {bar} ON $graphKeyword f:oo $relTypeKeyword * $preposition role"
                    )
                    // invalid property definition
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {b:ar} ON $graphKeyword foo $relTypeKeyword * $preposition role"
                    )
                    // missing graph name
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {*} ON $graphKeyword $relTypeKeyword * $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {*} ON $graphKeyword $relTypeKeyword * (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {*} ON $graphKeyword $relTypeKeyword A $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {*} ON $graphKeyword $relTypeKeyword A (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $relTypeKeyword * $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $relTypeKeyword * (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $relTypeKeyword A $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $relTypeKeyword A (*) $preposition role"
                    )
                    // missing property definition
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword * $relTypeKeyword * $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword * $relTypeKeyword * (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword * $relTypeKeyword A $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword * $relTypeKeyword A (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword foo $relTypeKeyword * $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword foo $relTypeKeyword * (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword foo $relTypeKeyword A $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword foo $relTypeKeyword A (*) $preposition role"
                    )
                    // missing property list
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword * $relTypeKeyword * $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword * $relTypeKeyword * (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword * $relTypeKeyword A $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword * $relTypeKeyword A (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $relTypeKeyword * $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $relTypeKeyword * (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $relTypeKeyword A $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $relTypeKeyword A (*) $preposition role"
                    )
                  }
              }

              Seq("ELEMENT", "ELEMENTS").foreach {
                elementKeyword =>
                  Seq(
                    ("*", ast.AllPropertyResource()(pos), "*", ast.AllGraphsScope()(pos)),
                    ("*", ast.AllPropertyResource()(pos), "foo", graphScopeFoo(pos)),
                    ("bar", ast.PropertiesResource(Seq("bar"))(pos), "*", ast.AllGraphsScope()(pos)),
                    ("bar", ast.PropertiesResource(Seq("bar"))(pos), "foo", graphScopeFoo(pos)),
                    ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "*", ast.AllGraphsScope()(pos)),
                    ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "foo", graphScopeFoo(pos))
                  ).foreach {
                    case (
                        properties: String,
                        resource: ast.ActionResource,
                        graphName: String,
                        graphScope: ast.GraphScope
                      ) =>
                      test(
                        s"validExpressions $verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $elementKeyword $preposition"
                      ) {
                        parsing(
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword * $preposition role"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(ast.ElementsAllQualifier() _),
                            Seq(literalRole),
                            immutable
                          )
                        parsing(
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword * (*) $preposition role"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(ast.ElementsAllQualifier() _),
                            Seq(literalRole),
                            immutable
                          )
                        parsing(
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword A $preposition $$role"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(elemQualifierA),
                            Seq(paramRole),
                            immutable
                          )
                        parsing(
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword A (*) $preposition role"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(elemQualifierA),
                            Seq(literalRole),
                            immutable
                          )
                        parsing(
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword `A B` (*) $preposition role"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(ast.ElementQualifier("A B") _),
                            Seq(literalRole),
                            immutable
                          )
                        parsing(
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword A, B (*) $preposition $$role1, $$role2"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(elemQualifierA, elemQualifierB),
                            Seq(paramRole1, paramRole2),
                            immutable
                          )
                        parsing(
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword * $preposition `r:ole`"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(ast.ElementsAllQualifier() _),
                            Seq(literalRColonOle),
                            immutable
                          )
                        parsing(
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword `:A` (*) $preposition role"
                        ) shouldGive
                          func(
                            ast.GraphPrivilege(action, graphScope)(pos),
                            resource,
                            List(ast.ElementQualifier(":A") _),
                            Seq(literalRole),
                            immutable
                          )
                      }

                      test(
                        s"parsingFailures$verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $elementKeyword $preposition"
                      ) {
                        assertFails[Statements](
                          s"$verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $elementKeyword * (*) $preposition role"
                        )
                        assertFails[Statements](
                          s"$verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $elementKeyword A $preposition role"
                        )
                        assertFails[Statements](
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword * (*)"
                        )
                        assertFails[Statements](
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword A B (*) $preposition role"
                        )
                        assertFails[Statements](
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword A (foo) $preposition role"
                        )
                        assertFails[Statements](
                          s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword * $preposition r:ole"
                        )
                      }
                  }

                  test(
                    s"validExpressions $verb$immutableString ${action.name} $graphKeyword $elementKeyword $preposition"
                  ) {
                    parsing(
                      s"$verb$immutableString ${action.name} {*} ON $graphKeyword `f:oo` $elementKeyword * $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(action, ast.NamedGraphsScope(Seq(namespacedName("f:oo"))) _)(pos),
                        ast.AllPropertyResource() _,
                        List(ast.ElementsAllQualifier() _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString ${action.name} {bar} ON $graphKeyword `f:oo` $elementKeyword * $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(action, ast.NamedGraphsScope(Seq(namespacedName("f:oo"))) _)(pos),
                        ast.PropertiesResource(Seq("bar")) _,
                        List(ast.ElementsAllQualifier() _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString ${action.name} {`b:ar`} ON $graphKeyword foo $elementKeyword * $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(action, graphScopeFoo)(pos),
                        ast.PropertiesResource(Seq("b:ar")) _,
                        List(ast.ElementsAllQualifier() _),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString ${action.name} {*} ON $graphKeyword foo, baz $elementKeyword A (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(action, graphScopeFooBaz)(pos),
                        ast.AllPropertyResource() _,
                        List(elemQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                    parsing(
                      s"$verb$immutableString ${action.name} {bar} ON $graphKeyword foo, baz $elementKeyword A (*) $preposition role"
                    ) shouldGive
                      func(
                        ast.GraphPrivilege(action, graphScopeFooBaz)(pos),
                        ast.PropertiesResource(Seq("bar")) _,
                        List(elemQualifierA),
                        Seq(literalRole),
                        immutable
                      )
                  }

                  test(
                    s"parsingFailures $verb$immutableString ${action.name} $graphKeyword $elementKeyword $preposition"
                  ) {
                    // Invalid graph name
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {*} ON $graphKeyword f:oo $elementKeyword * $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {bar} ON $graphKeyword f:oo $elementKeyword * $preposition role"
                    )
                    // invalid property definition
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {b:ar} ON $graphKeyword foo $elementKeyword * $preposition role"
                    )
                    // missing graph name
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {*} ON $graphKeyword $elementKeyword * $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {*} ON $graphKeyword $elementKeyword * (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {*} ON $graphKeyword $elementKeyword A $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {*} ON $graphKeyword $elementKeyword A (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $elementKeyword * $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $elementKeyword * (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $elementKeyword A $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $elementKeyword A (*) $preposition role"
                    )
                    // missing property definition
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword * $elementKeyword * $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword * $elementKeyword * (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword * $elementKeyword A $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword * $elementKeyword A (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword foo $elementKeyword * $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword foo $elementKeyword * (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword foo $elementKeyword A $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} ON $graphKeyword foo $elementKeyword A (*) $preposition role"
                    )
                    // missing property list
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword * $elementKeyword * $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword * $elementKeyword * (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword * $elementKeyword A $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword * $elementKeyword A (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $elementKeyword * $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $elementKeyword * (*) $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $elementKeyword A $preposition role"
                    )
                    assertFails[Statements](
                      s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $elementKeyword A (*) $preposition role"
                    )
                  }
              }

              // Needs to be separate loop to avoid duplicate tests since the test does not have any segment keyword
              Seq(
                ("*", ast.AllPropertyResource()(pos), "*", ast.AllGraphsScope()(pos)),
                ("*", ast.AllPropertyResource()(pos), "foo", graphScopeFoo(pos)),
                ("*", ast.AllPropertyResource()(pos), "$foo", graphScopeParamFoo(pos)),
                ("bar", ast.PropertiesResource(Seq("bar"))(pos), "*", ast.AllGraphsScope()(pos)),
                ("bar", ast.PropertiesResource(Seq("bar"))(pos), "foo", graphScopeFoo(pos)),
                ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "*", ast.AllGraphsScope()(pos)),
                ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "foo", graphScopeFoo(pos))
              ).foreach {
                case (
                    properties: String,
                    resource: ast.ActionResource,
                    graphName: String,
                    graphScope: ast.GraphScope
                  ) =>
                  test(
                    s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $preposition role"
                  ) {
                    yields[Statements](func(
                      ast.GraphPrivilege(action, graphScope)(pos),
                      resource,
                      List(ast.ElementsAllQualifier() _),
                      Seq(literalRole),
                      immutable
                    ))
                  }
              }
          }

          // Database instead of graph keyword

          test(s"$verb$immutableString ${action.name} ON DATABASES * $preposition role") {
            failsToParse[Statements]
          }

          test(s"$verb$immutableString ${action.name} ON DATABASE foo $preposition role") {
            failsToParse[Statements]
          }

          test(s"$verb$immutableString ${action.name} ON HOME DATABASE $preposition role") {
            failsToParse[Statements]
          }

          test(s"$verb$immutableString ${action.name} ON DEFAULT DATABASE $preposition role") {
            failsToParse[Statements]
          }
      }
  }
}
