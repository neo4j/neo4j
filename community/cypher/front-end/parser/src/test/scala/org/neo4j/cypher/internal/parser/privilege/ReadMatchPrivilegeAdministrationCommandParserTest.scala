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

class ReadMatchPrivilegeAdministrationCommandParserTest extends AdministrationCommandParserTestBase {

  // Granting/denying/revoking read and match to/from role

  Seq(
    (ast.GraphPrivilege(ast.ReadAction)(pos), "GRANT", "TO", grantGraphPrivilege: resourcePrivilegeFunc),
    (ast.GraphPrivilege(ast.ReadAction)(pos), "DENY", "TO", denyGraphPrivilege: resourcePrivilegeFunc),
    (ast.GraphPrivilege(ast.ReadAction)(pos), "REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: resourcePrivilegeFunc),
    (ast.GraphPrivilege(ast.ReadAction)(pos), "REVOKE DENY", "FROM", revokeDenyGraphPrivilege: resourcePrivilegeFunc),
    (ast.GraphPrivilege(ast.ReadAction)(pos), "REVOKE", "FROM", revokeGraphPrivilege: resourcePrivilegeFunc),
    (ast.GraphPrivilege(ast.MatchAction)(pos), "GRANT", "TO", grantGraphPrivilege: resourcePrivilegeFunc),
    (ast.GraphPrivilege(ast.MatchAction)(pos), "DENY", "TO", denyGraphPrivilege: resourcePrivilegeFunc),
    (ast.GraphPrivilege(ast.MatchAction)(pos), "REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: resourcePrivilegeFunc),
    (ast.GraphPrivilege(ast.MatchAction)(pos), "REVOKE DENY", "FROM", revokeDenyGraphPrivilege: resourcePrivilegeFunc),
    (ast.GraphPrivilege(ast.MatchAction)(pos), "REVOKE", "FROM", revokeGraphPrivilege: resourcePrivilegeFunc)
  ).foreach {
    case (privilege: ast.PrivilegeType, verb: String, preposition: String, func: resourcePrivilegeFunc) =>

      test(s"$verb ${privilege.name} { prop } ON DEFAULT GRAPH $preposition role"){
        yields(func(privilege, ast.PropertiesResource(propSeq)(pos), List(ast.DefaultGraphScope()(pos)), List(ast.ElementsAllQualifier() _), Seq(literalRole)))
      }

      test(s"$verb ${privilege.name} { prop } ON DEFAULT GRAPH NODE A $preposition role"){
        yields(func(privilege, ast.PropertiesResource(propSeq)(pos), List(ast.DefaultGraphScope()(pos)), List(labelQualifierA), Seq(literalRole)))
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
                case (properties: String, resource: ast.ActionResource, graphName: String, graphScope: ast.GraphScope) =>
                  val graphScopes = List(graphScope)

                  test( s"validExpressions $verb ${privilege.name} {$properties} $graphKeyword $graphName $nodeKeyword $preposition") {
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $nodeKeyword * $preposition $$role") shouldGive
                      func(privilege, resource, graphScopes, List(ast.LabelAllQualifier() _), Seq(paramRole))
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $nodeKeyword * (*) $preposition role") shouldGive
                      func(privilege, resource, graphScopes, List(ast.LabelAllQualifier() _), Seq(literalRole))
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $nodeKeyword A $preposition role") shouldGive
                      func(privilege, resource, graphScopes, List(labelQualifierA), Seq(literalRole))
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $nodeKeyword A (*) $preposition role") shouldGive
                      func(privilege, resource, graphScopes, List(labelQualifierA), Seq(literalRole))
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $nodeKeyword `A B` (*) $preposition role") shouldGive
                      func(privilege, resource, graphScopes, List(ast.LabelQualifier("A B") _), Seq(literalRole))
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $nodeKeyword A, B (*) $preposition role1, $$role2") shouldGive
                      func(privilege, resource, graphScopes, List(labelQualifierA, labelQualifierB), Seq(literalRole1, paramRole2))
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $nodeKeyword * $preposition `r:ole`") shouldGive
                      func(privilege, resource, graphScopes, List(ast.LabelAllQualifier() _), Seq(literalRColonOle))
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $nodeKeyword `:A` (*) $preposition role") shouldGive
                      func(privilege, resource, graphScopes, List(ast.LabelQualifier(":A") _), Seq(literalRole))
                  }

                  test( s"failToParse $verb ${privilege.name} {$properties} $graphKeyword $graphName $nodeKeyword $preposition") {
                    assertFails(s"$verb ${privilege.name} {$properties} $graphKeyword $graphName $nodeKeyword * (*) $preposition role")
                    assertFails(s"$verb ${privilege.name} {$properties} $graphKeyword $graphName $nodeKeyword A $preposition role")
                    assertFails(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $nodeKeyword * (*)")
                    assertFails(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $nodeKeyword A B (*) $preposition role")
                    assertFails(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $nodeKeyword A (foo) $preposition role")
                    assertFails(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $nodeKeyword * $preposition r:ole")
                  }
              }

              test( s"validExpressions $verb ${privilege.name} $graphKeyword $nodeKeyword $preposition") {
                parsing(s"$verb ${privilege.name} {*} ON $graphKeyword `f:oo` $nodeKeyword * $preposition role") shouldGive
                  func(privilege, ast.AllPropertyResource() _, List(ast.NamedGraphScope(literalFColonOo) _), List(ast.LabelAllQualifier() _), Seq(literalRole))
                parsing(s"$verb ${privilege.name} {bar} ON $graphKeyword `f:oo` $nodeKeyword * $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("bar")) _, List(ast.NamedGraphScope(literalFColonOo) _), List(ast.LabelAllQualifier() _), Seq(literalRole))
                parsing(s"$verb ${privilege.name} {`b:ar`} ON $graphKeyword foo $nodeKeyword * $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("b:ar")) _, List(graphScopeFoo), List(ast.LabelAllQualifier() _), Seq(literalRole))
                parsing(s"$verb ${privilege.name} {*} ON $graphKeyword foo, baz $nodeKeyword A (*) $preposition role") shouldGive
                  func(privilege, ast.AllPropertyResource() _, List(graphScopeFoo, graphScopeBaz), List(labelQualifierA), Seq(literalRole))
                parsing(s"$verb ${privilege.name} {bar} ON $graphKeyword foo, baz $nodeKeyword A (*) $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("bar")) _, List(graphScopeFoo, graphScopeBaz), List(labelQualifierA), Seq(literalRole))
              }

              test( s"parsingFailures $verb ${privilege.name} $graphKeyword $nodeKeyword $preposition") {
                // Invalid graph name
                assertFails(s"$verb ${privilege.name} {*} ON $graphKeyword f:oo $nodeKeyword * $preposition role")
                assertFails(s"$verb ${privilege.name} {bar} ON $graphKeyword f:oo $nodeKeyword * $preposition role")
                // mixing specific graph and *
                assertFails(s"$verb ${privilege.name} {*} ON $graphKeyword foo, * $nodeKeyword * $preposition role")
                assertFails(s"$verb ${privilege.name} {*} ON $graphKeyword *, foo $nodeKeyword * $preposition role")
                // invalid property definition
                assertFails(s"$verb ${privilege.name} {b:ar} ON $graphKeyword foo $nodeKeyword * $preposition role")
                // missing graph name
                assertFails(s"$verb ${privilege.name} {*} ON $graphKeyword $nodeKeyword * $preposition role")
                assertFails(s"$verb ${privilege.name} {*} ON $graphKeyword $nodeKeyword * (*) $preposition role")
                assertFails(s"$verb ${privilege.name} {*} ON $graphKeyword $nodeKeyword A $preposition role")
                assertFails(s"$verb ${privilege.name} {*} ON $graphKeyword $nodeKeyword A (*) $preposition role")
                assertFails(s"$verb ${privilege.name} {bar} ON $graphKeyword $nodeKeyword * $preposition role")
                assertFails(s"$verb ${privilege.name} {bar} ON $graphKeyword $nodeKeyword * (*) $preposition role")
                assertFails(s"$verb ${privilege.name} {bar} ON $graphKeyword $nodeKeyword A $preposition role")
                assertFails(s"$verb ${privilege.name} {bar} ON $graphKeyword $nodeKeyword A (*) $preposition role")
                // missing property definition
                assertFails(s"$verb ${privilege.name} ON $graphKeyword * $nodeKeyword * $preposition role")
                assertFails(s"$verb ${privilege.name} ON $graphKeyword * $nodeKeyword * (*) $preposition role")
                assertFails(s"$verb ${privilege.name} ON $graphKeyword * $nodeKeyword A $preposition role")
                assertFails(s"$verb ${privilege.name} ON $graphKeyword * $nodeKeyword A (*) $preposition role")
                assertFails(s"$verb ${privilege.name} ON $graphKeyword foo $nodeKeyword * $preposition role")
                assertFails(s"$verb ${privilege.name} ON $graphKeyword foo $nodeKeyword * (*) $preposition role")
                assertFails(s"$verb ${privilege.name} ON $graphKeyword foo $nodeKeyword A $preposition role")
                assertFails(s"$verb ${privilege.name} ON $graphKeyword foo $nodeKeyword A (*) $preposition role")
                // missing property list
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword * $nodeKeyword * $preposition role")
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword * $nodeKeyword * (*) $preposition role")
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword * $nodeKeyword A $preposition role")
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword * $nodeKeyword A (*) $preposition role")
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword foo $nodeKeyword * $preposition role")
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword foo $nodeKeyword * (*) $preposition role")
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword foo $nodeKeyword A $preposition role")
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword foo $nodeKeyword A (*) $preposition role")
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
                case (properties: String, resource: ast.ActionResource, graphName: String, graphScope: ast.GraphScope) =>
                  val graphScopes = List(graphScope)

                  test( s"validExpressions $verb ${privilege.name} {$properties} $graphKeyword $graphName $relTypeKeyword $preposition") {
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword * $preposition role") shouldGive
                      func(privilege, resource, graphScopes, List(ast.RelationshipAllQualifier() _), Seq(literalRole))
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword * (*) $preposition $$role") shouldGive
                      func(privilege, resource, graphScopes, List(ast.RelationshipAllQualifier() _), Seq(paramRole))
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword A $preposition role") shouldGive
                      func(privilege, resource, graphScopes, List(relQualifierA), Seq(literalRole))
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword A (*) $preposition role") shouldGive
                      func(privilege, resource, graphScopes, List(relQualifierA), Seq(literalRole))
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword `A B` (*) $preposition role") shouldGive
                      func(privilege, resource, graphScopes, List(ast.RelationshipQualifier("A B") _), Seq(literalRole))
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword A, B (*) $preposition $$role1, role2") shouldGive
                      func(privilege, resource, graphScopes, List(relQualifierA, relQualifierB), Seq(paramRole1, literalRole2))
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword * $preposition `r:ole`") shouldGive
                      func(privilege, resource, graphScopes, List(ast.RelationshipAllQualifier() _), Seq(literalRColonOle))
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword `:A` (*) $preposition role") shouldGive
                      func(privilege, resource, graphScopes, List(ast.RelationshipQualifier(":A") _), Seq(literalRole))
                  }

                  test( s"parsingFailures $verb ${privilege.name} {$properties} $graphKeyword $graphName $relTypeKeyword $preposition") {
                    assertFails(s"$verb ${privilege.name} {$properties} $graphKeyword $graphName $relTypeKeyword * (*) $preposition role")
                    assertFails(s"$verb ${privilege.name} {$properties} $graphKeyword $graphName $relTypeKeyword A $preposition role")
                    assertFails(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword * (*)")
                    assertFails(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword A B (*) $preposition role")
                    assertFails(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword A (foo) $preposition role")
                    assertFails(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword * $preposition r:ole")
                  }
              }

              test( s"validExpressions $verb ${privilege.name} $graphKeyword $relTypeKeyword $preposition") {
                parsing(s"$verb ${privilege.name} {*} ON $graphKeyword `f:oo` $relTypeKeyword * $preposition role") shouldGive
                  func(privilege, ast.AllPropertyResource() _, List(ast.NamedGraphScope(literalFColonOo) _), List(ast.RelationshipAllQualifier() _), Seq(literalRole))
                parsing(s"$verb ${privilege.name} {bar} ON $graphKeyword `f:oo` $relTypeKeyword * $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("bar")) _, List(ast.NamedGraphScope(literalFColonOo) _), List(ast.RelationshipAllQualifier() _), Seq(literalRole))
                parsing(s"$verb ${privilege.name} {`b:ar`} ON $graphKeyword foo $relTypeKeyword * $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("b:ar")) _, List(graphScopeFoo), List(ast.RelationshipAllQualifier() _), Seq(literalRole))
                parsing(s"$verb ${privilege.name} {*} ON $graphKeyword foo, baz $relTypeKeyword A (*) $preposition role") shouldGive
                  func(privilege, ast.AllPropertyResource() _, List(graphScopeFoo, graphScopeBaz), List(relQualifierA), Seq(literalRole))
                parsing(s"$verb ${privilege.name} {bar} ON $graphKeyword foo, baz $relTypeKeyword A (*) $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("bar")) _, List(graphScopeFoo, graphScopeBaz), List(relQualifierA), Seq(literalRole))
              }

              test( s"parsingFailures$verb ${privilege.name} $graphKeyword $relTypeKeyword $preposition") {
                // Invalid graph name
                assertFails(s"$verb ${privilege.name} {*} ON $graphKeyword f:oo $relTypeKeyword * $preposition role")
                assertFails(s"$verb ${privilege.name} {bar} ON $graphKeyword f:oo $relTypeKeyword * $preposition role")
                // invalid property definition
                assertFails(s"$verb ${privilege.name} {b:ar} ON $graphKeyword foo $relTypeKeyword * $preposition role")
                // missing graph name
                assertFails(s"$verb ${privilege.name} {*} ON $graphKeyword $relTypeKeyword * $preposition role")
                assertFails(s"$verb ${privilege.name} {*} ON $graphKeyword $relTypeKeyword * (*) $preposition role")
                assertFails(s"$verb ${privilege.name} {*} ON $graphKeyword $relTypeKeyword A $preposition role")
                assertFails(s"$verb ${privilege.name} {*} ON $graphKeyword $relTypeKeyword A (*) $preposition role")
                assertFails(s"$verb ${privilege.name} {bar} ON $graphKeyword $relTypeKeyword * $preposition role")
                assertFails(s"$verb ${privilege.name} {bar} ON $graphKeyword $relTypeKeyword * (*) $preposition role")
                assertFails(s"$verb ${privilege.name} {bar} ON $graphKeyword $relTypeKeyword A $preposition role")
                assertFails(s"$verb ${privilege.name} {bar} ON $graphKeyword $relTypeKeyword A (*) $preposition role")
                // missing property definition
                assertFails(s"$verb ${privilege.name} ON $graphKeyword * $relTypeKeyword * $preposition role")
                assertFails(s"$verb ${privilege.name} ON $graphKeyword * $relTypeKeyword * (*) $preposition role")
                assertFails(s"$verb ${privilege.name} ON $graphKeyword * $relTypeKeyword A $preposition role")
                assertFails(s"$verb ${privilege.name} ON $graphKeyword * $relTypeKeyword A (*) $preposition role")
                assertFails(s"$verb ${privilege.name} ON $graphKeyword foo $relTypeKeyword * $preposition role")
                assertFails(s"$verb ${privilege.name} ON $graphKeyword foo $relTypeKeyword * (*) $preposition role")
                assertFails(s"$verb ${privilege.name} ON $graphKeyword foo $relTypeKeyword A $preposition role")
                assertFails(s"$verb ${privilege.name} ON $graphKeyword foo $relTypeKeyword A (*) $preposition role")
                // missing property list
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword * $relTypeKeyword * $preposition role")
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword * $relTypeKeyword * (*) $preposition role")
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword * $relTypeKeyword A $preposition role")
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword * $relTypeKeyword A (*) $preposition role")
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword foo $relTypeKeyword * $preposition role")
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword foo $relTypeKeyword * (*) $preposition role")
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword foo $relTypeKeyword A $preposition role")
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword foo $relTypeKeyword A (*) $preposition role")
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
                case (properties: String, resource: ast.ActionResource, graphName: String, graphScope: ast.GraphScope) =>
                  val graphScopes = List(graphScope)

                  test( s"validExpressions $verb ${privilege.name} {$properties} $graphKeyword $graphName $elementKeyword $preposition") {
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $elementKeyword * $preposition role") shouldGive
                      func(privilege, resource, graphScopes, List(ast.ElementsAllQualifier() _), Seq(literalRole))
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $elementKeyword * (*) $preposition role") shouldGive
                      func(privilege, resource, graphScopes, List(ast.ElementsAllQualifier() _), Seq(literalRole))
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $elementKeyword A $preposition $$role") shouldGive
                      func(privilege, resource, graphScopes, List(elemQualifierA), Seq(paramRole))
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $elementKeyword A (*) $preposition role") shouldGive
                      func(privilege, resource, graphScopes, List(elemQualifierA), Seq(literalRole))
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $elementKeyword `A B` (*) $preposition role") shouldGive
                      func(privilege, resource, graphScopes, List(ast.ElementQualifier("A B") _), Seq(literalRole))
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $elementKeyword A, B (*) $preposition $$role1, $$role2") shouldGive
                      func(privilege, resource, graphScopes, List(elemQualifierA, elemQualifierB), Seq(paramRole1, paramRole2))
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $elementKeyword * $preposition `r:ole`") shouldGive
                      func(privilege, resource, graphScopes, List(ast.ElementsAllQualifier() _), Seq(literalRColonOle))
                    parsing(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $elementKeyword `:A` (*) $preposition role") shouldGive
                      func(privilege, resource, graphScopes, List(ast.ElementQualifier(":A") _), Seq(literalRole))
                  }

                  test( s"parsingFailures$verb ${privilege.name} {$properties} $graphKeyword $graphName $elementKeyword $preposition") {
                    assertFails(s"$verb ${privilege.name} {$properties} $graphKeyword $graphName $elementKeyword * (*) $preposition role")
                    assertFails(s"$verb ${privilege.name} {$properties} $graphKeyword $graphName $elementKeyword A $preposition role")
                    assertFails(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $elementKeyword * (*)")
                    assertFails(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $elementKeyword A B (*) $preposition role")
                    assertFails(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $elementKeyword A (foo) $preposition role")
                    assertFails(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $elementKeyword * $preposition r:ole")
                  }
              }

              test( s"validExpressions $verb ${privilege.name} $graphKeyword $elementKeyword $preposition") {
                parsing(s"$verb ${privilege.name} {*} ON $graphKeyword `f:oo` $elementKeyword * $preposition role") shouldGive
                  func(privilege, ast.AllPropertyResource() _, List(ast.NamedGraphScope(literalFColonOo) _), List(ast.ElementsAllQualifier() _), Seq(literalRole))
                parsing(s"$verb ${privilege.name} {bar} ON $graphKeyword `f:oo` $elementKeyword * $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("bar")) _, List(ast.NamedGraphScope(literalFColonOo) _), List(ast.ElementsAllQualifier() _), Seq(literalRole))
                parsing(s"$verb ${privilege.name} {`b:ar`} ON $graphKeyword foo $elementKeyword * $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("b:ar")) _, List(graphScopeFoo), List(ast.ElementsAllQualifier() _), Seq(literalRole))
                parsing(s"$verb ${privilege.name} {*} ON $graphKeyword foo, baz $elementKeyword A (*) $preposition role") shouldGive
                  func(privilege, ast.AllPropertyResource() _, List(graphScopeFoo, graphScopeBaz), List(elemQualifierA), Seq(literalRole))
                parsing(s"$verb ${privilege.name} {bar} ON $graphKeyword foo, baz $elementKeyword A (*) $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("bar")) _, List(graphScopeFoo, graphScopeBaz), List(elemQualifierA), Seq(literalRole))
              }

              test( s"parsingFailures $verb ${privilege.name} $graphKeyword $elementKeyword $preposition") {
                // Invalid graph name
                assertFails(s"$verb ${privilege.name} {*} ON $graphKeyword f:oo $elementKeyword * $preposition role")
                assertFails(s"$verb ${privilege.name} {bar} ON $graphKeyword f:oo $elementKeyword * $preposition role")
                // invalid property definition
                assertFails(s"$verb ${privilege.name} {b:ar} ON $graphKeyword foo $elementKeyword * $preposition role")
                // missing graph name
                assertFails(s"$verb ${privilege.name} {*} ON $graphKeyword $elementKeyword * $preposition role")
                assertFails(s"$verb ${privilege.name} {*} ON $graphKeyword $elementKeyword * (*) $preposition role")
                assertFails(s"$verb ${privilege.name} {*} ON $graphKeyword $elementKeyword A $preposition role")
                assertFails(s"$verb ${privilege.name} {*} ON $graphKeyword $elementKeyword A (*) $preposition role")
                assertFails(s"$verb ${privilege.name} {bar} ON $graphKeyword $elementKeyword * $preposition role")
                assertFails(s"$verb ${privilege.name} {bar} ON $graphKeyword $elementKeyword * (*) $preposition role")
                assertFails(s"$verb ${privilege.name} {bar} ON $graphKeyword $elementKeyword A $preposition role")
                assertFails(s"$verb ${privilege.name} {bar} ON $graphKeyword $elementKeyword A (*) $preposition role")
                // missing property definition
                assertFails(s"$verb ${privilege.name} ON $graphKeyword * $elementKeyword * $preposition role")
                assertFails(s"$verb ${privilege.name} ON $graphKeyword * $elementKeyword * (*) $preposition role")
                assertFails(s"$verb ${privilege.name} ON $graphKeyword * $elementKeyword A $preposition role")
                assertFails(s"$verb ${privilege.name} ON $graphKeyword * $elementKeyword A (*) $preposition role")
                assertFails(s"$verb ${privilege.name} ON $graphKeyword foo $elementKeyword * $preposition role")
                assertFails(s"$verb ${privilege.name} ON $graphKeyword foo $elementKeyword * (*) $preposition role")
                assertFails(s"$verb ${privilege.name} ON $graphKeyword foo $elementKeyword A $preposition role")
                assertFails(s"$verb ${privilege.name} ON $graphKeyword foo $elementKeyword A (*) $preposition role")
                // missing property list
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword * $elementKeyword * $preposition role")
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword * $elementKeyword * (*) $preposition role")
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword * $elementKeyword A $preposition role")
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword * $elementKeyword A (*) $preposition role")
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword foo $elementKeyword * $preposition role")
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword foo $elementKeyword * (*) $preposition role")
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword foo $elementKeyword A $preposition role")
                assertFails(s"$verb ${privilege.name} {} ON $graphKeyword foo $elementKeyword A (*) $preposition role")
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
            case (properties: String, resource: ast.ActionResource, graphName: String, graphScope: ast.GraphScope) =>

              test(s"$verb ${privilege.name} {$properties} ON $graphKeyword $graphName $preposition role") {
                yields(func(privilege, resource, List(graphScope), List(ast.ElementsAllQualifier() _), Seq(literalRole)))
              }
          }
      }

      // Database instead of graph keyword

      test(s"$verb ${privilege.name} ON DATABASES * $preposition role") {
        failsToParse
      }

      test(s"$verb ${privilege.name} ON DATABASE foo $preposition role") {
        failsToParse
      }

      test(s"$verb ${privilege.name} ON DEFAULT DATABASE $preposition role") {
        failsToParse
      }
  }

}
