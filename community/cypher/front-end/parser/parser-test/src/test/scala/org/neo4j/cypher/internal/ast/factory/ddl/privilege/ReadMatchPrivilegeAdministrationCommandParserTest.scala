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
package org.neo4j.cypher.internal.ast.factory.ddl.privilege

import org.neo4j.cypher.internal.ast.ActionResource
import org.neo4j.cypher.internal.ast.AllGraphsScope
import org.neo4j.cypher.internal.ast.AllPropertyResource
import org.neo4j.cypher.internal.ast.DefaultGraphScope
import org.neo4j.cypher.internal.ast.ElementQualifier
import org.neo4j.cypher.internal.ast.ElementsAllQualifier
import org.neo4j.cypher.internal.ast.GraphAction
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.GraphScope
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.LabelAllQualifier
import org.neo4j.cypher.internal.ast.LabelQualifier
import org.neo4j.cypher.internal.ast.MatchAction
import org.neo4j.cypher.internal.ast.NamedGraphsScope
import org.neo4j.cypher.internal.ast.PropertiesResource
import org.neo4j.cypher.internal.ast.ReadAction
import org.neo4j.cypher.internal.ast.RelationshipAllQualifier
import org.neo4j.cypher.internal.ast.RelationshipQualifier
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.ddl.AdministrationAndSchemaCommandParserTestBase

class ReadMatchPrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {
  // Granting/denying/revoking read and match to/from role

  Seq(
    (ReadAction, "GRANT", "TO", grantGraphPrivilege: resourcePrivilegeFunc),
    (ReadAction, "DENY", "TO", denyGraphPrivilege: resourcePrivilegeFunc),
    (ReadAction, "REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: resourcePrivilegeFunc),
    (ReadAction, "REVOKE DENY", "FROM", revokeDenyGraphPrivilege: resourcePrivilegeFunc),
    (ReadAction, "REVOKE", "FROM", revokeGraphPrivilege: resourcePrivilegeFunc),
    (MatchAction, "GRANT", "TO", grantGraphPrivilege: resourcePrivilegeFunc),
    (MatchAction, "DENY", "TO", denyGraphPrivilege: resourcePrivilegeFunc),
    (MatchAction, "REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: resourcePrivilegeFunc),
    (MatchAction, "REVOKE DENY", "FROM", revokeDenyGraphPrivilege: resourcePrivilegeFunc),
    (MatchAction, "REVOKE", "FROM", revokeGraphPrivilege: resourcePrivilegeFunc)
  ).foreach {
    case (action: GraphAction, verb: String, preposition: String, func: resourcePrivilegeFunc) =>
      Seq[Immutable](true, false).foreach {
        immutable =>
          val immutableString = immutableOrEmpty(immutable)
          test(s"$verb$immutableString ${action.name} { prop } ON HOME GRAPH $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(action, HomeGraphScope()(pos))(pos),
              PropertiesResource(propSeq)(pos),
              List(ElementsAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString ${action.name} { prop } ON HOME GRAPH NODE A $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(action, HomeGraphScope()(pos))(pos),
              PropertiesResource(propSeq)(pos),
              List(labelQualifierA),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString ${action.name} { prop } ON DEFAULT GRAPH $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(action, DefaultGraphScope()(pos))(pos),
              PropertiesResource(propSeq)(pos),
              List(ElementsAllQualifier() _),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString ${action.name} { prop } ON DEFAULT GRAPH NODE A $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(action, DefaultGraphScope()(pos))(pos),
              PropertiesResource(propSeq)(pos),
              List(labelQualifierA),
              Seq(literalRole),
              immutable
            )(pos))
          }

          Seq("GRAPH", "GRAPHS").foreach {
            graphKeyword =>
              Seq("NODE", "NODES").foreach {
                nodeKeyword =>
                  Seq(
                    ("*", AllPropertyResource()(pos), "*", AllGraphsScope()(pos)),
                    ("*", AllPropertyResource()(pos), "foo", graphScopeFoo(pos)),
                    ("bar", PropertiesResource(Seq("bar"))(pos), "*", AllGraphsScope()(pos)),
                    ("bar", PropertiesResource(Seq("bar"))(pos), "foo", graphScopeFoo(pos)),
                    ("foo, bar", PropertiesResource(Seq("foo", "bar"))(pos), "*", AllGraphsScope()(pos)),
                    ("foo, bar", PropertiesResource(Seq("foo", "bar"))(pos), "foo", graphScopeFoo(pos))
                  ).foreach {
                    case (
                        properties: String,
                        resource: ActionResource,
                        graphName: String,
                        graphScope: GraphScope
                      ) =>
                      test(
                        s"validExpressions $verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $nodeKeyword $preposition"
                      ) {
                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword * $preposition $$role" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(LabelAllQualifier() _),
                              Seq(paramRole),
                              immutable
                            )(pos)
                          )

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword * (*) $preposition role" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(LabelAllQualifier() _),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )
                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword A $preposition role" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(labelQualifierA),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword A (*) $preposition role" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(labelQualifierA),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword `A B` (*) $preposition role" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(LabelQualifier("A B") _),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword A, B (*) $preposition role1, $$role2" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(labelQualifierA, labelQualifierB),
                              Seq(literalRole1, paramRole2),
                              immutable
                            )(pos)
                          )

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword * $preposition `r:ole`" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(LabelAllQualifier() _),
                              Seq(literalRColonOle),
                              immutable
                            )(pos)
                          )

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword `:A` (*) $preposition role" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(LabelQualifier(":A") _),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )
                      }

                      test(
                        s"failToParse $verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $nodeKeyword $preposition"
                      ) {

                        s"$verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $nodeKeyword * (*) $preposition role" should
                          notParse[Statements]

                        s"$verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $nodeKeyword A $preposition role" should
                          notParse[Statements]

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword * (*)" should
                          notParse[Statements]

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword A B (*) $preposition role" should
                          notParse[Statements]

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword A (foo) $preposition role" should
                          notParse[Statements]

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $nodeKeyword * $preposition r:ole" should
                          notParse[Statements]
                      }
                  }

                  test(
                    s"validExpressions $verb$immutableString ${action.name} $graphKeyword $nodeKeyword $preposition"
                  ) {

                    s"$verb$immutableString ${action.name} {*} ON $graphKeyword `f:oo` $nodeKeyword * $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(action, NamedGraphsScope(Seq(namespacedName("f:oo"))) _)(pos),
                          AllPropertyResource() _,
                          List(LabelAllQualifier() _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString ${action.name} {bar} ON $graphKeyword `f:oo` $nodeKeyword * $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(action, NamedGraphsScope(Seq(namespacedName("f:oo"))) _)(pos),
                          PropertiesResource(Seq("bar")) _,
                          List(LabelAllQualifier() _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString ${action.name} {`b:ar`} ON $graphKeyword foo $nodeKeyword * $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(action, graphScopeFoo)(pos),
                          PropertiesResource(Seq("b:ar")) _,
                          List(LabelAllQualifier() _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString ${action.name} {*} ON $graphKeyword foo, baz $nodeKeyword A (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(action, graphScopeFooBaz)(pos),
                          AllPropertyResource() _,
                          List(labelQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString ${action.name} {bar} ON $graphKeyword foo, baz $nodeKeyword A (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(action, graphScopeFooBaz)(pos),
                          PropertiesResource(Seq("bar")) _,
                          List(labelQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                  }

                  test(
                    s"parsingFailures $verb$immutableString ${action.name} $graphKeyword $nodeKeyword $preposition"
                  ) {
                    // Invalid graph name

                    s"$verb$immutableString ${action.name} {*} ON $graphKeyword f:oo $nodeKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {bar} ON $graphKeyword f:oo $nodeKeyword * $preposition role" should
                      notParse[Statements]

                    // mixing specific graph and *

                    s"$verb$immutableString ${action.name} {*} ON $graphKeyword foo, * $nodeKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {*} ON $graphKeyword *, foo $nodeKeyword * $preposition role" should
                      notParse[Statements]

                    // invalid property definition

                    s"$verb$immutableString ${action.name} {b:ar} ON $graphKeyword foo $nodeKeyword * $preposition role" should
                      notParse[Statements]

                    // missing graph name

                    s"$verb$immutableString ${action.name} {*} ON $graphKeyword $nodeKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {*} ON $graphKeyword $nodeKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {*} ON $graphKeyword $nodeKeyword A $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {*} ON $graphKeyword $nodeKeyword A (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $nodeKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $nodeKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $nodeKeyword A $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $nodeKeyword A (*) $preposition role" should
                      notParse[Statements]

                    // missing property definition

                    s"$verb$immutableString ${action.name} ON $graphKeyword * $nodeKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} ON $graphKeyword * $nodeKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} ON $graphKeyword * $nodeKeyword A $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} ON $graphKeyword * $nodeKeyword A (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} ON $graphKeyword foo $nodeKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} ON $graphKeyword foo $nodeKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} ON $graphKeyword foo $nodeKeyword A $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} ON $graphKeyword foo $nodeKeyword A (*) $preposition role" should
                      notParse[Statements]

                    // missing property list

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword * $nodeKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword * $nodeKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword * $nodeKeyword A $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword * $nodeKeyword A (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $nodeKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $nodeKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $nodeKeyword A $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $nodeKeyword A (*) $preposition role" should
                      notParse[Statements]
                  }
              }

              Seq("RELATIONSHIP", "RELATIONSHIPS").foreach {
                relTypeKeyword =>
                  Seq(
                    ("*", AllPropertyResource()(pos), "*", AllGraphsScope()(pos)),
                    ("*", AllPropertyResource()(pos), "foo", graphScopeFoo(pos)),
                    ("bar", PropertiesResource(Seq("bar"))(pos), "*", AllGraphsScope()(pos)),
                    ("bar", PropertiesResource(Seq("bar"))(pos), "foo", graphScopeFoo(pos)),
                    ("foo, bar", PropertiesResource(Seq("foo", "bar"))(pos), "*", AllGraphsScope()(pos)),
                    ("foo, bar", PropertiesResource(Seq("foo", "bar"))(pos), "foo", graphScopeFoo(pos))
                  ).foreach {
                    case (
                        properties: String,
                        resource: ActionResource,
                        graphName: String,
                        graphScope: GraphScope
                      ) =>
                      test(
                        s"validExpressions $verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $relTypeKeyword $preposition"
                      ) {

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword * $preposition role" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(RelationshipAllQualifier() _),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword * (*) $preposition $$role" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(RelationshipAllQualifier() _),
                              Seq(paramRole),
                              immutable
                            )(pos)
                          )

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword A $preposition role" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(relQualifierA),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword A (*) $preposition role" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(relQualifierA),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword `A B` (*) $preposition role" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(RelationshipQualifier("A B") _),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword A, B (*) $preposition $$role1, role2" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(relQualifierA, relQualifierB),
                              Seq(paramRole1, literalRole2),
                              immutable
                            )(pos)
                          )

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword * $preposition `r:ole`" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(RelationshipAllQualifier() _),
                              Seq(literalRColonOle),
                              immutable
                            )(pos)
                          )

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword `:A` (*) $preposition role" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(RelationshipQualifier(":A") _),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )
                      }

                      test(
                        s"parsingFailures $verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $relTypeKeyword $preposition"
                      ) {

                        s"$verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $relTypeKeyword * (*) $preposition role" should
                          notParse[Statements]

                        s"$verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $relTypeKeyword A $preposition role" should
                          notParse[Statements]

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword * (*)" should
                          notParse[Statements]

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword A B (*) $preposition role" should
                          notParse[Statements]

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword A (foo) $preposition role" should
                          notParse[Statements]

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $relTypeKeyword * $preposition r:ole" should
                          notParse[Statements]
                      }
                  }

                  test(
                    s"validExpressions $verb$immutableString ${action.name} $graphKeyword $relTypeKeyword $preposition"
                  ) {

                    s"$verb$immutableString ${action.name} {*} ON $graphKeyword `f:oo` $relTypeKeyword * $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(action, NamedGraphsScope(Seq(namespacedName("f:oo"))) _)(pos),
                          AllPropertyResource() _,
                          List(RelationshipAllQualifier() _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString ${action.name} {bar} ON $graphKeyword `f:oo` $relTypeKeyword * $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(action, NamedGraphsScope(Seq(namespacedName("f:oo"))) _)(pos),
                          PropertiesResource(Seq("bar")) _,
                          List(RelationshipAllQualifier() _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString ${action.name} {`b:ar`} ON $graphKeyword foo $relTypeKeyword * $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(action, graphScopeFoo)(pos),
                          PropertiesResource(Seq("b:ar")) _,
                          List(RelationshipAllQualifier() _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString ${action.name} {*} ON $graphKeyword foo, baz $relTypeKeyword A (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(action, graphScopeFooBaz)(pos),
                          AllPropertyResource() _,
                          List(relQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString ${action.name} {bar} ON $graphKeyword foo, baz $relTypeKeyword A (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(action, graphScopeFooBaz)(pos),
                          PropertiesResource(Seq("bar")) _,
                          List(relQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                  }

                  test(
                    s"parsingFailures$verb$immutableString ${action.name} $graphKeyword $relTypeKeyword $preposition"
                  ) {
                    // Invalid graph name

                    s"$verb$immutableString ${action.name} {*} ON $graphKeyword f:oo $relTypeKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {bar} ON $graphKeyword f:oo $relTypeKeyword * $preposition role" should
                      notParse[Statements]

                    // invalid property definition

                    s"$verb$immutableString ${action.name} {b:ar} ON $graphKeyword foo $relTypeKeyword * $preposition role" should
                      notParse[Statements]

                    // missing graph name

                    s"$verb$immutableString ${action.name} {*} ON $graphKeyword $relTypeKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {*} ON $graphKeyword $relTypeKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {*} ON $graphKeyword $relTypeKeyword A $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {*} ON $graphKeyword $relTypeKeyword A (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $relTypeKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $relTypeKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $relTypeKeyword A $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $relTypeKeyword A (*) $preposition role" should
                      notParse[Statements]

                    // missing property definition

                    s"$verb$immutableString ${action.name} ON $graphKeyword * $relTypeKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} ON $graphKeyword * $relTypeKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} ON $graphKeyword * $relTypeKeyword A $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} ON $graphKeyword * $relTypeKeyword A (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} ON $graphKeyword foo $relTypeKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} ON $graphKeyword foo $relTypeKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} ON $graphKeyword foo $relTypeKeyword A $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} ON $graphKeyword foo $relTypeKeyword A (*) $preposition role" should
                      notParse[Statements]

                    // missing property list

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword * $relTypeKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword * $relTypeKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword * $relTypeKeyword A $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword * $relTypeKeyword A (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $relTypeKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $relTypeKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $relTypeKeyword A $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $relTypeKeyword A (*) $preposition role" should
                      notParse[Statements]
                  }
              }

              Seq("ELEMENT", "ELEMENTS").foreach {
                elementKeyword =>
                  Seq(
                    ("*", AllPropertyResource()(pos), "*", AllGraphsScope()(pos)),
                    ("*", AllPropertyResource()(pos), "foo", graphScopeFoo(pos)),
                    ("bar", PropertiesResource(Seq("bar"))(pos), "*", AllGraphsScope()(pos)),
                    ("bar", PropertiesResource(Seq("bar"))(pos), "foo", graphScopeFoo(pos)),
                    ("foo, bar", PropertiesResource(Seq("foo", "bar"))(pos), "*", AllGraphsScope()(pos)),
                    ("foo, bar", PropertiesResource(Seq("foo", "bar"))(pos), "foo", graphScopeFoo(pos))
                  ).foreach {
                    case (
                        properties: String,
                        resource: ActionResource,
                        graphName: String,
                        graphScope: GraphScope
                      ) =>
                      test(
                        s"validExpressions $verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $elementKeyword $preposition"
                      ) {

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword * $preposition role" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(ElementsAllQualifier() _),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword * (*) $preposition role" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(ElementsAllQualifier() _),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword A $preposition $$role" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(elemQualifierA),
                              Seq(paramRole),
                              immutable
                            )(pos)
                          )

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword A (*) $preposition role" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(elemQualifierA),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword `A B` (*) $preposition role" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(ElementQualifier("A B") _),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword A, B (*) $preposition $$role1, $$role2" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(elemQualifierA, elemQualifierB),
                              Seq(paramRole1, paramRole2),
                              immutable
                            )(pos)
                          )

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword * $preposition `r:ole`" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(ElementsAllQualifier() _),
                              Seq(literalRColonOle),
                              immutable
                            )(pos)
                          )

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword `:A` (*) $preposition role" should
                          parseTo[Statements](
                            func(
                              GraphPrivilege(action, graphScope)(pos),
                              resource,
                              List(ElementQualifier(":A") _),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )
                      }

                      test(
                        s"parsingFailures$verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $elementKeyword $preposition"
                      ) {

                        s"$verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $elementKeyword * (*) $preposition role" should
                          notParse[Statements]

                        s"$verb$immutableString ${action.name} {$properties} $graphKeyword $graphName $elementKeyword A $preposition role" should
                          notParse[Statements]

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword * (*)" should
                          notParse[Statements]

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword A B (*) $preposition role" should
                          notParse[Statements]

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword A (foo) $preposition role" should
                          notParse[Statements]

                        s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $elementKeyword * $preposition r:ole" should
                          notParse[Statements]
                      }
                  }

                  test(
                    s"validExpressions $verb$immutableString ${action.name} $graphKeyword $elementKeyword $preposition"
                  ) {

                    s"$verb$immutableString ${action.name} {*} ON $graphKeyword `f:oo` $elementKeyword * $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(action, NamedGraphsScope(Seq(namespacedName("f:oo"))) _)(pos),
                          AllPropertyResource() _,
                          List(ElementsAllQualifier() _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString ${action.name} {bar} ON $graphKeyword `f:oo` $elementKeyword * $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(action, NamedGraphsScope(Seq(namespacedName("f:oo"))) _)(pos),
                          PropertiesResource(Seq("bar")) _,
                          List(ElementsAllQualifier() _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString ${action.name} {`b:ar`} ON $graphKeyword foo $elementKeyword * $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(action, graphScopeFoo)(pos),
                          PropertiesResource(Seq("b:ar")) _,
                          List(ElementsAllQualifier() _),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString ${action.name} {*} ON $graphKeyword foo, baz $elementKeyword A (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(action, graphScopeFooBaz)(pos),
                          AllPropertyResource() _,
                          List(elemQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString ${action.name} {bar} ON $graphKeyword foo, baz $elementKeyword A (*) $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(action, graphScopeFooBaz)(pos),
                          PropertiesResource(Seq("bar")) _,
                          List(elemQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                  }

                  test(
                    s"parsingFailures $verb$immutableString ${action.name} $graphKeyword $elementKeyword $preposition"
                  ) {
                    // Invalid graph name

                    s"$verb$immutableString ${action.name} {*} ON $graphKeyword f:oo $elementKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {bar} ON $graphKeyword f:oo $elementKeyword * $preposition role" should
                      notParse[Statements]

                    // invalid property definition

                    s"$verb$immutableString ${action.name} {b:ar} ON $graphKeyword foo $elementKeyword * $preposition role" should
                      notParse[Statements]

                    // missing graph name

                    s"$verb$immutableString ${action.name} {*} ON $graphKeyword $elementKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {*} ON $graphKeyword $elementKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {*} ON $graphKeyword $elementKeyword A $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {*} ON $graphKeyword $elementKeyword A (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $elementKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $elementKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $elementKeyword A $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {bar} ON $graphKeyword $elementKeyword A (*) $preposition role" should
                      notParse[Statements]

                    // missing property definition

                    s"$verb$immutableString ${action.name} ON $graphKeyword * $elementKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} ON $graphKeyword * $elementKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} ON $graphKeyword * $elementKeyword A $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} ON $graphKeyword * $elementKeyword A (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} ON $graphKeyword foo $elementKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} ON $graphKeyword foo $elementKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} ON $graphKeyword foo $elementKeyword A $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} ON $graphKeyword foo $elementKeyword A (*) $preposition role" should
                      notParse[Statements]

                    // missing property list

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword * $elementKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword * $elementKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword * $elementKeyword A $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword * $elementKeyword A (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $elementKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $elementKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $elementKeyword A $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString ${action.name} {} ON $graphKeyword foo $elementKeyword A (*) $preposition role" should
                      notParse[Statements]
                  }
              }

              // Needs to be separate loop to avoid duplicate tests since the test does not have any segment keyword
              Seq(
                ("*", AllPropertyResource()(pos), "*", AllGraphsScope()(pos)),
                ("*", AllPropertyResource()(pos), "foo", graphScopeFoo(pos)),
                ("*", AllPropertyResource()(pos), "$foo", graphScopeParamFoo(pos)),
                ("bar", PropertiesResource(Seq("bar"))(pos), "*", AllGraphsScope()(pos)),
                ("bar", PropertiesResource(Seq("bar"))(pos), "foo", graphScopeFoo(pos)),
                ("foo, bar", PropertiesResource(Seq("foo", "bar"))(pos), "*", AllGraphsScope()(pos)),
                ("foo, bar", PropertiesResource(Seq("foo", "bar"))(pos), "foo", graphScopeFoo(pos))
              ).foreach {
                case (
                    properties: String,
                    resource: ActionResource,
                    graphName: String,
                    graphScope: GraphScope
                  ) =>
                  test(
                    s"$verb$immutableString ${action.name} {$properties} ON $graphKeyword $graphName $preposition role"
                  ) {
                    parsesTo[Statements](func(
                      GraphPrivilege(action, graphScope)(pos),
                      resource,
                      List(ElementsAllQualifier() _),
                      Seq(literalRole),
                      immutable
                    )(pos))
                  }
              }
          }

          // Database instead of graph keyword

          test(s"$verb$immutableString ${action.name} {*} ON DATABASES * $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString ${action.name} {*} ON DATABASE foo $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString ${action.name} {*} ON HOME DATABASE $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString ${action.name} {*} ON DEFAULT DATABASE $preposition role") {
            failsParsing[Statements]
          }

          // Alias with too many components

          test(s"$verb$immutableString ${action.name} {*} ON GRAPH `a`.`b`.`c` $preposition role") {
            // more than two components
            failsParsing[Statements]
              .withMessageContaining(
                "Invalid input ``a`.`b`.`c`` for name. Expected name to contain at most two components separated by `.`."
              )
          }
      }
  }
}
