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

import org.neo4j.cypher.internal.ast.AllGraphsScope
import org.neo4j.cypher.internal.ast.AllLabelResource
import org.neo4j.cypher.internal.ast.DefaultGraphScope
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.LabelAllQualifier
import org.neo4j.cypher.internal.ast.LabelsResource
import org.neo4j.cypher.internal.ast.RemoveLabelAction
import org.neo4j.cypher.internal.ast.SetLabelAction
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.ddl.AdministrationAndSchemaCommandParserTestBase
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc

class LabelPrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {
  private val labelResource = LabelsResource(Seq("label"))(_)

  Seq(
    ("GRANT", "TO", grantGraphPrivilege: resourcePrivilegeFunc),
    ("DENY", "TO", denyGraphPrivilege: resourcePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: resourcePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyGraphPrivilege: resourcePrivilegeFunc),
    ("REVOKE", "FROM", revokeGraphPrivilege: resourcePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: resourcePrivilegeFunc) =>
      Seq[Immutable](true, false).foreach {
        immutable =>
          val immutableString = immutableOrEmpty(immutable)
          Seq(
            ("SET", SetLabelAction),
            ("REMOVE", RemoveLabelAction)
          ).foreach {
            case (setOrRemove, action) =>
              test(s"$verb$immutableString $setOrRemove LABEL label ON GRAPH foo $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, graphScopeFoo)(_),
                  labelResource,
                  List(LabelAllQualifier()(_)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              // Multiple labels should be allowed

              test(s"$verb$immutableString $setOrRemove LABEL * ON GRAPH foo $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, graphScopeFoo)(_),
                  AllLabelResource()(_),
                  List(LabelAllQualifier()(_)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $setOrRemove LABEL label1, label2 ON GRAPH foo $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, graphScopeFoo)(_),
                  LabelsResource(Seq("label1", "label2"))(_),
                  List(LabelAllQualifier()(_)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              // Multiple graphs should be allowed

              test(s"$verb$immutableString $setOrRemove LABEL label ON GRAPHS * $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, AllGraphsScope()(_))(_),
                  labelResource,
                  List(LabelAllQualifier()(_)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $setOrRemove LABEL label ON GRAPHS foo,baz $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, graphScopeFooBaz)(_),
                  labelResource,
                  List(LabelAllQualifier()(_)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              // Home graph should be allowed

              test(s"$verb$immutableString $setOrRemove LABEL label ON HOME GRAPH $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, HomeGraphScope()(_))(_),
                  labelResource,
                  List(LabelAllQualifier()(_)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $setOrRemove LABEL * ON HOME GRAPH $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, HomeGraphScope()(_))(_),
                  AllLabelResource()(_),
                  List(LabelAllQualifier()(_)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              // Default graph should be allowed

              test(s"$verb$immutableString $setOrRemove LABEL label ON DEFAULT GRAPH $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, DefaultGraphScope()(_))(_),
                  labelResource,
                  List(LabelAllQualifier()(_)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $setOrRemove LABEL * ON DEFAULT GRAPH $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, DefaultGraphScope()(_))(_),
                  AllLabelResource()(_),
                  List(LabelAllQualifier()(_)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              // Multiple roles should be allowed

              test(s"$verb$immutableString $setOrRemove LABEL label ON GRAPHS foo $preposition role1, role2") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, graphScopeFoo)(_),
                  labelResource,
                  List(LabelAllQualifier()(_)),
                  Seq(literalRole1, literalRole2),
                  immutable
                )(pos))
              }

              // Parameter values

              test(s"$verb$immutableString $setOrRemove LABEL label ON GRAPH $$foo $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, graphScopeParamFoo)(_),
                  labelResource,
                  List(LabelAllQualifier()(_)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $setOrRemove LABEL label ON GRAPH foo $preposition $$role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, graphScopeFoo)(_),
                  labelResource,
                  List(LabelAllQualifier()(_)),
                  Seq(paramRole),
                  immutable
                )(pos))
              }

              // TODO: should this one be supported?
              test(s"$verb$immutableString $setOrRemove LABEL $$label ON GRAPH foo $preposition role") {
                failsParsing[Statements]
              }

              // LABELS instead of LABEL

              test(s"$verb$immutableString $setOrRemove LABELS label ON GRAPH * $preposition role") {
                failsParsing[Statements].withMessageStart("""Invalid input 'LABELS': expected""")
              }

              // Database instead of graph keyword

              test(s"$verb$immutableString $setOrRemove LABEL label ON DATABASES * $preposition role") {
                failsParsing[Statements].in {
                  case Cypher5JavaCc => _.withMessageStart("""Invalid input 'DATABASES': expected""")
                  case _ => _.withSyntaxErrorContaining(
                      """Invalid input 'DATABASES': expected"""
                    )
                }
              }

              test(s"$verb$immutableString $setOrRemove LABEL label ON DATABASE foo $preposition role") {
                failsParsing[Statements].in {
                  case Cypher5JavaCc => _.withMessageStart("""Invalid input 'DATABASE': expected""")
                  case _ => _.withSyntaxErrorContaining(
                      """Invalid input 'DATABASE': expected"""
                    )
                }
              }

              test(s"$verb$immutableString $setOrRemove LABEL label ON HOME DATABASE $preposition role") {
                failsParsing[Statements]
              }

              test(s"$verb$immutableString $setOrRemove LABEL label ON DEFAULT DATABASE $preposition role") {
                failsParsing[Statements]
              }

              // Alias with too many components
              test(s"$verb$immutableString $setOrRemove LABEL label ON GRAPH `a`.`b`.`c` $preposition role") {
                // more than two components
                failsParsing[Statements]
                  .withMessageContaining(
                    "Invalid input ``a`.`b`.`c`` for name. Expected name to contain at most two components separated by `.`."
                  )
              }
          }
      }
  }
}
