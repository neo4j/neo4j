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
import org.neo4j.cypher.internal.ast.AllPropertyResource
import org.neo4j.cypher.internal.ast.DefaultGraphScope
import org.neo4j.cypher.internal.ast.ElementsAllQualifier
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.LabelAllQualifier
import org.neo4j.cypher.internal.ast.PropertiesResource
import org.neo4j.cypher.internal.ast.RelationshipAllQualifier
import org.neo4j.cypher.internal.ast.SetPropertyAction
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.ddl.AdministrationAndSchemaCommandParserTestBase
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc

class PropertyPrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

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
          test(s"$verb$immutableString SET PROPERTY { prop } ON GRAPH foo $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(SetPropertyAction, graphScopeFoo)(_),
              PropertiesResource(propSeq)(_),
              List(ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          // Multiple properties should be allowed

          test(s"$verb$immutableString SET PROPERTY { * } ON GRAPH foo $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(SetPropertyAction, graphScopeFoo)(_),
              AllPropertyResource()(_),
              List(ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SET PROPERTY { prop1, prop2 } ON GRAPH foo $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(SetPropertyAction, graphScopeFoo)(_),
              PropertiesResource(Seq("prop1", "prop2"))(_),
              List(ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          // Home graph should be allowed

          test(s"$verb$immutableString SET PROPERTY { * } ON HOME GRAPH $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(SetPropertyAction, HomeGraphScope()(_))(_),
              AllPropertyResource()(_),
              List(ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON HOME GRAPH $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(SetPropertyAction, HomeGraphScope()(_))(_),
              PropertiesResource(propSeq)(_),
              List(ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON HOME GRAPH NODES A,B $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(SetPropertyAction, HomeGraphScope()(_))(_),
              PropertiesResource(propSeq)(_),
              List(labelQualifierA, labelQualifierB),
              Seq(literalRole),
              immutable
            )(pos))
          }

          // Default graph should be allowed

          test(s"$verb$immutableString SET PROPERTY { * } ON DEFAULT GRAPH $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(SetPropertyAction, DefaultGraphScope()(_))(_),
              AllPropertyResource()(_),
              List(ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON DEFAULT GRAPH $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(SetPropertyAction, DefaultGraphScope()(_))(_),
              PropertiesResource(propSeq)(_),
              List(ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON DEFAULT GRAPH NODES A,B $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(SetPropertyAction, DefaultGraphScope()(_))(_),
              PropertiesResource(propSeq)(_),
              List(labelQualifierA, labelQualifierB),
              Seq(literalRole),
              immutable
            )(pos))
          }

          // Multiple graphs should be allowed

          test(s"$verb$immutableString SET PROPERTY { prop } ON GRAPHS * $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(SetPropertyAction, AllGraphsScope()(_))(_),
              PropertiesResource(propSeq)(_),
              List(ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON GRAPHS foo,baz $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(SetPropertyAction, graphScopeFooBaz)(_),
              PropertiesResource(propSeq)(_),
              List(ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          // Qualifiers

          test(s"$verb$immutableString SET PROPERTY { prop } ON GRAPHS foo ELEMENTS A,B $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(SetPropertyAction, graphScopeFoo)(_),
              PropertiesResource(propSeq)(_),
              List(elemQualifierA, elemQualifierB),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON GRAPHS foo NODES A,B $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(SetPropertyAction, graphScopeFoo)(_),
              PropertiesResource(propSeq)(_),
              List(labelQualifierA, labelQualifierB),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON GRAPHS foo NODES * $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(SetPropertyAction, graphScopeFoo)(_),
              PropertiesResource(propSeq)(_),
              List(LabelAllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON GRAPHS foo RELATIONSHIPS A,B $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(SetPropertyAction, graphScopeFoo)(_),
              PropertiesResource(propSeq)(_),
              List(relQualifierA, relQualifierB),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON GRAPHS foo RELATIONSHIPS * $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(SetPropertyAction, graphScopeFoo)(_),
              PropertiesResource(propSeq)(_),
              List(RelationshipAllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          // Multiple roles should be allowed

          test(s"$verb$immutableString SET PROPERTY { prop } ON GRAPHS foo $preposition role1, role2") {
            parsesTo[Statements](func(
              GraphPrivilege(SetPropertyAction, graphScopeFoo)(_),
              PropertiesResource(propSeq)(_),
              List(ElementsAllQualifier()(_)),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          // Parameter values

          test(s"$verb$immutableString SET PROPERTY { prop } ON GRAPH $$foo $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(SetPropertyAction, graphScopeParamFoo)(_),
              PropertiesResource(propSeq)(_),
              List(ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON GRAPH foo $preposition $$role") {
            parsesTo[Statements](func(
              GraphPrivilege(SetPropertyAction, graphScopeFoo)(_),
              PropertiesResource(propSeq)(_),
              List(ElementsAllQualifier()(_)),
              Seq(paramRole),
              immutable
            )(pos))
          }

          // PROPERTYS/PROPERTIES instead of PROPERTY

          test(s"$verb$immutableString SET PROPERTYS { prop } ON GRAPH * $preposition role") {
            val offset = verb.length + immutableString.length + 5
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessage(
                  s"""Invalid input 'PROPERTYS': expected
                     |  "AUTH"
                     |  "DATABASE"
                     |  "LABEL"
                     |  "PASSWORD"
                     |  "PASSWORDS"
                     |  "PROPERTY"
                     |  "USER" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                )
              case _ => _.withSyntaxErrorContaining(
                  """Invalid input 'PROPERTYS': expected 'DATABASE ACCESS', 'AUTH ON DBMS', 'LABEL', 'PASSWORD', 'PASSWORDS', 'PROPERTY' or 'USER'"""
                )
            }
          }

          test(s"$verb$immutableString SET PROPERTIES { prop } ON GRAPH * $preposition role") {
            val offset = verb.length + immutableString.length + 5
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessage(
                  s"""Invalid input 'PROPERTIES': expected
                     |  "AUTH"
                     |  "DATABASE"
                     |  "LABEL"
                     |  "PASSWORD"
                     |  "PASSWORDS"
                     |  "PROPERTY"
                     |  "USER" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
                )
              case _ => _.withSyntaxErrorContaining(
                  """Invalid input 'PROPERTIES': expected 'DATABASE ACCESS', 'AUTH ON DBMS', 'LABEL', 'PASSWORD', 'PASSWORDS', 'PROPERTY' or 'USER'"""
                )
            }
          }

          // Database instead of graph keyword

          test(s"$verb$immutableString SET PROPERTY { prop } ON DATABASES * $preposition role") {
            val offset = verb.length + immutableString.length + 26
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessage(
                  s"""Invalid input 'DATABASES': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))"""
                )
              case _ => _.withSyntaxErrorContaining(
                  s"""Invalid input 'DATABASES': expected 'GRAPH', 'DEFAULT GRAPH', 'HOME GRAPH' or 'GRAPHS' (line 1, column ${offset + 1} (offset: $offset))"""
                )
            }
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON DATABASE foo $preposition role") {
            val offset = verb.length + immutableString.length + 26
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessage(
                  s"""Invalid input 'DATABASE': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))"""
                )
              case _ => _.withSyntaxErrorContaining(
                  s"""Invalid input 'DATABASE': expected 'GRAPH', 'DEFAULT GRAPH', 'HOME GRAPH' or 'GRAPHS' (line 1, column ${offset + 1} (offset: $offset))"""
                )
            }
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON HOME DATABASE $preposition role") {
            failsParsing[Statements].withMessageStart("Invalid input")
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON DEFAULT DATABASE $preposition role") {
            failsParsing[Statements].withMessageStart("Invalid input")
          }
      }
  }
}
