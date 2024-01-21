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
            yields[Statements](func(
              ast.GraphPrivilege(ast.SetPropertyAction, graphScopeFoo)(_),
              ast.PropertiesResource(propSeq)(_),
              List(ast.ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          // Multiple properties should be allowed

          test(s"$verb$immutableString SET PROPERTY { * } ON GRAPH foo $preposition role") {
            yields[Statements](func(
              ast.GraphPrivilege(ast.SetPropertyAction, graphScopeFoo)(_),
              ast.AllPropertyResource()(_),
              List(ast.ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString SET PROPERTY { prop1, prop2 } ON GRAPH foo $preposition role") {
            yields[Statements](func(
              ast.GraphPrivilege(ast.SetPropertyAction, graphScopeFoo)(_),
              ast.PropertiesResource(Seq("prop1", "prop2"))(_),
              List(ast.ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          // Home graph should be allowed

          test(s"$verb$immutableString SET PROPERTY { * } ON HOME GRAPH $preposition role") {
            yields[Statements](func(
              ast.GraphPrivilege(ast.SetPropertyAction, ast.HomeGraphScope()(_))(_),
              ast.AllPropertyResource()(_),
              List(ast.ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON HOME GRAPH $preposition role") {
            yields[Statements](func(
              ast.GraphPrivilege(ast.SetPropertyAction, ast.HomeGraphScope()(_))(_),
              ast.PropertiesResource(propSeq)(_),
              List(ast.ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON HOME GRAPH NODES A,B $preposition role") {
            yields[Statements](func(
              ast.GraphPrivilege(ast.SetPropertyAction, ast.HomeGraphScope()(_))(_),
              ast.PropertiesResource(propSeq)(_),
              List(labelQualifierA, labelQualifierB),
              Seq(literalRole),
              immutable
            ))
          }

          // Default graph should be allowed

          test(s"$verb$immutableString SET PROPERTY { * } ON DEFAULT GRAPH $preposition role") {
            yields[Statements](func(
              ast.GraphPrivilege(ast.SetPropertyAction, ast.DefaultGraphScope()(_))(_),
              ast.AllPropertyResource()(_),
              List(ast.ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON DEFAULT GRAPH $preposition role") {
            yields[Statements](func(
              ast.GraphPrivilege(ast.SetPropertyAction, ast.DefaultGraphScope()(_))(_),
              ast.PropertiesResource(propSeq)(_),
              List(ast.ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON DEFAULT GRAPH NODES A,B $preposition role") {
            yields[Statements](func(
              ast.GraphPrivilege(ast.SetPropertyAction, ast.DefaultGraphScope()(_))(_),
              ast.PropertiesResource(propSeq)(_),
              List(labelQualifierA, labelQualifierB),
              Seq(literalRole),
              immutable
            ))
          }

          // Multiple graphs should be allowed

          test(s"$verb$immutableString SET PROPERTY { prop } ON GRAPHS * $preposition role") {
            yields[Statements](func(
              ast.GraphPrivilege(ast.SetPropertyAction, ast.AllGraphsScope()(_))(_),
              ast.PropertiesResource(propSeq)(_),
              List(ast.ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON GRAPHS foo,baz $preposition role") {
            yields[Statements](func(
              ast.GraphPrivilege(ast.SetPropertyAction, graphScopeFooBaz)(_),
              ast.PropertiesResource(propSeq)(_),
              List(ast.ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          // Qualifiers

          test(s"$verb$immutableString SET PROPERTY { prop } ON GRAPHS foo ELEMENTS A,B $preposition role") {
            yields[Statements](func(
              ast.GraphPrivilege(ast.SetPropertyAction, graphScopeFoo)(_),
              ast.PropertiesResource(propSeq)(_),
              List(elemQualifierA, elemQualifierB),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON GRAPHS foo NODES A,B $preposition role") {
            yields[Statements](func(
              ast.GraphPrivilege(ast.SetPropertyAction, graphScopeFoo)(_),
              ast.PropertiesResource(propSeq)(_),
              List(labelQualifierA, labelQualifierB),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON GRAPHS foo NODES * $preposition role") {
            yields[Statements](func(
              ast.GraphPrivilege(ast.SetPropertyAction, graphScopeFoo)(_),
              ast.PropertiesResource(propSeq)(_),
              List(ast.LabelAllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON GRAPHS foo RELATIONSHIPS A,B $preposition role") {
            yields[Statements](func(
              ast.GraphPrivilege(ast.SetPropertyAction, graphScopeFoo)(_),
              ast.PropertiesResource(propSeq)(_),
              List(relQualifierA, relQualifierB),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON GRAPHS foo RELATIONSHIPS * $preposition role") {
            yields[Statements](func(
              ast.GraphPrivilege(ast.SetPropertyAction, graphScopeFoo)(_),
              ast.PropertiesResource(propSeq)(_),
              List(ast.RelationshipAllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          // Multiple roles should be allowed

          test(s"$verb$immutableString SET PROPERTY { prop } ON GRAPHS foo $preposition role1, role2") {
            yields[Statements](func(
              ast.GraphPrivilege(ast.SetPropertyAction, graphScopeFoo)(_),
              ast.PropertiesResource(propSeq)(_),
              List(ast.ElementsAllQualifier()(_)),
              Seq(literalRole1, literalRole2),
              immutable
            ))
          }

          // Parameter values

          test(s"$verb$immutableString SET PROPERTY { prop } ON GRAPH $$foo $preposition role") {
            yields[Statements](func(
              ast.GraphPrivilege(ast.SetPropertyAction, graphScopeParamFoo)(_),
              ast.PropertiesResource(propSeq)(_),
              List(ast.ElementsAllQualifier()(_)),
              Seq(literalRole),
              immutable
            ))
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON GRAPH foo $preposition $$role") {
            yields[Statements](func(
              ast.GraphPrivilege(ast.SetPropertyAction, graphScopeFoo)(_),
              ast.PropertiesResource(propSeq)(_),
              List(ast.ElementsAllQualifier()(_)),
              Seq(paramRole),
              immutable
            ))
          }

          // PROPERTYS/PROPERTIES instead of PROPERTY

          test(s"$verb$immutableString SET PROPERTYS { prop } ON GRAPH * $preposition role") {
            val offset = verb.length + immutableString.length + 5
            assertFailsWithMessage[Statements](
              testName,
              s"""Invalid input 'PROPERTYS': expected
                 |  "DATABASE"
                 |  "LABEL"
                 |  "PASSWORD"
                 |  "PASSWORDS"
                 |  "PROPERTY"
                 |  "USER" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
            )
          }

          test(s"$verb$immutableString SET PROPERTIES { prop } ON GRAPH * $preposition role") {
            val offset = verb.length + immutableString.length + 5
            assertFailsWithMessage[Statements](
              testName,
              s"""Invalid input 'PROPERTIES': expected
                 |  "DATABASE"
                 |  "LABEL"
                 |  "PASSWORD"
                 |  "PASSWORDS"
                 |  "PROPERTY"
                 |  "USER" (line 1, column ${offset + 1} (offset: $offset))""".stripMargin
            )
          }

          // Database instead of graph keyword

          test(s"$verb$immutableString SET PROPERTY { prop } ON DATABASES * $preposition role") {
            val offset = verb.length + immutableString.length + 26
            assertFailsWithMessage[Statements](
              testName,
              s"""Invalid input 'DATABASES': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))"""
            )
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON DATABASE foo $preposition role") {
            val offset = verb.length + immutableString.length + 26
            assertFailsWithMessage[Statements](
              testName,
              s"""Invalid input 'DATABASE': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))"""
            )
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON HOME DATABASE $preposition role") {
            failsToParse[Statements]
          }

          test(s"$verb$immutableString SET PROPERTY { prop } ON DEFAULT DATABASE $preposition role") {
            failsToParse[Statements]
          }
      }
  }
}
