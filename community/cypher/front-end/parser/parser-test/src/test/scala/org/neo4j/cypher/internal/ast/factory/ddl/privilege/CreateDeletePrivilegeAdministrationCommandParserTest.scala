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

import org.neo4j.cypher.internal.ast.CreateElementAction
import org.neo4j.cypher.internal.ast.DefaultGraphScope
import org.neo4j.cypher.internal.ast.DeleteElementAction
import org.neo4j.cypher.internal.ast.ElementsAllQualifier
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.RelationshipAllQualifier
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.ddl.AdministrationAndSchemaCommandParserTestBase
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc

class CreateDeletePrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq(
    ("GRANT", "TO", grantGraphPrivilege: noResourcePrivilegeFunc),
    ("DENY", "TO", denyGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE", "FROM", revokeGraphPrivilege: noResourcePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: noResourcePrivilegeFunc) =>
      Seq(
        ("CREATE", CreateElementAction),
        ("DELETE", DeleteElementAction)
      ).foreach {
        case (createOrDelete, action) =>
          Seq[Immutable](true, false).foreach {
            immutable =>
              val immutableString = immutableOrEmpty(immutable)
              test(s"$verb$immutableString $createOrDelete ON GRAPH foo $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, graphScopeFoo)(_),
                  List(ElementsAllQualifier()(_)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $createOrDelete ON GRAPH foo ELEMENTS A $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, graphScopeFoo)(_),
                  List(elemQualifierA),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $createOrDelete ON GRAPH foo NODE A $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, graphScopeFoo)(_),
                  List(labelQualifierA),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $createOrDelete ON GRAPH foo RELATIONSHIPS * $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, graphScopeFoo)(_),
                  List(RelationshipAllQualifier()(_)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              // Home graph

              test(s"$verb$immutableString $createOrDelete ON HOME GRAPH $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, HomeGraphScope()(_))(_),
                  List(ElementsAllQualifier()(_)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $createOrDelete ON HOME GRAPH $preposition role1, role2") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, HomeGraphScope()(_))(_),
                  List(ElementsAllQualifier()(_)),
                  Seq(literalRole1, literalRole2),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $createOrDelete ON HOME GRAPH $preposition $$role1, role2") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, HomeGraphScope()(_))(_),
                  List(ElementsAllQualifier()(_)),
                  Seq(paramRole1, literalRole2),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $createOrDelete ON HOME GRAPH RELATIONSHIPS * $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, HomeGraphScope()(_))(_),
                  List(RelationshipAllQualifier()(_)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              // Both Home and * should not parse
              test(s"$verb$immutableString $createOrDelete ON HOME GRAPH * $preposition role") {
                failsParsing[Statements]
              }

              // Default graph

              test(s"$verb$immutableString $createOrDelete ON DEFAULT GRAPH $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, DefaultGraphScope()(_))(_),
                  List(ElementsAllQualifier()(_)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $createOrDelete ON DEFAULT GRAPH $preposition role1, role2") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, DefaultGraphScope()(_))(_),
                  List(ElementsAllQualifier()(_)),
                  Seq(literalRole1, literalRole2),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $createOrDelete ON DEFAULT GRAPH $preposition $$role1, role2") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, DefaultGraphScope()(_))(_),
                  List(ElementsAllQualifier()(_)),
                  Seq(paramRole1, literalRole2),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $createOrDelete ON DEFAULT GRAPH RELATIONSHIPS * $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, DefaultGraphScope()(_))(_),
                  List(RelationshipAllQualifier()(_)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              // Both Default and * should not parse
              test(s"$verb$immutableString $createOrDelete ON DEFAULT GRAPH * $preposition role") {
                failsParsing[Statements]
              }

              // Database instead of graph
              test(s"$verb$immutableString $createOrDelete ON DATABASE blah $preposition role") {
                val offset = verb.length + immutableString.length + createOrDelete.length + 5
                failsParsing[Statements].in {
                  case Cypher5JavaCc => _.withMessageStart(
                      s"""Invalid input 'DATABASE': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))"""
                    )
                  case _ => _.withSyntaxErrorContaining(
                      s"""Invalid input 'DATABASE': expected 'GRAPH', 'DEFAULT GRAPH', 'HOME GRAPH' or 'GRAPHS' (line 1, column ${offset + 1} (offset: $offset))"""
                    )
                }
              }

              // Alias with too many components
              test(s"$verb$immutableString $createOrDelete ON GRAPH `a`.`b`.`c` $preposition role") {
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
