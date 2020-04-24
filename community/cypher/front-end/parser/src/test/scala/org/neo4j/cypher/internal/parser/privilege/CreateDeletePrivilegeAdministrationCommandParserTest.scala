/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.cypher.internal.ast.CreateElementAction
import org.neo4j.cypher.internal.ast.DeleteElementAction
import org.neo4j.cypher.internal.ast.PrivilegeType
import org.neo4j.cypher.internal.parser.AdministrationCommandParserTestBase
import org.neo4j.cypher.internal.util.InputPosition

class CreateDeletePrivilegeAdministrationCommandParserTest extends AdministrationCommandParserTestBase {

  type privilegeTypeFunction = () => InputPosition => PrivilegeType

  Seq(
    ("GRANT", "TO", grant: noResourcePrivilegeFunc),
    ("DENY", "TO", deny: noResourcePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrant: noResourcePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDeny: noResourcePrivilegeFunc),
    ("REVOKE", "FROM", revokeBoth: noResourcePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: noResourcePrivilegeFunc) =>

      Seq(
        ("CREATE", CreateElementAction),
        ("DELETE", DeleteElementAction)
      ).foreach {
        case (createOrDelete, action) =>

          test(s"$verb $createOrDelete ON GRAPH foo $preposition role") {
            yields(func(ast.GraphPrivilege(action)(_), List(ast.NamedGraphScope(literal("foo"))(_)), ast.ElementsAllQualifier()(_), Seq(literal("role"))))
          }

          test(s"$verb $createOrDelete ON GRAPH foo ELEMENTS bar $preposition role") {
            yields(func(ast.GraphPrivilege(action)(_), List(ast.NamedGraphScope(literal("foo"))(_)), ast.ElementsQualifier(Seq("bar"))(_), Seq(literal("role"))))
          }

          test(s"$verb $createOrDelete ON GRAPH foo NODE bar $preposition role") {
            yields(func(ast.GraphPrivilege(action)(_), List(ast.NamedGraphScope(literal("foo"))(_)), ast.LabelsQualifier(Seq("bar"))(_), Seq(literal("role"))))
          }

          test(s"$verb $createOrDelete ON GRAPH foo RELATIONSHIPS * $preposition role") {
            yields(func(ast.GraphPrivilege(action)(_), List(ast.NamedGraphScope(literal("foo"))(_)), ast.RelationshipAllQualifier()(_), Seq(literal("role"))))
          }

          test(s"$verb $createOrDelete ON DATABASE blah $preposition role") {
            failsToParse
          }
      }
  }
}
