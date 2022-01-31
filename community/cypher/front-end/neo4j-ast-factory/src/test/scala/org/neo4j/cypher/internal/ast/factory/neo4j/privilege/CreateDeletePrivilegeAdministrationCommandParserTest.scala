/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.ast.factory.neo4j.privilege

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.CreateElementAction
import org.neo4j.cypher.internal.ast.DeleteElementAction
import org.neo4j.cypher.internal.ast.factory.neo4j.AdministrationAndSchemaCommandParserTestBase

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

          test(s"$verb $createOrDelete ON GRAPH foo $preposition role") {
            yields(func(ast.GraphPrivilege(action, List(graphScopeFoo))(_), List(ast.ElementsAllQualifier()(_)), Seq(literalRole)))
          }

          test(s"$verb $createOrDelete ON GRAPH foo ELEMENTS A $preposition role") {
            yields(func(ast.GraphPrivilege(action, List(graphScopeFoo))(_), List(elemQualifierA), Seq(literalRole)))
          }

          test(s"$verb $createOrDelete ON GRAPH foo NODE A $preposition role") {
            yields(func(ast.GraphPrivilege(action, List(graphScopeFoo))(_), List(labelQualifierA), Seq(literalRole)))
          }

          test(s"$verb $createOrDelete ON GRAPH foo RELATIONSHIPS * $preposition role") {
            yields(func(ast.GraphPrivilege(action, List(graphScopeFoo))(_), List(ast.RelationshipAllQualifier()(_)), Seq(literalRole)))
          }

          // Home graph

          test(s"$verb $createOrDelete ON HOME GRAPH $preposition role") {
            yields(func(ast.GraphPrivilege(action, List(ast.HomeGraphScope()(_)))(_), List(ast.ElementsAllQualifier()(_)), Seq(literalRole)))
          }

          test(s"$verb $createOrDelete ON HOME GRAPH $preposition role1, role2") {
            yields(func(ast.GraphPrivilege(action, List(ast.HomeGraphScope()(_)))(_), List(ast.ElementsAllQualifier()(_)), Seq(literalRole1, literalRole2)))
          }

          test(s"$verb $createOrDelete ON HOME GRAPH $preposition $$role1, role2") {
            yields(func(ast.GraphPrivilege(action, List(ast.HomeGraphScope()(_)))(_), List(ast.ElementsAllQualifier()(_)), Seq(paramRole1, literalRole2)))
          }

          test(s"$verb $createOrDelete ON HOME GRAPH RELATIONSHIPS * $preposition role") {
            yields(func(ast.GraphPrivilege(action, List(ast.HomeGraphScope()(_)))(_), List(ast.RelationshipAllQualifier()(_)), Seq(literalRole)))
          }

          // Both Home and * should not parse
          test(s"$verb $createOrDelete ON HOME GRAPH * $preposition role") {
            failsToParse
          }

          // Default graph

          test(s"$verb $createOrDelete ON DEFAULT GRAPH $preposition role") {
            yields(func(ast.GraphPrivilege(action, List(ast.DefaultGraphScope()(_)))(_), List(ast.ElementsAllQualifier()(_)), Seq(literalRole)))
          }

          test(s"$verb $createOrDelete ON DEFAULT GRAPH $preposition role1, role2") {
            yields(func(ast.GraphPrivilege(action, List(ast.DefaultGraphScope()(_)))(_), List(ast.ElementsAllQualifier()(_)), Seq(literalRole1, literalRole2)))
          }

          test(s"$verb $createOrDelete ON DEFAULT GRAPH $preposition $$role1, role2") {
            yields(func(ast.GraphPrivilege(action, List(ast.DefaultGraphScope()(_)))(_), List(ast.ElementsAllQualifier()(_)), Seq(paramRole1, literalRole2)))
          }

          test(s"$verb $createOrDelete ON DEFAULT GRAPH RELATIONSHIPS * $preposition role") {
            yields(func(ast.GraphPrivilege(action, List(ast.DefaultGraphScope()(_)))(_), List(ast.RelationshipAllQualifier()(_)), Seq(literalRole)))
          }

          // Both Default and * should not parse
          test(s"$verb $createOrDelete ON DEFAULT GRAPH * $preposition role") {
            failsToParse
          }

          test(s"$verb $createOrDelete ON DATABASE blah $preposition role") {
            val offset = verb.length + createOrDelete.length + 5
            assertFailsWithMessage(testName,
              s"""Invalid input 'DATABASE': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))""")
          }
      }
  }
}
