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
import org.neo4j.cypher.internal.ast.factory.neo4j.AdministrationAndSchemaCommandParserTestBase

class MergePrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq(
    ("GRANT", "TO", grantGraphPrivilege: resourcePrivilegeFunc),
    ("DENY", "TO", denyGraphPrivilege: resourcePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: resourcePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyGraphPrivilege: resourcePrivilegeFunc),
    ("REVOKE", "FROM", revokeGraphPrivilege: resourcePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: resourcePrivilegeFunc) =>
      test(s"$verb MERGE { prop } ON GRAPH foo $preposition role") {
        yields(func(
          ast.GraphPrivilege(ast.MergeAdminAction, List(graphScopeFoo))(_),
          ast.PropertiesResource(propSeq)(_),
          List(ast.ElementsAllQualifier()(_)),
          Seq(literalRole)
        ))
      }

      // Multiple properties should be allowed

      test(s"$verb MERGE { * } ON GRAPH foo $preposition role") {
        yields(func(
          ast.GraphPrivilege(ast.MergeAdminAction, List(graphScopeFoo))(_),
          ast.AllPropertyResource()(_),
          List(ast.ElementsAllQualifier()(_)),
          Seq(literalRole)
        ))
      }

      test(s"$verb MERGE { prop1, prop2 } ON GRAPH foo $preposition role") {
        yields(func(
          ast.GraphPrivilege(ast.MergeAdminAction, List(graphScopeFoo))(_),
          ast.PropertiesResource(Seq("prop1", "prop2"))(_),
          List(ast.ElementsAllQualifier()(_)),
          Seq(literalRole)
        ))
      }

      // Home graph should be allowed

      test(s"$verb MERGE { * } ON HOME GRAPH $preposition role") {
        yields(func(
          ast.GraphPrivilege(ast.MergeAdminAction, List(ast.HomeGraphScope()(_)))(_),
          ast.AllPropertyResource()(_),
          List(ast.ElementsAllQualifier()(_)),
          Seq(literalRole)
        ))
      }

      test(s"$verb MERGE { prop1, prop2 } ON HOME GRAPH RELATIONSHIP * $preposition role") {
        yields(func(
          ast.GraphPrivilege(ast.MergeAdminAction, List(ast.HomeGraphScope()(_)))(_),
          ast.PropertiesResource(Seq("prop1", "prop2"))(_),
          List(ast.RelationshipAllQualifier()(_)),
          Seq(literalRole)
        ))
      }

      // Default graph should be allowed

      test(s"$verb MERGE { * } ON DEFAULT GRAPH $preposition role") {
        yields(func(
          ast.GraphPrivilege(ast.MergeAdminAction, List(ast.DefaultGraphScope()(_)))(_),
          ast.AllPropertyResource()(_),
          List(ast.ElementsAllQualifier()(_)),
          Seq(literalRole)
        ))
      }

      test(s"$verb MERGE { prop1, prop2 } ON DEFAULT GRAPH RELATIONSHIP * $preposition role") {
        yields(func(
          ast.GraphPrivilege(ast.MergeAdminAction, List(ast.DefaultGraphScope()(_)))(_),
          ast.PropertiesResource(Seq("prop1", "prop2"))(_),
          List(ast.RelationshipAllQualifier()(_)),
          Seq(literalRole)
        ))
      }

      // Multiple graphs should be allowed

      test(s"$verb MERGE { prop } ON GRAPHS * $preposition role") {
        yields(func(
          ast.GraphPrivilege(ast.MergeAdminAction, List(ast.AllGraphsScope()(_)))(_),
          ast.PropertiesResource(propSeq)(_),
          List(ast.ElementsAllQualifier()(_)),
          Seq(literalRole)
        ))
      }

      test(s"$verb MERGE { prop } ON GRAPHS foo,baz $preposition role") {
        yields(func(
          ast.GraphPrivilege(ast.MergeAdminAction, List(graphScopeFoo, graphScopeBaz))(_),
          ast.PropertiesResource(propSeq)(_),
          List(ast.ElementsAllQualifier()(_)),
          Seq(literalRole)
        ))
      }

      // Qualifiers

      test(s"$verb MERGE { prop } ON GRAPHS foo ELEMENTS A,B $preposition role") {
        yields(func(
          ast.GraphPrivilege(ast.MergeAdminAction, List(graphScopeFoo))(_),
          ast.PropertiesResource(propSeq)(_),
          List(elemQualifierA, elemQualifierB),
          Seq(literalRole)
        ))
      }

      test(s"$verb MERGE { prop } ON GRAPHS foo ELEMENT A $preposition role") {
        yields(func(
          ast.GraphPrivilege(ast.MergeAdminAction, List(graphScopeFoo))(_),
          ast.PropertiesResource(propSeq)(_),
          List(elemQualifierA),
          Seq(literalRole)
        ))
      }

      test(s"$verb MERGE { prop } ON GRAPHS foo NODES A,B $preposition role") {
        yields(func(
          ast.GraphPrivilege(ast.MergeAdminAction, List(graphScopeFoo))(_),
          ast.PropertiesResource(propSeq)(_),
          List(labelQualifierA, labelQualifierB),
          Seq(literalRole)
        ))
      }

      test(s"$verb MERGE { prop } ON GRAPHS foo NODES * $preposition role") {
        yields(func(
          ast.GraphPrivilege(ast.MergeAdminAction, List(graphScopeFoo))(_),
          ast.PropertiesResource(propSeq)(_),
          List(ast.LabelAllQualifier()(_)),
          Seq(literalRole)
        ))
      }

      test(s"$verb MERGE { prop } ON GRAPHS foo RELATIONSHIPS A,B $preposition role") {
        yields(func(
          ast.GraphPrivilege(ast.MergeAdminAction, List(graphScopeFoo))(_),
          ast.PropertiesResource(propSeq)(_),
          List(relQualifierA, relQualifierB),
          Seq(literalRole)
        ))
      }

      test(s"$verb MERGE { prop } ON GRAPHS foo RELATIONSHIP * $preposition role") {
        yields(func(
          ast.GraphPrivilege(ast.MergeAdminAction, List(graphScopeFoo))(_),
          ast.PropertiesResource(propSeq)(_),
          List(ast.RelationshipAllQualifier()(_)),
          Seq(literalRole)
        ))
      }

      // Multiple roles should be allowed

      test(s"$verb MERGE { prop } ON GRAPHS foo $preposition role1, role2") {
        yields(func(
          ast.GraphPrivilege(ast.MergeAdminAction, List(graphScopeFoo))(_),
          ast.PropertiesResource(propSeq)(_),
          List(ast.ElementsAllQualifier()(_)),
          Seq(literalRole1, literalRole2)
        ))
      }

      // Parameter values

      test(s"$verb MERGE { prop } ON GRAPH $$foo $preposition role") {
        yields(func(
          ast.GraphPrivilege(ast.MergeAdminAction, List(graphScopeParamFoo))(_),
          ast.PropertiesResource(propSeq)(_),
          List(ast.ElementsAllQualifier()(_)),
          Seq(literalRole)
        ))
      }

      test(s"$verb MERGE { prop } ON GRAPH foo $preposition $$role") {
        yields(func(
          ast.GraphPrivilege(ast.MergeAdminAction, List(graphScopeFoo))(_),
          ast.PropertiesResource(propSeq)(_),
          List(ast.ElementsAllQualifier()(_)),
          Seq(paramRole)
        ))
      }

      // Database instead of graph keyword

      test(s"$verb MERGE { prop } ON DATABASES * $preposition role") {
        val offset = verb.length + 19
        assertFailsWithMessage(
          testName,
          s"""Invalid input 'DATABASES': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))"""
        )
      }

      test(s"$verb MERGE { prop } ON DATABASE foo $preposition role") {
        val offset = verb.length + 19
        assertFailsWithMessage(
          testName,
          s"""Invalid input 'DATABASE': expected "DEFAULT", "GRAPH", "GRAPHS" or "HOME" (line 1, column ${offset + 1} (offset: $offset))"""
        )
      }

      test(s"$verb MERGE { prop } ON HOME DATABASE $preposition role") {
        failsToParse
      }

      test(s"$verb MERGE { prop } ON DEFAULT DATABASE $preposition role") {
        failsToParse
      }
  }
}
