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
import org.neo4j.cypher.internal.ast.AllPropertyResource
import org.neo4j.cypher.internal.ast.MergeAdminAction
import org.neo4j.cypher.internal.ast.PrivilegeType
import org.neo4j.cypher.internal.parser.AdministrationCommandParserTestBase
import org.neo4j.cypher.internal.util.InputPosition

class MergePrivilegeAdministrationCommandParserTest extends AdministrationCommandParserTestBase {
  type privilegeTypeFunction = () => InputPosition => PrivilegeType

  Seq(
    ("GRANT", "TO", grantGraphPrivilege: resourcePrivilegeFunc),
    ("DENY", "TO", denyGraphPrivilege: resourcePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: resourcePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyGraphPrivilege: resourcePrivilegeFunc),
    ("REVOKE", "FROM", revokeGraphPrivilege: resourcePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: resourcePrivilegeFunc) =>

      test(s"$verb MERGE { prop } ON GRAPH foo $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction)(_), ast.PropertiesResource(Seq("prop"))(_), List(ast.NamedGraphScope(literal("foo"))(_)), List(ast.ElementsAllQualifier()(_)), Seq(literal("role"))))
      }

      // Multiple properties should be allowed

      test(s"$verb MERGE { * } ON GRAPH foo $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction)(_), AllPropertyResource()(_), List(ast.NamedGraphScope(literal("foo"))(_)), List(ast.ElementsAllQualifier()(_)), Seq(literal("role"))))
      }

      test(s"$verb MERGE { prop1, prop2 } ON GRAPH foo $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction)(_), ast.PropertiesResource(Seq("prop1", "prop2"))(_), List(ast.NamedGraphScope(literal("foo"))(_)), List(ast.ElementsAllQualifier()(_)), Seq(literal("role"))))
      }

      // Default graph should be allowed

      test(s"$verb MERGE { * } ON DEFAULT GRAPH $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction)(_), AllPropertyResource()(_), List(ast.DefaultGraphScope()(_)), List(ast.ElementsAllQualifier()(_)), Seq(literal("role"))))
      }

      test(s"$verb MERGE { prop1, prop2 } ON DEFAULT GRAPH RELATIONSHIP * $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction)(_), ast.PropertiesResource(Seq("prop1", "prop2"))(_), List(ast.DefaultGraphScope()(_)), List(ast.RelationshipAllQualifier()(_)), Seq(literal("role"))))
      }

      // Multiple graphs should be allowed

      test(s"$verb MERGE { prop } ON GRAPHS * $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction)(_), ast.PropertiesResource(Seq("prop"))(_), List(ast.AllGraphsScope()(_)), List(ast.ElementsAllQualifier()(_)), Seq(literal("role"))))
      }

      test(s"$verb MERGE { prop } ON GRAPHS foo,bar $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction)(_), ast.PropertiesResource(Seq("prop"))(_), List(ast.NamedGraphScope(literal("foo"))(_), ast.NamedGraphScope(literal("bar"))(_)), List(ast.ElementsAllQualifier()(_)), Seq(literal("role"))))
      }

      // Qualifiers

      test(s"$verb MERGE { prop } ON GRAPHS foo ELEMENTS foo,bar $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction)(_), ast.PropertiesResource(Seq("prop"))(_), List(ast.NamedGraphScope(literal("foo"))(_)), List(ast.ElementQualifier("foo") _, ast.ElementQualifier("bar") _), Seq(literal("role"))))
      }

      test(s"$verb MERGE { prop } ON GRAPHS foo ELEMENT foo $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction)(_), ast.PropertiesResource(Seq("prop"))(_), List(ast.NamedGraphScope(literal("foo"))(_)), List(ast.ElementQualifier("foo")(_)), Seq(literal("role"))))
      }

      test(s"$verb MERGE { prop } ON GRAPHS foo NODES foo,bar $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction)(_), ast.PropertiesResource(Seq("prop"))(_), List(ast.NamedGraphScope(literal("foo"))(_)), List(ast.LabelQualifier("foo") _, ast.LabelQualifier("bar") _), Seq(literal("role"))))
      }

      test(s"$verb MERGE { prop } ON GRAPHS foo NODES * $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction)(_), ast.PropertiesResource(Seq("prop"))(_), List(ast.NamedGraphScope(literal("foo"))(_)), List(ast.LabelAllQualifier()(_)), Seq(literal("role"))))
      }

      test(s"$verb MERGE { prop } ON GRAPHS foo RELATIONSHIPS foo,bar $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction)(_), ast.PropertiesResource(Seq("prop"))(_), List(ast.NamedGraphScope(literal("foo"))(_)), List(ast.RelationshipQualifier("foo") _, ast.RelationshipQualifier("bar") _), Seq(literal("role"))))
      }

      test(s"$verb MERGE { prop } ON GRAPHS foo RELATIONSHIP * $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction)(_), ast.PropertiesResource(Seq("prop"))(_), List(ast.NamedGraphScope(literal("foo"))(_)), List(ast.RelationshipAllQualifier()(_)), Seq(literal("role"))))
      }

      // Multiple roles should be allowed
      test(s"$verb MERGE { prop } ON GRAPHS foo $preposition role1, role2") {
        yields(func(ast.GraphPrivilege(MergeAdminAction)(_), ast.PropertiesResource(Seq("prop"))(_), List(ast.NamedGraphScope(literal("foo"))(_)), List(ast.ElementsAllQualifier()(_)), Seq(literal("role1"), literal("role2"))))
      }

      // Parameter values

      test(s"$verb MERGE { prop } ON GRAPH $$foo $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction)(_), ast.PropertiesResource(Seq("prop"))(_), List(ast.NamedGraphScope(param("foo"))(_)), List(ast.ElementsAllQualifier()(_)), Seq(literal("role"))))
      }

      test(s"$verb MERGE { prop } ON GRAPH foo $preposition $$role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction)(_), ast.PropertiesResource(Seq("prop"))(_), List(ast.NamedGraphScope(literal("foo"))(_)), List(ast.ElementsAllQualifier()(_)), Seq(param("role"))))
      }

      // Database instead of graph keyword

      test(s"$verb MERGE { prop } ON DATABASES * $preposition role") {
        failsToParse
      }

      test(s"$verb MERGE { prop } ON DATABASE foo $preposition role") {
        failsToParse
      }

      test(s"$verb MERGE { prop } ON DEFAULT DATABASE $preposition role") {
        failsToParse
      }
  }
}
