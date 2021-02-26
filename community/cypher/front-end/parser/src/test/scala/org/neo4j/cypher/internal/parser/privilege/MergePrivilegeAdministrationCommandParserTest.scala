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
import org.neo4j.cypher.internal.ast.AllPropertyResource
import org.neo4j.cypher.internal.ast.MergeAdminAction
import org.neo4j.cypher.internal.parser.AdministrationCommandParserTestBase

class MergePrivilegeAdministrationCommandParserTest extends AdministrationCommandParserTestBase {

  Seq(
    ("GRANT", "TO", grantGraphPrivilege: resourcePrivilegeFunc),
    ("DENY", "TO", denyGraphPrivilege: resourcePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: resourcePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyGraphPrivilege: resourcePrivilegeFunc),
    ("REVOKE", "FROM", revokeGraphPrivilege: resourcePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: resourcePrivilegeFunc) =>

      test(s"$verb MERGE { prop } ON GRAPH foo $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction, List(graphScopeFoo))(_), ast.PropertiesResource(propSeq)(_), List(ast.ElementsAllQualifier()(_)), Seq(literalRole)))
      }

      // Multiple properties should be allowed

      test(s"$verb MERGE { * } ON GRAPH foo $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction, List(graphScopeFoo))(_), AllPropertyResource()(_), List(ast.ElementsAllQualifier()(_)), Seq(literalRole)))
      }

      test(s"$verb MERGE { prop1, prop2 } ON GRAPH foo $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction, List(graphScopeFoo))(_), ast.PropertiesResource(Seq("prop1", "prop2"))(_), List(ast.ElementsAllQualifier()(_)), Seq(literalRole)))
      }

      // Home graph should be allowed

      test(s"$verb MERGE { * } ON HOME GRAPH $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction, List(ast.HomeGraphScope()(_)))(_), AllPropertyResource()(_), List(ast.ElementsAllQualifier()(_)), Seq(literalRole)))
      }

      test(s"$verb MERGE { prop1, prop2 } ON HOME GRAPH RELATIONSHIP * $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction, List(ast.HomeGraphScope()(_)))(_), ast.PropertiesResource(Seq("prop1", "prop2"))(_), List(ast.RelationshipAllQualifier()(_)), Seq(literalRole)))
      }

      // Default graph should be allowed

      test(s"$verb MERGE { * } ON DEFAULT GRAPH $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction, List(ast.DefaultGraphScope()(_)))(_), AllPropertyResource()(_), List(ast.ElementsAllQualifier()(_)), Seq(literalRole)))
      }

      test(s"$verb MERGE { prop1, prop2 } ON DEFAULT GRAPH RELATIONSHIP * $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction, List(ast.DefaultGraphScope()(_)))(_), ast.PropertiesResource(Seq("prop1", "prop2"))(_), List(ast.RelationshipAllQualifier()(_)), Seq(literalRole)))
      }

      // Multiple graphs should be allowed

      test(s"$verb MERGE { prop } ON GRAPHS * $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction, List(ast.AllGraphsScope()(_)))(_), ast.PropertiesResource(propSeq)(_), List(ast.ElementsAllQualifier()(_)), Seq(literalRole)))
      }

      test(s"$verb MERGE { prop } ON GRAPHS foo,baz $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction, List(graphScopeFoo, graphScopeBaz))(_), ast.PropertiesResource(propSeq)(_), List(ast.ElementsAllQualifier()(_)), Seq(literalRole)))
      }

      // Qualifiers

      test(s"$verb MERGE { prop } ON GRAPHS foo ELEMENTS A,B $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction, List(graphScopeFoo))(_), ast.PropertiesResource(propSeq)(_), List(elemQualifierA, elemQualifierB), Seq(literalRole)))
      }

      test(s"$verb MERGE { prop } ON GRAPHS foo ELEMENT A $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction, List(graphScopeFoo))(_), ast.PropertiesResource(propSeq)(_), List(elemQualifierA), Seq(literalRole)))
      }

      test(s"$verb MERGE { prop } ON GRAPHS foo NODES A,B $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction, List(graphScopeFoo))(_), ast.PropertiesResource(propSeq)(_), List(labelQualifierA, labelQualifierB), Seq(literalRole)))
      }

      test(s"$verb MERGE { prop } ON GRAPHS foo NODES * $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction, List(graphScopeFoo))(_), ast.PropertiesResource(propSeq)(_), List(ast.LabelAllQualifier()(_)), Seq(literalRole)))
      }

      test(s"$verb MERGE { prop } ON GRAPHS foo RELATIONSHIPS A,B $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction, List(graphScopeFoo))(_), ast.PropertiesResource(propSeq)(_), List(relQualifierA, relQualifierB), Seq(literalRole)))
      }

      test(s"$verb MERGE { prop } ON GRAPHS foo RELATIONSHIP * $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction, List(graphScopeFoo))(_), ast.PropertiesResource(propSeq)(_), List(ast.RelationshipAllQualifier()(_)), Seq(literalRole)))
      }

      // Multiple roles should be allowed

      test(s"$verb MERGE { prop } ON GRAPHS foo $preposition role1, role2") {
        yields(func(ast.GraphPrivilege(MergeAdminAction, List(graphScopeFoo))(_), ast.PropertiesResource(propSeq)(_), List(ast.ElementsAllQualifier()(_)), Seq(literalRole1, literalRole2)))
      }

      // Parameter values

      test(s"$verb MERGE { prop } ON GRAPH $$foo $preposition role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction, List(graphScopeParamFoo))(_), ast.PropertiesResource(propSeq)(_), List(ast.ElementsAllQualifier()(_)), Seq(literalRole)))
      }

      test(s"$verb MERGE { prop } ON GRAPH foo $preposition $$role") {
        yields(func(ast.GraphPrivilege(MergeAdminAction, List(graphScopeFoo))(_), ast.PropertiesResource(propSeq)(_), List(ast.ElementsAllQualifier()(_)), Seq(paramRole)))
      }

      // Database instead of graph keyword

      test(s"$verb MERGE { prop } ON DATABASES * $preposition role") {
        failsToParse
      }

      test(s"$verb MERGE { prop } ON DATABASE foo $preposition role") {
        failsToParse
      }

      test(s"$verb MERGE { prop } ON HOME DATABASE $preposition role") {
        failsToParse
      }

      test(s"$verb MERGE { prop } ON DEFAULT DATABASE $preposition role") {
        failsToParse
      }
  }
}
