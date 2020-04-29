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
import org.neo4j.cypher.internal.ast.AllLabelResource
import org.neo4j.cypher.internal.ast.PrivilegeType
import org.neo4j.cypher.internal.ast.RemoveLabelAction
import org.neo4j.cypher.internal.ast.SetLabelAction
import org.neo4j.cypher.internal.parser.AdministrationCommandParserTestBase
import org.neo4j.cypher.internal.util.InputPosition

class LabelPrivilegeAdministrationCommandParserTest extends AdministrationCommandParserTestBase {

  type privilegeTypeFunction = () => InputPosition => PrivilegeType

  Seq(
    ("GRANT", "TO", grant: resourcePrivilegeFunc),
    ("DENY", "TO", deny: resourcePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrant: resourcePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDeny: resourcePrivilegeFunc),
    ("REVOKE", "FROM", revokeBoth: resourcePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: resourcePrivilegeFunc) =>

      Seq(
        ("SET", SetLabelAction),
        ("REMOVE", RemoveLabelAction)
      ).foreach {
        case (setOrRemove, action) =>

          test(s"$verb $setOrRemove LABEL label ON GRAPH foo $preposition role") {
            yields(func(ast.GraphPrivilege(action)(_), ast.LabelsResource(Seq("label"))(_), List(ast.NamedGraphScope(literal("foo"))(_)), ast.LabelAllQualifier()(_), Seq(literal("role"))))
          }

          // Multiple labels should be allowed

          test(s"$verb $setOrRemove LABEL * ON GRAPH foo $preposition role") {
            yields(func(ast.GraphPrivilege(action)(_), AllLabelResource()(_), List(ast.NamedGraphScope(literal("foo"))(_)), ast.LabelAllQualifier()(_), Seq(literal("role"))))
          }

          test(s"$verb $setOrRemove LABEL label1, label2 ON GRAPH foo $preposition role") {
            yields(func(ast.GraphPrivilege(action)(_), ast.LabelsResource(Seq("label1", "label2"))(_), List(ast.NamedGraphScope(literal("foo"))(_)), ast.LabelAllQualifier()(_), Seq(literal("role"))))
          }

          // Multiple graphs should be allowed

          test(s"$verb $setOrRemove LABEL label ON GRAPHS * $preposition role") {
            yields(func(ast.GraphPrivilege(action)(_), ast.LabelsResource(Seq("label"))(_), List(ast.AllGraphsScope()(_)), ast.LabelAllQualifier()(_), Seq(literal("role"))))
          }

          test(s"$verb $setOrRemove LABEL label ON GRAPHS foo,bar $preposition role") {
            yields(func(ast.GraphPrivilege(action)(_), ast.LabelsResource(Seq("label"))(_), List(ast.NamedGraphScope(literal("foo"))(_), ast.NamedGraphScope(literal("bar"))(_)), ast.LabelAllQualifier()(_), Seq(literal("role"))))
          }

          // Multiple roles should be allowed
          test(s"$verb $setOrRemove LABEL label ON GRAPHS foo $preposition role1, role2") {
            yields(func(ast.GraphPrivilege(action)(_), ast.LabelsResource(Seq("label"))(_), List(ast.NamedGraphScope(literal("foo"))(_)), ast.LabelAllQualifier()(_), Seq(literal("role1"), literal("role2"))))
          }

          // Parameter values

          test(s"$verb $setOrRemove LABEL label ON GRAPH $$foo $preposition role") {
            yields(func(ast.GraphPrivilege(action)(_), ast.LabelsResource(Seq("label"))(_), List(ast.NamedGraphScope(param("foo"))(_)), ast.LabelAllQualifier()(_), Seq(literal("role"))))
          }

          test(s"$verb $setOrRemove LABEL label ON GRAPH foo $preposition $$role") {
            yields(func(ast.GraphPrivilege(action)(_), ast.LabelsResource(Seq("label"))(_), List(ast.NamedGraphScope(literal("foo"))(_)), ast.LabelAllQualifier()(_), Seq(param("role"))))
          }

          // TODO: should this one be supported?
          test(s"$verb $setOrRemove LABEL $$label ON GRAPH foo $preposition role") {
            failsToParse
          }

          // LABELS instead of LABEL
          test(s"$verb $setOrRemove LABELS label ON GRAPH * $preposition role") {
            failsToParse
          }

          // Database instead of graph keyword

          test(s"$verb $setOrRemove LABEL label ON DATABASES * $preposition role") {
            failsToParse
          }

          test(s"$verb $setOrRemove LABEL label ON DATABASE foo $preposition role") {
            failsToParse
          }

          test(s"$verb $setOrRemove LABEL label ON DEFAULT DATABASE $preposition role") {
            failsToParse
          }
      }
  }
}
