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
import org.neo4j.cypher.internal.ast.AllLabelResource
import org.neo4j.cypher.internal.ast.RemoveLabelAction
import org.neo4j.cypher.internal.ast.SetLabelAction
import org.neo4j.cypher.internal.ast.factory.neo4j.AdministrationAndSchemaCommandParserTestBase

class LabelPrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {
  private val labelResource = ast.LabelsResource(Seq("label"))(_)

  Seq(
    ("GRANT", "TO", grantGraphPrivilege: resourcePrivilegeFunc),
    ("DENY", "TO", denyGraphPrivilege: resourcePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: resourcePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyGraphPrivilege: resourcePrivilegeFunc),
    ("REVOKE", "FROM", revokeGraphPrivilege: resourcePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: resourcePrivilegeFunc) =>
      Seq(
        ("SET", SetLabelAction),
        ("REMOVE", RemoveLabelAction)
      ).foreach {
        case (setOrRemove, action) =>
          test(s"$verb $setOrRemove LABEL label ON GRAPH foo $preposition role") {
            yields(func(
              ast.GraphPrivilege(action, List(graphScopeFoo))(_),
              labelResource,
              List(ast.LabelAllQualifier()(_)),
              Seq(literalRole)
            ))
          }

          // Multiple labels should be allowed

          test(s"$verb $setOrRemove LABEL * ON GRAPH foo $preposition role") {
            yields(func(
              ast.GraphPrivilege(action, List(graphScopeFoo))(_),
              AllLabelResource()(_),
              List(ast.LabelAllQualifier()(_)),
              Seq(literalRole)
            ))
          }

          test(s"$verb $setOrRemove LABEL label1, label2 ON GRAPH foo $preposition role") {
            yields(func(
              ast.GraphPrivilege(action, List(graphScopeFoo))(_),
              ast.LabelsResource(Seq("label1", "label2"))(_),
              List(ast.LabelAllQualifier()(_)),
              Seq(literalRole)
            ))
          }

          // Multiple graphs should be allowed

          test(s"$verb $setOrRemove LABEL label ON GRAPHS * $preposition role") {
            yields(func(
              ast.GraphPrivilege(action, List(ast.AllGraphsScope()(_)))(_),
              labelResource,
              List(ast.LabelAllQualifier()(_)),
              Seq(literalRole)
            ))
          }

          test(s"$verb $setOrRemove LABEL label ON GRAPHS foo,baz $preposition role") {
            yields(func(
              ast.GraphPrivilege(action, List(graphScopeFoo, graphScopeBaz))(_),
              labelResource,
              List(ast.LabelAllQualifier()(_)),
              Seq(literalRole)
            ))
          }

          // Home graph should be allowed

          test(s"$verb $setOrRemove LABEL label ON HOME GRAPH $preposition role") {
            yields(func(
              ast.GraphPrivilege(action, List(ast.HomeGraphScope()(_)))(_),
              labelResource,
              List(ast.LabelAllQualifier()(_)),
              Seq(literalRole)
            ))
          }

          test(s"$verb $setOrRemove LABEL * ON HOME GRAPH $preposition role") {
            yields(func(
              ast.GraphPrivilege(action, List(ast.HomeGraphScope()(_)))(_),
              ast.AllLabelResource()(_),
              List(ast.LabelAllQualifier()(_)),
              Seq(literalRole)
            ))
          }

          // Default graph should be allowed

          test(s"$verb $setOrRemove LABEL label ON DEFAULT GRAPH $preposition role") {
            yields(func(
              ast.GraphPrivilege(action, List(ast.DefaultGraphScope()(_)))(_),
              labelResource,
              List(ast.LabelAllQualifier()(_)),
              Seq(literalRole)
            ))
          }

          test(s"$verb $setOrRemove LABEL * ON DEFAULT GRAPH $preposition role") {
            yields(func(
              ast.GraphPrivilege(action, List(ast.DefaultGraphScope()(_)))(_),
              ast.AllLabelResource()(_),
              List(ast.LabelAllQualifier()(_)),
              Seq(literalRole)
            ))
          }

          // Multiple roles should be allowed

          test(s"$verb $setOrRemove LABEL label ON GRAPHS foo $preposition role1, role2") {
            yields(func(
              ast.GraphPrivilege(action, List(graphScopeFoo))(_),
              labelResource,
              List(ast.LabelAllQualifier()(_)),
              Seq(literalRole1, literalRole2)
            ))
          }

          // Parameter values

          test(s"$verb $setOrRemove LABEL label ON GRAPH $$foo $preposition role") {
            yields(func(
              ast.GraphPrivilege(action, List(graphScopeParamFoo))(_),
              labelResource,
              List(ast.LabelAllQualifier()(_)),
              Seq(literalRole)
            ))
          }

          test(s"$verb $setOrRemove LABEL label ON GRAPH foo $preposition $$role") {
            yields(func(
              ast.GraphPrivilege(action, List(graphScopeFoo))(_),
              labelResource,
              List(ast.LabelAllQualifier()(_)),
              Seq(paramRole)
            ))
          }

          // TODO: should this one be supported?
          test(s"$verb $setOrRemove LABEL $$label ON GRAPH foo $preposition role") {
            failsToParse
          }

          // LABELS instead of LABEL

          test(s"$verb $setOrRemove LABELS label ON GRAPH * $preposition role") {
            assertFailsWithMessageStart(testName, s"""Invalid input 'LABELS': expected""")
          }

          // Database instead of graph keyword

          test(s"$verb $setOrRemove LABEL label ON DATABASES * $preposition role") {
            assertFailsWithMessageStart(testName, s"""Invalid input 'DATABASES': expected""")
          }

          test(s"$verb $setOrRemove LABEL label ON DATABASE foo $preposition role") {
            assertFailsWithMessageStart(testName, s"""Invalid input 'DATABASE': expected""")
          }

          test(s"$verb $setOrRemove LABEL label ON HOME DATABASE $preposition role") {
            failsToParse
          }

          test(s"$verb $setOrRemove LABEL label ON DEFAULT DATABASE $preposition role") {
            failsToParse
          }
      }
  }
}
