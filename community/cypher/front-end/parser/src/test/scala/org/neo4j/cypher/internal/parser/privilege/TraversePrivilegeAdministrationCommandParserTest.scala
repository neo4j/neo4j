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
import org.neo4j.cypher.internal.ast.WriteAction
import org.neo4j.cypher.internal.parser.AdministrationCommandParserTestBase

class TraversePrivilegeAdministrationCommandParserTest extends AdministrationCommandParserTestBase {

  Seq(
    ("GRANT", "TO", grantGraphPrivilege: noResourcePrivilegeFunc),
    ("DENY", "TO", denyGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE", "FROM", revokeGraphPrivilege: noResourcePrivilegeFunc)
  ).foreach {
    case (command: String, preposition: String, func: noResourcePrivilegeFunc) =>

      test(s"$command TRAVERSE ON DEFAULT GRAPH $preposition role") {
        yields(func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.DefaultGraphScope()(_)), List(ast.ElementsAllQualifier() _), Seq(literal("role"))))
      }

      test(s"$command TRAVERSE ON DEFAULT GRAPH NODE Label $preposition role") {
        yields(func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.DefaultGraphScope()(_)), List(ast.LabelQualifier("Label") _), Seq(literal("role"))))
      }

      test(s"$command TRAVERSE ON DEFAULT GRAPH RELATIONSHIP * $preposition role") {
        yields(func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.DefaultGraphScope()(_)), List(ast.RelationshipAllQualifier() _), Seq(literal("role"))))
      }

      test(s"$command TRAVERSE ON DEFAULT GRAPH ELEMENT Label $preposition role") {
        yields(func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.DefaultGraphScope()(_)), List(ast.ElementQualifier("Label") _), Seq(literal("role"))))
      }

      Seq("GRAPH", "GRAPHS").foreach {
        graphKeyword =>
          test(s"$command TRAVERSE ON $graphKeyword * $preposition $$role") {
            yields(func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.AllGraphsScope() _), List(ast.ElementsAllQualifier() _), Seq(param("role"))))
          }

          test(s"$command TRAVERSE ON $graphKeyword foo $preposition role") {
            yields(func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.ElementsAllQualifier() _), Seq(literal("role"))))
          }

          test(s"$command TRAVERSE ON $graphKeyword $$foo $preposition role") {
            yields(func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(param("foo")) _), List(ast.ElementsAllQualifier() _), Seq(literal("role"))))
          }

          Seq("NODE", "NODES").foreach {
            nodeKeyword =>

              test( s"validExpressions $command $graphKeyword $nodeKeyword $preposition") {
                parsing(s"$command TRAVERSE ON $graphKeyword * $nodeKeyword * $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.AllGraphsScope() _), List(ast.LabelAllQualifier() _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword * $nodeKeyword * (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.AllGraphsScope() _), List(ast.LabelAllQualifier() _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword * $nodeKeyword A $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.AllGraphsScope() _), List(ast.LabelQualifier("A") _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword * $nodeKeyword A (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.AllGraphsScope() _), List(ast.LabelQualifier("A") _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword `*` $nodeKeyword A $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("*")) _), List(ast.LabelQualifier("A") _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword * $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.LabelAllQualifier() _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword * (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.LabelAllQualifier() _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.LabelQualifier("A") _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.LabelQualifier("A") _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition role1, $$role2") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.LabelQualifier("A") _), Seq(literal("role1"), param("role2")))
                parsing(s"$command TRAVERSE ON $graphKeyword `2foo` $nodeKeyword A (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("2foo")) _), List(ast.LabelQualifier("A") _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition `r:ole`") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.LabelQualifier("A") _), Seq(literal("r:ole")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword `A B` (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.LabelQualifier("A B") _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A, B (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.LabelQualifier("A") _,ast.LabelQualifier("B") _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A, B (*) $preposition role1, role2") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.LabelQualifier("A") _,ast.LabelQualifier("B") _), Seq(literal("role1"), literal("role2")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo, baz $nodeKeyword A (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _, ast.NamedGraphScope(literal("baz")) _), List(ast.LabelQualifier("A") _), Seq(literal("role")))
              }

              test( s"traverseParsingErrors $command $graphKeyword $nodeKeyword $preposition") {
                assertFails(s"$command TRAVERSE $graphKeyword * $nodeKeyword * (*) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A B (*) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A (foo) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword $nodeKeyword * $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword $nodeKeyword A $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword $nodeKeyword * (*) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword $nodeKeyword A (*) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition r:ole")
                assertFails(s"$command TRAVERSE ON $graphKeyword 2foo $nodeKeyword A (*) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword * $nodeKeyword * (*)")
              }
          }

          Seq("RELATIONSHIP", "RELATIONSHIPS").foreach {
            relTypeKeyword =>

              test( s"validExpressions $command $graphKeyword $relTypeKeyword $preposition") {
                parsing(s"$command TRAVERSE ON $graphKeyword * $relTypeKeyword * $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.AllGraphsScope() _), List(ast.RelationshipAllQualifier() _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword * $relTypeKeyword * (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.AllGraphsScope() _), List(ast.RelationshipAllQualifier() _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword * $relTypeKeyword A $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.AllGraphsScope() _), List(ast.RelationshipQualifier("A") _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword * $relTypeKeyword A (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.AllGraphsScope() _), List(ast.RelationshipQualifier("A") _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword `*` $relTypeKeyword A $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("*")) _), List(ast.RelationshipQualifier("A") _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword * $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.RelationshipAllQualifier() _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword * (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.RelationshipAllQualifier() _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.RelationshipQualifier("A") _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.RelationshipQualifier("A") _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition $$role1, role2") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.RelationshipQualifier("A") _), Seq(param("role1"), literal("role2")))
                parsing(s"$command TRAVERSE ON $graphKeyword `2foo` $relTypeKeyword A (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("2foo")) _), List(ast.RelationshipQualifier("A") _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition `r:ole`") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.RelationshipQualifier("A") _), Seq(literal("r:ole")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword `A B` (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.RelationshipQualifier("A B") _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A, B (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.RelationshipQualifier("A") _, ast.RelationshipQualifier("B")_), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A, B (*) $preposition role1, role2") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.RelationshipQualifier("A") _, ast.RelationshipQualifier("B")_), Seq(literal("role1"), literal("role2")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo, baz $relTypeKeyword A (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _, ast.NamedGraphScope(literal("baz")) _), List(ast.RelationshipQualifier("A") _), Seq(literal("role")))
              }

              test( s"traverseParsingErrors$command $graphKeyword $relTypeKeyword $preposition") {
                assertFails(s"$command TRAVERSE $graphKeyword * $relTypeKeyword * (*) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A B (*) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A (foo) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword $relTypeKeyword * $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword $relTypeKeyword A $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword $relTypeKeyword * (*) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword $relTypeKeyword A (*) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition r:ole")
                assertFails(s"$command TRAVERSE ON $graphKeyword 2foo $relTypeKeyword A (*) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword * $relTypeKeyword * (*)")
              }
          }

          Seq("ELEMENT", "ELEMENTS").foreach {
            elementKeyword =>

              test( s"validExpressions $command $graphKeyword $elementKeyword $preposition") {
                parsing(s"$command TRAVERSE ON $graphKeyword * $elementKeyword * $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.AllGraphsScope() _), List(ast.ElementsAllQualifier() _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword * $elementKeyword * (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.AllGraphsScope() _), List(ast.ElementsAllQualifier() _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword * $elementKeyword A $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.AllGraphsScope() _), List(ast.ElementQualifier("A") _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword * $elementKeyword A (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.AllGraphsScope() _), List(ast.ElementQualifier("A") _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword `*` $elementKeyword A $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("*")) _), List(ast.ElementQualifier("A") _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword * $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.ElementsAllQualifier() _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword * (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.ElementsAllQualifier() _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.ElementQualifier("A") _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.ElementQualifier("A") _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition role1, role2") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.ElementQualifier("A") _), Seq(literal("role1"), literal("role2")))
                parsing(s"$command TRAVERSE ON $graphKeyword `2foo` $elementKeyword A (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("2foo")) _), List(ast.ElementQualifier("A") _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition `r:ole`") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.ElementQualifier("A") _), Seq(literal("r:ole")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword `A B` (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.ElementQualifier("A B") _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A, B (*) $preposition role") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _) , List(ast.ElementQualifier("A") _,ast.ElementQualifier("B") _), Seq(literal("role")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A, B (*) $preposition $$role1, $$role2") shouldGive
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _), List(ast.ElementQualifier("A") _,ast.ElementQualifier("B") _), Seq(param("role1"), param("role2")))
                parsing(s"$command TRAVERSE ON $graphKeyword foo, baz $elementKeyword A (*) $preposition role")
                  func(ast.GraphPrivilege(ast.TraverseAction)(pos), List(ast.NamedGraphScope(literal("foo")) _, ast.NamedGraphScope(literal("baz")) _), List(ast.ElementQualifier("A") _), Seq(param("role")))
              }

              test( s"traverseParsingErrors $command $graphKeyword $elementKeyword $preposition") {
                assertFails(s"$command TRAVERSE $graphKeyword * $elementKeyword * (*) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A B (*) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A (foo) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword $elementKeyword * $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword $elementKeyword A $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword $elementKeyword * (*) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword $elementKeyword A (*) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition r:ole")
                assertFails(s"$command TRAVERSE ON $graphKeyword 2foo $elementKeyword A (*) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword * $elementKeyword * (*)")
              }
          }
      }

      // Mix of specific graph and *

      test(s"$command TRAVERSE ON GRAPH foo, * $preposition role") {
        failsToParse
      }

      test(s"$command TRAVERSE ON GRAPH *, foo $preposition role") {
        failsToParse
      }

      // Database instead of graph keyword

      test(s"$command TRAVERSE ON DATABASES * $preposition role") {
        failsToParse
      }

      test(s"$command TRAVERSE ON DATABASE foo $preposition role") {
        failsToParse
      }

      test(s"$command TRAVERSE ON DEFAULT DATABASE $preposition role") {
        failsToParse
      }
  }
}
