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
package org.neo4j.cypher.internal.v4_0.parser.privilege

import org.neo4j.cypher.internal.v4_0.ast
import org.neo4j.cypher.internal.v4_0.parser.AdministrationCommandParserTestBase

class RolePrivilegeAdministrationCommandParserTest extends AdministrationCommandParserTestBase {

  Seq(
    ("GRANT", "TO", grant: privilegeFunc),
    ("DENY", "TO", deny: privilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrant: privilegeFunc),
    ("REVOKE DENY", "FROM", revokeDeny: privilegeFunc),
    ("REVOKE", "FROM", revokeBoth: privilegeFunc)
  ).foreach {
    case (command: String, preposition: String, func: privilegeFunc) =>

      Seq("GRAPH", "GRAPHS").foreach {
        graphKeyword =>
          test(s"$command TRAVERSE ON $graphKeyword * $preposition role") {
            yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.AllQualifier() _, Seq("role")))
          }

          test(s"$command TRAVERSE ON $graphKeyword foo $preposition role") {
            yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.AllQualifier() _, Seq("role")))
          }

          Seq("NODE", "NODES").foreach {
            nodeKeyword =>

              test( s"validExpressions $command $graphKeyword $nodeKeyword $preposition") {
                parsing(s"$command TRAVERSE ON $graphKeyword * $nodeKeyword * $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.LabelAllQualifier() _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword * $nodeKeyword * (*) $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.LabelAllQualifier() _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword * $nodeKeyword A $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.LabelsQualifier(Seq("A")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword * $nodeKeyword A (*) $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.LabelsQualifier(Seq("A")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword `*` $nodeKeyword A $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("*") _, ast.LabelsQualifier(Seq("A")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword * $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelAllQualifier() _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword * (*) $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelAllQualifier() _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition role1, role2") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A")) _, Seq("role1", "role2"))
                parsing(s"$command TRAVERSE ON $graphKeyword `2foo` $nodeKeyword A (*) $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("2foo") _, ast.LabelsQualifier(Seq("A")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition `r:ole`") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A")) _, Seq("r:ole"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword `A B` (*) $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A B")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A, B (*) $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A", "B")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A, B (*) $preposition role1, role2") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A", "B")) _, Seq("role1", "role2"))
              }

              test( s"traverseParsingErrors$command $graphKeyword $nodeKeyword $preposition") {
                assertFails(s"$command TRAVERSE $graphKeyword * $nodeKeyword * (*) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A B (*) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A (foo) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword $nodeKeyword * $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword $nodeKeyword A $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword $nodeKeyword * (*) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword $nodeKeyword A (*) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition r:ole")
                assertFails(s"$command TRAVERSE ON $graphKeyword 2foo $nodeKeyword A (*) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword foo, baz $nodeKeyword A (*) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword * $nodeKeyword * (*)")
              }
          }

          Seq("RELATIONSHIP", "RELATIONSHIPS").foreach {
            relTypeKeyword =>

              test( s"validExpressions $command $graphKeyword $relTypeKeyword $preposition") {
                parsing(s"$command TRAVERSE ON $graphKeyword * $relTypeKeyword * $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.RelationshipAllQualifier() _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword * $relTypeKeyword * (*) $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.RelationshipAllQualifier() _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword * $relTypeKeyword A $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.RelationshipsQualifier(Seq("A")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword * $relTypeKeyword A (*) $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.RelationshipsQualifier(Seq("A")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword `*` $relTypeKeyword A $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("*") _, ast.RelationshipsQualifier(Seq("A")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword * $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.RelationshipAllQualifier() _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword * (*) $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.RelationshipAllQualifier() _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.RelationshipsQualifier(Seq("A")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.RelationshipsQualifier(Seq("A")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition role1, role2") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.RelationshipsQualifier(Seq("A")) _, Seq("role1", "role2"))
                parsing(s"$command TRAVERSE ON $graphKeyword `2foo` $relTypeKeyword A (*) $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("2foo") _, ast.RelationshipsQualifier(Seq("A")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition `r:ole`") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.RelationshipsQualifier(Seq("A")) _, Seq("r:ole"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword `A B` (*) $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.RelationshipsQualifier(Seq("A B")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A, B (*) $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.RelationshipsQualifier(Seq("A", "B")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A, B (*) $preposition role1, role2") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.RelationshipsQualifier(Seq("A", "B")) _, Seq("role1", "role2"))
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
                assertFails(s"$command TRAVERSE ON $graphKeyword foo, baz $relTypeKeyword A (*) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword * $relTypeKeyword * (*)")
              }
          }

          Seq("ELEMENT", "ELEMENTS").foreach {
            elementKeyword =>

              test( s"validExpressions $command $graphKeyword $elementKeyword $preposition") {
                parsing(s"$command TRAVERSE ON $graphKeyword * $elementKeyword * $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.AllQualifier() _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword * $elementKeyword * (*) $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.AllQualifier() _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword * $elementKeyword A $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.ElementsQualifier(Seq("A")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword * $elementKeyword A (*) $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.ElementsQualifier(Seq("A")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword `*` $elementKeyword A $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("*") _, ast.ElementsQualifier(Seq("A")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword * $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.AllQualifier() _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword * (*) $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.AllQualifier() _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.ElementsQualifier(Seq("A")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.ElementsQualifier(Seq("A")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition role1, role2") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.ElementsQualifier(Seq("A")) _, Seq("role1", "role2"))
                parsing(s"$command TRAVERSE ON $graphKeyword `2foo` $elementKeyword A (*) $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("2foo") _, ast.ElementsQualifier(Seq("A")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition `r:ole`") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.ElementsQualifier(Seq("A")) _, Seq("r:ole"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword `A B` (*) $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.ElementsQualifier(Seq("A B")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A, B (*) $preposition role") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.ElementsQualifier(Seq("A", "B")) _, Seq("role"))
                parsing(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A, B (*) $preposition role1, role2") shouldGive
                  func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.ElementsQualifier(Seq("A", "B")) _, Seq("role1", "role2"))
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
                assertFails(s"$command TRAVERSE ON $graphKeyword foo, baz $elementKeyword A (*) $preposition role")
                assertFails(s"$command TRAVERSE ON $graphKeyword * $elementKeyword * (*)")
              }
          }
      }
  }
}
