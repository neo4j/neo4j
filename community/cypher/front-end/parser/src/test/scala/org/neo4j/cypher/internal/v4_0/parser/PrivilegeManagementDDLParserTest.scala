/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v4_0.parser

import org.neo4j.cypher.internal.v4_0.ast

class PrivilegeManagementDDLParserTest extends DDLParserTestBase {

  //  Showing privileges

  test("SHOW PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowAllPrivileges() _))
  }

  test("catalog show privileges") {
    yields(ast.ShowPrivileges(ast.ShowAllPrivileges() _))
  }

  test("SHOW ALL PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowAllPrivileges() _))
  }

  test("SHOW USER user PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowUserPrivileges("user") _))
  }

  test("SHOW USER `us%er` PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowUserPrivileges("us%er") _))
  }

  test("SHOW ROLE role PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowRolePrivileges("role") _))
  }

  test("SHOW ROLE `ro%le` PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowRolePrivileges("ro%le") _))
  }

  test("SHOW PRIVILEGE") {
    failsToParse
  }

  test("SHOW PRIVILAGES") {
    failsToParse
  }

  test("SHOW PRIVELAGES") {
    failsToParse
  }

  test("SHOW privalages") {
    failsToParse
  }

  test("SHOW USER PRIVILEGES") {
    failsToParse
  }

  test("SHOW ALL USER user PRIVILEGES") {
    failsToParse
  }

  test("SHOW USER us%er PRIVILEGES") {
    failsToParse
  }

  test("SHOW ROLE PRIVILEGES") {
    failsToParse
  }

  test("SHOW ALL ROLE role PRIVILEGES") {
    failsToParse
  }

  test("SHOW ROLE ro%le PRIVILEGES") {
    failsToParse
  }

  //  Granting/revoking traverse to/from role

  Seq(
    ("GRANT", "TO", grant: grantOrRevokeFunc),
    ("REVOKE", "FROM", revoke: grantOrRevokeFunc)
  ).foreach {
    case (command: String, preposition: String, func: grantOrRevokeFunc) =>

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

              test(s"$command TRAVERSE ON $graphKeyword * $nodeKeyword * $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.LabelAllQualifier() _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword * $nodeKeyword * (*) $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.LabelAllQualifier() _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword * $nodeKeyword A $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword * $nodeKeyword A (*) $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword `*` $nodeKeyword A $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("*") _, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword * $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelAllQualifier() _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword * (*) $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelAllQualifier() _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition role1, role2") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A")) _, Seq("role1", "role2")))
              }

              test(s"$command TRAVERSE ON $graphKeyword `2foo` $nodeKeyword A (*) $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("2foo") _, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition `r:ole`") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A")) _, Seq("r:ole")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword `A B` (*) $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A B")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A, B (*) $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A", "B")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A, B (*) $preposition role1, role2") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A", "B")) _, Seq("role1", "role2")))
              }

              test(s"$command TRAVERSE $graphKeyword * $nodeKeyword * (*) $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A B (*) $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A (foo) $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword $nodeKeyword * $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword $nodeKeyword A $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword $nodeKeyword * (*) $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword $nodeKeyword A (*) $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition r:ole") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword 2foo $nodeKeyword A (*) $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword foo, baz $nodeKeyword A (*) $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword * $nodeKeyword * (*)") {
                failsToParse
              }
          }

          Seq("RELATIONSHIP", "RELATIONSHIPS").foreach {
            relTypeKeyword =>

              test(s"$command TRAVERSE ON $graphKeyword * $relTypeKeyword * $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.RelationshipAllQualifier() _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword * $relTypeKeyword * (*) $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.RelationshipAllQualifier() _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword * $relTypeKeyword A $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.RelationshipsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword * $relTypeKeyword A (*) $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.RelationshipsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword `*` $relTypeKeyword A $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("*") _, ast.RelationshipsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword * $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.RelationshipAllQualifier() _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword * (*) $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.RelationshipAllQualifier() _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.RelationshipsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.RelationshipsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition role1, role2") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.RelationshipsQualifier(Seq("A")) _, Seq("role1", "role2")))
              }

              test(s"$command TRAVERSE ON $graphKeyword `2foo` $relTypeKeyword A (*) $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("2foo") _, ast.RelationshipsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition `r:ole`") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.RelationshipsQualifier(Seq("A")) _, Seq("r:ole")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword `A B` (*) $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.RelationshipsQualifier(Seq("A B")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A, B (*) $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.RelationshipsQualifier(Seq("A", "B")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A, B (*) $preposition role1, role2") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.RelationshipsQualifier(Seq("A", "B")) _, Seq("role1", "role2")))
              }

              test(s"$command TRAVERSE $graphKeyword * $relTypeKeyword * (*) $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A B (*) $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A (foo) $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword $relTypeKeyword * $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword $relTypeKeyword A $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword $relTypeKeyword * (*) $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword $relTypeKeyword A (*) $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition r:ole") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword 2foo $relTypeKeyword A (*) $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword foo, baz $relTypeKeyword A (*) $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword * $relTypeKeyword * (*)") {
                failsToParse
              }
          }

          Seq("ELEMENT", "ELEMENTS").foreach {
            elementKeyword =>

              test(s"$command TRAVERSE ON $graphKeyword * $elementKeyword * $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.AllQualifier() _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword * $elementKeyword * (*) $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.AllQualifier() _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword * $elementKeyword A $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.ElementsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword * $elementKeyword A (*) $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.AllGraphsScope() _, ast.ElementsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword `*` $elementKeyword A $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("*") _, ast.ElementsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword * $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.AllQualifier() _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword * (*) $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.AllQualifier() _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.ElementsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.ElementsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition role1, role2") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.ElementsQualifier(Seq("A")) _, Seq("role1", "role2")))
              }

              test(s"$command TRAVERSE ON $graphKeyword `2foo` $elementKeyword A (*) $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("2foo") _, ast.ElementsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition `r:ole`") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.ElementsQualifier(Seq("A")) _, Seq("r:ole")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword `A B` (*) $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.ElementsQualifier(Seq("A B")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A, B (*) $preposition role") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.ElementsQualifier(Seq("A", "B")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A, B (*) $preposition role1, role2") {
                yields(func(ast.TraversePrivilege()(pos), ast.AllResource()(pos), ast.NamedGraphScope("foo") _, ast.ElementsQualifier(Seq("A", "B")) _, Seq("role1", "role2")))
              }

              test(s"$command TRAVERSE $graphKeyword * $elementKeyword * (*) $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A B (*) $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A (foo) $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword $elementKeyword * $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword $elementKeyword A $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword $elementKeyword * (*) $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword $elementKeyword A (*) $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition r:ole") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword 2foo $elementKeyword A (*) $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword foo, baz $elementKeyword A (*) $preposition role") {
                failsToParse
              }

              test(s"$command TRAVERSE ON $graphKeyword * $elementKeyword * (*)") {
                failsToParse
              }
          }
      }
  }

  // Granting/revoking read and match to/from role

  Seq(
    (ast.ReadPrivilege()(pos), "GRANT", "TO", grant: grantOrRevokeFunc),
    (ast.ReadPrivilege()(pos), "REVOKE", "FROM", revoke: grantOrRevokeFunc),
    (ast.MatchPrivilege()(pos), "GRANT", "TO", grant: grantOrRevokeFunc),
    (ast.MatchPrivilege()(pos), "REVOKE", "FROM", revoke: grantOrRevokeFunc)
  ).foreach {
    case (privilege: ast.PrivilegeType, command: String, preposition: String, func: grantOrRevokeFunc) =>

      Seq("GRAPH", "GRAPHS").foreach {
        graphKeyword =>

          Seq("NODE", "NODES").foreach {
            nodeKeyword =>

              Seq(
                ("*", ast.AllResource()(pos), "*", ast.AllGraphsScope()(pos)),
                ("*", ast.AllResource()(pos), "foo", ast.NamedGraphScope("foo")(pos)),
                ("bar", ast.PropertiesResource(Seq("bar"))(pos), "*", ast.AllGraphsScope()(pos)),
                ("bar", ast.PropertiesResource(Seq("bar"))(pos), "foo", ast.NamedGraphScope("foo")(pos)),
                ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "*", ast.AllGraphsScope()(pos)),
                ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "foo", ast.NamedGraphScope("foo")(pos))
              ).foreach {
                case (properties: String, resource: ast.ActionResource, dbName: String, graphScope: ast.GraphScope) =>

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $nodeKeyword * $preposition role") {
                    yields(func(privilege, resource, graphScope, ast.LabelAllQualifier() _, Seq("role")))
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $nodeKeyword * (*) $preposition role") {
                    yields(func(privilege, resource, graphScope, ast.LabelAllQualifier() _, Seq("role")))
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $nodeKeyword A $preposition role") {
                    yields(func(privilege, resource, graphScope, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $nodeKeyword A (*) $preposition role") {
                    yields(func(privilege, resource, graphScope, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $nodeKeyword `A B` (*) $preposition role") {
                    yields(func(privilege, resource, graphScope, ast.LabelsQualifier(Seq("A B")) _, Seq("role")))
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $nodeKeyword A, B (*) $preposition role1, role2") {
                    yields(func(privilege, resource, graphScope, ast.LabelsQualifier(Seq("A", "B")) _, Seq("role1", "role2")))
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $nodeKeyword * $preposition `r:ole`") {
                    yields(func(privilege, resource, graphScope, ast.LabelAllQualifier() _, Seq("r:ole")))
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $nodeKeyword `:A` (*) $preposition role") {
                    yields(func(privilege, resource, graphScope, ast.LabelsQualifier(Seq(":A")) _, Seq("role")))
                  }

                  test(s"$command ${privilege.name} ($properties) $graphKeyword $dbName $nodeKeyword * (*) $preposition role") {
                    failsToParse
                  }

                  test(s"$command ${privilege.name} ($properties) $graphKeyword $dbName $nodeKeyword A $preposition role") {
                    failsToParse
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $nodeKeyword * (*)") {
                    failsToParse
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $nodeKeyword A B (*) $preposition role") {
                    failsToParse
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $nodeKeyword A (foo) $preposition role") {
                    failsToParse
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $nodeKeyword * $preposition r:ole") {
                    failsToParse
                  }
              }

              test(s"$command ${privilege.name} (*) ON $graphKeyword `f:oo` $nodeKeyword * $preposition role") {
                yields(func(privilege, ast.AllResource() _, ast.NamedGraphScope("f:oo") _, ast.LabelAllQualifier() _, Seq("role")))
              }

              test(s"$command ${privilege.name} (bar) ON $graphKeyword `f:oo` $nodeKeyword * $preposition role") {
                yields(func(privilege, ast.PropertiesResource(Seq("bar")) _, ast.NamedGraphScope("f:oo") _, ast.LabelAllQualifier() _, Seq("role")))
              }

              test(s"$command ${privilege.name} (`b:ar`) ON $graphKeyword foo $nodeKeyword * $preposition role") {
                yields(func(privilege, ast.PropertiesResource(Seq("b:ar")) _, ast.NamedGraphScope("foo") _, ast.LabelAllQualifier() _, Seq("role")))
              }

              // Invalid graph name

              test(s"$command ${privilege.name} (*) ON $graphKeyword f:oo $nodeKeyword * $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (bar) ON $graphKeyword f:oo $nodeKeyword * $preposition role") {
                failsToParse
              }

              // multiple graphs not allowed

              test(s"$command ${privilege.name} (*) ON $graphKeyword foo, baz $nodeKeyword A (*) $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (bar) ON $graphKeyword foo, baz $nodeKeyword A (*) $preposition role") {
                failsToParse
              }

              // invalid property definition

              test(s"$command ${privilege.name} (b:ar) ON $graphKeyword foo $nodeKeyword * $preposition role") {
                failsToParse
              }

              // missing graph name

              test(s"$command ${privilege.name} (*) ON $graphKeyword $nodeKeyword * $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (*) ON $graphKeyword $nodeKeyword * (*) $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (*) ON $graphKeyword $nodeKeyword A $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (*) ON $graphKeyword $nodeKeyword A (*) $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (bar) ON $graphKeyword $nodeKeyword * $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (bar) ON $graphKeyword $nodeKeyword * (*) $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (bar) ON $graphKeyword $nodeKeyword A $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (bar) ON $graphKeyword $nodeKeyword A (*) $preposition role") {
                failsToParse
              }

              // missing property definition

              test(s"$command ${privilege.name} ON $graphKeyword * $nodeKeyword * $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} ON $graphKeyword * $nodeKeyword * (*) $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} ON $graphKeyword * $nodeKeyword A $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} ON $graphKeyword * $nodeKeyword A (*) $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} ON $graphKeyword foo $nodeKeyword * $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} ON $graphKeyword foo $nodeKeyword * (*) $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} ON $graphKeyword foo $nodeKeyword A $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} ON $graphKeyword foo $nodeKeyword A (*) $preposition role") {
                failsToParse
              }
          }

          Seq("RELATIONSHIP", "RELATIONSHIPS").foreach {
            relTypeKeyword =>

              Seq(
                ("*", ast.AllResource()(pos), "*", ast.AllGraphsScope()(pos)),
                ("*", ast.AllResource()(pos), "foo", ast.NamedGraphScope("foo")(pos)),
                ("bar", ast.PropertiesResource(Seq("bar"))(pos), "*", ast.AllGraphsScope()(pos)),
                ("bar", ast.PropertiesResource(Seq("bar"))(pos), "foo", ast.NamedGraphScope("foo")(pos)),
                ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "*", ast.AllGraphsScope()(pos)),
                ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "foo", ast.NamedGraphScope("foo")(pos))
              ).foreach {
                case (properties: String, resource: ast.ActionResource, dbName: String, graphScope: ast.GraphScope) =>

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $relTypeKeyword * $preposition role") {
                    yields(func(privilege, resource, graphScope, ast.RelationshipAllQualifier() _, Seq("role")))
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $relTypeKeyword * (*) $preposition role") {
                    yields(func(privilege, resource, graphScope, ast.RelationshipAllQualifier() _, Seq("role")))
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $relTypeKeyword A $preposition role") {
                    yields(func(privilege, resource, graphScope, ast.RelationshipsQualifier(Seq("A")) _, Seq("role")))
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $relTypeKeyword A (*) $preposition role") {
                    yields(func(privilege, resource, graphScope, ast.RelationshipsQualifier(Seq("A")) _, Seq("role")))
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $relTypeKeyword `A B` (*) $preposition role") {
                    yields(func(privilege, resource, graphScope, ast.RelationshipsQualifier(Seq("A B")) _, Seq("role")))
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $relTypeKeyword A, B (*) $preposition role1, role2") {
                    yields(func(privilege, resource, graphScope, ast.RelationshipsQualifier(Seq("A", "B")) _, Seq("role1", "role2")))
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $relTypeKeyword * $preposition `r:ole`") {
                    yields(func(privilege, resource, graphScope, ast.RelationshipAllQualifier() _, Seq("r:ole")))
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $relTypeKeyword `:A` (*) $preposition role") {
                    yields(func(privilege, resource, graphScope, ast.RelationshipsQualifier(Seq(":A")) _, Seq("role")))
                  }

                  test(s"$command ${privilege.name} ($properties) $graphKeyword $dbName $relTypeKeyword * (*) $preposition role") {
                    failsToParse
                  }

                  test(s"$command ${privilege.name} ($properties) $graphKeyword $dbName $relTypeKeyword A $preposition role") {
                    failsToParse
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $relTypeKeyword * (*)") {
                    failsToParse
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $relTypeKeyword A B (*) $preposition role") {
                    failsToParse
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $relTypeKeyword A (foo) $preposition role") {
                    failsToParse
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $relTypeKeyword * $preposition r:ole") {
                    failsToParse
                  }
              }

              test(s"$command ${privilege.name} (*) ON $graphKeyword `f:oo` $relTypeKeyword * $preposition role") {
                yields(func(privilege, ast.AllResource() _, ast.NamedGraphScope("f:oo") _, ast.RelationshipAllQualifier() _, Seq("role")))
              }

              test(s"$command ${privilege.name} (bar) ON $graphKeyword `f:oo` $relTypeKeyword * $preposition role") {
                yields(func(privilege, ast.PropertiesResource(Seq("bar")) _, ast.NamedGraphScope("f:oo") _, ast.RelationshipAllQualifier() _, Seq("role")))
              }

              test(s"$command ${privilege.name} (`b:ar`) ON $graphKeyword foo $relTypeKeyword * $preposition role") {
                yields(func(privilege, ast.PropertiesResource(Seq("b:ar")) _, ast.NamedGraphScope("foo") _, ast.RelationshipAllQualifier() _, Seq("role")))
              }

              // Invalid graph name

              test(s"$command ${privilege.name} (*) ON $graphKeyword f:oo $relTypeKeyword * $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (bar) ON $graphKeyword f:oo $relTypeKeyword * $preposition role") {
                failsToParse
              }

              // multiple graphs not allowed

              test(s"$command ${privilege.name} (*) ON $graphKeyword foo, baz $relTypeKeyword A (*) $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (bar) ON $graphKeyword foo, baz $relTypeKeyword A (*) $preposition role") {
                failsToParse
              }

              // invalid property definition

              test(s"$command ${privilege.name} (b:ar) ON $graphKeyword foo $relTypeKeyword * $preposition role") {
                failsToParse
              }

              // missing graph name

              test(s"$command ${privilege.name} (*) ON $graphKeyword $relTypeKeyword * $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (*) ON $graphKeyword $relTypeKeyword * (*) $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (*) ON $graphKeyword $relTypeKeyword A $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (*) ON $graphKeyword $relTypeKeyword A (*) $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (bar) ON $graphKeyword $relTypeKeyword * $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (bar) ON $graphKeyword $relTypeKeyword * (*) $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (bar) ON $graphKeyword $relTypeKeyword A $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (bar) ON $graphKeyword $relTypeKeyword A (*) $preposition role") {
                failsToParse
              }

              // missing property definition

              test(s"$command ${privilege.name} ON $graphKeyword * $relTypeKeyword * $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} ON $graphKeyword * $relTypeKeyword * (*) $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} ON $graphKeyword * $relTypeKeyword A $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} ON $graphKeyword * $relTypeKeyword A (*) $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} ON $graphKeyword foo $relTypeKeyword * $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} ON $graphKeyword foo $relTypeKeyword * (*) $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} ON $graphKeyword foo $relTypeKeyword A $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} ON $graphKeyword foo $relTypeKeyword A (*) $preposition role") {
                failsToParse
              }
          }

          Seq("ELEMENT", "ELEMENTS").foreach {
            elementKeyword =>

              Seq(
                ("*", ast.AllResource()(pos), "*", ast.AllGraphsScope()(pos)),
                ("*", ast.AllResource()(pos), "foo", ast.NamedGraphScope("foo")(pos)),
                ("bar", ast.PropertiesResource(Seq("bar"))(pos), "*", ast.AllGraphsScope()(pos)),
                ("bar", ast.PropertiesResource(Seq("bar"))(pos), "foo", ast.NamedGraphScope("foo")(pos)),
                ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "*", ast.AllGraphsScope()(pos)),
                ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "foo", ast.NamedGraphScope("foo")(pos))
              ).foreach {
                case (properties: String, resource: ast.ActionResource, dbName: String, graphScope: ast.GraphScope) =>

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $elementKeyword * $preposition role") {
                    yields(func(privilege, resource, graphScope, ast.AllQualifier() _, Seq("role")))
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $elementKeyword * (*) $preposition role") {
                    yields(func(privilege, resource, graphScope, ast.AllQualifier() _, Seq("role")))
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $elementKeyword A $preposition role") {
                    yields(func(privilege, resource, graphScope, ast.ElementsQualifier(Seq("A")) _, Seq("role")))
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $elementKeyword A (*) $preposition role") {
                    yields(func(privilege, resource, graphScope, ast.ElementsQualifier(Seq("A")) _, Seq("role")))
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $elementKeyword `A B` (*) $preposition role") {
                    yields(func(privilege, resource, graphScope, ast.ElementsQualifier(Seq("A B")) _, Seq("role")))
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $elementKeyword A, B (*) $preposition role1, role2") {
                    yields(func(privilege, resource, graphScope, ast.ElementsQualifier(Seq("A", "B")) _, Seq("role1", "role2")))
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $elementKeyword * $preposition `r:ole`") {
                    yields(func(privilege, resource, graphScope, ast.AllQualifier() _, Seq("r:ole")))
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $elementKeyword `:A` (*) $preposition role") {
                    yields(func(privilege, resource, graphScope, ast.ElementsQualifier(Seq(":A")) _, Seq("role")))
                  }

                  test(s"$command ${privilege.name} ($properties) $graphKeyword $dbName $elementKeyword * (*) $preposition role") {
                    failsToParse
                  }

                  test(s"$command ${privilege.name} ($properties) $graphKeyword $dbName $elementKeyword A $preposition role") {
                    failsToParse
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $elementKeyword * (*)") {
                    failsToParse
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $elementKeyword A B (*) $preposition role") {
                    failsToParse
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $elementKeyword A (foo) $preposition role") {
                    failsToParse
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $elementKeyword * $preposition r:ole") {
                    failsToParse
                  }
              }

              test(s"$command ${privilege.name} (*) ON $graphKeyword `f:oo` $elementKeyword * $preposition role") {
                yields(func(privilege, ast.AllResource() _, ast.NamedGraphScope("f:oo") _, ast.AllQualifier() _, Seq("role")))
              }

              test(s"$command ${privilege.name} (bar) ON $graphKeyword `f:oo` $elementKeyword * $preposition role") {
                yields(func(privilege, ast.PropertiesResource(Seq("bar")) _, ast.NamedGraphScope("f:oo") _, ast.AllQualifier() _, Seq("role")))
              }

              test(s"$command ${privilege.name} (`b:ar`) ON $graphKeyword foo $elementKeyword * $preposition role") {
                yields(func(privilege, ast.PropertiesResource(Seq("b:ar")) _, ast.NamedGraphScope("foo") _, ast.AllQualifier() _, Seq("role")))
              }

              // Invalid graph name

              test(s"$command ${privilege.name} (*) ON $graphKeyword f:oo $elementKeyword * $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (bar) ON $graphKeyword f:oo $elementKeyword * $preposition role") {
                failsToParse
              }

              // multiple graphs not allowed

              test(s"$command ${privilege.name} (*) ON $graphKeyword foo, baz $elementKeyword A (*) $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (bar) ON $graphKeyword foo, baz $elementKeyword A (*) $preposition role") {
                failsToParse
              }

              // invalid property definition

              test(s"$command ${privilege.name} (b:ar) ON $graphKeyword foo $elementKeyword * $preposition role") {
                failsToParse
              }

              // missing graph name

              test(s"$command ${privilege.name} (*) ON $graphKeyword $elementKeyword * $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (*) ON $graphKeyword $elementKeyword * (*) $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (*) ON $graphKeyword $elementKeyword A $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (*) ON $graphKeyword $elementKeyword A (*) $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (bar) ON $graphKeyword $elementKeyword * $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (bar) ON $graphKeyword $elementKeyword * (*) $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (bar) ON $graphKeyword $elementKeyword A $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} (bar) ON $graphKeyword $elementKeyword A (*) $preposition role") {
                failsToParse
              }

              // missing property definition

              test(s"$command ${privilege.name} ON $graphKeyword * $elementKeyword * $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} ON $graphKeyword * $elementKeyword * (*) $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} ON $graphKeyword * $elementKeyword A $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} ON $graphKeyword * $elementKeyword A (*) $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} ON $graphKeyword foo $elementKeyword * $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} ON $graphKeyword foo $elementKeyword * (*) $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} ON $graphKeyword foo $elementKeyword A $preposition role") {
                failsToParse
              }

              test(s"$command ${privilege.name} ON $graphKeyword foo $elementKeyword A (*) $preposition role") {
                failsToParse
              }
          }

          // Needs to be separate loop to avoid duplicate tests since the test does not have any $nodeKeyword
          Seq(
            ("*", ast.AllResource()(pos), "*", ast.AllGraphsScope()(pos)),
            ("*", ast.AllResource()(pos), "foo", ast.NamedGraphScope("foo")(pos)),
            ("bar", ast.PropertiesResource(Seq("bar"))(pos), "*", ast.AllGraphsScope()(pos)),
            ("bar", ast.PropertiesResource(Seq("bar"))(pos), "foo", ast.NamedGraphScope("foo")(pos)),
            ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "*", ast.AllGraphsScope()(pos)),
            ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "foo", ast.NamedGraphScope("foo")(pos))
          ).foreach {
            case (properties: String, resource: ast.ActionResource, dbName: String, graphScope: ast.GraphScope) =>

              test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $preposition role") {
                yields(func(privilege, resource, graphScope, ast.AllQualifier() _, Seq("role")))
              }
          }
      }
  }

}
