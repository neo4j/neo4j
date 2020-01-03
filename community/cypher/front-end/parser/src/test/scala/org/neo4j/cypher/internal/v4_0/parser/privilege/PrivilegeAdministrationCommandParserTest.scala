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

class PrivilegeAdministrationCommandParserTest extends AdministrationCommandParserTestBase {

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

  test("SHOW USER user1, user2 PRIVILEGES") {
    failsToParse
  }

  test("SHOW USERS user1, user2 PRIVILEGES") {
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

  test("SHOW ROLE role1, role2 PRIVILEGES") {
    failsToParse
  }

  test("SHOW ROLES role1, role2 PRIVILEGES") {
    failsToParse
  }

  test("SHOW ALL ROLE role PRIVILEGES") {
    failsToParse
  }

  test("SHOW ROLE ro%le PRIVILEGES") {
    failsToParse
  }

  // Granting/denying/revoking read and match to/from role
  Seq(
    (ast.ReadPrivilege()(pos), "GRANT", "TO", grant: privilegeFunc),
    (ast.ReadPrivilege()(pos), "DENY", "TO", deny: privilegeFunc),
    (ast.ReadPrivilege()(pos), "REVOKE GRANT", "FROM", revokeGrant: privilegeFunc),
    (ast.ReadPrivilege()(pos), "REVOKE DENY", "FROM", revokeDeny: privilegeFunc),
    (ast.ReadPrivilege()(pos), "REVOKE", "FROM", revokeBoth: privilegeFunc),
    (ast.MatchPrivilege()(pos), "GRANT", "TO", grant: privilegeFunc),
    (ast.MatchPrivilege()(pos), "DENY", "TO", deny: privilegeFunc),
    (ast.MatchPrivilege()(pos), "REVOKE GRANT", "FROM", revokeGrant: privilegeFunc),
    (ast.MatchPrivilege()(pos), "REVOKE DENY", "FROM", revokeDeny: privilegeFunc),
    (ast.MatchPrivilege()(pos), "REVOKE", "FROM", revokeBoth: privilegeFunc)
  ).foreach {
    case (privilege: ast.PrivilegeType, command: String, preposition: String, func: privilegeFunc) =>

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

                  test( s"validExpressions $command ${privilege.name} {$properties} $graphKeyword $dbName $nodeKeyword $preposition") {
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $nodeKeyword * $preposition role") shouldGive
                      func(privilege, resource, graphScope, ast.LabelAllQualifier() _, Seq("role"))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $nodeKeyword * (*) $preposition role") shouldGive
                      func(privilege, resource, graphScope, ast.LabelAllQualifier() _, Seq("role"))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $nodeKeyword A $preposition role") shouldGive
                      func(privilege, resource, graphScope, ast.LabelsQualifier(Seq("A")) _, Seq("role"))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $nodeKeyword A (*) $preposition role") shouldGive
                      func(privilege, resource, graphScope, ast.LabelsQualifier(Seq("A")) _, Seq("role"))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $nodeKeyword `A B` (*) $preposition role") shouldGive
                      func(privilege, resource, graphScope, ast.LabelsQualifier(Seq("A B")) _, Seq("role"))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $nodeKeyword A, B (*) $preposition role1, role2") shouldGive
                      func(privilege, resource, graphScope, ast.LabelsQualifier(Seq("A", "B")) _, Seq("role1", "role2"))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $nodeKeyword * $preposition `r:ole`") shouldGive
                      func(privilege, resource, graphScope, ast.LabelAllQualifier() _, Seq("r:ole"))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $nodeKeyword `:A` (*) $preposition role") shouldGive
                      func(privilege, resource, graphScope, ast.LabelsQualifier(Seq(":A")) _, Seq("role"))
                  }

                  test( s"failToParse $command ${privilege.name} {$properties} $graphKeyword $dbName $nodeKeyword $preposition") {
                    assertFails(s"$command ${privilege.name} {$properties} $graphKeyword $dbName $nodeKeyword * (*) $preposition role")
                    assertFails(s"$command ${privilege.name} {$properties} $graphKeyword $dbName $nodeKeyword A $preposition role")
                    assertFails(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $nodeKeyword * (*)")
                    assertFails(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $nodeKeyword A B (*) $preposition role")
                    assertFails(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $nodeKeyword A (foo) $preposition role")
                    assertFails(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $nodeKeyword * $preposition r:ole")
                  }
              }

              test( s"validExpressions $command ${privilege.name} $graphKeyword $nodeKeyword $preposition") {
                parsing(s"$command ${privilege.name} {*} ON $graphKeyword `f:oo` $nodeKeyword * $preposition role") shouldGive
                  func(privilege, ast.AllResource() _, ast.NamedGraphScope("f:oo") _, ast.LabelAllQualifier() _, Seq("role"))
                parsing(s"$command ${privilege.name} {bar} ON $graphKeyword `f:oo` $nodeKeyword * $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("bar")) _, ast.NamedGraphScope("f:oo") _, ast.LabelAllQualifier() _, Seq("role"))
                parsing(s"$command ${privilege.name} {`b:ar`} ON $graphKeyword foo $nodeKeyword * $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("b:ar")) _, ast.NamedGraphScope("foo") _, ast.LabelAllQualifier() _, Seq("role"))
              }

              test( s"parsingFailures $command ${privilege.name} $graphKeyword $nodeKeyword $preposition") {
                // Invalid graph name
                assertFails(s"$command ${privilege.name} {*} ON $graphKeyword f:oo $nodeKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} {bar} ON $graphKeyword f:oo $nodeKeyword * $preposition role")
                // multiple graphs not allowed
                assertFails(s"$command ${privilege.name} {*} ON $graphKeyword foo, baz $nodeKeyword A (*) $preposition role")
                assertFails(s"$command ${privilege.name} {bar} ON $graphKeyword foo, baz $nodeKeyword A (*) $preposition role")
                // invalid property definition
                assertFails(s"$command ${privilege.name} {b:ar} ON $graphKeyword foo $nodeKeyword * $preposition role")
                // missing graph name
                assertFails(s"$command ${privilege.name} {*} ON $graphKeyword $nodeKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} {*} ON $graphKeyword $nodeKeyword * (*) $preposition role")
                assertFails(s"$command ${privilege.name} {*} ON $graphKeyword $nodeKeyword A $preposition role")
                assertFails(s"$command ${privilege.name} {*} ON $graphKeyword $nodeKeyword A (*) $preposition role")
                assertFails(s"$command ${privilege.name} {bar} ON $graphKeyword $nodeKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} {bar} ON $graphKeyword $nodeKeyword * (*) $preposition role")
                assertFails(s"$command ${privilege.name} {bar} ON $graphKeyword $nodeKeyword A $preposition role")
                assertFails(s"$command ${privilege.name} {bar} ON $graphKeyword $nodeKeyword A (*) $preposition role")
                // missing property definition
                assertFails(s"$command ${privilege.name} ON $graphKeyword * $nodeKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} ON $graphKeyword * $nodeKeyword * (*) $preposition role")
                assertFails(s"$command ${privilege.name} ON $graphKeyword * $nodeKeyword A $preposition role")
                assertFails(s"$command ${privilege.name} ON $graphKeyword * $nodeKeyword A (*) $preposition role")
                assertFails(s"$command ${privilege.name} ON $graphKeyword foo $nodeKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} ON $graphKeyword foo $nodeKeyword * (*) $preposition role")
                assertFails(s"$command ${privilege.name} ON $graphKeyword foo $nodeKeyword A $preposition role")
                assertFails(s"$command ${privilege.name} ON $graphKeyword foo $nodeKeyword A (*) $preposition role")
                // missing property list
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword * $nodeKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword * $nodeKeyword * (*) $preposition role")
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword * $nodeKeyword A $preposition role")
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword * $nodeKeyword A (*) $preposition role")
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword foo $nodeKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword foo $nodeKeyword * (*) $preposition role")
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword foo $nodeKeyword A $preposition role")
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword foo $nodeKeyword A (*) $preposition role")
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

                  test( s"validExpressions $command ${privilege.name} {$properties} $graphKeyword $dbName $relTypeKeyword $preposition") {
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $relTypeKeyword * $preposition role") shouldGive
                      func(privilege, resource, graphScope, ast.RelationshipAllQualifier() _, Seq("role"))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $relTypeKeyword * (*) $preposition role") shouldGive
                      func(privilege, resource, graphScope, ast.RelationshipAllQualifier() _, Seq("role"))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $relTypeKeyword A $preposition role") shouldGive
                      func(privilege, resource, graphScope, ast.RelationshipsQualifier(Seq("A")) _, Seq("role"))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $relTypeKeyword A (*) $preposition role") shouldGive
                      func(privilege, resource, graphScope, ast.RelationshipsQualifier(Seq("A")) _, Seq("role"))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $relTypeKeyword `A B` (*) $preposition role") shouldGive
                      func(privilege, resource, graphScope, ast.RelationshipsQualifier(Seq("A B")) _, Seq("role"))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $relTypeKeyword A, B (*) $preposition role1, role2") shouldGive
                      func(privilege, resource, graphScope, ast.RelationshipsQualifier(Seq("A", "B")) _, Seq("role1", "role2"))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $relTypeKeyword * $preposition `r:ole`") shouldGive
                      func(privilege, resource, graphScope, ast.RelationshipAllQualifier() _, Seq("r:ole"))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $relTypeKeyword `:A` (*) $preposition role") shouldGive
                      func(privilege, resource, graphScope, ast.RelationshipsQualifier(Seq(":A")) _, Seq("role"))
                  }

                  test( s"parsingFailures $command ${privilege.name} {$properties} $graphKeyword $dbName $relTypeKeyword $preposition") {
                    assertFails(s"$command ${privilege.name} {$properties} $graphKeyword $dbName $relTypeKeyword * (*) $preposition role")
                    assertFails(s"$command ${privilege.name} {$properties} $graphKeyword $dbName $relTypeKeyword A $preposition role")
                    assertFails(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $relTypeKeyword * (*)")
                    assertFails(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $relTypeKeyword A B (*) $preposition role")
                    assertFails(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $relTypeKeyword A (foo) $preposition role")
                    assertFails(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $relTypeKeyword * $preposition r:ole")
                  }
              }

              test( s"validExpressions $command ${privilege.name} $graphKeyword $relTypeKeyword $preposition") {
                parsing(s"$command ${privilege.name} {*} ON $graphKeyword `f:oo` $relTypeKeyword * $preposition role") shouldGive
                  func(privilege, ast.AllResource() _, ast.NamedGraphScope("f:oo") _, ast.RelationshipAllQualifier() _, Seq("role"))
                parsing(s"$command ${privilege.name} {bar} ON $graphKeyword `f:oo` $relTypeKeyword * $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("bar")) _, ast.NamedGraphScope("f:oo") _, ast.RelationshipAllQualifier() _, Seq("role"))
                parsing(s"$command ${privilege.name} {`b:ar`} ON $graphKeyword foo $relTypeKeyword * $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("b:ar")) _, ast.NamedGraphScope("foo") _, ast.RelationshipAllQualifier() _, Seq("role"))
              }

              test( s"parsingFailures$command ${privilege.name} $graphKeyword $relTypeKeyword $preposition") {
                // Invalid graph name
                assertFails(s"$command ${privilege.name} {*} ON $graphKeyword f:oo $relTypeKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} {bar} ON $graphKeyword f:oo $relTypeKeyword * $preposition role")
                // multiple graphs not allowed
                assertFails(s"$command ${privilege.name} {*} ON $graphKeyword foo, baz $relTypeKeyword A (*) $preposition role")
                assertFails(s"$command ${privilege.name} {bar} ON $graphKeyword foo, baz $relTypeKeyword A (*) $preposition role")
                // invalid property definition
                assertFails(s"$command ${privilege.name} {b:ar} ON $graphKeyword foo $relTypeKeyword * $preposition role")
                // missing graph name
                assertFails(s"$command ${privilege.name} {*} ON $graphKeyword $relTypeKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} {*} ON $graphKeyword $relTypeKeyword * (*) $preposition role")
                assertFails(s"$command ${privilege.name} {*} ON $graphKeyword $relTypeKeyword A $preposition role")
                assertFails(s"$command ${privilege.name} {*} ON $graphKeyword $relTypeKeyword A (*) $preposition role")
                assertFails(s"$command ${privilege.name} {bar} ON $graphKeyword $relTypeKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} {bar} ON $graphKeyword $relTypeKeyword * (*) $preposition role")
                assertFails(s"$command ${privilege.name} {bar} ON $graphKeyword $relTypeKeyword A $preposition role")
                assertFails(s"$command ${privilege.name} {bar} ON $graphKeyword $relTypeKeyword A (*) $preposition role")
                // missing property definition
                assertFails(s"$command ${privilege.name} ON $graphKeyword * $relTypeKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} ON $graphKeyword * $relTypeKeyword * (*) $preposition role")
                assertFails(s"$command ${privilege.name} ON $graphKeyword * $relTypeKeyword A $preposition role")
                assertFails(s"$command ${privilege.name} ON $graphKeyword * $relTypeKeyword A (*) $preposition role")
                assertFails(s"$command ${privilege.name} ON $graphKeyword foo $relTypeKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} ON $graphKeyword foo $relTypeKeyword * (*) $preposition role")
                assertFails(s"$command ${privilege.name} ON $graphKeyword foo $relTypeKeyword A $preposition role")
                assertFails(s"$command ${privilege.name} ON $graphKeyword foo $relTypeKeyword A (*) $preposition role")
                // missing property list
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword * $relTypeKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword * $relTypeKeyword * (*) $preposition role")
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword * $relTypeKeyword A $preposition role")
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword * $relTypeKeyword A (*) $preposition role")
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword foo $relTypeKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword foo $relTypeKeyword * (*) $preposition role")
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword foo $relTypeKeyword A $preposition role")
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword foo $relTypeKeyword A (*) $preposition role")
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

                  test( s"validExpressions $command ${privilege.name} {$properties} $graphKeyword $dbName $elementKeyword $preposition") {
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $elementKeyword * $preposition role") shouldGive
                      func(privilege, resource, graphScope, ast.AllQualifier() _, Seq("role"))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $elementKeyword * (*) $preposition role") shouldGive
                      func(privilege, resource, graphScope, ast.AllQualifier() _, Seq("role"))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $elementKeyword A $preposition role") shouldGive
                      func(privilege, resource, graphScope, ast.ElementsQualifier(Seq("A")) _, Seq("role"))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $elementKeyword A (*) $preposition role") shouldGive
                      func(privilege, resource, graphScope, ast.ElementsQualifier(Seq("A")) _, Seq("role"))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $elementKeyword `A B` (*) $preposition role") shouldGive
                      func(privilege, resource, graphScope, ast.ElementsQualifier(Seq("A B")) _, Seq("role"))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $elementKeyword A, B (*) $preposition role1, role2") shouldGive
                      func(privilege, resource, graphScope, ast.ElementsQualifier(Seq("A", "B")) _, Seq("role1", "role2"))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $elementKeyword * $preposition `r:ole`") shouldGive
                      func(privilege, resource, graphScope, ast.AllQualifier() _, Seq("r:ole"))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $elementKeyword `:A` (*) $preposition role") shouldGive
                      func(privilege, resource, graphScope, ast.ElementsQualifier(Seq(":A")) _, Seq("role"))
                  }

                  test( s"parsingFailures$command ${privilege.name} {$properties} $graphKeyword $dbName $elementKeyword $preposition") {
                    assertFails(s"$command ${privilege.name} {$properties} $graphKeyword $dbName $elementKeyword * (*) $preposition role")
                    assertFails(s"$command ${privilege.name} {$properties} $graphKeyword $dbName $elementKeyword A $preposition role")
                    assertFails(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $elementKeyword * (*)")
                    assertFails(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $elementKeyword A B (*) $preposition role")
                    assertFails(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $elementKeyword A (foo) $preposition role")
                    assertFails(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $elementKeyword * $preposition r:ole")
                  }
              }

              test( s"validExpressions $command ${privilege.name} $graphKeyword $elementKeyword $preposition") {
                parsing(s"$command ${privilege.name} {*} ON $graphKeyword `f:oo` $elementKeyword * $preposition role") shouldGive
                  func(privilege, ast.AllResource() _, ast.NamedGraphScope("f:oo") _, ast.AllQualifier() _, Seq("role"))
                parsing(s"$command ${privilege.name} {bar} ON $graphKeyword `f:oo` $elementKeyword * $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("bar")) _, ast.NamedGraphScope("f:oo") _, ast.AllQualifier() _, Seq("role"))
                parsing(s"$command ${privilege.name} {`b:ar`} ON $graphKeyword foo $elementKeyword * $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("b:ar")) _, ast.NamedGraphScope("foo") _, ast.AllQualifier() _, Seq("role"))
              }

              test( s"parsingFailures $command ${privilege.name} $graphKeyword $elementKeyword $preposition") {
                // Invalid graph name
                assertFails(s"$command ${privilege.name} {*} ON $graphKeyword f:oo $elementKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} {bar} ON $graphKeyword f:oo $elementKeyword * $preposition role")
                // multiple graphs not allowed
                assertFails(s"$command ${privilege.name} {*} ON $graphKeyword foo, baz $elementKeyword A (*) $preposition role")
                assertFails(s"$command ${privilege.name} {bar} ON $graphKeyword foo, baz $elementKeyword A (*) $preposition role")
                // invalid property definition
                assertFails(s"$command ${privilege.name} {b:ar} ON $graphKeyword foo $elementKeyword * $preposition role")
                // missing graph name
                assertFails(s"$command ${privilege.name} {*} ON $graphKeyword $elementKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} {*} ON $graphKeyword $elementKeyword * (*) $preposition role")
                assertFails(s"$command ${privilege.name} {*} ON $graphKeyword $elementKeyword A $preposition role")
                assertFails(s"$command ${privilege.name} {*} ON $graphKeyword $elementKeyword A (*) $preposition role")
                assertFails(s"$command ${privilege.name} {bar} ON $graphKeyword $elementKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} {bar} ON $graphKeyword $elementKeyword * (*) $preposition role")
                assertFails(s"$command ${privilege.name} {bar} ON $graphKeyword $elementKeyword A $preposition role")
                assertFails(s"$command ${privilege.name} {bar} ON $graphKeyword $elementKeyword A (*) $preposition role")
                // missing property definition
                assertFails(s"$command ${privilege.name} ON $graphKeyword * $elementKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} ON $graphKeyword * $elementKeyword * (*) $preposition role")
                assertFails(s"$command ${privilege.name} ON $graphKeyword * $elementKeyword A $preposition role")
                assertFails(s"$command ${privilege.name} ON $graphKeyword * $elementKeyword A (*) $preposition role")
                assertFails(s"$command ${privilege.name} ON $graphKeyword foo $elementKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} ON $graphKeyword foo $elementKeyword * (*) $preposition role")
                assertFails(s"$command ${privilege.name} ON $graphKeyword foo $elementKeyword A $preposition role")
                assertFails(s"$command ${privilege.name} ON $graphKeyword foo $elementKeyword A (*) $preposition role")
                // missing property list
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword * $elementKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword * $elementKeyword * (*) $preposition role")
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword * $elementKeyword A $preposition role")
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword * $elementKeyword A (*) $preposition role")
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword foo $elementKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword foo $elementKeyword * (*) $preposition role")
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword foo $elementKeyword A $preposition role")
                assertFails(s"$command ${privilege.name} {} ON $graphKeyword foo $elementKeyword A (*) $preposition role")
              }
          }

          // Needs to be separate loop to avoid duplicate tests since the test does not have any segment keyword
          Seq(
            ("*", ast.AllResource()(pos), "*", ast.AllGraphsScope()(pos)),
            ("*", ast.AllResource()(pos), "foo", ast.NamedGraphScope("foo")(pos)),
            ("bar", ast.PropertiesResource(Seq("bar"))(pos), "*", ast.AllGraphsScope()(pos)),
            ("bar", ast.PropertiesResource(Seq("bar"))(pos), "foo", ast.NamedGraphScope("foo")(pos)),
            ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "*", ast.AllGraphsScope()(pos)),
            ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "foo", ast.NamedGraphScope("foo")(pos))
          ).foreach {
            case (properties: String, resource: ast.ActionResource, dbName: String, graphScope: ast.GraphScope) =>

              test(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $preposition role") {
                yields(func(privilege, resource, graphScope, ast.AllQualifier() _, Seq("role")))
              }
          }
      }
  }

}
