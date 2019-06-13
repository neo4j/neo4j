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
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.util.InputPosition

class PrivilegeManagementDDLParserTest extends DDLParserTestBase {

  //  Granting/revoking roles to/from users

  type grantOrRevokeRoleFunc = (Seq[String], Seq[String]) => InputPosition => ast.Statement

  def grantRole(r: Seq[String], u: Seq[String]): InputPosition => ast.Statement = GrantRolesToUsers(r, u)

  def revokeRole(r: Seq[String], u: Seq[String]): InputPosition => ast.Statement = RevokeRolesFromUsers(r, u)

  Seq("ROLE", "ROLES").foreach {
    roleKeyword =>

      Seq(
        ("GRANT", "TO", grantRole: grantOrRevokeRoleFunc),
        ("REVOKE", "FROM", revokeRole: grantOrRevokeRoleFunc)
      ).foreach {
        case (command: String, preposition: String, func: grantOrRevokeRoleFunc) =>

          test(s"$command $roleKeyword foo $preposition abc") {
            yields(func(Seq("foo"), Seq("abc")))
          }

          test(s"CATALOG $command $roleKeyword foo $preposition abc") {
            yields(func(Seq("foo"), Seq("abc")))
          }

          test(s"$command $roleKeyword foo, bar $preposition abc") {
            yields(func(Seq("foo", "bar"), Seq("abc")))
          }

          test(s"$command $roleKeyword foo $preposition abc, def") {
            yields(func(Seq("foo"), Seq("abc", "def")))
          }

          test(s"$command $roleKeyword foo,bla,roo $preposition bar, baz,abc,  def") {
            yields(func(Seq("foo", "bla", "roo"), Seq("bar", "baz", "abc", "def")))
          }

          test(s"$command $roleKeyword `fo:o` $preposition bar") {
            yields(func(Seq("fo:o"), Seq("bar")))
          }

          test(s"$command $roleKeyword foo $preposition `b:ar`") {
            yields(func(Seq("foo"), Seq("b:ar")))
          }

          test(s"$command $roleKeyword `$$f00`,bar $preposition abc,`$$a&c`") {
            yields(func(Seq("$f00", "bar"), Seq("abc", "$a&c")))
          }

          // Should fail to parse if not following the pattern $command $roleKeyword role(s) $preposition user(s)

          test(s"$command $roleKeyword") {
            failsToParse
          }

          test(s"$command $roleKeyword foo") {
            failsToParse
          }

          test(s"$command $roleKeyword foo $preposition") {
            failsToParse
          }

          test(s"$command $roleKeyword $preposition abc") {
            failsToParse
          }

          // Should fail to parse when invalid user or role name

          test(s"$command $roleKeyword $$f00 $preposition abc") {
            failsToParse
          }

          test(s"$command $roleKeyword fo:o $preposition bar") {
            failsToParse
          }

          test(s"$command $roleKeyword foo $preposition b:ar") {
            failsToParse
          }
      }

      // Should fail to parse when mixing TO and FROM

      test(s"GRANT $roleKeyword foo FROM abc") {
        failsToParse
      }

      test(s"REVOKE $roleKeyword foo TO abc") {
        failsToParse
      }
  }

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

  type grantOrRevokeFunc = (PrivilegeType, ActionResource, GraphScope, PrivilegeQualifier, Seq[String]) => InputPosition => ast.Statement

  def grant(p: PrivilegeType, a: ActionResource, s: GraphScope, q: PrivilegeQualifier, r: Seq[String]): InputPosition => ast.Statement = GrantPrivilege(p, a, s, q, r)

  def revoke(p: PrivilegeType, a: ActionResource, s: GraphScope, q: PrivilegeQualifier, r: Seq[String]): InputPosition => ast.Statement = RevokePrivilege(p, a, s, q, r)

  //  Granting/revoking traverse to/from role

  Seq(
    ("GRANT", "TO", grant: grantOrRevokeFunc),
    ("REVOKE", "FROM", revoke: grantOrRevokeFunc)
  ).foreach {
    case (command: String, preposition: String, func: grantOrRevokeFunc) =>

      Seq("GRAPH", "GRAPHS").foreach {
        graphKeyword =>
          test(s"$command TRAVERSE ON $graphKeyword * $preposition role") {
            yields(func(TraversePrivilege()(pos), AllResource()(pos), ast.AllGraphsScope() _, ast.AllQualifier() _, Seq("role")))
          }

          test(s"$command TRAVERSE ON $graphKeyword foo $preposition role") {
            yields(func(TraversePrivilege()(pos), AllResource()(pos), ast.NamedGraphScope("foo") _, ast.AllQualifier() _, Seq("role")))
          }

          Seq("NODE", "NODES").foreach {
            nodeKeyword =>

              test(s"$command TRAVERSE ON $graphKeyword * $nodeKeyword * $preposition role") {
                yields(func(TraversePrivilege()(pos), AllResource()(pos), ast.AllGraphsScope() _, ast.AllQualifier() _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword * $nodeKeyword * (*) $preposition role") {
                yields(func(TraversePrivilege()(pos), AllResource()(pos), ast.AllGraphsScope() _, ast.AllQualifier() _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword * $nodeKeyword A $preposition role") {
                yields(func(TraversePrivilege()(pos), AllResource()(pos), ast.AllGraphsScope() _, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword * $nodeKeyword A (*) $preposition role") {
                yields(func(TraversePrivilege()(pos), AllResource()(pos), ast.AllGraphsScope() _, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword `*` $nodeKeyword A $preposition role") {
                yields(func(TraversePrivilege()(pos), AllResource()(pos), ast.NamedGraphScope("*") _, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword * $preposition role") {
                yields(func(TraversePrivilege()(pos), AllResource()(pos), ast.NamedGraphScope("foo") _, ast.AllQualifier() _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword * (*) $preposition role") {
                yields(func(TraversePrivilege()(pos), AllResource()(pos), ast.NamedGraphScope("foo") _, ast.AllQualifier() _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A $preposition role") {
                yields(func(TraversePrivilege()(pos), AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition role") {
                yields(func(TraversePrivilege()(pos), AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition role1, role2") {
                yields(func(TraversePrivilege()(pos), AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A")) _, Seq("role1", "role2")))
              }

              test(s"$command TRAVERSE ON $graphKeyword `2foo` $nodeKeyword A (*) $preposition role") {
                yields(func(TraversePrivilege()(pos), AllResource()(pos), ast.NamedGraphScope("2foo") _, ast.LabelsQualifier(Seq("A")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition `r:ole`") {
                yields(func(TraversePrivilege()(pos), AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A")) _, Seq("r:ole")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword `A B` (*) $preposition role") {
                yields(func(TraversePrivilege()(pos), AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A B")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A, B (*) $preposition role") {
                yields(func(TraversePrivilege()(pos), AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A", "B")) _, Seq("role")))
              }

              test(s"$command TRAVERSE ON $graphKeyword foo $nodeKeyword A, B (*) $preposition role1, role2") {
                yields(func(TraversePrivilege()(pos), AllResource()(pos), ast.NamedGraphScope("foo") _, ast.LabelsQualifier(Seq("A", "B")) _, Seq("role1", "role2")))
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
      }
  }

  // Granting/revoking read and match to/from role

  Seq(
    (ReadPrivilege()(pos), "GRANT", "TO", grant: grantOrRevokeFunc),
    (ReadPrivilege()(pos), "REVOKE", "FROM", revoke: grantOrRevokeFunc),
    (MatchPrivilege()(pos), "GRANT", "TO", grant: grantOrRevokeFunc),
    (MatchPrivilege()(pos), "REVOKE", "FROM", revoke: grantOrRevokeFunc)
  ).foreach {
    case (privilege: PrivilegeType, command: String, preposition: String, func: grantOrRevokeFunc) =>

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
                case (properties: String, resource: ActionResource, dbName: String, graphScope: GraphScope) =>

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $nodeKeyword * $preposition role") {
                    yields(func(privilege, resource, graphScope, ast.AllQualifier() _, Seq("role")))
                  }

                  test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $nodeKeyword * (*) $preposition role") {
                    yields(func(privilege, resource, graphScope, ast.AllQualifier() _, Seq("role")))
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
                    yields(func(privilege, resource, graphScope, ast.AllQualifier() _, Seq("r:ole")))
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
                yields(func(privilege, ast.AllResource() _, ast.NamedGraphScope("f:oo") _, ast.AllQualifier() _, Seq("role")))
              }

              test(s"$command ${privilege.name} (bar) ON $graphKeyword `f:oo` $nodeKeyword * $preposition role") {
                yields(func(privilege, ast.PropertiesResource(Seq("bar")) _, ast.NamedGraphScope("f:oo") _, ast.AllQualifier() _, Seq("role")))
              }

              test(s"$command ${privilege.name} (`b:ar`) ON $graphKeyword foo $nodeKeyword * $preposition role") {
                yields(func(privilege, ast.PropertiesResource(Seq("b:ar")) _, ast.NamedGraphScope("foo") _, ast.AllQualifier() _, Seq("role")))
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

          // Needs to be separate loop to avoid duplicate tests since the test does not have any $nodeKeyword
          Seq(
            ("*", ast.AllResource()(pos), "*", ast.AllGraphsScope()(pos)),
            ("*", ast.AllResource()(pos), "foo", ast.NamedGraphScope("foo")(pos)),
            ("bar", ast.PropertiesResource(Seq("bar"))(pos), "*", ast.AllGraphsScope()(pos)),
            ("bar", ast.PropertiesResource(Seq("bar"))(pos), "foo", ast.NamedGraphScope("foo")(pos)),
            ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "*", ast.AllGraphsScope()(pos)),
            ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "foo", ast.NamedGraphScope("foo")(pos))
          ).foreach {
            case (properties: String, resource: ActionResource, dbName: String, graphScope: GraphScope) =>

              test(s"$command ${privilege.name} ($properties) ON $graphKeyword $dbName $preposition role") {
                yields(func(privilege, resource, graphScope, ast.AllQualifier() _, Seq("role")))
              }
          }
      }
  }
}
