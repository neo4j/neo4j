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
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.parser.AdministrationCommandParserTestBase

class PrivilegeAdministrationCommandParserTest extends AdministrationCommandParserTestBase {

  //  Showing privileges

  test("SHOW PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowAllPrivileges() _, None, None, None))
  }

  test("catalog show privileges") {
    yields(ast.ShowPrivileges(ast.ShowAllPrivileges() _, None, None, None))
  }

  test("SHOW ALL PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowAllPrivileges() _, None, None, None))
  }

  // yield / skip / limit / order by / where
  Seq(
    ("ALL", ast.ShowAllPrivileges() _),
    ("USER neo4j", ast.ShowUserPrivileges(Some(literal("neo4j"))) _),
    ("ROLE $role", ast.ShowRolePrivileges(param("role")) _)
  ).foreach{ case (privType, privilege) => {
    test(s"SHOW $privType PRIVILEGES WHERE access = 'GRANTED'") {
      yields(ast.ShowPrivileges(privilege, None, Some(ast.Where(equals(varFor("access"), literalString("GRANTED"))) _), None))
    }

    test(s"SHOW $privType PRIVILEGES WHERE access = 'GRANTED' AND action = 'match'") {
      val accessPredicate = equals(varFor("access"), literalString("GRANTED"))
      val matchPredicate = equals(varFor("action"), literalString("match"))
      yields(ast.ShowPrivileges(privilege, None, Some(ast.Where(and(accessPredicate, matchPredicate)) _), None))
    }

    test(s"SHOW $privType PRIVILEGES YIELD access ORDER BY access") {
      val orderBy = ast.OrderBy(List(ast.AscSortItem(varFor("access")) _)) _
      val columns = ast.Return(false, ast.ReturnItems(false, List(UnaliasedReturnItem(varFor("access"), "access") _)) _, Some(orderBy), None, None) _
      yields(ast.ShowPrivileges(privilege, Some(columns), None, None))
    }

    test(s"SHOW $privType PRIVILEGES YIELD access ORDER BY access WHERE access ='none'") {
      val orderBy = ast.OrderBy(List(ast.AscSortItem(varFor("access")) _)) _
      val columns = ast.Return(false, ast.ReturnItems(false, List(UnaliasedReturnItem(varFor("access"), "access") _)) _, Some(orderBy), None, None) _
      val where = ast.Where(equals(varFor("access"), literalString("none"))) _
      yields(ast.ShowPrivileges(privilege, Some(columns), Some(where), None))
    }

    test(s"SHOW $privType PRIVILEGES YIELD access ORDER BY access SKIP 1 LIMIT 10 WHERE access ='none'") {
      val orderBy = ast.OrderBy(List(ast.AscSortItem(varFor("access")) _)) _
      val columns = ast.Return(false, ast.ReturnItems(false, List(UnaliasedReturnItem(varFor("access"), "access") _)) _, Some(orderBy),
        Some(ast.Skip(literalInt(1)) _), Some(ast.Limit(literalInt(10)) _)) _
      val where = ast.Where(equals(varFor("access"), literalString("none"))) _
      yields(ast.ShowPrivileges(privilege, Some(columns), Some(where), None))
    }

    test(s"SHOW $privType PRIVILEGES YIELD access SKIP -1") {
      val columns = ast.Return(false, ast.ReturnItems(false, List(UnaliasedReturnItem(varFor("access"), "access") _)) _, None,
        Some(ast.Skip(literalInt(-1)) _), None) _
      yields(ast.ShowPrivileges(privilege, Some(columns), None, None))
    }

    test(s"SHOW $privType PRIVILEGES YIELD access, action SKIP 1 RETURN access, action") {
      failsToParse
    }

    test(s"SHOW $privType PRIVILEGES YIELD access, action WHERE access = 'none' RETURN action") {
      failsToParse
    }
  }}

  test("SHOW USER user PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowUserPrivileges(Some(literal("user"))) _, None, None, None))
  }

  test("SHOW USER $user PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowUserPrivileges(Some(param("user"))) _, None, None, None))
  }

  test("SHOW USER `us%er` PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowUserPrivileges(Some(literal("us%er"))) _, None, None, None))
  }

  test("SHOW USER PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowUserPrivileges(None) _, None, None, None))
  }

  test("SHOW ROLE role PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowRolePrivileges(literal("role")) _, None, None, None))
  }

  test("SHOW ROLE $role PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowRolePrivileges(param("role")) _, None, None, None))
  }

  test("SHOW ROLE `ro%le` PRIVILEGES") {
    yields(ast.ShowPrivileges(ast.ShowRolePrivileges(literal("ro%le")) _, None, None, None))
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
    (ast.ReadPrivilege()(pos), "GRANT", "TO", grant: resourcePrivilegeFunc),
    (ast.ReadPrivilege()(pos), "DENY", "TO", deny: resourcePrivilegeFunc),
    (ast.ReadPrivilege()(pos), "REVOKE GRANT", "FROM", revokeGrant: resourcePrivilegeFunc),
    (ast.ReadPrivilege()(pos), "REVOKE DENY", "FROM", revokeDeny: resourcePrivilegeFunc),
    (ast.ReadPrivilege()(pos), "REVOKE", "FROM", revokeBoth: resourcePrivilegeFunc),
    (ast.MatchPrivilege()(pos), "GRANT", "TO", grant: resourcePrivilegeFunc),
    (ast.MatchPrivilege()(pos), "DENY", "TO", deny: resourcePrivilegeFunc),
    (ast.MatchPrivilege()(pos), "REVOKE GRANT", "FROM", revokeGrant: resourcePrivilegeFunc),
    (ast.MatchPrivilege()(pos), "REVOKE DENY", "FROM", revokeDeny: resourcePrivilegeFunc),
    (ast.MatchPrivilege()(pos), "REVOKE", "FROM", revokeBoth: resourcePrivilegeFunc)
  ).foreach {
    case (privilege: ast.PrivilegeType, command: String, preposition: String, func: resourcePrivilegeFunc) =>

      Seq("GRAPH", "GRAPHS").foreach {
        graphKeyword =>

          Seq("NODE", "NODES").foreach {
            nodeKeyword =>

              Seq(
                ("*", ast.AllPropertyResource()(pos), "*", ast.AllGraphsScope()(pos)),
                ("*", ast.AllPropertyResource()(pos), "foo", ast.NamedGraphScope(literal("foo"))(pos)),
                ("bar", ast.PropertiesResource(Seq("bar"))(pos), "*", ast.AllGraphsScope()(pos)),
                ("bar", ast.PropertiesResource(Seq("bar"))(pos), "foo", ast.NamedGraphScope(literal("foo"))(pos)),
                ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "*", ast.AllGraphsScope()(pos)),
                ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "foo", ast.NamedGraphScope(literal("foo"))(pos))
              ).foreach {
                case (properties: String, resource: ast.ActionResource, dbName: String, graphScope: ast.GraphScope) =>
                  val graphScopes = List(graphScope)

                  test( s"validExpressions $command ${privilege.name} {$properties} $graphKeyword $dbName $nodeKeyword $preposition") {
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $nodeKeyword * $preposition $$role") shouldGive
                      func(privilege, resource, graphScopes, ast.LabelAllQualifier() _, Seq(param("role")))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $nodeKeyword * (*) $preposition role") shouldGive
                      func(privilege, resource, graphScopes, ast.LabelAllQualifier() _, Seq(literal("role")))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $nodeKeyword A $preposition role") shouldGive
                      func(privilege, resource, graphScopes, ast.LabelsQualifier(Seq("A")) _, Seq(literal("role")))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $nodeKeyword A (*) $preposition role") shouldGive
                      func(privilege, resource, graphScopes, ast.LabelsQualifier(Seq("A")) _, Seq(literal("role")))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $nodeKeyword `A B` (*) $preposition role") shouldGive
                      func(privilege, resource, graphScopes, ast.LabelsQualifier(Seq("A B")) _, Seq(literal("role")))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $nodeKeyword A, B (*) $preposition role1, $$role2") shouldGive
                      func(privilege, resource, graphScopes, ast.LabelsQualifier(Seq("A", "B")) _, Seq(literal("role1"), param("role2")))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $nodeKeyword * $preposition `r:ole`") shouldGive
                      func(privilege, resource, graphScopes, ast.LabelAllQualifier() _, Seq(literal("r:ole")))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $nodeKeyword `:A` (*) $preposition role") shouldGive
                      func(privilege, resource, graphScopes, ast.LabelsQualifier(Seq(":A")) _, Seq(literal("role")))
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
                  func(privilege, ast.AllPropertyResource() _, List(ast.NamedGraphScope(literal("f:oo")) _), ast.LabelAllQualifier() _, Seq(literal("role")))
                parsing(s"$command ${privilege.name} {bar} ON $graphKeyword `f:oo` $nodeKeyword * $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("bar")) _, List(ast.NamedGraphScope(literal("f:oo")) _), ast.LabelAllQualifier() _, Seq(literal("role")))
                parsing(s"$command ${privilege.name} {`b:ar`} ON $graphKeyword foo $nodeKeyword * $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("b:ar")) _, List(ast.NamedGraphScope(literal("foo")) _), ast.LabelAllQualifier() _, Seq(literal("role")))
                parsing(s"$command ${privilege.name} {*} ON $graphKeyword foo, baz $nodeKeyword A (*) $preposition role") shouldGive
                  func(privilege, ast.AllPropertyResource() _, List(ast.NamedGraphScope(literal("foo")) _, ast.NamedGraphScope(literal("baz")) _), ast.LabelsQualifier(Seq("A")) _, Seq(literal("role")))
                parsing(s"$command ${privilege.name} {bar} ON $graphKeyword foo, baz $nodeKeyword A (*) $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("bar")) _, List(ast.NamedGraphScope(literal("foo")) _, ast.NamedGraphScope(literal("baz")) _), ast.LabelsQualifier(Seq("A")) _, Seq(literal("role")))
              }

              test( s"parsingFailures $command ${privilege.name} $graphKeyword $nodeKeyword $preposition") {
                // Invalid graph name
                assertFails(s"$command ${privilege.name} {*} ON $graphKeyword f:oo $nodeKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} {bar} ON $graphKeyword f:oo $nodeKeyword * $preposition role")
                // mixing specific graph and *
                assertFails(s"$command ${privilege.name} {*} ON $graphKeyword foo, * $nodeKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} {*} ON $graphKeyword *, foo $nodeKeyword * $preposition role")
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
                ("*", ast.AllPropertyResource()(pos), "*", ast.AllGraphsScope()(pos)),
                ("*", ast.AllPropertyResource()(pos), "foo", ast.NamedGraphScope(literal("foo"))(pos)),
                ("bar", ast.PropertiesResource(Seq("bar"))(pos), "*", ast.AllGraphsScope()(pos)),
                ("bar", ast.PropertiesResource(Seq("bar"))(pos), "foo", ast.NamedGraphScope(literal("foo"))(pos)),
                ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "*", ast.AllGraphsScope()(pos)),
                ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "foo", ast.NamedGraphScope(literal("foo"))(pos))
              ).foreach {
                case (properties: String, resource: ast.ActionResource, dbName: String, graphScope: ast.GraphScope) =>
                  val graphScopes = List(graphScope)

                  test( s"validExpressions $command ${privilege.name} {$properties} $graphKeyword $dbName $relTypeKeyword $preposition") {
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $relTypeKeyword * $preposition role") shouldGive
                      func(privilege, resource, graphScopes, ast.RelationshipAllQualifier() _, Seq(literal("role")))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $relTypeKeyword * (*) $preposition $$role") shouldGive
                      func(privilege, resource, graphScopes, ast.RelationshipAllQualifier() _, Seq(param("role")))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $relTypeKeyword A $preposition role") shouldGive
                      func(privilege, resource, graphScopes, ast.RelationshipsQualifier(Seq("A")) _, Seq(literal("role")))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $relTypeKeyword A (*) $preposition role") shouldGive
                      func(privilege, resource, graphScopes, ast.RelationshipsQualifier(Seq("A")) _, Seq(literal("role")))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $relTypeKeyword `A B` (*) $preposition role") shouldGive
                      func(privilege, resource, graphScopes, ast.RelationshipsQualifier(Seq("A B")) _, Seq(literal("role")))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $relTypeKeyword A, B (*) $preposition $$role1, role2") shouldGive
                      func(privilege, resource, graphScopes, ast.RelationshipsQualifier(Seq("A", "B")) _, Seq(param("role1"), literal("role2")))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $relTypeKeyword * $preposition `r:ole`") shouldGive
                      func(privilege, resource, graphScopes, ast.RelationshipAllQualifier() _, Seq(literal("r:ole")))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $relTypeKeyword `:A` (*) $preposition role") shouldGive
                      func(privilege, resource, graphScopes, ast.RelationshipsQualifier(Seq(":A")) _, Seq(literal("role")))
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
                  func(privilege, ast.AllPropertyResource() _, List(ast.NamedGraphScope(literal("f:oo")) _), ast.RelationshipAllQualifier() _, Seq(literal("role")))
                parsing(s"$command ${privilege.name} {bar} ON $graphKeyword `f:oo` $relTypeKeyword * $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("bar")) _, List(ast.NamedGraphScope(literal("f:oo")) _), ast.RelationshipAllQualifier() _, Seq(literal("role")))
                parsing(s"$command ${privilege.name} {`b:ar`} ON $graphKeyword foo $relTypeKeyword * $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("b:ar")) _, List(ast.NamedGraphScope(literal("foo")) _), ast.RelationshipAllQualifier() _, Seq(literal("role")))
                parsing(s"$command ${privilege.name} {*} ON $graphKeyword foo, baz $relTypeKeyword A (*) $preposition role") shouldGive
                  func(privilege, ast.AllPropertyResource() _, List(ast.NamedGraphScope(literal("foo")) _, ast.NamedGraphScope(literal("baz")) _), ast.RelationshipsQualifier(Seq("A")) _, Seq(literal("role")))
                parsing(s"$command ${privilege.name} {bar} ON $graphKeyword foo, baz $relTypeKeyword A (*) $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("bar")) _, List(ast.NamedGraphScope(literal("foo")) _, ast.NamedGraphScope(literal("baz")) _), ast.RelationshipsQualifier(Seq("A")) _, Seq(literal("role")))
              }

              test( s"parsingFailures$command ${privilege.name} $graphKeyword $relTypeKeyword $preposition") {
                // Invalid graph name
                assertFails(s"$command ${privilege.name} {*} ON $graphKeyword f:oo $relTypeKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} {bar} ON $graphKeyword f:oo $relTypeKeyword * $preposition role")
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
                ("*", ast.AllPropertyResource()(pos), "*", ast.AllGraphsScope()(pos)),
                ("*", ast.AllPropertyResource()(pos), "foo", ast.NamedGraphScope(literal("foo"))(pos)),
                ("bar", ast.PropertiesResource(Seq("bar"))(pos), "*", ast.AllGraphsScope()(pos)),
                ("bar", ast.PropertiesResource(Seq("bar"))(pos), "foo", ast.NamedGraphScope(literal("foo"))(pos)),
                ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "*", ast.AllGraphsScope()(pos)),
                ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "foo", ast.NamedGraphScope(literal("foo"))(pos))
              ).foreach {
                case (properties: String, resource: ast.ActionResource, dbName: String, graphScope: ast.GraphScope) =>
                  val graphScopes = List(graphScope)

                  test( s"validExpressions $command ${privilege.name} {$properties} $graphKeyword $dbName $elementKeyword $preposition") {
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $elementKeyword * $preposition role") shouldGive
                      func(privilege, resource, graphScopes, ast.ElementsAllQualifier() _, Seq(literal("role")))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $elementKeyword * (*) $preposition role") shouldGive
                      func(privilege, resource, graphScopes, ast.ElementsAllQualifier() _, Seq(literal("role")))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $elementKeyword A $preposition $$role") shouldGive
                      func(privilege, resource, graphScopes, ast.ElementsQualifier(Seq("A")) _, Seq(param("role")))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $elementKeyword A (*) $preposition role") shouldGive
                      func(privilege, resource, graphScopes, ast.ElementsQualifier(Seq("A")) _, Seq(literal("role")))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $elementKeyword `A B` (*) $preposition role") shouldGive
                      func(privilege, resource, graphScopes, ast.ElementsQualifier(Seq("A B")) _, Seq(literal("role")))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $elementKeyword A, B (*) $preposition $$role1, $$role2") shouldGive
                      func(privilege, resource, graphScopes, ast.ElementsQualifier(Seq("A", "B")) _, Seq(param("role1"), param("role2")))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $elementKeyword * $preposition `r:ole`") shouldGive
                      func(privilege, resource, graphScopes, ast.ElementsAllQualifier() _, Seq(literal("r:ole")))
                    parsing(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $elementKeyword `:A` (*) $preposition role") shouldGive
                      func(privilege, resource, graphScopes, ast.ElementsQualifier(Seq(":A")) _, Seq(literal("role")))
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
                  func(privilege, ast.AllPropertyResource() _, List(ast.NamedGraphScope(literal("f:oo")) _), ast.ElementsAllQualifier() _, Seq(literal("role")))
                parsing(s"$command ${privilege.name} {bar} ON $graphKeyword `f:oo` $elementKeyword * $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("bar")) _, List(ast.NamedGraphScope(literal("f:oo")) _), ast.ElementsAllQualifier() _, Seq(literal("role")))
                parsing(s"$command ${privilege.name} {`b:ar`} ON $graphKeyword foo $elementKeyword * $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("b:ar")) _, List(ast.NamedGraphScope(literal("foo")) _), ast.ElementsAllQualifier() _, Seq(literal("role")))
                parsing(s"$command ${privilege.name} {*} ON $graphKeyword foo, baz $elementKeyword A (*) $preposition role") shouldGive
                  func(privilege, ast.AllPropertyResource() _, List(ast.NamedGraphScope(literal("foo")) _, ast.NamedGraphScope(literal("baz")) _), ast.ElementsQualifier(Seq("A")) _, Seq(literal("role")))
                parsing(s"$command ${privilege.name} {bar} ON $graphKeyword foo, baz $elementKeyword A (*) $preposition role") shouldGive
                  func(privilege, ast.PropertiesResource(Seq("bar")) _, List(ast.NamedGraphScope(literal("foo")) _, ast.NamedGraphScope(literal("baz")) _), ast.ElementsQualifier(Seq("A")) _, Seq(literal("role")))
              }

              test( s"parsingFailures $command ${privilege.name} $graphKeyword $elementKeyword $preposition") {
                // Invalid graph name
                assertFails(s"$command ${privilege.name} {*} ON $graphKeyword f:oo $elementKeyword * $preposition role")
                assertFails(s"$command ${privilege.name} {bar} ON $graphKeyword f:oo $elementKeyword * $preposition role")
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
            ("*", ast.AllPropertyResource()(pos), "*", ast.AllGraphsScope()(pos)),
            ("*", ast.AllPropertyResource()(pos), "foo", ast.NamedGraphScope(literal("foo"))(pos)),
            ("*", ast.AllPropertyResource()(pos), "$foo", ast.NamedGraphScope(param("foo"))(pos)),
            ("bar", ast.PropertiesResource(Seq("bar"))(pos), "*", ast.AllGraphsScope()(pos)),
            ("bar", ast.PropertiesResource(Seq("bar"))(pos), "foo", ast.NamedGraphScope(literal("foo"))(pos)),
            ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "*", ast.AllGraphsScope()(pos)),
            ("foo, bar", ast.PropertiesResource(Seq("foo", "bar"))(pos), "foo", ast.NamedGraphScope(literal("foo"))(pos))
          ).foreach {
            case (properties: String, resource: ast.ActionResource, dbName: String, graphScope: ast.GraphScope) =>

              test(s"$command ${privilege.name} {$properties} ON $graphKeyword $dbName $preposition role") {
                yields(func(privilege, resource, List(graphScope), ast.ElementsAllQualifier() _, Seq(literal("role"))))
              }
          }
      }

      // Database instead of graph keyword

      test(s"$command ${privilege.name} ON DATABASES * $preposition role") {
        failsToParse
      }

      test(s"$command ${privilege.name} ON DATABASE foo $preposition role") {
        failsToParse
      }

      test(s"$command ${privilege.name} ON DEFAULT DATABASE $preposition role") {
        failsToParse
      }
  }

}
