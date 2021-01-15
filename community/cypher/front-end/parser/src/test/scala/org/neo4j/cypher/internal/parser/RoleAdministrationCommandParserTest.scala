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
package org.neo4j.cypher.internal.parser

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.util.InputPosition

class RoleAdministrationCommandParserTest extends AdministrationCommandParserTestBase {
  private val roleString = "role"

  //  Showing roles

  test("SHOW ROLES") {
    yields(ast.ShowRoles(withUsers = false, showAll = true, None))
  }

  test("USE neo4j SHOW ROLES") {
    yields(ast.ShowRoles(withUsers = false, showAll = true, None))
  }

  test("CATALOG SHOW ALL ROLES") {
    yields(ast.ShowRoles(withUsers = false, showAll = true, None))
  }

  test("CATALOG SHOW POPULATED ROLES") {
    yields(ast.ShowRoles(withUsers = false, showAll = false, None))
  }

  test("SHOW ROLES WITH USERS") {
    yields(ast.ShowRoles(withUsers = true, showAll = true, None))
  }

  test("CATALOG SHOW ALL ROLES WITH USERS") {
    yields(ast.ShowRoles(withUsers = true, showAll = true, None))
  }

  test("SHOW POPULATED ROLES WITH USERS") {
    yields(ast.ShowRoles(withUsers = true, showAll = false, None))
  }

  test("CATALOG SHOW ALL ROLES YIELD role") {
    yields(ast.ShowRoles(withUsers = false, showAll = true, Some(Left((yieldClause(returnItems(variableReturnItem(roleString))), None)))))
  }

  test("CATALOG SHOW ALL ROLES WHERE role='PUBLIC'") {
    yields(ast.ShowRoles(withUsers = false, showAll = true, Some(Right(where(equals(varFor(roleString), literalString("PUBLIC")))))))
  }

  test("SHOW ALL ROLES YIELD role RETURN role") {
    yields(ast.ShowRoles(withUsers = false, showAll = true,
      Some(Left((yieldClause(returnItems(variableReturnItem(roleString))),
      Some(returnClause(returnItems(variableReturnItem(roleString))))
    )))))
  }

  test("SHOW POPULATED ROLES YIELD role WHERE role='PUBLIC' RETURN role") {
    yields(ast.ShowRoles(withUsers = false, showAll = false,
      Some(Left((yieldClause(returnItems(variableReturnItem(roleString)), where = Some(where(equals(varFor(roleString), literalString("PUBLIC"))))),
        Some(returnClause(returnItems(variableReturnItem(roleString))))
    )))))
  }

  test("SHOW POPULATED ROLES YIELD * RETURN *") {
    yields(ast.ShowRoles(withUsers = false, showAll = false, Some(Left((yieldClause(returnAllItems), Some(returnClause(returnAllItems)))))))
  }

  test("SHOW ROLES WITH USERS YIELD * LIMIT 10 WHERE foo='bar' RETURN some,columns LIMIT 10") {
    yields(ast.ShowRoles(withUsers = true, showAll = true,
      Some(Left((yieldClause(returnAllItems, limit = Some(limit(10)), where = Some(where(equals(varFor("foo"), literalString("bar"))))),Some(returnClause(returnItems(variableReturnItem("some"), variableReturnItem("columns")), limit = Some(limit(10))))
    )))))
  }

  test("CATALOG SHOW ROLE") {
    failsToParse
  }

  test("SHOW ALL ROLE") {
    failsToParse
  }

  test("SHOW POPULATED ROLE") {
    failsToParse
  }

  test("SHOW ROLE role") {
    failsToParse
  }

  test("SHOW ROLE WITH USERS") {
    failsToParse
  }

  test("CATALOG SHOW ROLES WITH USER") {
    failsToParse
  }

  test("SHOW ROLE WITH USER") {
    failsToParse
  }

  test("SHOW ALL ROLE WITH USERS") {
    failsToParse
  }

  test("SHOW ALL ROLES WITH USER") {
    failsToParse
  }

  test("SHOW ALL ROLE WITH USER") {
    failsToParse
  }

  test("CATALOG SHOW POPULATED ROLE WITH USERS") {
    failsToParse
  }

  test("CATALOG SHOW POPULATED ROLES WITH USER") {
    failsToParse
  }

  test("CATALOG SHOW POPULATED ROLE WITH USER") {
    failsToParse
  }

  test("CATALOG SHOW ROLES WITH USER user") {
    failsToParse
  }

  test("SHOW POPULATED ROLES YIELD *,blah RETURN role") {
    failsToParse
  }

  //  Creating role

  test("CREATE ROLE foo") {
    yields(ast.CreateRole(literalFoo, None, ast.IfExistsThrowError))
  }

  test("CREATE ROLE $foo") {
    yields(ast.CreateRole(paramFoo, None, ast.IfExistsThrowError))
  }

  test("CATALOG CREATE ROLE `foo`") {
    yields(ast.CreateRole(literalFoo, None, ast.IfExistsThrowError))
  }

  test("CREATE ROLE ``") {
    yields(ast.CreateRole(literalEmpty, None, ast.IfExistsThrowError))
  }

  test("CREATE ROLE foo AS COPY OF bar") {
    yields(ast.CreateRole(literalFoo, Some(literalBar), ast.IfExistsThrowError))
  }

  test("CREATE ROLE foo AS COPY OF $bar") {
    yields(ast.CreateRole(literalFoo, Some(param("bar")), ast.IfExistsThrowError))
  }

  test("CREATE ROLE foo AS COPY OF ``") {
    yields(ast.CreateRole(literalFoo, Some(literalEmpty), ast.IfExistsThrowError))
  }

  test("CREATE ROLE `` AS COPY OF bar") {
    yields(ast.CreateRole(literalEmpty, Some(literalBar), ast.IfExistsThrowError))
  }

  test("CREATE ROLE foo IF NOT EXISTS") {
    yields(ast.CreateRole(literalFoo, None, ast.IfExistsDoNothing))
  }

  test("CREATE ROLE foo IF NOT EXISTS AS COPY OF bar") {
    yields(ast.CreateRole(literalFoo, Some(literalBar), ast.IfExistsDoNothing))
  }

  test("CREATE OR REPLACE ROLE foo") {
    yields(ast.CreateRole(literalFoo, None, ast.IfExistsReplace))
  }

  test("CREATE OR REPLACE ROLE foo AS COPY OF bar") {
    yields(ast.CreateRole(literalFoo, Some(literalBar), ast.IfExistsReplace))
  }

  test("CREATE OR REPLACE ROLE foo IF NOT EXISTS") {
    yields(ast.CreateRole(literalFoo, None, ast.IfExistsInvalidSyntax))
  }

  test("CREATE OR REPLACE ROLE foo IF NOT EXISTS AS COPY OF bar") {
    yields(ast.CreateRole(literalFoo, Some(literalBar), ast.IfExistsInvalidSyntax))
  }

  test("CATALOG CREATE ROLE \"foo\"") {
    failsToParse
  }

  test("CREATE ROLE f%o") {
    failsToParse
  }

  test("CREATE ROLE  IF NOT EXISTS") {
    failsToParse
  }

  test("CREATE ROLE foo IF EXISTS") {
    failsToParse
  }

  test("CREATE OR REPLACE ROLE ") {
    failsToParse
  }

  test("CREATE ROLE foo AS COPY OF") {
    failsToParse
  }

  test("CREATE ROLE foo IF NOT EXISTS AS COPY OF") {
    failsToParse
  }

  test("CREATE OR REPLACE ROLE foo AS COPY OF") {
    failsToParse
  }

  // Altering role

  test("ALTER ROLE foo SET NAME bar") {
    yields(ast.AlterRole(literalFoo, literalBar))
  }

  test("ALTER ROLE foo SET NAME $bar") {
    yields(ast.AlterRole(literalFoo, param("bar")))
  }

  test("ALTER ROLE $foo SET NAME bar") {
    yields(ast.AlterRole(param("foo"), literalBar))
  }

  test("ALTER ROLE $foo SET NAME $bar") {
    yields(ast.AlterRole(param("foo"), param("bar")))
  }

  test("ALTER ROLE foo SET NAME ``") {
    yields(ast.AlterRole(literalFoo, literalEmpty))
  }

  test("ALTER ROLE `` SET NAME bar") {
    yields(ast.AlterRole(literalEmpty, literalBar))
  }

  test("ALTER ROLE foo SET NAME") {
    failsToParse
  }

  test("ALTER ROLE SET NAME bar") {
    failsToParse
  }

  test("ALTER ROLE SET NAME") {
    failsToParse
  }

  test("ALTER ROLE foo SET NAME TO bar") {
    failsToParse
  }

  test("RENAME ROLE foo SET NAME bar") {
    failsToParse
  }

  //  Dropping role

  test("DROP ROLE foo") {
    yields(ast.DropRole(literalFoo, ifExists = false))
  }

  test("DROP ROLE $foo") {
    yields(ast.DropRole(paramFoo, ifExists = false))
  }

  test("DROP ROLE ``") {
    yields(ast.DropRole(literalEmpty, ifExists = false))
  }

  test("DROP ROLE foo IF EXISTS") {
    yields(ast.DropRole(literalFoo, ifExists = true))
  }

  test("DROP ROLE `` IF EXISTS") {
    yields(ast.DropRole(literalEmpty, ifExists = true))
  }

  test("DROP ROLE ") {
    failsToParse
  }

  test("DROP ROLE  IF EXISTS") {
    failsToParse
  }

  test("DROP ROLE foo IF NOT EXISTS") {
    failsToParse
  }

  //  Granting/revoking roles to/from users

  private type grantOrRevokeRoleFunc = (Seq[String], Seq[String]) => InputPosition => ast.Statement

  private def grantRole(r: Seq[String], u: Seq[String]): InputPosition => ast.Statement = ast.GrantRolesToUsers(r.map(Left(_)), u.map(Left(_)))

  private def revokeRole(r: Seq[String], u: Seq[String]): InputPosition => ast.Statement = ast.RevokeRolesFromUsers(r.map(Left(_)), u.map(Left(_)))

  Seq("ROLE", "ROLES").foreach {
    roleKeyword =>

      Seq(
        ("GRANT", "TO", grantRole: grantOrRevokeRoleFunc),
        ("REVOKE", "FROM", revokeRole: grantOrRevokeRoleFunc)
      ).foreach {
        case (verb: String, preposition: String, func: grantOrRevokeRoleFunc) =>

          test(s"$verb $roleKeyword foo $preposition abc") {
            yields(func(Seq("foo"), Seq("abc")))
          }

          test(s"CATALOG $verb $roleKeyword foo $preposition abc") {
            yields(func(Seq("foo"), Seq("abc")))
          }

          test(s"$verb $roleKeyword foo, bar $preposition abc") {
            yields(func(Seq("foo", "bar"), Seq("abc")))
          }

          test(s"$verb $roleKeyword foo $preposition abc, def") {
            yields(func(Seq("foo"), Seq("abc", "def")))
          }

          test(s"$verb $roleKeyword foo,bla,roo $preposition bar, baz,abc,  def") {
            yields(func(Seq("foo", "bla", "roo"), Seq("bar", "baz", "abc", "def")))
          }

          test(s"$verb $roleKeyword `fo:o` $preposition bar") {
            yields(func(Seq("fo:o"), Seq("bar")))
          }

          test(s"$verb $roleKeyword foo $preposition `b:ar`") {
            yields(func(Seq("foo"), Seq("b:ar")))
          }

          test(s"$verb $roleKeyword `$$f00`,bar $preposition abc,`$$a&c`") {
            yields(func(Seq("$f00", "bar"), Seq("abc", "$a&c")))
          }

          // Should fail to parse if not following the pattern $command $roleKeyword role(s) $preposition user(s)

          test(s"$verb $roleKeyword") {
            failsToParse
          }

          test(s"$verb $roleKeyword foo") {
            failsToParse
          }

          test(s"$verb $roleKeyword foo $preposition") {
            failsToParse
          }

          test(s"$verb $roleKeyword $preposition abc") {
            failsToParse
          }

          // Should fail to parse when invalid user or role name

          test(s"$verb $roleKeyword fo:o $preposition bar") {
            failsToParse
          }

          test(s"$verb $roleKeyword foo $preposition b:ar") {
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

      // ROLES TO USER only have GRANT and REVOKE and not DENY

      test(s"DENY $roleKeyword foo TO abc") {
        failsToParse
      }
  }

  test("GRANT ROLE $a TO $x") {
    yields(ast.GrantRolesToUsers(Seq(param("a")), Seq(param("x"))))
  }

  test("REVOKE ROLE $a FROM $x") {
    yields(ast.RevokeRolesFromUsers(Seq(param("a")), Seq(param("x"))))
  }

  test("GRANT ROLES a, $b, $c TO $x, y, z") {
    yields(ast.GrantRolesToUsers(Seq(literal("a"), param("b"), param("c")), Seq(param("x"), literal("y"), literal("z"))))
  }

  test("REVOKE ROLES a, $b, $c FROM $x, y, z") {
    yields(ast.RevokeRolesFromUsers(Seq(literal("a"), param("b"), param("c")), Seq(param("x"), literal("y"), literal("z"))))
  }
}
