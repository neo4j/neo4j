/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.util.InputPosition

class RoleAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {
  private val roleString = "role"

  //  Showing roles

  Seq("ROLES", "ROLE").foreach(roleKeyword => {

    test(s"SHOW $roleKeyword") {
      yields(ast.ShowRoles(withUsers = false, showAll = true, None))
    }

    test(s"SHOW ALL $roleKeyword") {
      yields(_ => ast.ShowRoles(withUsers = false, showAll = true, None)(pos))
    }

    test(s"SHOW POPULATED $roleKeyword") {
      yields(_ => ast.ShowRoles(withUsers = false, showAll = false, None)(pos))
    }

    Seq("USERS", "USER").foreach(userKeyword => {

      test(s"SHOW $roleKeyword WITH $userKeyword") {
        yields(ast.ShowRoles(withUsers = true, showAll = true, None))
      }

      test(s"SHOW ALL $roleKeyword WITH $userKeyword") {
        yields(_ => ast.ShowRoles(withUsers = true, showAll = true, None)(pos))
      }

      test(s"SHOW POPULATED $roleKeyword WITH $userKeyword") {
        yields(ast.ShowRoles(withUsers = true, showAll = false, None))
      }

    })

  })

  test("USE neo4j SHOW ROLES") {
    yields(ast.ShowRoles(withUsers = false, showAll = true, None))
  }

  test("USE GRAPH SYSTEM SHOW ROLES") {
    yields(ast.ShowRoles(withUsers = false, showAll = true, None))
  }

  test("SHOW ALL ROLES YIELD role") {
    yields(_ =>
      ast.ShowRoles(
        withUsers = false,
        showAll = true,
        Some(Left((yieldClause(returnItems(variableReturnItem(roleString))), None)))
      )(pos)
    )
  }

  test("SHOW ALL ROLE YIELD role") {
    yields(_ =>
      ast.ShowRoles(
        withUsers = false,
        showAll = true,
        Some(Left((yieldClause(returnItems(variableReturnItem(roleString))), None)))
      )(pos)
    )
  }

  test("SHOW ALL ROLES WHERE role='PUBLIC'") {
    yields(_ =>
      ast.ShowRoles(
        withUsers = false,
        showAll = true,
        Some(Right(where(equals(varFor(roleString), literalString("PUBLIC")))))
      )(pos)
    )
  }

  test("SHOW ALL ROLE WHERE role='PUBLIC'") {
    yields(_ =>
      ast.ShowRoles(
        withUsers = false,
        showAll = true,
        Some(Right(where(equals(varFor(roleString), literalString("PUBLIC")))))
      )(pos)
    )
  }

  test("SHOW ALL ROLES YIELD role RETURN role") {
    yields(ast.ShowRoles(
      withUsers = false,
      showAll = true,
      Some(Left((
        yieldClause(returnItems(variableReturnItem(roleString))),
        Some(returnClause(returnItems(variableReturnItem(roleString))))
      )))
    ))
  }

  test("SHOW ALL ROLES YIELD return, return RETURN return") {
    yields(ast.ShowRoles(
      withUsers = false,
      showAll = true,
      Some(Left((
        yieldClause(returnItems(variableReturnItem("return"), variableReturnItem("return"))),
        Some(returnClause(returnItems(variableReturnItem("return"))))
      )))
    ))
  }

  test("SHOW POPULATED ROLES YIELD role WHERE role='PUBLIC' RETURN role") {
    yields(ast.ShowRoles(
      withUsers = false,
      showAll = false,
      Some(Left((
        yieldClause(
          returnItems(variableReturnItem(roleString)),
          where = Some(where(equals(varFor(roleString), literalString("PUBLIC"))))
        ),
        Some(returnClause(returnItems(variableReturnItem(roleString))))
      )))
    ))
  }

  test("SHOW POPULATED ROLES YIELD * RETURN *") {
    yields(ast.ShowRoles(
      withUsers = false,
      showAll = false,
      Some(Left((yieldClause(returnAllItems), Some(returnClause(returnAllItems)))))
    ))
  }

  test("SHOW POPULATED ROLE WITH USER YIELD * RETURN *") {
    yields(ast.ShowRoles(
      withUsers = true,
      showAll = false,
      Some(Left((yieldClause(returnAllItems), Some(returnClause(returnAllItems)))))
    ))
  }

  test("SHOW ROLES WITH USERS YIELD * LIMIT 10 WHERE foo='bar' RETURN some,columns LIMIT 10") {
    yields(ast.ShowRoles(
      withUsers = true,
      showAll = true,
      Some(Left((
        yieldClause(
          returnAllItems,
          limit = Some(limit(10)),
          where = Some(where(equals(varFor("foo"), literalString("bar"))))
        ),
        Some(returnClause(
          returnItems(variableReturnItem("some"), variableReturnItem("columns")),
          limit = Some(limit(10))
        ))
      )))
    ))
  }

  test("SHOW POPULATED ROLES YIELD role ORDER BY role SKIP -1") {
    yields(_ =>
      ast.ShowRoles(
        withUsers = false,
        showAll = false,
        Some(Left((
          yieldClause(
            returnItems(variableReturnItem(roleString)),
            Some(orderBy(sortItem(varFor(roleString)))),
            Some(skip(-1))
          ),
          None
        )))
      )(pos)
    )
  }

  test("SHOW POPULATED ROLES YIELD role ORDER BY role LIMIT -1") {
    yields(_ =>
      ast.ShowRoles(
        withUsers = false,
        showAll = false,
        Some(Left((
          yieldClause(
            returnItems(variableReturnItem(roleString)),
            Some(orderBy(sortItem(varFor(roleString)))),
            limit = Some(limit(-1))
          ),
          None
        )))
      )(pos)
    )
  }

  test("SHOW POPULATED ROLES YIELD role ORDER BY role SKIP -1*4 + 2") {
    failsToParse
  }

  test("SHOW ROLE role") {
    assertFailsWithMessage(
      testName,
      "Invalid input '': expected \",\", \"PRIVILEGE\" or \"PRIVILEGES\" (line 1, column 15 (offset: 14))"
    )
  }

  test("SHOW ROLES YIELD (123 + xyz)") {
    failsToParse
  }

  test("SHOW ROLES YIELD (123 + xyz) AS foo") {
    failsToParse
  }

  test("SHOW ALL ROLES YIELD role RETURN") {
    assertFailsWithMessage(
      testName,
      "Invalid input '': expected \"*\", \"DISTINCT\" or an expression (line 1, column 33 (offset: 32))"
    )
  }

  test("SHOW ROLES WITH USER user") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'user': expected "WHERE", "YIELD" or <EOF> (line 1, column 22 (offset: 21))"""
    )
  }

  test("SHOW POPULATED ROLES YIELD *,blah RETURN role") {
    val exceptionMessage =
      s"""Invalid input ',': expected
         |  "LIMIT"
         |  "ORDER"
         |  "RETURN"
         |  "SKIP"
         |  "WHERE"
         |  <EOF> (line 1, column 29 (offset: 28))""".stripMargin
    assertFailsWithMessage(testName, exceptionMessage)
  }

  //  Creating role

  test("CREATE ROLE foo") {
    yields(ast.CreateRole(literalFoo, None, ast.IfExistsThrowError))
  }

  test("CREATE ROLE $foo") {
    yields(ast.CreateRole(paramFoo, None, ast.IfExistsThrowError))
  }

  test("CREATE ROLE `fo!$o`") {
    yields(_ => ast.CreateRole(literal("fo!$o"), None, ast.IfExistsThrowError)(pos))
  }

  test("CREATE ROLE ``") {
    yields(ast.CreateRole(literalEmpty, None, ast.IfExistsThrowError))
  }

  test("CREATE ROLE foo AS COPY OF bar") {
    yields(ast.CreateRole(literalFoo, Some(literalBar), ast.IfExistsThrowError))
  }

  test("CREATE ROLE foo AS COPY OF $bar") {
    yields(ast.CreateRole(literalFoo, Some(stringParam("bar")), ast.IfExistsThrowError))
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

  test("CREATE ROLE \"foo\"") {
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
    assertFailsWithMessage(
      testName,
      "Invalid input '': expected a parameter or an identifier (line 1, column 23 (offset: 22))"
    )
  }

  test("CREATE ROLE foo AS COPY OF") {
    assertFailsWithMessage(
      testName,
      "Invalid input '': expected a parameter or an identifier (line 1, column 27 (offset: 26))"
    )
  }

  test("CREATE ROLE foo IF NOT EXISTS AS COPY OF") {
    assertFailsWithMessage(
      testName,
      "Invalid input '': expected a parameter or an identifier (line 1, column 41 (offset: 40))"
    )
  }

  test("CREATE OR REPLACE ROLE foo AS COPY OF") {
    assertFailsWithMessage(
      testName,
      "Invalid input '': expected a parameter or an identifier (line 1, column 38 (offset: 37))"
    )
  }

  test("CREATE ROLE foo UNION CREATE ROLE foo2") {
    assertFailsWithMessage(
      testName,
      "Invalid input 'UNION': expected \"AS\", \"IF\" or <EOF> (line 1, column 17 (offset: 16))"
    )
  }

  // Renaming role

  test("RENAME ROLE foo TO bar") {
    yields(ast.RenameRole(literalFoo, literalBar, ifExists = false))
  }

  test("RENAME ROLE foo TO $bar") {
    yields(ast.RenameRole(literalFoo, stringParam("bar"), ifExists = false))
  }

  test("RENAME ROLE $foo TO bar") {
    yields(ast.RenameRole(stringParam("foo"), literalBar, ifExists = false))
  }

  test("RENAME ROLE $foo TO $bar") {
    yields(ast.RenameRole(stringParam("foo"), stringParam("bar"), ifExists = false))
  }

  test("RENAME ROLE foo IF EXISTS TO bar") {
    yields(ast.RenameRole(literalFoo, literalBar, ifExists = true))
  }

  test("RENAME ROLE foo IF EXISTS TO $bar") {
    yields(ast.RenameRole(literalFoo, stringParam("bar"), ifExists = true))
  }

  test("RENAME ROLE $foo IF EXISTS TO bar") {
    yields(ast.RenameRole(stringParam("foo"), literalBar, ifExists = true))
  }

  test("RENAME ROLE $foo IF EXISTS TO $bar") {
    yields(ast.RenameRole(stringParam("foo"), stringParam("bar"), ifExists = true))
  }

  test("RENAME ROLE foo TO ``") {
    yields(ast.RenameRole(literalFoo, literalEmpty, ifExists = false))
  }

  test("RENAME ROLE `` TO bar") {
    yields(ast.RenameRole(literalEmpty, literalBar, ifExists = false))
  }

  test("RENAME ROLE foo TO") {
    assertFailsWithMessage(
      testName,
      "Invalid input '': expected a parameter or an identifier (line 1, column 19 (offset: 18))"
    )
  }

  test("RENAME ROLE TO bar") {
    assertFailsWithMessage(testName, "Invalid input 'bar': expected \"IF\" or \"TO\" (line 1, column 16 (offset: 15))")
  }

  test("RENAME ROLE TO") {
    assertFailsWithMessage(testName, "Invalid input '': expected \"IF\" or \"TO\" (line 1, column 15 (offset: 14))")
  }

  test("RENAME ROLE foo SET NAME TO bar") {
    assertFailsWithMessage(testName, "Invalid input 'SET': expected \"IF\" or \"TO\" (line 1, column 17 (offset: 16))")
  }

  test("RENAME ROLE foo SET NAME bar") {
    assertFailsWithMessage(testName, "Invalid input 'SET': expected \"IF\" or \"TO\" (line 1, column 17 (offset: 16))")
  }

  test("ALTER ROLE foo SET NAME bar") {
    assertFailsWithMessage(
      testName,
      """Invalid input 'ROLE': expected
        |  "ALIAS"
        |  "CURRENT"
        |  "DATABASE"
        |  "SERVER"
        |  "USER" (line 1, column 7 (offset: 6))""".stripMargin
    )
  }

  test("RENAME ROLE foo IF EXIST TO bar") {
    assertFailsWithMessage(testName, "Invalid input 'EXIST': expected \"EXISTS\" (line 1, column 20 (offset: 19))")
  }

  test("RENAME ROLE foo IF NOT EXISTS TO bar") {
    assertFailsWithMessage(testName, "Invalid input 'NOT': expected \"EXISTS\" (line 1, column 20 (offset: 19))")
  }

  test("RENAME ROLE foo TO bar IF EXISTS") {
    assertFailsWithMessage(testName, "Invalid input 'IF': expected <EOF> (line 1, column 24 (offset: 23))")
  }

  test("RENAME IF EXISTS ROLE foo TO bar") {
    assertFailsWithMessage(
      testName,
      "Invalid input 'IF': expected \"ROLE\", \"SERVER\" or \"USER\" (line 1, column 8 (offset: 7))"
    )
  }

  test("RENAME OR REPLACE ROLE foo TO bar") {
    assertFailsWithMessage(
      testName,
      "Invalid input 'OR': expected \"ROLE\", \"SERVER\" or \"USER\" (line 1, column 8 (offset: 7))"
    )
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
    assertFailsWithMessage(
      testName,
      "Invalid input '': expected a parameter or an identifier (line 1, column 10 (offset: 9))"
    )
  }

  test("DROP ROLE  IF EXISTS") {
    failsToParse
  }

  test("DROP ROLE foo IF NOT EXISTS") {
    failsToParse
  }

  //  Granting/revoking roles to/from users

  private type grantOrRevokeRoleFunc = (Seq[String], Seq[String]) => InputPosition => ast.AdministrationCommand

  private def grantRole(r: Seq[String], u: Seq[String]): InputPosition => ast.AdministrationCommand =
    ast.GrantRolesToUsers(r.map(Left(_)), u.map(Left(_)))

  private def revokeRole(r: Seq[String], u: Seq[String]): InputPosition => ast.AdministrationCommand =
    ast.RevokeRolesFromUsers(r.map(Left(_)), u.map(Left(_)))

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
            yields(func(Seq("$f00", "bar"), Seq("abc", s"$$a&c")))
          }

          // Should fail to parse if not following the pattern $command $roleKeyword role(s) $preposition user(s)

          test(s"$verb $roleKeyword") {
            val expected = roleKeyword match {
              case "ROLE" => """Invalid input '': expected "MANAGEMENT", a parameter or an identifier"""
              case _      => """Invalid input '': expected a parameter or an identifier"""
            }
            assertFailsWithMessageStart(testName, expected)
          }

          test(s"$verb $roleKeyword foo") {
            assertFailsWithMessageStart(testName, s"""Invalid input '': expected "," or "$preposition"""")
          }

          test(s"$verb $roleKeyword foo $preposition") {
            assertFailsWithMessageStart(testName, "Invalid input '': expected a parameter or an identifier")
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
  }

  test(s"GRANT ROLE $$a TO $$x") {
    yields(ast.GrantRolesToUsers(Seq(stringParam("a")), Seq(stringParam("x"))))
  }

  test(s"REVOKE ROLE $$a FROM $$x") {
    yields(ast.RevokeRolesFromUsers(Seq(stringParam("a")), Seq(stringParam("x"))))
  }

  test(s"GRANT ROLES a, $$b, $$c TO $$x, y, z") {
    yields(ast.GrantRolesToUsers(
      Seq(literal("a"), stringParam("b"), stringParam("c")),
      Seq(stringParam("x"), literal("y"), literal("z"))
    ))
  }

  test(s"REVOKE ROLES a, $$b, $$c FROM $$x, y, z") {
    yields(ast.RevokeRolesFromUsers(
      Seq(literal("a"), stringParam("b"), stringParam("c")),
      Seq(stringParam("x"), literal("y"), literal("z"))
    ))
  }

  // ROLE[S] TO USER only have GRANT and REVOKE and not DENY

  test(s"DENY ROLE foo TO abc") {
    assertFailsWithMessageStart(testName, """Invalid input 'foo': expected "MANAGEMENT"""")
  }

  test("DENY ROLES foo TO abc") {
    assertFailsWithMessageStart(
      testName,
      """Invalid input 'ROLES': expected
        |  "ACCESS"
        |  "ALIAS"
        |  "ALL"
        |  "ALTER"
        |  "ASSIGN"
        |  "COMPOSITE"
        |  "CONSTRAINT"
        |  "CONSTRAINTS"
        |  "CREATE"""".stripMargin
    )
  }
}
