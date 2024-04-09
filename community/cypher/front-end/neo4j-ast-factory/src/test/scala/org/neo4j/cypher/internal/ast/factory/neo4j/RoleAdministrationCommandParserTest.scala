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
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.Antlr
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsing.JavaCc
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.exceptions.SyntaxException

class RoleAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {
  private val roleString = "role"

  //  Showing roles

  Seq("ROLES", "ROLE").foreach(roleKeyword => {
    test(s"SHOW $roleKeyword") {
      parsesTo[Statements](ast.ShowRoles(withUsers = false, showAll = true, None)(pos))
    }

    test(s"SHOW ALL $roleKeyword") {
      parsesTo[Statements](ast.ShowRoles(withUsers = false, showAll = true, None)(pos))
    }

    test(s"SHOW POPULATED $roleKeyword") {
      parsesTo[Statements](ast.ShowRoles(withUsers = false, showAll = false, None)(pos))
    }

    Seq("USERS", "USER").foreach(userKeyword => {
      test(s"SHOW $roleKeyword WITH $userKeyword") {
        parsesTo[Statements](ast.ShowRoles(withUsers = true, showAll = true, None)(pos))
      }

      test(s"SHOW ALL $roleKeyword WITH $userKeyword") {
        parsesTo[Statements](ast.ShowRoles(withUsers = true, showAll = true, None)(pos))
      }

      test(s"SHOW POPULATED $roleKeyword WITH $userKeyword") {
        parsesTo[Statements](ast.ShowRoles(withUsers = true, showAll = false, None)(pos))
      }

    })

  })

  test("USE neo4j SHOW ROLES") {
    parsesTo[Statements](ast.ShowRoles(withUsers = false, showAll = true, None)(pos))
  }

  test("USE GRAPH SYSTEM SHOW ROLES") {
    parsesTo[Statements](ast.ShowRoles(withUsers = false, showAll = true, None)(pos))
  }

  test("SHOW ALL ROLES YIELD role") {
    parsesTo[Statements](
      ast.ShowRoles(
        withUsers = false,
        showAll = true,
        Some(Left((yieldClause(returnItems(variableReturnItem(roleString))), None)))
      )(pos)
    )
  }

  test("SHOW ALL ROLE YIELD role") {
    parsesTo[Statements](
      ast.ShowRoles(
        withUsers = false,
        showAll = true,
        Some(Left((yieldClause(returnItems(variableReturnItem(roleString))), None)))
      )(pos)
    )
  }

  test("SHOW ALL ROLES WHERE role='PUBLIC'") {
    parsesTo[Statements](
      ast.ShowRoles(
        withUsers = false,
        showAll = true,
        Some(Right(where(equals(varFor(roleString), literalString("PUBLIC")))))
      )(pos)
    )
  }

  test("SHOW ALL ROLE WHERE role='PUBLIC'") {
    parsesTo[Statements](
      ast.ShowRoles(
        withUsers = false,
        showAll = true,
        Some(Right(where(equals(varFor(roleString), literalString("PUBLIC")))))
      )(pos)
    )
  }

  test("SHOW ALL ROLES YIELD role RETURN role") {
    parsesTo[Statements](ast.ShowRoles(
      withUsers = false,
      showAll = true,
      Some(Left((
        yieldClause(returnItems(variableReturnItem(roleString))),
        Some(returnClause(returnItems(variableReturnItem(roleString))))
      )))
    )(pos))
  }

  test("SHOW ALL ROLES YIELD return, return RETURN return") {
    parsesTo[Statements](ast.ShowRoles(
      withUsers = false,
      showAll = true,
      Some(Left((
        yieldClause(returnItems(variableReturnItem("return"), variableReturnItem("return"))),
        Some(returnClause(returnItems(variableReturnItem("return"))))
      )))
    )(pos))
  }

  test("SHOW POPULATED ROLES YIELD role WHERE role='PUBLIC' RETURN role") {
    parsesTo[Statements](ast.ShowRoles(
      withUsers = false,
      showAll = false,
      Some(Left((
        yieldClause(
          returnItems(variableReturnItem(roleString)),
          where = Some(where(equals(varFor(roleString), literalString("PUBLIC"))))
        ),
        Some(returnClause(returnItems(variableReturnItem(roleString))))
      )))
    )(pos))
  }

  test("SHOW POPULATED ROLES YIELD * RETURN *") {
    parsesTo[Statements](ast.ShowRoles(
      withUsers = false,
      showAll = false,
      Some(Left((yieldClause(returnAllItems), Some(returnClause(returnAllItems)))))
    )(pos))
  }

  test("SHOW POPULATED ROLE WITH USER YIELD * RETURN *") {
    parsesTo[Statements](ast.ShowRoles(
      withUsers = true,
      showAll = false,
      Some(Left((yieldClause(returnAllItems), Some(returnClause(returnAllItems)))))
    )(pos))
  }

  test("SHOW ROLES WITH USERS YIELD * LIMIT 10 WHERE foo='bar' RETURN some,columns LIMIT 10") {
    parsesTo[Statements](ast.ShowRoles(
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
    )(pos))
  }

  test("SHOW POPULATED ROLES YIELD role ORDER BY role SKIP -1") {
    parsesTo[Statements](
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
    parsesTo[Statements](
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
    failsParsing[Statements]
  }

  // TODO Missing comma in message
  test("SHOW ROLE role") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected \",\", \"PRIVILEGE\" or \"PRIVILEGES\" (line 1, column 15 (offset: 14))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Missing 'PRIVILEGE', 'PRIVILEGES' at '' (line 1, column 15 (offset: 14))
          |"SHOW ROLE role"
          |               ^""".stripMargin
      ))
  }

  test("SHOW ROLES YIELD (123 + xyz)") {
    failsParsing[Statements]
  }

  test("SHOW ROLES YIELD (123 + xyz) AS foo") {
    failsParsing[Statements]
  }

  test("SHOW ALL ROLES YIELD role RETURN") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected \"*\", \"DISTINCT\" or an expression (line 1, column 33 (offset: 32))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input '': expected 'DISTINCT', '*', an expression (line 1, column 33 (offset: 32))
          |"SHOW ALL ROLES YIELD role RETURN"
          |                                 ^""".stripMargin
      ))
  }

  // TODO Check Message, potential loss of information
  test("SHOW ROLES WITH USER user") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'user': expected "WHERE", "YIELD" or <EOF> (line 1, column 22 (offset: 21))"""
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Extraneous input 'user': expected ';', <EOF> (line 1, column 22 (offset: 21))
          |"SHOW ROLES WITH USER user"
          |                      ^""".stripMargin
      ))
  }

  // TODO Check Message, potential loss of information
  test("SHOW POPULATED ROLES YIELD *,blah RETURN role") {
    val exceptionMessage =
      s"""Invalid input ',': expected
         |  "LIMIT"
         |  "ORDER"
         |  "RETURN"
         |  "SKIP"
         |  "WHERE"
         |  <EOF> (line 1, column 29 (offset: 28))""".stripMargin
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart(exceptionMessage))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input ',': expected ';', <EOF> (line 1, column 29 (offset: 28))
          |"SHOW POPULATED ROLES YIELD *,blah RETURN role"
          |                             ^""".stripMargin
      ))
  }

  //  Creating role

  test("CREATE ROLE foo") {
    parsesTo[Statements](ast.CreateRole(literalFoo, None, ast.IfExistsThrowError)(pos))
  }

  test("CREATE ROLE $foo") {
    parsesTo[Statements](ast.CreateRole(paramFoo, None, ast.IfExistsThrowError)(pos))
  }

  test("CREATE ROLE `fo!$o`") {
    parsesTo[Statements](ast.CreateRole(literal("fo!$o"), None, ast.IfExistsThrowError)(pos))
  }

  test("CREATE ROLE ``") {
    parsesTo[Statements](ast.CreateRole(literalEmpty, None, ast.IfExistsThrowError)(pos))
  }

  test("CREATE ROLE foo AS COPY OF bar") {
    parsesTo[Statements](ast.CreateRole(literalFoo, Some(literalBar), ast.IfExistsThrowError)(pos))
  }

  test("CREATE ROLE foo AS COPY OF $bar") {
    parsesTo[Statements](ast.CreateRole(literalFoo, Some(stringParam("bar")), ast.IfExistsThrowError)(pos))
  }

  test("CREATE ROLE foo AS COPY OF ``") {
    parsesTo[Statements](ast.CreateRole(literalFoo, Some(literalEmpty), ast.IfExistsThrowError)(pos))
  }

  test("CREATE ROLE `` AS COPY OF bar") {
    parsesTo[Statements](ast.CreateRole(literalEmpty, Some(literalBar), ast.IfExistsThrowError)(pos))
  }

  test("CREATE ROLE foo IF NOT EXISTS") {
    parsesTo[Statements](ast.CreateRole(literalFoo, None, ast.IfExistsDoNothing)(pos))
  }

  test("CREATE ROLE foo IF NOT EXISTS AS COPY OF bar") {
    parsesTo[Statements](ast.CreateRole(literalFoo, Some(literalBar), ast.IfExistsDoNothing)(pos))
  }

  test("CREATE OR REPLACE ROLE foo") {
    parsesTo[Statements](ast.CreateRole(literalFoo, None, ast.IfExistsReplace)(pos))
  }

  test("CREATE OR REPLACE ROLE foo AS COPY OF bar") {
    parsesTo[Statements](ast.CreateRole(literalFoo, Some(literalBar), ast.IfExistsReplace)(pos))
  }

  test("CREATE OR REPLACE ROLE foo IF NOT EXISTS") {
    parsesTo[Statements](ast.CreateRole(literalFoo, None, ast.IfExistsInvalidSyntax)(pos))
  }

  test("CREATE OR REPLACE ROLE foo IF NOT EXISTS AS COPY OF bar") {
    parsesTo[Statements](ast.CreateRole(literalFoo, Some(literalBar), ast.IfExistsInvalidSyntax)(pos))
  }

  test("CREATE ROLE \"foo\"") {
    failsParsing[Statements]
  }

  test("CREATE ROLE f%o") {
    failsParsing[Statements]
  }

  test("CREATE ROLE  IF NOT EXISTS") {
    failsParsing[Statements]
  }

  test("CREATE ROLE foo IF EXISTS") {
    failsParsing[Statements]
  }

  test("CREATE OR REPLACE ROLE ") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected a parameter or an identifier (line 1, column 23 (offset: 22))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input '': expected an identifier, '$' (line 1, column 23 (offset: 22))
          |"CREATE OR REPLACE ROLE"
          |                       ^""".stripMargin
      ))
  }

  test("CREATE ROLE foo AS COPY OF") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected a parameter or an identifier (line 1, column 27 (offset: 26))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input '': expected an identifier, '$' (line 1, column 27 (offset: 26))
          |"CREATE ROLE foo AS COPY OF"
          |                           ^""".stripMargin
      ))
  }

  test("CREATE ROLE foo IF NOT EXISTS AS COPY OF") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected a parameter or an identifier (line 1, column 41 (offset: 40))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input '': expected an identifier, '$' (line 1, column 41 (offset: 40))
          |"CREATE ROLE foo IF NOT EXISTS AS COPY OF"
          |                                         ^""".stripMargin
      ))
  }

  test("CREATE OR REPLACE ROLE foo AS COPY OF") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected a parameter or an identifier (line 1, column 38 (offset: 37))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input '': expected an identifier, '$' (line 1, column 38 (offset: 37))
          |"CREATE OR REPLACE ROLE foo AS COPY OF"
          |                                      ^""".stripMargin
      ))
  }

  // TODO Check Message, potential loss of information
  test("CREATE ROLE foo UNION CREATE ROLE foo2") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'UNION': expected \"AS\", \"IF\" or <EOF> (line 1, column 17 (offset: 16))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input 'UNION': expected ';', <EOF> (line 1, column 17 (offset: 16))
          |"CREATE ROLE foo UNION CREATE ROLE foo2"
          |                 ^""".stripMargin
      ))
  }

  // Renaming role

  test("RENAME ROLE foo TO bar") {
    parsesTo[Statements](ast.RenameRole(literalFoo, literalBar, ifExists = false)(pos))
  }

  test("RENAME ROLE foo TO $bar") {
    parsesTo[Statements](ast.RenameRole(literalFoo, stringParam("bar"), ifExists = false)(pos))
  }

  test("RENAME ROLE $foo TO bar") {
    parsesTo[Statements](ast.RenameRole(stringParam("foo"), literalBar, ifExists = false)(pos))
  }

  test("RENAME ROLE $foo TO $bar") {
    parsesTo[Statements](ast.RenameRole(stringParam("foo"), stringParam("bar"), ifExists = false)(pos))
  }

  test("RENAME ROLE foo IF EXISTS TO bar") {
    parsesTo[Statements](ast.RenameRole(literalFoo, literalBar, ifExists = true)(pos))
  }

  test("RENAME ROLE foo IF EXISTS TO $bar") {
    parsesTo[Statements](ast.RenameRole(literalFoo, stringParam("bar"), ifExists = true)(pos))
  }

  test("RENAME ROLE $foo IF EXISTS TO bar") {
    parsesTo[Statements](ast.RenameRole(stringParam("foo"), literalBar, ifExists = true)(pos))
  }

  test("RENAME ROLE $foo IF EXISTS TO $bar") {
    parsesTo[Statements](ast.RenameRole(stringParam("foo"), stringParam("bar"), ifExists = true)(pos))
  }

  test("RENAME ROLE foo TO ``") {
    parsesTo[Statements](ast.RenameRole(literalFoo, literalEmpty, ifExists = false)(pos))
  }

  test("RENAME ROLE `` TO bar") {
    parsesTo[Statements](ast.RenameRole(literalEmpty, literalBar, ifExists = false)(pos))
  }

  test("RENAME ROLE foo TO") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected a parameter or an identifier (line 1, column 19 (offset: 18))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input '': expected an identifier, '$' (line 1, column 19 (offset: 18))
          |"RENAME ROLE foo TO"
          |                   ^""".stripMargin
      ))
  }

  test("RENAME ROLE TO bar") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'bar': expected \"IF\" or \"TO\" (line 1, column 16 (offset: 15))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input 'bar': expected 'IF', 'TO' (line 1, column 16 (offset: 15))
          |"RENAME ROLE TO bar"
          |                ^""".stripMargin
      ))
  }

  test("RENAME ROLE TO") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected \"IF\" or \"TO\" (line 1, column 15 (offset: 14))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input '': expected 'IF', 'TO' (line 1, column 15 (offset: 14))
          |"RENAME ROLE TO"
          |               ^""".stripMargin
      ))
  }

  test("RENAME ROLE foo SET NAME TO bar") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'SET': expected \"IF\" or \"TO\" (line 1, column 17 (offset: 16))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input 'SET': expected 'IF', 'TO' (line 1, column 17 (offset: 16))
          |"RENAME ROLE foo SET NAME TO bar"
          |                 ^""".stripMargin
      ))
  }

  test("RENAME ROLE foo SET NAME bar") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'SET': expected \"IF\" or \"TO\" (line 1, column 17 (offset: 16))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input 'SET': expected 'IF', 'TO' (line 1, column 17 (offset: 16))
          |"RENAME ROLE foo SET NAME bar"
          |                 ^""".stripMargin
      ))
  }

  test("ALTER ROLE foo SET NAME bar") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        """Invalid input 'ROLE': expected
          |  "ALIAS"
          |  "CURRENT"
          |  "DATABASE"
          |  "SERVER"
          |  "USER" (line 1, column 7 (offset: 6))""".stripMargin
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input 'ROLE': expected 'ALIAS', 'CURRENT', 'DATABASE', 'USER', 'SERVER' (line 1, column 7 (offset: 6))
          |"ALTER ROLE foo SET NAME bar"
          |       ^""".stripMargin
      ))
  }

  test("RENAME ROLE foo IF EXIST TO bar") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'EXIST': expected \"EXISTS\" (line 1, column 20 (offset: 19))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input 'EXIST': expected 'EXISTS' (line 1, column 20 (offset: 19))
          |"RENAME ROLE foo IF EXIST TO bar"
          |                    ^""".stripMargin
      ))
  }

  test("RENAME ROLE foo IF NOT EXISTS TO bar") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'NOT': expected \"EXISTS\" (line 1, column 20 (offset: 19))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Extraneous input 'NOT': expected 'EXISTS' (line 1, column 20 (offset: 19))
          |"RENAME ROLE foo IF NOT EXISTS TO bar"
          |                    ^""".stripMargin
      ))
  }

  test("RENAME ROLE foo TO bar IF EXISTS") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart("Invalid input 'IF': expected <EOF> (line 1, column 24 (offset: 23))"))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input 'IF': expected ';', <EOF> (line 1, column 24 (offset: 23))
          |"RENAME ROLE foo TO bar IF EXISTS"
          |                        ^""".stripMargin
      ))
  }

  test("RENAME IF EXISTS ROLE foo TO bar") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'IF': expected \"ROLE\", \"SERVER\" or \"USER\" (line 1, column 8 (offset: 7))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input 'IF': expected 'ROLE', 'SERVER', 'USER' (line 1, column 8 (offset: 7))
          |"RENAME IF EXISTS ROLE foo TO bar"
          |        ^""".stripMargin
      ))
  }

  test("RENAME OR REPLACE ROLE foo TO bar") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input 'OR': expected \"ROLE\", \"SERVER\" or \"USER\" (line 1, column 8 (offset: 7))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input 'OR': expected 'ROLE', 'SERVER', 'USER' (line 1, column 8 (offset: 7))
          |"RENAME OR REPLACE ROLE foo TO bar"
          |        ^""".stripMargin
      ))
  }

  //  Dropping role

  test("DROP ROLE foo") {
    parsesTo[Statements](ast.DropRole(literalFoo, ifExists = false)(pos))
  }

  test("DROP ROLE $foo") {
    parsesTo[Statements](ast.DropRole(paramFoo, ifExists = false)(pos))
  }

  test("DROP ROLE ``") {
    parsesTo[Statements](ast.DropRole(literalEmpty, ifExists = false)(pos))
  }

  test("DROP ROLE foo IF EXISTS") {
    parsesTo[Statements](ast.DropRole(literalFoo, ifExists = true)(pos))
  }

  test("DROP ROLE `` IF EXISTS") {
    parsesTo[Statements](ast.DropRole(literalEmpty, ifExists = true)(pos))
  }

  test("DROP ROLE ") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
        "Invalid input '': expected a parameter or an identifier (line 1, column 10 (offset: 9))"
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input '': expected an identifier, '$' (line 1, column 10 (offset: 9))
          |"DROP ROLE"
          |          ^""".stripMargin
      ))
  }

  test("DROP ROLE  IF EXISTS") {
    failsParsing[Statements]
  }

  test("DROP ROLE foo IF NOT EXISTS") {
    failsParsing[Statements]
  }

  //  Granting/revoking roles to/from users

  private type grantOrRevokeRoleFunc = (Seq[String], Seq[String]) => InputPosition => ast.AdministrationCommand

  private def grantRole(r: Seq[String], u: Seq[String]): InputPosition => ast.AdministrationCommand =
    ast.GrantRolesToUsers(
      r.map(roleName => literalString(roleName)),
      u.map(userName => literalString(userName))
    )

  private def revokeRole(r: Seq[String], u: Seq[String]): InputPosition => ast.AdministrationCommand =
    ast.RevokeRolesFromUsers(
      r.map(roleName => literalString(roleName)),
      u.map(userName => literalString(userName))
    )

  Seq("ROLE", "ROLES").foreach {
    roleKeyword =>
      Seq(
        ("GRANT", "TO", grantRole: grantOrRevokeRoleFunc),
        ("REVOKE", "FROM", revokeRole: grantOrRevokeRoleFunc)
      ).foreach {
        case (verb: String, preposition: String, func: grantOrRevokeRoleFunc) =>
          test(s"$verb $roleKeyword foo $preposition abc") {
            yields[Statements](func(Seq("foo"), Seq("abc")))
          }

          test(s"$verb $roleKeyword foo, bar $preposition abc") {
            yields[Statements](func(Seq("foo", "bar"), Seq("abc")))
          }

          test(s"$verb $roleKeyword foo $preposition abc, def") {
            yields[Statements](func(Seq("foo"), Seq("abc", "def")))
          }

          test(s"$verb $roleKeyword foo,bla,roo $preposition bar, baz,abc,  def") {
            yields[Statements](func(Seq("foo", "bla", "roo"), Seq("bar", "baz", "abc", "def")))
          }

          test(s"$verb $roleKeyword `fo:o` $preposition bar") {
            yields[Statements](func(Seq("fo:o"), Seq("bar")))
          }

          test(s"$verb $roleKeyword foo $preposition `b:ar`") {
            yields[Statements](func(Seq("foo"), Seq("b:ar")))
          }

          test(s"$verb $roleKeyword `$$f00`,bar $preposition abc,`$$a&c`") {
            yields[Statements](func(Seq("$f00", "bar"), Seq("abc", s"$$a&c")))
          }

          // Should fail to parse if not following the pattern $command $roleKeyword role(s) $preposition user(s)

          test(s"$verb $roleKeyword") {
            val javaCcExpected = roleKeyword match {
              case "ROLE" => """Invalid input '': expected "MANAGEMENT", a parameter or an identifier"""
              case _      => """Invalid input '': expected a parameter or an identifier"""
            }

            // TODO Loss of information on ROLE case
            val antlrExpected = roleKeyword match {
              case "ROLE" =>
                """No viable alternative"""
              case _ =>
                """Mismatched input '': expected an identifier, '$'""".stripMargin
            }

            testName should notParse[Statements]
              .parseIn(JavaCc)(_.withMessageStart(javaCcExpected))
              .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(antlrExpected))
          }

          test(s"$verb $roleKeyword foo") {
            testName should notParse[Statements]
              .parseIn(JavaCc)(_.withMessageStart(s"""Invalid input '': expected "," or "$preposition""""))
              .parseIn(Antlr)(
                _.throws[SyntaxException].withMessageStart(s"""Mismatched input '': expected ',', '$preposition'""")
              )
          }

          test(s"$verb $roleKeyword foo $preposition") {
            testName should notParse[Statements]
              .parseIn(JavaCc)(_.withMessageStart("Invalid input '': expected a parameter or an identifier"))
              .parseIn(Antlr)(_.throws[SyntaxException].withMessageStart(
                """Mismatched input '': expected an identifier, '$'""".stripMargin
              ))
          }

          test(s"$verb $roleKeyword $preposition abc") {
            failsParsing[Statements]
          }

          // Should fail to parse when invalid user or role name

          test(s"$verb $roleKeyword fo:o $preposition bar") {
            failsParsing[Statements]
          }

          test(s"$verb $roleKeyword foo $preposition b:ar") {
            failsParsing[Statements]
          }
      }

      // Should fail to parse when mixing TO and FROM

      test(s"GRANT $roleKeyword foo FROM abc") {
        failsParsing[Statements]
      }

      test(s"REVOKE $roleKeyword foo TO abc") {
        failsParsing[Statements]
      }
  }

  test(s"GRANT ROLE $$a TO $$x") {
    yields[Statements](ast.GrantRolesToUsers(Seq(stringParam("a")), Seq(stringParam("x"))))
  }

  test(s"REVOKE ROLE $$a FROM $$x") {
    yields[Statements](ast.RevokeRolesFromUsers(Seq(stringParam("a")), Seq(stringParam("x"))))
  }

  test(s"GRANT ROLES a, $$b, $$c TO $$x, y, z") {
    yields[Statements](ast.GrantRolesToUsers(
      Seq(literal("a"), stringParam("b"), stringParam("c")),
      Seq(stringParam("x"), literal("y"), literal("z"))
    ))
  }

  test(s"REVOKE ROLES a, $$b, $$c FROM $$x, y, z") {
    yields[Statements](ast.RevokeRolesFromUsers(
      Seq(literal("a"), stringParam("b"), stringParam("c")),
      Seq(stringParam("x"), literal("y"), literal("z"))
    ))
  }

  // ROLE[S] TO USER only have GRANT and REVOKE and not DENY

  test(s"DENY ROLE foo TO abc") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart("""Invalid input 'foo': expected "MANAGEMENT""""))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input 'foo': expected 'MANAGEMENT' (line 1, column 11 (offset: 10))
          |"DENY ROLE foo TO abc"
          |           ^""".stripMargin
      ))
  }

  test("DENY ROLES foo TO abc") {
    testName should notParse[Statements]
      .parseIn(JavaCc)(_.withMessageStart(
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
      ))
      .parseIn(Antlr)(_.throws[SyntaxException].withMessage(
        """Mismatched input 'ROLES': expected 'IMMUTABLE', 'ALL', 'CREATE', 'ACCESS', 'START', 'STOP', 'INDEX', 'INDEXES', 'CONSTRAINT', 'CONSTRAINTS', 'NAME', 'TRANSACTION', 'TERMINATE', 'ALTER', 'ASSIGN', 'ALIAS', 'COMPOSITE', 'DATABASE', 'PRIVILEGE', 'ROLE', 'SERVER', 'USER', 'EXECUTE', 'RENAME', 'IMPERSONATE', 'DROP', 'LOAD', 'DELETE', 'MERGE', 'TRAVERSE', 'MATCH', 'READ', 'REMOVE', 'SET', 'SHOW', 'WRITE' (line 1, column 6 (offset: 5))
          |"DENY ROLES foo TO abc"
          |      ^""".stripMargin
      ))
  }
}
