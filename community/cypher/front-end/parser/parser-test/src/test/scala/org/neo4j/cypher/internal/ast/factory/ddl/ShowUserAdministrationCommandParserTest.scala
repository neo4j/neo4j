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
package org.neo4j.cypher.internal.ast.factory.ddl

import org.neo4j.cypher.internal.ast.ShowCurrentUserClause
import org.neo4j.cypher.internal.ast.ShowUsers
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5

class ShowUserAdministrationCommandParserTest extends UserAdministrationCommandParserTestBase {

  //  Show users

  test("SHOW USERS") {
    parsesTo[Statements](ShowUsers(None, withAuth = false, asCommands = false)(pos))
  }

  test("SHOW USER") {
    parsesTo[Statements](ShowUsers(None, withAuth = false, asCommands = false)(pos))
  }

  test("USE system SHOW USERS") {
    def expected(resolveStrictly: Boolean) = {
      ShowUsers(None, withAuth = false, asCommands = false)(pos).withGraph(Some(use(List("system"), resolveStrictly)))
    }

    parsesIn[Statement] {
      case Cypher5 => _.toAst(expected(resolveStrictly = false))
      case _       => _.toAst(expected(resolveStrictly = true))
    }
  }

  test("SHOW USERS WHERE user = 'GRANTED'") {
    parsesTo[Statements](ShowUsers(
      Some(Right(where(equals(varUser, grantedString)))),
      withAuth = false,
      asCommands = false
    )(pos))
  }

  test("SHOW USER WHERE user = 'GRANTED'") {
    parsesTo[Statements](ShowUsers(
      Some(Right(where(equals(varUser, grantedString)))),
      withAuth = false,
      asCommands = false
    )(pos))
  }

  test("SHOW USERS WHERE user = 'GRANTED' AND action = 'match'") {
    val accessPredicate = equals(varUser, grantedString)
    val matchPredicate = equals(varFor(actionString), literalString("match"))
    parsesTo[Statements](ShowUsers(
      Some(Right(where(and(accessPredicate, matchPredicate)))),
      withAuth = false,
      asCommands = false
    )(pos))
  }

  test("SHOW USERS WHERE user = 'GRANTED' OR action = 'match'") {
    val accessPredicate = equals(varUser, grantedString)
    val matchPredicate = equals(varFor(actionString), literalString("match"))
    parsesTo[Statements](ShowUsers(
      Some(Right(where(or(accessPredicate, matchPredicate)))),
      withAuth = false,
      asCommands = false
    )(pos))
  }

  test("SHOW USERS YIELD user ORDER BY user") {
    val columns = yieldClause(returnItems(variableReturnItem(userString)), Some(orderBy(sortItem(varUser))))
    parsesTo[Statements](ShowUsers(Some(Left((columns, None))), withAuth = false, asCommands = false)(pos))
  }

  test("SHOW USERS YIELD user ORDER BY user WHERE user ='none'") {
    val orderByClause = orderBy(sortItem(varUser))
    val whereClause = where(equals(varUser, noneString))
    val columns =
      yieldClause(returnItems(variableReturnItem(userString)), Some(orderByClause), where = Some(whereClause))
    parsesTo[Statements](ShowUsers(Some(Left((columns, None))), withAuth = false, asCommands = false)(pos))
  }

  test("SHOW USERS YIELD user ORDER BY user SKIP 1 LIMIT 10 WHERE user ='none'") {
    val orderByClause = orderBy(sortItem(varUser))
    val whereClause = where(equals(varUser, noneString))
    val columns = yieldClause(
      returnItems(variableReturnItem(userString)),
      Some(orderByClause),
      Some(skip(1)),
      Some(limit(10)),
      Some(whereClause)
    )
    parsesTo[Statements](ShowUsers(Some(Left((columns, None))), withAuth = false, asCommands = false)(pos))
  }

  test("SHOW USERS YIELD user ORDER BY user OFFSET 1 LIMIT 10 WHERE user ='none'") {
    val orderByClause = orderBy(sortItem(varUser))
    val whereClause = where(equals(varUser, noneString))
    val columns = yieldClause(
      returnItems(variableReturnItem(userString)),
      Some(orderByClause),
      Some(skip(1)),
      Some(limit(10)),
      Some(whereClause)
    )
    parsesTo[Statements](ShowUsers(Some(Left((columns, None))), withAuth = false, asCommands = false)(pos))
  }

  test("SHOW USERS YIELD user SKIP -1") {
    val columns = yieldClause(returnItems(variableReturnItem(userString)), skip = Some(skip(-1)))
    parsesTo[Statements](ShowUsers(Some(Left((columns, None))), withAuth = false, asCommands = false)(pos))
  }

  test("SHOW USERS YIELD user RETURN user ORDER BY user") {
    parsesTo[Statements](ShowUsers(
      Some(Left((
        yieldClause(returnItems(variableReturnItem(userString))),
        Some(returnClause(returnItems(variableReturnItem(userString)), Some(orderBy(sortItem(varUser)))))
      ))),
      withAuth = false,
      asCommands = false
    )(pos))
  }

  test("SHOW USERS YIELD user, suspended as suspended WHERE suspended RETURN DISTINCT user") {
    val suspendedVar = varFor("suspended")
    parsesTo[Statements](ShowUsers(
      Some(Left((
        yieldClause(
          returnItems(variableReturnItem(userString), aliasedReturnItem(suspendedVar)),
          where = Some(where(suspendedVar))
        ),
        Some(returnClause(returnItems(variableReturnItem(userString)), distinct = true))
      ))),
      withAuth = false,
      asCommands = false
    )(pos))
  }

  test("SHOW USERS YIELD * RETURN *") {
    parsesTo[Statements](ShowUsers(
      Some(Left((yieldClause(returnAllItems), Some(returnClause(returnAllItems))))),
      withAuth = false,
      asCommands = false
    )(pos))
  }

  test("SHOW USERS YIELD *") {
    parsesTo[Statements](ShowUsers(
      Some(Left((yieldClause(returnAllItems), None))),
      withAuth = false,
      asCommands = false
    )(pos))
  }

  test("SHOW USER YIELD *") {
    parsesTo[Statements](ShowUsers(
      Some(Left((yieldClause(returnAllItems), None))),
      withAuth = false,
      asCommands = false
    )(pos))
  }

  test("SHOW USERS WITH AUTH") {
    parsesTo[Statements](ShowUsers(None, withAuth = true, asCommands = false)(pos))
  }

  test("SHOW USER WITH AUTH WHERE user = 'GRANTED'") {
    parsesTo[Statements](ShowUsers(
      Some(Right(where(equals(varUser, grantedString)))),
      withAuth = true,
      asCommands = false
    )(pos))
  }

  test("SHOW USERS WITH AUTH YIELD user ORDER BY user") {
    val columns = yieldClause(returnItems(variableReturnItem(userString)), Some(orderBy(sortItem(varUser))))
    parsesTo[Statements](ShowUsers(Some(Left((columns, None))), withAuth = true, asCommands = false)(pos))
  }

  test("SHOW USERS AS COMMANDS") {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'COMMANDS': expected 'PRIVILEGE' or 'PRIVILEGES'")
      case _       => _.toAst(ShowUsers(None, withAuth = false, asCommands = true)(pos))
    }
  }

  test("SHOW USER AS COMMAND YIELD user, command ORDER BY user") {
    val columns = yieldClause(
      returnItems(variableReturnItem(userString), variableReturnItem("command")),
      Some(orderBy(sortItem(varUser)))
    )
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'COMMAND': expected 'PRIVILEGE' or 'PRIVILEGES'")
      case _       => _.toAst(ShowUsers(Some(Left((columns, None))), withAuth = false, asCommands = true)(pos))
    }
  }

  test("SHOW USER WITH AUTH AS COMMAND WHERE provider = 'native'") {
    parsesIn[Statement] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'AS': expected 'WHERE', 'YIELD' or <EOF>")
      case _ => _.toAst(ShowUsers(
          Some(Right(where(equals(varFor("provider"), literalString("native"))))),
          withAuth = true,
          asCommands = true
        )(pos))
    }
  }

  // fails parsing

  test("SHOW USERS YIELD *,blah RETURN user") {
    failsParsing[Statements]
  }

  test("SHOW USERS YIELD (123 + xyz)") {
    failsParsing[Statements]
  }

  test("SHOW USERS YIELD (123 + xyz) AS foo") {
    failsParsing[Statements]
  }

  test("SHOW USERS WHERE user = 'GRANTED' WITH AUTH") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'WITH': expected an expression or <EOF> (line 1, column 35 (offset: 34))
        |"SHOW USERS WHERE user = 'GRANTED' WITH AUTH"
        |                                   ^""".stripMargin
    )
  }

  test("SHOW USERS YIELD * WITH AUTH") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'WITH': expected 'ORDER BY', 'LIMIT', 'OFFSET', 'RETURN', 'SKIP', 'WHERE' or <EOF> (line 1, column 20 (offset: 19))
        |"SHOW USERS YIELD * WITH AUTH"
        |                    ^""".stripMargin
    )
  }

  test("SHOW USERS AS COMMANDS WITH AUTH") {
    failsParsing[Statement].in {
      case Cypher5 => _.withAnyFailure
      case _ => _.withSyntaxError(
          """Invalid input 'WITH': expected 'WHERE', 'YIELD' or <EOF> (line 1, column 24 (offset: 23))
            |"SHOW USERS AS COMMANDS WITH AUTH"
            |                        ^""".stripMargin
        )
    }
  }

  // Show current user

  test("SHOW CURRENT USER") {
    assertAst(
      singleQuery(ShowCurrentUserClause(None, List.empty, yieldAll = false, None)(defaultPos))
    )
  }

  test("SHOW CURRENT USER YIELD * WHERE suspended = false RETURN roles") {
    assertAst(
      singleQuery(
        ShowCurrentUserClause(
          None,
          List.empty,
          yieldAll = true,
          Some(withFromYield(returnAllItems, where = Some(where(equals(varFor("suspended"), falseLiteral)))))
        )(defaultPos),
        return_(variableReturnItem("roles"))
      ),
      comparePosition = false
    )
  }

  test("SHOW CURRENT USER YIELD *") {
    assertAst(
      singleQuery(
        ShowCurrentUserClause(
          None,
          List.empty,
          yieldAll = true,
          Some(withFromYield(returnAllItems))
        )(defaultPos)
      ),
      comparePosition = false
    )
  }

  test("SHOW CURRENT USER WHERE user = 'GRANTED'") {
    assertAst(
      singleQuery(
        ShowCurrentUserClause(
          Some(where(equals(varUser, grantedString))),
          List.empty,
          yieldAll = false,
          None
        )(defaultPos)
      ),
      comparePosition = false
    )
  }

  test("USE system SHOW CURRENT USER") {
    assertAstVersionBased(
      cypher5 =>
        singleQuery(
          use(List("system"), !cypher5),
          ShowCurrentUserClause(None, List.empty, yieldAll = false, None)(defaultPos)
        ),
      comparePosition = false
    )
  }

  test("SHOW CURRENT USER RETURN *") {
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'RETURN'")
      case _ => _.toAstPositioned(singleQuery(
          ShowCurrentUserClause(None, List.empty, yieldAll = false, None)(defaultPos),
          returnAll
        ))
    }
  }

  test("SHOW CURRENT USER WITH AUTH") {
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'WITH': expected 'WHERE', 'YIELD' or <EOF> (line 1, column 19 (offset: 18))
            |"SHOW CURRENT USER WITH AUTH"
            |                   ^""".stripMargin
        )
      case _ => _.toAstPositioned(singleQuery(
          ShowCurrentUserClause(None, List.empty, yieldAll = false, None)(defaultPos),
          with_(variableReturnItem("AUTH"))
        ))
    }
  }

  // fails parsing

  test("SHOW CURRENT USERS") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'USERS': expected 'USER' (line 1, column 14 (offset: 13))
            |"SHOW CURRENT USERS"
            |              ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'USERS': expected 'GRAPH TYPE' or 'USER' (line 1, column 14 (offset: 13))
            |"SHOW CURRENT USERS"
            |              ^""".stripMargin
        )
    }
  }

  test("SHOW CURRENT USERS YIELD *") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'USERS': expected 'USER' (line 1, column 14 (offset: 13))
            |"SHOW CURRENT USERS YIELD *"
            |              ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'USERS': expected 'GRAPH TYPE' or 'USER' (line 1, column 14 (offset: 13))
            |"SHOW CURRENT USERS YIELD *"
            |              ^""".stripMargin
        )
    }
  }

  test("SHOW CURRENT USERS WHERE user = 'GRANTED'") {
    failsParsing[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'USERS': expected 'USER' (line 1, column 14 (offset: 13))
            |"SHOW CURRENT USERS WHERE user = 'GRANTED'"
            |              ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'USERS': expected 'GRAPH TYPE' or 'USER' (line 1, column 14 (offset: 13))
            |"SHOW CURRENT USERS WHERE user = 'GRANTED'"
            |              ^""".stripMargin
        )
    }
  }
}
