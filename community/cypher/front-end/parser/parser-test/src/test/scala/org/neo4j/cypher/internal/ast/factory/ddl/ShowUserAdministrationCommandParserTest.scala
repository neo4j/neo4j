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

import org.neo4j.cypher.internal.ast.ShowCurrentUser
import org.neo4j.cypher.internal.ast.ShowUsers
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc

class ShowUserAdministrationCommandParserTest extends UserAdministrationCommandParserTestBase {

  //  Show users

  test("SHOW USERS") {
    parsesTo[Statements](ShowUsers(None, withAuth = false)(pos))
  }

  test("SHOW USER") {
    parsesTo[Statements](ShowUsers(None, withAuth = false)(pos))
  }

  test("USE system SHOW USERS") {
    parsesTo[Statements](ShowUsers(None, withAuth = false)(pos).withGraph(Some(use(List("system")))))
  }

  test("SHOW USERS WHERE user = 'GRANTED'") {
    parsesTo[Statements](ShowUsers(Some(Right(where(equals(varUser, grantedString)))), withAuth = false)(pos))
  }

  test("SHOW USER WHERE user = 'GRANTED'") {
    parsesTo[Statements](ShowUsers(Some(Right(where(equals(varUser, grantedString)))), withAuth = false)(pos))
  }

  test("SHOW USERS WHERE user = 'GRANTED' AND action = 'match'") {
    val accessPredicate = equals(varUser, grantedString)
    val matchPredicate = equals(varFor(actionString), literalString("match"))
    parsesTo[Statements](ShowUsers(Some(Right(where(and(accessPredicate, matchPredicate)))), withAuth = false)(pos))
  }

  test("SHOW USERS WHERE user = 'GRANTED' OR action = 'match'") {
    val accessPredicate = equals(varUser, grantedString)
    val matchPredicate = equals(varFor(actionString), literalString("match"))
    parsesTo[Statements](ShowUsers(Some(Right(where(or(accessPredicate, matchPredicate)))), withAuth = false)(pos))
  }

  test("SHOW USERS YIELD user ORDER BY user") {
    val columns = yieldClause(returnItems(variableReturnItem(userString)), Some(orderBy(sortItem(varUser))))
    parsesTo[Statements](ShowUsers(Some(Left((columns, None))), withAuth = false)(pos))
  }

  test("SHOW USERS YIELD user ORDER BY user WHERE user ='none'") {
    val orderByClause = orderBy(sortItem(varUser))
    val whereClause = where(equals(varUser, noneString))
    val columns =
      yieldClause(returnItems(variableReturnItem(userString)), Some(orderByClause), where = Some(whereClause))
    parsesTo[Statements](ShowUsers(Some(Left((columns, None))), withAuth = false)(pos))
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
    parsesTo[Statements](ShowUsers(Some(Left((columns, None))), withAuth = false)(pos))
  }

  test("SHOW USERS YIELD user SKIP -1") {
    val columns = yieldClause(returnItems(variableReturnItem(userString)), skip = Some(skip(-1)))
    parsesTo[Statements](ShowUsers(Some(Left((columns, None))), withAuth = false)(pos))
  }

  test("SHOW USERS YIELD user RETURN user ORDER BY user") {
    parsesTo[Statements](ShowUsers(
      Some(Left((
        yieldClause(returnItems(variableReturnItem(userString))),
        Some(returnClause(returnItems(variableReturnItem(userString)), Some(orderBy(sortItem(varUser)))))
      ))),
      withAuth = false
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
      withAuth = false
    )(pos))
  }

  test("SHOW USERS YIELD * RETURN *") {
    parsesTo[Statements](ShowUsers(
      Some(Left((yieldClause(returnAllItems), Some(returnClause(returnAllItems))))),
      withAuth = false
    )(pos))
  }

  test("SHOW USERS YIELD *") {
    parsesTo[Statements](ShowUsers(Some(Left((yieldClause(returnAllItems), None))), withAuth = false)(pos))
  }

  test("SHOW USER YIELD *") {
    parsesTo[Statements](ShowUsers(Some(Left((yieldClause(returnAllItems), None))), withAuth = false)(pos))
  }

  test("SHOW USERS WITH AUTH") {
    parsesTo[Statements](ShowUsers(None, withAuth = true)(pos))
  }

  test("SHOW USER WITH AUTH WHERE user = 'GRANTED'") {
    parsesTo[Statements](ShowUsers(Some(Right(where(equals(varUser, grantedString)))), withAuth = true)(pos))
  }

  test("SHOW USERS WITH AUTH YIELD user ORDER BY user") {
    val columns = yieldClause(returnItems(variableReturnItem(userString)), Some(orderBy(sortItem(varUser))))
    parsesTo[Statements](ShowUsers(Some(Left((columns, None))), withAuth = true)(pos))
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
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input 'WITH': expected"
        )
      case _ => _.withSyntaxError(
          """Invalid input 'WITH': expected an expression or <EOF> (line 1, column 35 (offset: 34))
            |"SHOW USERS WHERE user = 'GRANTED' WITH AUTH"
            |                                   ^""".stripMargin
        )
    }
  }

  test("SHOW USERS YIELD * WITH AUTH") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(
          """Invalid input 'WITH': expected
            |  "LIMIT"
            |  "ORDER"
            |  "RETURN"
            |  "SKIP"
            |  "WHERE"
            |  <EOF> (line 1, column 20 (offset: 19))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'WITH': expected 'ORDER BY', 'LIMIT', 'RETURN', 'SKIP', 'WHERE' or <EOF> (line 1, column 20 (offset: 19))
            |"SHOW USERS YIELD * WITH AUTH"
            |                    ^""".stripMargin
        )
    }
  }

  // Show current user

  test("SHOW CURRENT USER") {
    parsesTo[Statements](ShowCurrentUser(None)(pos))
  }

  test("SHOW CURRENT USER YIELD * WHERE suspended = false RETURN roles") {
    val suspendedVar = varFor("suspended")
    val yield_ = yieldClause(returnAllItems, where = Some(where(equals(suspendedVar, falseLiteral))))
    val return_ = returnClause(returnItems(variableReturnItem("roles")))
    val yieldOrWhere = Some(Left(yield_ -> Some(return_)))
    parsesTo[Statements](ShowCurrentUser(yieldOrWhere)(pos))
  }

  test("SHOW CURRENT USER YIELD *") {
    parsesTo[Statements](ShowCurrentUser(Some(Left((yieldClause(returnAllItems), None))))(pos))
  }

  test("SHOW CURRENT USER WHERE user = 'GRANTED'") {
    parsesTo[Statements](ShowCurrentUser(Some(Right(where(equals(varUser, grantedString)))))(pos))
  }

  // fails parsing

  test("SHOW CURRENT USERS") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(
          """Invalid input 'USERS': expected "USER" (line 1, column 14 (offset: 13))"""
        )
      case _ => _.withSyntaxError(
          """Invalid input 'USERS': expected 'USER' (line 1, column 14 (offset: 13))
            |"SHOW CURRENT USERS"
            |              ^""".stripMargin
        )
    }
  }

  test("SHOW CURRENT USERS YIELD *") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(
          """Invalid input 'USERS': expected "USER" (line 1, column 14 (offset: 13))"""
        )
      case _ => _.withSyntaxError(
          """Invalid input 'USERS': expected 'USER' (line 1, column 14 (offset: 13))
            |"SHOW CURRENT USERS YIELD *"
            |              ^""".stripMargin
        )
    }
  }

  test("SHOW CURRENT USERS WHERE user = 'GRANTED'") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(
          """Invalid input 'USERS': expected "USER" (line 1, column 14 (offset: 13))"""
        )
      case _ => _.withSyntaxError(
          """Invalid input 'USERS': expected 'USER' (line 1, column 14 (offset: 13))
            |"SHOW CURRENT USERS WHERE user = 'GRANTED'"
            |              ^""".stripMargin
        )
    }
  }

  test("SHOW CURRENT USER WITH AUTH") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessage(
          """Invalid input 'WITH': expected "WHERE", "YIELD" or <EOF> (line 1, column 19 (offset: 18))"""
        )
      case _ => _.withSyntaxError(
          """Invalid input 'WITH': expected 'WHERE', 'YIELD' or <EOF> (line 1, column 19 (offset: 18))
            |"SHOW CURRENT USER WITH AUTH"
            |                   ^""".stripMargin
        )
    }
  }
}
