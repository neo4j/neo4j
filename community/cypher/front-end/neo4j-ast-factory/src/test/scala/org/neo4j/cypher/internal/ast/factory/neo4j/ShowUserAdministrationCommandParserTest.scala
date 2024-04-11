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

class ShowUserAdministrationCommandParserTest extends UserAdministrationCommandParserTestBase {

  //  Show users

  test("SHOW USERS") {
    parsesTo[Statements](ast.ShowUsers(None)(pos))
  }

  test("SHOW USER") {
    parsesTo[Statements](ast.ShowUsers(None)(pos))
  }

  test("USE system SHOW USERS") {
    parsesTo[Statements](ast.ShowUsers(None)(pos).withGraph(Some(use(List("system")))))
  }

  test("SHOW USERS WHERE user = 'GRANTED'") {
    parsesTo[Statements](ast.ShowUsers(Some(Right(where(equals(varUser, grantedString)))))(pos))
  }

  test("SHOW USER WHERE user = 'GRANTED'") {
    parsesTo[Statements](ast.ShowUsers(Some(Right(where(equals(varUser, grantedString)))))(pos))
  }

  test("SHOW USERS WHERE user = 'GRANTED' AND action = 'match'") {
    val accessPredicate = equals(varUser, grantedString)
    val matchPredicate = equals(varFor(actionString), literalString("match"))
    parsesTo[Statements](ast.ShowUsers(Some(Right(where(and(accessPredicate, matchPredicate)))))(pos))
  }

  test("SHOW USERS WHERE user = 'GRANTED' OR action = 'match'") {
    val accessPredicate = equals(varUser, grantedString)
    val matchPredicate = equals(varFor(actionString), literalString("match"))
    parsesTo[Statements](ast.ShowUsers(Some(Right(where(or(accessPredicate, matchPredicate)))))(pos))
  }

  test("SHOW USERS YIELD user ORDER BY user") {
    val columns = yieldClause(returnItems(variableReturnItem(userString)), Some(orderBy(sortItem(varUser))))
    parsesTo[Statements](ast.ShowUsers(Some(Left((columns, None))))(pos))
  }

  test("SHOW USERS YIELD user ORDER BY user WHERE user ='none'") {
    val orderByClause = orderBy(sortItem(varUser))
    val whereClause = where(equals(varUser, noneString))
    val columns =
      yieldClause(returnItems(variableReturnItem(userString)), Some(orderByClause), where = Some(whereClause))
    parsesTo[Statements](ast.ShowUsers(Some(Left((columns, None))))(pos))
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
    parsesTo[Statements](ast.ShowUsers(Some(Left((columns, None))))(pos))
  }

  test("SHOW USERS YIELD user SKIP -1") {
    val columns = yieldClause(returnItems(variableReturnItem(userString)), skip = Some(skip(-1)))
    parsesTo[Statements](ast.ShowUsers(Some(Left((columns, None))))(pos))
  }

  test("SHOW USERS YIELD user RETURN user ORDER BY user") {
    parsesTo[Statements](ast.ShowUsers(
      Some(Left((
        yieldClause(returnItems(variableReturnItem(userString))),
        Some(returnClause(returnItems(variableReturnItem(userString)), Some(orderBy(sortItem(varUser)))))
      )))
    )(pos))
  }

  test("SHOW USERS YIELD user, suspended as suspended WHERE suspended RETURN DISTINCT user") {
    val suspendedVar = varFor("suspended")
    parsesTo[Statements](ast.ShowUsers(
      Some(Left((
        yieldClause(
          returnItems(variableReturnItem(userString), aliasedReturnItem(suspendedVar)),
          where = Some(where(suspendedVar))
        ),
        Some(returnClause(returnItems(variableReturnItem(userString)), distinct = true))
      )))
    )(pos))
  }

  test("SHOW USERS YIELD * RETURN *") {
    parsesTo[Statements](
      ast.ShowUsers(Some(Left((yieldClause(returnAllItems), Some(returnClause(returnAllItems))))))(pos)
    )
  }

  test("SHOW USERS YIELD *") {
    parsesTo[Statements](ast.ShowUsers(Some(Left((yieldClause(returnAllItems), None))))(pos))
  }

  test("SHOW USER YIELD *") {
    parsesTo[Statements](ast.ShowUsers(Some(Left((yieldClause(returnAllItems), None))))(pos))
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

  // Show current user

  test("SHOW CURRENT USER") {
    parsesTo[Statements](ast.ShowCurrentUser(None)(pos))
  }

  test("SHOW CURRENT USER YIELD * WHERE suspended = false RETURN roles") {
    val suspendedVar = varFor("suspended")
    val yield_ = yieldClause(returnAllItems, where = Some(where(equals(suspendedVar, falseLiteral))))
    val return_ = returnClause(returnItems(variableReturnItem("roles")))
    val yieldOrWhere = Some(Left(yield_ -> Some(return_)))
    parsesTo[Statements](ast.ShowCurrentUser(yieldOrWhere)(pos))
  }

  test("SHOW CURRENT USER YIELD *") {
    parsesTo[Statements](ast.ShowCurrentUser(Some(Left((yieldClause(returnAllItems), None))))(pos))
  }

  test("SHOW CURRENT USER WHERE user = 'GRANTED'") {
    parsesTo[Statements](ast.ShowCurrentUser(Some(Right(where(equals(varUser, grantedString)))))(pos))
  }

  // fails parsing

  test("SHOW CURRENT USERS") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input 'USERS': expected "USER" (line 1, column 14 (offset: 13))"""
    )
  }

  test("SHOW CURRENT USERS YIELD *") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input 'USERS': expected "USER" (line 1, column 14 (offset: 13))"""
    )
  }

  test("SHOW CURRENT USERS WHERE user = 'GRANTED'") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input 'USERS': expected "USER" (line 1, column 14 (offset: 13))"""
    )
  }
}
