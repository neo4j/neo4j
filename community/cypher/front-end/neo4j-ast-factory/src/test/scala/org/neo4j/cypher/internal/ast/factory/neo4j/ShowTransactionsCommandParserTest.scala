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
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.IntegerType

/* Tests for listing transactions */
class ShowTransactionsCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq("TRANSACTION", "TRANSACTIONS").foreach { transactionKeyword =>
    test(s"SHOW $transactionKeyword") {
      assertAst(
        singleQuery(ast.ShowTransactionsClause(Left(List.empty), None, List.empty, yieldAll = false)(defaultPos))
      )
    }

    test(s"SHOW $transactionKeyword 'db1-transaction-123'") {
      assertAst(
        singleQuery(
          ast.ShowTransactionsClause(
            Right(literalString("db1-transaction-123")),
            None,
            List.empty,
            yieldAll = false
          )(defaultPos)
        )
      )
    }

    test(s"""SHOW $transactionKeyword "db1-transaction-123"""") {
      assertAst(
        singleQuery(
          ast.ShowTransactionsClause(
            Right(literalString("db1-transaction-123")),
            None,
            List.empty,
            yieldAll = false
          )(defaultPos)
        )
      )
    }

    test(s"SHOW $transactionKeyword 'my.db-transaction-123'") {
      assertAst(
        singleQuery(ast.ShowTransactionsClause(
          Right(literalString("my.db-transaction-123")),
          None,
          List.empty,
          yieldAll = false
        )(defaultPos))
      )
    }

    test(s"SHOW $transactionKeyword $$param") {
      assertAst(singleQuery(ast.ShowTransactionsClause(
        Right(parameter("param", CTAny)),
        None,
        List.empty,
        yieldAll = false
      )(defaultPos)))
    }

    test(s"SHOW $transactionKeyword $$where") {
      assertAst(singleQuery(ast.ShowTransactionsClause(
        Right(parameter("where", CTAny)),
        None,
        List.empty,
        yieldAll = false
      )(defaultPos)))
    }

    test(s"""SHOW $transactionKeyword 'db1 - transaction - 123', "db2-transaction-45a6"""") {
      assertAst(singleQuery(ast.ShowTransactionsClause(
        Left(List("db1 - transaction - 123", "db2-transaction-45a6")),
        None,
        List.empty,
        yieldAll = false
      )(defaultPos)))
    }

    test(s"SHOW $transactionKeyword 'yield-transaction-123'") {
      assertAst(
        singleQuery(ast.ShowTransactionsClause(
          Right(literalString("yield-transaction-123")),
          None,
          List.empty,
          yieldAll = false
        )(defaultPos))
      )
    }

    test(s"SHOW $transactionKeyword 'where-transaction-123'") {
      assertAst(
        singleQuery(ast.ShowTransactionsClause(
          Right(literalString("where-transaction-123")),
          None,
          List.empty,
          yieldAll = false
        )(defaultPos))
      )
    }

    test(s"USE db SHOW $transactionKeyword") {
      assertAst(
        singleQuery(
          use(List("db")),
          ast.ShowTransactionsClause(Left(List.empty), None, List.empty, yieldAll = false)(pos)
        ),
        comparePosition = false
      )
    }

  }

  test("SHOW TRANSACTION db-transaction-123") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(
        Right(subtract(subtract(varFor("db"), varFor("transaction")), literalInt(123))),
        None,
        List.empty,
        yieldAll = false
      )(pos)
    ))
  }

  test("SHOW TRANSACTION 'neo4j'+'-transaction-'+3") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(
        Right(add(add(literalString("neo4j"), literalString("-transaction-")), literalInt(3))),
        None,
        List.empty,
        yieldAll = false
      )(pos)
    ))
  }

  test("SHOW TRANSACTION ('neo4j'+'-transaction-'+3)") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(
        Right(add(add(literalString("neo4j"), literalString("-transaction-")), literalInt(3))),
        None,
        List.empty,
        yieldAll = false
      )(pos)
    ))
  }

  test("SHOW TRANSACTIONS ['db1-transaction-123', 'db2-transaction-456']") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(
        Right(listOfString("db1-transaction-123", "db2-transaction-456")),
        None,
        List.empty,
        yieldAll = false
      )(pos)
    ))
  }

  test("SHOW TRANSACTION foo") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(Right(varFor("foo")), None, List.empty, yieldAll = false)(pos)
    ))
  }

  test("SHOW TRANSACTION x+2") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(Right(add(varFor("x"), literalInt(2))), None, List.empty, yieldAll = false)(pos)
    ))
  }

  test("SHOW TRANSACTIONS YIELD") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(Right(varFor("YIELD")), None, List.empty, yieldAll = false)(pos)
    ))
  }

  test("SHOW TRANSACTIONS YIELD (123 + xyz)") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(
        Right(function("YIELD", add(literalInt(123), varFor("xyz")))),
        None,
        List.empty,
        yieldAll = false
      )(pos)
    ))
  }

  test("SHOW TRANSACTIONS ALL") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(Right(varFor("ALL")), None, List.empty, yieldAll = false)(pos)
    ))
  }

  // Filtering tests

  test("SHOW TRANSACTION WHERE transactionId = 'db1-transaction-123'") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(
        Left(List.empty),
        Some(where(equals(varFor("transactionId"), literalString("db1-transaction-123")))),
        List.empty,
        yieldAll = false
      )(defaultPos)
    ))
  }

  test("SHOW TRANSACTIONS YIELD database") {
    assertAst(
      singleQuery(
        ast.ShowTransactionsClause(
          Left(List.empty),
          None,
          List(commandResultItem("database")),
          yieldAll = false
        )(pos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("database")))
      ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS 'db1-transaction-123', 'db2-transaction-456' YIELD *") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(
        Left(List("db1-transaction-123", "db2-transaction-456")),
        None,
        List.empty,
        yieldAll = true
      )(
        defaultPos
      ),
      withFromYield(returnAllItems)
    ))
  }

  test("SHOW TRANSACTIONS 'db1-transaction-123', 'db2-transaction-456', 'yield' YIELD *") {
    assertAst(
      singleQuery(
        ast.ShowTransactionsClause(
          Left(List("db1-transaction-123", "db2-transaction-456", "yield")),
          None,
          List.empty,
          yieldAll = true
        )(pos),
        withFromYield(returnAllItems)
      ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS YIELD * ORDER BY transactionId SKIP 2 LIMIT 5") {
    assertAst(
      singleQuery(
        ast.ShowTransactionsClause(Left(List.empty), None, List.empty, yieldAll = true)(pos),
        withFromYield(returnAllItems, Some(orderBy(sortItem(varFor("transactionId")))), Some(skip(2)), Some(limit(5)))
      ),
      comparePosition = false
    )
  }

  test("USE db SHOW TRANSACTIONS YIELD transactionId, activeLockCount AS pp WHERE pp < 50 RETURN transactionId") {
    assertAst(
      singleQuery(
        use(List("db")),
        ast.ShowTransactionsClause(
          Left(List.empty),
          None,
          List(
            commandResultItem("transactionId"),
            commandResultItem("activeLockCount", Some("pp"))
          ),
          yieldAll = false
        )(pos),
        withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("transactionId", "pp")),
          where = Some(where(lessThan(varFor("pp"), literalInt(50L))))
        ),
        return_(variableReturnItem("transactionId"))
      ),
      comparePosition = false
    )
  }

  test(
    "USE db SHOW TRANSACTIONS YIELD transactionId, activeLockCount AS pp ORDER BY pp SKIP 2 LIMIT 5 WHERE pp < 50 RETURN transactionId"
  ) {
    assertAst(
      singleQuery(
        use(List("db")),
        ast.ShowTransactionsClause(
          Left(List.empty),
          None,
          List(
            commandResultItem("transactionId"),
            commandResultItem("activeLockCount", Some("pp"))
          ),
          yieldAll = false
        )(pos),
        withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("transactionId", "pp")),
          Some(orderBy(sortItem(varFor("pp")))),
          Some(skip(2)),
          Some(limit(5)),
          Some(where(lessThan(varFor("pp"), literalInt(50L))))
        ),
        return_(variableReturnItem("transactionId"))
      ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS $param YIELD transactionId AS TRANSACTION, database AS OUTPUT") {
    assertAst(
      singleQuery(
        ast.ShowTransactionsClause(
          Right(parameter("param", CTAny)),
          None,
          List(commandResultItem("transactionId", Some("TRANSACTION")), commandResultItem("database", Some("OUTPUT"))),
          yieldAll = false
        )(pos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("TRANSACTION", "OUTPUT")))
      ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS 'where' YIELD transactionId AS TRANSACTION, database AS OUTPUT") {
    assertAst(
      singleQuery(
        ast.ShowTransactionsClause(
          Right(literalString("where")),
          None,
          List(commandResultItem("transactionId", Some("TRANSACTION")), commandResultItem("database", Some("OUTPUT"))),
          yieldAll = false
        )(pos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("TRANSACTION", "OUTPUT")))
      ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTION 'db1-transaction-123' WHERE transactionId = 'db1-transaction-124'") {
    assertAst(
      singleQuery(ast.ShowTransactionsClause(
        Right(literalString("db1-transaction-123")),
        Some(where(equals(varFor("transactionId"), literalString("db1-transaction-124")))),
        List.empty,
        yieldAll = false
      )(pos)),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTION 'yield' WHERE transactionId = 'where'") {
    assertAst(
      singleQuery(ast.ShowTransactionsClause(
        Right(literalString("yield")),
        Some(where(equals(varFor("transactionId"), literalString("where")))),
        List.empty,
        yieldAll = false
      )(pos)),
      comparePosition = false
    )
  }

  test(
    "SHOW TRANSACTION 'db1-transaction-123', 'db1-transaction-124' WHERE transactionId IN ['db1-transaction-124', 'db1-transaction-125']"
  ) {
    assertAst(
      singleQuery(ast.ShowTransactionsClause(
        Left(List("db1-transaction-123", "db1-transaction-124")),
        Some(where(in(varFor("transactionId"), listOfString("db1-transaction-124", "db1-transaction-125")))),
        List.empty,
        yieldAll = false
      )(pos)),
      comparePosition = false
    )
  }

  test(
    "SHOW TRANSACTION db1-transaction-123 WHERE transactionId IN ['db1-transaction-124', 'db1-transaction-125']"
  ) {
    assertAst(
      singleQuery(ast.ShowTransactionsClause(
        Right(subtract(subtract(varFor("db1"), varFor("transaction")), literalInt(123))),
        Some(where(in(varFor("transactionId"), listOfString("db1-transaction-124", "db1-transaction-125")))),
        List.empty,
        yieldAll = false
      )(pos)),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS ['db1-transaction-123', 'db2-transaction-456'] YIELD *") {
    assertAst(
      singleQuery(
        ast.ShowTransactionsClause(
          Right(listOfString("db1-transaction-123", "db2-transaction-456")),
          None,
          List.empty,
          yieldAll = true
        )(pos),
        withFromYield(returnAllItems)
      ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS $x+'123' YIELD transactionId AS TRANSACTION, database AS SHOW") {
    assertAst(
      singleQuery(
        ast.ShowTransactionsClause(
          Right(add(parameter("x", CTAny), literalString("123"))),
          None,
          List(commandResultItem("transactionId", Some("TRANSACTION")), commandResultItem("database", Some("SHOW"))),
          yieldAll = false
        )(pos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("TRANSACTION", "SHOW")))
      ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS where YIELD *") {
    assertAst(
      singleQuery(
        ast.ShowTransactionsClause(
          Right(varFor("where")),
          None,
          List.empty,
          yieldAll = true
        )(pos),
        withFromYield(returnAllItems)
      ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS yield YIELD *") {
    assertAst(
      singleQuery(
        ast.ShowTransactionsClause(
          Right(varFor("yield")),
          None,
          List.empty,
          yieldAll = true
        )(pos),
        withFromYield(returnAllItems)
      ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS show YIELD *") {
    assertAst(
      singleQuery(
        ast.ShowTransactionsClause(
          Right(varFor("show")),
          None,
          List.empty,
          yieldAll = true
        )(pos),
        withFromYield(returnAllItems)
      ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS terminate YIELD *") {
    assertAst(
      singleQuery(
        ast.ShowTransactionsClause(
          Right(varFor("terminate")),
          None,
          List.empty,
          yieldAll = true
        )(pos),
        withFromYield(returnAllItems)
      ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS YIELD yield") {
    assertAst(
      singleQuery(
        ast.ShowTransactionsClause(
          Left(List.empty),
          None,
          List(commandResultItem("yield")),
          yieldAll = false
        )(pos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("yield")))
      ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS where WHERE true") {
    assertAst(
      singleQuery(
        ast.ShowTransactionsClause(
          Right(varFor("where")),
          Some(where(trueLiteral)),
          List.empty,
          yieldAll = false
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS yield WHERE true") {
    assertAst(
      singleQuery(
        ast.ShowTransactionsClause(
          Right(varFor("yield")),
          Some(where(trueLiteral)),
          List.empty,
          yieldAll = false
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS show WHERE true") {
    assertAst(
      singleQuery(
        ast.ShowTransactionsClause(
          Right(varFor("show")),
          Some(where(trueLiteral)),
          List.empty,
          yieldAll = false
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS terminate WHERE true") {
    assertAst(
      singleQuery(
        ast.ShowTransactionsClause(
          Right(varFor("terminate")),
          Some(where(trueLiteral)),
          List.empty,
          yieldAll = false
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS `yield` YIELD *") {
    assertAst(
      singleQuery(
        ast.ShowTransactionsClause(
          Right(varFor("yield")),
          None,
          List.empty,
          yieldAll = true
        )(pos),
        withFromYield(returnAllItems)
      ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS `where` WHERE true") {
    assertAst(
      singleQuery(
        ast.ShowTransactionsClause(
          Right(varFor("where")),
          Some(where(trueLiteral)),
          List.empty,
          yieldAll = false
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS YIELD a ORDER BY a WHERE a = 1") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(
        Left(List.empty),
        None,
        List(commandResultItem("a")),
        yieldAll = false
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("a")),
        Some(orderBy(sortItem(varFor("a")))),
        where = Some(where(equals(varFor("a"), literalInt(1))))
      )
    ))
  }

  test("SHOW TRANSACTIONS YIELD a AS b ORDER BY b WHERE b = 1") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(
        Left(List.empty),
        None,
        List(commandResultItem("a", Some("b"))),
        yieldAll = false
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("b")),
        Some(orderBy(sortItem(varFor("b")))),
        where = Some(where(equals(varFor("b"), literalInt(1))))
      )
    ))
  }

  test("SHOW TRANSACTIONS YIELD a AS b ORDER BY a WHERE a = 1") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(
        Left(List.empty),
        None,
        List(commandResultItem("a", Some("b"))),
        yieldAll = false
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("b")),
        Some(orderBy(sortItem(varFor("b")))),
        where = Some(where(equals(varFor("b"), literalInt(1))))
      )
    ))
  }

  test("SHOW TRANSACTIONS YIELD a ORDER BY EXISTS { (a) } WHERE EXISTS { (a) }") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(
        Left(List.empty),
        None,
        List(commandResultItem("a")),
        yieldAll = false
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("a")),
        Some(orderBy(sortItem(simpleExistsExpression(patternForMatch(nodePat(Some("a"))), None)))),
        where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("a"))), None)))
      )
    ))
  }

  test("SHOW TRANSACTIONS YIELD a ORDER BY EXISTS { (b) } WHERE EXISTS { (b) }") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(
        Left(List.empty),
        None,
        List(commandResultItem("a")),
        yieldAll = false
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("a")),
        Some(orderBy(sortItem(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))),
        where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))
      )
    ))
  }

  test("SHOW TRANSACTIONS YIELD a AS b ORDER BY COUNT { (b) } WHERE EXISTS { (b) }") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(
        Left(List.empty),
        None,
        List(commandResultItem("a", Some("b"))),
        yieldAll = false
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("b")),
        Some(orderBy(sortItem(simpleCountExpression(patternForMatch(nodePat(Some("b"))), None)))),
        where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))
      )
    ))
  }

  test("SHOW TRANSACTIONS YIELD a AS b ORDER BY EXISTS { (a) } WHERE COLLECT { MATCH (a) RETURN a } <> []") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(
        Left(List.empty),
        None,
        List(commandResultItem("a", Some("b"))),
        yieldAll = false
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("b")),
        Some(orderBy(sortItem(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))),
        where = Some(where(notEquals(
          simpleCollectExpression(patternForMatch(nodePat(Some("b"))), None, return_(returnItem(varFor("b"), "a"))),
          listOf()
        )))
      )
    ))
  }

  test("SHOW TRANSACTIONS YIELD a AS b ORDER BY b + COUNT { () } WHERE b OR EXISTS { () }") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(
        Left(List.empty),
        None,
        List(commandResultItem("a", Some("b"))),
        yieldAll = false
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("b")),
        Some(orderBy(sortItem(add(varFor("b"), simpleCountExpression(patternForMatch(nodePat()), None))))),
        where = Some(where(or(varFor("b"), simpleExistsExpression(patternForMatch(nodePat()), None))))
      )
    ))
  }

  test("SHOW TRANSACTIONS YIELD a AS b ORDER BY a + EXISTS { () } WHERE a OR ALL (x IN [1, 2] WHERE x IS :: INT)") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(
        Left(List.empty),
        None,
        List(commandResultItem("a", Some("b"))),
        yieldAll = false
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("b")),
        Some(orderBy(sortItem(add(varFor("b"), simpleExistsExpression(patternForMatch(nodePat()), None))))),
        where = Some(where(or(
          varFor("b"),
          AllIterablePredicate(
            varFor("x"),
            listOfInt(1, 2),
            Some(isTyped(varFor("x"), IntegerType(isNullable = true)(pos)))
          )(pos)
        )))
      )
    ))
  }

  test(
    "SHOW TRANSACTIONS 'id', 'id' YIELD username as transactionId, transactionId as username WHERE size(transactionId) > 0 RETURN transactionId as username"
  ) {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(
        Left(List("id", "id")),
        None,
        List(
          commandResultItem("username", Some("transactionId")),
          commandResultItem("transactionId", Some("username"))
        ),
        yieldAll = false
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("transactionId", "username")),
        where = Some(where(
          greaterThan(size(varFor("transactionId")), literalInt(0))
        ))
      ),
      return_(aliasedReturnItem("transactionId", "username"))
    ))
  }

  // Negative tests

  test("SHOW TRANSACTION db-transaction-123, abc") {
    failsToParse[Statements]
  }

  test("SHOW TRANSACTIONS 'db-transaction-123', $param") {
    failsToParse[Statements]
  }

  test("SHOW TRANSACTIONS $param, 'db-transaction-123'") {
    failsToParse[Statements]
  }

  test("SHOW TRANSACTIONS $param, $param2") {
    failsToParse[Statements]
  }

  test("SHOW TRANSACTIONS ['db1-transaction-123', 'db2-transaction-456'], abc") {
    failsToParse[Statements]
  }

  test("SHOW TRANSACTION foo, 'abc'") {
    failsToParse[Statements]
  }

  test("SHOW TRANSACTION x+2, abc") {
    failsToParse[Statements]
  }

  test("SHOW TRANSACTIONS YIELD * YIELD *") {
    failsToParse[Statements]
  }

  test("SHOW TRANSACTIONS YIELD (123 + xyz) AS foo") {
    failsToParse[Statements]
  }

  test("SHOW TRANSACTIONS WHERE transactionId = 'db1-transaction-123' YIELD *") {
    failsToParse[Statements]
  }

  test("SHOW TRANSACTIONS WHERE transactionId = 'db1-transaction-123' RETURN *") {
    failsToParse[Statements]
  }

  test("SHOW TRANSACTIONS YIELD a b RETURN *") {
    failsToParse[Statements]
  }

  test("SHOW TRANSACTIONS RETURN *") {
    failsToParse[Statements]
  }

  test("SHOW CURRENT USER TRANSACTION") {
    failsToParse[Statements]
  }

  test("SHOW USER user TRANSACTION") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input 'TRANSACTION': expected ",", "PRIVILEGE" or "PRIVILEGES" (line 1, column 16 (offset: 15))""".stripMargin
    )
  }

  test("SHOW TRANSACTION EXECUTED BY USER user") {
    failsToParse[Statements]
  }

  test("SHOW ALL TRANSACTIONS") {
    failsToParse[Statements]
  }

  // Invalid clause order

  for (prefix <- Seq("USE neo4j", "")) {
    test(s"$prefix SHOW TRANSACTIONS YIELD * WITH * MATCH (n) RETURN n") {
      // Can't parse WITH after SHOW
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix UNWIND range(1,10) as b SHOW TRANSACTIONS YIELD * RETURN *") {
      // Can't parse SHOW  after UNWIND
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW TRANSACTIONS WITH name, type RETURN *") {
      // Can't parse WITH after SHOW
      // parses varFor("WITH")
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'name': expected")
    }

    test(s"$prefix WITH 'n' as n SHOW TRANSACTIONS YIELD name RETURN name as numIndexes") {
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW TRANSACTIONS RETURN name as numIndexes") {
      // parses varFor("RETURN")
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'name': expected")
    }

    test(s"$prefix SHOW TRANSACTIONS WITH 1 as c RETURN name as numIndexes") {
      // parses varFor("WITH")
      assertFailsWithMessageStart[Statements](testName, "Invalid input '1': expected")
    }

    test(s"$prefix SHOW TRANSACTIONS WITH 1 as c") {
      // parses varFor("WITH")
      assertFailsWithMessageStart[Statements](testName, "Invalid input '1': expected")
    }

    test(s"$prefix SHOW TRANSACTIONS YIELD a WITH a RETURN a") {
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW TRANSACTIONS YIELD as UNWIND as as a RETURN a") {
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'UNWIND': expected")
    }

    test(s"$prefix SHOW TRANSACTIONS RETURN id2 YIELD id2") {
      // parses varFor("RETURN")
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'id2': expected")
    }
  }

  // Brief/verbose not allowed

  test("SHOW TRANSACTION BRIEF") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(Right(varFor("BRIEF")), None, List.empty, yieldAll = false)(pos)
    ))
  }

  test("SHOW TRANSACTIONS BRIEF YIELD *") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(Right(varFor("BRIEF")), None, List.empty, yieldAll = true)(pos),
      withFromYield(returnAllItems)
    ))
  }

  test("SHOW TRANSACTIONS BRIEF WHERE transactionId = 'db1-transaction-123'") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(
        Right(varFor("BRIEF")),
        Some(where(equals(varFor("transactionId"), literalString("db1-transaction-123")))),
        List.empty,
        yieldAll = false
      )(pos)
    ))
  }

  test("SHOW TRANSACTION VERBOSE") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(Right(varFor("VERBOSE")), None, List.empty, yieldAll = false)(pos)
    ))
  }

  test("SHOW TRANSACTIONS VERBOSE YIELD *") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(Right(varFor("VERBOSE")), None, List.empty, yieldAll = true)(pos),
      withFromYield(returnAllItems)
    ))
  }

  test("SHOW TRANSACTIONS VERBOSE WHERE transactionId = 'db1-transaction-123'") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(
        Right(varFor("VERBOSE")),
        Some(where(equals(varFor("transactionId"), literalString("db1-transaction-123")))),
        List.empty,
        yieldAll = false
      )(pos)
    ))
  }

  test("SHOW TRANSACTION OUTPUT") {
    assertAst(singleQuery(
      ast.ShowTransactionsClause(Right(varFor("OUTPUT")), None, List.empty, yieldAll = false)(pos)
    ))
  }

  test("SHOW TRANSACTION BRIEF OUTPUT") {
    failsToParse[Statements]
  }

  test("SHOW TRANSACTIONS BRIEF RETURN *") {
    failsToParse[Statements]
  }

  test("SHOW TRANSACTION VERBOSE OUTPUT") {
    failsToParse[Statements]
  }

  test("SHOW TRANSACTIONS VERBOSE RETURN *") {
    failsToParse[Statements]
  }

}
