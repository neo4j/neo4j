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
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.IntegerType

/* Tests for terminating transactions */
class TerminateTransactionsCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq("TRANSACTION", "TRANSACTIONS").foreach { transactionKeyword =>
    test(s"TERMINATE $transactionKeyword") {
      assertAst(
        singleQuery(ast.TerminateTransactionsClause(Left(List.empty), List.empty, yieldAll = false, None)(defaultPos))
      )
    }

    test(s"TERMINATE $transactionKeyword 'db1-transaction-123'") {
      assertAst(
        singleQuery(ast.TerminateTransactionsClause(
          Right(literalString("db1-transaction-123")),
          List.empty,
          yieldAll = false,
          None
        )(defaultPos))
      )
    }

    test(s"""TERMINATE $transactionKeyword "db1-transaction-123"""") {
      assertAst(
        singleQuery(ast.TerminateTransactionsClause(
          Right(literalString("db1-transaction-123")),
          List.empty,
          yieldAll = false,
          None
        )(defaultPos))
      )
    }

    test(s"TERMINATE $transactionKeyword 'my.db-transaction-123'") {
      assertAst(
        singleQuery(
          ast.TerminateTransactionsClause(
            Right(literalString("my.db-transaction-123")),
            List.empty,
            yieldAll = false,
            None
          )(pos)
        ),
        comparePosition = false
      )
    }

    test(s"TERMINATE $transactionKeyword $$param") {
      assertAst(
        singleQuery(
          ast.TerminateTransactionsClause(Right(parameter("param", CTAny)), List.empty, yieldAll = false, None)(pos)
        ),
        comparePosition = false
      )
    }

    test(s"TERMINATE $transactionKeyword $$yield") {
      assertAst(
        singleQuery(
          ast.TerminateTransactionsClause(Right(parameter("yield", CTAny)), List.empty, yieldAll = false, None)(pos)
        ),
        comparePosition = false
      )
    }

    test(s"""TERMINATE $transactionKeyword 'db1 - transaction - 123', "db2-transaction-45a6"""") {
      assertAst(singleQuery(ast.TerminateTransactionsClause(
        Left(List("db1 - transaction - 123", "db2-transaction-45a6")),
        List.empty,
        yieldAll = false,
        None
      )(defaultPos)))
    }

    test(s"TERMINATE $transactionKeyword 'yield-transaction-123'") {
      assertAst(
        singleQuery(ast.TerminateTransactionsClause(
          Right(literalString("yield-transaction-123")),
          List.empty,
          yieldAll = false,
          None
        )(defaultPos))
      )
    }

    test(s"TERMINATE $transactionKeyword 'where-transaction-123'") {
      assertAst(
        singleQuery(
          ast.TerminateTransactionsClause(
            Right(literalString("where-transaction-123")),
            List.empty,
            yieldAll = false,
            None
          )(pos)
        ),
        comparePosition = false
      )
    }

    test(s"USE db TERMINATE $transactionKeyword 'db1-transaction-123'") {
      assertAst(
        singleQuery(
          use(List("db")),
          ast.TerminateTransactionsClause(
            Right(literalString("db1-transaction-123")),
            List.empty,
            yieldAll = false,
            None
          )(pos)
        ),
        comparePosition = false
      )
    }

  }

  test("TERMINATE TRANSACTION db-transaction-123") {
    assertAst(singleQuery(
      ast.TerminateTransactionsClause(
        Right(subtract(subtract(varFor("db"), varFor("transaction")), literalInt(123))),
        List.empty,
        yieldAll = false,
        None
      )(pos)
    ))
  }

  test("TERMINATE TRANSACTIONS ['db1-transaction-123', 'db2-transaction-456']") {
    assertAst(singleQuery(
      ast.TerminateTransactionsClause(
        Right(listOfString("db1-transaction-123", "db2-transaction-456")),
        List.empty,
        yieldAll = false,
        None
      )(pos)
    ))
  }

  test("TERMINATE TRANSACTION foo") {
    assertAst(singleQuery(
      ast.TerminateTransactionsClause(Right(varFor("foo")), List.empty, yieldAll = false, None)(pos)
    ))
  }

  test("TERMINATE TRANSACTION x+2") {
    assertAst(singleQuery(
      ast.TerminateTransactionsClause(Right(add(varFor("x"), literalInt(2))), List.empty, yieldAll = false, None)(pos)
    ))
  }

  test("TERMINATE TRANSACTIONS ALL") {
    assertAst(singleQuery(
      ast.TerminateTransactionsClause(Right(varFor("ALL")), List.empty, yieldAll = false, None)(pos)
    ))
  }

  // Filtering

  test(
    "TERMINATE TRANSACTION 'db1-transaction-123', 'db2-transaction-456' WHERE transactionId = 'db1-transaction-123'"
  ) {
    assertAst(singleQuery(ast.TerminateTransactionsClause(
      Left(List("db1-transaction-123", "db2-transaction-456")),
      List.empty,
      yieldAll = false,
      Some(InputPosition(67, 1, 68))
    )(defaultPos)))
  }

  test("TERMINATE TRANSACTIONS YIELD username") {
    assertAst(
      singleQuery(
        ast.TerminateTransactionsClause(
          Left(List.empty),
          List(commandResultItem("username")),
          yieldAll = false,
          None
        )(pos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("username")))
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS 'db1-transaction-123', 'db2-transaction-456' YIELD *") {
    assertAst(
      singleQuery(
        ast.TerminateTransactionsClause(
          Left(List("db1-transaction-123", "db2-transaction-456")),
          List.empty,
          yieldAll = true,
          None
        )(defaultPos),
        withFromYield(returnAllItems)
      )
    )
  }

  test("TERMINATE TRANSACTIONS 'db1-transaction-123', 'db2-transaction-456', 'yield' YIELD *") {
    assertAst(
      singleQuery(
        ast.TerminateTransactionsClause(
          Left(List("db1-transaction-123", "db2-transaction-456", "yield")),
          List.empty,
          yieldAll = true,
          None
        )(pos),
        withFromYield(returnAllItems)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS $param YIELD * ORDER BY transactionId SKIP 2 LIMIT 5") {
    assertAst(
      singleQuery(
        ast.TerminateTransactionsClause(Right(parameter("param", CTAny)), List.empty, yieldAll = true, None)(pos),
        withFromYield(returnAllItems, Some(orderBy(sortItem(varFor("transactionId")))), Some(skip(2)), Some(limit(5)))
      ),
      comparePosition = false
    )
  }

  test(
    "USE db TERMINATE TRANSACTIONS 'db1-transaction-123' YIELD transactionId, username AS pp ORDER BY pp SKIP 2 LIMIT 5 WHERE length(pp) < 5 RETURN transactionId"
  ) {
    assertAst(
      singleQuery(
        use(List("db")),
        ast.TerminateTransactionsClause(
          Right(literalString("db1-transaction-123")),
          List(commandResultItem("transactionId"), commandResultItem("username", Some("pp"))),
          yieldAll = false,
          None
        )(pos),
        withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("transactionId", "pp")),
          Some(orderBy(sortItem(varFor("pp")))),
          Some(skip(2)),
          Some(limit(5)),
          Some(where(lessThan(function("length", varFor("pp")), literalInt(5L))))
        ),
        return_(variableReturnItem("transactionId"))
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS 'where' YIELD transactionId AS TRANSACTION, username AS OUTPUT") {
    assertAst(
      singleQuery(
        ast.TerminateTransactionsClause(
          Right(literalString("where")),
          List(commandResultItem("transactionId", Some("TRANSACTION")), commandResultItem("username", Some("OUTPUT"))),
          yieldAll = false,
          None
        )(pos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("TRANSACTION", "OUTPUT")))
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTION 'yield' YIELD * WHERE transactionId = 'where'") {
    assertAst(
      singleQuery(
        ast.TerminateTransactionsClause(Right(literalString("yield")), List.empty, yieldAll = true, None)(pos),
        withFromYield(returnAllItems, where = Some(where(equals(varFor("transactionId"), literalString("where")))))
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTION $yield YIELD * WHERE transactionId IN ['yield', $where]") {
    assertAst(
      singleQuery(
        ast.TerminateTransactionsClause(Right(parameter("yield", CTAny)), List.empty, yieldAll = true, None)(pos),
        withFromYield(
          returnAllItems,
          where = Some(where(in(varFor("transactionId"), listOf(literalString("yield"), parameter("where", CTAny)))))
        )
      ),
      comparePosition = false
    )
  }

  test(
    "TERMINATE TRANSACTION db1-transaction-123 WHERE transactionId IN ['db1-transaction-124', 'db1-transaction-125']"
  ) {
    assertAst(
      singleQuery(ast.TerminateTransactionsClause(
        Right(subtract(subtract(varFor("db1"), varFor("transaction")), literalInt(123))),
        List.empty,
        yieldAll = false,
        Some(InputPosition(42, 1, 43))
      )(pos)),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS ['db1-transaction-123', 'db2-transaction-456'] YIELD *") {
    assertAst(
      singleQuery(
        ast.TerminateTransactionsClause(
          Right(listOfString("db1-transaction-123", "db2-transaction-456")),
          List.empty,
          yieldAll = true,
          None
        )(pos),
        withFromYield(returnAllItems)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS x*2 YIELD transactionId AS TRANSACTION, database AS SHOW") {
    assertAst(
      singleQuery(
        ast.TerminateTransactionsClause(
          Right(multiply(varFor("x"), literalInt(2))),
          List(commandResultItem("transactionId", Some("TRANSACTION")), commandResultItem("database", Some("SHOW"))),
          yieldAll = false,
          None
        )(pos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("TRANSACTION", "SHOW")))
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS where YIELD *") {
    assertAst(
      singleQuery(
        ast.TerminateTransactionsClause(
          Right(varFor("where")),
          List.empty,
          yieldAll = true,
          None
        )(pos),
        withFromYield(returnAllItems)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS yield YIELD *") {
    assertAst(
      singleQuery(
        ast.TerminateTransactionsClause(
          Right(varFor("yield")),
          List.empty,
          yieldAll = true,
          None
        )(pos),
        withFromYield(returnAllItems)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS show YIELD *") {
    assertAst(
      singleQuery(
        ast.TerminateTransactionsClause(
          Right(varFor("show")),
          List.empty,
          yieldAll = true,
          None
        )(pos),
        withFromYield(returnAllItems)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS terminate YIELD *") {
    assertAst(
      singleQuery(
        ast.TerminateTransactionsClause(
          Right(varFor("terminate")),
          List.empty,
          yieldAll = true,
          None
        )(pos),
        withFromYield(returnAllItems)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS YIELD yield") {
    assertAst(
      singleQuery(
        ast.TerminateTransactionsClause(
          Left(List.empty),
          List(commandResultItem("yield")),
          yieldAll = false,
          None
        )(pos),
        withFromYield(returnAllItems.withDefaultOrderOnColumns(List("yield")))
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS where WHERE true") {
    assertAst(
      singleQuery(
        ast.TerminateTransactionsClause(
          Right(varFor("where")),
          List.empty,
          yieldAll = false,
          Some(InputPosition(29, 1, 30))
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS yield WHERE true") {
    assertAst(
      singleQuery(
        ast.TerminateTransactionsClause(
          Right(varFor("yield")),
          List.empty,
          yieldAll = false,
          Some(InputPosition(29, 1, 30))
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS show WHERE true") {
    assertAst(
      singleQuery(
        ast.TerminateTransactionsClause(
          Right(varFor("show")),
          List.empty,
          yieldAll = false,
          Some(InputPosition(28, 1, 29))
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS terminate WHERE true") {
    assertAst(
      singleQuery(
        ast.TerminateTransactionsClause(
          Right(varFor("terminate")),
          List.empty,
          yieldAll = false,
          Some(InputPosition(33, 1, 34))
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS `yield` YIELD *") {
    assertAst(
      singleQuery(
        ast.TerminateTransactionsClause(
          Right(varFor("yield")),
          List.empty,
          yieldAll = true,
          None
        )(pos),
        withFromYield(returnAllItems)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS `where` WHERE true") {
    assertAst(
      singleQuery(
        ast.TerminateTransactionsClause(
          Right(varFor("where")),
          List.empty,
          yieldAll = false,
          Some(InputPosition(31, 1, 32))
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS YIELD a ORDER BY a WHERE a = 1") {
    assertAst(singleQuery(
      ast.TerminateTransactionsClause(
        Left(List.empty),
        List(commandResultItem("a")),
        yieldAll = false,
        None
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("a")),
        Some(orderBy(sortItem(varFor("a")))),
        where = Some(where(equals(varFor("a"), literalInt(1))))
      )
    ))
  }

  test("TERMINATE TRANSACTIONS YIELD a AS b ORDER BY b WHERE b = 1") {
    assertAst(singleQuery(
      ast.TerminateTransactionsClause(
        Left(List.empty),
        List(commandResultItem("a", Some("b"))),
        yieldAll = false,
        None
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("b")),
        Some(orderBy(sortItem(varFor("b")))),
        where = Some(where(equals(varFor("b"), literalInt(1))))
      )
    ))
  }

  test("TERMINATE TRANSACTIONS YIELD a AS b ORDER BY a WHERE a = 1") {
    assertAst(singleQuery(
      ast.TerminateTransactionsClause(
        Left(List.empty),
        List(commandResultItem("a", Some("b"))),
        yieldAll = false,
        None
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("b")),
        Some(orderBy(sortItem(varFor("b")))),
        where = Some(where(equals(varFor("b"), literalInt(1))))
      )
    ))
  }

  test("TERMINATE TRANSACTIONS YIELD a ORDER BY EXISTS { (a) } WHERE EXISTS { (a) }") {
    assertAst(singleQuery(
      ast.TerminateTransactionsClause(
        Left(List.empty),
        List(commandResultItem("a")),
        yieldAll = false,
        None
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("a")),
        Some(orderBy(sortItem(simpleExistsExpression(patternForMatch(nodePat(Some("a"))), None)))),
        where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("a"))), None)))
      )
    ))
  }

  test("TERMINATE TRANSACTIONS YIELD a ORDER BY EXISTS { (b) } WHERE EXISTS { (b) }") {
    assertAst(singleQuery(
      ast.TerminateTransactionsClause(
        Left(List.empty),
        List(commandResultItem("a")),
        yieldAll = false,
        None
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("a")),
        Some(orderBy(sortItem(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))),
        where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))
      )
    ))
  }

  test("TERMINATE TRANSACTIONS YIELD a AS b ORDER BY COUNT { (b) } WHERE EXISTS { (b) }") {
    assertAst(singleQuery(
      ast.TerminateTransactionsClause(
        Left(List.empty),
        List(commandResultItem("a", Some("b"))),
        yieldAll = false,
        None
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("b")),
        Some(orderBy(sortItem(simpleCountExpression(patternForMatch(nodePat(Some("b"))), None)))),
        where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))
      )
    ))
  }

  test("TERMINATE TRANSACTIONS YIELD a AS b ORDER BY EXISTS { (a) } WHERE COLLECT { MATCH (a) RETURN a } <> []") {
    assertAst(singleQuery(
      ast.TerminateTransactionsClause(
        Left(List.empty),
        List(commandResultItem("a", Some("b"))),
        yieldAll = false,
        None
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

  test("TERMINATE TRANSACTIONS YIELD a AS b ORDER BY b + COUNT { () } WHERE b OR EXISTS { () }") {
    assertAst(singleQuery(
      ast.TerminateTransactionsClause(
        Left(List.empty),
        List(commandResultItem("a", Some("b"))),
        yieldAll = false,
        None
      )(pos),
      withFromYield(
        returnAllItems.withDefaultOrderOnColumns(List("b")),
        Some(orderBy(sortItem(add(varFor("b"), simpleCountExpression(patternForMatch(nodePat()), None))))),
        where = Some(where(or(varFor("b"), simpleExistsExpression(patternForMatch(nodePat()), None))))
      )
    ))
  }

  test(
    "TERMINATE TRANSACTIONS YIELD a AS b ORDER BY a + EXISTS { () } WHERE a OR ALL (x IN [1, 2] WHERE x IS :: INT)"
  ) {
    assertAst(singleQuery(
      ast.TerminateTransactionsClause(
        Left(List.empty),
        List(commandResultItem("a", Some("b"))),
        yieldAll = false,
        None
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
    "TERMINATE TRANSACTIONS 'id', 'id' YIELD username as transactionId, transactionId as username WHERE size(transactionId) > 0 RETURN transactionId as username"
  ) {
    assertAst(singleQuery(
      ast.TerminateTransactionsClause(
        Left(List("id", "id")),
        List(
          commandResultItem("username", Some("transactionId")),
          commandResultItem("transactionId", Some("username"))
        ),
        yieldAll = false,
        None
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

  test("TERMINATE TRANSACTIONS 'db1-transaction-123' YIELD") {
    // missing what is yielded
    failsParsing[Statements]
  }

  test("TERMINATE TRANSACTIONS 'db1-transaction-123' YIELD * YIELD *") {
    failsParsing[Statements]
  }

  test("TERMINATE TRANSACTIONS 'db1-transaction-123' WHERE transactionId = 'db1-transaction-123' YIELD *") {
    failsParsing[Statements]
  }

  test("TERMINATE TRANSACTIONS 'db1-transaction-123' WHERE transactionId = 'db1-transaction-123' RETURN *") {
    // Missing YIELD
    failsParsing[Statements]
  }

  test("TERMINATE TRANSACTIONS 'db1-transaction-123' YIELD a b RETURN *") {
    failsParsing[Statements]
  }

  test("TERMINATE TRANSACTIONS 'db1-transaction-123' YIELD (123 + xyz) AS foo") {
    failsParsing[Statements]
  }

  test("TERMINATE TRANSACTIONS 'db1-transaction-123' RETURN *") {
    failsParsing[Statements]
  }

  test("TERMINATE TRANSACTION db-transaction-123, abc") {
    failsParsing[Statements]
  }

  test("TERMINATE TRANSACTIONS 'db-transaction-123', $param") {
    failsParsing[Statements]
  }

  test("TERMINATE TRANSACTIONS $param, 'db-transaction-123'") {
    failsParsing[Statements]
  }

  test("TERMINATE TRANSACTIONS $param, $param2") {
    failsParsing[Statements]
  }

  test("TERMINATE TRANSACTIONS ['db1-transaction-123', 'db2-transaction-456'], abc") {
    failsParsing[Statements]
  }

  test("TERMINATE TRANSACTION foo, 'abc'") {
    failsParsing[Statements]
  }

  test("TERMINATE TRANSACTION x+2, abc") {
    failsParsing[Statements]
  }

  test("TERMINATE CURRENT USER TRANSACTION") {
    failsParsing[Statements]
  }

  test("TERMINATE USER user TRANSACTION") {
    failsParsing[Statements]
  }

  test("TERMINATE TRANSACTION EXECUTED BY USER user") {
    failsParsing[Statements]
  }

  test("TERMINATE ALL TRANSACTIONS") {
    failsParsing[Statements]
  }

  // Invalid clause order

  for (prefix <- Seq("USE neo4j", "")) {
    test(s"$prefix TERMINATE TRANSACTIONS WITH * MATCH (n) RETURN n") {
      // Can't parse WITH after TERMINATE
      // parses varFor("WITH") * function("MATCH", varFor("n"))
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'RETURN': expected")
    }

    test(s"$prefix TERMINATE TRANSACTIONS YIELD * WITH * MATCH (n) RETURN n") {
      // Can't parse WITH after TERMINATE
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix UNWIND range(1,10) as b TERMINATE TRANSACTIONS YIELD * RETURN *") {
      // Can't parse TERMINATE  after UNWIND
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'TERMINATE': expected")
    }

    test(s"$prefix TERMINATE TRANSACTIONS WITH name, type RETURN *") {
      // Can't parse WITH after TERMINATE
      // parses varFor("WITH")
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'name': expected")
    }

    test(s"$prefix WITH 'n' as n TERMINATE TRANSACTIONS YIELD name RETURN name as numIndexes") {
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'TERMINATE': expected")
    }

    test(s"$prefix TERMINATE TRANSACTIONS RETURN name as numIndexes") {
      // parses varFor("RETURN")
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'name': expected")
    }

    test(s"$prefix TERMINATE TRANSACTIONS WITH 1 as c RETURN name as numIndexes") {
      // parses varFor("WITH")
      assertFailsWithMessageStart[Statements](testName, "Invalid input '1': expected")
    }

    test(s"$prefix TERMINATE TRANSACTIONS WITH 1 as c") {
      // parses varFor("WITH")
      assertFailsWithMessageStart[Statements](testName, "Invalid input '1': expected")
    }

    test(s"$prefix TERMINATE TRANSACTIONS YIELD a WITH a RETURN a") {
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix TERMINATE TRANSACTIONS UNWIND as as a RETURN a") {
      // parses varFor("UNWIND")
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'as': expected")
    }

    test(s"$prefix TERMINATE TRANSACTIONS YIELD as UNWIND as as a RETURN a") {
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'UNWIND': expected")
    }

    test(s"$prefix TERMINATE TRANSACTIONS RETURN id2 YIELD id2") {
      // parses varFor("RETURN")
      assertFailsWithMessageStart[Statements](testName, "Invalid input 'id2': expected")
    }
  }

}
