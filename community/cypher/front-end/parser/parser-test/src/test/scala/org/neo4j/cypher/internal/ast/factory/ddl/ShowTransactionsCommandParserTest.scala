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

import org.neo4j.cypher.internal.ast.CommaSeparatedNames
import org.neo4j.cypher.internal.ast.ExpressionNames
import org.neo4j.cypher.internal.ast.NoNames
import org.neo4j.cypher.internal.ast.ShowTransactionsClause
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.IntegerType

/* Tests for listing transactions */
class ShowTransactionsCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq("TRANSACTION", "TRANSACTIONS").foreach { transactionKeyword =>
    test(s"SHOW $transactionKeyword") {
      assertAstVersionBased(fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            NoNames,
            None,
            List.empty,
            yieldAll = false,
            None,
            fromCypher5
          )(defaultPos)
        )
      )
    }

    test(s"SHOW $transactionKeyword 'db1-transaction-123'") {
      assertAstVersionBased(fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            ExpressionNames(literalString("db1-transaction-123")),
            None,
            List.empty,
            yieldAll = false,
            None,
            fromCypher5
          )(defaultPos)
        )
      )
    }

    test(s"""SHOW $transactionKeyword "db1-transaction-123"""") {
      assertAstVersionBased(fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            ExpressionNames(literalString("db1-transaction-123")),
            None,
            List.empty,
            yieldAll = false,
            None,
            fromCypher5
          )(defaultPos)
        )
      )
    }

    test(s"SHOW $transactionKeyword 'my.db-transaction-123'") {
      assertAstVersionBased(fromCypher5 =>
        singleQuery(ShowTransactionsClause(
          ExpressionNames(literalString("my.db-transaction-123")),
          None,
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(defaultPos))
      )
    }

    test(s"SHOW $transactionKeyword $$param") {
      assertAstVersionBased(fromCypher5 =>
        singleQuery(ShowTransactionsClause(
          ExpressionNames(parameter("param", CTAny)),
          None,
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(defaultPos))
      )
    }

    test(s"SHOW $transactionKeyword $$where") {
      assertAstVersionBased(fromCypher5 =>
        singleQuery(ShowTransactionsClause(
          ExpressionNames(parameter("where", CTAny)),
          None,
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(defaultPos))
      )
    }

    test(s"""SHOW $transactionKeyword 'db1 - transaction - 123', "db2-transaction-45a6"""") {
      assertAstVersionBased(
        fromCypher5 =>
          singleQuery(ShowTransactionsClause(
            CommaSeparatedNames(listOfString("db1 - transaction - 123", "db2-transaction-45a6")),
            None,
            List.empty,
            yieldAll = false,
            None,
            fromCypher5
          )(defaultPos)),
        obfuscator = false
      )
    }

    test(s"SHOW $transactionKeyword 'yield-transaction-123'") {
      assertAstVersionBased(fromCypher5 =>
        singleQuery(ShowTransactionsClause(
          ExpressionNames(literalString("yield-transaction-123")),
          None,
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(defaultPos))
      )
    }

    test(s"SHOW $transactionKeyword 'where-transaction-123'") {
      assertAstVersionBased(fromCypher5 =>
        singleQuery(ShowTransactionsClause(
          ExpressionNames(literalString("where-transaction-123")),
          None,
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(defaultPos))
      )
    }

    test(s"USE db SHOW $transactionKeyword") {
      assertAstVersionBased(
        fromCypher5 =>
          singleQuery(
            use(List("db"), !fromCypher5),
            ShowTransactionsClause(NoNames, None, List.empty, yieldAll = false, None, fromCypher5)(pos)
          ),
        comparePosition = false
      )
    }

  }

  test("SHOW TRANSACTION db-transaction-123") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          ExpressionNames(subtract(subtract(varFor("db"), varFor("transaction")), literalInt(123))),
          None,
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTION 'neo4j'+'-transaction-'+3") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          ExpressionNames(add(add(literalString("neo4j"), literalString("-transaction-")), literalInt(3))),
          None,
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTION ('neo4j'+'-transaction-'+3)") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          ExpressionNames(add(add(literalString("neo4j"), literalString("-transaction-")), literalInt(3))),
          None,
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTIONS ['db1-transaction-123', 'db2-transaction-456']") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          ExpressionNames(listOfString("db1-transaction-123", "db2-transaction-456")),
          None,
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTIONS [null]") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          ExpressionNames(listOf(nullLiteral)),
          None,
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTION foo") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          ExpressionNames(varFor("foo")),
          None,
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTION x+2") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          ExpressionNames(add(varFor("x"), literalInt(2))),
          None,
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(
          pos
        )
      )
    )
  }

  test("SHOW TRANSACTIONS YIELD") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          ExpressionNames(varFor("YIELD")),
          None,
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTIONS YIELD (123 + xyz)") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          ExpressionNames(function("YIELD", add(literalInt(123), varFor("xyz")))),
          None,
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTIONS ALL") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          ExpressionNames(varFor("ALL")),
          None,
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(pos)
      )
    )
  }

  // Filtering tests

  test("SHOW TRANSACTION WHERE transactionId = 'db1-transaction-123'") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          NoNames,
          Some(where(equals(varFor("transactionId"), literalString("db1-transaction-123")))),
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(defaultPos)
      )
    )
  }

  test("SHOW TRANSACTIONS YIELD database") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            NoNames,
            None,
            List(commandResultItem("database")),
            yieldAll = false,
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("database")))),
            fromCypher5
          )(pos)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS 'db1-transaction-123', 'db2-transaction-456' YIELD *") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          CommaSeparatedNames(listOfString("db1-transaction-123", "db2-transaction-456")),
          None,
          List.empty,
          yieldAll = true,
          Some(withFromYield(returnAllItems)),
          fromCypher5
        )(defaultPos)
      )
    )
  }

  test("SHOW TRANSACTIONS 'db1-transaction-123', 'db2-transaction-456', 'yield' YIELD *") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            CommaSeparatedNames(listOfString("db1-transaction-123", "db2-transaction-456", "yield")),
            None,
            List.empty,
            yieldAll = true,
            Some(withFromYield(returnAllItems)),
            fromCypher5
          )(pos)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS YIELD * ORDER BY transactionId SKIP 2 LIMIT 5") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            NoNames,
            None,
            List.empty,
            yieldAll = true,
            Some(withFromYield(
              returnAllItems,
              Some(orderBy(sortItem(varFor("transactionId")))),
              Some(skip(2)),
              Some(limit(5))
            )),
            fromCypher5
          )(pos)
        ),
      comparePosition = false
    )
  }

  test("USE db SHOW TRANSACTIONS YIELD transactionId, activeLockCount AS pp WHERE pp < 50 RETURN transactionId") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          use(List("db"), !fromCypher5),
          ShowTransactionsClause(
            NoNames,
            None,
            List(
              commandResultItem("transactionId"),
              commandResultItem("activeLockCount", Some("pp"))
            ),
            yieldAll = false,
            Some(withFromYield(
              returnAllItems.withDefaultOrderOnColumns(List("transactionId", "pp")),
              where = Some(where(lessThan(varFor("pp"), literalInt(50L))))
            )),
            fromCypher5
          )(pos),
          return_(variableReturnItem("transactionId"))
        ),
      comparePosition = false
    )
  }

  test(
    "USE db SHOW TRANSACTIONS YIELD transactionId, activeLockCount AS pp ORDER BY pp SKIP 2 LIMIT 5 WHERE pp < 50 RETURN transactionId"
  ) {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          use(List("db"), !fromCypher5),
          ShowTransactionsClause(
            NoNames,
            None,
            List(
              commandResultItem("transactionId"),
              commandResultItem("activeLockCount", Some("pp"))
            ),
            yieldAll = false,
            Some(withFromYield(
              returnAllItems.withDefaultOrderOnColumns(List("transactionId", "pp")),
              Some(orderBy(sortItem(varFor("pp")))),
              Some(skip(2)),
              Some(limit(5)),
              Some(where(lessThan(varFor("pp"), literalInt(50L))))
            )),
            fromCypher5
          )(pos),
          return_(variableReturnItem("transactionId"))
        ),
      comparePosition = false
    )
  }

  test(
    "USE db SHOW TRANSACTIONS YIELD transactionId, activeLockCount AS pp ORDER BY pp OFFSET 2 LIMIT 5 WHERE pp < 50 RETURN transactionId"
  ) {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          use(List("db"), !fromCypher5),
          ShowTransactionsClause(
            NoNames,
            None,
            List(
              commandResultItem("transactionId"),
              commandResultItem("activeLockCount", Some("pp"))
            ),
            yieldAll = false,
            Some(withFromYield(
              returnAllItems.withDefaultOrderOnColumns(List("transactionId", "pp")),
              Some(orderBy(sortItem(varFor("pp")))),
              Some(skip(2)),
              Some(limit(5)),
              Some(where(lessThan(varFor("pp"), literalInt(50L))))
            )),
            fromCypher5
          )(pos),
          return_(variableReturnItem("transactionId"))
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS $param YIELD transactionId AS TRANSACTION, database AS OUTPUT") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            ExpressionNames(parameter("param", CTAny)),
            None,
            List(
              commandResultItem("transactionId", Some("TRANSACTION")),
              commandResultItem("database", Some("OUTPUT"))
            ),
            yieldAll = false,
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("TRANSACTION", "OUTPUT")))),
            fromCypher5
          )(pos)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS 'where' YIELD transactionId AS TRANSACTION, database AS OUTPUT") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            ExpressionNames(literalString("where")),
            None,
            List(
              commandResultItem("transactionId", Some("TRANSACTION")),
              commandResultItem("database", Some("OUTPUT"))
            ),
            yieldAll = false,
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("TRANSACTION", "OUTPUT")))),
            fromCypher5
          )(pos)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTION 'db1-transaction-123' WHERE transactionId = 'db1-transaction-124'") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(ShowTransactionsClause(
          ExpressionNames(literalString("db1-transaction-123")),
          Some(where(equals(varFor("transactionId"), literalString("db1-transaction-124")))),
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(pos)),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTION 'yield' WHERE transactionId = 'where'") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(ShowTransactionsClause(
          ExpressionNames(literalString("yield")),
          Some(where(equals(varFor("transactionId"), literalString("where")))),
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(pos)),
      comparePosition = false
    )
  }

  test(
    "SHOW TRANSACTION 'db1-transaction-123', 'db1-transaction-124' WHERE transactionId IN ['db1-transaction-124', 'db1-transaction-125']"
  ) {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(ShowTransactionsClause(
          CommaSeparatedNames(listOfString("db1-transaction-123", "db1-transaction-124")),
          Some(where(in(varFor("transactionId"), listOfString("db1-transaction-124", "db1-transaction-125")))),
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(pos)),
      comparePosition = false,
      obfuscator = false
    )
  }

  test(
    "SHOW TRANSACTION db1-transaction-123 WHERE transactionId IN ['db1-transaction-124', 'db1-transaction-125']"
  ) {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(ShowTransactionsClause(
          ExpressionNames(subtract(subtract(varFor("db1"), varFor("transaction")), literalInt(123))),
          Some(where(in(varFor("transactionId"), listOfString("db1-transaction-124", "db1-transaction-125")))),
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(pos)),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS ['db1-transaction-123', 'db2-transaction-456'] YIELD *") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            ExpressionNames(listOfString("db1-transaction-123", "db2-transaction-456")),
            None,
            List.empty,
            yieldAll = true,
            Some(withFromYield(returnAllItems)),
            fromCypher5
          )(pos)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS $x+'123' YIELD transactionId AS TRANSACTION, database AS SHOW") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            ExpressionNames(add(parameter("x", CTAny), literalString("123"))),
            None,
            List(commandResultItem("transactionId", Some("TRANSACTION")), commandResultItem("database", Some("SHOW"))),
            yieldAll = false,
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("TRANSACTION", "SHOW")))),
            fromCypher5
          )(pos)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS where YIELD *") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            ExpressionNames(varFor("where")),
            None,
            List.empty,
            yieldAll = true,
            Some(withFromYield(returnAllItems)),
            fromCypher5
          )(pos)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS yield YIELD *") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            ExpressionNames(varFor("yield")),
            None,
            List.empty,
            yieldAll = true,
            Some(withFromYield(returnAllItems)),
            fromCypher5
          )(pos)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS show YIELD *") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            ExpressionNames(varFor("show")),
            None,
            List.empty,
            yieldAll = true,
            Some(withFromYield(returnAllItems)),
            fromCypher5
          )(pos)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS terminate YIELD *") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            ExpressionNames(varFor("terminate")),
            None,
            List.empty,
            yieldAll = true,
            Some(withFromYield(returnAllItems)),
            fromCypher5
          )(pos)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS YIELD yield") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            NoNames,
            None,
            List(commandResultItem("yield")),
            yieldAll = false,
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("yield")))),
            fromCypher5
          )(pos)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS where WHERE true") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            ExpressionNames(varFor("where")),
            Some(where(trueLiteral)),
            List.empty,
            yieldAll = false,
            None,
            fromCypher5
          )(pos)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS yield WHERE true") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            ExpressionNames(varFor("yield")),
            Some(where(trueLiteral)),
            List.empty,
            yieldAll = false,
            None,
            fromCypher5
          )(pos)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS show WHERE true") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            ExpressionNames(varFor("show")),
            Some(where(trueLiteral)),
            List.empty,
            yieldAll = false,
            None,
            fromCypher5
          )(pos)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS terminate WHERE true") {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            ExpressionNames(varFor("terminate")),
            Some(where(trueLiteral)),
            List.empty,
            yieldAll = false,
            None,
            fromCypher5
          )(pos)
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS `yield` YIELD *") {
    def expected(yieldIsEscaped: Boolean, returnCypher5Types: Boolean) =
      singleQuery(
        ShowTransactionsClause(
          ExpressionNames(varFor("yield", yieldIsEscaped)),
          None,
          List.empty,
          yieldAll = true,
          Some(withFromYield(returnAllItems)),
          returnCypher5Types
        )(pos)
      )
    parsesIn[Statement] {
      case Cypher5 => _.toAst(expected(yieldIsEscaped = true, returnCypher5Types = true))
      case _       => _.toAst(expected(yieldIsEscaped = false, returnCypher5Types = false))
    }
  }

  test("SHOW TRANSACTIONS `where` WHERE true") {
    def expected(whereIsEscaped: Boolean, returnCypher5Types: Boolean) =
      singleQuery(
        ShowTransactionsClause(
          ExpressionNames(varFor("where", whereIsEscaped)),
          Some(where(trueLiteral)),
          List.empty,
          yieldAll = false,
          None,
          returnCypher5Types
        )(pos)
      )
    parsesIn[Statement] {
      case Cypher5 => _.toAst(expected(whereIsEscaped = true, returnCypher5Types = true))
      case _       => _.toAst(expected(whereIsEscaped = false, returnCypher5Types = false))
    }
  }

  test("SHOW TRANSACTIONS YIELD a ORDER BY a WHERE a = 1") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          NoNames,
          None,
          List(commandResultItem("a")),
          yieldAll = false,
          Some(withFromYield(
            returnAllItems.withDefaultOrderOnColumns(List("a")),
            Some(orderBy(sortItem(varFor("a")))),
            where = Some(where(equals(varFor("a"), literalInt(1))))
          )),
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTIONS YIELD a AS b ORDER BY b WHERE b = 1") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          NoNames,
          None,
          List(commandResultItem("a", Some("b"))),
          yieldAll = false,
          Some(withFromYield(
            returnAllItems.withDefaultOrderOnColumns(List("b")),
            Some(orderBy(sortItem(varFor("b")))),
            where = Some(where(equals(varFor("b"), literalInt(1))))
          )),
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTIONS YIELD a AS b ORDER BY a WHERE a = 1") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          NoNames,
          None,
          List(commandResultItem("a", Some("b"))),
          yieldAll = false,
          Some(withFromYield(
            returnAllItems.withDefaultOrderOnColumns(List("b")),
            Some(orderBy(sortItem(varFor("b")))),
            where = Some(where(equals(varFor("b"), literalInt(1))))
          )),
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTIONS YIELD a ORDER BY EXISTS { (a) } WHERE EXISTS { (a) }") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          NoNames,
          None,
          List(commandResultItem("a")),
          yieldAll = false,
          Some(withFromYield(
            returnAllItems.withDefaultOrderOnColumns(List("a")),
            Some(orderBy(sortItem(simpleExistsExpression(patternForMatch(nodePat(Some("a"))), None)))),
            where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("a"))), None)))
          )),
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTIONS YIELD a ORDER BY EXISTS { (b) } WHERE EXISTS { (b) }") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          NoNames,
          None,
          List(commandResultItem("a")),
          yieldAll = false,
          Some(withFromYield(
            returnAllItems.withDefaultOrderOnColumns(List("a")),
            Some(orderBy(sortItem(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))),
            where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))
          )),
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTIONS YIELD a AS b ORDER BY COUNT { (b) } WHERE EXISTS { (b) }") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          NoNames,
          None,
          List(commandResultItem("a", Some("b"))),
          yieldAll = false,
          Some(withFromYield(
            returnAllItems.withDefaultOrderOnColumns(List("b")),
            Some(orderBy(sortItem(simpleCountExpression(patternForMatch(nodePat(Some("b"))), None)))),
            where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))
          )),
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTIONS YIELD a AS b ORDER BY EXISTS { (a) } WHERE COLLECT { MATCH (a) RETURN a } <> []") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          NoNames,
          None,
          List(commandResultItem("a", Some("b"))),
          yieldAll = false,
          Some(withFromYield(
            returnAllItems.withDefaultOrderOnColumns(List("b")),
            Some(orderBy(sortItem(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))),
            where = Some(where(notEquals(
              simpleCollectExpression(
                patternForMatch(nodePat(Some("b"))),
                None,
                return_(returnItem(varFor("b"), "a"))
              ),
              listOf()
            )))
          )),
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTIONS YIELD a AS b ORDER BY b + COUNT { () } WHERE b OR EXISTS { () }") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          NoNames,
          None,
          List(commandResultItem("a", Some("b"))),
          yieldAll = false,
          Some(withFromYield(
            returnAllItems.withDefaultOrderOnColumns(List("b")),
            Some(orderBy(sortItem(add(varFor("b"), simpleCountExpression(patternForMatch(nodePat()), None))))),
            where = Some(where(or(varFor("b"), simpleExistsExpression(patternForMatch(nodePat()), None))))
          )),
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTIONS YIELD a AS b ORDER BY a + EXISTS { () } WHERE a OR ALL (x IN [1, 2] WHERE x IS :: INT)") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          NoNames,
          None,
          List(commandResultItem("a", Some("b"))),
          yieldAll = false,
          Some(withFromYield(
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
          )),
          fromCypher5
        )(pos)
      )
    )
  }

  test(
    "SHOW TRANSACTIONS 'id', 'id' YIELD username as transactionId, transactionId as username WHERE size(transactionId) > 0 RETURN transactionId as username"
  ) {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            CommaSeparatedNames(listOfString("id", "id")),
            None,
            List(
              commandResultItem("username", Some("transactionId")),
              commandResultItem("transactionId", Some("username"))
            ),
            yieldAll = false,
            Some(withFromYield(
              returnAllItems.withDefaultOrderOnColumns(List("transactionId", "username")),
              where = Some(where(
                greaterThan(size(varFor("transactionId")), literalInt(0))
              ))
            )),
            fromCypher5
          )(pos),
          return_(aliasedReturnItem("transactionId", "username"))
        ),
      obfuscator = false
    )
  }

  test(
    "SHOW TRANSACTIONS YIELD name RETURN name ORDER BY name"
  ) {
    assertAstVersionBased(
      fromCypher5 =>
        singleQuery(
          ShowTransactionsClause(
            NoNames,
            None,
            List(commandResultItem("name")),
            yieldAll = false,
            Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("name")))),
            fromCypher5
          )(pos),
          return_(orderBy(sortItem(varFor("name"))), variableReturnItem("name"))
        ),
      comparePosition = false
    )
  }

  test("SHOW TRANSACTIONS WHERE transactionId = 'db1-transaction-123' RETURN *") {
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'RETURN'")
      case _ =>
        _.toAstPositioned(singleQuery(
          ShowTransactionsClause(
            NoNames,
            Some(where(equals(varFor("transactionId"), literalString("db1-transaction-123")))),
            List.empty,
            yieldAll = false,
            None,
            returnCypher5Types = false
          )(pos),
          returnAll
        ))
    }
  }

  test("SHOW TRANSACTIONS WHERE true RETURN *") {
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'RETURN'")
      case _ =>
        _.toAstPositioned(singleQuery(
          ShowTransactionsClause(
            NoNames,
            Some(where(trueLiteral)),
            List.empty,
            yieldAll = false,
            None,
            returnCypher5Types = false
          )(pos),
          returnAll
        ))
    }
  }

  test("SHOW TRANSACTIONS RETURN *") {
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input ''")
      case _ =>
        _.toAstPositioned(singleQuery(
          ShowTransactionsClause(
            NoNames,
            None,
            List.empty,
            yieldAll = false,
            None,
            returnCypher5Types = false
          )(pos),
          returnAll
        ))
    }
  }

  // Negative tests

  test("SHOW TRANSACTION db-transaction-123, abc") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTIONS 'db-transaction-123', $param") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTIONS 'db-transaction-123', null") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTIONS $param, 'db-transaction-123'") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTIONS $param, $param2") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTIONS ['db1-transaction-123', 'db2-transaction-456'], abc") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTION foo, 'abc'") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTION x+2, abc") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTIONS YIELD * YIELD *") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTIONS YIELD (123 + xyz) AS foo") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTIONS WHERE transactionId = 'db1-transaction-123' YIELD *") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTIONS YIELD a b RETURN *") {
    failsParsing[Statements]
  }

  test("SHOW CURRENT USER TRANSACTION") {
    failsParsing[Statements]
  }

  test("SHOW USER user TRANSACTION") {
    failsParsing[Statements].withSyntaxError(
      """Invalid input 'TRANSACTION': expected 'PRIVILEGE' or 'PRIVILEGES' (line 1, column 16 (offset: 15))
        |"SHOW USER user TRANSACTION"
        |                ^""".stripMargin
    )
  }

  test("SHOW TRANSACTION EXECUTED BY USER user") {
    failsParsing[Statements]
  }

  test("SHOW ALL TRANSACTIONS") {
    failsParsing[Statements]
  }

  // Invalid clause order

  for (prefix <- Seq("USE neo4j", "")) {
    test(s"$prefix SHOW TRANSACTIONS RETURN id2 YIELD id2") {
      failsParsing[Statements].in {
        case Cypher5 =>
          // parses varFor("RETURN")
          _.withSyntaxErrorContaining(
            """Invalid input 'id2': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"""
          )
        case _ => _.withSyntaxErrorContaining(
            "Invalid input 'YIELD': expected an expression, ',', 'AS', 'GROUP BY', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', " +
              "'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', " +
              "'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF>"
          )
      }
    }
  }

  // Brief/verbose not allowed

  test("SHOW TRANSACTION BRIEF") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          ExpressionNames(varFor("BRIEF")),
          None,
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTIONS BRIEF YIELD *") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          ExpressionNames(varFor("BRIEF")),
          None,
          List.empty,
          yieldAll = true,
          Some(withFromYield(returnAllItems)),
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTIONS BRIEF WHERE transactionId = 'db1-transaction-123'") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          ExpressionNames(varFor("BRIEF")),
          Some(where(equals(varFor("transactionId"), literalString("db1-transaction-123")))),
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTION VERBOSE") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          ExpressionNames(varFor("VERBOSE")),
          None,
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTIONS VERBOSE YIELD *") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          ExpressionNames(varFor("VERBOSE")),
          None,
          List.empty,
          yieldAll = true,
          Some(withFromYield(returnAllItems)),
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTIONS VERBOSE WHERE transactionId = 'db1-transaction-123'") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          ExpressionNames(varFor("VERBOSE")),
          Some(where(equals(varFor("transactionId"), literalString("db1-transaction-123")))),
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTION OUTPUT") {
    assertAstVersionBased(fromCypher5 =>
      singleQuery(
        ShowTransactionsClause(
          ExpressionNames(varFor("OUTPUT")),
          None,
          List.empty,
          yieldAll = false,
          None,
          fromCypher5
        )(pos)
      )
    )
  }

  test("SHOW TRANSACTION BRIEF OUTPUT") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTIONS BRIEF RETURN *") {
    // Parses `BRIEF` as transaction id
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'RETURN'")
      case _ => _.toAstPositioned(singleQuery(
          ShowTransactionsClause(
            ExpressionNames(varFor("BRIEF")),
            None,
            List.empty,
            yieldAll = false,
            None,
            returnCypher5Types = false
          )(pos),
          returnAll
        ))
    }
  }

  test("SHOW TRANSACTIONS tx BRIEF RETURN *") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTION VERBOSE OUTPUT") {
    failsParsing[Statements]
  }

  test("SHOW TRANSACTIONS VERBOSE RETURN *") {
    // Parses `VERBOSE` as transaction id
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'RETURN'")
      case _ => _.toAstPositioned(singleQuery(
          ShowTransactionsClause(
            ExpressionNames(varFor("VERBOSE")),
            None,
            List.empty,
            yieldAll = false,
            None,
            returnCypher5Types = false
          )(pos),
          returnAll
        ))
    }
  }

  test("SHOW TRANSACTIONS tx VERBOSE RETURN *") {
    failsParsing[Statements]
  }

}
