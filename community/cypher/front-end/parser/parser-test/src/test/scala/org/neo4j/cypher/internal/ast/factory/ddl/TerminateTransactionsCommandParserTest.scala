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
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.TerminateTransactionsClause
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.IntegerType

/* Tests for terminating transactions */
class TerminateTransactionsCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  override protected def ignorePrettifier: Boolean = true

  Seq("TRANSACTION", "TRANSACTIONS").foreach { transactionKeyword =>
    test(s"TERMINATE $transactionKeyword 'db1-transaction-123'") {
      assertAst(
        singleQuery(TerminateTransactionsClause(
          ExpressionNames(literalString("db1-transaction-123")),
          List.empty,
          yieldAll = false,
          None,
          None
        )(defaultPos))
      )
    }

    test(s"""TERMINATE $transactionKeyword "db1-transaction-123"""") {
      assertAst(
        singleQuery(TerminateTransactionsClause(
          ExpressionNames(literalString("db1-transaction-123")),
          List.empty,
          yieldAll = false,
          None,
          None
        )(defaultPos))
      )
    }

    test(s"TERMINATE $transactionKeyword 'my.db-transaction-123'") {
      assertAst(
        singleQuery(
          TerminateTransactionsClause(
            ExpressionNames(literalString("my.db-transaction-123")),
            List.empty,
            yieldAll = false,
            None,
            None
          )(pos)
        ),
        comparePosition = false
      )
    }

    test(s"TERMINATE $transactionKeyword $$param") {
      assertAst(
        singleQuery(
          TerminateTransactionsClause(
            ExpressionNames(parameter("param", CTAny)),
            List.empty,
            yieldAll = false,
            None,
            None
          )(pos)
        ),
        comparePosition = false
      )
    }

    test(s"TERMINATE $transactionKeyword $$yield") {
      assertAst(
        singleQuery(
          TerminateTransactionsClause(
            ExpressionNames(parameter("yield", CTAny)),
            List.empty,
            yieldAll = false,
            None,
            None
          )(pos)
        ),
        comparePosition = false
      )
    }

    test(s"""TERMINATE $transactionKeyword 'db1 - transaction - 123', "db2-transaction-45a6"""") {
      assertAst(
        singleQuery(TerminateTransactionsClause(
          CommaSeparatedNames(listOfString("db1 - transaction - 123", "db2-transaction-45a6")),
          List.empty,
          yieldAll = false,
          None,
          None
        )(defaultPos)),
        obfuscator = false
      )
    }

    test(s"TERMINATE $transactionKeyword 'yield-transaction-123'") {
      assertAst(
        singleQuery(TerminateTransactionsClause(
          ExpressionNames(literalString("yield-transaction-123")),
          List.empty,
          yieldAll = false,
          None,
          None
        )(defaultPos))
      )
    }

    test(s"TERMINATE $transactionKeyword 'where-transaction-123'") {
      assertAst(
        singleQuery(
          TerminateTransactionsClause(
            ExpressionNames(literalString("where-transaction-123")),
            List.empty,
            yieldAll = false,
            None,
            None
          )(pos)
        ),
        comparePosition = false
      )
    }

    test(s"USE db TERMINATE $transactionKeyword 'db1-transaction-123'") {
      assertAstVersionBased(
        cypher5 =>
          singleQuery(
            use(List("db"), !cypher5),
            TerminateTransactionsClause(
              ExpressionNames(literalString("db1-transaction-123")),
              List.empty,
              yieldAll = false,
              None,
              None
            )(pos)
          ),
        comparePosition = false
      )
    }

  }

  test("TERMINATE TRANSACTION db-transaction-123") {
    assertAst(singleQuery(
      TerminateTransactionsClause(
        ExpressionNames(subtract(subtract(varFor("db"), varFor("transaction")), literalInt(123))),
        List.empty,
        yieldAll = false,
        None,
        None
      )(pos)
    ))
  }

  test("TERMINATE TRANSACTIONS ['db1-transaction-123', 'db2-transaction-456']") {
    assertAst(singleQuery(
      TerminateTransactionsClause(
        ExpressionNames(listOfString("db1-transaction-123", "db2-transaction-456")),
        List.empty,
        yieldAll = false,
        None,
        None
      )(pos)
    ))
  }

  test("TERMINATE TRANSACTIONS ['db1-transaction-123', null]") {
    assertAst(singleQuery(
      TerminateTransactionsClause(
        ExpressionNames(listOf("db1-transaction-123", nullLiteral)),
        List.empty,
        yieldAll = false,
        None,
        None
      )(pos)
    ))
  }

  test("TERMINATE TRANSACTION foo") {
    assertAst(singleQuery(
      TerminateTransactionsClause(ExpressionNames(varFor("foo")), List.empty, yieldAll = false, None, None)(pos)
    ))
  }

  test("TERMINATE TRANSACTION x+2") {
    assertAst(singleQuery(
      TerminateTransactionsClause(
        ExpressionNames(add(varFor("x"), literalInt(2))),
        List.empty,
        yieldAll = false,
        None,
        None
      )(pos)
    ))
  }

  test("TERMINATE TRANSACTIONS ALL") {
    assertAst(singleQuery(
      TerminateTransactionsClause(ExpressionNames(varFor("ALL")), List.empty, yieldAll = false, None, None)(pos)
    ))
  }

  // Filtering

  test(
    "TERMINATE TRANSACTION 'db1-transaction-123', 'db2-transaction-456' WHERE transactionId = 'db1-transaction-123'"
  ) {
    assertAst(
      singleQuery(TerminateTransactionsClause(
        CommaSeparatedNames(listOfString("db1-transaction-123", "db2-transaction-456")),
        List.empty,
        yieldAll = false,
        None,
        Some(InputPosition(67, 1, 68))
      )(defaultPos)),
      obfuscator = false
    )
  }

  test("TERMINATE TRANSACTIONS 'id' YIELD username") {
    assertAst(
      singleQuery(
        TerminateTransactionsClause(
          ExpressionNames(literalString("id")),
          List(commandResultItem("username")),
          yieldAll = false,
          Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("username")))),
          None
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS 'db1-transaction-123', 'db2-transaction-456' YIELD *") {
    assertAst(
      singleQuery(
        TerminateTransactionsClause(
          CommaSeparatedNames(listOfString("db1-transaction-123", "db2-transaction-456")),
          List.empty,
          yieldAll = true,
          Some(withFromYield(returnAllItems)),
          None
        )(defaultPos)
      )
    )
  }

  test("TERMINATE TRANSACTIONS 'db1-transaction-123', 'db2-transaction-456', 'yield' YIELD *") {
    assertAst(
      singleQuery(
        TerminateTransactionsClause(
          CommaSeparatedNames(listOfString("db1-transaction-123", "db2-transaction-456", "yield")),
          List.empty,
          yieldAll = true,
          Some(withFromYield(returnAllItems)),
          None
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS $param YIELD * ORDER BY transactionId SKIP 2 LIMIT 5") {
    assertAst(
      singleQuery(
        TerminateTransactionsClause(
          ExpressionNames(parameter("param", CTAny)),
          List.empty,
          yieldAll = true,
          Some(withFromYield(
            returnAllItems,
            Some(orderBy(sortItem(varFor("transactionId")))),
            Some(skip(2)),
            Some(limit(5))
          )),
          None
        )(pos)
      ),
      comparePosition = false
    )
  }

  test(
    "USE db TERMINATE TRANSACTIONS 'db1-transaction-123' YIELD transactionId, username AS pp ORDER BY pp SKIP 2 LIMIT 5 WHERE length(pp) < 5 RETURN transactionId"
  ) {
    assertAstVersionBased(
      cypher5 =>
        singleQuery(
          use(List("db"), !cypher5),
          TerminateTransactionsClause(
            ExpressionNames(literalString("db1-transaction-123")),
            List(commandResultItem("transactionId"), commandResultItem("username", Some("pp"))),
            yieldAll = false,
            Some(
              withFromYield(
                returnAllItems.withDefaultOrderOnColumns(List("transactionId", "pp")),
                Some(orderBy(sortItem(varFor("pp")))),
                Some(skip(2)),
                Some(limit(5)),
                Some(where(lessThan(function("length", varFor("pp")), literalInt(5L))))
              )
            ),
            None
          )(pos),
          return_(variableReturnItem("transactionId"))
        ),
      comparePosition = false
    )
  }

  test(
    "USE db TERMINATE TRANSACTIONS 'db1-transaction-123' YIELD transactionId, username AS pp ORDER BY pp OFFSET 2 LIMIT 5 WHERE length(pp) < 5 RETURN transactionId"
  ) {
    assertAstVersionBased(
      cypher5 =>
        singleQuery(
          use(List("db"), !cypher5),
          TerminateTransactionsClause(
            ExpressionNames(literalString("db1-transaction-123")),
            List(commandResultItem("transactionId"), commandResultItem("username", Some("pp"))),
            yieldAll = false,
            Some(
              withFromYield(
                returnAllItems.withDefaultOrderOnColumns(List("transactionId", "pp")),
                Some(orderBy(sortItem(varFor("pp")))),
                Some(skip(2)),
                Some(limit(5)),
                Some(where(lessThan(function("length", varFor("pp")), literalInt(5L))))
              )
            ),
            None
          )(pos),
          return_(variableReturnItem("transactionId"))
        ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS 'where' YIELD transactionId AS TRANSACTION, username AS OUTPUT") {
    assertAst(
      singleQuery(
        TerminateTransactionsClause(
          ExpressionNames(literalString("where")),
          List(commandResultItem("transactionId", Some("TRANSACTION")), commandResultItem("username", Some("OUTPUT"))),
          yieldAll = false,
          Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("TRANSACTION", "OUTPUT")))),
          None
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTION 'yield' YIELD * WHERE transactionId = 'where'") {
    assertAst(
      singleQuery(
        TerminateTransactionsClause(
          ExpressionNames(literalString("yield")),
          List.empty,
          yieldAll = true,
          Some(withFromYield(
            returnAllItems,
            where = Some(where(equals(varFor("transactionId"), literalString("where"))))
          )),
          None
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTION $yield YIELD * WHERE transactionId IN ['yield', $where]") {
    assertAst(
      singleQuery(
        TerminateTransactionsClause(
          ExpressionNames(parameter("yield", CTAny)),
          List.empty,
          yieldAll = true,
          Some(withFromYield(
            returnAllItems,
            where = Some(where(in(varFor("transactionId"), listOf(literalString("yield"), parameter("where", CTAny)))))
          )),
          None
        )(pos)
      ),
      comparePosition = false
    )
  }

  test(
    "TERMINATE TRANSACTION db1-transaction-123 WHERE transactionId IN ['db1-transaction-124', 'db1-transaction-125']"
  ) {
    assertAst(
      singleQuery(TerminateTransactionsClause(
        ExpressionNames(subtract(subtract(varFor("db1"), varFor("transaction")), literalInt(123))),
        List.empty,
        yieldAll = false,
        None,
        Some(InputPosition(42, 1, 43))
      )(pos)),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS ['db1-transaction-123', 'db2-transaction-456'] YIELD *") {
    assertAst(
      singleQuery(
        TerminateTransactionsClause(
          ExpressionNames(listOfString("db1-transaction-123", "db2-transaction-456")),
          List.empty,
          yieldAll = true,
          Some(withFromYield(returnAllItems)),
          None
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS x*2 YIELD transactionId AS TRANSACTION, database AS SHOW") {
    assertAst(
      singleQuery(
        TerminateTransactionsClause(
          ExpressionNames(multiply(varFor("x"), literalInt(2))),
          List(commandResultItem("transactionId", Some("TRANSACTION")), commandResultItem("database", Some("SHOW"))),
          yieldAll = false,
          Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("TRANSACTION", "SHOW")))),
          None
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS where YIELD *") {
    assertAst(
      singleQuery(
        TerminateTransactionsClause(
          ExpressionNames(varFor("where")),
          List.empty,
          yieldAll = true,
          Some(withFromYield(returnAllItems)),
          None
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS yield YIELD *") {
    assertAst(
      singleQuery(
        TerminateTransactionsClause(
          ExpressionNames(varFor("yield")),
          List.empty,
          yieldAll = true,
          Some(withFromYield(returnAllItems)),
          None
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS show YIELD *") {
    assertAst(
      singleQuery(
        TerminateTransactionsClause(
          ExpressionNames(varFor("show")),
          List.empty,
          yieldAll = true,
          Some(withFromYield(returnAllItems)),
          None
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS terminate YIELD *") {
    assertAst(
      singleQuery(
        TerminateTransactionsClause(
          ExpressionNames(varFor("terminate")),
          List.empty,
          yieldAll = true,
          Some(withFromYield(returnAllItems)),
          None
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS 'id' YIELD yield") {
    assertAst(
      singleQuery(
        TerminateTransactionsClause(
          ExpressionNames(literalString("id")),
          List(commandResultItem("yield")),
          yieldAll = false,
          Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("yield")))),
          None
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS where WHERE true") {
    assertAst(
      singleQuery(
        TerminateTransactionsClause(
          ExpressionNames(varFor("where")),
          List.empty,
          yieldAll = false,
          None,
          Some(InputPosition(29, 1, 30))
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS yield WHERE true") {
    assertAst(
      singleQuery(
        TerminateTransactionsClause(
          ExpressionNames(varFor("yield")),
          List.empty,
          yieldAll = false,
          None,
          Some(InputPosition(29, 1, 30))
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS show WHERE true") {
    assertAst(
      singleQuery(
        TerminateTransactionsClause(
          ExpressionNames(varFor("show")),
          List.empty,
          yieldAll = false,
          None,
          Some(InputPosition(28, 1, 29))
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS terminate WHERE true") {
    assertAst(
      singleQuery(
        TerminateTransactionsClause(
          ExpressionNames(varFor("terminate")),
          List.empty,
          yieldAll = false,
          None,
          Some(InputPosition(33, 1, 34))
        )(pos)
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS `yield` YIELD *") {
    def expected(yieldIsEscaped: Boolean) =
      singleQuery(
        TerminateTransactionsClause(
          ExpressionNames(varFor("yield", yieldIsEscaped)),
          List.empty,
          yieldAll = true,
          Some(withFromYield(returnAllItems)),
          None
        )(pos)
      )
    parsesIn[Statement] {
      case Cypher5 => _.toAst(expected(yieldIsEscaped = true))
      case _       => _.toAst(expected(yieldIsEscaped = false))
    }
  }

  test("TERMINATE TRANSACTIONS `where` WHERE true") {
    def expected(whereIsEscaped: Boolean) =
      singleQuery(
        TerminateTransactionsClause(
          ExpressionNames(varFor("where", whereIsEscaped)),
          List.empty,
          yieldAll = false,
          None,
          Some(InputPosition(31, 1, 32))
        )(pos)
      )
    parsesIn[Statement] {
      case Cypher5 => _.toAst(expected(whereIsEscaped = true))
      case _       => _.toAst(expected(whereIsEscaped = false))
    }
  }

  test("TERMINATE TRANSACTIONS 'id' YIELD a ORDER BY a WHERE a = 1") {
    assertAst(singleQuery(
      TerminateTransactionsClause(
        ExpressionNames(literalString("id")),
        List(commandResultItem("a")),
        yieldAll = false,
        Some(withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("a")),
          Some(orderBy(sortItem(varFor("a")))),
          where = Some(where(equals(varFor("a"), literalInt(1))))
        )),
        None
      )(pos)
    ))
  }

  test("TERMINATE TRANSACTIONS 'id' YIELD a AS b ORDER BY b WHERE b = 1") {
    assertAst(singleQuery(
      TerminateTransactionsClause(
        ExpressionNames(literalString("id")),
        List(commandResultItem("a", Some("b"))),
        yieldAll = false,
        Some(withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("b")),
          Some(orderBy(sortItem(varFor("b")))),
          where = Some(where(equals(varFor("b"), literalInt(1))))
        )),
        None
      )(pos)
    ))
  }

  test("TERMINATE TRANSACTIONS 'id' YIELD a AS b ORDER BY a WHERE a = 1") {
    assertAst(singleQuery(
      TerminateTransactionsClause(
        ExpressionNames(literalString("id")),
        List(commandResultItem("a", Some("b"))),
        yieldAll = false,
        Some(withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("b")),
          Some(orderBy(sortItem(varFor("b")))),
          where = Some(where(equals(varFor("b"), literalInt(1))))
        )),
        None
      )(pos)
    ))
  }

  test("TERMINATE TRANSACTIONS 'id' YIELD a ORDER BY EXISTS { (a) } WHERE EXISTS { (a) }") {
    assertAst(singleQuery(
      TerminateTransactionsClause(
        ExpressionNames(literalString("id")),
        List(commandResultItem("a")),
        yieldAll = false,
        Some(withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("a")),
          Some(orderBy(sortItem(simpleExistsExpression(patternForMatch(nodePat(Some("a"))), None)))),
          where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("a"))), None)))
        )),
        None
      )(pos)
    ))
  }

  test("TERMINATE TRANSACTIONS 'id' YIELD a ORDER BY EXISTS { (b) } WHERE EXISTS { (b) }") {
    assertAst(singleQuery(
      TerminateTransactionsClause(
        ExpressionNames(literalString("id")),
        List(commandResultItem("a")),
        yieldAll = false,
        Some(withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("a")),
          Some(orderBy(sortItem(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))),
          where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))
        )),
        None
      )(pos)
    ))
  }

  test("TERMINATE TRANSACTIONS 'id' YIELD a AS b ORDER BY COUNT { (b) } WHERE EXISTS { (b) }") {
    assertAst(singleQuery(
      TerminateTransactionsClause(
        ExpressionNames(literalString("id")),
        List(commandResultItem("a", Some("b"))),
        yieldAll = false,
        Some(withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("b")),
          Some(orderBy(sortItem(simpleCountExpression(patternForMatch(nodePat(Some("b"))), None)))),
          where = Some(where(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))
        )),
        None
      )(pos)
    ))
  }

  test("TERMINATE TRANSACTIONS 'id' YIELD a AS b ORDER BY EXISTS { (a) } WHERE COLLECT { MATCH (a) RETURN a } <> []") {
    assertAst(singleQuery(
      TerminateTransactionsClause(
        ExpressionNames(literalString("id")),
        List(commandResultItem("a", Some("b"))),
        yieldAll = false,
        Some(withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("b")),
          Some(orderBy(sortItem(simpleExistsExpression(patternForMatch(nodePat(Some("b"))), None)))),
          where = Some(where(notEquals(
            simpleCollectExpression(patternForMatch(nodePat(Some("b"))), None, return_(returnItem(varFor("b"), "a"))),
            listOf()
          )))
        )),
        None
      )(pos)
    ))
  }

  test("TERMINATE TRANSACTIONS 'id' YIELD a AS b ORDER BY b + COUNT { () } WHERE b OR EXISTS { () }") {
    assertAst(singleQuery(
      TerminateTransactionsClause(
        ExpressionNames(literalString("id")),
        List(commandResultItem("a", Some("b"))),
        yieldAll = false,
        Some(withFromYield(
          returnAllItems.withDefaultOrderOnColumns(List("b")),
          Some(orderBy(sortItem(add(varFor("b"), simpleCountExpression(patternForMatch(nodePat()), None))))),
          where = Some(where(or(varFor("b"), simpleExistsExpression(patternForMatch(nodePat()), None))))
        )),
        None
      )(pos)
    ))
  }

  test(
    "TERMINATE TRANSACTIONS 'id' YIELD a AS b ORDER BY a + EXISTS { () } WHERE a OR ALL (x IN [1, 2] WHERE x IS :: INT)"
  ) {
    assertAst(singleQuery(
      TerminateTransactionsClause(
        ExpressionNames(literalString("id")),
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
        None
      )(pos)
    ))
  }

  test(
    "TERMINATE TRANSACTIONS 'id', 'id' YIELD username as transactionId, transactionId as username WHERE size(transactionId) > 0 RETURN transactionId as username"
  ) {
    assertAst(
      singleQuery(
        TerminateTransactionsClause(
          CommaSeparatedNames(listOfString("id", "id")),
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
          None
        )(pos),
        return_(aliasedReturnItem("transactionId", "username"))
      ),
      obfuscator = false
    )
  }

  test(
    "TERMINATE TRANSACTIONS 'id' YIELD name RETURN name ORDER BY name"
  ) {
    assertAst(
      singleQuery(
        TerminateTransactionsClause(
          ExpressionNames(literalString("id")),
          List(commandResultItem("name")),
          yieldAll = false,
          Some(withFromYield(returnAllItems.withDefaultOrderOnColumns(List("name")))),
          None
        )(pos),
        return_(orderBy(sortItem(varFor("name"))), variableReturnItem("name"))
      ),
      comparePosition = false
    )
  }

  test("TERMINATE TRANSACTIONS 'db1-transaction-123' WHERE transactionId = 'db1-transaction-123' RETURN *") {
    // Missing YIELD
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'RETURN'")
      case _ => _.toAstPositioned(singleQuery(
          TerminateTransactionsClause(
            ExpressionNames(literalString("db1-transaction-123")),
            List.empty,
            yieldAll = false,
            None,
            Some(InputPosition(45, 1, 46))
          )(defaultPos),
          returnAll
        ))
    }
  }

  test("TERMINATE TRANSACTIONS 'db1-transaction-123' RETURN *") {
    // Missing YIELD
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'RETURN'")
      case _ => _.toAstPositioned(singleQuery(
          TerminateTransactionsClause(
            ExpressionNames(literalString("db1-transaction-123")),
            List.empty,
            yieldAll = false,
            None,
            None
          )(defaultPos),
          returnAll
        ))
    }
  }

  test("TERMINATE TRANSACTIONS id WHERE true RETURN *") {
    // Missing YIELD
    parsesIn[Statements] {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'RETURN'")
      case _ => _.toAstPositioned(singleQuery(
          TerminateTransactionsClause(
            ExpressionNames(varFor("id")),
            List.empty,
            yieldAll = false,
            None,
            Some(InputPosition(26, 1, 27))
          )(defaultPos),
          returnAll
        ))
    }
  }

  // Negative tests

  test("TERMINATE TRANSACTION") {
    failsParsing[Statements].withSyntaxErrorContaining("Invalid input '': expected a string or an expression")
  }

  test("TERMINATE TRANSACTIONS") {
    failsParsing[Statements].withSyntaxErrorContaining("Invalid input '': expected a string or an expression")
  }

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

  test("TERMINATE TRANSACTIONS 'db1-transaction-123' YIELD a b RETURN *") {
    failsParsing[Statements]
  }

  test("TERMINATE TRANSACTIONS 'db1-transaction-123' YIELD (123 + xyz) AS foo") {
    failsParsing[Statements]
  }

  test("TERMINATE TRANSACTION db-transaction-123, abc") {
    failsParsing[Statements]
  }

  test("TERMINATE TRANSACTIONS 'db-transaction-123', $param") {
    failsParsing[Statements]
  }

  test("TERMINATE TRANSACTIONS 'db-transaction-123', null") {
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
    test(s"$prefix TERMINATE TRANSACTIONS RETURN id2 YIELD id2") {
      // parses varFor("RETURN")
      failsParsing[Statements].in {
        case Cypher5 => _.withSyntaxErrorContaining(
            """Invalid input 'id2': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"""
          )
        case _ => _.withSyntaxErrorContaining(
            "Invalid input 'id2': expected an expression, 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', " +
              "'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
              "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WHERE', 'WITH', 'YIELD' or <EOF>"
          )
      }
    }

    test(s"$prefix TERMINATE TRANSACTIONS tx RETURN id2 YIELD id2") {
      failsParsing[Statements].in {
        case Cypher5 => _.withSyntaxErrorContaining(
            """Invalid input 'RETURN': expected an expression, 'SHOW', 'TERMINATE', 'WHERE', 'YIELD' or <EOF>"""
          )
        case _ => _.withSyntaxErrorContaining(
            "Invalid input 'YIELD': expected an expression, ',', 'AS', 'GROUP BY', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', " +
              "'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', " +
              "'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF>"
          )
      }
    }
  }

}
