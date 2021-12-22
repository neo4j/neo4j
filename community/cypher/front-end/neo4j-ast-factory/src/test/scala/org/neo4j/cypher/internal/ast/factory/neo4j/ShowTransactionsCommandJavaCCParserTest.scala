/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalatest.FunSuiteLike

/* Tests for listing transactions */
class ShowTransactionsCommandJavaCCParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName with AstConstructionTestSupport {

  Seq("TRANSACTION", "TRANSACTIONS").foreach { transactionKeyword =>

    test(s"SHOW $transactionKeyword") {
      assertJavaCCAST(testName, query(ast.ShowTransactionsClause(Left(List.empty), None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW $transactionKeyword 'db1-transaction-123'") {
      assertJavaCCAST(testName, query(ast.ShowTransactionsClause(Left(List("db1-transaction-123")), None, hasYield = false)(defaultPos)))
    }

    test(s"""SHOW $transactionKeyword "db1-transaction-123"""") {
      assertJavaCCAST(testName, query(ast.ShowTransactionsClause(Left(List("db1-transaction-123")), None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW $transactionKeyword 'my.db-transaction-123'") {
      assertJavaCCAST(testName, query(ast.ShowTransactionsClause(Left(List("my.db-transaction-123")), None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW $transactionKeyword $$param") {
      assertJavaCCAST(testName, query(ast.ShowTransactionsClause(
        Right(Parameter("param", CTAny)(1, 7 + transactionKeyword.length, 6 + transactionKeyword.length)),
        None,
        hasYield = false)
      (defaultPos)))
    }

    test(s"""SHOW $transactionKeyword "db1 - transaction - 123", 'db2-transaction-45a6'""") {
      assertJavaCCAST(testName, query(ast.ShowTransactionsClause(Left(List("db1 - transaction - 123", "db2-transaction-45a6")), None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW $transactionKeyword 'yield-transaction-123'") {
      assertJavaCCAST(testName, query(ast.ShowTransactionsClause(Left(List("yield-transaction-123")), None, hasYield = false)(defaultPos)))
    }

    test(s"SHOW $transactionKeyword 'where-transaction-123'") {
      assertJavaCCAST(testName, query(ast.ShowTransactionsClause(Left(List("where-transaction-123")), None, hasYield = false)(defaultPos)))
    }

    test(s"USE db SHOW $transactionKeyword") {
      assertJavaCCAST(testName, query(use(varFor("db")), ast.ShowTransactionsClause(Left(List.empty), None, hasYield = false)(pos)), comparePosition = false)
    }
  }

  // Filtering tests

  test("SHOW TRANSACTION WHERE transactionId = 'db1-transaction-123'") {
    assertJavaCCAST(testName, query(
      ast.ShowTransactionsClause(
        Left(List.empty),
        Some(Where(
          Equals(Variable("transactionId")(1, 24, 23),
            StringLiteral("db1-transaction-123")(1, 40, 39)
          )(1, 38, 37)
        )(1, 18, 17)),
        hasYield = false)
      (defaultPos)))
  }

  test("SHOW TRANSACTIONS YIELD database") {
    assertJavaCCAST(testName,
      query(ast.ShowTransactionsClause(Left(List.empty), None, hasYield = true)(pos), yieldClause(returnItems(variableReturnItem("database")))),
      comparePosition = false)
  }

  test("SHOW TRANSACTIONS 'db1-transaction-123', 'db2-transaction-456' YIELD *") {
    assertJavaCCAST(testName, query(ast.ShowTransactionsClause(Left(List("db1-transaction-123", "db2-transaction-456")), None, hasYield = true)(defaultPos), yieldClause(returnAllItems)))
  }

  test("SHOW TRANSACTIONS 'db1-transaction-123', 'db2-transaction-456', 'yield' YIELD *") {
    assertJavaCCAST(testName,
      query(ast.ShowTransactionsClause(Left(List("db1-transaction-123", "db2-transaction-456", "yield")), None, hasYield = true)(pos),
        yieldClause(returnAllItems)),
      comparePosition = false)
  }

  test("SHOW TRANSACTIONS YIELD * ORDER BY transactionId SKIP 2 LIMIT 5") {
    assertJavaCCAST(testName,
      query(ast.ShowTransactionsClause(Left(List.empty), None, hasYield = true)(pos),
        yieldClause(returnAllItems, Some(orderBy(sortItem(varFor("transactionId")))), Some(skip(2)), Some(limit(5)))),
      comparePosition = false)
  }

  test("USE db SHOW TRANSACTIONS YIELD transactionId, activeLockCount AS pp WHERE pp < 50 RETURN transactionId") {
    assertJavaCCAST(testName, query(
      use(varFor("db")),
      ast.ShowTransactionsClause(Left(List.empty), None, hasYield = true)(pos),
      yieldClause(returnItems(variableReturnItem("transactionId"), aliasedReturnItem("activeLockCount", "pp")),
        where = Some(where(lessThan(varFor("pp"), literalInt(50L))))),
      return_(variableReturnItem("transactionId"))),
      comparePosition = false)
  }

  test("USE db SHOW TRANSACTIONS YIELD transactionId, activeLockCount AS pp ORDER BY pp SKIP 2 LIMIT 5 WHERE pp < 50 RETURN transactionId") {
    assertJavaCCAST(testName, query(
      use(varFor("db")),
      ast.ShowTransactionsClause(Left(List.empty), None, hasYield = true)(pos),
      yieldClause(returnItems(variableReturnItem("transactionId"), aliasedReturnItem("activeLockCount", "pp")),
        Some(orderBy(sortItem(varFor("pp")))),
        Some(skip(2)),
        Some(limit(5)),
        Some(where(lessThan(varFor("pp"), literalInt(50L))))),
      return_(variableReturnItem("transactionId"))),
      comparePosition = false)
  }

  test("SHOW TRANSACTIONS 'db1-transaction-123' YIELD transactionId AS TRANSACTION, database AS OUTPUT") {
    assertJavaCCAST(testName, query(ast.ShowTransactionsClause(Left(List("db1-transaction-123")), None, hasYield = true)(pos),
      yieldClause(returnItems(aliasedReturnItem("transactionId", "TRANSACTION"), aliasedReturnItem("database", "OUTPUT")))),
      comparePosition = false)
  }

  test("SHOW TRANSACTIONS 'where' YIELD transactionId AS TRANSACTION, database AS OUTPUT") {
    assertJavaCCAST(testName, query(ast.ShowTransactionsClause(Left(List("where")), None, hasYield = true)(pos),
      yieldClause(returnItems(aliasedReturnItem("transactionId", "TRANSACTION"), aliasedReturnItem("database", "OUTPUT")))),
      comparePosition = false)
  }

  test("SHOW TRANSACTION 'db1-transaction-123' WHERE transactionId = 'db1-transaction-124'") {
    assertJavaCCAST(testName,
      query(ast.ShowTransactionsClause(Left(List("db1-transaction-123")),
        Some(where(equals(varFor("transactionId"), literalString("db1-transaction-124")))), hasYield = false)(pos)),
      comparePosition = false)
  }

  test("SHOW TRANSACTION 'yield' WHERE transactionId = 'where'") {
    assertJavaCCAST(testName,
      query(ast.ShowTransactionsClause(Left(List("yield")),
        Some(where(equals(varFor("transactionId"), literalString("where")))), hasYield = false)(pos)),
      comparePosition = false)
  }

  test("SHOW TRANSACTION 'db1-transaction-123', 'db1-transaction-124' WHERE transactionId IN ['db1-transaction-124', 'db1-transaction-125']") {
    assertJavaCCAST(testName,
      query(ast.ShowTransactionsClause(Left(List("db1-transaction-123", "db1-transaction-124")),
        Some(where(in(varFor("transactionId"), listOfString("db1-transaction-124", "db1-transaction-125")))), hasYield = false)(pos)),
      comparePosition = false)
  }

  // Negative tests

  test("SHOW TRANSACTIONS db-transaction-123") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTIONS 'db-transaction-123', $param") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTIONS $param, 'db-transaction-123'") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTIONS $param, $param2") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTIONS ['db1-transaction-123', 'db2-transaction-456']") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTION foo") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTION x+2") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTIONS YIELD") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTIONS YIELD * YIELD *") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTIONS YIELD (123 + xyz)") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTIONS YIELD (123 + xyz) AS foo") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTIONS WHERE transactionId = 'db1-transaction-123' YIELD *") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTIONS WHERE transactionId = 'db1-transaction-123' RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTIONS YIELD a b RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTIONS RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW CURRENT USER TRANSACTION") {
    assertSameAST(testName)
  }

  test("SHOW USER user TRANSACTION") {
    assertJavaCCException(testName,
      """Invalid input 'TRANSACTION': expected ",", "PRIVILEGE" or "PRIVILEGES" (line 1, column 16 (offset: 15))""".stripMargin)
  }

  test("SHOW TRANSACTION EXECUTED BY USER user") {
    assertSameAST(testName)
  }

  test("SHOW ALL TRANSACTIONS") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTIONS ALL") {
    assertSameAST(testName)
  }


  // Invalid clause order

  for (prefix <- Seq("USE neo4j", "")) {
    test(s"$prefix SHOW TRANSACTIONS YIELD * WITH * MATCH (n) RETURN n") {
      // Can't parse WITH after SHOW
      assertJavaCCExceptionStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix UNWIND range(1,10) as b SHOW TRANSACTIONS YIELD * RETURN *") {
      // Can't parse SHOW  after UNWIND
      assertJavaCCExceptionStart(testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW TRANSACTIONS WITH name, type RETURN *") {
      // Can't parse WITH after SHOW
      assertJavaCCExceptionStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix WITH 'n' as n SHOW TRANSACTIONS YIELD name RETURN name as numIndexes") {
      assertJavaCCExceptionStart(testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW TRANSACTIONS RETURN name as numIndexes") {
      assertJavaCCExceptionStart(testName, "Invalid input 'RETURN': expected")
    }

    test(s"$prefix SHOW TRANSACTIONS WITH 1 as c RETURN name as numIndexes") {
      assertJavaCCExceptionStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW TRANSACTIONS WITH 1 as c") {
      assertJavaCCExceptionStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW TRANSACTIONS YIELD a WITH a RETURN a") {
      assertJavaCCExceptionStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix SHOW TRANSACTIONS YIELD as UNWIND as as a RETURN a") {
      assertJavaCCExceptionStart(testName, "Invalid input 'UNWIND': expected")
    }

    test(s"$prefix SHOW TRANSACTIONS YIELD id SHOW TRANSACTIONS YIELD id2 RETURN id2") {
      assertJavaCCExceptionStart(testName, "Invalid input 'SHOW': expected")
    }

    test(s"$prefix SHOW TRANSACTIONS RETURN id2 YIELD id2") {
      assertJavaCCExceptionStart(testName, "Invalid input 'RETURN': expected")
    }
  }

  // Brief/verbose not allowed

  test("SHOW TRANSACTION BRIEF") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTIONS BRIEF YIELD *") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTIONS BRIEF WHERE transactionId = 'db1-transaction-123'") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTION VERBOSE") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTIONS VERBOSE YIELD *") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTIONS VERBOSE WHERE transactionId = 'db1-transaction-123'") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTION OUTPUT") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTION BRIEF OUTPUT") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTIONS BRIEF RETURN *") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTION VERBOSE OUTPUT") {
    assertSameAST(testName)
  }

  test("SHOW TRANSACTIONS VERBOSE RETURN *") {
    assertSameAST(testName)
  }

}
