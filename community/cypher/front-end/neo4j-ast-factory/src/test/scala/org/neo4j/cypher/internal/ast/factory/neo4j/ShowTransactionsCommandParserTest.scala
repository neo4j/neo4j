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
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.util.symbols.CTAny

/* Tests for listing transactions */
class ShowTransactionsCommandParserTest extends JavaccParserAstTestBase[Statement] {

  implicit val parser: JavaccRule[Statement] = JavaccRule.Statement

  Seq("TRANSACTION", "TRANSACTIONS").foreach { transactionKeyword =>

    test(s"SHOW $transactionKeyword") {
      yields(_ => query(ast.ShowTransactionsClause(Left(List.empty), None, hasYield = false)(pos)))
    }

    test(s"SHOW $transactionKeyword 'db1-transaction-123'") {
      yields(_ => query(ast.ShowTransactionsClause(Left(List("db1-transaction-123")), None, hasYield = false)(pos)))
    }

    test(s"""SHOW $transactionKeyword "db1-transaction-123"""") {
      yields(_ => query(ast.ShowTransactionsClause(Left(List("db1-transaction-123")), None, hasYield = false)(pos)))
    }

    test(s"SHOW $transactionKeyword 'my.db-transaction-123'") {
      yields(_ => query(ast.ShowTransactionsClause(Left(List("my.db-transaction-123")), None, hasYield = false)(pos)))
    }

    test(s"SHOW $transactionKeyword $$param") {
      yields(_ => query(ast.ShowTransactionsClause(Right(parameter("param", CTAny)), None, hasYield = false)(pos)))
    }

    test(s"""SHOW $transactionKeyword 'db1 - transaction - 123', "db2-transaction-45a6"""") {
      yields(_ => query(ast.ShowTransactionsClause(Left(List("db1 - transaction - 123", "db2-transaction-45a6")), None, hasYield = false)(pos)))
    }

    test(s"SHOW $transactionKeyword 'yield-transaction-123'") {
      yields(_ => query(ast.ShowTransactionsClause(Left(List("yield-transaction-123")), None, hasYield = false)(pos)))
    }

    test(s"SHOW $transactionKeyword 'where-transaction-123'") {
      yields(_ => query(ast.ShowTransactionsClause(Left(List("where-transaction-123")), None, hasYield = false)(pos)))
    }

    test(s"USE db SHOW $transactionKeyword") {
      yields(_ => query(use(varFor("db")), ast.ShowTransactionsClause(Left(List.empty), None, hasYield = false)(pos)))
    }

  }

  // Filtering tests

  test("SHOW TRANSACTION WHERE transactionId = 'db1-transaction-123'") {
    yields(_ => query(ast.ShowTransactionsClause(Left(List.empty), Some(where(equals(varFor("transactionId"), literalString("db1-transaction-123")))), hasYield = false)(pos)))
  }

  test("SHOW TRANSACTIONS YIELD database") {
    yields(_ => query(ast.ShowTransactionsClause(Left(List.empty), None, hasYield = true)(pos), yieldClause(returnItems(variableReturnItem("database")))))
  }

  test("SHOW TRANSACTIONS 'db1-transaction-123', 'db2-transaction-456' YIELD *") {
    yields(_ => query(ast.ShowTransactionsClause(Left(List("db1-transaction-123", "db2-transaction-456")), None, hasYield = true)(pos), yieldClause(returnAllItems)))
  }

  test("SHOW TRANSACTIONS 'db1-transaction-123', 'db2-transaction-456', 'yield' YIELD *") {
    yields(_ => query(ast.ShowTransactionsClause(Left(List("db1-transaction-123", "db2-transaction-456", "yield")), None, hasYield = true)(pos), yieldClause(returnAllItems)))
  }

  test("SHOW TRANSACTIONS YIELD * ORDER BY transactionId SKIP 2 LIMIT 5") {
    yields(_ => query(ast.ShowTransactionsClause(Left(List.empty), None, hasYield = true)(pos),
      yieldClause(returnAllItems, Some(orderBy(sortItem(varFor("transactionId")))), Some(skip(2)), Some(limit(5)))
    ))
  }

  test("USE db SHOW TRANSACTIONS YIELD transactionId, activeLockCount AS pp WHERE pp < 50 RETURN transactionId") {
    yields(_ => query(
      use(varFor("db")),
      ast.ShowTransactionsClause(Left(List.empty), None, hasYield = true)(pos),
      yieldClause(returnItems(variableReturnItem("transactionId"), aliasedReturnItem("activeLockCount", "pp")),
        where = Some(where(lessThan(varFor("pp"), literalInt(50L))))),
      return_(variableReturnItem("transactionId"))
    ))
  }

  test("USE db SHOW TRANSACTIONS YIELD transactionId, activeLockCount AS pp ORDER BY pp SKIP 2 LIMIT 5 WHERE pp < 50 RETURN transactionId") {
    yields(_ => query(
      use(varFor("db")),
      ast.ShowTransactionsClause(Left(List.empty), None, hasYield = true)(pos),
      yieldClause(returnItems(variableReturnItem("transactionId"), aliasedReturnItem("activeLockCount", "pp")),
        Some(orderBy(sortItem(varFor("pp")))),
        Some(skip(2)),
        Some(limit(5)),
        Some(where(lessThan(varFor("pp"), literalInt(50L))))),
      return_(variableReturnItem("transactionId"))
    ))
  }

  test("SHOW TRANSACTIONS 'db1-transaction-123' YIELD transactionId AS TRANSACTION, database AS OUTPUT") {
    yields(_ => query(ast.ShowTransactionsClause(Left(List("db1-transaction-123")), None, hasYield = true)(pos),
      yieldClause(returnItems(aliasedReturnItem("transactionId", "TRANSACTION"), aliasedReturnItem("database", "OUTPUT")))))
  }

  test("SHOW TRANSACTIONS 'where' YIELD transactionId AS TRANSACTION, database AS OUTPUT") {
    yields(_ => query(ast.ShowTransactionsClause(Left(List("where")), None, hasYield = true)(pos),
      yieldClause(returnItems(aliasedReturnItem("transactionId", "TRANSACTION"), aliasedReturnItem("database", "OUTPUT")))))
  }

  test("SHOW TRANSACTION 'db1-transaction-123' WHERE transactionId = 'db1-transaction-124'") {
    yields(_ => query(ast.ShowTransactionsClause(Left(List("db1-transaction-123")), Some(where(equals(varFor("transactionId"), literalString("db1-transaction-124")))), hasYield = false)(pos)))
  }

  test("SHOW TRANSACTION 'yield' WHERE transactionId = 'where'") {
    yields(_ => query(ast.ShowTransactionsClause(Left(List("yield")), Some(where(equals(varFor("transactionId"), literalString("where")))), hasYield = false)(pos)))
  }

  test("SHOW TRANSACTION 'db1-transaction-123', 'db1-transaction-124' WHERE transactionId IN ['db1-transaction-124', 'db1-transaction-125']") {
    yields(_ => query(ast.ShowTransactionsClause(Left(List("db1-transaction-123", "db1-transaction-124")), Some(where(in(varFor("transactionId"), listOfString("db1-transaction-124", "db1-transaction-125")))), hasYield = false)(pos)))
  }

  // Negative tests

  test("SHOW TRANSACTION db-transaction-123") {
    failsToParse
  }

  test("SHOW TRANSACTIONS 'db-transaction-123', $param") {
    failsToParse
  }

  test("SHOW TRANSACTIONS $param, 'db-transaction-123'") {
    failsToParse
  }

  test("SHOW TRANSACTIONS $param, $param2") {
    failsToParse
  }

  test("SHOW TRANSACTIONS ['db1-transaction-123', 'db2-transaction-456']") {
    failsToParse
  }

  test("SHOW TRANSACTION foo") {
    failsToParse
  }

  test("SHOW TRANSACTION x+2") {
    failsToParse
  }

  test("SHOW TRANSACTIONS YIELD") {
    failsToParse
  }

  test("SHOW TRANSACTIONS YIELD * YIELD *") {
    failsToParse
  }

  test("SHOW TRANSACTIONS WHERE transactionId = 'db1-transaction-123' YIELD *") {
    failsToParse
  }

  test("SHOW TRANSACTIONS WHERE transactionId = 'db1-transaction-123' RETURN *") {
    failsToParse
  }

  test("SHOW TRANSACTIONS YIELD a b RETURN *") {
    failsToParse
  }

  test("SHOW TRANSACTIONS RETURN *") {
    failsToParse
  }

  test("SHOW CURRENT USER TRANSACTION") {
    failsToParse
  }

  test("SHOW USER user TRANSACTION") {
    failsToParse
  }

  test("SHOW TRANSACTION EXECUTED BY USER user") {
    failsToParse
  }

  test("SHOW ALL TRANSACTIONS") {
    failsToParse
  }

  test("SHOW TRANSACTIONS ALL") {
    failsToParse
  }


  // Invalid clause order

  for (prefix <- Seq("USE neo4j", "")) {
    test(s"$prefix SHOW TRANSACTIONS YIELD * WITH * MATCH (n) RETURN n") {
      // Can't parse WITH after SHOW
      failsToParse
    }

    test(s"$prefix UNWIND range(1,10) as b SHOW TRANSACTIONS YIELD * RETURN *") {
      // Can't parse SHOW  after UNWIND
      failsToParse
    }

    test(s"$prefix SHOW TRANSACTIONS WITH name, type RETURN *") {
      // Can't parse WITH after SHOW
      failsToParse
    }

    test(s"$prefix WITH 'n' as n SHOW TRANSACTIONS YIELD name RETURN name as numIndexes") {
      failsToParse
    }

    test(s"$prefix SHOW TRANSACTIONS RETURN name as numIndexes") {
      failsToParse
    }

    test(s"$prefix SHOW TRANSACTIONS WITH 1 as c RETURN name as numIndexes") {
      failsToParse
    }

    test(s"$prefix SHOW TRANSACTIONS WITH 1 as c") {
      failsToParse
    }

    test(s"$prefix SHOW TRANSACTIONS YIELD a WITH a RETURN a") {
      failsToParse
    }

    test(s"$prefix SHOW TRANSACTIONS YIELD as UNWIND as as a RETURN a") {
      failsToParse
    }

    test(s"$prefix SHOW TRANSACTIONS YIELD id SHOW TRANSACTIONS YIELD id2 RETURN id2") {
      failsToParse
    }

    test(s"$prefix SHOW TRANSACTIONS RETURN id2 YIELD id2") {
      failsToParse
    }
  }

  // Brief/verbose not allowed

  test("SHOW TRANSACTION BRIEF") {
    failsToParse
  }

  test("SHOW TRANSACTIONS BRIEF YIELD *") {
    failsToParse
  }

  test("SHOW TRANSACTIONS BRIEF WHERE transactionId = 'db1-transaction-123'") {
    failsToParse
  }

  test("SHOW TRANSACTION VERBOSE") {
    failsToParse
  }

  test("SHOW TRANSACTIONS VERBOSE YIELD *") {
    failsToParse
  }

  test("SHOW TRANSACTIONS VERBOSE WHERE transactionId = 'db1-transaction-123'") {
    failsToParse
  }

  test("SHOW TRANSACTION OUTPUT") {
    failsToParse
  }

  test("SHOW TRANSACTION BRIEF OUTPUT") {
    failsToParse
  }

  test("SHOW TRANSACTIONS BRIEF RETURN *") {
    failsToParse
  }

  test("SHOW TRANSACTION VERBOSE OUTPUT") {
    failsToParse
  }

  test("SHOW TRANSACTIONS VERBOSE RETURN *") {
    failsToParse
  }

}
