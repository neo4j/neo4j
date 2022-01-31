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
import org.neo4j.cypher.internal.util.symbols.CTAny

/* Tests for terminating transactions */
class TerminateTransactionsCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq("TRANSACTION", "TRANSACTIONS").foreach { transactionKeyword =>

    test(s"TERMINATE $transactionKeyword") {
      assertAst(query(ast.TerminateTransactionsClause(Left(List.empty))(defaultPos)))
    }

    test(s"TERMINATE $transactionKeyword 'db1-transaction-123'") {
      assertAst(query(ast.TerminateTransactionsClause(Left(List("db1-transaction-123")))(defaultPos)))
    }

    test(s"""TERMINATE $transactionKeyword "db1-transaction-123"""") {
      assertAst(query(ast.TerminateTransactionsClause(Left(List("db1-transaction-123")))(defaultPos)))
    }

    test(s"TERMINATE $transactionKeyword 'my.db-transaction-123'") {
      assertAst(query(ast.TerminateTransactionsClause(Left(List("my.db-transaction-123")))(pos)), comparePosition = false)
    }

    test(s"TERMINATE $transactionKeyword $$param") {
      assertAst(query(ast.TerminateTransactionsClause(Right(parameter("param", CTAny)))(pos)), comparePosition = false)
    }

    test(s"""TERMINATE $transactionKeyword 'db1 - transaction - 123', "db2-transaction-45a6"""") {
      assertAst(query(ast.TerminateTransactionsClause(Left(List("db1 - transaction - 123", "db2-transaction-45a6")))(defaultPos)))
    }

    test(s"TERMINATE $transactionKeyword 'yield-transaction-123'") {
      assertAst(query(ast.TerminateTransactionsClause(Left(List("yield-transaction-123")))(defaultPos)))
    }

    test(s"TERMINATE $transactionKeyword 'where-transaction-123'") {
      assertAst(query(ast.TerminateTransactionsClause(Left(List("where-transaction-123")))(pos)), comparePosition = false)
    }

    test(s"USE db TERMINATE $transactionKeyword 'db1-transaction-123'") {
      assertAst(query(use(varFor("db")), ast.TerminateTransactionsClause(Left(List("db1-transaction-123")))(pos)),
        comparePosition = false)
    }

  }

  // Filtering is not allowed

  test("TERMINATE TRANSACTIONS YIELD") {
    failsToParse
  }

  test("TERMINATE TRANSACTION WHERE transactionId = 'db1-transaction-123'") {
    failsToParse
  }

  test("TERMINATE TRANSACTIONS YIELD database") {
    failsToParse
  }

  test("TERMINATE TRANSACTIONS 'db1-transaction-123', 'db2-transaction-456' YIELD *") {
    failsToParse
  }

  test("TERMINATE TRANSACTIONS YIELD * ORDER BY transactionId SKIP 2 LIMIT 5") {
    failsToParse
  }

  test("USE db TERMINATE TRANSACTIONS YIELD transactionId, activeLockCount AS pp ORDER BY pp SKIP 2 LIMIT 5 WHERE pp < 50 RETURN transactionId") {
    failsToParse
  }

  test("TERMINATE TRANSACTIONS 'db1-transaction-123' YIELD transactionId AS TRANSACTION, database AS OUTPUT") {
    failsToParse
  }

  test("TERMINATE TRANSACTIONS YIELD * YIELD *") {
    failsToParse
  }

  test("TERMINATE TRANSACTIONS WHERE transactionId = 'db1-transaction-123' YIELD *") {
    failsToParse
  }

  test("TERMINATE TRANSACTIONS WHERE transactionId = 'db1-transaction-123' RETURN *") {
    failsToParse
  }

  test("TERMINATE TRANSACTIONS YIELD a b RETURN *") {
    failsToParse
  }

  test("TERMINATE TRANSACTIONS RETURN *") {
    failsToParse
  }

  // Negative tests

  test("TERMINATE TRANSACTION db-transaction-123") {
    failsToParse
  }

  test("TERMINATE TRANSACTIONS 'db-transaction-123', $param") {
    failsToParse
  }

  test("TERMINATE TRANSACTIONS $param, 'db-transaction-123'") {
    failsToParse
  }

  test("TERMINATE TRANSACTIONS $param, $param2") {
    failsToParse
  }

  test("TERMINATE TRANSACTIONS ['db1-transaction-123', 'db2-transaction-456']") {
    failsToParse
  }

  test("TERMINATE TRANSACTION foo") {
    failsToParse
  }

  test("TERMINATE TRANSACTION x+2") {
    failsToParse
  }

  test("TERMINATE CURRENT USER TRANSACTION") {
    failsToParse
  }

  test("TERMINATE USER user TRANSACTION") {
    failsToParse
  }

  test("TERMINATE TRANSACTION EXECUTED BY USER user") {
    failsToParse
  }

  test("TERMINATE ALL TRANSACTIONS") {
    failsToParse
  }

  test("TERMINATE TRANSACTIONS ALL") {
    failsToParse
  }

  // Invalid clause order

  for (prefix <- Seq("USE neo4j", "")) {

    test(s"$prefix TERMINATE TRANSACTIONS WITH * MATCH (n) RETURN n") {
      // Can't parse WITH after TERMINATE
      assertFailsWithMessageStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix TERMINATE TRANSACTIONS YIELD * WITH * MATCH (n) RETURN n") {
      // Can't parse WITH after TERMINATE
      assertFailsWithMessageStart(testName, "Invalid input 'YIELD': expected")
    }

    test(s"$prefix UNWIND range(1,10) as b TERMINATE TRANSACTIONS YIELD * RETURN *") {
      // Can't parse TERMINATE  after UNWIND
      assertFailsWithMessageStart(testName, "Invalid input 'TERMINATE': expected")
    }

    test(s"$prefix TERMINATE TRANSACTIONS WITH name, type RETURN *") {
      // Can't parse WITH after TERMINATE
      assertFailsWithMessageStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix WITH 'n' as n TERMINATE TRANSACTIONS YIELD name RETURN name as numIndexes") {
      assertFailsWithMessageStart(testName, "Invalid input 'TERMINATE': expected")
    }

    test(s"$prefix TERMINATE TRANSACTIONS RETURN name as numIndexes") {
      assertFailsWithMessageStart(testName, "Invalid input 'RETURN': expected")
    }

    test(s"$prefix TERMINATE TRANSACTIONS WITH 1 as c RETURN name as numIndexes") {
      assertFailsWithMessageStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix TERMINATE TRANSACTIONS WITH 1 as c") {
       assertFailsWithMessageStart(testName, "Invalid input 'WITH': expected")
    }

    test(s"$prefix TERMINATE TRANSACTIONS YIELD a WITH a RETURN a") {
      assertFailsWithMessageStart(testName, "Invalid input 'YIELD': expected")
    }

    test(s"$prefix TERMINATE TRANSACTIONS UNWIND as as a RETURN a") {
      assertFailsWithMessageStart(testName, "Invalid input 'UNWIND': expected")
    }

    test(s"$prefix TERMINATE TRANSACTIONS TERMINATE TRANSACTIONS YIELD id2 RETURN id2") {
      assertFailsWithMessageStart(testName, "Invalid input 'TERMINATE': expected")
    }

    test(s"$prefix TERMINATE TRANSACTIONS YIELD as UNWIND as as a RETURN a") {
      assertFailsWithMessageStart(testName, "Invalid input 'YIELD': expected")
    }

    test(s"$prefix TERMINATE TRANSACTIONS YIELD id TERMINATE TRANSACTIONS YIELD id2 RETURN id2") {
      assertFailsWithMessageStart(testName, "Invalid input 'YIELD': expected")
    }

    test(s"$prefix TERMINATE TRANSACTIONS RETURN id2 YIELD id2") {
      assertFailsWithMessageStart(testName, "Invalid input 'RETURN': expected")
    }
  }

}
