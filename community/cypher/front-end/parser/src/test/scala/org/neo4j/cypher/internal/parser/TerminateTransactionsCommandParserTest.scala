/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.parser

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.parboiled.scala.Rule1

/* Tests for terminating transactions */
class TerminateTransactionsCommandParserTest extends ParserAstTest[ast.Statement]
                                             with Statement
                                             with ast.AstConstructionTestSupport {
  implicit val parser: Rule1[ast.Statement] = Statement

  Seq("TRANSACTION", "TRANSACTIONS").foreach { transactionKeyword =>

    test(s"TERMINATE $transactionKeyword") {
      yields(_ => query(ast.TerminateTransactionsClause(Left(List.empty))(pos)))
    }

    test(s"TERMINATE $transactionKeyword 'db1-transaction-123'") {
      yields(_ => query(ast.TerminateTransactionsClause(Left(List("db1-transaction-123")))(pos)))
    }

    test(s"""TERMINATE $transactionKeyword "db1-transaction-123"""") {
      yields(_ => query(ast.TerminateTransactionsClause(Left(List("db1-transaction-123")))(pos)))
    }

    test(s"TERMINATE $transactionKeyword 'my.db-transaction-123'") {
      yields(_ => query(ast.TerminateTransactionsClause(Left(List("my.db-transaction-123")))(pos)))
    }

    test(s"TERMINATE $transactionKeyword $$param") {
      yields(_ => query(ast.TerminateTransactionsClause(Right(parameter("param", CTAny)))(pos)))
    }

    test(s"""TERMINATE $transactionKeyword 'db1 - transaction - 123', "db2-transaction-45a6"""") {
      yields(_ => query(ast.TerminateTransactionsClause(Left(List("db1 - transaction - 123", "db2-transaction-45a6")))(pos)))
    }

    test(s"TERMINATE $transactionKeyword 'yield-transaction-123'") {
      yields(_ => query(ast.TerminateTransactionsClause(Left(List("yield-transaction-123")))(pos)))
    }

    test(s"TERMINATE $transactionKeyword 'where-transaction-123'") {
      yields(_ => query(ast.TerminateTransactionsClause(Left(List("where-transaction-123")))(pos)))
    }

    test(s"USE db TERMINATE $transactionKeyword 'db1-transaction-123'") {
      yields(_ => query(use(varFor("db")), ast.TerminateTransactionsClause(Left(List("db1-transaction-123")))(pos)))
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
    test(s"$prefix TERMINATE TRANSACTIONS YIELD * WITH * MATCH (n) RETURN n") {
      // Can't parse WITH after TERMINATE
      failsToParse
    }

    test(s"$prefix UNWIND range(1,10) as b TERMINATE TRANSACTIONS YIELD * RETURN *") {
      // Can't parse TERMINATE  after UNWIND
      failsToParse
    }

    test(s"$prefix TERMINATE TRANSACTIONS WITH name, type RETURN *") {
      // Can't parse WITH after TERMINATE
      failsToParse
    }

    test(s"$prefix WITH 'n' as n TERMINATE TRANSACTIONS YIELD name RETURN name as numIndexes") {
      failsToParse
    }

    test(s"$prefix TERMINATE TRANSACTIONS RETURN name as numIndexes") {
      failsToParse
    }

    test(s"$prefix TERMINATE TRANSACTIONS WITH 1 as c RETURN name as numIndexes") {
      failsToParse
    }

    test(s"$prefix TERMINATE TRANSACTIONS WITH 1 as c") {
      failsToParse
    }

    test(s"$prefix TERMINATE TRANSACTIONS YIELD a WITH a RETURN a") {
      failsToParse
    }

    test(s"$prefix TERMINATE TRANSACTIONS YIELD as UNWIND as as a RETURN a") {
      failsToParse
    }

    test(s"$prefix TERMINATE TRANSACTIONS YIELD id TERMINATE TRANSACTIONS YIELD id2 RETURN id2") {
      failsToParse
    }

    test(s"$prefix TERMINATE TRANSACTIONS RETURN id2 YIELD id2") {
      failsToParse
    }
  }

}
