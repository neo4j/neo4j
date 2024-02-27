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
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.util.InputPosition

class CallInTransactionSemanticAnalysisTest extends SemanticAnalysisTestSuite {

  test("nested CALL { ... } IN TRANSACTIONS") {
    val query = "CALL { CALL { CREATE (x) } IN TRANSACTIONS } IN TRANSACTIONS RETURN 1 AS result"
    expectErrorsFrom(
      query,
      Set(
        SemanticError("Nested CALL { ... } IN TRANSACTIONS is not supported", InputPosition(7, 1, 8))
      )
    )
  }

  test("regular CALL nested in CALL { ... } IN TRANSACTIONS") {
    val query = "CALL { CALL { CREATE (x) } } IN TRANSACTIONS RETURN 1 AS result"
    expectNoErrorsFrom(query)
  }

  test("CALL { ... } IN TRANSACTIONS nested in a regular CALL") {
    val query = "CALL { CALL { CREATE (x) } IN TRANSACTIONS } RETURN 1 AS result"
    expectErrorsFrom(
      query,
      Set(
        SemanticError("CALL { ... } IN TRANSACTIONS nested in a regular CALL is not supported", InputPosition(7, 1, 8))
      )
    )
  }

  test("CALL { ... } IN TRANSACTIONS nested in a regular CALL and nested CALL { ... } IN TRANSACTIONS") {
    val query = "CALL { CALL { CALL { CREATE (x) } IN TRANSACTIONS } IN TRANSACTIONS } RETURN 1 AS result"
    expectErrorsFrom(
      query,
      Set(
        SemanticError("Nested CALL { ... } IN TRANSACTIONS is not supported", InputPosition(14, 1, 15)),
        SemanticError("CALL { ... } IN TRANSACTIONS nested in a regular CALL is not supported", InputPosition(7, 1, 8))
      )
    )
  }

  test("CALL { ... } IN TRANSACTIONS in a UNION") {
    val query =
      """CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN 1 AS result
        |UNION
        |CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN 2 AS result
        |UNION
        |CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN 3 AS result""".stripMargin
    expectErrorsFrom(
      query,
      List(
        SemanticError("CALL { ... } IN TRANSACTIONS in a UNION is not supported", InputPosition(0, 1, 1)),
        SemanticError("CALL { ... } IN TRANSACTIONS in a UNION is not supported", InputPosition(61, 4, 1)),
        SemanticError("CALL { ... } IN TRANSACTIONS in a UNION is not supported", InputPosition(122, 7, 1))
      )
    )
  }

  test("CALL { ... } IN TRANSACTIONS in first part of UNION") {
    val query =
      """CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN 1 AS result
        |UNION
        |RETURN 2 AS result""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError("CALL { ... } IN TRANSACTIONS in a UNION is not supported", InputPosition(0, 1, 1))
      )
    )
  }

  test("CALL { ... } IN TRANSACTIONS in second part of UNION") {
    val query =
      """RETURN 1 AS result
        |UNION
        |CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN 2 AS result""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError("CALL { ... } IN TRANSACTIONS in a UNION is not supported", InputPosition(25, 3, 1))
      )
    )
  }

  test("CALL { ... } IN TRANSACTIONS with a preceding write clause") {
    val query =
      """CREATE (foo)
        |WITH foo AS foo
        |CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN foo AS foo""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError("CALL { ... } IN TRANSACTIONS after a write clause is not supported", InputPosition(29, 3, 1))
      )
    )
  }

  test("Multiple CALL { ... } IN TRANSACTIONS with preceding write clauses") {
    val query =
      """CREATE (foo)
        |WITH foo AS foo
        |CALL { CREATE (x) } IN TRANSACTIONS
        |CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN foo AS foo""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError("CALL { ... } IN TRANSACTIONS after a write clause is not supported", InputPosition(29, 3, 1)),
        SemanticError("CALL { ... } IN TRANSACTIONS after a write clause is not supported", InputPosition(65, 4, 1))
      )
    )
  }

  test("Multiple CALL { ... } IN TRANSACTIONS with a write clause between them") {
    val query =
      """CALL { CREATE (x) } IN TRANSACTIONS
        |CREATE (foo)
        |WITH foo AS foo
        |CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN foo AS foo""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError("CALL { ... } IN TRANSACTIONS after a write clause is not supported", InputPosition(65, 4, 1))
      )
    )
  }

  test("CALL { ... } IN TRANSACTIONS with a preceding nested write clause") {
    val query =
      """CALL { CREATE (foo) RETURN foo AS foo }
        |WITH foo AS foo
        |CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN foo AS foo""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError("CALL { ... } IN TRANSACTIONS after a write clause is not supported", InputPosition(56, 3, 1))
      )
    )
  }

  test("CALL { ... } IN TRANSACTIONS with a preceding nested write clause in a unit subquery") {
    val query =
      """CALL { CREATE (x) }
        |WITH 1 AS foo
        |CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN foo AS foo""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError("CALL { ... } IN TRANSACTIONS after a write clause is not supported", InputPosition(34, 3, 1))
      )
    )
  }

  test("Multiple CALL { ... } IN TRANSACTIONS that contain write clauses, but no write clauses in between") {
    val query =
      """CALL { CREATE (x) } IN TRANSACTIONS
        |WITH 1 AS foo
        |CALL { CREATE (y) } IN TRANSACTIONS
        |RETURN foo AS foo""".stripMargin
    expectNoErrorsFrom(query)
  }

  test("CALL { ... } IN TRANSACTIONS with a following write clause") {
    val query =
      """CALL { CREATE (x) } IN TRANSACTIONS
        |CREATE (foo)
        |RETURN foo AS foo""".stripMargin
    expectNoErrorsFrom(query)
  }

  test("CALL IN TRANSACTIONS with batchSize 1") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN TRANSACTIONS OF 1 ROW
        |""".stripMargin
    expectNoErrorsFrom(query)
  }

  test("CALL IN TRANSACTIONS with batchSize 0") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN TRANSACTIONS OF 0 ROWS
        |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError("Invalid input. '0' is not a valid value. Must be a positive integer.", InputPosition(40, 3, 22))
      )
    )
  }

  test("CALL IN TRANSACTIONS with batchSize -1") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN TRANSACTIONS OF -1 ROWS
        |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError("Invalid input. '-1' is not a valid value. Must be a positive integer.", InputPosition(40, 3, 22))
      )
    )
  }

  test("CALL IN TRANSACTIONS with batchSize 1.5") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN TRANSACTIONS OF 1.5 ROWS
        |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "Invalid input. '1.5' is not a valid value. Must be a positive integer.",
          InputPosition(40, 3, 22)
        ),
        SemanticError("Type mismatch: expected Integer but was Float", InputPosition(40, 3, 22))
      )
    )
  }

  test("CALL IN TRANSACTIONS with batchSize 'foo'") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN TRANSACTIONS OF 'foo' ROWS
        |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "Invalid input. 'foo' is not a valid value. Must be a positive integer.",
          InputPosition(40, 3, 22)
        ),
        SemanticError("Type mismatch: expected Integer but was String", InputPosition(40, 3, 22))
      )
    )
  }

  test("CALL IN TRANSACTIONS with batchSize NULL") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN TRANSACTIONS OF NULL ROWS
        |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "Invalid input. 'NULL' is not a valid value. Must be a positive integer.",
          InputPosition(40, 3, 22)
        )
      )
    )
  }

  test("CALL IN TRANSACTIONS with batchSize larger than Long.Max") {
    val batchSize = Long.MaxValue.toString + "0"
    val query =
      s"""CALL {
         |  CREATE ()
         |} IN TRANSACTIONS OF $batchSize ROWS
         |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError("integer is too large", InputPosition(40, 3, 22))
      )
    )
  }

  test("CALL IN TRANSACTIONS with batchSize with a variable reference") {
    val query =
      s"""WITH 1 AS b
         |CALL {
         |  CREATE ()
         |} IN TRANSACTIONS OF b ROWS
         |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "It is not allowed to refer to variables in OF ... ROWS, so that the value for OF ... ROWS can be statically calculated.",
          InputPosition(52, 4, 22)
        )
      )
    )
  }

  test("CALL IN TRANSACTIONS with batchSize with a PatternExpression") {
    val query =
      s"""CALL {
         |  CREATE ()
         |} IN TRANSACTIONS OF size(()--()) ROWS
         |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "It is not allowed to use patterns in the expression for OF ... ROWS, so that the value for OF ... ROWS can be statically calculated.",
          InputPosition(40, 3, 22)
        )
      )
    )
  }

  test("CALL IN TRANSACTIONS with batchSize with a PatternComprehension") {
    val query =
      s"""CALL {
         |  CREATE ()
         |} IN TRANSACTIONS OF [path IN ()--() | 5] ROWS
         |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "It is not allowed to use patterns in the expression for OF ... ROWS, so that the value for OF ... ROWS can be statically calculated.",
          InputPosition(40, 3, 22)
        ),
        SemanticError("Type mismatch: expected Integer but was List<Integer>", InputPosition(40, 3, 22))
      )
    )
  }

  test("CALL IN TRANSACTIONS with batchSize with a CountExpression") {
    val query =
      s"""CALL {
         |  CREATE ()
         |} IN TRANSACTIONS OF COUNT { ()--() } ROWS
         |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "It is not allowed to use patterns in the expression for OF ... ROWS, so that the value for OF ... ROWS can be statically calculated.",
          InputPosition(40, 3, 22)
        )
      )
    )
  }

  test("CALL IN TRANSACTIONS with concurrency 1") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN 1 CONCURRENT TRANSACTIONS
        |""".stripMargin
    expectNoErrorsFrom(query)
  }

  test("CALL IN TRANSACTIONS with concurrency 0") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN 0 CONCURRENT TRANSACTIONS
        |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError("Invalid input. '0' is not a valid value. Must be a positive integer.", InputPosition(24, 3, 6))
      )
    )
  }

  test("CALL IN TRANSACTIONS with concurrency -1") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN -1 CONCURRENT TRANSACTIONS
        |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError("Invalid input. '-1' is not a valid value. Must be a positive integer.", InputPosition(24, 3, 6))
      )
    )
  }

  test("CALL IN TRANSACTIONS with concurrency 1.5") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN 1.5 CONCURRENT TRANSACTIONS
        |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "Invalid input. '1.5' is not a valid value. Must be a positive integer.",
          InputPosition(24, 3, 6)
        ),
        SemanticError("Type mismatch: expected Integer but was Float", InputPosition(24, 3, 6))
      )
    )
  }

  test("CALL IN TRANSACTIONS with concurrency 'foo'") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN 'foo' CONCURRENT TRANSACTIONS
        |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "Invalid input. 'foo' is not a valid value. Must be a positive integer.",
          InputPosition(24, 3, 6)
        ),
        SemanticError("Type mismatch: expected Integer but was String", InputPosition(24, 3, 6))
      )
    )
  }

  test("CALL IN TRANSACTIONS with concurrency NULL") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN NULL CONCURRENT TRANSACTIONS
        |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "Invalid input. 'NULL' is not a valid value. Must be a positive integer.",
          InputPosition(24, 3, 6)
        )
      )
    )
  }

  test("CALL IN TRANSACTIONS with concurrency larger than Long.Max") {
    val concurrency = Long.MaxValue.toString + "0"
    val query =
      s"""CALL {
         |  CREATE ()
         |} IN $concurrency CONCURRENT TRANSACTIONS
         |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError("integer is too large", InputPosition(24, 3, 6))
      )
    )
  }

  test("CALL IN TRANSACTIONS with concurrency as a variable reference") {
    val query =
      s"""WITH 1 AS b
         |CALL {
         |  CREATE ()
         |} IN b CONCURRENT TRANSACTIONS
         |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "It is not allowed to refer to variables in IN ... CONCURRENT, so that the value for IN ... CONCURRENT can be statically calculated.",
          InputPosition(36, 4, 6)
        )
      )
    )
  }

  test("CALL IN TRANSACTIONS with concurrency as a size PatternExpression") {
    val query =
      s"""CALL {
         |  CREATE ()
         |} IN size(()--()) CONCURRENT TRANSACTIONS
         |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "It is not allowed to use patterns in the expression for IN ... CONCURRENT, so that the value for IN ... CONCURRENT can be statically calculated.",
          InputPosition(24, 3, 6)
        )
      )
    )
  }

  test("CALL IN TRANSACTIONS with concurrency as a path PatternComprehension") {
    val query =
      s"""CALL {
         |  CREATE ()
         |} IN [path IN ()--() | 5] CONCURRENT TRANSACTIONS
         |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "It is not allowed to use patterns in the expression for IN ... CONCURRENT, so that the value for IN ... CONCURRENT can be statically calculated.",
          InputPosition(24, 3, 6)
        ),
        SemanticError("Type mismatch: expected Integer but was List<Integer>", InputPosition(24, 3, 6))
      )
    )
  }

  test("CALL IN TRANSACTIONS with concurrency as a CountExpression") {
    val query =
      s"""CALL {
         |  CREATE ()
         |} IN COUNT { ()--() } CONCURRENT TRANSACTIONS
         |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "It is not allowed to use patterns in the expression for IN ... CONCURRENT, so that the value for IN ... CONCURRENT can be statically calculated.",
          InputPosition(24, 3, 6)
        )
      )
    )
  }

  test("CALL IN TRANSACTIONS ON ERROR BREAK should pass semantic check") {
    val query =
      """CALL {
        |  RETURN 1 AS v
        |} IN TRANSACTIONS 
        |  ON ERROR BREAK 
        |  RETURN v
        |""".stripMargin
    expectNoErrorsFrom(query)
  }

  test("CALL IN TRANSACTIONS ON ERROR BREAK without inner and outer return should pass semantic check") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN TRANSACTIONS 
        |  ON ERROR BREAK 
        |""".stripMargin
    expectNoErrorsFrom(query)
  }

  test("CALL IN TRANSACTIONS ON ERROR BREAK with inner return and no outer return should fail semantic check") {
    val query =
      """CALL {
        |  RETURN 1 AS v
        |} IN TRANSACTIONS 
        |  ON ERROR BREAK 
        |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "Query cannot conclude with CALL (must be a RETURN clause, an update clause, a unit subquery call, or a procedure call with no YIELD)",
          InputPosition(0, 1, 1)
        )
      )
    )
  }

  test("CALL IN TRANSACTIONS ON ERROR CONTINUE REPORT STATUS AS status should pass semantic check") {
    val query =
      """CALL {
        |  RETURN 1 AS v
        |} IN TRANSACTIONS 
        |  ON ERROR CONTINUE 
        |  REPORT STATUS AS status
        |  RETURN v, status
        |""".stripMargin
    expectNoErrorsFrom(query)
  }

  test("CALL IN TRANSACTIONS ON ERROR CONTINUE REPORT STATUS without outer RETURN should fail semantic check") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN TRANSACTIONS ON ERROR CONTINUE REPORT STATUS AS status
        |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "Query cannot conclude with CALL (must be a RETURN clause, an update clause, a unit subquery call, or a procedure call with no YIELD)",
          InputPosition(0, 1, 1)
        )
      )
    )
  }

  test(
    "CALL IN TRANSACTIONS  ON ERROR CONTINUE REPORT STATUS AS <v> should fail semantic check if <v> has already been scoped"
  ) {
    val query =
      """WITH {} AS v
        |CALL {
        |  CREATE ()
        |} IN TRANSACTIONS ON ERROR CONTINUE REPORT STATUS AS v RETURN v
        |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "Variable `v` already declared",
          InputPosition(85, 4, 54)
        )
      )
    )
  }

  test("CALL IN TRANSACTIONS ON ERROR BREAK REPORT STATUS AS status should pass semantic check") {
    val query =
      """CALL {
        |  RETURN 1 AS v
        |} IN TRANSACTIONS 
        |  ON ERROR BREAK
        |  REPORT STATUS AS status
        |  RETURN v, status
        |""".stripMargin
    expectNoErrorsFrom(query)
  }

  test("CALL IN TRANSACTIONS REPORT STATUS should fail semantic check") {
    val query =
      """CALL {
        |  RETURN 1 AS v
        |} IN TRANSACTIONS
        |  REPORT STATUS AS status
        |  RETURN v, status
        |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "REPORT STATUS can only be used when specifying ON ERROR CONTINUE or ON ERROR BREAK",
          InputPosition(43, 4, 3)
        )
      )
    )
  }

  test("CALL IN TRANSACTIONS ON ERROR FAIL REPORT STATUS should fail semantic check") {
    val query =
      """CALL {
        |  RETURN 1 AS v
        |} IN TRANSACTIONS
        |  ON ERROR FAIL
        |  REPORT STATUS AS status
        |  RETURN v, status
        |""".stripMargin
    expectErrorsFrom(
      query,
      Set(
        SemanticError(
          "REPORT STATUS can only be used when specifying ON ERROR CONTINUE or ON ERROR BREAK",
          InputPosition(59, 5, 3)
        )
      )
    )
  }
}
