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

import org.neo4j.cypher.internal.CypherVersion.Cypher5
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.p
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.gqlstatus.GqlHelper.getGql22003
import org.neo4j.gqlstatus.GqlHelper.getGql42001_42I25
import org.neo4j.gqlstatus.GqlHelper.getGql42001_42N71

class CallInTransactionSemanticAnalysisTest extends SemanticAnalysisTestSuite {

  test("nested CALL { ... } IN TRANSACTIONS") {
    val query = "CALL { CALL { CREATE (x) } IN TRANSACTIONS } IN TRANSACTIONS RETURN 1 AS result"
    run(query).hasError(
      GqlHelper.getGql42001_42N58(7, 1, 8),
      "Nested CALL { ... } IN TRANSACTIONS is not supported",
      p(7, 1, 8)
    )
  }

  test("regular CALL nested in CALL { ... } IN TRANSACTIONS") {
    val query = "CALL { CALL { CREATE (x) } } IN TRANSACTIONS RETURN 1 AS result"
    run(query).hasNoErrors
  }

  test("CALL { ... } IN TRANSACTIONS nested in a regular CALL") {
    val query = "CALL { CALL { CREATE (x) } IN TRANSACTIONS } RETURN 1 AS result"
    run(query).hasError(
      GqlHelper.getGql42001_42N58(7, 1, 8),
      "CALL { ... } IN TRANSACTIONS nested in a regular CALL is not supported",
      p(7, 1, 8)
    )
  }

  test("CALL { ... } IN TRANSACTIONS nested in a regular CALL and nested CALL { ... } IN TRANSACTIONS") {
    val query = "CALL { CALL { CALL { CREATE (x) } IN TRANSACTIONS } IN TRANSACTIONS } RETURN 1 AS result"
    run(query).hasErrors(
      SemanticError(
        GqlHelper.getGql42001_42N58(14, 1, 15),
        "Nested CALL { ... } IN TRANSACTIONS is not supported",
        p(14, 1, 15)
      ),
      SemanticError(
        GqlHelper.getGql42001_42N58(7, 1, 8),
        "CALL { ... } IN TRANSACTIONS nested in a regular CALL is not supported",
        p(7, 1, 8)
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
    run(query).hasErrors(
      SemanticError(
        GqlHelper.getGql42001_42N47(0, 1, 1),
        "CALL { ... } IN TRANSACTIONS in a UNION is not supported",
        p(0, 1, 1)
      ),
      SemanticError(
        GqlHelper.getGql42001_42N47(61, 4, 1),
        "CALL { ... } IN TRANSACTIONS in a UNION is not supported",
        p(61, 4, 1)
      ),
      SemanticError(
        GqlHelper.getGql42001_42N47(122, 7, 1),
        "CALL { ... } IN TRANSACTIONS in a UNION is not supported",
        p(122, 7, 1)
      )
    )
  }

  test("CALL { ... } IN TRANSACTIONS in first part of UNION") {
    val query =
      """CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN 1 AS result
        |UNION
        |RETURN 2 AS result""".stripMargin
    run(query).hasError(
      GqlHelper.getGql42001_42N47(0, 1, 1),
      "CALL { ... } IN TRANSACTIONS in a UNION is not supported",
      p(0, 1, 1)
    )
  }

  test("CALL { ... } IN TRANSACTIONS in second part of UNION") {
    val query =
      """RETURN 1 AS result
        |UNION
        |CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN 2 AS result""".stripMargin
    run(query).hasError(
      GqlHelper.getGql42001_42N47(25, 3, 1),
      "CALL { ... } IN TRANSACTIONS in a UNION is not supported",
      p(25, 3, 1)
    )
  }

  test("CALL { ... } IN TRANSACTIONS with a preceding write clause") {
    val query =
      """CREATE (foo)
        |WITH foo AS foo
        |CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN foo AS foo""".stripMargin
    run(query).hasError(
      getGql42001_42I25(29, 3, 1),
      "CALL { ... } IN TRANSACTIONS after a write clause is not supported",
      p(29, 3, 1)
    )
  }

  test("Multiple CALL { ... } IN TRANSACTIONS with preceding write clauses") {
    val query =
      """CREATE (foo)
        |WITH foo AS foo
        |CALL { CREATE (x) } IN TRANSACTIONS
        |CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN foo AS foo""".stripMargin
    run(query).hasErrors(
      getGql42001_42I25(29, 3, 1),
      "CALL { ... } IN TRANSACTIONS after a write clause is not supported",
      p(29, 3, 1),
      getGql42001_42I25(65, 4, 1),
      "CALL { ... } IN TRANSACTIONS after a write clause is not supported",
      p(65, 4, 1)
    )
  }

  test("Multiple CALL { ... } IN TRANSACTIONS with a write clause between them") {
    val query =
      """CALL { CREATE (x) } IN TRANSACTIONS
        |CREATE (foo)
        |WITH foo AS foo
        |CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN foo AS foo""".stripMargin
    run(query).hasError(
      getGql42001_42I25(65, 4, 1),
      "CALL { ... } IN TRANSACTIONS after a write clause is not supported",
      p(65, 4, 1)
    )
  }

  test("CALL { ... } IN TRANSACTIONS with a preceding nested write clause") {
    val query =
      """CALL { CREATE (foo) RETURN foo AS foo }
        |WITH foo AS foo
        |CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN foo AS foo""".stripMargin
    run(query).hasError(
      getGql42001_42I25(56, 3, 1),
      "CALL { ... } IN TRANSACTIONS after a write clause is not supported",
      p(56, 3, 1)
    )
  }

  test("CALL { ... } IN TRANSACTIONS with a preceding nested write clause in a unit subquery") {
    val query =
      """CALL { CREATE (x) }
        |WITH 1 AS foo
        |CALL { CREATE (x) } IN TRANSACTIONS
        |RETURN foo AS foo""".stripMargin
    run(query).hasError(
      getGql42001_42I25(34, 3, 1),
      "CALL { ... } IN TRANSACTIONS after a write clause is not supported",
      p(34, 3, 1)
    )
  }

  test("Multiple CALL { ... } IN TRANSACTIONS that contain write clauses, but no write clauses in between") {
    val query =
      """CALL { CREATE (x) } IN TRANSACTIONS
        |WITH 1 AS foo
        |CALL { CREATE (y) } IN TRANSACTIONS
        |RETURN foo AS foo""".stripMargin
    run(query).hasNoErrors
  }

  test("CALL { ... } IN TRANSACTIONS with a following write clause") {
    val query =
      """CALL { CREATE (x) } IN TRANSACTIONS
        |CREATE (foo)
        |RETURN foo AS foo""".stripMargin
    run(query).hasNoErrors
  }

  test("CALL IN TRANSACTIONS with batchSize 1") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN TRANSACTIONS OF 1 ROW
        |""".stripMargin
    run(query).hasNoErrors
  }

  test("CALL IN TRANSACTIONS with batchSize 0") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN TRANSACTIONS OF 0 ROWS
        |""".stripMargin
    run(query).hasErrors(
      SemanticError.specifiedNumberOutOfRange(
        "OF ... ROWS",
        "INTEGER NOT NULL",
        1,
        Long.MaxValue,
        "0",
        "Invalid input. '0' is not a valid value. Must be a positive integer.",
        p(40, 3, 22).withInputLength(1)
      )
    )
  }

  test("CALL IN TRANSACTIONS with batchSize -1") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN TRANSACTIONS OF -1 ROWS
        |""".stripMargin
    run(query).hasErrors(
      SemanticError.specifiedNumberOutOfRange(
        "OF ... ROWS",
        "INTEGER NOT NULL",
        1,
        Long.MaxValue,
        "-1",
        "Invalid input. '-1' is not a valid value. Must be a positive integer.",
        p(40, 3, 22).withInputLength(2)
      )
    )
  }

  test("CALL IN TRANSACTIONS with batchSize 1.5") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN TRANSACTIONS OF 1.5 ROWS
        |""".stripMargin
    run(query).hasErrors(
      SemanticError.specifiedNumberOutOfRange(
        "OF ... ROWS",
        "INTEGER NOT NULL",
        1,
        Long.MaxValue,
        "1.5",
        "Invalid input. '1.5' is not a valid value. Must be a positive integer.",
        p(40, 3, 22).withInputLength(3)
      ),
      SemanticError.typeMismatch(
        List("INTEGER"),
        "FLOAT",
        "Type mismatch: expected Integer but was Float",
        p(40, 3, 22).withInputLength(3)
      )
    )
  }

  test("CALL IN TRANSACTIONS with batchSize 'foo'") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN TRANSACTIONS OF 'foo' ROWS
        |""".stripMargin
    run(query).hasErrors(
      SemanticError.specifiedNumberOutOfRange(
        "OF ... ROWS",
        "INTEGER NOT NULL",
        1,
        Long.MaxValue,
        "foo",
        "Invalid input. 'foo' is not a valid value. Must be a positive integer.",
        p(40, 3, 22).withInputLength(5)
      ),
      SemanticError.typeMismatch(
        List("INTEGER"),
        "STRING",
        "Type mismatch: expected Integer but was String",
        p(40, 3, 22).withInputLength(5)
      )
    )
  }

  test("CALL IN TRANSACTIONS with batchSize NULL") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN TRANSACTIONS OF NULL ROWS
        |""".stripMargin
    run(query).hasErrors(
      SemanticError.specifiedNumberOutOfRange(
        "OF ... ROWS",
        "INTEGER NOT NULL",
        1,
        Long.MaxValue,
        "NULL",
        "Invalid input. 'NULL' is not a valid value. Must be a positive integer.",
        p(40, 3, 22).withInputLength(4)
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
    run(query).hasError(
      getGql22003(batchSize, 40, 3, 22),
      "integer is too large",
      p(40, 3, 22).withInputLength(batchSize.length)
    )
  }

  test("CALL IN TRANSACTIONS with batchSize with a variable reference") {
    val query =
      s"""WITH 1 AS b
         |CALL {
         |  CREATE ()
         |} IN TRANSACTIONS OF b ROWS
         |""".stripMargin
    run(query).hasError(
      GqlHelper.getGql42001_42N28("OF ... ROWS", 52, 4, 22),
      "It is not allowed to refer to variables in OF ... ROWS, so that the value for OF ... ROWS can be statically calculated.",
      p(52, 4, 22)
    )
  }

  test("CALL IN TRANSACTIONS with batchSize with a PatternExpression") {
    val query =
      s"""CALL {
         |  CREATE ()
         |} IN TRANSACTIONS OF size(()--()) ROWS
         |""".stripMargin
    run(query).hasErrors(
      SemanticError(
        GqlHelper.getGql42001_42N28("OF ... ROWS", 40, 3, 22),
        "It is not allowed to use patterns in the expression for OF ... ROWS, so that the value for OF ... ROWS can be statically calculated.",
        p(40, 3, 22)
      ),
      SemanticError.patternExpressionInSize(p(45, 3, 27))
    )
  }

  test("CALL IN TRANSACTIONS with batchSize with a PatternComprehension") {
    val query =
      s"""CALL {
         |  CREATE ()
         |} IN TRANSACTIONS OF [path IN ()--() | 5] ROWS
         |""".stripMargin
    run(query).hasErrors(
      SemanticError(
        GqlHelper.getGql42001_42N28("OF ... ROWS", 40, 3, 22),
        "It is not allowed to use patterns in the expression for OF ... ROWS, so that the value for OF ... ROWS can be statically calculated.",
        p(40, 3, 22)
      ),
      SemanticError.typeMismatch(
        List("INTEGER"),
        "LIST<INTEGER>",
        "Type mismatch: expected Integer but was List<Integer>",
        p(40, 3, 22)
      ),
      SemanticError.invalidUseOfPatternExpression(p(49, 3, 31))
    )
  }

  test("CALL IN TRANSACTIONS with batchSize with a CountExpression") {
    val query =
      s"""CALL {
         |  CREATE ()
         |} IN TRANSACTIONS OF COUNT { ()--() } ROWS
         |""".stripMargin
    run(query).hasError(
      GqlHelper.getGql42001_42N28("OF ... ROWS", 40, 3, 22),
      "It is not allowed to use patterns in the expression for OF ... ROWS, so that the value for OF ... ROWS can be statically calculated.",
      p(40, 3, 22)
    )
  }

  test("CALL IN TRANSACTIONS with concurrency 1") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN 1 CONCURRENT TRANSACTIONS
        |""".stripMargin
    run(query).hasNoErrors
  }

  test("CALL IN TRANSACTIONS with concurrency 0") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN 0 CONCURRENT TRANSACTIONS
        |""".stripMargin
    run(query).hasErrors(
      SemanticError.specifiedNumberOutOfRange(
        "IN ... CONCURRENT",
        "INTEGER NOT NULL",
        1,
        Long.MaxValue,
        "0",
        "Invalid input. '0' is not a valid value. Must be a positive integer.",
        p(24, 3, 6).withInputLength(1)
      )
    )
  }

  test("CALL IN TRANSACTIONS with concurrency -1") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN -1 CONCURRENT TRANSACTIONS
        |""".stripMargin
    run(query).hasErrors(
      SemanticError.specifiedNumberOutOfRange(
        "IN ... CONCURRENT",
        "INTEGER NOT NULL",
        1,
        Long.MaxValue,
        "-1",
        "Invalid input. '-1' is not a valid value. Must be a positive integer.",
        p(24, 3, 6).withInputLength(2)
      )
    )
  }

  test("CALL IN TRANSACTIONS with concurrency 1.5") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN 1.5 CONCURRENT TRANSACTIONS
        |""".stripMargin
    run(query).hasErrors(
      SemanticError.specifiedNumberOutOfRange(
        "IN ... CONCURRENT",
        "INTEGER NOT NULL",
        1,
        Long.MaxValue,
        "1.5",
        "Invalid input. '1.5' is not a valid value. Must be a positive integer.",
        p(24, 3, 6).withInputLength(3)
      ),
      SemanticError.typeMismatch(
        List("INTEGER"),
        "FLOAT",
        "Type mismatch: expected Integer but was Float",
        p(24, 3, 6).withInputLength(3)
      )
    )
  }

  test("CALL IN TRANSACTIONS with concurrency 'foo'") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN 'foo' CONCURRENT TRANSACTIONS
        |""".stripMargin
    run(query).hasErrors(
      SemanticError.specifiedNumberOutOfRange(
        "IN ... CONCURRENT",
        "INTEGER NOT NULL",
        1,
        Long.MaxValue,
        "foo",
        "Invalid input. 'foo' is not a valid value. Must be a positive integer.",
        p(24, 3, 6).withInputLength(5)
      ),
      SemanticError.typeMismatch(
        List("INTEGER"),
        "STRING",
        "Type mismatch: expected Integer but was String",
        p(24, 3, 6).withInputLength(5)
      )
    )
  }

  test("CALL IN TRANSACTIONS with concurrency NULL") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN NULL CONCURRENT TRANSACTIONS
        |""".stripMargin
    run(query).hasErrors(
      SemanticError.specifiedNumberOutOfRange(
        "IN ... CONCURRENT",
        "INTEGER NOT NULL",
        1,
        Long.MaxValue,
        "NULL",
        "Invalid input. 'NULL' is not a valid value. Must be a positive integer.",
        p(24, 3, 6).withInputLength(4)
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
    run(query).hasError(
      getGql22003(concurrency, 24, 3, 6),
      "integer is too large",
      p(24, 3, 6).withInputLength(concurrency.length)
    )
  }

  test("CALL IN TRANSACTIONS with concurrency as a variable reference") {
    val query =
      s"""WITH 1 AS b
         |CALL {
         |  CREATE ()
         |} IN b CONCURRENT TRANSACTIONS
         |""".stripMargin
    run(query).hasError(
      GqlHelper.getGql42001_42N28("IN ... CONCURRENT", 36, 4, 6),
      "It is not allowed to refer to variables in IN ... CONCURRENT, so that the value for IN ... CONCURRENT can be statically calculated.",
      p(36, 4, 6)
    )
  }

  test("CALL IN TRANSACTIONS with concurrency as a size PatternExpression") {
    val query =
      s"""CALL {
         |  CREATE ()
         |} IN size(()--()) CONCURRENT TRANSACTIONS
         |""".stripMargin
    run(query).hasErrors(
      SemanticError(
        GqlHelper.getGql42001_42N28("IN ... CONCURRENT", 24, 3, 6),
        "It is not allowed to use patterns in the expression for IN ... CONCURRENT, so that the value for IN ... CONCURRENT can be statically calculated.",
        p(24, 3, 6)
      ),
      SemanticError.patternExpressionInSize(p(29, 3, 11))
    )
  }

  test("CALL IN TRANSACTIONS with concurrency as a path PatternComprehension") {
    val query =
      s"""CALL {
         |  CREATE ()
         |} IN [path IN ()--() | 5] CONCURRENT TRANSACTIONS
         |""".stripMargin
    run(query).hasErrors(
      SemanticError(
        GqlHelper.getGql42001_42N28("IN ... CONCURRENT", 24, 3, 6),
        "It is not allowed to use patterns in the expression for IN ... CONCURRENT, so that the value for IN ... CONCURRENT can be statically calculated.",
        p(24, 3, 6)
      ),
      SemanticError.typeMismatch(
        List("INTEGER"),
        "LIST<INTEGER>",
        "Type mismatch: expected Integer but was List<Integer>",
        p(24, 3, 6)
      ),
      SemanticError.invalidUseOfPatternExpression(p(33, 3, 15))
    )
  }

  test("CALL IN TRANSACTIONS with concurrency as a CountExpression") {
    val query =
      s"""CALL {
         |  CREATE ()
         |} IN COUNT { ()--() } CONCURRENT TRANSACTIONS
         |""".stripMargin
    run(query).hasError(
      GqlHelper.getGql42001_42N28("IN ... CONCURRENT", 24, 3, 6),
      "It is not allowed to use patterns in the expression for IN ... CONCURRENT, so that the value for IN ... CONCURRENT can be statically calculated.",
      p(24, 3, 6)
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
    run(query).hasNoErrors
  }

  test("CALL IN TRANSACTIONS ON ERROR BREAK with inner return and no outer return should fail semantic check") {
    val query =
      """CALL {
        |  RETURN 1 AS v
        |} IN TRANSACTIONS 
        |  ON ERROR BREAK 
        |""".stripMargin
    run(query).hasError(
      getGql42001_42N71(0, 1, 1),
      "Query cannot conclude with CALL (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
      p(0, 1, 1)
    )
  }

  test("CALL IN TRANSACTIONS ON ERROR RETRY should pass semantic check") {
    val query =
      """CALL () {
        |  RETURN 1 AS v
        |} IN TRANSACTIONS
        |  ON ERROR RETRY
        |  RETURN v
        |""".stripMargin
    run(query, disabledVersions = Set(Cypher5)).hasNoErrors
  }

  test("CALL IN TRANSACTIONS ON ERROR RETRY with inner return and no outer return should fail semantic check") {
    val query =
      """CALL () {
        |  RETURN 1 AS v
        |} IN TRANSACTIONS
        |  ON ERROR RETRY
        |""".stripMargin
    run(query, disabledVersions = Set(Cypher5)).hasError(
      getGql42001_42N71(0, 1, 1),
      "Query cannot conclude with CALL (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
      p(0, 1, 1)
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
    run(query).hasNoErrors
  }

  test("CALL IN TRANSACTIONS ON ERROR RETRY THEN CONTINUE REPORT STATUS AS status should pass semantic check") {
    val query =
      """CALL () {
        |  RETURN 1 AS v
        |} IN TRANSACTIONS
        |  ON ERROR RETRY THEN CONTINUE
        |  REPORT STATUS AS status
        |  RETURN v, status
        |""".stripMargin
    run(query, disabledVersions = Set(Cypher5)).hasNoErrors
  }

  test("CALL IN TRANSACTIONS ON ERROR CONTINUE REPORT STATUS without outer RETURN should fail semantic check") {
    val query =
      """CALL {
        |  CREATE ()
        |} IN TRANSACTIONS ON ERROR CONTINUE REPORT STATUS AS status
        |""".stripMargin
    run(query).hasError(
      getGql42001_42N71(0, 1, 1),
      "Query cannot conclude with CALL (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
      p(0, 1, 1)
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
    run(query).hasError(
      GqlHelper.getGql42001_42N59("v", 85, 4, 54),
      "Variable `v` already declared",
      p(85, 4, 54)
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
    run(query).hasNoErrors
  }

  test("CALL IN TRANSACTIONS REPORT STATUS should fail semantic check") {
    val query =
      """CALL {
        |  RETURN 1 AS v
        |} IN TRANSACTIONS
        |  REPORT STATUS AS status
        |  RETURN v, status
        |""".stripMargin
    run(query).hasError(
      GqlHelper.getGql42001_42I36(43, 4, 3),
      "REPORT STATUS can only be used when specifying ON ERROR CONTINUE or ON ERROR BREAK",
      p(43, 4, 3)
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
    run(query)
      .hasError(
        GqlHelper.getGql42001_42I36(59, 5, 3),
        "REPORT STATUS can only be used when specifying ON ERROR CONTINUE or ON ERROR BREAK",
        p(59, 5, 3)
      )
  }

  test("CALL IN TRANSACTIONS ON ERROR RETRY REPORT STATUS should fail semantic check") {
    val query =
      """CALL () {
        |  RETURN 1 AS v
        |} IN TRANSACTIONS
        |  ON ERROR RETRY
        |  REPORT STATUS AS status
        |  RETURN v, status
        |""".stripMargin
    run(query, disabledVersions = Set(Cypher5))
      .hasError(
        GqlHelper.getGql42001_42I36(63, 5, 3),
        "REPORT STATUS can only be used when specifying ON ERROR CONTINUE or ON ERROR BREAK",
        p(63, 5, 3)
      )
  }

  // ============================================================
  // CIP-260: DISJOINT BY (CYPHER 25 only)
  // ============================================================

  test("CALL (a) { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (a)") {
    val query = "WITH 1 AS a CALL (a) { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (a)"
    run(query, disabledVersions = Set(Cypher5)).hasNoErrors
  }

  test("CALL { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY AUTO") {
    val query = "CALL { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY AUTO"
    run(query, disabledVersions = Set(Cypher5)).hasNoErrors
  }

  test("CALL { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY NONE") {
    val query = "CALL { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY NONE"
    run(query, disabledVersions = Set(Cypher5)).hasNoErrors
  }

  test("CALL (a) { CREATE (n) } IN TRANSACTIONS DISJOINT BY (a) should reject DISJOINT BY without CONCURRENT") {
    val query = "WITH 1 AS a CALL (a) { CREATE (n) } IN TRANSACTIONS DISJOINT BY (a)"
    run(query, disabledVersions = Set(Cypher5))
      .hasError(
        GqlHelper.getGql42001_42N7A(52, 1, 53),
        "DISJOINT BY can only be used in CALL { ... } IN CONCURRENT TRANSACTIONS",
        p(52, 1, 53)
      )
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS DISJOINT BY AUTO should reject DISJOINT BY AUTO without CONCURRENT") {
    val query = "CALL { CREATE (n) } IN TRANSACTIONS DISJOINT BY AUTO"
    run(query, disabledVersions = Set(Cypher5))
      .hasError(
        GqlHelper.getGql42001_42N7A(36, 1, 37),
        "DISJOINT BY can only be used in CALL { ... } IN CONCURRENT TRANSACTIONS",
        p(36, 1, 37)
      )
  }

  test("CALL { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (randomUUID()) should reject non-deterministic") {
    val query = "CALL { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (randomUUID())"
    run(query, disabledVersions = Set(Cypher5))
      .hasError(
        GqlHelper.getGql42001_42N7B(60, 1, 61),
        "DISJOINT BY expressions must be deterministic",
        p(60, 1, 61)
      )
  }

  test("CALL { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (rand()) should reject non-deterministic") {
    val query = "CALL { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (rand())"
    run(query, disabledVersions = Set(Cypher5))
      .hasError(
        GqlHelper.getGql42001_42N7B(60, 1, 61),
        "DISJOINT BY expressions must be deterministic",
        p(60, 1, 61)
      )
  }

  test("CALL { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (COUNT { (m) }) should reject subquery expression") {
    val query = "CALL { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (COUNT { (m) })"
    run(query, disabledVersions = Set(Cypher5))
      .hasError(
        GqlHelper.getGql42001_42N7C(60, 1, 61),
        "DISJOINT BY expressions must not contain subquery expressions",
        p(60, 1, 61)
      )
  }

  test("CALL (a) { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (a, a + 1) accepts well-typed expressions") {
    val query = "WITH 1 AS a CALL (a) { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (a, a + 1)"
    run(query, disabledVersions = Set(Cypher5)).hasNoErrors
  }

  test("CALL (a) { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (a + true) should reject type mismatch") {
    val query = "WITH 1 AS a CALL (a) { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (a + true)"
    run(query, disabledVersions = Set(Cypher5))
      .hasErrorMessages("Type mismatch: expected Float, Integer, String or List<T> but was Boolean")
  }

  test("CALL (a) { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (count(a)) should reject aggregation") {
    val query = "WITH 1 AS a CALL (a) { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (count(a))"
    run(query, disabledVersions = Set(Cypher5))
      .hasErrorMessages("Invalid use of aggregating function count(...) in this context")
  }

  test("CALL { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (undefinedVar) should reject unbound variable") {
    val query = "CALL { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (undefinedVar)"
    run(query, disabledVersions = Set(Cypher5))
      .hasErrorMessages("Variable `undefinedVar` not defined")
  }

  // DISJOINT BY expressions are evaluated in the CALL subquery's inner scope:
  // only variables explicitly imported (or all-imported via CALL (*)) are visible.

  test("DISJOINT BY rejects an outer-scope variable that is not imported via the scope clause") {
    val query =
      """WITH 10 AS a, 1 AS b
        |CALL (a) { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (b)
        |""".stripMargin
    run(query, disabledVersions = Set(Cypher5))
      .hasErrorMessages("Variable `b` not defined")
  }

  test("DISJOINT BY rejects an outer-scope variable that is not imported via legacy importing-with form") {
    val query =
      """WITH 10 AS a, 1 AS b
        |CALL { WITH a CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (b)
        |RETURN a
        |""".stripMargin
    run(query, disabledVersions = Set(Cypher5))
      .hasErrorMessages("Variable `b` not defined")
  }

  test("DISJOINT BY rejects a variable declared inside the subquery body") {
    val query =
      """WITH 10 AS a
        |CALL (a) { WITH a, 1 AS local CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (local)
        |""".stripMargin
    run(query, disabledVersions = Set(Cypher5))
      .hasErrorMessages("Variable `local` not defined")
  }

  test("DISJOINT BY accepts a variable that is explicitly imported via the scope clause") {
    val query =
      """WITH 10 AS a
        |CALL (a) { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (a)
        |""".stripMargin
    run(query, disabledVersions = Set(Cypher5)).hasNoErrors
  }

  test("DISJOINT BY accepts an outer-scope variable when all variables are imported via CALL (*)") {
    val query =
      """WITH 10 AS a, 1 AS b
        |CALL (*) { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (b)
        |""".stripMargin
    run(query, disabledVersions = Set(Cypher5)).hasNoErrors
  }

  test("DISJOINT BY accepts arithmetic over multiple imported variables") {
    val query =
      """WITH 10 AS a, 1 AS b
        |CALL (a, b) { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (a + b)
        |""".stripMargin
    run(query, disabledVersions = Set(Cypher5)).hasNoErrors
  }
}
