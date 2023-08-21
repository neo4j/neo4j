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
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsBatchParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsErrorParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsReportParameters
import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.cst.factory.neo4j.Cst
import org.neo4j.cypher.internal.cst.factory.neo4j.Cst.SubqueryClause
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.symbols.CTAny

class CypherTransactionsParserTest extends ParserSyntaxTreeBase[Cst.SubqueryClause, ast.Clause]
    with VerifyAstPositionTestSupport {

  implicit private val javaccRule: JavaccRule[Clause] = JavaccRule.SubqueryClause
  implicit private val antlrRule: AntlrRule[SubqueryClause] = AntlrRule.SubqueryClause

  test("CALL { CREATE (n) } IN TRANSACTIONS") {
    val expected =
      SubqueryCall(
        SingleQuery(
          Seq(create(
            nodePat(Some("n"), namePos = (1, 16, 15), position = (1, 15, 14)),
            (1, 8, 7)
          ))
        )(defaultPos),
        Some(InTransactionsParameters(None, None, None)((1, 21, 20)))
      )(defaultPos)

    parsing(testName) shouldVerify { actual =>
      actual shouldBe expected
      verifyPositions(actual, expected)
    }
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 1 ROW") {
    val expected = subqueryCallInTransactions(
      inTransactionsParameters(
        Some(InTransactionsBatchParameters(literalInt(1))(pos)),
        None,
        None
      ),
      create(nodePat(Some("n")))
    )
    gives(expected)
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 1 ROWS") {
    val expected = subqueryCallInTransactions(
      inTransactionsParameters(
        Some(InTransactionsBatchParameters(literalInt(1))(pos)),
        None,
        None
      ),
      create(nodePat(Some("n")))
    )
    gives(expected)
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 42 ROW") {
    val expected =
      subqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(literalInt(42))(pos)),
          None,
          None
        ),
        create(nodePat(Some("n")))
      )
    gives(expected)
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 42 ROWS") {
    val expected =
      subqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(literalInt(42))(pos)),
          None,
          None
        ),
        create(nodePat(Some("n")))
      )
    gives(expected)
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF $param ROWS") {
    val expected =
      subqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(parameter("param", CTAny))(pos)),
          None,
          None
        ),
        create(nodePat(Some("n")))
      )
    gives(expected)
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF NULL ROWS") {
    val expected =
      subqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(nullLiteral)(pos)),
          None,
          None
        ),
        create(nodePat(Some("n")))
      )
    gives(expected)
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS REPORT STATUS AS status") {
    val expected =
      subqueryCallInTransactions(
        inTransactionsParameters(
          None,
          None,
          Some(InTransactionsReportParameters(Variable("status")(pos))(pos))
        ),
        create(nodePat(Some("n")))
      )
    gives(expected)
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 50 ROWS REPORT STATUS AS status") {
    val expected =
      subqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(literalInt(50))(pos)),
          None,
          Some(InTransactionsReportParameters(Variable("status")(pos))(pos))
        ),
        create(nodePat(Some("n")))
      )
    gives(expected)
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS REPORT STATUS AS status OF 50 ROWS") {
    val expected =
      subqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(literalInt(50))(pos)),
          None,
          Some(InTransactionsReportParameters(Variable("status")(pos))(pos))
        ),
        create(nodePat(Some("n")))
      )
    gives(expected)
  }

  // For each error behaviour, allow all possible orders of OF ROWS, ON ERROR and REPORT STATUS
  // The combination FAIL and REPORT STATUS should parse but will be disallowed in semantic checking
  Seq(
    ("BREAK", OnErrorBreak),
    ("FAIL", OnErrorFail),
    ("CONTINUE", OnErrorContinue)
  ).foreach {

    case (errorKeyword, errorBehaviour) =>
      val errorString = s"ON ERROR $errorKeyword"
      val rowString = "OF 50 ROWS"
      val statusString = "REPORT STATUS AS status"

      val errorRowPermutations = List(errorString, rowString).permutations.toList
      val errorStatusPermutations = List(errorString, statusString).permutations.toList
      val errorRowStatusPermutations = List(errorString, rowString, statusString).permutations.toList

      test(s"CALL { CREATE (n) } IN TRANSACTIONS $errorString") {
        val expected =
          subqueryCallInTransactions(
            inTransactionsParameters(
              None,
              Some(InTransactionsErrorParameters(errorBehaviour)(pos)),
              None
            ),
            create(nodePat(Some("n")))
          )
        gives(expected)
      }

      errorRowPermutations.foreach(permutation =>
        test(s"CALL { CREATE (n) } IN TRANSACTIONS ${permutation.head} ${permutation(1)}") {
          val expected =
            subqueryCallInTransactions(
              inTransactionsParameters(
                Some(InTransactionsBatchParameters(literalInt(50))(pos)),
                Some(InTransactionsErrorParameters(errorBehaviour)(pos)),
                None
              ),
              create(nodePat(Some("n")))
            )
          gives(expected)
        }
      )

      errorStatusPermutations.foreach(permutation =>
        test(s"CALL { CREATE (n) } IN TRANSACTIONS ${permutation.head} ${permutation(1)}") {
          val expected =
            subqueryCallInTransactions(
              inTransactionsParameters(
                None,
                Some(InTransactionsErrorParameters(errorBehaviour)(pos)),
                Some(InTransactionsReportParameters(Variable("status")(pos))(pos))
              ),
              create(nodePat(Some("n")))
            )
          gives(expected)
        }
      )

      errorRowStatusPermutations.foreach(permutation =>
        test(s"CALL { CREATE (n) } IN TRANSACTIONS ${permutation.head} ${permutation(1)} ${permutation(2)}") {
          val expected =
            subqueryCallInTransactions(
              inTransactionsParameters(
                Some(InTransactionsBatchParameters(literalInt(50))(pos)),
                Some(InTransactionsErrorParameters(errorBehaviour)(pos)),
                Some(InTransactionsReportParameters(Variable("status")(pos))(pos))
              ),
              create(nodePat(Some("n")))
            )
          gives(expected)
        }
      )
  }

  // Negative tests

  test("CALL { CREATE (n) } IN TRANSACTIONS ON ERROR BREAK ON ERROR CONTINUE") {
    assertFailsWithMessageStart(
      testName,
      "Duplicated ON ERROR parameter"
    )
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS ON ERROR BREAK CONTINUE") {
    assertFailsWithMessageContains(
      testName,
      "Encountered \" \"CONTINUE\" \"CONTINUE\"\" at line 1, column 52.\n\nWas expecting one of:\n\n<EOF> \n    \"OF\" ...\n    \"ON\" ...\n    \"REPORT\" ..."
    )
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS ON ERROR BREAK REPORT STATUS AS status ON ERROR CONTINUE") {
    assertFailsWithMessageStart(
      testName,
      "Duplicated ON ERROR parameter"
    )
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS REPORT STATUS AS status REPORT STATUS AS other") {
    assertFailsWithMessageStart(
      testName,
      "Duplicated REPORT STATUS parameter"
    )
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 5 ROWS ON ERROR BREAK REPORT STATUS AS status OF 42 ROWS") {
    assertFailsWithMessageStart(
      testName,
      "Duplicated OF ROWS parameter"
    )
  }
}
