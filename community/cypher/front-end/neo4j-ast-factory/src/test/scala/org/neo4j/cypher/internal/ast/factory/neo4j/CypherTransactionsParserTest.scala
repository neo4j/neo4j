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

import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsBatchParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsErrorParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsReportParameters
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.ParserSupport.NotAntlr
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.symbols.CTAny

class CypherTransactionsParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("CALL { CREATE (n) } IN TRANSACTIONS") {
    parses[SubqueryCall](NotAntlr).toAstPositioned {
      SubqueryCall(
        SingleQuery(
          Seq(create(
            nodePat(Some("n"), namePos = (1, 16, 15), position = (1, 15, 14)),
            (1, 8, 7)
          ))
        )(defaultPos),
        Some(InTransactionsParameters(None, None, None)((1, 21, 20)))
      )(defaultPos)
    }
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 1 ROW") {
    parses[SubqueryCall](NotAntlr).toAst {
      subqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(literalInt(1))(pos)),
          None,
          None
        ),
        create(nodePat(Some("n")))
      )
    }
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 1 ROWS") {
    parses[SubqueryCall](NotAntlr).toAst {
      subqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(literalInt(1))(pos)),
          None,
          None
        ),
        create(nodePat(Some("n")))
      )
    }
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 42 ROW") {
    parses[SubqueryCall](NotAntlr).toAst {
      subqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(literalInt(42))(pos)),
          None,
          None
        ),
        create(nodePat(Some("n")))
      )
    }
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 42 ROWS") {
    parses[SubqueryCall](NotAntlr).toAst {
      subqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(literalInt(42))(pos)),
          None,
          None
        ),
        create(nodePat(Some("n")))
      )
    }
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF $param ROWS") {
    parses[SubqueryCall](NotAntlr).toAst {
      subqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(parameter("param", CTAny))(pos)),
          None,
          None
        ),
        create(nodePat(Some("n")))
      )
    }
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF NULL ROWS") {
    parses[SubqueryCall](NotAntlr).toAst {
      subqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(nullLiteral)(pos)),
          None,
          None
        ),
        create(nodePat(Some("n")))
      )
    }
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
    gives[SubqueryCall](expected)
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
    gives[SubqueryCall](expected)
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
    gives[SubqueryCall](expected)
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
        gives[SubqueryCall](expected)
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
          gives[SubqueryCall](expected)
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
          gives[SubqueryCall](expected)
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
          gives[SubqueryCall](expected)
        }
      )
  }

  // Negative tests

  test("CALL { CREATE (n) } IN TRANSACTIONS ON ERROR BREAK ON ERROR CONTINUE") {
    assertFailsWithMessageStart[SubqueryCall](
      testName,
      "Duplicated ON ERROR parameter"
    )
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS ON ERROR BREAK CONTINUE") {
    assertFailsWithMessageContains[SubqueryCall](
      testName,
      "Encountered \" \"CONTINUE\" \"CONTINUE\"\" at line 1, column 52.\n\nWas expecting one of:\n\n<EOF> \n    \"OF\" ...\n    \"ON\" ...\n    \"REPORT\" ..."
    )
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS ON ERROR BREAK REPORT STATUS AS status ON ERROR CONTINUE") {
    assertFailsWithMessageStart[SubqueryCall](
      testName,
      "Duplicated ON ERROR parameter"
    )
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS REPORT STATUS AS status REPORT STATUS AS other") {
    assertFailsWithMessageStart[SubqueryCall](
      testName,
      "Duplicated REPORT STATUS parameter"
    )
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 5 ROWS ON ERROR BREAK REPORT STATUS AS status OF 42 ROWS") {
    assertFailsWithMessageStart[SubqueryCall](
      testName,
      "Duplicated OF ROWS parameter"
    )
  }
}
