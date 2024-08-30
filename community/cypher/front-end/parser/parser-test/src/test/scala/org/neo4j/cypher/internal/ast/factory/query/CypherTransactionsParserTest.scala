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
package org.neo4j.cypher.internal.ast.factory.query

import org.neo4j.cypher.internal.ast.ImportingWithSubqueryCall
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsBatchParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsConcurrencyParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsErrorParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsReportParameters
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.exceptions.SyntaxException

class CypherTransactionsParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("CALL { CREATE (n) } IN TRANSACTIONS") {
    parses[SubqueryCall].toAstPositioned {
      ImportingWithSubqueryCall(
        SingleQuery(
          Seq(create(
            nodePat(Some("n"), namePos = (1, 16, 15), position = (1, 15, 14)),
            (1, 8, 7)
          ))
        )(defaultPos),
        Some(InTransactionsParameters(None, None, None, None)((1, 24, 23)))
      )(defaultPos)
    }
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 1 ROW") {
    parses[SubqueryCall].toAst {
      importingWithSubqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(literalInt(1))(pos)),
          None,
          None,
          None
        ),
        create(nodePat(Some("n")))
      )
    }
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 1 ROWS") {
    parses[SubqueryCall].toAst {
      importingWithSubqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(literalInt(1))(pos)),
          None,
          None,
          None
        ),
        create(nodePat(Some("n")))
      )
    }
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 42 ROW") {
    parses[SubqueryCall].toAst {
      importingWithSubqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(literalInt(42))(pos)),
          None,
          None,
          None
        ),
        create(nodePat(Some("n")))
      )
    }
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 42 ROWS") {
    parses[SubqueryCall].toAst {
      importingWithSubqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(literalInt(42))(pos)),
          None,
          None,
          None
        ),
        create(nodePat(Some("n")))
      )
    }
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF $param ROWS") {
    parses[SubqueryCall].toAst {
      importingWithSubqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(parameter("param", CTAny))(pos)),
          None,
          None,
          None
        ),
        create(nodePat(Some("n")))
      )
    }
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF NULL ROWS") {
    parses[SubqueryCall].toAst {
      importingWithSubqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(nullLiteral)(pos)),
          None,
          None,
          None
        ),
        create(nodePat(Some("n")))
      )
    }
  }

  test("CALL { CREATE (n) } IN CONCURRENT TRANSACTIONS OF 13 ROWS") {
    val expected =
      importingWithSubqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(literalInt(13))(pos)),
          Some(InTransactionsConcurrencyParameters(None)(pos)),
          None,
          None
        ),
        create(nodePat(Some("n")))
      )
    gives[SubqueryCall](expected)
  }

  test("CALL { CREATE (n) } IN 1 CONCURRENT TRANSACTIONS") {
    val expected = importingWithSubqueryCallInTransactions(
      inTransactionsParameters(
        None,
        Some(InTransactionsConcurrencyParameters(Some(literalInt(1)))(pos)),
        None,
        None
      ),
      create(nodePat(Some("n")))
    )
    gives[SubqueryCall](expected)
  }

  test("CALL { CREATE (n) } IN 19 CONCURRENT TRANSACTIONS") {
    val expected =
      importingWithSubqueryCallInTransactions(
        inTransactionsParameters(
          None,
          Some(InTransactionsConcurrencyParameters(Some(literalInt(19)))(pos)),
          None,
          None
        ),
        create(nodePat(Some("n")))
      )
    gives[SubqueryCall](expected)
  }

  test("CALL { CREATE (n) } IN 19 CONCURRENT TRANSACTIONS OF 13 ROWS") {
    val expected =
      importingWithSubqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(literalInt(13))(pos)),
          Some(InTransactionsConcurrencyParameters(Some(literalInt(19)))(pos)),
          None,
          None
        ),
        create(nodePat(Some("n")))
      )
    gives[SubqueryCall](expected)
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS REPORT STATUS AS status") {
    val expected =
      importingWithSubqueryCallInTransactions(
        inTransactionsParameters(
          None,
          None,
          None,
          Some(InTransactionsReportParameters(Variable("status")(pos))(pos))
        ),
        create(nodePat(Some("n")))
      )
    testName should parseTo[SubqueryCall](expected)
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 50 ROWS REPORT STATUS AS status") {
    val expected =
      importingWithSubqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(literalInt(50))(pos)),
          None,
          None,
          Some(InTransactionsReportParameters(Variable("status")(pos))(pos))
        ),
        create(nodePat(Some("n")))
      )
    testName should parseTo[SubqueryCall](expected)
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS REPORT STATUS AS status OF 50 ROWS") {
    val expected =
      importingWithSubqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(literalInt(50))(pos)),
          None,
          None,
          Some(InTransactionsReportParameters(Variable("status")(pos))(pos))
        ),
        create(nodePat(Some("n")))
      )
    testName should parseTo[SubqueryCall](expected)
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
      val concurrencyString = "7 CONCURRENT"
      val statusString = "REPORT STATUS AS status"

      val errorRowPermutations = List(errorString, rowString).permutations.toList
      val errorStatusPermutations = List(errorString, statusString).permutations.toList
      val errorRowStatusPermutations = List(errorString, rowString, statusString).permutations.toList

      val expectedBatchParams = Some(InTransactionsBatchParameters(literalInt(50))(pos))
      val expectedConcurrencyParams = Some(InTransactionsConcurrencyParameters(Some(literalInt(7)))(pos))
      val expectedErrorParams = Some(InTransactionsErrorParameters(errorBehaviour)(pos))
      val expectedStatusParams = Some(InTransactionsReportParameters(Variable("status")(pos))(pos))

      test(s"CALL { CREATE (n) } IN TRANSACTIONS $errorString") {
        val expected =
          importingWithSubqueryCallInTransactions(
            inTransactionsParameters(
              None,
              None,
              expectedErrorParams,
              None
            ),
            create(nodePat(Some("n")))
          )
        testName should parseTo[SubqueryCall](expected)
      }

      errorRowPermutations.foreach(permutation => {
        test(s"CALL { CREATE (n) } IN TRANSACTIONS ${permutation.head} ${permutation(1)}") {
          val expected =
            importingWithSubqueryCallInTransactions(
              inTransactionsParameters(
                expectedBatchParams,
                None,
                expectedErrorParams,
                None
              ),
              create(nodePat(Some("n")))
            )
          testName should parseTo[SubqueryCall](expected)
        }
        test(s"CALL { CREATE (n) } IN $concurrencyString TRANSACTIONS ${permutation.head} ${permutation(1)}") {
          val expected =
            importingWithSubqueryCallInTransactions(
              inTransactionsParameters(
                expectedBatchParams,
                expectedConcurrencyParams,
                expectedErrorParams,
                None
              ),
              create(nodePat(Some("n")))
            )
          gives[SubqueryCall](expected)
        }
      })

      errorStatusPermutations.foreach(permutation => {
        test(s"CALL { CREATE (n) } IN TRANSACTIONS ${permutation.head} ${permutation(1)}") {
          val expected =
            importingWithSubqueryCallInTransactions(
              inTransactionsParameters(
                None,
                None,
                expectedErrorParams,
                expectedStatusParams
              ),
              create(nodePat(Some("n")))
            )
          testName should parseTo[SubqueryCall](expected)
        }
        test(s"CALL { CREATE (n) } IN $concurrencyString TRANSACTIONS ${permutation.head} ${permutation(1)}") {
          val expected =
            importingWithSubqueryCallInTransactions(
              inTransactionsParameters(
                None,
                expectedConcurrencyParams,
                expectedErrorParams,
                expectedStatusParams
              ),
              create(nodePat(Some("n")))
            )
          gives[SubqueryCall](expected)
        }
      })

      errorRowStatusPermutations.foreach(permutation => {
        test(s"CALL { CREATE (n) } IN TRANSACTIONS ${permutation.head} ${permutation(1)} ${permutation(2)}") {
          val expected =
            importingWithSubqueryCallInTransactions(
              inTransactionsParameters(
                expectedBatchParams,
                None,
                expectedErrorParams,
                expectedStatusParams
              ),
              create(nodePat(Some("n")))
            )
          testName should parseTo[SubqueryCall](expected)
        }
        test(
          s"CALL { CREATE (n) } IN $concurrencyString TRANSACTIONS ${permutation.head} ${permutation(1)} ${permutation(2)}"
        ) {
          val expected =
            importingWithSubqueryCallInTransactions(
              inTransactionsParameters(
                expectedBatchParams,
                expectedConcurrencyParams,
                expectedErrorParams,
                expectedStatusParams
              ),
              create(nodePat(Some("n")))
            )
          gives[SubqueryCall](expected)
        }
      })
  }

  // Negative tests
  test("CALL { CREATE (n) } IN TRANSACTIONS ON ERROR BREAK ON ERROR CONTINUE") {
    failsParsing[Statements].withMessageStart("Duplicated ON ERROR parameter")
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS ON ERROR BREAK CONTINUE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input 'CONTINUE'")
      case _ => _.withSyntaxError(
          """Invalid input 'CONTINUE': expected 'FOREACH', 'REPORT STATUS AS', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'ON ERROR', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OF', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 52 (offset: 51))
            |"CALL { CREATE (n) } IN TRANSACTIONS ON ERROR BREAK CONTINUE"
            |                                                    ^""".stripMargin
        )
    }
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS ON ERROR BREAK REPORT STATUS AS status ON ERROR CONTINUE") {
    failsParsing[Statements]
      .withMessageStart("Duplicated ON ERROR parameter")
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS REPORT STATUS AS status REPORT STATUS AS other") {
    failsParsing[Statements]
      .withMessageStart("Duplicated REPORT STATUS parameter")
      .in {
        case Cypher5JavaCc => _.throws[OpenCypherExceptionFactory.SyntaxException]
        case _             => _.throws[SyntaxException]
      }
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 5 ROWS ON ERROR BREAK REPORT STATUS AS status OF 42 ROWS") {
    failsParsing[Statements]
      .withMessageStart("Duplicated OF ROWS parameter")
      .in {
        case Cypher5JavaCc => _.throws[OpenCypherExceptionFactory.SyntaxException]
        case _             => _.throws[SyntaxException]
      }
  }
}
