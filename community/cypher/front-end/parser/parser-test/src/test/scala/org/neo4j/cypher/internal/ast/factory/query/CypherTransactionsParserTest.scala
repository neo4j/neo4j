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
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsDisjointByMode
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsDisjointByParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsErrorParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsReportParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsRetryParameters
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.gqlstatus.GqlStatusInfoCodes

class CypherTransactionsParserTest extends AstParsingTestBase {

  test("CALL { CREATE (n) } IN TRANSACTIONS") {
    parses[SubqueryCall].toAstPositioned {
      ImportingWithSubqueryCall(
        SingleQuery(
          Seq(create(
            nodePat(Some("n"), namePos = (1, 16, 15), position = (1, 15, 14)),
            (1, 8, 7)
          ))
        )(defaultPos),
        Some(InTransactionsParameters(None, None, None, None, None)((1, 24, 23))),
        optional = false
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
    parses[SubqueryCall].toAst {
      importingWithSubqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(literalInt(13))(pos)),
          Some(InTransactionsConcurrencyParameters(None)(pos)),
          None,
          None
        ),
        create(nodePat(Some("n")))
      )
    }
  }

  test("CALL { CREATE (n) } IN 1 CONCURRENT TRANSACTIONS") {
    parses[SubqueryCall].toAst {
      importingWithSubqueryCallInTransactions(
        inTransactionsParameters(
          None,
          Some(InTransactionsConcurrencyParameters(Some(literalInt(1)))(pos)),
          None,
          None
        ),
        create(nodePat(Some("n")))
      )
    }
  }

  test("CALL { CREATE (n) } IN 19 CONCURRENT TRANSACTIONS") {
    parses[SubqueryCall].toAst {
      importingWithSubqueryCallInTransactions(
        inTransactionsParameters(
          None,
          Some(InTransactionsConcurrencyParameters(Some(literalInt(19)))(pos)),
          None,
          None
        ),
        create(nodePat(Some("n")))
      )
    }
  }

  test("CALL { CREATE (n) } IN 19 CONCURRENT TRANSACTIONS OF 13 ROWS") {
    parses[SubqueryCall].toAst {
      importingWithSubqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(literalInt(13))(pos)),
          Some(InTransactionsConcurrencyParameters(Some(literalInt(19)))(pos)),
          None,
          None
        ),
        create(nodePat(Some("n")))
      )
    }
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS REPORT STATUS AS status") {
    val expected =
      importingWithSubqueryCallInTransactions(
        inTransactionsParameters(
          None,
          None,
          None,
          Some(InTransactionsReportParameters(varFor("status"))(pos))
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
          Some(InTransactionsReportParameters(varFor("status"))(pos))
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
          Some(InTransactionsReportParameters(varFor("status"))(pos))
        ),
        create(nodePat(Some("n")))
      )
    testName should parseTo[SubqueryCall](expected)
  }

  // For each error behaviour, allow all possible orders of OF ROWS, ON ERROR and REPORT STATUS
  // The combination FAIL and REPORT STATUS should parse but will be disallowed in semantic checking
  Seq(
    ("BREAK", OnErrorBreak, None),
    ("FAIL", OnErrorFail, None),
    ("CONTINUE", OnErrorContinue, None),
    ("RETRY", OnErrorRetryThenFail, None),
    ("RETRY THEN BREAK", OnErrorRetryThenBreak, None),
    ("RETRY THEN FAIL", OnErrorRetryThenFail, None),
    ("RETRY THEN CONTINUE", OnErrorRetryThenContinue, None),
    ("RETRY 1.8 SECONDS", OnErrorRetryThenFail, Some(InTransactionsRetryParameters(Some(literalFloat(1.8)))(pos))),
    (
      "RETRY FOR 1.8 SEC THEN BREAK",
      OnErrorRetryThenBreak,
      Some(InTransactionsRetryParameters(Some(literalFloat(1.8)))(pos))
    ),
    (
      "RETRY FOR 1 SECOND THEN FAIL",
      OnErrorRetryThenFail,
      Some(InTransactionsRetryParameters(Some(literalInt(1)))(pos))
    ),
    (
      "RETRY 3 SEC THEN CONTINUE",
      OnErrorRetryThenContinue,
      Some(InTransactionsRetryParameters(Some(literalInt(3)))(pos))
    )
  ).foreach {
    case (errorKeyword, errorBehaviour, retryParams) =>
      val errorString = s"ON ERROR $errorKeyword"
      val rowString = "OF 50 ROWS"
      val concurrencyString = "7 CONCURRENT"
      val statusString = "REPORT STATUS AS status"
      val disjointByString = "DISJOINT BY AUTO"

      val errorRowPermutations = List(errorString, rowString).permutations.toList
      val errorStatusPermutations = List(errorString, statusString).permutations.toList
      val errorRowStatusPermutations = List(errorString, rowString, statusString).permutations.toList
      val errorRowStatusDisjointPermutations =
        List(errorString, rowString, statusString, disjointByString).permutations.toList

      val expectedBatchParams = Some(InTransactionsBatchParameters(literalInt(50))(pos))
      val expectedConcurrencyParams = Some(InTransactionsConcurrencyParameters(Some(literalInt(7)))(pos))
      val expectedErrorParams = Some(InTransactionsErrorParameters(errorBehaviour, retryParams)(pos))
      val expectedStatusParams = Some(InTransactionsReportParameters(varFor("status"))(pos))
      val expectedDisjointByParams =
        Some(InTransactionsDisjointByParameters(InTransactionsDisjointByMode.DisjointByAuto)(pos))

      test(s"CALL () { CREATE (n) } IN TRANSACTIONS $errorString") {
        val expected =
          scopeClauseSubqueryCallInTransactionsNoImports(
            inTransactionsParameters(
              None,
              None,
              expectedErrorParams,
              None
            ),
            create(nodePat(Some("n")))
          )
        parses[SubqueryCall].toAst(expected)
      }

      errorRowPermutations.foreach(permutation => {
        test(s"CALL () { CREATE (n) } IN TRANSACTIONS ${permutation.head} ${permutation(1)}") {
          val expected =
            scopeClauseSubqueryCallInTransactionsNoImports(
              inTransactionsParameters(
                expectedBatchParams,
                None,
                expectedErrorParams,
                None
              ),
              create(nodePat(Some("n")))
            )
          parses[SubqueryCall].toAst(expected)
        }
        test(s"CALL () { CREATE (n) } IN $concurrencyString TRANSACTIONS ${permutation.head} ${permutation(1)}") {
          val expected =
            scopeClauseSubqueryCallInTransactionsNoImports(
              inTransactionsParameters(
                expectedBatchParams,
                expectedConcurrencyParams,
                expectedErrorParams,
                None
              ),
              create(nodePat(Some("n")))
            )
          parses[SubqueryCall].toAst(expected)
        }
      })

      errorStatusPermutations.foreach(permutation => {
        test(s"CALL () { CREATE (n) } IN TRANSACTIONS ${permutation.head} ${permutation(1)}") {
          val expected =
            scopeClauseSubqueryCallInTransactionsNoImports(
              inTransactionsParameters(
                None,
                None,
                expectedErrorParams,
                expectedStatusParams
              ),
              create(nodePat(Some("n")))
            )
          parses[SubqueryCall].toAst(expected)
        }
        test(s"CALL () { CREATE (n) } IN $concurrencyString TRANSACTIONS ${permutation.head} ${permutation(1)}") {
          val expected =
            scopeClauseSubqueryCallInTransactionsNoImports(
              inTransactionsParameters(
                None,
                expectedConcurrencyParams,
                expectedErrorParams,
                expectedStatusParams
              ),
              create(nodePat(Some("n")))
            )
          parses[SubqueryCall].toAst(expected)
        }
      })

      errorRowStatusPermutations.foreach(permutation => {
        test(s"CALL () { CREATE (n) } IN TRANSACTIONS ${permutation.head} ${permutation(1)} ${permutation(2)}") {
          val expected =
            scopeClauseSubqueryCallInTransactionsNoImports(
              inTransactionsParameters(
                expectedBatchParams,
                None,
                expectedErrorParams,
                expectedStatusParams
              ),
              create(nodePat(Some("n")))
            )
          parses[SubqueryCall].toAst(expected)
        }
        test(
          s"CALL () { CREATE (n) } IN $concurrencyString TRANSACTIONS ${permutation.head} ${permutation(1)} ${permutation(2)}"
        ) {
          val expected =
            scopeClauseSubqueryCallInTransactionsNoImports(
              inTransactionsParameters(
                expectedBatchParams,
                expectedConcurrencyParams,
                expectedErrorParams,
                expectedStatusParams
              ),
              create(nodePat(Some("n")))
            )
          parses[SubqueryCall].toAst(expected)
        }
      })

      errorRowStatusDisjointPermutations.foreach(permutation => {
        test(
          s"CALL () { CREATE (n) } IN $concurrencyString TRANSACTIONS ${permutation.head} ${permutation(1)} ${permutation(2)} ${permutation(3)}"
        ) {
          val expected =
            scopeClauseSubqueryCallInTransactionsNoImports(
              inTransactionsParameters(
                expectedBatchParams,
                expectedConcurrencyParams,
                expectedErrorParams,
                expectedStatusParams,
                expectedDisjointByParams
              ),
              create(nodePat(Some("n")))
            )
          parsesIn[SubqueryCall] {
            case Cypher5 => _.withAnyFailure
            case _       => _.toAst(expected)
          }
        }
      })
  }

  // Negative tests
  test("CALL () { CREATE (n) } IN TRANSACTIONS ON ERROR BREAK ON ERROR CONTINUE") {
    failsParsing[Statements].withMessageStart("Duplicated ON ERROR parameter")
  }

  test("CALL () { CREATE (n) } IN TRANSACTIONS ON ERROR BREAK CONTINUE") {
    failsParsing[Statements].withSyntaxErrorContaining("Invalid input 'CONTINUE'")
  }

  test("CALL () { CREATE (n) } IN TRANSACTIONS ON ERROR RETRY CONTINUE") {
    failsParsing[Statements].withSyntaxErrorContaining("Invalid input ")
  }

  test("CALL () { CREATE (n) } IN TRANSACTIONS ON ERROR RETRY THEN RETRY") {
    failsParsing[Statements].withSyntaxErrorContaining("Invalid input 'RETRY'")
  }

  test("CALL () { CREATE (n) } IN TRANSACTIONS ON ERROR RETRY FOR THEN FAIL") {
    failsParsing[Statements].withSyntaxErrorContaining("Invalid input")
  }

  test("CALL () { CREATE (n) } IN TRANSACTIONS ON ERROR BREAK REPORT STATUS AS status ON ERROR CONTINUE") {
    failsParsing[Statements]
      .withSyntaxErrorContaining(
        "Duplicated ON ERROR parameter",
        GqlStatusInfoCodes.STATUS_42N19,
        "error: syntax error or access rule violation - duplicate clause. Duplicate `ON ERROR` clause."
      )
  }

  test("CALL () { CREATE (n) } IN TRANSACTIONS REPORT STATUS AS status REPORT STATUS AS other") {
    failsParsing[Statements]
      .withSyntaxErrorContaining(
        "Duplicated REPORT STATUS parameter",
        GqlStatusInfoCodes.STATUS_42N19,
        "error: syntax error or access rule violation - duplicate clause. Duplicate `REPORT STATUS` clause."
      )
  }

  test("CALL () { CREATE (n) } IN TRANSACTIONS OF 5 ROWS ON ERROR BREAK REPORT STATUS AS status OF 42 ROWS") {
    failsParsing[Statements]
      .withSyntaxErrorContaining(
        "Duplicated OF ROWS parameter",
        GqlStatusInfoCodes.STATUS_42N19,
        "error: syntax error or access rule violation - duplicate clause. Duplicate `OF ROWS` clause."
      )
  }

  // ============================================================
  // CIP-260: DISJOINT BY (CYPHER 25 only)
  // ============================================================

  test("CALL (a) { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (a)") {
    parsesIn[SubqueryCall] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst {
          scopeClauseSubqueryCallInTransactions(
            isImportingAll = false,
            importedVariables = Seq(varFor("a")),
            inTransactionsParameters(
              None,
              Some(InTransactionsConcurrencyParameters(None)(pos)),
              None,
              None,
              Some(InTransactionsDisjointByParameters(
                InTransactionsDisjointByMode.DisjointByExpressions(Seq(varFor("a")))
              )(pos))
            ),
            create(nodePat(Some("n")))
          )
        }
    }
  }

  test("CALL (a, b) { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (a, b)") {
    parsesIn[SubqueryCall] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst {
          scopeClauseSubqueryCallInTransactions(
            isImportingAll = false,
            importedVariables = Seq(varFor("a"), varFor("b")),
            inTransactionsParameters(
              None,
              Some(InTransactionsConcurrencyParameters(None)(pos)),
              None,
              None,
              Some(InTransactionsDisjointByParameters(
                InTransactionsDisjointByMode.DisjointByExpressions(Seq(varFor("a"), varFor("b")))
              )(pos))
            ),
            create(nodePat(Some("n")))
          )
        }
    }
  }

  test("CALL (line) { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (line.user, line.movieId)") {
    parsesIn[SubqueryCall] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst {
          scopeClauseSubqueryCallInTransactions(
            isImportingAll = false,
            importedVariables = Seq(varFor("line")),
            inTransactionsParameters(
              None,
              Some(InTransactionsConcurrencyParameters(None)(pos)),
              None,
              None,
              Some(InTransactionsDisjointByParameters(
                InTransactionsDisjointByMode.DisjointByExpressions(Seq(
                  prop("line", "user"),
                  prop("line", "movieId")
                ))
              )(pos))
            ),
            create(nodePat(Some("n")))
          )
        }
    }
  }

  test("CALL { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY AUTO") {
    parsesIn[SubqueryCall] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst {
          importingWithSubqueryCallInTransactions(
            inTransactionsParameters(
              None,
              Some(InTransactionsConcurrencyParameters(None)(pos)),
              None,
              None,
              Some(InTransactionsDisjointByParameters(InTransactionsDisjointByMode.DisjointByAuto)(pos))
            ),
            create(nodePat(Some("n")))
          )
        }
    }
  }

  test("CALL { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY NONE") {
    parsesIn[SubqueryCall] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst {
          importingWithSubqueryCallInTransactions(
            inTransactionsParameters(
              None,
              Some(InTransactionsConcurrencyParameters(None)(pos)),
              None,
              None,
              Some(InTransactionsDisjointByParameters(InTransactionsDisjointByMode.DisjointByNone)(pos))
            ),
            create(nodePat(Some("n")))
          )
        }
    }
  }

  test("CALL (a) { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (`AUTO`)") {
    // Backtick-escaped AUTO is parsed as a variable named AUTO (manual mode with one expression).
    parsesIn[SubqueryCall] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst {
          scopeClauseSubqueryCallInTransactions(
            isImportingAll = false,
            importedVariables = Seq(varFor("a")),
            inTransactionsParameters(
              None,
              Some(InTransactionsConcurrencyParameters(None)(pos)),
              None,
              None,
              Some(InTransactionsDisjointByParameters(
                InTransactionsDisjointByMode.DisjointByExpressions(Seq(varFor("AUTO")))
              )(pos))
            ),
            create(nodePat(Some("n")))
          )
        }
    }
  }

  test("CALL (a) { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (`NONE`)") {
    // Backtick-escaped NONE is parsed as a variable named NONE (manual mode with one expression).
    parsesIn[SubqueryCall] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst {
          scopeClauseSubqueryCallInTransactions(
            isImportingAll = false,
            importedVariables = Seq(varFor("a")),
            inTransactionsParameters(
              None,
              Some(InTransactionsConcurrencyParameters(None)(pos)),
              None,
              None,
              Some(InTransactionsDisjointByParameters(
                InTransactionsDisjointByMode.DisjointByExpressions(Seq(varFor("NONE")))
              )(pos))
            ),
            create(nodePat(Some("n")))
          )
        }
    }
  }

  test(
    "CALL (a, b, c) { CREATE (n) } IN CONCURRENT TRANSACTIONS OF 5 ROWS DISJOINT BY (a, b, c) ON ERROR RETRY THEN CONTINUE REPORT STATUS AS s"
  ) {
    parsesIn[SubqueryCall] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst {
          scopeClauseSubqueryCallInTransactions(
            isImportingAll = false,
            importedVariables = Seq(varFor("a"), varFor("b"), varFor("c")),
            inTransactionsParameters(
              Some(InTransactionsBatchParameters(literalInt(5))(pos)),
              Some(InTransactionsConcurrencyParameters(None)(pos)),
              Some(InTransactionsErrorParameters(OnErrorRetryThenContinue, None)(pos)),
              Some(InTransactionsReportParameters(varFor("s"))(pos)),
              Some(InTransactionsDisjointByParameters(
                InTransactionsDisjointByMode.DisjointByExpressions(Seq(varFor("a"), varFor("b"), varFor("c")))
              )(pos))
            ),
            create(nodePat(Some("n")))
          )
        }
    }
  }

  test("CALL { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY") {
    // Empty manual list is a parse error.
    parsesIn[SubqueryCall] {
      case Cypher5 => _.withAnyFailure
      case _       => _.withAnyFailure
    }
  }

  test("CALL (a) { CREATE (n) } IN TRANSACTIONS DISJOINT BY (a)") {
    // DISJOINT BY without CONCURRENT parses successfully (semantic error caught later).
    parsesIn[SubqueryCall] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst {
          scopeClauseSubqueryCallInTransactions(
            isImportingAll = false,
            importedVariables = Seq(varFor("a")),
            inTransactionsParameters(
              None,
              None,
              None,
              None,
              Some(InTransactionsDisjointByParameters(
                InTransactionsDisjointByMode.DisjointByExpressions(Seq(varFor("a")))
              )(pos))
            ),
            create(nodePat(Some("n")))
          )
        }
    }
  }

  test("CALL { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY AUTO DISJOINT BY NONE") {
    // Duplicate DISJOINT BY parameter (Cypher5 fails earlier with `BATCH` not allowed at all).
    parsesIn[Statements] {
      case Cypher5 => _.withAnyFailure
      case _       => _.withSyntaxErrorContaining("Duplicated DISJOINT BY parameters")
    }
  }

  test("CALL (a) { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY a") {
    // CIP amendment: bare expression list without parens is now a parse error.
    parsesIn[SubqueryCall] {
      case Cypher5 => _.withAnyFailure
      case _       => _.withAnyFailure
    }
  }

  test("CALL { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY ()") {
    // Empty paren list is a parse error.
    parsesIn[SubqueryCall] {
      case Cypher5 => _.withAnyFailure
      case _       => _.withAnyFailure
    }
  }

  test("CALL (a) { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY (a") {
    // Unterminated parens — parse error.
    parsesIn[SubqueryCall] {
      case Cypher5 => _.withAnyFailure
      case _       => _.withAnyFailure
    }
  }

  test("CALL (a) { CREATE (n) } IN CONCURRENT TRANSACTIONS DISJOINT BY a)") {
    // Missing opening paren — parse error.
    parsesIn[SubqueryCall] {
      case Cypher5 => _.withAnyFailure
      case _       => _.withAnyFailure
    }
  }
}
