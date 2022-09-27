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
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.symbols.CTAny

class CypherTransactionsParserTest extends JavaccParserAstTestBase[Clause] with VerifyAstPositionTestSupport {

  implicit private val parser: JavaccRule[Clause] = JavaccRule.SubqueryClause

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

  test("CALL { CREATE (n) } IN TRANSACTIONS ON ERROR BREAK REPORT STATUS AS status") {
    val expected =
      subqueryCallInTransactions(
        inTransactionsParameters(
          None,
          Some(InTransactionsErrorParameters(OnErrorBreak)(pos)),
          Some(InTransactionsReportParameters(Variable("status")(pos))(pos))
        ),
        create(nodePat(Some("n")))
      )
    gives(expected)
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS REPORT STATUS AS status ON ERROR BREAK") {
    val expected =
      subqueryCallInTransactions(
        inTransactionsParameters(
          None,
          Some(InTransactionsErrorParameters(OnErrorBreak)(pos)),
          Some(InTransactionsReportParameters(Variable("status")(pos))(pos))
        ),
        create(nodePat(Some("n")))
      )
    gives(expected)
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS ON ERROR FAIL REPORT STATUS AS status") {
    val expected =
      subqueryCallInTransactions(
        inTransactionsParameters(
          None,
          Some(InTransactionsErrorParameters(OnErrorFail)(pos)),
          Some(InTransactionsReportParameters(Variable("status")(pos))(pos))
        ),
        create(nodePat(Some("n")))
      )
    gives(expected)
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS ON ERROR CONTINUE") {
    val expected =
      subqueryCallInTransactions(
        inTransactionsParameters(
          None,
          Some(InTransactionsErrorParameters(OnErrorContinue)(pos)),
          None
        ),
        create(nodePat(Some("n")))
      )
    gives(expected)
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 50 ROWS ON ERROR BREAK REPORT STATUS AS status") {
    val expected =
      subqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(literalInt(50))(pos)),
          Some(InTransactionsErrorParameters(OnErrorBreak)(pos)),
          Some(InTransactionsReportParameters(Variable("status")(pos))(pos))
        ),
        create(nodePat(Some("n")))
      )
    gives(expected)
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 50 ROWS ON ERROR FAIL REPORT STATUS AS status") {
    val expected =
      subqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(literalInt(50))(pos)),
          Some(InTransactionsErrorParameters(OnErrorFail)(pos)),
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

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 50 ROWS ON ERROR CONTINUE REPORT STATUS AS status") {
    val expected =
      subqueryCallInTransactions(
        inTransactionsParameters(
          Some(InTransactionsBatchParameters(literalInt(50))(pos)),
          Some(InTransactionsErrorParameters(OnErrorContinue)(pos)),
          Some(InTransactionsReportParameters(Variable("status")(pos))(pos))
        ),
        create(nodePat(Some("n")))
      )
    gives(expected)
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS ON ERROR BREAK ON ERROR CONTINUE") {
    assertFailsWithMessageStart(
      testName,
      "Duplicated ON ERROR parameter"
    )
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS ON ERROR BREAK CONTINUE") {
    assertFailsWithMessageContains(
      testName,
      "Encountered \" \"CONTINUE\" \"CONTINUE\"\" at line 1, column 52.\n\nWas expecting one of:\n\n<EOF> \n    \"ON\" ...\n    \"REPORT\" ..."
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
}
