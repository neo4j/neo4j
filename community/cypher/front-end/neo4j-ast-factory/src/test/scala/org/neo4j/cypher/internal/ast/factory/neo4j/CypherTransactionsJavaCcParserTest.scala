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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsParameters
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalatest.FunSuiteLike

class CypherTransactionsJavaCcParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName with AstConstructionTestSupport {

  test("CALL { CREATE (n) } IN TRANSACTIONS") {
    val expected = query(
      SubqueryCall(
        SingleQuery(
          Seq(create(
            nodePat("n", (1, 15, 14)),
            (1, 8, 7))))
        (defaultPos),
        Some(InTransactionsParameters(None)(1, 21, 20))
      )(defaultPos)
    )
    assertJavaCCAST(testName, expected)
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 1 ROW") {
    val expected = query(subqueryCallInTransactions(inTransactionsParameters(Some(literalInt(1))), create(nodePat("n"))))
    assertJavaCCAST(testName, expected, comparePosition = false)
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 1 ROWS") {
    val expected = query(subqueryCallInTransactions(inTransactionsParameters(Some(literalInt(1))), create(nodePat("n"))))
    assertJavaCCAST(testName, expected, comparePosition = false)
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 42 ROW") {
    val expected = query(subqueryCallInTransactions(inTransactionsParameters(Some(literalInt(42))), create(nodePat("n"))))
    assertJavaCCAST(testName, expected, comparePosition = false)
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF 42 ROWS") {
    val expected = query(subqueryCallInTransactions(inTransactionsParameters(Some(literalInt(42))), create(nodePat("n"))))
    assertJavaCCAST(testName, expected, comparePosition = false)
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF $param ROWS") {
    val expected = query(subqueryCallInTransactions(inTransactionsParameters(Some(parameter("param", CTAny))), create(nodePat("n"))))
    assertJavaCCAST(testName, expected, comparePosition = false)
  }

  test("CALL { CREATE (n) } IN TRANSACTIONS OF NULL ROWS") {
    val expected = query(subqueryCallInTransactions(inTransactionsParameters(Some(nullLiteral)), create(nodePat("n"))))
    assertJavaCCAST(testName, expected, comparePosition = false)
  }
}
