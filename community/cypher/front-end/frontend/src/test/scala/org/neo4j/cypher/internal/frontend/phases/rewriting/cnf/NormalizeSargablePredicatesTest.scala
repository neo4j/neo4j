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
package org.neo4j.cypher.internal.frontend.phases.rewriting.cnf

import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.rewriting.AstRewritingTestSupport
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class NormalizeSargablePredicatesTest extends CypherFunSuite with AstRewritingTestSupport {

  Seq(
    ("<", ">="),
    ("<=", ">"),
    (">", "<="),
    (">=", "<")
  ).foreach {
    case (symbol, oppositeSymbol) =>
      test(s"NOT x $symbol y should not be rewritten when using an operand of unknown type") {
        assertIsNotRewritten(s"RETURN NOT $$x $symbol 10")
      }

      test(s"NOT x $symbol y should not be rewritten when using a NaN Double literal (rhs)") {
        assertIsNotRewritten(s"RETURN NOT(1.0 $symbol (0.0/0.0))")
      }

      test(s"NOT x $symbol y should not be rewritten when using a NaN Double literal (lhs)") {
        assertIsNotRewritten(s"RETURN NOT((0.0/0.0) $symbol 1.0)")
      }

      test(s"NOT x $symbol y should be rewritten when using Double literals") {
        assertIsRewritten(s"RETURN NOT(1.0 $symbol 4.0) AS result", s"RETURN 1.0 $oppositeSymbol 4.0 AS result")
      }

      test(s"NOT x $symbol y should not be rewritten when using an operator that is missing signatures") {
        assertIsNotRewritten(s"RETURN NOT(1.0 $symbol (4.0 + 1.0))")
      }

      test(s"NOT x $symbol y should not be rewritten when using an operator that can evaluate to float") {
        assertIsNotRewritten(s"RETURN NOT(1.0 $symbol (4.0 * 1.0))")
      }

      test(s"NOT x $symbol y should be rewritten when using an operator that cannot evaluate to float") {
        assertIsRewritten(
          s"RETURN NOT(1.0 $symbol (1.0 = 4.0)) AS result",
          s"RETURN 1.0 $oppositeSymbol (1.0 = 4.0) AS result"
        )
      }

      test(s"NOT x $symbol y should not be rewritten when using an operator that can be a float or an int") {
        assertIsNotRewritten(s"UNWIND [1, 1.0] AS number RETURN NOT(1.0 $symbol number)")
      }

      test(s"NOT x $symbol y should not be rewritten when using an operator that can be a string or an int") {
        assertIsNotRewritten(s"UNWIND [1, \"foo\"] AS number RETURN NOT(1.0 $symbol number) AS result")
      }

      test(s"NOT x $symbol y should not be rewritten when using a function that can evaluate to a float") {
        assertIsNotRewritten(s"RETURN NOT(avg(1.0) $symbol 5.0)")
      }

      test(s"NOT x $symbol y should be rewritten when using a function that cannot evaluate to a float") {
        assertIsRewritten(
          s"RETURN NOT(COUNT(1.0) $symbol 5.0) AS result",
          s"RETURN COUNT(1.0) $oppositeSymbol 5.0 AS result"
        )
      }

      test(
        s"NOT x $symbol y should be rewritten when using an operand on the RHS that cannot be a number even if one side is NaN"
      ) {
        assertIsRewritten(
          s"RETURN NOT((0.0/0.0) $symbol \"foo\") AS result",
          s"RETURN (0.0/0.0) $oppositeSymbol \"foo\" AS result"
        )
      }

      test(s"NOT x $symbol y should be rewritten when using an operand on the RHS that cannot be a number") {
        assertIsRewritten(
          s"RETURN NOT(1.0 $symbol \"5.0\") AS result",
          s"RETURN 1.0 $oppositeSymbol \"5.0\" AS result"
        )
      }

      test(s"NOT x $symbol y should be rewritten when using an operand on the LHS that cannot be a number") {
        assertIsRewritten(
          s"RETURN NOT(\"5.0\" $symbol 5.0) AS result",
          s"RETURN \"5.0\" $oppositeSymbol 5.0 AS result"
        )
      }

      test(s"NOT x $symbol y should be rewritten when using an operand on the both sides that cannot be a number") {
        assertIsRewritten(
          s"RETURN NOT(\"5.0\" $symbol \"foo\") AS result",
          s"RETURN \"5.0\" $oppositeSymbol \"foo\" AS result"
        )
      }
  }

  private def assertIsNotRewritten(query: String): Unit = {
    assertRewrite(query, query)
  }

  private def assertIsRewritten(originalQuery: String, expectedQuery: String): Unit = {
    assertRewrite(originalQuery, expectedQuery)
  }

  private def assertRewrite(originalQuery: String, expectedQuery: String): Unit = {
    val original = parse(originalQuery, OpenCypherExceptionFactory(None))
    val expected = parse(expectedQuery, OpenCypherExceptionFactory(None))

    val checkResult = original.semanticCheck.run(SemanticState.clean, SemanticCheckContext.default)
    val semanticTable = SemanticTable(types = checkResult.state.typeTable)
    val rewriter = normalizeSargablePredicatesRewriter(semanticTable)

    val result = original.rewrite(rewriter)
    assert(result === expected)
  }
}
