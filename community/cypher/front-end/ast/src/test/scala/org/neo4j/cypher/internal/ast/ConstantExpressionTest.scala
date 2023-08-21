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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.expressions.ConstantExpression
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ConstantExpressionTest extends CypherFunSuite {

  test("tests") {
    assertIsConstant(SignedDecimalIntegerLiteral("42")(null))
    assertIsConstant(ExplicitParameter("42", CTAny)(null))
    assertIsConstant(ListLiteral(Seq(SignedDecimalIntegerLiteral("42")(null)))(null))
  }

  private def assertIsConstant(e: Expression) = {
    val unapply = ConstantExpression.unapply(e)
    if (unapply.isEmpty) fail(s"$e should be considered constant")
  }
}
