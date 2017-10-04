/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions._

class ConstantExpressionTest extends CypherFunSuite {
  test("tests") {
    assertIsConstant(SignedDecimalIntegerLiteral("42")(null))
    assertIsConstant(Parameter("42", CTAny)(null))
    assertIsConstant(ListLiteral(Seq(SignedDecimalIntegerLiteral("42")(null)))(null))
  }

  private def assertIsConstant(e: Expression) = {
    val unapply = ConstantExpression.unapply(e)
    if (unapply.isEmpty) fail(s"$e should be considered constant")
  }
}
