/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.ast.prettifier

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ExpressionStringifierTest extends CypherFunSuite with AstConstructionTestSupport {

  private val tests: Seq[(Expression, String)] = Seq(
  )

  private val stringifier = ExpressionStringifier()

  for (((expr, expectedResult), idx) <- tests.zipWithIndex) {
    test(s"[$idx] should produce $expectedResult") {
      withClue(expr) {
        lazy val stringifiedExpr = stringifier(expr)
        noException should be thrownBy stringifiedExpr
        stringifiedExpr shouldBe expectedResult
      }
    }
  }
}
