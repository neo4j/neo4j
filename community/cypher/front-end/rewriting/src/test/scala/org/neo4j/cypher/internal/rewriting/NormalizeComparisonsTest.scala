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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.InvalidNotEquals
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeComparisons
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class NormalizeComparisonsTest extends CypherFunSuite with AstConstructionTestSupport {

  private val expression = varFor("foo")

  private val comparisons = List(
    equals(expression, expression),
    notEquals(expression, expression),
    lessThan(expression, expression),
    lessThanOrEqual(expression, expression),
    greaterThan(expression, expression),
    greaterThanOrEqual(expression, expression),
    InvalidNotEquals(expression, expression)(pos)
  )

  comparisons.foreach { operator =>
    test(operator.toString) {
      val rewritten = operator.endoRewrite(normalizeComparisons.instance)

      rewritten.lhs shouldNot be theSameInstanceAs rewritten.rhs
    }
  }

  test("extract multiple hasLabels") {
    val original = hasLabels(varFor("a"), "X", "Y")

    original.endoRewrite(normalizeComparisons.instance) should equal(
      ands(hasLabels("a", "X"), hasLabels("a", "Y"))
    )
  }

  test("does not extract single hasLabels") {
    val original = hasLabels("a", "Y")

    original.endoRewrite(normalizeComparisons.instance) should equal(original)
  }
}
