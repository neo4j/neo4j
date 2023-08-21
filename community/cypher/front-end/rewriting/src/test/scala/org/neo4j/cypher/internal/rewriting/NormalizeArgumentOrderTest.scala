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
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeArgumentOrder
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class NormalizeArgumentOrderTest extends CypherFunSuite with AstConstructionTestSupport {

  test("a.prop = b.prop rewritten to: a.prop = b.prop") {
    val lhs = prop("a", "prop")
    val rhs = prop("b", "prop")

    val input = equals(lhs, rhs)

    normalizeArgumentOrder.instance(input) should equal(input)
  }

  test("12 = a.prop rewritten to: a.prop = 12") {
    val lhs = literalInt(12)
    val rhs = prop("a", "prop")

    val input = equals(lhs, rhs)
    val expected = equals(rhs, lhs)

    normalizeArgumentOrder.instance(input) should equal(expected)
  }

  test("a < n.prop rewritten to: n.prop > a") {
    val lhs = id(varFor("a"))
    val rhs = prop("n", "prop")

    val input = lessThan(lhs, rhs)

    normalizeArgumentOrder.instance(input) should equal(greaterThan(rhs, lhs))
  }

  test("a <= n.prop rewritten to: n.prop >= a") {
    val lhs = id(varFor("a"))
    val rhs = prop("n", "prop")

    val input = lessThanOrEqual(lhs, rhs)

    normalizeArgumentOrder.instance(input) should equal(greaterThanOrEqual(rhs, lhs))
  }

  test("a > n.prop rewritten to: n.prop < a") {
    val lhs = id(varFor("a"))
    val rhs = prop("n", "prop")

    val input = greaterThan(lhs, rhs)

    normalizeArgumentOrder.instance(input) should equal(lessThan(rhs, lhs))
  }

  test("a >= n.prop rewritten to: n.prop <= a") {
    val lhs = id(varFor("a"))
    val rhs = prop("n", "prop")

    val input = greaterThanOrEqual(lhs, rhs)

    normalizeArgumentOrder.instance(input) should equal(lessThanOrEqual(rhs, lhs))
  }
}

trait NormalizeArgumentOrderIdTestBase extends CypherFunSuite with AstConstructionTestSupport {

  protected def makeId(e: Expression): FunctionInvocation

  test("id(a) = id(b) rewritten to: id(a) = id(b)") {
    val lhs = makeId(varFor("a"))
    val rhs = makeId(varFor("b"))

    val input = equals(lhs, rhs)

    normalizeArgumentOrder.instance(input) should equal(input)
  }

  test("23 = makeId(a) rewritten to: id(a) = 23") {
    val lhs = literalInt(12)
    val rhs = makeId(varFor("a"))

    val input = equals(lhs, rhs)
    val expected = equals(rhs, lhs)

    normalizeArgumentOrder.instance(input) should equal(expected)
  }

  test("a.prop = makeId(b) rewritten to: id(b) = a.prop") {
    val lhs = prop("a", "prop")
    val rhs = makeId(varFor("b"))

    val input = equals(rhs, lhs)

    normalizeArgumentOrder.instance(input) should equal(input)
  }

  test("id(a) = b.prop rewritten to: id(a) = b.prop") {
    val lhs = makeId(varFor("a"))
    val rhs = prop("b", "prop")

    val input = equals(lhs, rhs)

    normalizeArgumentOrder.instance(input) should equal(input)
  }
}

class NormalizeArgumentOrderIdTest extends NormalizeArgumentOrderIdTestBase {
  override protected def makeId(e: Expression): FunctionInvocation = id(e)
}

class NormalizeArgumentOrderElementIdTest extends NormalizeArgumentOrderIdTestBase {
  override protected def makeId(e: Expression): FunctionInvocation = elementId(e)
}
