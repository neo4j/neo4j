/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_5.rewriting

import org.neo4j.cypher.internal.v3_5.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.normalizeArgumentOrder
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.expressions._

class NormalizeArgumentOrderTest extends CypherFunSuite with AstConstructionTestSupport {

  test("a.prop = b.prop rewritten to: a.prop = b.prop") {
    val lhs: Expression = Property(varFor("a"), PropertyKeyName("prop")_)_
    val rhs: Expression = Property(varFor("b"), PropertyKeyName("prop")_)_

    val input: Expression = Equals(lhs, rhs)_

    normalizeArgumentOrder(input) should equal(input)
  }

  test("12 = a.prop rewritten to: a.prop = 12") {
    val lhs: Expression = SignedDecimalIntegerLiteral("12")_
    val rhs: Expression = Property(varFor("a"), PropertyKeyName("prop")_)_

    val input: Expression = Equals(lhs, rhs)_
    val expected: Expression = Equals(rhs, lhs)_

    normalizeArgumentOrder(input) should equal(expected)
  }

  test("id(a) = id(b) rewritten to: id(a) = id(b)") {
    val lhs: Expression = id("a")
    val rhs: Expression = id("b")

    val input: Expression = Equals(lhs, rhs)_

    normalizeArgumentOrder(input) should equal(input)
  }

  test("23 = id(a) rewritten to: id(a) = 23") {
    val lhs: Expression = SignedDecimalIntegerLiteral("12")_
    val rhs: Expression = id("a")

    val input: Expression = Equals(lhs, rhs)_
    val expected: Expression = Equals(rhs, lhs)_

    normalizeArgumentOrder(input) should equal(expected)
  }

  test("a.prop = id(b) rewritten to: id(b) = a.prop") {
    val lhs: Expression = Property(varFor("a"), PropertyKeyName("prop")_)_
    val rhs: Expression = id("b")

    val input: Expression = Equals(rhs, lhs)_

    normalizeArgumentOrder(input) should equal(input)
  }

  test("id(a) = b.prop rewritten to: id(a) = b.prop") {
    val lhs: Expression = id("a")
    val rhs: Expression = Property(varFor("b"), PropertyKeyName("prop")_)_

    val input: Expression = Equals(lhs, rhs)_

    normalizeArgumentOrder(input) should equal(input)
  }

  test("a < n.prop rewritten to: n.prop > a") {
    val lhs: Expression = id("a")
    val rhs: Expression = Property(varFor("n"), PropertyKeyName("prop")_)_

    val input: Expression = LessThan(lhs, rhs)_

    normalizeArgumentOrder(input) should equal(GreaterThan(rhs, lhs)(pos))
  }

  test("a <= n.prop rewritten to: n.prop >= a") {
    val lhs: Expression = id("a")
    val rhs: Expression = Property(varFor("n"), PropertyKeyName("prop")_)_

    val input: Expression = LessThanOrEqual(lhs, rhs)_

    normalizeArgumentOrder(input) should equal(GreaterThanOrEqual(rhs, lhs)(pos))
  }

  test("a > n.prop rewritten to: n.prop < a") {
    val lhs: Expression = id("a")
    val rhs: Expression = Property(varFor("n"), PropertyKeyName("prop")_)_

    val input: Expression = GreaterThan(lhs, rhs)_

    normalizeArgumentOrder(input) should equal(LessThan(rhs, lhs)(pos))
  }

  test("a >= n.prop rewritten to: n.prop <= a") {
    val lhs: Expression = id("a")
    val rhs: Expression = Property(varFor("n"), PropertyKeyName("prop")_)_

    val input: Expression = GreaterThanOrEqual(lhs, rhs)_

    normalizeArgumentOrder(input) should equal(LessThanOrEqual(rhs, lhs)(pos))
  }

  private def id(name: String): FunctionInvocation =
    FunctionInvocation(FunctionName("id")(pos), distinct = false, Array(Variable(name)(pos)))(pos)
}


