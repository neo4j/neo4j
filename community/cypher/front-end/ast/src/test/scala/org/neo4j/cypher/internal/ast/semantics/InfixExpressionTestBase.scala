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
package org.neo4j.cypher.internal.ast.semantics

import org.neo4j.cypher.internal.expressions.DummyExpression
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.TypeSpec
import org.scalatest.Assertion

abstract class InfixExpressionTestBase(ctr: (Expression, Expression) => Expression) extends SemanticFunSuite {

  test("Should type check infix expressions deeply") {
    val exp1 = index(parameter("p", CTAny), 0)
    val exp2 = index(parameter("p", CTAny), 1)

    val result = SemanticExpressionCheck.simple(ctr(exp1, exp2))(SemanticState.clean)
    result.errors should be(empty)
    val astNodes = result.state.typeTable.keys.map(_.node)
    astNodes should contain(exp1)
    astNodes should contain(exp2)
  }

  protected def testValidTypes(lhsTypes: TypeSpec, rhsTypes: TypeSpec)(expected: TypeSpec): Assertion = {
    val (result, expression) = evaluateWithTypes(lhsTypes, rhsTypes)
    result.errors shouldBe empty
    types(expression)(result.state) should equal(expected)
  }

  protected def testInvalidApplication(lhsTypes: TypeSpec, rhsTypes: TypeSpec)(message: String): Assertion = {
    val (result, _) = evaluateWithTypes(lhsTypes, rhsTypes)
    result.errors should not be empty
    result.errors.head.msg should equal(message)
  }

  protected def evaluateWithTypes(lhsTypes: TypeSpec, rhsTypes: TypeSpec): (SemanticCheckResult, Expression) = {
    val lhs = DummyExpression(lhsTypes)
    val rhs = DummyExpression(rhsTypes)

    val expression = ctr(lhs, rhs)

    val state = SemanticExpressionCheck.simple(Seq(lhs, rhs))(SemanticState.clean).state
    (SemanticExpressionCheck.simple(expression)(state), expression)
  }
}
