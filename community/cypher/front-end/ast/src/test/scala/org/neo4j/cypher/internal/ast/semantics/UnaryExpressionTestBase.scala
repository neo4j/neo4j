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
import org.neo4j.cypher.internal.util.symbols.TypeSpec
import org.scalatest.Assertion

abstract class UnaryExpressionTestBase(ctr: Expression => Expression) extends SemanticFunSuite {

  protected def testValidTypes(lhsTypes: TypeSpec)(expected: TypeSpec): Assertion = {
    val (result, expression) = evaluateWithTypes(lhsTypes)
    result.errors shouldBe empty
    types(expression)(result.state) should equal(expected)
  }

  protected def testInvalidApplication(lhsTypes: TypeSpec)(message: String): Assertion = {
    val (result, _) = evaluateWithTypes(lhsTypes)
    result.errors should not be empty
    result.errors.head.msg should equal(message)
  }

  protected def evaluateWithTypes(lhsTypes: TypeSpec): (SemanticCheckResult, Expression) = {
    val lhs = DummyExpression(lhsTypes)

    val expression = ctr(lhs)

    val initialState = SemanticState.clean

    val state = SemanticExpressionCheck.simple(lhs)(initialState).state
    (SemanticExpressionCheck.simple(expression)(state), expression)
  }
}
