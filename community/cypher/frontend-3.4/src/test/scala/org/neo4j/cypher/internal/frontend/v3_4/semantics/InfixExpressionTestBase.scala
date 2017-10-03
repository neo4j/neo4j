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
package org.neo4j.cypher.internal.frontend.v3_4.semantics

import org.neo4j.cypher.internal.frontend.v3_4.ast.DummyExpression
import org.neo4j.cypher.internal.aux.v3_4.symbols._
import org.neo4j.cypher.internal.v3_4.expressions.Expression

abstract class InfixExpressionTestBase(ctr: (Expression, Expression) => Expression) extends SemanticFunSuite {

  protected def testValidTypes(lhsTypes: TypeSpec, rhsTypes: TypeSpec)(expected: TypeSpec) {
    val (result, expression) = evaluateWithTypes(lhsTypes, rhsTypes)
    result.errors shouldBe empty
    types(expression)(result.state) should equal(expected)
  }

  protected def testInvalidApplication(lhsTypes: TypeSpec, rhsTypes: TypeSpec)(message: String) {
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
