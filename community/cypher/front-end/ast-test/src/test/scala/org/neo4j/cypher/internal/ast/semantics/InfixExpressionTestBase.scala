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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.DummyExpression
import org.neo4j.cypher.internal.ast.SemanticCheckInTest.SemanticCheckWithDefaultContext
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.TypeSpec
import org.scalatest.Assertion

object InfixExpressionTestBase {
  sealed trait ExpectedResult

  object ExpectedResult {
    case class Valid(expected: TypeSpec) extends ExpectedResult
    case class Error(message: String) extends ExpectedResult
  }
}

abstract class InfixExpressionTestBase(ctr: (Expression, Expression) => Expression) extends SemanticFunSuite {
  import InfixExpressionTestBase.ExpectedResult

  test("Should type check infix expressions deeply") {
    val exp1 = index(parameter("p", CTAny), 0)
    val exp2 = index(parameter("p", CTAny), 1)

    val result = SemanticExpressionCheck.simple(ctr(exp1, exp2)).run(SemanticState.clean, CypherVersion.Cypher25)
    result.errors should be(empty)
    val astNodes = result.state.typeTable.keys.map(_.node)
    astNodes should contain(exp1)
    astNodes should contain(exp2)
  }

  protected def testValidTypes(
    lhsTypes: TypeSpec,
    rhsTypes: TypeSpec
  )(expected: TypeSpec): Assertion =
    testValidTypesIn(lhsTypes, rhsTypes)(_ => expected)

  protected def testValidTypesIn(
    lhsTypes: TypeSpec,
    rhsTypes: TypeSpec
  )(expected: CypherVersion => TypeSpec): Assertion = {
    for (version <- CypherVersion.values()) withClue(s"[$version] ") {
      val (result, expression) = evaluateWithTypes(lhsTypes, rhsTypes, version)
      result.errors shouldBe empty
      types(expression)(result.state) should equal(expected(version))
    }
    succeed
  }

  protected def testValidTypes(
    lhsTypes: TypeSpec,
    rhsTypes: TypeSpec,
    cypherVersion: CypherVersion
  )(expected: TypeSpec): Assertion = {
    val (result, expression) = evaluateWithTypes(lhsTypes, rhsTypes, cypherVersion)
    result.errors shouldBe empty
    types(expression)(result.state) should equal(expected)
  }

  protected def testInvalidApplication(
    lhsTypes: TypeSpec,
    rhsTypes: TypeSpec
  )(message: String): Assertion =
    testInvalidApplicationIn(lhsTypes, rhsTypes)(_ => message)

  protected def testInvalidApplicationIn(
    lhsTypes: TypeSpec,
    rhsTypes: TypeSpec
  )(message: CypherVersion => String): Assertion = {
    for (version <- CypherVersion.values()) withClue(s"[$version] ") {
      val (result, _) = evaluateWithTypes(lhsTypes, rhsTypes, version)
      result.errors should not be empty
      result.errors.head.msg should equal(message(version))
    }
    succeed
  }

  protected def testInvalidApplication(
    lhsTypes: TypeSpec,
    rhsTypes: TypeSpec,
    cypherVersion: CypherVersion
  )(message: String): Assertion = {
    val (result, _) = evaluateWithTypes(lhsTypes, rhsTypes, cypherVersion)
    result.errors should not be empty
    result.errors.head.msg should equal(message)
  }

  protected def testTypesIn(
    lhsTypes: TypeSpec,
    rhsTypes: TypeSpec
  )(expected: CypherVersion => ExpectedResult): Assertion = {
    for (version <- CypherVersion.values()) withClue(s"[$version] ") {
      val (result, expression) = evaluateWithTypes(lhsTypes, rhsTypes, version)
      expected(version) match {
        case ExpectedResult.Valid(expectedType) =>
          result.errors shouldBe empty
          types(expression)(result.state) should equal(expectedType)
        case ExpectedResult.Error(message) =>
          result.errors should not be empty
          result.errors.head.msg should equal(message)
      }
    }
    succeed
  }

  protected def evaluateWithTypes(
    lhsTypes: TypeSpec,
    rhsTypes: TypeSpec,
    cypherVersion: CypherVersion = CypherVersion.Cypher25
  ): (SemanticCheckResult, Expression) = {
    val lhs = DummyExpression(lhsTypes)
    val rhs = DummyExpression(rhsTypes)

    val expression = ctr(lhs, rhs)

    val state = SemanticExpressionCheck.simple(Seq(lhs, rhs)).run(SemanticState.clean, cypherVersion).state
    (SemanticExpressionCheck.simple(expression).run(state, cypherVersion), expression)
  }
}
