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
package org.neo4j.cypher.internal.ast.semantics.functions

import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticFunSuite
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.DummyExpression
import org.neo4j.cypher.internal.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.symbols.TypeSpec

abstract class FunctionTestBase(funcName: String) extends SemanticFunSuite {

  protected val context: SemanticContext = SemanticContext.Simple

  protected def testValidTypes(argumentTypes: TypeSpec*)(expected: TypeSpec): Unit = {
    val (result, invocation) = evaluateWithTypes(argumentTypes.toIndexedSeq)
    result.errors shouldBe empty
    types(invocation)(result.state) should equal(expected)
  }

  protected def testInvalidApplication(argumentTypes: TypeSpec*)(message: String): Unit = {
    val (result, _) = evaluateWithTypes(argumentTypes.toIndexedSeq)
    result.errors should not be empty
    result.errors.head.msg should equal(message)
  }

  protected def evaluateWithTypes(argumentTypes: IndexedSeq[TypeSpec]): (SemanticCheckResult, FunctionInvocation) = {
    val arguments = argumentTypes.map(DummyExpression(_))

    val invocation = FunctionInvocation(
      FunctionName(funcName)(DummyPosition(6)),
      distinct = false,
      arguments
    )(DummyPosition(5))

    val state = SemanticExpressionCheck.check(context, arguments)(SemanticState.clean).state
    (SemanticExpressionCheck.check(context, invocation)(state), invocation)
  }
}

trait AggregationFunctionTestBase {
  self: FunctionTestBase =>

  override protected val context: SemanticContext = SemanticContext.Results
}
