/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_4.semantics.functions

import org.neo4j.cypher.internal.util.v3_4.DummyPosition
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.frontend.v3_4.semantics.{SemanticCheckResult, SemanticExpressionCheck, SemanticFunSuite, SemanticState}
import org.neo4j.cypher.internal.v3_4.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.v3_4.expressions.{DummyExpression, FunctionInvocation, FunctionName}

abstract class FunctionTestBase(funcName: String) extends SemanticFunSuite {

  protected val context: SemanticContext = SemanticContext.Simple

  protected def testValidTypes(argumentTypes: TypeSpec*)(expected: TypeSpec) {
    val (result, invocation) = evaluateWithTypes(argumentTypes.toIndexedSeq)
    result.errors shouldBe empty
    types(invocation)(result.state) should equal(expected)
  }

  protected def testInvalidApplication(argumentTypes: TypeSpec*)(message: String) {
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
