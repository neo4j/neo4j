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
package org.neo4j.cypher.internal.frontend.v3_3.ast.functions

import org.neo4j.cypher.internal.frontend.v3_3.ast.DummyExpression
import org.neo4j.cypher.internal.frontend.v3_3.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_3.{DummyPosition, SemanticCheckResult, SemanticState, ast}

abstract class FunctionTestBase(funcName: String) extends CypherFunSuite {

  protected val context: SemanticContext = SemanticContext.Simple

  protected def testValidTypes(argumentTypes: TypeSpec*)(expected: TypeSpec) {
    val (result, invocation) = evaluateWithTypes(argumentTypes.toIndexedSeq)
    result.errors shouldBe empty
    invocation.types(result.state) should equal(expected)
  }

  protected def testInvalidApplication(argumentTypes: TypeSpec*)(message: String) {
    val (result, _) = evaluateWithTypes(argumentTypes.toIndexedSeq)
    result.errors should not be empty
    result.errors.head.msg should equal(message)
  }

  protected def evaluateWithTypes(argumentTypes: IndexedSeq[TypeSpec]): (SemanticCheckResult, ast.FunctionInvocation) = {
    val arguments = argumentTypes.map(DummyExpression(_))

    val invocation = ast.FunctionInvocation(
      ast.FunctionName(funcName)(DummyPosition(6)),
      distinct = false,
      arguments
    )(DummyPosition(5))

    val state = arguments.semanticCheck(context)(SemanticState.clean).state
    (invocation.semanticCheck(context)(state), invocation)
  }
}
