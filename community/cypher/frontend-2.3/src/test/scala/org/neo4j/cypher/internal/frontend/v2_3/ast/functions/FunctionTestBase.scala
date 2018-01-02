/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.frontend.v2_3.ast.functions

import org.neo4j.cypher.internal.frontend.v2_3.ast.DummyExpression
import org.neo4j.cypher.internal.frontend.v2_3.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{DummyPosition, SemanticCheckResult, SemanticState, ast}

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
