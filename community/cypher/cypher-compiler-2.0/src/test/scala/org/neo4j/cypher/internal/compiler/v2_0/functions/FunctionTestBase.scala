/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.functions

import org.neo4j.cypher.internal.compiler.v2_0._
import ast.Expression.SemanticContext
import symbols._
import org.scalatest.Assertions

abstract class FunctionTestBase(funcName: String) extends Assertions {

  protected def testValidTypes(argumentTypes: TypeSpec*)(expected: TypeSpec) {
    val (result, invocation) = evaluateWithTypes(argumentTypes.toIndexedSeq)
    assert(result.errors.isEmpty)
    assert(invocation.types(result.state) === expected)
  }

  protected def testInvalidApplication(argumentTypes: TypeSpec*)(message: String) {
    val (result, _) = evaluateWithTypes(argumentTypes.toIndexedSeq)
    assert(result.errors.nonEmpty)
    assert(result.errors.head.msg === message)
  }

  protected def evaluateWithTypes(argumentTypes: IndexedSeq[TypeSpec]): (SemanticCheckResult, ast.FunctionInvocation) = {
    val arguments = argumentTypes.zipWithIndex.map {
      case (t, i) => DummyExpression(t, DummyToken(i*2+4, i*2+5))
    }

    val invocation = ast.FunctionInvocation(
      ast.Identifier(funcName, DummyToken(6, 7)),
      distinct = false,
      arguments,
      DummyToken(5, 14)
    )

    val state = arguments.semanticCheck(SemanticContext.Simple)(SemanticState.clean).state
    (invocation.semanticCheck(SemanticContext.Simple)(state), invocation)
  }

}
