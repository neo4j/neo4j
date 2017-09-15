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
package org.neo4j.cypher.internal.frontend.v3_4.ast.functions

import org.neo4j.cypher.internal.frontend.v3_4.SemanticCheck
import org.neo4j.cypher.internal.frontend.v3_4.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_4.ast.{Function, FunctionInvocation}
import org.neo4j.cypher.internal.frontend.v3_4.semantics.{SemanticExpressionCheck, SemanticCheckResult, SemanticError, SemanticState}
import org.neo4j.cypher.internal.frontend.v3_4.symbols._

case object ToBoolean extends Function {

  def name = "toBoolean"

  override protected def semanticCheck(ctx: SemanticContext, invocation: FunctionInvocation): SemanticCheck =
    checkMinArgs(invocation, 1) ifOkChain
      checkMaxArgs(invocation, 1) ifOkChain
      checkTypeOfArgument(invocation) ifOkChain
      SemanticExpressionCheck.specifyType(CTBoolean, invocation)

  private def checkTypeOfArgument(invocation: FunctionInvocation): SemanticCheck = (s: SemanticState) => {
    val argument = invocation.args.head
    val specifiedType = s.expressionType(argument).specified
    val correctType = Seq(CTString, CTBoolean, CTAny).foldLeft(false) {
      case (acc, t) => acc || specifiedType.contains(t)
    }

    if (correctType) SemanticCheckResult.success(s)
    else {
      val message = s"Type mismatch: expected Boolean or String but was ${specifiedType.mkString(", ")}"
      SemanticCheckResult.error(s, SemanticError(message, argument.position))
    }
  }
}
