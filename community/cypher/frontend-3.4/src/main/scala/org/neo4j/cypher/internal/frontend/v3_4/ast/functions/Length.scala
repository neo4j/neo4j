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

import org.neo4j.cypher.internal.frontend.v3_4.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_4.ast.{ExpressionSignature, Function, FunctionInvocation, SimpleTypedFunction}
import org.neo4j.cypher.internal.frontend.v3_4.notification.LengthOnNonPathNotification
import org.neo4j.cypher.internal.frontend.v3_4.symbols._
import org.neo4j.cypher.internal.frontend.v3_4.{SemanticCheckResult, SemanticState}

case object Length extends Function with SimpleTypedFunction {
  def name = "length"

  //NOTE using CTString and CTCollection here is deprecated
  override val signatures = Vector(
    ExpressionSignature(Vector(CTString), CTInteger),
    ExpressionSignature(Vector(CTList(CTAny)), CTInteger),
    ExpressionSignature(Vector(CTPath), CTInteger)
  )

  override def semanticCheck(ctx: SemanticContext, invocation: FunctionInvocation) =
    super.semanticCheck(ctx, invocation) chain checkForInvalidUsage(ctx, invocation)

  def checkForInvalidUsage(ctx: SemanticContext, invocation: FunctionInvocation) = (originalState: SemanticState) => {
    val newState = invocation.args.foldLeft(originalState) {
      case (state, expr) if state.expressionType(expr).actual != CTPath.invariant =>
        state.addNotification(LengthOnNonPathNotification(expr.position))
      case (state, expr) =>
        state
    }

    SemanticCheckResult(newState, Seq.empty)
  }
}
