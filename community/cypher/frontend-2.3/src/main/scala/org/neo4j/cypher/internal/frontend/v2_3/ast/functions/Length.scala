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

import org.neo4j.cypher.internal.frontend.v2_3.{SemanticCheckResult, SemanticState}
import org.neo4j.cypher.internal.frontend.v2_3.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v2_3.ast.{FunctionInvocation, Function, SimpleTypedFunction}
import org.neo4j.cypher.internal.frontend.v2_3.notification.LengthOnNonPathNotification
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

case object Length extends Function with SimpleTypedFunction {
  def name = "length"

  //NOTE using CTString and CTCollection here is deprecated
  val signatures = Vector(
    Signature(Vector(CTString), CTInteger),
    Signature(Vector(CTCollection(CTAny)), CTInteger),
    Signature(Vector(CTPath), CTInteger)
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
