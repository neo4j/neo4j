/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_1.ast.functions

import org.neo4j.cypher.internal.frontend.v3_1.{SemanticCheckResult, SemanticError, SemanticState, _}
import org.neo4j.cypher.internal.frontend.v3_1.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_1.ast.{Function, FunctionInvocation}
import org.neo4j.cypher.internal.frontend.v3_1.symbols._

case object ToFloat extends Function {
  def name = "toFloat"

  override protected def semanticCheck(ctx: SemanticContext, invocation: FunctionInvocation): SemanticCheck =
    checkMinArgs(invocation, 1) ifOkChain
      checkMaxArgs(invocation, 1) ifOkChain
      checkTypeOfArgument(invocation) ifOkChain
      invocation.specifyType(CTFloat)

  private def checkTypeOfArgument(invocation: FunctionInvocation): SemanticCheck = (s: SemanticState) => {
    val e = invocation.args.head

    s.expressionType(e).specified match {
      case CTFloat.invariant |
           CTInteger.invariant |
           CTString.invariant |
           CTNumber.invariant |
           CTAny.invariant => SemanticCheckResult.success(s)

      case
        CTAny.covariant => SemanticCheckResult.success(s)

      case x =>
        val message = s"Type mismatch: expected Number or String but was ${x.mkString(", ")}"
        SemanticCheckResult.error(s, SemanticError(message, invocation.args.head.position))
    }
  }
}
