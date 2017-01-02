/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher.internal.frontend.v3_1.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_1.ast.{Function, FunctionInvocation}
import org.neo4j.cypher.internal.frontend.v3_1.symbols._
import org.neo4j.cypher.internal.frontend.v3_1.{SemanticCheck, SemanticCheckResult, SemanticError, SemanticState}

case object ToBoolean extends Function {

  def name = "toBoolean"

  override protected def semanticCheck(ctx: SemanticContext, invocation: FunctionInvocation): SemanticCheck =
    checkMinArgs(invocation, 1) ifOkChain
      checkMaxArgs(invocation, 1) ifOkChain
      checkTypeOfArgument(invocation) ifOkChain
      invocation.specifyType(CTBoolean)

  private def checkTypeOfArgument(invocation: FunctionInvocation): SemanticCheck = (s: SemanticState) => {
    val argument = invocation.args.head
    val specifiedType = s.expressionType(argument).specified
    val correctType = Seq(CTString, CTBoolean, CTAny).foldLeft(false) {
      case (acc, t) => acc || specifiedType.contains(t)
    }

    if (correctType) SemanticCheckResult.success(s)
    else {
      val message = s"Type mismatch: expected Number or String but was ${specifiedType.mkString(", ")}"
      SemanticCheckResult.error(s, SemanticError(message, argument.position))
    }
  }
}
