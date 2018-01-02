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

import org.neo4j.cypher.internal.frontend.v2_3.ast.Function
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticCheck, SemanticError, ast}

// this implementation exists only to handle the case where "reduce(x = 0, x in y : foo)" is parsed as a function invocation,
// rather than a ReduceExpression
case object Reduce extends Function {
  def name = "reduce"

  override def semanticCheckHook(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation): SemanticCheck =
    semanticCheck(ctx, invocation)

  def semanticCheck(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation): SemanticCheck =
    SemanticError(s"${name}(...) requires '| expression' (an accumulation expression)", invocation.position)
}
