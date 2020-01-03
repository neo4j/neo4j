/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.logical.plans.UserFunctionSignature
import org.neo4j.cypher.internal.runtime.interpreted.GraphElementPropertyFunctions
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext}
import org.neo4j.values._


case class FunctionInvocation(signature: UserFunctionSignature, input: Array[Expression])
  extends Expression with GraphElementPropertyFunctions {

  override def arguments: Seq[Expression] = input

  override def children: Seq[AstNode[_]] = input

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    val query = state.query
    val argValues = input.map(arg => {
      arg(ctx, state)
    })
    call(query, argValues)
  }

  protected def call(query: QueryContext, argValues: Array[AnyValue]): AnyValue =
    query.callFunction(signature.id, argValues, signature.allowed)

  override def toString = s"${signature.name}(${input.mkString(",")})"

  override def rewrite(f: Expression => Expression): Expression =
    f(FunctionInvocation(signature, input.map(a => a.rewrite(f))))
}
