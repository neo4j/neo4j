/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, GraphElementPropertyFunctions}
import org.neo4j.cypher.internal.v3_4.logical.plans.UserFunctionSignature
import org.neo4j.values._

abstract class FunctionInvocation(signature: UserFunctionSignature, arguments: IndexedSeq[Expression])
  extends Expression with GraphElementPropertyFunctions {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    val query = state.query
    val argValues = arguments.map(arg => {
      arg(ctx, state)
    })
    call(query, argValues)
  }

  protected def call(query: QueryContext,
                   argValues: IndexedSeq[AnyValue]): AnyValue


  override def symbolTableDependencies = arguments.flatMap(_.symbolTableDependencies).toSet

  override def toString = s"${signature.name}(${arguments.mkString(",")})"
}

case class FunctionInvocationById(signature: UserFunctionSignature, arguments: IndexedSeq[Expression])
  extends FunctionInvocation(signature, arguments) {

  protected def call(query: QueryContext,
                   argValues: IndexedSeq[AnyValue]): AnyValue = {
    query.callFunction(signature.id.get, argValues, signature.allowed)
  }

  override def rewrite(f: (Expression) => Expression) =
    f(FunctionInvocationById(signature, arguments.map(a => a.rewrite(f))))
}

case class FunctionInvocationByName(signature: UserFunctionSignature, arguments: IndexedSeq[Expression])
  extends FunctionInvocation(signature, arguments) {

  protected def call(query: QueryContext,
                     argValues: IndexedSeq[AnyValue]): AnyValue = {
    query.callFunction(signature.name, argValues, signature.allowed)
  }

  override def rewrite(f: (Expression) => Expression) =
    f(FunctionInvocationByName(signature, arguments.map(a => a.rewrite(f))))
}
