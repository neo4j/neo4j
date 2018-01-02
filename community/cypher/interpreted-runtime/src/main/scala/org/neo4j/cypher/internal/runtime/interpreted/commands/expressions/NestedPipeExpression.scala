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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualValues

/*
Contains an expression that is really a pipe. An inner expression is run for every row returned by the inner pipe, and
the result of the NestedPipeExpression evaluation is a collection containing the result of these inner expressions
 */
case class NestedPipeExpression(pipe: Pipe, inner: Expression) extends Expression {
  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    val innerState = state.withInitialContext(ctx).withDecorator(state.decorator.innerDecorator(owningPipe))
    val results = pipe.createResults(innerState)
    val map = results.map(ctx => inner(ctx, state))
    VirtualValues.list(map.toArray:_*)
  }

  override def rewrite(f: (Expression) => Expression) = f(NestedPipeExpression(pipe, inner.rewrite(f)))

  override def arguments = List(inner)

  override def symbolTableDependencies = Set()

  override def toString: String = s"NestedExpression()"
}
