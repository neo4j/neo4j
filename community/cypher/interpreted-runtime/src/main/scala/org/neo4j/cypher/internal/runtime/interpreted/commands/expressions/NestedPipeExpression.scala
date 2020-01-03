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

import java.util

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualValues

/**
  * Expression that is really a pipe. An inner expression is run for every row returned by the inner pipe, and
  * the result of the NestedPipeExpression evaluation is a collection containing the result of these inner expressions
  */
case class NestedPipeExpression(pipe: Pipe,
                                inner: Expression,
                                availableExpressionVariables: Seq[ExpressionVariable]) extends Expression {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    val initialContext = pipe.executionContextFactory.copyWith(ctx)
    availableExpressionVariables.foreach { expVar =>
      initialContext.set(expVar.name, state.expressionVariables(expVar.offset))
    }
    val innerState = state.withInitialContext(initialContext).withDecorator(state.decorator.innerDecorator(owningPipe))

    val results = pipe.createResults(innerState)
    val all = new util.ArrayList[AnyValue]()
    while (results.hasNext) {
      all.add(inner(results.next(), state))
    }
    VirtualValues.fromList(all)
  }

  override def rewrite(f: Expression => Expression): Expression = f(NestedPipeExpression(pipe, inner.rewrite(f), availableExpressionVariables))

  override def arguments: Seq[Expression] = Seq(inner)

  override def children: Seq[AstNode[_]] = Seq(inner) ++ availableExpressionVariables

  override def toString: String = s"NestedExpression()"
}
