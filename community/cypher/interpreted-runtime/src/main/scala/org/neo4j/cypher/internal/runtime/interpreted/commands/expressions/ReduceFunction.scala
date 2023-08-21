/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.ListSupport
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue

case class ReduceFunction(
  collection: Expression,
  innerVariableName: String,
  innerVariableOffset: Int,
  expression: Expression,
  accVariableName: String,
  accVariableOffset: Int,
  init: Expression
) extends NullInNullOutExpression(collection) with ListSupport {

  override def compute(value: AnyValue, ctx: ReadableRow, state: QueryState): AnyValue = {
    val list = makeTraversable(value)
    val iterator = list.iterator()
    val initialAcc = init(ctx, state)

    state.expressionVariables(accVariableOffset) = initialAcc
    while (iterator.hasNext) {
      state.expressionVariables(innerVariableOffset) = iterator.next()
      state.expressionVariables(accVariableOffset) = expression(ctx, state)
    }
    state.expressionVariables(accVariableOffset)
  }

  override def rewrite(f: Expression => Expression): Expression =
    f(ReduceFunction(
      collection.rewrite(f),
      innerVariableName,
      innerVariableOffset,
      expression.rewrite(f),
      accVariableName,
      accVariableOffset,
      init.rewrite(f)
    ))

  override def arguments: Seq[Expression] = Seq(collection, init)

  override def children: Seq[Expression] = Seq(collection, expression, init)

}
