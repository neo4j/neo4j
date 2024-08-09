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
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualValues

case class ExtractFunction(
  collection: Expression,
  innerVariableName: String,
  innerVariableOffset: Int,
  expression: Expression
) extends NullInNullOutExpression(collection)
    with ListSupport {

  override def compute(value: AnyValue, row: ReadableRow, state: QueryState): ListValue = {
    val list = makeTraversable(value)
    val extracted = new Array[AnyValue](list.intSize())
    val values = list.iterator()
    var i = 0
    while (values.hasNext) {
      val value = values.next()
      state.expressionVariables(innerVariableOffset) = value
      extracted(i) = expression(row, state)
      i += 1
    }
    VirtualValues.list(extracted: _*)
  }

  def rewrite(f: Expression => Expression): Expression =
    f(ExtractFunction(collection.rewrite(f), innerVariableName, innerVariableOffset, expression.rewrite(f)))

  override def children: Seq[Expression] = Seq(collection, expression)

  def arguments: Seq[Expression] = Seq(collection)

}
