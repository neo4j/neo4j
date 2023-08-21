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

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.ListValueBuilder
import org.neo4j.values.virtual.VirtualValues.EMPTY_LIST

object ListLiteral {
  val empty: Literal = Literal(EMPTY_LIST)
}

case class ListLiteral(override val arguments: Expression*) extends Expression {
  private val args = arguments.toArray

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    val result = ListValueBuilder.newListBuilder(args.size)
    var i = 0
    while (i < args.length) {
      result.add(args(i).apply(row, state))
      i += 1
    }
    result.build()
  }

  def rewrite(f: Expression => Expression): Expression = f(ListLiteral(arguments.map(f): _*))

  override def children: Seq[AstNode[_]] = arguments
}
