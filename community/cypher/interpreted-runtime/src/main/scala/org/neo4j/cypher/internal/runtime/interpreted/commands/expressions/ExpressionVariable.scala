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

import org.neo4j.cypher.internal.runtime.{ExecutionContext, ast}
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.v4_0.expressions.LogicalVariable
import org.neo4j.values.AnyValue

object ExpressionVariable {
  def of(e: LogicalVariable): ExpressionVariable = {
    val ev = ast.ExpressionVariable.cast(e)
    ExpressionVariable(ev.offset, ev.name)
  }
}

case class ExpressionVariable(offset: Int, name: String) extends VariableCommand(name) {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = state.expressionVariables(offset)

  override def children: Seq[AstNode[_]] = Seq.empty
}
