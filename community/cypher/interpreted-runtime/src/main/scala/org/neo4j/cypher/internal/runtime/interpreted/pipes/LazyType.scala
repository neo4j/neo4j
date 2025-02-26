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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.ast.semantics.TokenTable
import org.neo4j.cypher.internal.expressions.DynamicRelTypeExpression
import org.neo4j.cypher.internal.expressions.RelTypeExpression
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.WriteQueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.internal.kernel.api.TokenWrite

sealed abstract class LazyType {
  def getOrCreateType(row: ReadableRow, state: QueryState): Int

  def getOrCreateType(row: ReadableRow, state: QueryState, token: TokenWrite): Int

  def getId(row: ReadableRow, state: QueryState): Int

  def stringified: String
}

case class LazyTypeDynamic(expr: Expression, stringified: String) extends LazyType {

  def getOrCreateType(row: ReadableRow, state: QueryState): Int = {
    CypherFunctions.getOrCreateDynamicRelType(expr(row, state), state.query)
  }

  def getOrCreateType(row: ReadableRow, state: QueryState, token: TokenWrite): Int = {
    CypherFunctions.getOrCreateDynamicRelType(expr(row, state), token)
  }

  def getId(row: ReadableRow, state: QueryState): Int = {
    val name = CypherFunctions.evaluateSingleDynamicRelType(expr(row, state))
    state.query.getOptRelTypeId(name).getOrElse(LazyType.UNKNOWN)
  }
}

case class LazyTypeStatic(name: String) extends LazyType {
  private var id = LazyType.UNKNOWN

  def stringified: String = name

  def getOrCreateType(row: ReadableRow, state: QueryState): Int =
    getOrCreateType(state.query)

  def getOrCreateType(row: ReadableRow, state: QueryState, token: TokenWrite): Int =
    getOrCreateType(token)

  def getOrCreateType(context: WriteQueryContext): Int = {
    if (id == LazyType.UNKNOWN) {
      id = context.getOrCreateRelTypeId(name)
    }
    id
  }

  def getOrCreateType(token: TokenWrite): Int = {
    if (id == LazyType.UNKNOWN) {
      id = token.relationshipTypeGetOrCreateForName(name)
    }
    id
  }

  def getId(context: ReadTokenContext): Int = {
    if (id == LazyLabel.UNKNOWN) {
      id = context.getOptRelTypeId(name).getOrElse(LazyType.UNKNOWN)
    }
    id
  }

  def getId(row: ReadableRow, state: QueryState): Int =
    getId(state.query)
}

object LazyTypeStatic {

  def apply(relTypeName: RelTypeName)(implicit table: TokenTable): LazyTypeStatic = {
    val typ = LazyTypeStatic(relTypeName.name)
    typ.id = table.id(relTypeName)
    typ
  }
}

object LazyType {
  val UNKNOWN: Int = -1

  def apply(
    relTypeExpression: RelTypeExpression,
    table: TokenTable,
    commandExpressionConverter: org.neo4j.cypher.internal.expressions.Expression => Expression
  ): LazyType = {
    relTypeExpression match {
      case name: RelTypeName => LazyTypeStatic(name)(table)
      case dyn @ DynamicRelTypeExpression(expression, _) =>
        LazyTypeDynamic(commandExpressionConverter(expression), dyn.asCanonicalStringVal)
    }
  }

  def apply(relTypeName: RelTypeName)(implicit table: TokenTable): LazyTypeStatic =
    LazyTypeStatic(relTypeName)
}
