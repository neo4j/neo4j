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
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.values.AnyValue

case class StringInterpolation(stringParts: Seq[Expression], expressions: Seq[Expression]) extends Expression {
  private val stringPartsArray = stringParts.toArray
  private val expressionsArray = expressions.toArray

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    val literalPartValues = new Array[AnyValue](stringPartsArray.length)
    var j = 0
    while (j < literalPartValues.length) {
      literalPartValues(j) = stringPartsArray(j)(row, state)
      j += 1
    }

    val expressionValues = new Array[AnyValue](expressionsArray.length)
    var i = 0
    while (i < expressionValues.length) {
      expressionValues(i) = expressionsArray(i)(row, state)
      i += 1
    }
    CypherFunctions.stringInterpolate(literalPartValues, expressionValues)
  }

  override def rewrite(f: Expression => Expression): Expression =
    f(StringInterpolation(stringParts.map(e => e.rewrite(f)), expressions.map(e => e.rewrite(f))))

  override def arguments: Seq[Expression] = stringParts ++ expressions

  override def children: Seq[AstNode[_]] = stringParts ++ expressions
}
