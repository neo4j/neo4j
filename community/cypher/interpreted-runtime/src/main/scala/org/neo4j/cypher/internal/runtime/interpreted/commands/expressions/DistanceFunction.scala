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
import org.neo4j.cypher.operations.CypherFunctions.distance
import org.neo4j.values.AnyValue

case class DistanceFunction(p1: Expression, p2: Expression) extends Expression {

  override def apply(row: ReadableRow, state: QueryState): AnyValue = distance(p1(row, state), p2(row, state))

  override def rewrite(f: Expression => Expression): Expression = f(DistanceFunction(p1.rewrite(f), p2.rewrite(f)))

  override def arguments: collection.Seq[Expression] = p1.arguments ++ p2.arguments

  override def children: collection.Seq[AstNode[_]] = Seq(p1, p2)

  override def toString: String = "Distance(" + p1 + ", " + p2 + ")"
}
