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

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.values.AnyValue

case class PointFunction(data: Expression) extends NullInNullOutExpression(data) {

  override def compute(value: AnyValue, ctx: ExecutionContext, state: QueryState): AnyValue =
    CypherFunctions.point(value, state.query)

  override def rewrite(f: Expression => Expression): Expression = f(PointFunction(data.rewrite(f)))

  override def arguments: Seq[Expression] = data.arguments

  override def children: Seq[AstNode[_]] = Seq(data)

  override def symbolTableDependencies: Set[String] = data.symbolTableDependencies

  override def toString: String = "Point(" + data + ")"
}
