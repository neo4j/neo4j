/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, ListSupport}
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.values._
import org.neo4j.values.storable.Values.NO_VALUE

case class ContainerIndex(expression: Expression, index: Expression) extends Expression
with ListSupport {
  def arguments = Seq(expression, index)

  override def apply(ctx: ExecutionContext,
                     state: QueryState): AnyValue = expression(ctx, state) match {
    case NO_VALUE => NO_VALUE
    case value =>
      val idx = index(ctx, state)
      if (idx eq NO_VALUE) NO_VALUE else CypherFunctions.containerIndex(value, idx, state.query)
  }

  def rewrite(f: (Expression) => Expression): Expression = f(ContainerIndex(expression.rewrite(f), index.rewrite(f)))

  def symbolTableDependencies: Set[String] = expression.symbolTableDependencies ++ index.symbolTableDependencies
}
