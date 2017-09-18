/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualValues

object ListLiteral {
  val empty = Literal(Seq())
}

case class ListLiteral(arguments: Expression*) extends Expression {
  def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    val argumentValues = arguments.map { expression =>
      expression(ctx, state)
    }
    VirtualValues.list(argumentValues: _*)
  }

  def rewrite(f: (Expression) => Expression): Expression = f(ListLiteral(arguments.map(f): _*))

  def symbolTableDependencies = arguments.flatMap(_.symbolTableDependencies).toSet
}
