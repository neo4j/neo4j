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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.ListSupport
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.{ListValue, VirtualValues}

case class ExtractFunction(collection: Expression, id: String, expression: Expression)
  extends NullInNullOutExpression(collection)
  with ListSupport
  with Closure {
  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): ListValue = {
    val list = makeTraversable(value)
    VirtualValues.transform(list, new java.util.function.Function[AnyValue, AnyValue]() {
      override def apply(v1: AnyValue): AnyValue = expression(m.newWith1(id, v1), state)
    })
  }

  def rewrite(f: (Expression) => Expression) = f(ExtractFunction(collection.rewrite(f), id, expression.rewrite(f)))

  override def children = Seq(collection, expression)


  def arguments: Seq[Expression] = Seq(collection)

  def symbolTableDependencies = symbolTableDependencies(collection, expression, id)
}
