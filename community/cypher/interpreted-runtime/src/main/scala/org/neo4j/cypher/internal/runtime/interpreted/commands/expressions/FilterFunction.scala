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

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.ListSupport
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualValues

import scala.collection.mutable.ArrayBuffer

case class FilterFunction(collection: Expression, id: String, predicate: Predicate)
  extends NullInNullOutExpression(collection)
  with ListSupport
  with Closure {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState) = {
    val list = makeTraversable(value)
    val innerContext = m.createClone()
    val filtered = new ArrayBuffer[AnyValue]
    val inputs = list.iterator()
    while (inputs.hasNext()) {
      val value = inputs.next()
      if (predicate.isTrue(innerContext.set(id, value), state)) {
        filtered += value
      }
    }
    VirtualValues.list(filtered.toArray:_*)
  }

  def rewrite(f: (Expression) => Expression) =
    f(FilterFunction(collection.rewrite(f), id, predicate.rewriteAsPredicate(f)))

  override def children = Seq(collection, predicate)

  def arguments: Seq[Expression] = Seq(collection)

  def symbolTableDependencies = symbolTableDependencies(collection, predicate, id)
}
