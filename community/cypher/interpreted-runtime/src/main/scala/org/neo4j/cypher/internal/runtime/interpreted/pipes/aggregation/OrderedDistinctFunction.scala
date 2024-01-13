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
package org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.memory.HeapEstimator.shallowSizeOfInstance
import org.neo4j.values.AnyValue

class OrderedDistinctFunction(value: Expression, inner: AggregationFunction)
    extends AggregationFunction {
  private var prevSeen: AnyValue = _

  override def apply(ctx: ReadableRow, state: QueryState): Unit = {
    val data = value(ctx, state)
    if (prevSeen == null || prevSeen != data) {
      prevSeen = data
      inner(ctx, state)
    }
  }

  override def result(state: QueryState): AnyValue = inner.result(state)
}

object OrderedDistinctFunction {
  val SHALLOW_SIZE: Long = shallowSizeOfInstance(classOf[OrderedDistinctFunction])
}
