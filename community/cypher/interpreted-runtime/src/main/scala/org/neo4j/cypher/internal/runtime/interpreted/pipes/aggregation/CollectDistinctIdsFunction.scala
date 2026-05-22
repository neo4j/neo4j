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

import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.UnorderedLongSetListValue

class CollectDistinctIdsFunction(value: Expression, memoryTracker: MemoryTracker) extends AggregationFunction {
  // we use a fairly large initial capacity since we expect this to grow pretty big
  // and benchmarks shows that we spend a lot of time resizing otherwise
  private[this] val builder = UnorderedLongSetListValue.heapTrackingBuilder(memoryTracker, 1024)

  override def apply(data: ReadableRow, state: QueryState): Unit = {
    value(data, state) match {
      case IsNoValue() => onNoValue(state)
      case v           => builder.add(CypherFunctions.asLong(v))
    }
  }

  override def result(state: QueryState): AnyValue = {
    builder.buildAndClose()
  }
}
