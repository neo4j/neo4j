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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.aggregation

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions.Expression
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.QueryState
import org.neo4j.values.AnyValue

class DistinctFunction(value: Expression, inner: AggregationFunction) extends AggregationFunction {
  private val seen = scala.collection.mutable.Set[AnyValue]()
  private var seenNull = false

  override def apply(ctx: ExecutionContext, state: QueryState) {
    val data = value(ctx, state)

    if (data == null) {
      if (!seenNull) {
        seenNull = true
        inner(ctx, state)
      }
    } else {
      if (!seen.contains(data)) {
        seen += data
        inner(ctx, state)
      }
    }
  }

  override def result(state: QueryState): AnyValue = inner.result(state)
}
