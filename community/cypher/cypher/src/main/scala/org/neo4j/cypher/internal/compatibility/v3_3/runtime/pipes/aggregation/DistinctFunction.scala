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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.aggregation

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_3._
import org.neo4j.cypher.internal.compiler.v3_3.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_3.commands.predicates.Equivalent
import org.neo4j.cypher.internal.compiler.v3_3.pipes.QueryState

class DistinctFunction(value: Expression, inner: AggregationFunction) extends AggregationFunction {
  private val seen = scala.collection.mutable.Set[Equivalent]()
  private var seenNull = false

  override def apply(ctx: ExecutionContext)(implicit state: QueryState) {
    val data = value(ctx)

    if (data == null) {
      if (!seenNull) {
        seenNull = true
        inner(ctx)
      }
    } else {
      val equiValue = Equivalent(data)
      if (!seen.contains(equiValue)) {
        seen += equiValue
        inner(ctx)
      }
    }
  }

  override def result(implicit state: QueryState) = inner.result
}
