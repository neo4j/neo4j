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

import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.DistinctFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.OrderedDistinctFunction
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.memory.MemoryTracker

case class Distinct(innerAggregator: AggregationExpression, expression: Expression, isOrdered: Boolean)
    extends AggregationWithInnerExpression(expression) {
  override val expectedInnerType: CypherType = CTAny

  override def createAggregationFunction(memoryTracker: MemoryTracker): AggregationFunction = {
    if (isOrdered) {
      memoryTracker.allocateHeap(OrderedDistinctFunction.SHALLOW_SIZE)
      new OrderedDistinctFunction(expression, innerAggregator.createAggregationFunction(memoryTracker))
    } else {
      memoryTracker.allocateHeap(DistinctFunction.SHALLOW_SIZE)
      new DistinctFunction(expression, innerAggregator.createAggregationFunction(memoryTracker), memoryTracker)
    }
  }

  override def rewrite(f: Expression => Expression): Expression = innerAggregator.rewrite(f) match {
    case inner: AggregationExpression => f(Distinct(inner, expression.rewrite(f), isOrdered))
    case _                            => f(Distinct(innerAggregator, expression.rewrite(f), isOrdered))
  }

  override def arguments: Seq[Expression] = Seq(expression, innerAggregator)

  override def children: Seq[AstNode[_]] = Seq(expression, innerAggregator)
}
