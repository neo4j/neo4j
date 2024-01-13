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

import org.neo4j.cypher.internal.expressions.ArgumentOrder
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.PercentileContFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.PercentileDiscFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.PercentilesFunction
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.memory.MemoryTracker

case class PercentileCont(anInner: Expression, percentile: Expression, order: ArgumentOrder)
    extends AggregationWithInnerExpression(anInner) {

  override def createAggregationFunction(memoryTracker: MemoryTracker): AggregationFunction = {
    memoryTracker.allocateHeap(PercentileContFunction.SHALLOW_SIZE)
    new PercentileContFunction(anInner, percentile, memoryTracker, order)
  }

  def expectedInnerType: CypherType = CTNumber

  override def rewrite(f: Expression => Expression): Expression =
    f(PercentileCont(anInner.rewrite(f), percentile.rewrite(f), order))

  override def children: Seq[AstNode[_]] = Seq(anInner, percentile)
}

case class PercentileDisc(anInner: Expression, percentile: Expression, order: ArgumentOrder)
    extends AggregationWithInnerExpression(anInner) {

  override def createAggregationFunction(memoryTracker: MemoryTracker): AggregationFunction = {
    memoryTracker.allocateHeap(PercentileDiscFunction.SHALLOW_SIZE)
    new PercentileDiscFunction(anInner, percentile, memoryTracker, order)
  }

  def expectedInnerType: CypherType = CTNumber

  override def rewrite(f: Expression => Expression): Expression =
    f(PercentileDisc(anInner.rewrite(f), percentile.rewrite(f), order))

  override def children: Seq[AstNode[_]] = Seq(anInner, percentile)
}

case class Percentiles(
  anInner: Expression,
  percentiles: Expression,
  keys: Expression,
  isDiscretes: Expression,
  order: ArgumentOrder
) extends AggregationWithInnerExpression(anInner) {

  override def createAggregationFunction(memoryTracker: MemoryTracker): AggregationFunction = {
    memoryTracker.allocateHeap(PercentilesFunction.SHALLOW_SIZE)
    new PercentilesFunction(anInner, percentiles, keys, isDiscretes, memoryTracker, order)
  }

  def expectedInnerType: CypherType = CTNumber

  override def rewrite(f: Expression => Expression): Expression =
    f(Percentiles(anInner.rewrite(f), percentiles.rewrite(f), keys.rewrite(f), isDiscretes.rewrite(f), order))

  override def children: Seq[AstNode[_]] = Seq(anInner, percentiles, keys, isDiscretes)
}
