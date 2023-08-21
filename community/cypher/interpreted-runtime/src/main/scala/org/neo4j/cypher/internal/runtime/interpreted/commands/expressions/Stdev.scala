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
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.StdevFunction
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.memory.MemoryTracker

case class Stdev(anInner: Expression) extends AggregationWithInnerExpression(anInner) {

  override def createAggregationFunction(memoryTracker: MemoryTracker): AggregationFunction = {
    memoryTracker.allocateHeap(StdevFunction.SHALLOW_SIZE)
    new StdevFunction(anInner, false)
  }

  override def expectedInnerType: CypherType = CTNumber

  override def rewrite(f: Expression => Expression): Expression = f(Stdev(anInner.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(anInner)
}

case class StdevP(anInner: Expression) extends AggregationWithInnerExpression(anInner) {

  override def createAggregationFunction(memoryTracker: MemoryTracker): AggregationFunction =
    new StdevFunction(anInner, true)

  override def expectedInnerType: CypherType = CTNumber

  override def rewrite(f: Expression => Expression): Expression = f(StdevP(anInner.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(anInner)
}
