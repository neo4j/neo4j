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

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.virtual.HeapTrackingListValueBuilder
import org.neo4j.values.virtual.ListValue

/**
 * An expression which delegates evaluation to a pipe.
 */
abstract class NestedPipeExpression(
  pipe: Pipe,
  availableExpressionVariables: Array[ExpressionVariable],
  owningPlanId: Id
) extends Expression {

  protected def createNestedResults(row: ReadableRow, state: QueryState): ClosingIterator[CypherRow] = {
    val initialContext: CypherRow = createInitialContext(row, state)
    val innerState =
      state.withInitialContextAndDecorator(initialContext, state.decorator.innerDecorator(owningPlanId))

    pipe.createResults(innerState)
  }

  protected def createInitialContext(row: ReadableRow, state: QueryState): CypherRow = {
    val initialContext = pipe.rowFactory.copyWith(row)
    availableExpressionVariables.foreach { expVar =>
      initialContext.set(expVar.name, state.expressionVariables(expVar.offset))
    }
    initialContext
  }

  protected def collectResults(
    state: QueryState,
    results: ClosingIterator[CypherRow],
    projection: Expression,
    memoryTracker: MemoryTracker
  ): ListValue = {
    val all = HeapTrackingListValueBuilder.newHeapTrackingListBuilder(memoryTracker)
    while (results.hasNext) {
      all.add(projection(results.next(), state))
    }
    all.buildAndClose()
  }

  override def toString: String = s"${getClass.getSimpleName}()"
}
