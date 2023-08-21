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
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ExpressionVariable
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NestedPipeExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id

/**
 * Slotted variant of [[org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NestedPipeCollectExpression]]
 */
abstract class NestedPipeSlottedExpression(
  pipe: Pipe,
  slots: SlotConfiguration,
  availableExpressionVariables: Array[ExpressionVariable],
  owningPlanId: Id
) extends NestedPipeExpression(pipe, availableExpressionVariables, owningPlanId) {

  // NOTE: we make this distinct in the case of multiple availableExpressionVariables maps to the same slot
  private val expVarSlotsInNestedPlan =
    availableExpressionVariables.map(ev => slots.getReferenceOffsetFor(ev.name)).distinct

  override protected def createInitialContext(ctx: ReadableRow, state: QueryState): SlottedRow = {
    val initialContext = new SlottedRow(slots)
    initialContext.copyFrom(ctx, slots.numberOfLongs, slots.numberOfReferences - expVarSlotsInNestedPlan.length)
    var i = 0
    while (i < expVarSlotsInNestedPlan.length) {
      val expVar = availableExpressionVariables(i)
      initialContext.setRefAt(expVarSlotsInNestedPlan(i), state.expressionVariables(expVar.offset))
      i += 1
    }
    initialContext
  }
}
