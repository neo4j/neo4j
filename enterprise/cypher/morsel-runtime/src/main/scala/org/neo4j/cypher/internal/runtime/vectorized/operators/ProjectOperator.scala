/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v3_4.runtime._
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}

class ProjectOperator(slot: Slot, expression: Expression, pipeline: PipelineInformation) extends MiddleOperator {

  private val project = slot match {
    case LongSlot(offset, _, _, _) =>
      // We just pass along Long slot expressions without evaluation
      (ctx: ExecutionContext, state: OldQueryState) =>

    case RefSlot(offset, _, _, _) =>
      (ctx: ExecutionContext, state: OldQueryState) =>
        val result = expression(ctx, state)
        ctx.setRefAt(offset, result)
  }

  override def operate(iterationState: Iteration, data: Morsel, context: QueryContext, state: QueryState): Unit = {
    var currentRow = 0
    val longCount = pipeline.numberOfLongs
    val refCount = pipeline.numberOfReferences
    val executionContext = new MorselExecutionContext(data, longCount, refCount, currentRow = currentRow)
    val queryState = new OldQueryState(context, resources = null, params = state.params)

    while(currentRow < data.validRows) {
      executionContext.currentRow = currentRow
      project(executionContext, queryState)
      currentRow += 1
    }
  }
}
