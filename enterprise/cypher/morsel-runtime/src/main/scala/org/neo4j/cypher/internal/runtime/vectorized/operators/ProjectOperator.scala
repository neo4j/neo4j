/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v3_4.runtime._
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized._

class ProjectOperator(val projectionOps: Map[Slot, Expression], slots: SlotConfiguration) extends MiddleOperator {

  private val project = projectionOps.map {
    case (LongSlot(_, _, _),_) =>
      // We just pass along Long slot expressions without evaluation
      (_: ExecutionContext, _: OldQueryState) =>

    case (RefSlot(offset, _, _), expression) =>
      (ctx: ExecutionContext, state: OldQueryState) =>
        val result = expression(ctx, state)
        ctx.setRefAt(offset, result)
  }.toArray

  override def operate(iterationState: Iteration, data: Morsel, context: QueryContext, state: QueryState): Unit = {
    var currentRow = 0
    val longCount = slots.numberOfLongs
    val refCount = slots.numberOfReferences
    val executionContext = new MorselExecutionContext(data, longCount, refCount, currentRow = currentRow)
    val queryState = new OldQueryState(context, resources = null, params = state.params)

    while(currentRow < data.validRows) {
      executionContext.currentRow = currentRow
      project.foreach(p => p(executionContext, queryState))
      currentRow += 1
    }
  }
}
