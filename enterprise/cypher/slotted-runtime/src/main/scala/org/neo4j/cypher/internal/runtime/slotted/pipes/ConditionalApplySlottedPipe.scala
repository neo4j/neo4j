/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.values.storable.Values

case class ConditionalApplySlottedPipe(lhs: Pipe,
                                       rhs: Pipe,
                                       longOffsets: Seq[Int],
                                       refOffsets: Seq[Int],
                                       negated: Boolean,
                                       slots: SlotConfiguration)
                                      (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(lhs) with Pipe {

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    input.flatMap {
      lhsContext =>

        if (condition(lhsContext)) {
          val rhsState = state.withInitialContext(lhsContext)
          rhs.createResults(rhsState)
        }
        else {
          val output = SlottedExecutionContext(slots)
          lhsContext.copyTo(output)
          Iterator.single(output)
        }
    }

  private def condition(context: ExecutionContext) = {
    val cond = longOffsets.exists(offset => !NullChecker.entityIsNull(context.getLongAt(offset))) ||
      refOffsets.exists(context.getRefAt(_) != Values.NO_VALUE)
    if (negated) !cond else cond
  }

}
