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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.PrimitiveExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.Id
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, LongSlot, PipelineInformation, RefSlot}
import org.neo4j.cypher.internal.frontend.v3_3.InternalException

case class ConditionalApplyRegisterPipe(lhs: Pipe, rhs: Pipe, items: Seq[String], negated: Boolean,
                                        pipelineInformation: PipelineInformation)
                                       (val id: Id = new Id)
  extends PipeWithSource(lhs) with Pipe {

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    input.flatMap {
      lhsContext =>

        if (condition(lhsContext)) {
          val rhsState = state.withInitialContext(lhsContext)
          rhs.createResults(rhsState)
        }
        else {
          val output = PrimitiveExecutionContext(pipelineInformation)
          output.copyFrom(lhsContext)
          Iterator.single(output)
        }
    }

  private def condition(context: ExecutionContext) = {

    def notNull(name: String): Boolean = pipelineInformation.get(name) match {
      case Some(s: LongSlot) => context.getLongAt(s.offset) != -1L
      case Some(s: RefSlot) => context.getRefAt(s.offset) != null
      case _ => throw new InternalException(s"Uh oh... There was no slot for `$name`")
    }

    val cond = items.exists(notNull(_))
    if (negated) !cond else cond
  }

}
