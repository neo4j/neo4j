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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted.PrimitiveExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{ExecutionContext, PipelineInformation}
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlanId

case class UnionSlottedPipe(lhs: Pipe, rhs: Pipe,
                            lhsInfo: PipelineInformation,
                            rhsInfo: PipelineInformation,
                            unionInfo: PipelineInformation,
                            mapSlots: Iterable[(ExecutionContext, ExecutionContext, PipelineInformation, QueryState) => Unit])
                           (val id: LogicalPlanId = LogicalPlanId.DEFAULT) extends Pipe {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val left = lhs.createResults(state)
    val right = rhs.createResults(state)

    new Iterator[ExecutionContext] {
      override def hasNext: Boolean = left.hasNext || right.hasNext

      override def next(): ExecutionContext = if (left.hasNext) {
        val in = left.next()
        val out = PrimitiveExecutionContext(unionInfo)
        mapSlots.foreach(f => f(in, out, lhsInfo, state))
        out
      }
      else {
        val in = right.next()
        val out = PrimitiveExecutionContext(unionInfo)
        mapSlots.foreach(f => f(in, out, rhsInfo, state))
        out
      }
    }
  }
}

case class FastLeftHandUnionSlottedPipe(lhs: Pipe, rhs: Pipe,
                            rhsInfo: PipelineInformation,
                            unionInfo: PipelineInformation,
                            mapSlots: Iterable[(ExecutionContext, ExecutionContext, PipelineInformation, QueryState) => Unit])
                           (val id: LogicalPlanId = LogicalPlanId.DEFAULT) extends Pipe {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val left = lhs.createResults(state)
    val right = rhs.createResults(state)

    new Iterator[ExecutionContext] {
      override def hasNext: Boolean = left.hasNext || right.hasNext

      override def next(): ExecutionContext = if (left.hasNext) left.next()
      else {
        val in = right.next()
        val out = PrimitiveExecutionContext(unionInfo)
        mapSlots.foreach(f => f(in, out, rhsInfo, state))
        out
      }
    }
  }
}

case class FastRightHandUnionSlottedPipe(lhs: Pipe, rhs: Pipe,
                            lhsInfo: PipelineInformation,
                            unionInfo: PipelineInformation,
                            mapSlots: Iterable[(ExecutionContext, ExecutionContext, PipelineInformation, QueryState) => Unit])
                           (val id: LogicalPlanId = LogicalPlanId.DEFAULT) extends Pipe {

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val left = lhs.createResults(state)
    val right = rhs.createResults(state)

    new Iterator[ExecutionContext] {
      override def hasNext: Boolean = left.hasNext || right.hasNext

      override def next(): ExecutionContext = if (left.hasNext) {
        val in = left.next()
        val out = PrimitiveExecutionContext(unionInfo)
        mapSlots.foreach(f => f(in, out, lhsInfo, state))
        out
      }
      else right.next()
    }
  }
}

case class FastUnionSlottedPipe(lhs: Pipe, rhs: Pipe)
                               (val id: LogicalPlanId = LogicalPlanId.DEFAULT) extends Pipe {
  // This pipe is only applicable if the ordering of the variables on the lhs equals the ordering on the rhs

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val left = lhs.createResults(state)
    val right = rhs.createResults(state)
    new Iterator[ExecutionContext] {
      override def hasNext: Boolean = left.hasNext || right.hasNext
      override def next(): ExecutionContext = if (left.hasNext) left.next()
      else right.next()
    }
  }
}
