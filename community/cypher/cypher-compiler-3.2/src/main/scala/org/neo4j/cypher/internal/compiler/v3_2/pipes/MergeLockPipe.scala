/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v3_2.pipes

import org.neo4j.cypher.internal.compiler.v3_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_2.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_2.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.Id
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.{Exclusive, LockMode}

case class MergeLockPipe(src: Pipe, locksToGrab: Seq[MergeLockDescription], lockMode: LockMode)
                        (val id: Id = new Id)
                        (implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(src, pipeMonitor) {


  private val exclusive = lockMode == Exclusive

  override protected def internalCreateResults(input: Iterator[ExecutionContext],
                                               state: QueryState): Iterator[ExecutionContext] = {
    input map { currentRow =>
      for {
        MergeLockDescription(label, props) <- locksToGrab
        labelId = label.getOrCreateId(state.query)
      } {

        val predicates = for {
          (propKey, expression) <- props
          propId = propKey.getOrCreateId(state.query)
        } yield {
          val value = expression(currentRow)(state)
          (propId, value)
        }

        state.query.grabMergeLocks(labelId.id, predicates, exclusive)
      }

      currentRow
    }
  }
}

case class MergeLockDescription(label: LazyLabel, props: Seq[(KeyToken, Expression)])