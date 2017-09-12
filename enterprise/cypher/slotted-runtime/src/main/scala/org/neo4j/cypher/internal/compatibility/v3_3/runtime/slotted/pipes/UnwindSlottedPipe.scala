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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Expression
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.ListSupport
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.Id
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.PrimitiveExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, PipelineInformation}
import org.neo4j.values.AnyValue

import scala.annotation.tailrec
import scala.collection.JavaConverters._

case class UnwindSlottedPipe(source: Pipe, collection: Expression, offset: Int, pipeline: PipelineInformation)
                            (val id: Id = new Id) extends PipeWithSource(source) with ListSupport {
  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    new UnwindIterator(input, state)

  private class UnwindIterator(input: Iterator[ExecutionContext], state: QueryState) extends Iterator[ExecutionContext] {
    private var currentInputRow: ExecutionContext = _
    private var unwindIterator: Iterator[AnyValue] = _
    private var nextItem: PrimitiveExecutionContext = _

    prefetch()

    override def hasNext: Boolean = nextItem != null

    override def next(): ExecutionContext =
      if (hasNext) {
        val ret = nextItem
        prefetch()
        ret
      } else {
        Iterator.empty.next() // Fail nicely
      }

    @tailrec
    private def prefetch() {
      nextItem = null
      if (unwindIterator != null && unwindIterator.hasNext) {
        nextItem = PrimitiveExecutionContext(pipeline)
        currentInputRow.copyTo(nextItem)
        nextItem.setRefAt(offset, unwindIterator.next())
      } else {
        if (input.hasNext) {
          currentInputRow = input.next()
          val value: AnyValue = collection(currentInputRow, state)
          unwindIterator = makeTraversable(value).iterator.asScala
          prefetch()
        }
      }
    }
  }
}
