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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, ListSupport}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.values.AnyValue

import scala.annotation.tailrec
import scala.collection.JavaConverters._

case class UnwindSlottedPipe(source: Pipe,
                             collection: Expression,
                             offset: Int,
                             slots: SlotConfiguration)
                            (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) with ListSupport {
  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    new UnwindIterator(input, state)

  private class UnwindIterator(input: Iterator[ExecutionContext], state: QueryState) extends Iterator[ExecutionContext] {
    private var currentInputRow: ExecutionContext = _
    private var unwindIterator: Iterator[AnyValue] = _
    private var nextItem: SlottedExecutionContext = _

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
        nextItem = SlottedExecutionContext(slots)
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
