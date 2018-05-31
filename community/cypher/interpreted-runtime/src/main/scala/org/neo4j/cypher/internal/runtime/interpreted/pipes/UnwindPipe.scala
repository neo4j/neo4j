/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.ListSupport
import org.opencypher.v9_0.util.attribution.Id
import org.neo4j.values.AnyValue

import scala.annotation.tailrec
import scala.collection.JavaConverters._

case class UnwindPipe(source: Pipe, collection: Expression, variable: String)
                     (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) with ListSupport {

  collection.registerOwningPipe(this)

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    if (input.hasNext) new UnwindIterator(input, state) else Iterator.empty
  }

  private class UnwindIterator(input: Iterator[ExecutionContext], state: QueryState) extends Iterator[ExecutionContext] {
    private var context: ExecutionContext = _
    private var unwindIterator: Iterator[AnyValue] = _
    private var nextItem: ExecutionContext = _

    prefetch()

    override def hasNext: Boolean = nextItem != null

    override def next(): ExecutionContext = {
      if (hasNext) {
        val ret = nextItem
        prefetch()
        ret
      } else Iterator.empty.next()
    }

    @tailrec
    private def prefetch() {
      nextItem = null
      if (unwindIterator != null && unwindIterator.hasNext) {
        nextItem = executionContextFactory.copyWith(context, variable, unwindIterator.next())
      } else {
        if (input.hasNext) {
          context = input.next()
          unwindIterator = makeTraversable(collection(context, state)).iterator.asScala
          prefetch()
        }
      }
    }
  }
}
