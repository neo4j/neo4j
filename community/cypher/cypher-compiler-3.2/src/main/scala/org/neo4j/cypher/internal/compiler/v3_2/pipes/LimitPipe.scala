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
package org.neo4j.cypher.internal.compiler.v3_2.pipes

import org.neo4j.cypher.internal.compiler.v3_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_2.commands.expressions.{Expression, NumericHelper}
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.Id

import scala.collection.Iterator.empty

case class LimitPipe(source: Pipe, exp: Expression)
                    (val id: Id = new Id)
  extends PipeWithSource(source) with NumericHelper {

  exp.registerOwningPipe(this)

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {

    if(input.isEmpty)
      return Iterator.empty

    val limit = asLong(exp(state.createOrGetInitialContext())(state))

    new LimitIterator(limit, input)
  }

  class LimitIterator(limit: Long, iterator: Iterator[ExecutionContext]) extends Iterator[ExecutionContext] {
    private var remaining = limit

    def hasNext = remaining > 0 && iterator.hasNext

    def next(): ExecutionContext =
      if (remaining > 0) {
        remaining -= 1
        iterator.next()
      }
      else empty.next()
  }
}
