/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NumericHelper
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.values.storable.FloatingPointValue

case class SkipPipe(source: Pipe, exp: Expression)
                   (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {

  protected def internalCreateResults(input: Iterator[CypherRow], state: QueryState): Iterator[CypherRow] = {
    val skipNumber = NumericHelper.evaluateStaticallyKnownNumber(exp, state)
    if (skipNumber.isInstanceOf[FloatingPointValue]) {
      val skip = skipNumber.doubleValue()
      throw new InvalidArgumentException(s"SKIP: Invalid input. '$skip' is not a valid value. Must be a non-negative integer.")
    }
    val skip = skipNumber.longValue()

    if (skip < 0) {
      throw new InvalidArgumentException(s"SKIP: Invalid input. '$skip' is not a valid value. Must be a non-negative integer.")
    }

    if(input.isEmpty)
      return Iterator.empty

    SkipPipe.drop(skip, input)
  }

}

object SkipPipe {
  def drop[T](n: Long, iterator: Iterator[T]): Iterator[T] = {
    var j = 0L
    while (j < n && iterator.hasNext) {
      iterator.next()
      j += 1
    }
    iterator
  }
}
