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

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.kernel.impl.util.ValueUtils

import scala.collection.Map

class FakePipe(val data: Iterator[Map[String, Any]]) extends Pipe {

  private var _countingIterator: CountingIterator[ExecutionContext] = _

  def this(data: Traversable[Map[String, Any]]) = this(data.toIterator)

  override def internalCreateResults(state: QueryState): CountingIterator[ExecutionContext] = {
    _countingIterator = new CountingIterator(data.map(m => ExecutionContext(collection.mutable.Map(m.mapValues(ValueUtils.of).toSeq: _*))))
    _countingIterator
  }

  def numberOfPulledRows: Int = _countingIterator.count

  override val id: Id = Id.INVALID_ID

  class CountingIterator[T](inner: Iterator[T]) extends Iterator[T] {
    private var _count = 0

    override def hasNext: Boolean = inner.hasNext

    override def next(): T = {
      _count += 1
      inner.next()
    }

    def count: Int = _count
  }
}
