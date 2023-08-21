/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.FakePipe.CountingIterator
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.storageengine.api.RelationshipVisitor

import scala.collection.Map

case class FakePipe(data: Iterable[Map[String, Any]]) extends Pipe {

  private var _countingIterator: CountingIterator[CypherRow] = _
  private var _createCount = 0

  def this(data: Iterator[Map[String, Any]]) = this(data.toList)

  override def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    _createCount += 1
    _countingIterator = new CountingIterator(data.map(m =>
      CypherRow(collection.mutable.Map(m.mapValues(ValueUtils.of).toSeq: _*))
    ).iterator)
    _countingIterator
  }

  def numberOfPulledRows: Int = _countingIterator.count

  def wasClosed: Boolean = _countingIterator.wasClosed

  def resetClosed(): Unit = _countingIterator.resetClosed()

  def currentIterator: CountingIterator[CypherRow] = _countingIterator

  def createCount: Int = _createCount

  override val id: Id = Id.INVALID_ID
}

object FakePipe {

  class CountingIterator[T](inner: Iterator[T]) extends ClosingIterator[T] {
    private var _count = 0
    private var _closed = false

    override protected[this] def closeMore(): Unit = _closed = true

    override protected[this] def innerHasNext: Boolean = inner.hasNext

    override def next(): T = {
      _count += 1
      inner.next()
    }

    def count: Int = _count

    def wasClosed: Boolean = _closed

    def resetClosed(): Unit = _closed = false
  }

  class CountingLongIterator(inner: ClosingLongIterator) extends ClosingLongIterator {
    private var _count = 0
    private var _closed = false

    override def close(): Unit = _closed = true

    override protected[this] def innerHasNext: Boolean = inner.hasNext

    override def next(): Long = {
      _count += 1
      inner.next()
    }

    def count: Int = _count

    def wasClosed: Boolean = _closed

    def resetClosed(): Unit = _closed = false

  }

  class CountingRelationshipIterator(inner: ClosingLongIterator with RelationshipIterator) extends ClosingLongIterator
      with RelationshipIterator {
    private var _count = 0
    private var _closed = false

    override def close(): Unit = _closed = true

    override protected[this] def innerHasNext: Boolean = inner.hasNext

    override def next(): Long = {
      _count += 1
      inner.next()
    }

    def count: Int = _count

    def wasClosed: Boolean = _closed

    def resetClosed(): Unit = _closed = false

    override def relationshipVisit[EXCEPTION <: Exception](
      relationshipId: Long,
      visitor: RelationshipVisitor[EXCEPTION]
    ): Boolean = {
      visitor.visit(relationshipId, typeId(), startNodeId(), endNodeId())
      true
    }

    override def startNodeId(): Long = inner.startNodeId()

    override def endNodeId(): Long = inner.endNodeId()

    override def typeId(): Int = inner.typeId()
  }
}
