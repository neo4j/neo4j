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
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.InputCursor
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

trait SideEffectingInputStream[CONTEXT <: RuntimeContext] {
  self: RuntimeTestSuite[CONTEXT] =>

  abstract class OnIntervalInputDataStream(inner: InputDataStream, sideEffectInterval: Long)
      extends OnNextInputDataStream(inner) {

    private val countdown = new CountDown(sideEffectInterval, sideEffectInterval)

    override protected def onNext(offset: Long): Unit = if (countdown.tick()) onInterval(offset)

    protected def onInterval(offset: Long): Unit
  }

  abstract class OnNextInputDataStream(inner: InputDataStream)
      extends InputDataStream {

    protected var offset = 0L

    override def nextInputBatch(): InputCursor = {
      Option(inner.nextInputBatch())
        .map(cursor =>
          new InputCursor {
            override def next(): Boolean = {
              val next = cursor.next()
              if (next) onNext(offset)
              offset += 1
              next
            }
            override def value(offset: Int): AnyValue = cursor.value(offset)
            override def close(): Unit = cursor.close()
          }
        )
        .orNull
    }

    protected def onNext(offset: Long): Unit
  }

  abstract class OnIntervalQuerySubscriber(inner: QuerySubscriber, sideEffectInterval: Long)
      extends OnNextQuerySubscriber(inner) {

    private val countdown = new CountDown(1L, sideEffectInterval)

    override protected def onNext(offset: Long): Unit = if (countdown.tick()) onInterval(offset)

    protected def onInterval(offset: Long): Unit
  }

  abstract class OnNextQuerySubscriber(inner: QuerySubscriber)
      extends QuerySubscriber {

    var offset = 0L

    override def onResult(numberOfFields: Int): Unit = inner.onResult(numberOfFields)

    override def onRecord(): Unit = {
      // triggering here rather than in onRecordCompleted to avoid letting clean-up happen
      onNext(offset)
      offset += 1
      inner.onRecord()
    }
    override def onField(offset: Int, value: AnyValue): Unit = inner.onField(offset, value)
    override def onError(throwable: Throwable): Unit = inner.onError(throwable)
    override def onRecordCompleted(): Unit = inner.onRecordCompleted()
    override def onResultCompleted(statistics: QueryStatistics): Unit = inner.onResultCompleted(statistics)

    protected def onNext(offset: Long): Unit
  }

  class CountDown(initial: Long, interval: Long) {
    private var countdown = initial

    def tick(): Boolean = {
      countdown -= 1
      if (countdown == 0) {
        countdown = interval
        true
      } else {
        false
      }
    }
  }
}
