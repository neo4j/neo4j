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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.internal.runtime.QueryStatistics

import java.util.concurrent.atomic.AtomicInteger

trait CountingQueryContext {
  this: DelegatingQueryContext =>

  /**
   * The statistics tracked by this context. Statistics should be read through [[getStatistics]].
   */
  protected def getTrackedStatistics: QueryStatistics

  override def getOptStatistics: Option[QueryStatistics] = Some(getStatistics)

  def getStatistics: QueryStatistics = {
    val statistics = getTrackedStatistics
    inner match {
      case context: CountingQueryContext => statistics + context.getStatistics
      case _                             => statistics
    }
  }
}

object CountingQueryContext {

  class Counter {
    val counter: AtomicInteger = new AtomicInteger()

    def count: Int = counter.get()

    def increase(amount: Int = 1): Unit = {
      counter.addAndGet(amount)
    }
  }

  object Counter {

    def apply(initialValue: Int): Counter = {
      val counter = new Counter
      counter.counter.set(initialValue)
      counter
    }
  }
}
