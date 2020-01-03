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
package org.neo4j.cypher.internal.runtime.spec.tests

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.neo4j.graphdb
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

import scala.collection.JavaConverters._

class TestSubscriber extends QuerySubscriber {

  private val records = new ConcurrentLinkedQueue[Seq[AnyValue]]()
  private var current: ConcurrentLinkedQueue[AnyValue] = _
  private val done = new AtomicBoolean(false)
  private val numberOfSeenRecords = new AtomicInteger(0)

  override def onResult(numberOfFields: Int): Unit = {
    numberOfSeenRecords.set(0)
  }

  override def onRecord(): Unit = {
    current = new ConcurrentLinkedQueue[AnyValue]()
    numberOfSeenRecords.incrementAndGet()
  }

  override def onField(value: AnyValue): Unit = {
    current.add(value)
  }

  override def onRecordCompleted(): Unit = {
    records.add(current.asScala.toSeq)
  }

  override def onError(throwable: Throwable): Unit = {

  }

  override def onResultCompleted(statistics: graphdb.QueryStatistics): Unit = {
    done.set(true)
  }

  def isCompleted: Boolean = done.get()

  def lastSeen: Seq[AnyValue] = current.asScala.toSeq

  def numberOfSeenResults: Int = numberOfSeenRecords.get()

  def allSeen: Seq[Seq[AnyValue]] = records.asScala.toSeq
}
