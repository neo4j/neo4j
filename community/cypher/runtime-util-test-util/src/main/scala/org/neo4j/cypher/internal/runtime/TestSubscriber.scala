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
package org.neo4j.cypher.internal.runtime

import org.neo4j.graphdb
import org.neo4j.internal.helpers.Exceptions
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

trait TestSubscriber extends QuerySubscriber {
  def isCompleted: Boolean

  def lastSeen: Seq[AnyValue]

  def numberOfSeenResults: Long

  def allSeen: Seq[Seq[AnyValue]]

  def queryStatistics: graphdb.QueryStatistics

  def error: Option[Throwable]
}

object TestSubscriber {
  def concurrent: TestSubscriber = new ConcurrentTestSubscriber
  def singleThreaded: TestSubscriber = new SingleThreadedTestSubscriber
  def counting: CountingSubscriber = new CountingSubscriber

  private class ConcurrentTestSubscriber extends TestSubscriber {
    private val records = new ConcurrentLinkedQueue[Seq[AnyValue]]()
    private var current: ConcurrentLinkedQueue[AnyValue] = _
    private val done = new AtomicBoolean(false)
    @volatile private var _error: Throwable = _
    private val numberOfSeenRecords = new AtomicLong(0)
    private val statistics = new AtomicReference[graphdb.QueryStatistics](null)

    override def onResult(numberOfFields: Int): Unit = {
      numberOfSeenRecords.set(0)
    }

    override def onRecord(): Unit = {
      current = new ConcurrentLinkedQueue[AnyValue]()
      numberOfSeenRecords.incrementAndGet()
    }

    override def onField(offset: Int, value: AnyValue): Unit = {
      current.add(value)
    }

    override def onRecordCompleted(): Unit = {
      records.add(current.asScala.toSeq)
    }

    override def onError(throwable: Throwable): Unit = this.synchronized {
      _error = Exceptions.chain(_error, throwable)
      done.set(true)
    }

    override def onResultCompleted(statistics: graphdb.QueryStatistics): Unit = {
      this.statistics.set(statistics)
      done.set(true)
    }

    override def isCompleted: Boolean = done.get()

    override def lastSeen: Seq[AnyValue] = current.asScala.toSeq

    override def numberOfSeenResults: Long = numberOfSeenRecords.get()

    override def allSeen: Seq[Seq[AnyValue]] = records.asScala.toSeq

    override def queryStatistics: graphdb.QueryStatistics = statistics.get()
    override def error: Option[Throwable] = Option(_error)
  }

  private class SingleThreadedTestSubscriber extends TestSubscriber {
    private val records = new mutable.ArrayBuffer[Seq[AnyValue]]()
    private var current: mutable.ArrayBuffer[AnyValue] = _
    private var done = false
    private var numberOfSeenRecords = 0
    private var statistics: graphdb.QueryStatistics = _

    override def onResult(numberOfFields: Int): Unit = {
      numberOfSeenRecords = 0
    }

    override def onRecord(): Unit = {
      current = new mutable.ArrayBuffer[AnyValue]()
      numberOfSeenRecords += 1
    }

    override def onField(offset: Int, value: AnyValue): Unit = {
      current += value
    }

    override def onRecordCompleted(): Unit = {
      records += current.toSeq
    }

    override def onError(throwable: Throwable): Unit = {
      // TODO: Support errors
    }

    override def error: Option[Throwable] = None // TODO: Support errors

    override def onResultCompleted(statistics: graphdb.QueryStatistics): Unit = {
      this.statistics = statistics
      done = true
    }

    override def isCompleted: Boolean = done

    override def lastSeen: Seq[AnyValue] = current.toSeq

    override def numberOfSeenResults: Long = numberOfSeenRecords

    override def allSeen: Seq[Seq[AnyValue]] = records.toSeq

    override def queryStatistics: graphdb.QueryStatistics = statistics
  }

  final class CountingSubscriber extends TestSubscriber {
    private val _numberOfSeenRecords = new AtomicLong(0)
    private val _completed = new AtomicBoolean(false)
    private var _error: Throwable = null
    private val _statistics = new AtomicReference[graphdb.QueryStatistics](null)

    override def error: Option[Throwable] = Option(_error)

    override def onResult(numberOfFields: Int): Unit = {}
    override def onRecord(): Unit = {}
    override def onField(offset: Int, value: AnyValue): Unit = {}
    override def onRecordCompleted(): Unit = _numberOfSeenRecords.incrementAndGet()

    override def onError(throwable: Throwable): Unit = this.synchronized {
      _error = Exceptions.chain(_error, throwable)
      _completed.set(true)
    }

    override def onResultCompleted(statistics: graphdb.QueryStatistics): Unit = {
      _statistics.set(statistics)
      _completed.set(true)
    }

    override def isCompleted: Boolean = _completed.get()

    override def lastSeen: Seq[AnyValue] =
      throw new UnsupportedOperationException("Use TestSubscriber.concurrent instead")

    override def numberOfSeenResults: Long = _numberOfSeenRecords.get()

    override def allSeen: Seq[Seq[AnyValue]] =
      throw new UnsupportedOperationException("Use TestSubscriber.concurrent instead")

    override def queryStatistics: graphdb.QueryStatistics = _statistics.get()
  }
}
