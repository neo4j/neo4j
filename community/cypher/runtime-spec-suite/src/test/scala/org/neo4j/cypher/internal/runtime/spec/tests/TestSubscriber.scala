/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.util.Arrays.copyOf

import org.neo4j.graphdb
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

import scala.collection.mutable.ArrayBuffer

class TestSubscriber extends QuerySubscriber {

  private val records = ArrayBuffer.empty[List[AnyValue]]
  @volatile private var current: Array[AnyValue] = _
  @volatile private var done = false
  private var numberOfSeenRecords = 0

  override def onResult(numberOfFields: Int): Unit = {
    numberOfSeenRecords = 0
    current = new Array[AnyValue](numberOfFields)
  }

  override def onRecord(): Unit = {
    numberOfSeenRecords += 1
  }

  override def onField(offset: Int, value: AnyValue): Unit = {
    current(offset) = value
  }

  override def onRecordCompleted(): Unit = {
    records.append(current.toList)
  }

  override def onError(throwable: Throwable): Unit = {

  }

  override def onResultCompleted(statistics: graphdb.QueryStatistics): Unit = {
    done = true
  }

  def isCompleted: Boolean = done

  def lastSeen: Seq[AnyValue] = copyOf(current, current.length)

  def resultsInLastBatch: Int = numberOfSeenRecords

  //convert to list since nested array equality doesn't work nicely in tests
  def allSeen: Seq[Seq[AnyValue]] = records
}
