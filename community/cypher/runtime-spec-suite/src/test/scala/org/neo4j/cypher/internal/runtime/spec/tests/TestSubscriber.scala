package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.graphdb
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

import scala.collection.mutable.ArrayBuffer

class TestSubscriber extends QuerySubscriber {

  private val records = ArrayBuffer.empty[List[AnyValue]]
  private var current: Array[AnyValue] = _
  private var done = false
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

  def lastSeen: Seq[AnyValue] = current

  def resultsInLastBatch: Int = numberOfSeenRecords

  //convert to list since nested array equality doesn't work nicely in tests
  def allSeen: Seq[Seq[AnyValue]] = records
}
