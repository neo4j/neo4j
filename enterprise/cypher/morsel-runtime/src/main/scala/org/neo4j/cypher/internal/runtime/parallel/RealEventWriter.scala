/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.parallel

import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer

/**
  * EventWriter which accepts DataPoints and serializes them to a provided lineWriter. Uses RingBuffers.
  */
class RealEventWriter(lineWriter: String => Unit) extends EventWriter {

  private val MAGIC_NUMBER = 1024
  private val buffersByThread: Array[RingBuffer] =
    (0 until MAGIC_NUMBER).map(_ => new RingBuffer).toArray

  private var t0: Long = -1

  override def report(dataPoint: DataPoint): Unit = {
    buffersByThread(dataPoint.threadId.toInt).produce(dataPoint)
  }

  private val SEPARATOR = ","
  private val HEADER = Array("queryId", "threadId", "scheduledTime(us)", "startTime(us)", "stopTime(us)", "pipeline")

  override def flush(): Unit = {
    val dataByThread =
      for (threadBuffer <- buffersByThread) yield {
        val dataPoints = new ArrayBuffer[DataPoint]()
        threadBuffer.consume(dataPoint => dataPoints += dataPoint)
        dataPoints
      }

    if (t0 == -1) {
      t0 = dataByThread.filter(_.nonEmpty).map(_.head.startTime).min
      lineWriter(HEADER.mkString(SEPARATOR))
    }

    for {
      threadData <- dataByThread
      dataPoint <- threadData
    } lineWriter(serialize(dataPoint))
  }

  private def serialize(dataPoint: DataPoint): String = {
    def toDuration(nanoSnapshot:Long) = TimeUnit.NANOSECONDS.toMicros(nanoSnapshot-t0)

    Array(
      dataPoint.queryId.toString,
      dataPoint.threadId.toString,
      toDuration(dataPoint.scheduledTime).toString,
      toDuration(dataPoint.startTime).toString,
      toDuration(dataPoint.stopTime).toString,
      dataPoint.task.toString
    ).mkString(SEPARATOR)
  }

  class RingBuffer() {
    @volatile private var produceCount: Int = 0
    @volatile private var consumeCount: Int = 0

    val size: Int = 1 << 10 // 1024
    val mask: Int = size - 1
    val buffer = new Array[DataPoint](size)

    def produce(dp: DataPoint): Unit = {
      var claimed = -1
      var toProduce = produceCount
      while (claimed == -1) {
        var consume = consumeCount
        if (toProduce - size < consume) {
          claimed = toProduce & mask
          buffer(claimed) = dp
          produceCount += 1
        } else
          Thread.sleep(0, 1000)
      }
    }

    def consume(f: DataPoint => Unit): Unit = {
      var i = consumeCount
      val targetCount = produceCount
      while (i < targetCount) {
        f(buffer(i & mask))
        i += 1
      }
      consumeCount = i
    }
  }
}
