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

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

import scala.collection.mutable.ArrayBuffer

class ThreadSafeDataWriterTest extends CypherFunSuite {

  test("ringbuffer should produce correctly when empty") {
    val ringBufferSize = 1024
    val ringBuffer = new RingBuffer(ringBufferSize)
    val points = ArrayBuffer[DataPoint]()
    ringBuffer.consume(points += _)
    points.length should equal(0)
  }

  test("ringbuffer should produce correctly with partially filled") {
    val ringBufferSize = 1024
    val ringBuffer = new RingBuffer(ringBufferSize)
    val points = ArrayBuffer[DataPoint]()
    ringBuffer.produce(dataPointFor(1, 1))
    ringBuffer.consume(points += _)
    points.length should equal(1)
  }

  test("ringbuffer should produce correctly when full") {
    val ringBufferSize = 1024
    val ringBuffer = new RingBuffer(ringBufferSize)

    val expectedPoints: Seq[DataPoint] = dataPointsFor(0, ringBufferSize, 1)
    val points = ArrayBuffer[DataPoint]()

    expectedPoints.foreach(point => ringBuffer.produce(point))

    ringBuffer.consume(points += _)

    points.toList should equal(expectedPoints)
  }

  test("ringbuffer should be reusable after consuming full buffer") {
    val ringBufferSize = 1024
    val ringBuffer = new RingBuffer(ringBufferSize)

    val expectedPoints: Seq[DataPoint] = dataPointsFor(0, ringBufferSize, 1)
    val points = ArrayBuffer[DataPoint]()

    expectedPoints.foreach(point => ringBuffer.produce(point))
    ringBuffer.consume(points += _)
    points.toList should equal(expectedPoints)

    points.clear()

    ringBuffer.consume(points += _)
    points.toList should equal(Nil)

    expectedPoints.foreach(point => ringBuffer.produce(point))
    ringBuffer.consume(points += _)
    points.toList should equal(expectedPoints)
  }

  test("ringbuffer should error when overfilling") {
    val ringBufferSize = 1024
    val ringBuffer = new RingBuffer(ringBufferSize)

    val expectedPoints: Seq[DataPoint] = dataPointsFor(0, ringBufferSize, 1)

    expectedPoints.foreach(point => ringBuffer.produce(point))

    an[Exception] should be thrownBy ringBuffer.produce(dataPointFor(1, 1))
  }

  test("writer should not lose data points when concurrently written and flushed by many threads") {
    val innerWriter = new CollectingDataPointWriter
    val writer = new ThreadSafeDataWriter(innerWriter)

    val pointsPerThread = 512
    val threadCount = 32
    val latch = new CountDownLatch(threadCount)
    val threads = (0 until threadCount)
      .map(i => {
        val min = i * pointsPerThread
        val max = min + pointsPerThread
        ProduceConsumeThread(min, max, latch, writer)
      })

    threads.foreach(_.start)

    val isFinished = latch.await(10, TimeUnit.SECONDS)

    if (!isFinished) {
      throw new RuntimeException("Test threads did not finish on time")
    }

    val expectedPoints = threads.flatMap(thread => dataPointsFor(thread.min, thread.max, thread.getId.toInt))

    innerWriter.points.size should equal(expectedPoints.size)
    innerWriter.points.toSet should equal(expectedPoints.toSet)
  }

  private def dataPointsFor(min: Int, max: Int, threadId: Int): Seq[DataPoint] = (min until max).map(i => dataPointFor(i, threadId))

  private def dataPointFor(i: Int, threadId: Int) = DataPoint(i, i, i, i, i, threadId, i, i, NOP)

  class CollectingDataPointWriter extends DataPointWriter {
    val points: ArrayBuffer[DataPoint] = ArrayBuffer[DataPoint]()

    override def write(dataPoint: DataPoint): Unit = points += dataPoint

    override def flush(): Unit = {}
  }

  case class ProduceConsumeThread(min: Int, max: Int, latch: CountDownLatch, dataPointWriter: DataPointWriter) extends Thread {
    override def run(): Unit = {
      dataPointsFor(min, max, getId.toInt).foreach(dataPointWriter.write)
      dataPointWriter.flush()
      latch.countDown()
    }
  }

  object NOP extends Task {
    override def executeWorkUnit(): Seq[Task] = Nil

    override def canContinue: Boolean = false
  }

}
