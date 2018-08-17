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

import scala.collection.mutable.ArrayBuffer

/**
  * DataPointWriter which can be written to in a thread-safe way, and writes to a delegate on flush.
  * <br/>
  * NOTE: in its present form this class is primarily a utility for developer-support.
  */
class ThreadSafeDataWriter(delegate: DataPointWriter,
                           ringBufferBitSize: Int = RingBuffer.defaultBitSize,
                           ringBufferMaxRetries: Int = RingBuffer.defaultMaxRetries) extends DataPointWriter {

  private val MAX_CLIENT_THREADS: Int = 1024
  private val buffersByThread: Array[RingBuffer] =
    (0 until MAX_CLIENT_THREADS).map(_ => new RingBuffer(ringBufferBitSize, ringBufferMaxRetries)).toArray

  private var t0: Long = -1

  override def write(dataPoint: DataPoint): Unit = {
    if (dataPoint.executionThreadId < MAX_CLIENT_THREADS) {
      buffersByThread(dataPoint.executionThreadId.toInt).produce(dataPoint)
    } else {
      throw new IllegalArgumentException(s"Thread ID exceeded maximum: ${dataPoint.executionThreadId} > ${MAX_CLIENT_THREADS - 1}")
    }
  }

  /**
    * Flush all data to the inner data writer.
    * <br/>
    * <br/>
    * WARNING: method is synchronized!
    * <br/>
    * <br/>
    * flush() is only called from SchedulerTracer.stopQuery(), at which point no more data will be written for that particular query,
    * but it can be called from multiple threads concurrently, potentially resulting in ring buffers being modified (consumed) by different threads.
    * <ul>
    * <li>When there is no concurrency (max one query at any time) flush() will flush all & only points of one query.</li>
    * <li>With concurrent queries flush() may flush some (not necessarily all) points of multiple queries.
    * Additionally, there is race/contention on delegate.flush().
    * The current solution of synchronizing flush() protects against multiple races, including concurrent modification of RingBuffer.consumeCount.</li>
    * </ul>
    */
  override def flush(): Unit = synchronized {
    val dataByThread: Array[ArrayBuffer[DataPoint]] =
      for (threadBuffer <- buffersByThread) yield {
        val dataPoints = new ArrayBuffer[DataPoint]()
        threadBuffer.consume(dataPoint => dataPoints += dataPoint)
        dataPoints
      }

    if (t0 == -1) {
      /*
       Race (if not synchronized)
        1. thread-1 & thread-2 get here
        2. thread-1 proceeds to delegate.flush() before thread-2 proceeds
        3. thread-2 updates t0 (again)
       */
      t0 = dataByThread.filter(_.nonEmpty).map(_.head.scheduledTime).min
    }

    for {
      threadData <- dataByThread
      dataPoint <- threadData
    } delegate.write(dataPoint.withTimeZero(t0))

    // Race (if not synchronized)
    //  > race/contention here when concurrent queries flush
    delegate.flush()
  }
}

object RingBuffer {
  val defaultBitSize: Int = 10 // buffer size 1024
  val defaultMaxRetries: Int = 1000
}

/**
  * Single producer, single consumer ring buffer.
  *
  * This means that the ring buffer is safe to use with one thread producing values, and another one consuming them.
  *
  * @param bitSize size of the ringbuffer in number of bits
  * @param maxRetries number of retries to attempt if the buffer is full
  */
class RingBuffer(bitSize: Int = RingBuffer.defaultBitSize,
                 private val maxRetries: Int = RingBuffer.defaultMaxRetries) {

  @volatile private var produceCount: Int = 0
  @volatile private var consumeCount: Int = 0

  val size: Int = 1 << bitSize
  private val mask: Int = size - 1
  private val buffer = new Array[DataPoint](size)

  /**
    * Produce (add) one additional DataPoint into this RingBuffer. Not Thread-safe.
    *
    * @param dp the DataPoint to produce
    */
  def produce(dp: DataPoint): Unit = {
    var claimed = -1
    val snapshotProduce = produceCount
    var retries = 0
    while (claimed == -1) {
      val snapshotConsume = consumeCount
      if (snapshotProduce - size < snapshotConsume) {
        claimed = snapshotProduce & mask
        buffer(claimed) = dp
        produceCount += 1
      } else {
        retries += 1
        if (retries < maxRetries) {
          Thread.sleep(0, 1000)
        } else {
          // full buffer can prevent query execution from making progress
          throw new RuntimeException("Exceeded max retries")
        }
      }
    }
  }

  /**
    * Consume all available DataPoints from this RingBuffer. Not thread-safe.
    */
  def consume(f: DataPoint => Unit): Unit = {
    var snapshotConsume = consumeCount
    val snapshotProduce = produceCount
    while (snapshotConsume < snapshotProduce) {
      f(buffer(snapshotConsume & mask))
      snapshotConsume += 1
    }
    consumeCount = snapshotConsume
  }
}
