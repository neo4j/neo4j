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
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

/**
  * The sloppy event writer is intended for in IDE debugging, and stupidly assumes that once flush is
  * called there will be no concurrent writes, so it's fine to just clear the internal data structures
  * with little thread safety. This kinda works in a single query test environment.
  */
class SloppyEventWriter extends EventWriter {
  private val MAGIC_NUMBER = 1024
  private val dataByThread: Array[ArrayBuffer[DataPoint]] =
    (0 until MAGIC_NUMBER).map(_ => new ArrayBuffer[DataPoint]).toArray

  override def report(dataPoint: DataPoint): Unit = {
    dataByThread(dataPoint.executionThreadId.toInt) += dataPoint
  }

  override def flush(): Unit = {
    val stringOutput = result()
    dataByThread.foreach(_.clear())
    println(stringOutput)
  }

  private def result(): String = {
    val t0 = dataByThread.filter(_.nonEmpty).map(_.head.startTime).min

    def toDuration(nanoSnapshot:Long) = TimeUnit.NANOSECONDS.toMicros(nanoSnapshot-t0)

    val sb = new StringBuilder
    sb ++= "queryId threadId scheduledTime(us) startTime(us) stopTime(us) pipeline\n"
    for (dp <- dataByThread.flatten) {
      sb ++= "  %d    %5d    %10d  %10d  %10d    %s\n"
        .format(dp.queryId,
                dp.executionThreadId,
                toDuration(dp.scheduledTime),
                toDuration(dp.startTime),
                toDuration(dp.stopTime),
                dp.task.toString)
    }
    sb.result()
  }
}

/**
  * Tracer of a scheduler.
  */
class SpatulaTracer(eventWriter: EventWriter) extends SchedulerTracer {

  private val queryCounter = new AtomicInteger()

  override def traceQuery(): QueryExecutionTracer =
    QueryTracer(queryCounter.incrementAndGet())

  case class QueryTracer(id: Int) extends QueryExecutionTracer {
    override def scheduleWorkUnit(task: Task): ScheduledWorkUnitEvent = {
      val scheduledTime = currentTime()
      val schedulingThread = Thread.currentThread().getId
      ScheduledWorkUnit(id, scheduledTime, schedulingThread, task)
    }

    override def stopQuery(): Unit =
      eventWriter.flush()
  }

  case class ScheduledWorkUnit(id: Int, scheduledTime: Long, schedulingThreadId: Long, task: Task) extends ScheduledWorkUnitEvent {
    override def start(): WorkUnitEvent = {
      val startTime = currentTime()
      WorkUnit(id, schedulingThreadId, scheduledTime, Thread.currentThread().getId, startTime, task)
    }
  }

  case class WorkUnit(queryId: Int,
                      schedulingThreadId: Long,
                      scheduledTime: Long,
                      executionThreadId: Long,
                      startTime: Long,
                      task: Task) extends WorkUnitEvent {

    override def stop(): Unit = {
      val stopTime = currentTime()
      eventWriter.report(
        DataPoint(queryId, schedulingThreadId, scheduledTime, executionThreadId, startTime, stopTime, task))
    }
  }

  private def currentTime(): Long = System.nanoTime()
}
