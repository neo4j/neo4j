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

import java.util.concurrent.atomic.AtomicInteger

/**
  * Scheduler tracer that collect times and thread ids of events and report them as DataPoints.
  */
class DataPointSchedulerTracer(dataPointWriter: DataPointWriter) extends SchedulerTracer {

  private val queryCounter = new AtomicInteger()

  override def traceQuery(): QueryExecutionTracer =
    QueryTracer(queryCounter.incrementAndGet())

  case class QueryTracer(queryId: Int) extends QueryExecutionTracer {
    private final val NO_UPSTREAM : Long = -1
    override def scheduleWorkUnit(task: Task, upstreamWorkUnit: Option[WorkUnitEvent]): ScheduledWorkUnitEvent = {
      val scheduledTime = currentTime()
      val schedulingThread = Thread.currentThread().getId
      val upstreamWorkUnitId = upstreamWorkUnit.map(_.id).getOrElse(NO_UPSTREAM)
      ScheduledWorkUnit(upstreamWorkUnitId, queryId, scheduledTime, schedulingThread, task)
    }

    override def stopQuery(): Unit =
      dataPointWriter.flush()
  }

  case class ScheduledWorkUnit(upstreamWorkUnitId: Long, queryId: Int, scheduledTime: Long, schedulingThreadId: Long, task: Task) extends ScheduledWorkUnitEvent {
    override def start(): WorkUnitEvent = {
      val startTime = currentTime()
      WorkUnit(workUnitId,
               upstreamWorkUnitId,
               queryId,
               schedulingThreadId,
               scheduledTime,
               Thread.currentThread().getId,
               startTime,
               task)
    }

    private def workUnitId = scheduledTime
  }

  case class WorkUnit(override val id: Long,
                      upstreamId: Long,
                      queryId: Int,
                      schedulingThreadId: Long,
                      scheduledTime: Long,
                      executionThreadId: Long,
                      startTime: Long,
                      task: Task) extends WorkUnitEvent {

    override def stop(): Unit = {
      val stopTime = currentTime()
      dataPointWriter.write(
        DataPoint(id, upstreamId, queryId, schedulingThreadId, scheduledTime, executionThreadId, startTime, stopTime, task))
    }
  }

  private def currentTime(): Long = System.nanoTime()
}
