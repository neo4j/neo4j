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

/**
  * Scheduler tracer. Globally traces query execution..
  */
trait SchedulerTracer {
  /**
    * Trace a query execution
    */
  def traceQuery(): QueryExecutionTracer
}

/**
  * Tracer for a particular query execution
  */
trait QueryExecutionTracer {

  /**
    * Trace the scheduling of a work unit for this query execution
    */
  def scheduleWorkUnit(task: Task, upstreamWorkUnitEvent: Option[WorkUnitEvent]): ScheduledWorkUnitEvent

  /**
    * End of query execution
    */
  def stopQuery(): Unit
}

/**
  * Work unit event of a particular query execution
  */
trait ScheduledWorkUnitEvent {

  /**
    * Trace the start of a work unit event for this query execution
    */
  def start(): WorkUnitEvent
}

/**
  * Work unit event of a particular query execution
  */
trait WorkUnitEvent {

  /**
    * Unique identifier
    */
  def id: Long

  /**
    * Trace the stop of this work unit event.
    */
  def stop(): Unit
}

object SchedulerTracer {
  val NoSchedulerTracer: SchedulerTracer = new SchedulerTracer {
    override def traceQuery(): QueryExecutionTracer = NoQueryExecutionTracer
  }

  val NoQueryExecutionTracer: QueryExecutionTracer = new QueryExecutionTracer {
    override def scheduleWorkUnit(task: Task, upstreamWorkUnitEvent: Option[WorkUnitEvent]): ScheduledWorkUnitEvent = NoScheduledWorkUnitEvent
    override def stopQuery(): Unit = {}
  }

  val NoScheduledWorkUnitEvent: ScheduledWorkUnitEvent = new ScheduledWorkUnitEvent {
    override def start(): WorkUnitEvent = NoWorkUnitEvent
  }

  val NoWorkUnitEvent: WorkUnitEvent = new WorkUnitEvent {
    override def stop(): Unit = {}

    override def id: Long = -1
  }
}

